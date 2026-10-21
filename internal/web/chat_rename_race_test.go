package web

import (
	"context"
	"crypto/ed25519"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/sartoopjj/thefeed/internal/client"
)

// Renaming a config mutates perServerChat.name IN PLACE (syncServersLocked
// keeps the live client and only swaps the nickname), while availability
// probes read that field from their own goroutines for as long as a probe
// runs. Run under -race: with the read unguarded this reports a data race on
// chatHub.servers[...].name.
func TestChatRenameDuringProbeIsRaceFree(t *testing.T) {
	s := newChatTestServer(t)
	h := s.chat

	id, err := client.NewChatIdentity(make([]byte, 32))
	if err != nil {
		t.Fatal(err)
	}
	h.identity = id
	h.ctx = context.Background()

	// A resolver that swallows packets, so probes stay in flight while we rename.
	pc, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer pc.Close()

	pub, _, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatal(err)
	}
	srvKey := base64.RawURLEncoding.EncodeToString(pub)

	writeProfiles := func(nickname string) {
		pl := ProfileList{
			Active: "p1",
			Profiles: []Profile{{
				ID: "p1", Nickname: nickname,
				Config: Config{
					Domain: "race.example.com", Key: "k", ServerKey: srvKey,
					Resolvers: []string{pc.LocalAddr().String()},
				},
			}},
			ResolverBank: []string{pc.LocalAddr().String()},
		}
		raw, err := json.Marshal(pl)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(s.dataDir, "profiles.json"), raw, 0o600); err != nil {
			t.Fatal(err)
		}
	}

	writeProfiles("original")
	h.syncServers()
	h.mu.Lock()
	n := len(h.servers)
	h.mu.Unlock()
	if n != 1 {
		t.Fatalf("expected the config to produce 1 chat server, got %d", n)
	}
	h.mu.Lock()
	h.enabled = map[string]bool{"race.example.com": true}
	h.mu.Unlock()

	var wg sync.WaitGroup

	// Writer: the real rename path.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 60; i++ {
			writeProfiles(fmt.Sprintf("renamed-%d", i))
			h.syncServers()
		}
	}()

	// Reader: the availability path, which copies ps.name into the response.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 60; i++ {
			rec := httptest.NewRecorder()
			s.handleChatAvailability(rec,
				httptest.NewRequest(http.MethodGet, "/api/chat/availability?retry=1", nil))
		}
	}()

	wg.Wait()
}

// Hammers every path that takes chatHub.mu at once, including the ones that
// now take it a second time through serverName/probing. A lock cycle would
// hang here instead of failing, so the watchdog turns that into a failure.
func TestChatHubLocksDoNotDeadlock(t *testing.T) {
	s := newChatTestServer(t)
	h := s.chat

	id, err := client.NewChatIdentity(make([]byte, 32))
	if err != nil {
		t.Fatal(err)
	}
	h.identity = id
	h.ctx = context.Background()

	pc, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer pc.Close()
	pub, _, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatal(err)
	}
	srvKey := base64.RawURLEncoding.EncodeToString(pub)

	pl := ProfileList{Active: "p1", ResolverBank: []string{pc.LocalAddr().String()}}
	for i := 0; i < 3; i++ {
		pl.Profiles = append(pl.Profiles, Profile{
			ID: fmt.Sprintf("p%d", i), Nickname: fmt.Sprintf("cfg%d", i),
			Config: Config{
				Domain: fmt.Sprintf("d%d.example.com", i), Key: "k", ServerKey: srvKey,
				Resolvers: []string{pc.LocalAddr().String()},
			},
		})
	}
	raw, err := json.Marshal(pl)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(s.dataDir, "profiles.json"), raw, 0o600); err != nil {
		t.Fatal(err)
	}
	h.syncServers()
	h.mu.Lock()
	for k := range h.servers {
		h.enabled[k] = true
	}
	h.mu.Unlock()

	done := make(chan struct{})
	go func() {
		defer close(done)
		var wg sync.WaitGroup
		hammer := func(f func()) {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for i := 0; i < 25; i++ {
					f()
				}
			}()
		}
		hammer(func() { h.syncServers() })
		hammer(func() {
			s.handleChatAvailability(httptest.NewRecorder(),
				httptest.NewRequest(http.MethodGet, "/api/chat/availability", nil))
		})
		hammer(func() {
			s.handleChatInfo(httptest.NewRecorder(),
				httptest.NewRequest(http.MethodGet, "/api/chat/info", nil))
		})
		hammer(func() { h.pollAllServers(context.Background(), nil, 50*time.Millisecond) })
		hammer(func() { h.clearBackoffs() })
		hammer(func() { _ = h.pollOrder(time.Now()) })
		wg.Wait()
	}()

	select {
	case <-done:
	case <-time.After(60 * time.Second):
		t.Fatal("deadlock: chat hub locks did not make progress in 60s")
	}
}
