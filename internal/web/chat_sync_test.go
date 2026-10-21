package web

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func testServerKey(t *testing.T) string {
	t.Helper()
	pub, _, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	return base64.RawURLEncoding.EncodeToString(pub)
}

// newChatSyncServer: one active config that pins a server key, its chat server
// built the way startup builds it.
func newChatSyncServer(t *testing.T) *Server {
	t.Helper()
	s := newChatTestServer(t)
	if err := s.saveProfiles(&ProfileList{Active: "pa", Profiles: []Profile{
		{ID: "pa", Nickname: "A", Config: Config{Domain: "a.example.com", Key: "k", ServerKey: testServerKey(t)}},
	}}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	h := s.chat
	h.mu.Lock()
	h.ctx = ctx
	if err := h.ensureIdentityLocked(); err != nil {
		t.Fatal(err)
	}
	h.rebuildServersLocked()
	h.mu.Unlock()
	return s
}

func postProfiles(t *testing.T, s *Server, body map[string]any) {
	t.Helper()
	b, _ := json.Marshal(body)
	w := httptest.NewRecorder()
	s.handleProfiles(w, httptest.NewRequest(http.MethodPost, "/api/profiles", strings.NewReader(string(b))))
	if w.Code != http.StatusOK {
		t.Fatalf("POST /api/profiles %v: %d %s", body["action"], w.Code, w.Body.String())
	}
}

func chatServer(s *Server, key string) *perServerChat {
	s.chat.mu.Lock()
	defer s.chat.mu.Unlock()
	return s.chat.servers[key]
}

// trackStop wraps a server's cancel so the test can see it was stopped.
func trackStop(s *Server, key string) *atomic.Bool {
	var stopped atomic.Bool
	s.chat.mu.Lock()
	ps := s.chat.servers[key]
	orig := ps.cancel
	ps.cancel = func() { stopped.Store(true); orig() }
	s.chat.mu.Unlock()
	return &stopped
}

func findProfileID(t *testing.T, s *Server, domain string) string {
	t.Helper()
	for _, p := range loadProfilesT(t, s).Profiles {
		if p.Config.Domain == domain {
			return p.ID
		}
	}
	t.Fatalf("no profile for %s", domain)
	return ""
}

// Adding a second (non-active) config never re-inits the fetcher, so chat's
// reset never ran and the new server stayed unusable until a switch/restart.
func TestChatAddedConfigGetsServerWithoutSwitch(t *testing.T) {
	s := newChatSyncServer(t)
	a := chatServer(s, "a.example.com")
	if a == nil {
		t.Fatal("startup did not build the active config's chat server")
	}
	a.backoffUntil = time.Now().Add(time.Hour) // per-server state that must survive

	postProfiles(t, s, map[string]any{"action": "create", "profile": map[string]any{
		"nickname": "B", "config": map[string]any{"domain": "B.example.com.", "key": "k2", "serverKey": testServerKey(t)}}})

	if chatServer(s, "b.example.com") == nil {
		t.Fatal("added config has no chat server until a switch/restart (the bug)")
	}
	if got := chatServer(s, "a.example.com"); got != a || got.backoffUntil.IsZero() {
		t.Fatal("unchanged server was rebuilt and lost its state")
	}
	if loadProfilesT(t, s).Active != "pa" {
		t.Fatal("adding a config must not switch the active one")
	}
}

func TestChatAddedConfigWithoutServerKeyHasNoChat(t *testing.T) {
	s := newChatSyncServer(t)
	postProfiles(t, s, map[string]any{"action": "create", "profile": map[string]any{
		"nickname": "N", "config": map[string]any{"domain": "nokey.example.com", "key": "k"}}})
	if chatServer(s, "nokey.example.com") != nil {
		t.Fatal("chat is fail-closed: a config without sk= must not get a chat server")
	}
}

func TestChatEditedConfigServerIsRebuiltAndOldStopped(t *testing.T) {
	s := newChatSyncServer(t)
	skB := testServerKey(t)
	postProfiles(t, s, map[string]any{"action": "create", "profile": map[string]any{
		"nickname": "B", "config": map[string]any{"domain": "b.example.com", "key": "k2", "serverKey": skB}}})
	id := findProfileID(t, s, "b.example.com")

	// Rename only: same client, new name.
	old := chatServer(s, "b.example.com")
	postProfiles(t, s, map[string]any{"action": "update", "profile": map[string]any{
		"id": id, "nickname": "B renamed", "config": map[string]any{"domain": "b.example.com", "key": "k2", "serverKey": skB}}})
	if got := chatServer(s, "b.example.com"); got != old || got.name != "B renamed" {
		t.Fatalf("rename should keep the client and update its name (same=%v name=%q)", got == old, got.name)
	}

	// New extra domains: the client must be rebuilt, the old one stopped.
	stopped := trackStop(s, "b.example.com")
	postProfiles(t, s, map[string]any{"action": "update", "profile": map[string]any{
		"id": id, "nickname": "B renamed", "config": map[string]any{
			"domain": "b.example.com", "key": "k2", "serverKey": skB, "extraDomains": []string{"x.b.example.com"}}}})
	if got := chatServer(s, "b.example.com"); got == old || got == nil {
		t.Fatal("edited config kept its stale chat client")
	}
	if !stopped.Load() {
		t.Fatal("replaced chat client was not stopped (leaks its fetcher goroutines)")
	}
}

func TestChatDeletedConfigServerIsDroppedAndStopped(t *testing.T) {
	s := newChatSyncServer(t)
	postProfiles(t, s, map[string]any{"action": "create", "profile": map[string]any{
		"nickname": "B", "config": map[string]any{"domain": "b.example.com", "key": "k2", "serverKey": testServerKey(t)}}})
	stopped := trackStop(s, "b.example.com")
	postProfiles(t, s, map[string]any{"action": "delete", "profile": map[string]any{"id": findProfileID(t, s, "b.example.com")}})
	if chatServer(s, "b.example.com") != nil {
		t.Fatal("deleted config still has a chat server")
	}
	if !stopped.Load() {
		t.Fatal("dropped chat client was not stopped")
	}
	if chatServer(s, "a.example.com") == nil {
		t.Fatal("deleting another config dropped the active one's server")
	}
}

func TestChatFullRebuildStopsOldClients(t *testing.T) {
	s := newChatSyncServer(t)
	stopped := trackStop(s, "a.example.com")
	s.chat.mu.Lock()
	s.chat.rebuildServersLocked()
	s.chat.mu.Unlock()
	if !stopped.Load() {
		t.Fatal("rebuild replaced a client without stopping it")
	}
}

func TestSameChatConfig(t *testing.T) {
	base := Config{Domain: "a.example.com", Key: "k", ServerKey: "sk", ExtraDomains: []string{"x"}}
	cases := []struct {
		name string
		edit func(c *Config)
		same bool
	}{
		{"identical", func(c *Config) {}, true},
		{"domain case/dot", func(c *Config) { c.Domain = "A.example.com." }, true},
		{"resolvers are shared, not per server", func(c *Config) { c.Resolvers = []string{"1.1.1.1"} }, true},
		{"connection settings are global", func(c *Config) { c.RateLimit = 9; c.QueryMode = "double" }, true},
		{"key", func(c *Config) { c.Key = "k2" }, false},
		{"server key", func(c *Config) { c.ServerKey = "sk2" }, false},
		{"extra domains", func(c *Config) { c.ExtraDomains = []string{"y"} }, false},
	}
	for _, tc := range cases {
		c := base
		c.ExtraDomains = append([]string(nil), base.ExtraDomains...)
		tc.edit(&c)
		if got := sameChatConfig(base, c); got != tc.same {
			t.Errorf("%s: sameChatConfig = %v, want %v", tc.name, got, tc.same)
		}
	}
}
