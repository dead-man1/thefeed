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
	"regexp"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/sartoopjj/thefeed/internal/client"
)

// The frontend aborts the availability request after CHAT_AVAIL_TIMEOUT_MS and
// falls back to "no servers available". A sweep that outlives that abort is
// thrown away, so the UI reports 0 servers while a working one exists. Read the
// real constant so the two sides cannot drift apart.
func TestChatAvailSweepBeatsFrontendAbort(t *testing.T) {
	src, err := os.ReadFile("static/js/chat.js")
	if err != nil {
		t.Fatal(err)
	}
	m := regexp.MustCompile(`CHAT_AVAIL_TIMEOUT_MS\s*=\s*(\d+)`).FindSubmatch(src)
	if m == nil {
		t.Fatal("CHAT_AVAIL_TIMEOUT_MS not found in chat.js")
	}
	ms, err := strconv.Atoi(string(m[1]))
	if err != nil {
		t.Fatal(err)
	}
	frontend := time.Duration(ms) * time.Millisecond
	// Generous headroom: the first paint is a convenience, never a deadline the
	// UI can trip over.
	if chatAvailFirstPaint*2 > frontend {
		t.Fatalf("first paint %v leaves no margin under the frontend's %v abort",
			chatAvailFirstPaint, frontend)
	}
	// A slow network must not be mistaken for a dead server, so the background
	// probe outlives the first paint by a wide margin.
	if chatAvailProbeTimeout < 30*time.Second {
		t.Fatalf("probe budget %v gives up on a working config too soon",
			chatAvailProbeTimeout)
	}
	if chatAvailProbeTimeout <= chatAvailFirstPaint {
		t.Fatal("probes must outlive the response, or stragglers are abandoned")
	}
}

// The response must never wait for a probe still in flight: waiting for the
// slowest server is what pushed it past the frontend's abort and showed
// "0 active servers". A row being probed is reported as checking, and a row
// with no verdict yet must not be blamed for anything.
func TestChatAvailabilityDoesNotWaitForProbeInFlight(t *testing.T) {
	s := newChatTestServer(t)
	h := s.chat
	id, err := client.NewChatIdentity(make([]byte, 32))
	if err != nil {
		t.Fatal(err)
	}
	h.identity = id
	ps := &perServerChat{serverKey: "slow.example.com", domain: "slow.example.com"}
	h.servers = map[string]*perServerChat{ps.serverKey: ps}
	h.enabled = map[string]bool{ps.serverKey: true}

	// Stand in for a background probe from an earlier list open.
	if !h.beginProbe(ps.serverKey) {
		t.Fatal("could not claim the probe")
	}
	defer h.endProbe(ps.serverKey)

	rec := httptest.NewRecorder()
	start := time.Now()
	s.handleChatAvailability(rec, httptest.NewRequest(http.MethodGet, "/api/chat/availability", nil))
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Fatalf("response took %v: it waited on the in-flight probe", elapsed)
	}

	var resp struct {
		Reason  string             `json:"reason"`
		Servers []chatServerResult `json:"servers"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatal(err)
	}
	if len(resp.Servers) != 1 || !resp.Servers[0].Checking {
		t.Fatalf("in-flight probe not reported as checking: %+v", resp.Servers)
	}
	if resp.Servers[0].Reason != "" || resp.Reason != "" {
		t.Fatalf("blamed a server with no verdict yet: %+v / %q", resp.Servers[0], resp.Reason)
	}
	if ps.failures != 0 || !ps.backoffUntil.IsZero() {
		t.Fatalf("penalised a server with no verdict: failures=%d", ps.failures)
	}
}

// Repeated list opens must not stack probes on the same server.
func TestChatProbeClaimIsExclusive(t *testing.T) {
	h := newChatTestServer(t).chat
	if !h.beginProbe("a.example.com") {
		t.Fatal("first claim refused")
	}
	if h.beginProbe("a.example.com") {
		t.Fatal("second claim granted: duplicate probes would stack")
	}
	if !h.beginProbe("b.example.com") {
		t.Fatal("a different server was blocked")
	}
	h.endProbe("a.example.com")
	if !h.beginProbe("a.example.com") {
		t.Fatal("claim not released")
	}
}

// A probe the caller cut short taught us nothing. It must report "checking"
// and record NO failure -- only a verdict we actually received may park a
// server (see chat-confirmed-stays-available).
func TestChatProbeCutShortIsNotAVerdict(t *testing.T) {
	s := newChatTestServer(t)
	h := s.chat
	f, err := client.NewFetcher("slow.example.com", "k", []string{"192.0.2.1"})
	if err != nil {
		t.Fatal(err)
	}
	ps := &perServerChat{serverKey: "slow.example.com", domain: "slow.example.com", f: f}

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // the caller gave up before this probe got anywhere
	res, _ := h.probeOneServer(ctx, ps, false, chatAvailProbeTimeout)

	if !res.Checking {
		t.Fatalf("cut-short probe returned a verdict: %+v", res)
	}
	if res.Available {
		t.Fatal("cut-short probe claimed the server works")
	}
	if res.Reason != "" {
		t.Fatalf("cut-short probe blamed the server: %q", res.Reason)
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if ps.failures != 0 || !ps.backoffUntil.IsZero() {
		t.Fatalf("cut-short probe penalised the server: failures=%d until=%v",
			ps.failures, ps.backoffUntil)
	}
}

// The flip side: spending our OWN budget IS a verdict. Without this the row
// sits on "در حال بررسی" forever and the server is never backed off.
func TestChatProbeOwnBudgetExpiryIsAVerdict(t *testing.T) {
	s := newChatTestServer(t)
	h := s.chat
	id, err := client.NewChatIdentity(make([]byte, 32))
	if err != nil {
		t.Fatal(err)
	}
	h.identity = id

	// A resolver that accepts packets and never answers: the budget must run
	// out on its own, with the caller's context still perfectly alive.
	pc, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer pc.Close()

	f, err := client.NewFetcher("dead.example.com", "k", []string{pc.LocalAddr().String()})
	if err != nil {
		t.Fatal(err)
	}
	pub, _, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.SetServerPublicKey(base64.RawURLEncoding.EncodeToString(pub)); err != nil {
		t.Fatal(err)
	}
	ps := &perServerChat{
		serverKey: "dead.example.com", domain: "dead.example.com",
		f: f, client: client.NewChatClient(f, id),
	}

	res, _ := h.probeOneServer(context.Background(), ps, true, 400*time.Millisecond)

	if res.Checking {
		t.Fatal("a probe that used its whole budget must not stay 'checking' forever")
	}
	if res.Available {
		t.Fatal("claimed a silent server works")
	}
	if res.Reason == "" {
		t.Fatal("no reason given, so the UI has nothing to show")
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if ps.failures == 0 || ps.backoffUntil.IsZero() {
		t.Fatalf("unreachable server not backed off: failures=%d", ps.failures)
	}
}

// Concurrent list opens while probes are in flight. The probe goroutines
// outlive each request and share the handler's locals, so this is the shape
// that hides data races (one such write-back bug was found by review, not by
// this test -- keep it running under -race).
func TestChatAvailabilityConcurrentOpens(t *testing.T) {
	s := newChatTestServer(t)
	h := s.chat
	id, err := client.NewChatIdentity(make([]byte, 32))
	if err != nil {
		t.Fatal(err)
	}
	h.identity = id
	h.ctx = context.Background()

	// Resolvers that accept packets and never answer: probes stay in flight
	// across every request below.
	h.servers = map[string]*perServerChat{}
	h.enabled = map[string]bool{}
	for i := 0; i < 4; i++ {
		pc, err := net.ListenPacket("udp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		defer pc.Close()
		key := fmt.Sprintf("s%d.example.com", i)
		f, err := client.NewFetcher(key, "k", []string{pc.LocalAddr().String()})
		if err != nil {
			t.Fatal(err)
		}
		pub, _, err := ed25519.GenerateKey(nil)
		if err != nil {
			t.Fatal(err)
		}
		if err := f.SetServerPublicKey(base64.RawURLEncoding.EncodeToString(pub)); err != nil {
			t.Fatal(err)
		}
		// Without an ACTIVE resolver list the fetcher fails instantly and no
		// probe is ever in flight -- which is what this test needs.
		f.SetActiveResolvers([]string{pc.LocalAddr().String()})
		h.servers[key] = &perServerChat{
			serverKey: key, domain: key, f: f, client: client.NewChatClient(f, id),
		}
		h.enabled[key] = true
	}

	var wg sync.WaitGroup
	start := time.Now()
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			rec := httptest.NewRecorder()
			s.handleChatAvailability(rec, httptest.NewRequest(http.MethodGet, "/api/chat/availability", nil))
			if rec.Code != 200 {
				t.Errorf("status %d", rec.Code)
				return
			}
			var resp struct {
				Servers []chatServerResult `json:"servers"`
			}
			if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
				t.Error(err)
				return
			}
			if len(resp.Servers) != 4 {
				t.Errorf("got %d servers, want 4", len(resp.Servers))
			}
			// Nothing answered, so nothing may be claimed available.
			for _, sv := range resp.Servers {
				if sv.Available {
					t.Errorf("%s reported available with no answer", sv.Key)
				}
			}
		}()
	}
	wg.Wait()
	// None of these requests may wait for the probes themselves: that is the
	// regression that pushed the response past the frontend's abort.
	if elapsed := time.Since(start); elapsed > chatAvailFirstPaint+5*time.Second {
		t.Fatalf("requests took %v: they waited on the in-flight probes", elapsed)
	}
}
