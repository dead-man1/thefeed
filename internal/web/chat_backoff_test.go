package web

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sartoopjj/thefeed/internal/client"
)

// A broken config must not keep costing a probe every couple of minutes: each
// retry competes for the shared resolver pool and starves the server the user
// actually chats on. Consecutive failures double the wait, up to a ceiling.
func TestChatBackoffEscalates(t *testing.T) {
	s := newChatTestServer(t)
	h := s.chat
	ps := &perServerChat{serverKey: "dead.example.com"}

	transient := errors.New("dns timeout")
	base := chatServerBackoff(transient)

	var last time.Duration
	for i := 1; i <= 8; i++ {
		until := h.noteFailure(ps, transient)
		got := time.Until(until).Round(time.Second)

		want := base << (i - 1)
		if want > chatTransientBackoffMax {
			want = chatTransientBackoffMax
		}
		if d := got - want; d > 2*time.Second || d < -2*time.Second {
			t.Fatalf("failure %d: backoff %v, want ~%v", i, got, want)
		}
		if got < last {
			t.Fatalf("failure %d: backoff shrank from %v to %v", i, last, got)
		}
		last = got
	}
	// A plain connectivity failure must NOT climb to the 30-minute ceiling:
	// nothing polls a parked server, so there is nothing to discover recovery.
	if last != chatTransientBackoffMax {
		t.Fatalf("transient backoff settled at %v, want the %v ceiling", last, chatTransientBackoffMax)
	}
	if chatTransientBackoffMax >= chatBackoffMax {
		t.Fatal("a transient failure is treated as harshly as a verified negative")
	}
}

// A verified "this server has no messenger" already waits a long time; the
// escalation must not push it past the ceiling either.
func TestChatBackoffRespectsCeilingForVerifiedNegative(t *testing.T) {
	h := newChatTestServer(t).chat
	ps := &perServerChat{serverKey: "nochat.example.com"}
	for i := 0; i < 5; i++ {
		until := h.noteFailure(ps, client.ErrChatDisabled)
		if d := time.Until(until); d > chatBackoffMax+2*time.Second {
			t.Fatalf("backoff %v exceeds the %v ceiling", d, chatBackoffMax)
		}
	}
}

// One good round trip must wipe the penalty, or a server that recovers would
// stay half-ignored for half an hour.
func TestChatSuccessResetsBackoff(t *testing.T) {
	h := newChatTestServer(t).chat
	transient := errors.New("dns timeout")

	for _, tc := range []struct {
		name    string
		succeed func(*perServerChat)
	}{
		{"markReachable", h.markReachable},
		{"setBackoff(zero)", func(ps *perServerChat) { h.setBackoff(ps, time.Time{}) }},
	} {
		ps := &perServerChat{serverKey: "flaky.example.com"}
		for i := 0; i < 4; i++ {
			h.noteFailure(ps, transient)
		}
		tc.succeed(ps)

		h.mu.Lock()
		failures, backoff, reason := ps.failures, ps.backoffUntil, ps.lastReason
		h.mu.Unlock()
		if failures != 0 {
			t.Errorf("%s: failures = %d, want 0", tc.name, failures)
		}
		if !backoff.IsZero() {
			t.Errorf("%s: still backed off until %v", tc.name, backoff)
		}
		if reason != "" {
			t.Errorf("%s: stale reason %q", tc.name, reason)
		}
		// The next failure must start from the base wait again, not the old one.
		if d := time.Until(h.noteFailure(ps, transient)); d > chatServerBackoff(transient)+2*time.Second {
			t.Errorf("%s: backoff did not restart from the base: %v", tc.name, d)
		}
	}
}

// Manual retry is the user saying "try everything now".
func TestChatClearBackoffsResetsFailureCounts(t *testing.T) {
	h := newChatTestServer(t).chat
	a := &perServerChat{serverKey: "a.example.com"}
	b := &perServerChat{serverKey: "b.example.com"}
	h.servers = map[string]*perServerChat{a.serverKey: a, b.serverKey: b}
	for i := 0; i < 3; i++ {
		h.noteFailure(a, errors.New("x"))
		h.noteFailure(b, errors.New("x"))
	}
	h.clearBackoffs()

	h.mu.Lock()
	defer h.mu.Unlock()
	for _, ps := range []*perServerChat{a, b} {
		if ps.failures != 0 || !ps.backoffUntil.IsZero() {
			t.Fatalf("%s: failures=%d backoff=%v, want cleared", ps.serverKey, ps.failures, ps.backoffUntil)
		}
	}
}

// Only chatPollConcurrency servers poll at once, so order still decides who
// gets a slot first: a failing config must not take one from the live server.
func TestChatPollOrderPutsHealthyServersFirst(t *testing.T) {
	h := newChatTestServer(t).chat
	good := &perServerChat{serverKey: "good.example.com"}
	flaky := &perServerChat{serverKey: "flaky.example.com"}
	disabled := &perServerChat{serverKey: "off.example.com"}
	backedOff := &perServerChat{serverKey: "dead.example.com"}

	h.servers = map[string]*perServerChat{
		good.serverKey: good, flaky.serverKey: flaky,
		disabled.serverKey: disabled, backedOff.serverKey: backedOff,
	}
	h.enabled = map[string]bool{
		good.serverKey: true, flaky.serverKey: true, backedOff.serverKey: true,
	}
	// flaky failed before but its backoff has expired, so it is retryable --
	// and must still queue behind a server with a clean record.
	h.noteFailure(flaky, errors.New("x"))
	h.mu.Lock()
	flaky.backoffUntil = time.Time{}
	h.mu.Unlock()
	h.setBackoff(backedOff, time.Now().Add(time.Hour)) // still backed off: skipped

	order := h.pollOrder(time.Now())
	if len(order) != 2 {
		var keys []string
		for _, ps := range order {
			keys = append(keys, ps.serverKey)
		}
		t.Fatalf("polling %v, want only the two enabled and not-backed-off servers", keys)
	}
	if order[0] != good {
		t.Fatalf("first polled is %s, want the healthy %s", order[0].serverKey, good.serverKey)
	}
	if order[1] != flaky {
		t.Fatalf("second polled is %s, want the flaky %s", order[1].serverKey, flaky.serverKey)
	}
}

// A server the user never opted into must not be polled: polling opens a read
// session and publishes the identity.
func TestChatPollOrderSkipsDisabled(t *testing.T) {
	h := newChatTestServer(t).chat
	ps := &perServerChat{serverKey: "off.example.com"}
	h.servers = map[string]*perServerChat{ps.serverKey: ps}
	h.enabled = map[string]bool{}
	if got := h.pollOrder(time.Now()); len(got) != 0 {
		t.Fatalf("polled %d disabled servers, want 0", len(got))
	}
}

// Guards the poll constants against a careless future edit.
func TestChatPollBudgetsAreSane(t *testing.T) {
	if chatPollServerTimeout <= 0 {
		t.Fatal("background polls must be bounded")
	}
	// A poll is several DNS round trips, each allowed defaultTimeout; a tighter
	// budget would fail slow-but-working servers.
	if min := 3 * time.Duration(defaultTimeout) * time.Second; chatPollServerTimeout < min {
		t.Fatalf("poll budget %v is under %v (~3 query timeouts)", chatPollServerTimeout, min)
	}
	if chatPollConcurrency < 2 {
		t.Fatalf("concurrency %d serialises the cycle again", chatPollConcurrency)
	}
	if chatBackoffMax < chatPollInterval {
		t.Fatalf("backoff ceiling %v is below the idle poll interval %v, so a dead "+
			"server would be retried every cycle anyway", chatBackoffMax, chatPollInterval)
	}
}

func fanOutServers(n int) []*perServerChat {
	out := make([]*perServerChat, n)
	for i := range out {
		out[i] = &perServerChat{serverKey: fmt.Sprintf("s%d.example.com", i)}
	}
	return out
}

// Inboxes are independent: a slow server must only cost its own slot, never
// hold up the others. Deadlocks (and so fails) if the fan-out is sequential.
func TestChatPollFanOutRunsInParallel(t *testing.T) {
	h := newChatTestServer(t).chat
	gate := make(chan struct{})
	var inFlight atomic.Int32

	go func() {
		deadline := time.Now().Add(5 * time.Second)
		for time.Now().Before(deadline) {
			if inFlight.Load() >= int32(chatPollConcurrency) {
				close(gate)
				return
			}
			time.Sleep(time.Millisecond)
		}
		t.Error("never reached chatPollConcurrency polls at once: the fan-out is serial")
		close(gate)
	}()

	got := h.pollFanOut(context.Background(), 0, fanOutServers(chatPollConcurrency),
		func(context.Context, *perServerChat) (int, error) {
			inFlight.Add(1)
			defer inFlight.Add(-1)
			<-gate
			return 1, nil
		})
	if got != chatPollConcurrency {
		t.Fatalf("got %d new messages, want %d", got, chatPollConcurrency)
	}
}

// The cap is what keeps the shared resolver pool usable: each server has its
// own fetcher with its own rate limit, so in-flight polls multiply the rate.
func TestChatPollFanOutCapsConcurrency(t *testing.T) {
	h := newChatTestServer(t).chat
	var inFlight, peak atomic.Int32

	h.pollFanOut(context.Background(), 0, fanOutServers(4*chatPollConcurrency),
		func(context.Context, *perServerChat) (int, error) {
			n := inFlight.Add(1)
			defer inFlight.Add(-1)
			for {
				old := peak.Load()
				if n <= old || peak.CompareAndSwap(old, n) {
					break
				}
			}
			time.Sleep(2 * time.Millisecond)
			return 0, nil
		})

	if p := int(peak.Load()); p > chatPollConcurrency {
		t.Fatalf("%d polls in flight at once, cap is %d", p, chatPollConcurrency)
	}
}

// One failing server must be backed off without costing the healthy ones their
// results -- the whole point of polling them side by side.
func TestChatPollFanOutIsolatesFailures(t *testing.T) {
	h := newChatTestServer(t).chat
	order := fanOutServers(3)
	bad := order[1]

	got := h.pollFanOut(context.Background(), 0, order,
		func(_ context.Context, ps *perServerChat) (int, error) {
			if ps == bad {
				return 0, errors.New("dns timeout")
			}
			return 2, nil
		})
	if got != 4 {
		t.Fatalf("got %d new messages, want 4 from the two healthy servers", got)
	}

	h.mu.Lock()
	defer h.mu.Unlock()
	if bad.failures != 1 || bad.backoffUntil.IsZero() {
		t.Fatalf("failing server not backed off: failures=%d until=%v", bad.failures, bad.backoffUntil)
	}
	for _, ps := range []*perServerChat{order[0], order[2]} {
		if ps.failures != 0 || !ps.backoffUntil.IsZero() {
			t.Fatalf("%s penalised for another server's failure", ps.serverKey)
		}
	}
}

// Parallel polls each report their own done/total; the UI needs one number.
func TestChatPollProgressAggregates(t *testing.T) {
	var mu sync.Mutex
	var lastDone, lastTotal int
	agg := newChatPollProgress(func(done, total int) {
		mu.Lock()
		lastDone, lastTotal = done, total
		mu.Unlock()
	})

	var wg sync.WaitGroup
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			agg.forServer(fmt.Sprintf("s%d", i))(2, 5)
		}(i)
	}
	wg.Wait()

	// Final call sees every server's last report.
	agg.forServer("s0")(2, 5)
	mu.Lock()
	defer mu.Unlock()
	if lastDone != 6 || lastTotal != 15 {
		t.Fatalf("aggregate %d/%d, want 6/15", lastDone, lastTotal)
	}
}

// A nil progress callback must stay nil, not become a wrapper that panics.
func TestChatPollProgressNilStaysNil(t *testing.T) {
	if cb := newChatPollProgress(nil).forServer("s"); cb != nil {
		t.Fatal("wrapped a nil progress callback")
	}
}

// A server already being polled is SKIPPED, not queued, and the skip must not
// look like a failure: with a multi-minute budget, queueing would stack a
// goroutine per cycle, and blaming the server would back off a healthy one.
func TestChatPollBusyServerIsSkippedNotPenalised(t *testing.T) {
	h := newChatTestServer(t).chat
	ps := &perServerChat{serverKey: "busy.example.com"}

	ps.pollMu.Lock() // stand in for a poll already running
	n, err := h.pollServer(context.Background(), ps, nil, "me")
	ps.pollMu.Unlock()

	if !errors.Is(err, errChatPollBusy) {
		t.Fatalf("err = %v, want errChatPollBusy", err)
	}
	if n != 0 {
		t.Fatalf("got %d messages from a skipped poll", n)
	}

	// And the fan-out must not turn that skip into a backoff.
	h.pollFanOut(context.Background(), 0, []*perServerChat{ps},
		func(context.Context, *perServerChat) (int, error) { return 0, errChatPollBusy })
	h.mu.Lock()
	defer h.mu.Unlock()
	if ps.failures != 0 || !ps.backoffUntil.IsZero() {
		t.Fatalf("a skipped poll was counted as a failure: failures=%d", ps.failures)
	}
}

// Our own shutdown or the user navigating away says nothing about the server.
func TestChatPollCallerExitDoesNotPenalise(t *testing.T) {
	h := newChatTestServer(t).chat
	ps := &perServerChat{serverKey: "fine.example.com"}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	h.pollFanOut(ctx, 0, []*perServerChat{ps},
		func(context.Context, *perServerChat) (int, error) {
			return 0, context.Canceled
		})

	h.mu.Lock()
	defer h.mu.Unlock()
	if ps.failures != 0 || !ps.backoffUntil.IsZero() {
		t.Fatalf("caller's own exit blamed on the server: failures=%d", ps.failures)
	}
}

// budget 0 must inherit the caller's deadline: the manual refresh gives itself
// minutes precisely because draining a backlog takes that long, and FetchInbox
// throws away everything it decoded if a deadline fires.
func TestChatPollBudgetZeroInheritsCallerDeadline(t *testing.T) {
	h := newChatTestServer(t).chat
	ps := &perServerChat{serverKey: "slow.example.com"}

	want := time.Now().Add(7 * time.Minute)
	ctx, cancel := context.WithDeadline(context.Background(), want)
	defer cancel()

	var got time.Time
	var ok bool
	h.pollFanOut(ctx, 0, []*perServerChat{ps},
		func(pctx context.Context, _ *perServerChat) (int, error) {
			got, ok = pctx.Deadline()
			return 0, nil
		})
	if !ok {
		t.Fatal("no deadline passed through")
	}
	if d := got.Sub(want); d > time.Second || d < -time.Second {
		t.Fatalf("poll deadline %v, want the caller's %v", got, want)
	}
}

// ...and a non-zero budget still caps the background loop.
func TestChatPollBudgetCapsBackgroundLoop(t *testing.T) {
	h := newChatTestServer(t).chat
	ps := &perServerChat{serverKey: "slow.example.com"}

	var got time.Time
	h.pollFanOut(context.Background(), 2*time.Second, []*perServerChat{ps},
		func(pctx context.Context, _ *perServerChat) (int, error) {
			got, _ = pctx.Deadline()
			return 0, nil
		})
	if d := time.Until(got); d > 3*time.Second || d < time.Second {
		t.Fatalf("poll deadline in %v, want ~2s", d)
	}
}

// The background budget must be able to drain a real inbox, not just a
// message or two: FetchInbox discards everything on a deadline, so too tight a
// budget means a backlog never drains at all.
func TestChatPollBudgetCanDrainAnInbox(t *testing.T) {
	// ~2 queries per message plus handshake, each allowed defaultTimeout, and
	// on a degraded network a query is seconds. Minutes, not seconds.
	if chatPollServerTimeout < 2*time.Minute {
		t.Fatalf("background poll budget %v cannot drain a backlogged inbox",
			chatPollServerTimeout)
	}
}
