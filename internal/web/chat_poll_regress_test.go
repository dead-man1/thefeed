package web

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/sartoopjj/thefeed/internal/client"
)

// Reproduces the "messages never arrive" regression a too-tight poll budget
// causes. FetchInbox is all-or-nothing on a deadline (internal/client/chat.go:
// "if ctx.Err() != nil { return nil, ctx.Err() }"), so a drain that cannot
// finish inside the budget delivers NOTHING -- and every retry hits the same
// wall, so the inbox never drains at all.
func TestChatPollTooTightBudgetDeliversNothing(t *testing.T) {
	h := newChatTestServer(t).chat

	// Stand-in for FetchInbox draining a backlog: all 12 messages or none.
	const drain = 400 * time.Millisecond
	const backlog = 12
	poll := func(ctx context.Context, _ *perServerChat) (int, error) {
		select {
		case <-time.After(drain):
			return backlog, nil
		case <-ctx.Done():
			return 0, ctx.Err() // everything already decoded is thrown away
		}
	}

	// A budget shorter than the drain: nothing is ever delivered, however
	// many cycles run.
	starved := &perServerChat{serverKey: "starved.example.com"}
	for cycle := 0; cycle < 3; cycle++ {
		if got := h.pollFanOut(context.Background(), drain/4, []*perServerChat{starved}, poll); got != 0 {
			t.Fatalf("cycle %d: got %d, expected the all-or-nothing discard", cycle, got)
		}
	}
	h.mu.Lock()
	starvedFails := starved.failures
	h.mu.Unlock()
	if starvedFails != 3 {
		t.Fatalf("failures = %d, want 3 (each cycle hit the same wall)", starvedFails)
	}

	// The same server with a budget that fits drains on the first try.
	ok := &perServerChat{serverKey: "ok.example.com"}
	if got := h.pollFanOut(context.Background(), 10*time.Second, []*perServerChat{ok}, poll); got != backlog {
		t.Fatalf("got %d messages, want the whole %d-message backlog", got, backlog)
	}

	// And the real background budget must be on the right side of this.
	if chatPollServerTimeout <= drain {
		t.Fatalf("background budget %v is in the starving regime", chatPollServerTimeout)
	}
}

// Reproduces the second regression: a short loss of connectivity parking a
// HEALTHY server for half an hour. At the active cadence (20s) a handful of
// cycles is under two minutes of bad network -- that must not buy a backoff
// long enough to look like the messenger is dead, because a parked server is
// not polled, so nothing discovers the recovery.
func TestChatShortOutageDoesNotParkServerForHalfAnHour(t *testing.T) {
	h := newChatTestServer(t).chat
	ps := &perServerChat{serverKey: "flaky.example.com"}
	transient := errors.New("dns timeout")

	// Six failed cycles at the active cadence ~= 2 minutes of bad signal.
	cycles := 6
	var last time.Duration
	for i := 0; i < cycles; i++ {
		last = time.Until(h.noteFailure(ps, transient))
	}
	outage := time.Duration(cycles) * chatPollActiveInterval

	if last > chatTransientBackoffMax+2*time.Second {
		t.Fatalf("%v of bad network parked the server for %v", outage, last)
	}
	// The consequence that matters: silence must not vastly outlast the outage.
	if last > 4*outage {
		t.Fatalf("%v outage bought %v of silence", outage, last)
	}
	// A verified "no messenger here" is the only thing allowed the long wait.
	verified := &perServerChat{serverKey: "nochat.example.com"}
	if d := time.Until(h.noteFailure(verified, client.ErrChatDisabled)); d < chatTransientBackoffMax {
		t.Fatalf("verified negative only waits %v", d)
	}
}

// Closes the gap in the test above: it showed the wrong backoff VALUE, this
// shows the consequence. A parked server is not in pollOrder, so nothing
// polls it and nothing discovers that the network came back. pollOrder takes
// `now`, so we can walk the clock forward through the real code.
func TestChatParkedServerIsNotPolledUntilBackoffEnds(t *testing.T) {
	h := newChatTestServer(t).chat
	ps := &perServerChat{serverKey: "flaky.example.com"}
	h.servers = map[string]*perServerChat{ps.serverKey: ps}
	h.enabled = map[string]bool{ps.serverKey: true}

	start := time.Now()
	// ~2 minutes of bad network at the active cadence.
	cycles := 6
	for i := 0; i < cycles; i++ {
		h.noteFailure(ps, errors.New("dns timeout"))
	}
	outage := time.Duration(cycles) * chatPollActiveInterval

	inOrder := func(at time.Time) bool {
		for _, p := range h.pollOrder(at) {
			if p == ps {
				return true
			}
		}
		return false
	}

	if inOrder(start) {
		t.Fatal("a server that just failed should be backed off")
	}
	// The network is back, but nothing polls until the backoff lapses. That
	// silence must be comparable to the outage, not an order of magnitude more.
	if inOrder(start.Add(chatTransientBackoffMax - time.Second)) {
		t.Fatal("backoff ended earlier than configured")
	}
	if !inOrder(start.Add(chatTransientBackoffMax + time.Second)) {
		t.Fatalf("after %v of bad network the server is still unpolled past %v: "+
			"nothing can discover the recovery", outage, chatTransientBackoffMax)
	}
	// The regression this guards: with a flat 30-minute ceiling, a two-minute
	// outage left the user in silence for half an hour.
	if !inOrder(start.Add(10 * time.Minute)) {
		t.Fatal("still parked 10 minutes after a 2-minute outage")
	}
}
