package notify

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

// recorder is a listener's far end: what it was sent and whether it was
// ended.
type recorder struct {
	l     *Listener
	mu    sync.Mutex
	ended []Reason
}

func newRecorder(h *Hub) *recorder {
	r := &recorder{}
	r.l = h.NewListener(func() {}, func(reason Reason) {
		r.mu.Lock()
		r.ended = append(r.ended, reason)
		r.mu.Unlock()
	})
	return r
}

func (r *recorder) endedWith() []Reason {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]Reason(nil), r.ended...)
}

func payloads(ns []Notification) []string {
	out := make([]string, len(ns))
	for i, n := range ns {
		out[i] = n.Payload
	}
	return out
}

func note(payload string) Notification {
	return Notification{Database: "d", Channel: "c", Payload: payload}
}

// TestLocalDelivery: a notification reaches the listeners of its
// channel in its database and nobody else, in publish order.
func TestLocalDelivery(t *testing.T) {
	h := NewHub(context.Background(), Config{})
	a, b, other, otherDB := newRecorder(h), newRecorder(h), newRecorder(h), newRecorder(h)
	a.l.Listen("d", "c")
	a.l.Listen("d", "c") // twice is once
	b.l.Listen("d", "c")
	other.l.Listen("d", "x")
	otherDB.l.Listen("e", "c")

	h.Publish([]Notification{note("1"), note("2")})
	h.Publish([]Notification{note("3")})
	for _, r := range []*recorder{a, b} {
		if got := fmt.Sprint(payloads(r.l.Take())); got != "[1 2 3]" {
			t.Fatalf("listener got %s, want [1 2 3]", got)
		}
	}
	if other.l.Pending() || otherDB.l.Pending() {
		t.Fatal("a listener of another channel or database was notified")
	}

	b.l.Unlisten("d", "c")
	a.l.UnlistenAll()
	h.Publish([]Notification{note("4")})
	if a.l.Pending() || b.l.Pending() {
		t.Fatal("notified after UNLISTEN")
	}
	if h.Listening() != 2 {
		t.Fatalf("Listening() = %d, want 2", h.Listening())
	}
	other.l.Close()
	otherDB.l.Close()
	if h.Listening() != 0 {
		t.Fatalf("Listening() = %d after Close, want 0", h.Listening())
	}
}

// TestSessionQueueBound: a listener that does not take its
// notifications is ended when its queue fills, and the publisher is
// neither blocked nor failed.
func TestSessionQueueBound(t *testing.T) {
	h := NewHub(context.Background(), Config{SessionQueueBytes: 1000})
	slow, quick := newRecorder(h), newRecorder(h)
	slow.l.Listen("d", "c")
	quick.l.Listen("d", "c")
	for i := 0; i < 100; i++ {
		h.Publish([]Notification{note(fmt.Sprint(i))})
		quick.l.Take()
	}
	if got := slow.endedWith(); len(got) != 1 || got[0] != ReasonQueueFull {
		t.Fatalf("slow listener ended with %v, want one ReasonQueueFull", got)
	}
	if got := quick.endedWith(); len(got) != 0 {
		t.Fatalf("the listener that kept up was ended: %v", got)
	}
}

// link joins hubs the way the admin RPC does, with a switch to cut it.
type link struct {
	mu    sync.Mutex
	hubs  map[int32]*Hub
	down  map[int32]bool
	calls int
	// dupe delivers every batch twice, as a call retried after its
	// answer was lost would.
	dupe bool
}

func (k *link) send(_ context.Context, to int32, b *Batch) error {
	k.mu.Lock()
	h, down, dupe := k.hubs[to], k.down[to], k.dupe
	k.calls++
	k.mu.Unlock()
	if down || h == nil {
		return errors.New("unreachable")
	}
	h.Deliver(b)
	if dupe {
		h.Deliver(b)
	}
	return nil
}

func (k *link) setDown(id int32, down bool) {
	k.mu.Lock()
	k.down[id] = down
	k.mu.Unlock()
}

func twoHubs(t *testing.T, peerBytes int) (*link, *Hub, *Hub) {
	return twoHubsRetaining(t, peerBytes, 0)
}

func twoHubsRetaining(t *testing.T, peerBytes int, retention time.Duration) (*link, *Hub, *Hub) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	k := &link{hubs: map[int32]*Hub{}, down: map[int32]bool{}}
	mk := func(id, peer int32) *Hub {
		return NewHub(ctx, Config{NodeID: id, Peers: func() []int32 { return []int32{peer} }, Send: k.send,
			PeerQueueBytes: peerBytes, PeerRetention: retention})
	}
	a, b := mk(1, 2), mk(2, 1)
	k.hubs[1], k.hubs[2] = a, b
	return k, a, b
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(time.Millisecond)
	}
}

// TestPeerDeliveryOnce: a notification published on one node reaches a
// listener on another once, in order, even when every call is delivered
// twice.
func TestPeerDeliveryOnce(t *testing.T) {
	k, a, b := twoHubs(t, 0)
	k.dupe = true
	r := newRecorder(b)
	r.l.Listen("d", "c")
	var want []string
	for i := 0; i < 2000; i++ {
		a.Publish([]Notification{note(fmt.Sprint(i))})
		want = append(want, fmt.Sprint(i))
	}
	var got []string
	waitFor(t, "the notifications to cross", func() bool {
		got = append(got, payloads(r.l.Take())...)
		return len(got) >= len(want)
	})
	a.Flush(context.Background())
	got = append(got, payloads(r.l.Take())...)
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("got %d notifications, want %d in order; first few %v", len(got), len(want), got[:min(len(got), 10)])
	}
	if len(r.endedWith()) != 0 {
		t.Fatalf("listener ended: %v", r.endedWith())
	}
}

// TestPeerUnreachableThenBack: what a node could not hand a peer is
// kept, in order, and delivered when the peer is reachable again.
func TestPeerUnreachableThenBack(t *testing.T) {
	k, a, b := twoHubs(t, 0)
	r := newRecorder(b)
	r.l.Listen("d", "c")
	k.setDown(2, true)
	for i := 0; i < 50; i++ {
		a.Publish([]Notification{note(fmt.Sprint(i))})
	}
	time.Sleep(3 * minBackoff)
	if r.l.Pending() {
		t.Fatal("delivered through a cut link")
	}
	k.setDown(2, false)
	var got []string
	waitFor(t, "the queued notifications", func() bool {
		got = append(got, payloads(r.l.Take())...)
		return len(got) >= 50
	})
	for i, p := range got {
		if p != fmt.Sprint(i) {
			t.Fatalf("notification %d is %q: out of order", i, p)
		}
	}
	if len(r.endedWith()) != 0 {
		t.Fatalf("listener ended although nothing was lost: %v", r.endedWith())
	}
}

// TestPeerQueueOverflow: a node that had to drop notifications for an
// unreachable peer says so when it reaches it again, and that peer's
// listeners are ended rather than left having missed them. A listener
// that arrives afterwards is untouched, and the lost marker is not
// applied twice.
func TestPeerQueueOverflow(t *testing.T) {
	k, a, b := twoHubs(t, 2000)
	k.dupe = true
	r, silent := newRecorder(b), newRecorder(b)
	r.l.Listen("d", "c")
	k.setDown(2, true)
	for i := 0; i < 500; i++ {
		a.Publish([]Notification{note(fmt.Sprint(i))})
	}
	// Once the queue has been dropped and the peer is known to be out
	// of reach, nothing more is kept for it.
	waitFor(t, "a failed call", func() bool {
		a.mu.Lock()
		defer a.mu.Unlock()
		return a.peers[2].failing
	})
	for i := 0; i < 500; i++ {
		a.Publish([]Notification{note("unreachable")})
	}
	a.mu.Lock()
	held := len(a.peers[2].queue)
	a.mu.Unlock()
	if held != 0 {
		t.Fatalf("%d notifications held for a peer that has already lost some and cannot be reached", held)
	}
	k.setDown(2, false)
	waitFor(t, "the listener to be ended", func() bool { return len(r.endedWith()) > 0 })
	if got := r.endedWith(); len(got) != 1 || got[0] != ReasonPeerLost {
		t.Fatalf("ended with %v, want one ReasonPeerLost", got)
	}
	a.Flush(context.Background())
	// Nothing from after the gap was queued for the session that is
	// being ended, and a session that listens to nothing has missed
	// nothing.
	for _, p := range payloads(r.l.Take()) {
		if p == "unreachable" {
			t.Fatal("a listener that missed notifications was sent a later one")
		}
	}
	if got := silent.endedWith(); len(got) != 0 {
		t.Fatalf("a session that never listened was ended: %v", got)
	}

	late := newRecorder(b)
	late.l.Listen("d", "c")
	a.Publish([]Notification{note("after")})
	waitFor(t, "a notification after the loss", late.l.Pending)
	if got := fmt.Sprint(payloads(late.l.Take())); got != "[after]" {
		t.Fatalf("late listener got %s", got)
	}
	if len(late.endedWith()) != 0 {
		t.Fatalf("a listener that missed nothing was ended: %v", late.endedWith())
	}
}

// TestPeerUnsupported: a peer that does not know the call (an older
// release) is not retried and holds nothing up.
func TestPeerUnsupported(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var mu sync.Mutex
	calls := 0
	h := NewHub(ctx, Config{NodeID: 1, Peers: func() []int32 { return []int32{2} },
		Send: func(context.Context, int32, *Batch) error {
			mu.Lock()
			calls++
			mu.Unlock()
			return ErrUnsupported
		}})
	r := newRecorder(h)
	r.l.Listen("d", "c")
	h.Publish([]Notification{note("1")})
	h.Flush(ctx)
	waitFor(t, "the call", func() bool { mu.Lock(); defer mu.Unlock(); return calls > 0 })
	h.Flush(ctx)
	time.Sleep(3 * minBackoff)
	mu.Lock()
	defer mu.Unlock()
	if calls != 1 {
		t.Fatalf("an unsupported peer was called %d times, want 1", calls)
	}
	if got := fmt.Sprint(payloads(r.l.Take())); got != "[1]" {
		t.Fatalf("local listener got %s", got)
	}
}

// TestSenderRestart: sequence numbers start over with the sender's
// incarnation, and a batch from the old one arriving late is ignored.
func TestSenderRestart(t *testing.T) {
	h := NewHub(context.Background(), Config{NodeID: 2})
	r := newRecorder(h)
	r.l.Listen("d", "c")
	h.Deliver(&Batch{From: 1, Epoch: 10, Seq: 1, Notifications: []Notification{note("a"), note("b")}})
	h.Deliver(&Batch{From: 1, Epoch: 20, Seq: 1, Notifications: []Notification{note("c")}})
	h.Deliver(&Batch{From: 1, Epoch: 10, Seq: 3, Notifications: []Notification{note("stale")}})
	h.Deliver(&Batch{From: 1, Epoch: 20, Seq: 1, Notifications: []Notification{note("c"), note("d")}})
	if got := fmt.Sprint(payloads(r.l.Take())); got != "[a b c d]" {
		t.Fatalf("got %s, want [a b c d]", got)
	}
}

// TestPeerRetention: what a node owes a peer it cannot reach is given
// up after the retention period, and the peer's listeners are ended
// when it is back — a notification that late is replaced by the
// reconnect.
func TestPeerRetention(t *testing.T) {
	k, a, b := twoHubsRetaining(t, 0, 2*minBackoff)
	r := newRecorder(b)
	r.l.Listen("d", "c")
	k.setDown(2, true)
	a.Publish([]Notification{note("late")})
	waitFor(t, "the queue to be given up", func() bool {
		a.mu.Lock()
		defer a.mu.Unlock()
		return a.peers[2].lost && len(a.peers[2].queue) == 0
	})
	k.setDown(2, false)
	waitFor(t, "the listener to be ended", func() bool { return len(r.endedWith()) > 0 })
	if got := r.endedWith(); len(got) != 1 || got[0] != ReasonPeerLost {
		t.Fatalf("ended with %v, want one ReasonPeerLost", got)
	}
	if r.l.Pending() {
		t.Fatal("the notification given up on was delivered after all")
	}
}
