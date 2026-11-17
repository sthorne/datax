package rpc

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/sthorne/datax/pkg/base"
	"github.com/sthorne/datax/pkg/util/hlc"
	"github.com/sthorne/datax/pkg/util/stop"
)

// unusedAddr is a loopback address nothing listens on.
func unusedAddr(t *testing.T) string {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := lis.Addr().String()
	_ = lis.Close()
	return addr
}

// TestTransportLocalityAddress: a peer with a locality address is dialed
// there while it answers, and at its public address once it does not —
// avoided for localityRetryAfter, then re-probed in the background and
// taken back when it answers again.
func TestTransportLocalityAddress(t *testing.T) {
	defer func(d time.Duration) { localityRetryAfter = d }(localityRetryAfter)
	localityRetryAfter = 50 * time.Millisecond

	clock := hlc.NewClock(nil, 0)
	public, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	gs := NewServer(clock, ServerHandlers{}, nil)
	go func() { _ = gs.Serve(public) }()
	defer gs.Stop()
	private, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	gs2 := NewServer(clock, ServerHandlers{}, nil)
	go func() { _ = gs2.Serve(private) }()
	defer gs2.Stop()

	stopper := stop.NewStopper()
	defer stopper.Stop()
	locAddr := map[base.NodeID]string{2: private.Addr().String(), 3: unusedAddr(t)}
	tr := NewTransport(clock, stopper, func(base.NodeID) (string, error) { return public.Addr().String(), nil })
	defer tr.Close()
	tr.SetLocalityResolver(func(id base.NodeID) string { return locAddr[id] })

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// n2: the locality address answers and carries the traffic.
	if _, _, err := tr.Ping(ctx, 2); err != nil {
		t.Fatal(err)
	}
	if got := tr.ConnAddr(2); got != private.Addr().String() {
		t.Fatalf("n2 dialed at %q, want its locality address %s", got, private.Addr())
	}
	// n4 has none: its public address.
	if _, _, err := tr.Ping(ctx, 4); err != nil {
		t.Fatal(err)
	}
	if got := tr.ConnAddr(4); got != public.Addr().String() {
		t.Fatalf("n4 dialed at %q, want its public address", got)
	}
	if d, f := tr.LocalityStats(); d != 1 || f != 0 {
		t.Fatalf("stats after n2, n4: dials %d fallbacks %d, want 1, 0", d, f)
	}

	// n3: the locality address does not answer; the public one works,
	// and the failure is remembered rather than probed on every call.
	for i := 0; i < 3; i++ {
		if _, _, err := tr.Ping(ctx, 3); err != nil {
			t.Fatalf("n3 over its public address: %v", err)
		}
	}
	if got := tr.ConnAddr(3); got != public.Addr().String() {
		t.Fatalf("n3 dialed at %q, want the public fallback", got)
	}
	if _, f := tr.LocalityStats(); f != 1 {
		t.Fatalf("fallbacks %d, want 1 (the failure is remembered)", f)
	}

	// The locality address comes up: after localityRetryAfter a
	// background probe finds it, and the next dial moves to it.
	lis, err := net.Listen("tcp", locAddr[3])
	if err != nil {
		t.Logf("cannot re-listen on %s (%v); skipping the recovery half", locAddr[3], err)
		return
	}
	gs3 := NewServer(clock, ServerHandlers{}, nil)
	go func() { _ = gs3.Serve(lis) }()
	defer gs3.Stop()
	deadline := time.Now().Add(5 * time.Second)
	for tr.ConnAddr(3) != locAddr[3] {
		if time.Now().After(deadline) {
			t.Fatalf("n3 still dialed at %q after its locality address came up", tr.ConnAddr(3))
		}
		if _, _, err := tr.Ping(ctx, 3); err != nil {
			t.Fatal(err)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// TestPickAddrWithoutLocality: no locality address, or one equal to the
// public address, is the public address with no probe.
func TestPickAddrWithoutLocality(t *testing.T) {
	tr := NewTransport(hlc.NewClock(nil, 0), nil, nil)
	if got := tr.PickAddr(1, "203.0.113.1:26257", ""); got != "203.0.113.1:26257" {
		t.Fatal(got)
	}
	if got := tr.PickAddr(1, "203.0.113.1:26257", "203.0.113.1:26257"); got != "203.0.113.1:26257" {
		t.Fatal(got)
	}
	if d, f := tr.LocalityStats(); d != 0 || f != 0 {
		t.Fatalf("dials %d fallbacks %d", d, f)
	}
}
