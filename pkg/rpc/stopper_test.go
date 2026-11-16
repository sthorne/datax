package rpc

import (
	"context"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/sthorne/datax/pkg/util/hlc"
	"github.com/sthorne/datax/pkg/util/stop"
)

// TestHandlersRunAsStopperTasks (issue #324): with a stopper in the
// handlers, a call running when it stops has its context canceled and
// holds the stopper's closers (the node's engine closer among them)
// until it returns; a call arriving after that is answered Unavailable
// without reaching the handler.
func TestHandlersRunAsStopperTasks(t *testing.T) {
	clock := hlc.NewClock(nil, 0)
	nodeStopper := stop.NewStopper()
	var closed atomic.Bool
	nodeStopper.AddCloser(func() { closed.Store(true) })
	var calls atomic.Int32
	entered := make(chan struct{})
	var closedUnder atomic.Bool
	handlers := ServerHandlers{
		Stopper: nodeStopper,
		Admin: func(ctx context.Context, data []byte) ([]byte, error) {
			if calls.Add(1) == 1 {
				close(entered)
				<-ctx.Done() // the stop began
				time.Sleep(20 * time.Millisecond)
				closedUnder.Store(closed.Load())
			}
			return []byte(`{}`), nil
		},
	}
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	gs := NewServer(clock, handlers, nil)
	go func() { _ = gs.Serve(lis) }()
	defer gs.Stop()

	clientStopper := stop.NewStopper()
	defer clientStopper.Stop()
	tr := NewTransport(clock, clientStopper, nil)
	defer tr.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	first := make(chan error, 1)
	go func() {
		var resp map[string]any
		first <- tr.Call(ctx, lis.Addr().String(), "admin", map[string]any{}, &resp)
	}()
	select {
	case <-entered:
	case err := <-first:
		t.Fatalf("the first call returned before reaching the handler: %v", err)
	}
	nodeStopper.Stop()
	if closedUnder.Load() {
		t.Fatal("the stopper ran its closers while a handler was still running")
	}
	if err := <-first; err != nil {
		t.Fatalf("the call running at the stop: %v", err)
	}

	var resp map[string]any
	err = tr.Call(ctx, lis.Addr().String(), "admin", map[string]any{}, &resp)
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("a call after the stop: %v, want Unavailable", err)
	}
	if n := calls.Load(); n != 1 {
		t.Fatalf("the handler ran %d times, want once (the call after the stop must not reach it)", n)
	}
}
