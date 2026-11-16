package testcluster

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sthorne/datax/pkg/base"
	"github.com/sthorne/datax/pkg/cluster"
	"github.com/sthorne/datax/pkg/rpc"
	"github.com/sthorne/datax/pkg/server"
	"github.com/sthorne/datax/pkg/util/hlc"
)

// TestNodeStopDrainsRPCHandlers (issue #324): an RPC handler that is
// inside the node when it stops — here an admin call held by a test
// hook, standing for one in the middle of an engine read — finishes
// before the node closes its engines. Before the fix the gRPC server's
// Stop, run as a closer, did not wait for running handlers, and the
// engine closer ran under them: a data race on pebble's DB.Close and
// then a nil-pointer SIGSEGV that took the test binary down.
//
// The test is deterministic either way. The engine-close hook records a
// handler still running at that moment (the bug), and then lets it
// finish before the engines close, so the broken build fails the test
// rather than crashing. With the fix Stop is held up by the handler as
// a running task; the stopper's drain hook says so, and only then is
// the handler let go.
func TestNodeStopDrainsRPCHandlers(t *testing.T) {
	var (
		armed       atomic.Bool
		entered     = make(chan struct{})
		release     = make(chan struct{})
		releaseOnce sync.Once
		handlerDone = make(chan struct{})
		closedUnder atomic.Bool // the engines began closing under the handler
	)
	letGo := func() { releaseOnce.Do(func() { close(release) }) }
	n := startDiskNode(t, t.TempDir(), true, "", func(cfg *server.Config) {
		cfg.TestingAdminHook = func(ctx context.Context, op string) func() {
			if op != "node-status" || !armed.CompareAndSwap(true, false) {
				return nil
			}
			close(entered)
			<-release
			return func() { close(handlerDone) }
		}
		cfg.TestingBeforeEngineClose = func() {
			select {
			case <-entered:
			default:
				return // the handler never started
			}
			select {
			case <-handlerDone:
			default:
				closedUnder.Store(true)
				letGo()
				<-handlerDone
			}
		}
	})
	stopped := make(chan struct{})
	t.Cleanup(func() {
		letGo()
		select {
		case <-stopped:
		default:
			n.Stop()
		}
	})
	n.Stopper().SetTestingDrainHook(func(int) { letGo() })

	armed.Store(true)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	trans := rpc.NewTransport(hlc.NewClock(nil, base.DefaultMaxClockOffset), nil, nil)
	defer trans.Close()
	go func() {
		var resp cluster.AdminResponse
		_ = trans.Call(ctx, n.Addr(), "admin", cluster.AdminRequest{Op: "node-status"}, &resp)
	}()
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal("the admin call never reached the handler")
	}

	go func() {
		n.Stop()
		close(stopped)
	}()
	select {
	case <-stopped:
	case <-time.After(60 * time.Second):
		t.Fatal("Stop did not return")
	}
	select {
	case <-handlerDone:
	default:
		t.Fatal("Stop returned while the admin handler was still running")
	}
	if closedUnder.Load() {
		t.Fatal("the node began closing its engines while an admin handler was still running (issue #324)")
	}
}
