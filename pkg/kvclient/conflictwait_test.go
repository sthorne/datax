package kvclient

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/sthorne/datax/pkg/storage"
	"github.com/sthorne/datax/pkg/storage/enginepb"
)

func intentOf(id uuid.UUID) storage.Intent {
	return storage.Intent{Txn: enginepb.TxnMeta{ID: id}}
}

// TestConflictBackoff: waits start at a few milliseconds, double to the
// cap, and are drawn from the upper half of each.
func TestConflictBackoff(t *testing.T) {
	for n := range 64 {
		want := min(conflictPollMin<<min(n, 8), conflictPollMax)
		for range 20 {
			if d := conflictBackoff(n); d < want/2 || d > want {
				t.Fatalf("wait %d: %s, want in [%s, %s]", n, d, want/2, want)
			}
		}
	}
}

// TestTxnWaits: a finish wakes every watcher of that transaction, at once
// and even when it lands before the wait begins; watches of other
// transactions wait out their delay; and nothing is left behind.
func TestTxnWaits(t *testing.T) {
	ctx := context.Background()
	var w txnWaits
	a, b := uuid.New(), uuid.New()

	// Finished between the watch and the wait (the push saw it live).
	cw := w.watch([]storage.Intent{intentOf(a), intentOf(b), intentOf(a)})
	if len(cw.ids) != 2 {
		t.Fatalf("watch of a, b, a registered %d transactions, want 2", len(cw.ids))
	}
	w.finished(a)
	start := time.Now()
	if err := cw.wait(ctx, a, time.Minute); err != nil {
		t.Fatal(err)
	}
	if took := time.Since(start); took > time.Second {
		t.Fatalf("wait on a finished transaction took %s", took)
	}

	// Two watchers of one transaction, woken together; a watcher of
	// another waits out its delay.
	c := uuid.New()
	woke := make(chan time.Duration, 2)
	for range 2 {
		cw := w.watch([]storage.Intent{intentOf(c)})
		go func() {
			start := time.Now()
			_ = cw.wait(ctx, c, time.Minute)
			woke <- time.Since(start)
		}()
	}
	other := w.watch([]storage.Intent{intentOf(b)})
	time.Sleep(10 * time.Millisecond)
	w.finished(c)
	for range 2 {
		if took := <-woke; took > 10*time.Second {
			t.Fatalf("watcher woken after %s", took)
		}
	}
	start = time.Now()
	if err := other.wait(ctx, b, 20*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	if took := time.Since(start); took < 20*time.Millisecond {
		t.Fatalf("watcher of an unfinished transaction woke after %s", took)
	}

	// A cancelled context ends the wait.
	cctx, cancel := context.WithCancel(ctx)
	cancel()
	if err := w.watch([]storage.Intent{intentOf(b)}).wait(cctx, b, time.Minute); err == nil {
		t.Fatal("wait under a cancelled context returned nil")
	}

	// Released watches and finishes leave no entries.
	w.watch([]storage.Intent{intentOf(a)}).release()
	w.finished(uuid.New())
	if n := len(w.m); n != 0 {
		t.Fatalf("%d entries left behind", n)
	}
}
