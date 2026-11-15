package testcluster

import (
	"context"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/kvclient"
)

// wakeBound is how soon after its holder finishes a transaction blocked on
// the holder's intent must proceed, as the median of wakeTrials trials.
// Polling the holder's record every 100 ms, a waiter proceeded 50 ms
// after the holder finished on average and up to 100 ms after; woken by
// the holder's coordinator it proceeds after one push and its own write.
const (
	wakeBound  = 30 * time.Millisecond
	wakeTrials = 5
)

// TestWaiterWokenWhenHolderFinishes: a transaction waiting on a live
// intent proceeds within milliseconds of the intent's transaction
// committing or aborting, not at its next poll of the holder's record —
// even after waiting long enough for the poll's backoff to reach its
// longest interval.
func TestWaiterWokenWhenHolderFinishes(t *testing.T) {
	tc := Start(t, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	db := tc.Nodes[0].DB()

	// trial makes a holder lay down an intent on k, starts a waiter
	// writing k (lower priority, so it waits rather than abort the
	// holder), lets it wait, finishes the holder with finish, and returns
	// how long after finish returned the waiter's write did.
	trial := func(t *testing.T, k keys.Key, pipelined bool, finish func(*kvclient.Txn, keys.Key) error) time.Duration {
		t.Helper()
		holder := db.NewTxn("holder")
		holder.TestingSetPriority(1000)
		if pipelined {
			holder.EnablePipelining()
		}
		if err := holder.Put(ctx, k, []byte("held")); err != nil {
			t.Fatal(err)
		}
		waiter := db.NewTxn("waiter")
		waiter.TestingSetPriority(1)
		done := make(chan error, 1)
		var proceeded time.Time
		go func() {
			err := waiter.Put(ctx, k, []byte("mine"))
			proceeded = time.Now()
			done <- err
		}()
		// Long enough for the waiter's backoff to reach its cap.
		time.Sleep(250 * time.Millisecond)
		select {
		case err := <-done:
			t.Fatalf("waiter did not wait for the holder: %v", err)
		default:
		}
		if err := finish(holder, k); err != nil {
			t.Fatalf("holder: %v", err)
		}
		finished := time.Now()
		if err := <-done; err != nil {
			t.Fatalf("waiter: %v", err)
		}
		if err := waiter.Commit(ctx); err != nil {
			t.Fatalf("waiter's commit: %v", err)
		}
		return proceeded.Sub(finished)
	}
	run := func(t *testing.T, name string, pipelined bool, finish func(*kvclient.Txn, keys.Key) error) {
		var took []time.Duration
		for i := range wakeTrials {
			took = append(took, trial(t, deadlockKey(434, fmt.Sprintf("%s%d", name, i)), pipelined, finish))
		}
		slices.Sort(took)
		t.Logf("waiter proceeded after the holder finished in %v", took)
		if med := took[len(took)/2]; med > wakeBound {
			t.Fatalf("median wait after the holder finished %s, want under %s", med, wakeBound)
		}
	}

	t.Run("commit", func(t *testing.T) {
		run(t, "c", false, func(h *kvclient.Txn, _ keys.Key) error { return h.Commit(ctx) })
	})
	t.Run("rollback", func(t *testing.T) {
		run(t, "r", false, func(h *kvclient.Txn, _ keys.Key) error { return h.Rollback(ctx) })
	})
	t.Run("parallel commit", func(t *testing.T) {
		// A deferred write makes the commit a parallel one: the client
		// returns once the commit is implied, and the record is
		// finalized and the intents resolved after.
		run(t, "p", true, func(h *kvclient.Txn, k keys.Key) error {
			var b kvclient.WriteBatch
			b.Put(append(k.Clone(), "-other"...), []byte("x"))
			if err := h.RunBatch(ctx, &b); err != nil {
				return err
			}
			return h.Commit(ctx)
		})
	})
}
