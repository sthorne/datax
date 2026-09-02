package testcluster

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/kvclient"
)

// deadlockKey builds a key under the given test table prefix.
func deadlockKey(table uint64, k string) keys.Key {
	return append(keys.TableDataPrefix(table).Clone(), k...)
}

// runDeadlockCycle drives n transactions into a perfect cycle: txn i locks
// key i, then (once all locks are held) each txn i writes key (i+1) mod n.
// Returns per-txn outcome errors and the wall time the contended phase took.
func runDeadlockCycle(t *testing.T, tc *TestCluster, table uint64, n int) ([]error, time.Duration) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	db := tc.Nodes[0].DB()
	for i := 0; i < n; i++ {
		if err := db.Put(ctx, deadlockKey(table, fmt.Sprintf("k%d", i)), []byte("v")); err != nil {
			t.Fatal(err)
		}
	}

	txns := make([]*kvclient.Txn, n)
	for i := range txns {
		txns[i] = db.NewTxn(fmt.Sprintf("cycle-%d", i))
		// Equal priorities: priority-based aborts need a strictly greater
		// pusher, so they can never fire — cycle DETECTION must resolve
		// the deadlock, and its deterministic victim choice (ID tie-break)
		// yields exactly one abort.
		txns[i].TestingSetPriority(7)
		if _, err := txns[i].GetForUpdate(ctx, deadlockKey(table, fmt.Sprintf("k%d", i))); err != nil {
			t.Fatalf("txn %d initial lock: %v", i, err)
		}
	}

	// Every transaction now writes its neighbor's locked key — a perfect
	// n-cycle. Without detection each would burn the full conflict budget.
	start := time.Now()
	errs := make([]error, n)
	done := make(chan int, n)
	for i := range txns {
		go func(i int) {
			err := txns[i].Put(ctx, deadlockKey(table, fmt.Sprintf("k%d", (i+1)%n)), []byte("w"))
			if err == nil {
				err = txns[i].Commit(ctx)
			} else {
				_ = txns[i].Rollback(ctx)
			}
			errs[i] = err
			done <- i
		}(i)
	}
	for range txns {
		<-done
	}
	return errs, time.Since(start)
}

// TestDeadlockTwoTxns: a 2-cycle resolves quickly with exactly one victim;
// the survivor commits. Regression test for issue #13 (previously both
// burned the full conflict budget).
func TestDeadlockTwoTxns(t *testing.T) {
	tc := Start(t, 3)
	errs, elapsed := runDeadlockCycle(t, tc, 910, 2)
	assertOneVictim(t, errs, elapsed)
}

// TestDeadlockThreeTxns: a 3-cycle — the chain walk crosses an
// intermediate waiter — still resolves with exactly one victim.
func TestDeadlockThreeTxns(t *testing.T) {
	tc := Start(t, 3)
	errs, elapsed := runDeadlockCycle(t, tc, 911, 3)
	assertOneVictim(t, errs, elapsed)
}

func assertOneVictim(t *testing.T, errs []error, elapsed time.Duration) {
	t.Helper()
	victims := 0
	for i, err := range errs {
		if err == nil {
			continue
		}
		if !kvclient.IsRetryable(err) {
			t.Fatalf("txn %d failed non-retryably: %v", i, err)
		}
		victims++
	}
	if victims != 1 {
		t.Fatalf("deadlock resolved with %d victims (errors: %v), want exactly 1", victims, errs)
	}
	// Well under the 10s conflict budget: detection did the work, not the
	// timeout backstop.
	if elapsed > 5*time.Second {
		t.Fatalf("deadlock took %s to resolve — backstop, not detection", elapsed)
	}
}

// TestNonDeadlockedWaiterSurvives: a plain waiter behind a slow-but-live
// lock holder is NOT aborted by the detector and no longer trips the old
// 2s budget — it waits, then proceeds.
func TestNonDeadlockedWaiterSurvives(t *testing.T) {
	tc := Start(t, 3)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	db := tc.Nodes[0].DB()
	key := deadlockKey(912, "k")
	if err := db.Put(ctx, key, []byte("v")); err != nil {
		t.Fatal(err)
	}

	// Deadline-based flip retries — under heavy load a run of losses is
	// slow, not wrong (issue #61).
	deadline := time.Now().Add(45 * time.Second)
	for {
		if time.Now().After(deadline) {
			t.Fatal("holder kept losing the priority flip until the deadline")
		}
		holder := db.NewTxn("holder")
		if _, err := holder.GetForUpdate(ctx, key); err != nil {
			t.Fatal(err)
		}

		done := make(chan error, 1)
		go func() {
			w := db.NewTxn("waiter")
			err := w.Put(ctx, key, []byte("w"))
			if err == nil {
				err = w.Commit(ctx)
			} else {
				_ = w.Rollback(ctx)
			}
			done <- err
		}()

		// Hold the lock for 3s — past the OLD 2s budget. The waiter must
		// neither time out nor be aborted by a phantom cycle.
		select {
		case err := <-done:
			// Priority flip: the waiter aborted the holder. Retry.
			if err != nil {
				t.Fatalf("waiter failed while lock held: %v", err)
			}
			_ = holder.Rollback(ctx)
			continue
		case <-time.After(3 * time.Second):
		}
		if err := holder.Commit(ctx); err != nil {
			// The waiter's winning push surfaced after the 3s window — a
			// late flip loss, so retry the scenario (issue #61).
			_ = holder.Rollback(ctx)
			<-done
			continue
		}
		if err := <-done; err != nil {
			t.Fatalf("waiter failed after 3s hold: %v", err)
		}
		return
	}
}
