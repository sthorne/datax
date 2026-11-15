package testcluster

import (
	"context"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/kvclient"
)

// doomedWaitBound is how long an operation may take to give up on a lock
// it cannot outwait. Without the check it waits for the holder — here
// until the operation returns or holderCap passes — so the bound sits
// far below holderCap while leaving room for -race.
const (
	doomedWaitBound = 2 * time.Second
	holderCap       = 4 * time.Second
)

// holdThenCommit runs holder's commit once release is closed or after
// holderCap, and reports the commit's error on the returned channel.
func holdThenCommit(ctx context.Context, holder *kvclient.Txn, release <-chan struct{}) <-chan error {
	done := make(chan error, 1)
	go func() {
		select {
		case <-release:
		case <-time.After(holderCap):
		}
		done <- holder.Commit(ctx)
	}()
	return done
}

// TestDoomedWaiterRestartsAtOnce: a transaction blocked on another's
// intent over a key it has itself read cannot commit unless the holder
// aborts — the holder's version would land inside its read span above
// its read timestamp — so it restarts at once rather than wait to learn
// the holder committed (issue #315). On the bank workload these waits
// ended in "read within uncertainty interval" nearly every time, a poll
// or more after they began.
func TestDoomedWaiterRestartsAtOnce(t *testing.T) {
	tc := Start(t, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	db := tc.Nodes[0].DB()
	key := func(k string) keys.Key { return append(keys.TableDataPrefix(432), k...) }

	// run reads k in a low-priority transaction unless skipRead, lets a
	// higher-priority holder write k and keep it, then runs op against k.
	// The holder commits once op has returned.
	type outcome struct {
		waiter    *kvclient.Txn
		err       error // op's
		took      time.Duration
		holderErr error // the holder's commit's
	}
	run := func(t *testing.T, k keys.Key, skipRead bool, waiterPri, holderPri int32, op func(*kvclient.Txn) error) outcome {
		t.Helper()
		if err := db.Put(ctx, k, []byte("v0")); err != nil {
			t.Fatal(err)
		}
		waiter := db.NewTxn("waiter")
		waiter.TestingSetPriority(waiterPri)
		if !skipRead {
			if _, err := waiter.Get(ctx, k); err != nil {
				t.Fatal(err)
			}
		}
		holder := db.NewTxn("holder")
		holder.TestingSetPriority(holderPri)
		if err := holder.Put(ctx, k, []byte("held")); err != nil {
			t.Fatal(err)
		}
		release := make(chan struct{})
		committed := holdThenCommit(ctx, holder, release)
		start := time.Now()
		err := op(waiter)
		took := time.Since(start)
		close(release)
		t.Logf("op took %s: %v", took, err)
		return outcome{waiter: waiter, err: err, took: took, holderErr: <-committed}
	}

	t.Run("write over a read key", func(t *testing.T) {
		o := run(t, key("w"), false, 1, 1000, func(txn *kvclient.Txn) error {
			return txn.Put(ctx, key("w"), []byte("mine"))
		})
		defer func() { _ = o.waiter.Rollback(ctx) }()
		if !kvclient.IsRetryable(o.err) {
			t.Fatalf("write: got %v, want a retryable error", o.err)
		}
		if o.took > doomedWaitBound {
			t.Fatalf("write waited %s for a holder it could not outwait", o.took)
		}
		if o.holderErr != nil {
			t.Fatalf("holder: %v", o.holderErr)
		}
	})

	t.Run("read of a read key inside the uncertainty window", func(t *testing.T) {
		// The holder began after the waiter read the key, well within
		// the clock's maximum offset, so its intent lies in the waiter's
		// uncertainty window and the second read must push it.
		o := run(t, key("r"), false, 1, 1000, func(txn *kvclient.Txn) error {
			_, err := txn.Get(ctx, key("r"))
			return err
		})
		defer func() { _ = o.waiter.Rollback(ctx) }()
		if !kvclient.IsRetryable(o.err) {
			t.Fatalf("read: got %v, want a retryable error", o.err)
		}
		if o.took > doomedWaitBound {
			t.Fatalf("read waited %s for a holder it could not outwait", o.took)
		}
		if o.holderErr != nil {
			t.Fatalf("holder: %v", o.holderErr)
		}
	})

	t.Run("a key not read still waits", func(t *testing.T) {
		o := run(t, key("n"), true, 1, 1000, func(txn *kvclient.Txn) error {
			return txn.Put(ctx, key("n"), []byte("mine"))
		})
		if o.err != nil {
			t.Fatalf("write: %v", o.err)
		}
		if o.took < holderCap/2 {
			t.Fatalf("write took %s: it should have waited for the holder", o.took)
		}
		if o.holderErr != nil {
			t.Fatalf("holder: %v", o.holderErr)
		}
		if err := o.waiter.Commit(ctx); err != nil {
			t.Fatalf("waiter's commit after the holder's: %v", err)
		}
	})

	t.Run("a stronger writer still aborts the holder", func(t *testing.T) {
		o := run(t, key("s"), false, 1000, 1, func(txn *kvclient.Txn) error {
			return txn.Put(ctx, key("s"), []byte("mine"))
		})
		if o.err != nil {
			t.Fatalf("write: %v", o.err)
		}
		if err := o.waiter.Commit(ctx); err != nil {
			t.Fatalf("waiter's commit: %v", err)
		}
		if !kvclient.IsRetryable(o.holderErr) {
			t.Fatalf("holder's commit: got %v, want it aborted", o.holderErr)
		}
	})
}

// TestContendedTransfersFailFast runs bank-shaped transfers — read two
// accounts, write both, commit — from many workers over a few accounts,
// each attempt in a transaction of its own as a SQL client's explicit
// transaction would be, and requires that no attempt ending in a
// retryable error ran for seconds. Before issue #315 such attempts took
// three seconds on average: they waited out the record expiry of
// transactions whose commits had failed, and waited on transactions
// whose commit could only make them restart.
func TestContendedTransfersFailFast(t *testing.T) {
	tc := Start(t, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	db := tc.Nodes[0].DB()
	const (
		accounts = 12
		workers  = 8
		runFor   = 3 * time.Second
		// Well below the five-second record expiry a stranded intent
		// cost, with room for -race and a loaded CI machine.
		attemptBound = 3 * time.Second
	)
	acct := func(i int) keys.Key { return append(keys.TableDataPrefix(433), fmt.Sprintf("acct%02d", i)...) }
	for i := 0; i < accounts; i++ {
		if err := db.Put(ctx, acct(i), []byte("100")); err != nil {
			t.Fatal(err)
		}
	}

	type result struct {
		commits, restarts int
		slowest           time.Duration // of the attempts that restarted
		err               error
	}
	results := make(chan result, workers)
	deadline := time.Now().Add(runFor)
	for w := 0; w < workers; w++ {
		go func(w int) {
			var r result
			rng := rand.New(rand.NewSource(int64(w)))
			for time.Now().Before(deadline) {
				a := rng.Intn(accounts)
				b := (a + 1 + rng.Intn(accounts-1)) % accounts
				start := time.Now()
				txn := db.NewTxn("transfer")
				err := func() error {
					va, err := txn.Get(ctx, acct(a))
					if err != nil {
						return err
					}
					vb, err := txn.Get(ctx, acct(b))
					if err != nil {
						return err
					}
					if err := txn.Put(ctx, acct(a), va); err != nil {
						return err
					}
					if err := txn.Put(ctx, acct(b), vb); err != nil {
						return err
					}
					return txn.Commit(ctx)
				}()
				switch {
				case err == nil:
					r.commits++
				case kvclient.IsRetryable(err):
					_ = txn.Rollback(ctx)
					r.restarts++
					r.slowest = max(r.slowest, time.Since(start))
				default:
					_ = txn.Rollback(ctx)
					r.err = err
					results <- r
					return
				}
			}
			results <- r
		}(w)
	}
	var total result
	for w := 0; w < workers; w++ {
		r := <-results
		if r.err != nil {
			t.Fatalf("transfer: %v", r.err)
		}
		total.commits += r.commits
		total.restarts += r.restarts
		total.slowest = max(total.slowest, r.slowest)
	}
	t.Logf("%d transfers committed, %d attempts restarted, the slowest of them in %s", total.commits, total.restarts, total.slowest)
	if total.commits == 0 {
		t.Fatal("no transfer committed")
	}
	if total.slowest > attemptBound {
		t.Fatalf("an attempt that ended in a restart ran for %s", total.slowest)
	}
}
