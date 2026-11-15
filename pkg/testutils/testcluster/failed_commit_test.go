package testcluster

import (
	"context"
	"testing"
	"time"

	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/kvclient"
	"github.com/sthorne/datax/pkg/kvserver"
)

// failedCommitReleaseBound is how long a reader may wait on the intents
// of a transaction whose commit has already failed. Before issue #315
// the wait lasted until the failed transaction's record expired
// (kvserver.TxnExpiration after its last heartbeat); the bound leaves
// ample room under -race while staying well short of that.
const failedCommitReleaseBound = 2 * time.Second

// readerWaitOn reads key in a fresh transaction and returns what it read
// and how long the read took. A read pushes without the power to abort,
// so it waits for as long as a live transaction holds an intent on key.
func readerWaitOn(t *testing.T, ctx context.Context, db *kvclient.DB, key keys.Key) ([]byte, time.Duration) {
	t.Helper()
	reader := db.NewTxn("reader")
	defer func() { _ = reader.Rollback(ctx) }()
	start := time.Now()
	v, err := reader.Get(ctx, key)
	if err != nil {
		t.Fatalf("reader: %v", err)
	}
	return v, time.Since(start)
}

// TestFailedCommitReleasesIntents: a commit that fails with a retryable
// error aborts the transaction's record and resolves its intents before
// it returns, so no one waits on a transaction that has given up. Issue
// #315: the record was left PENDING with its heartbeat stopped, and a
// SQL session's ROLLBACK after the failed COMMIT found the transaction
// finished and did nothing; on the bank workload every such failure held
// two accounts for up to five seconds, and the transactions queued
// behind them filled most of the workers' time.
func TestFailedCommitReleasesIntents(t *testing.T) {
	tc := Start(t, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	db := tc.Nodes[0].DB()
	key := func(k string) keys.Key { return append(keys.TableDataPrefix(431), k...) }

	t.Run("refresh fails at commit", func(t *testing.T) {
		read, pushed := key("read"), key("pushed")
		if err := db.Put(ctx, read, []byte("v1")); err != nil {
			t.Fatal(err)
		}
		txn := db.NewTxn("doomed-at-commit")
		if _, err := txn.Get(ctx, read); err != nil {
			t.Fatal(err)
		}
		// Someone else changes what it read...
		if err := db.Put(ctx, read, []byte("v2")); err != nil {
			t.Fatal(err)
		}
		// ...and a later read of the key it writes pushes its write
		// above its read timestamp, so the commit must refresh, and the
		// refresh finds the change.
		if _, err := db.Get(ctx, pushed); err != nil {
			t.Fatal(err)
		}
		if err := txn.Put(ctx, pushed, []byte("lost")); err != nil {
			t.Fatal(err)
		}
		err := txn.Commit(ctx)
		if !kvclient.IsRetryable(err) {
			t.Fatalf("commit: got %v, want a retryable error", err)
		}
		_ = txn.Rollback(ctx) // what a SQL client sends; must not be needed
		v, waited := readerWaitOn(t, ctx, db, pushed)
		if v != nil {
			t.Fatalf("read %q over the failed transaction's write, want nothing", v)
		}
		if waited > failedCommitReleaseBound {
			t.Fatalf("reader waited %s on the intent of a transaction whose commit had failed (record expiry is %s)", waited, kvserver.TxnExpiration)
		}
	})

	t.Run("past its deadline", func(t *testing.T) {
		k := key("deadline")
		txn := db.NewTxn("past-deadline")
		if err := txn.Put(ctx, k, []byte("lost")); err != nil {
			t.Fatal(err)
		}
		// A schema lease it planned under expired at its own timestamp:
		// the commit is refused before anything is sent.
		txn.UpdateDeadline(txn.ReadTimestamp())
		err := txn.Commit(ctx)
		if !kvclient.IsRetryable(err) {
			t.Fatalf("commit: got %v, want a retryable error", err)
		}
		v, waited := readerWaitOn(t, ctx, db, k)
		if v != nil {
			t.Fatalf("read %q over the failed transaction's write, want nothing", v)
		}
		if waited > failedCommitReleaseBound {
			t.Fatalf("reader waited %s on the intent of a transaction whose commit had failed (record expiry is %s)", waited, kvserver.TxnExpiration)
		}
	})
}
