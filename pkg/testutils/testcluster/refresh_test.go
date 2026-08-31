package testcluster

import (
	"context"
	"testing"
	"time"

	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/kvclient"
)

// TestTxnRefreshAvoidsRestart: a transaction reads key A, then a
// NON-transactional write bumps the timestamp cache high on the same range
// via a read... more directly: another client writes key B (which the txn
// then wants to write) at a higher timestamp. v1 surfaced 40001; with read
// refresh the transaction verifies A is unchanged and commits.
func TestTxnRefreshAvoidsRestart(t *testing.T) {
	tc := Start(t, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	db := tc.Nodes[0].DB()

	keyA := append(keys.TableDataPrefix(700), "a"...)
	keyB := append(keys.TableDataPrefix(700), "b"...)
	if err := db.Put(ctx, keyA, []byte("a0")); err != nil {
		t.Fatal(err)
	}
	if err := db.Put(ctx, keyB, []byte("b0")); err != nil {
		t.Fatal(err)
	}

	txn := db.NewTxn("refresher")
	v, err := txn.Get(ctx, keyA)
	if err != nil || string(v) != "a0" {
		t.Fatalf("read A: %q %v", v, err)
	}

	// A later non-txn write to B forces the txn's write of B above its
	// current timestamps (WriteTooOld).
	if err := db.Put(ctx, keyB, []byte("b1")); err != nil {
		t.Fatal(err)
	}

	// v1: this returned RetryableError. Now: refresh of [A] succeeds (A is
	// untouched) and the write proceeds at the bumped timestamp.
	if err := txn.Put(ctx, keyB, []byte("b-txn")); err != nil {
		t.Fatalf("write B should have refreshed, got: %v", err)
	}
	if err := txn.Commit(ctx); err != nil {
		t.Fatalf("commit after refresh: %v", err)
	}
	v, err = db.Get(ctx, keyB)
	if err != nil || string(v) != "b-txn" {
		t.Fatalf("final B: %q %v", v, err)
	}
}

// TestTxnRefreshFailsWhenReadInvalidated: if the refresh window contains a
// write to a key the transaction already read, the refresh must fail and
// the conflict surfaces as a retryable error — never a silent stale read.
func TestTxnRefreshFailsWhenReadInvalidated(t *testing.T) {
	tc := Start(t, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	db := tc.Nodes[0].DB()

	keyA := append(keys.TableDataPrefix(701), "a"...)
	keyB := append(keys.TableDataPrefix(701), "b"...)
	if err := db.Put(ctx, keyA, []byte("a0")); err != nil {
		t.Fatal(err)
	}
	if err := db.Put(ctx, keyB, []byte("b0")); err != nil {
		t.Fatal(err)
	}

	txn := db.NewTxn("doomed-reader")
	if _, err := txn.Get(ctx, keyA); err != nil {
		t.Fatal(err)
	}
	// Invalidate the read AND force a timestamp bump on B.
	if err := db.Put(ctx, keyA, []byte("a1")); err != nil {
		t.Fatal(err)
	}
	if err := db.Put(ctx, keyB, []byte("b1")); err != nil {
		t.Fatal(err)
	}

	err := txn.Put(ctx, keyB, []byte("b-txn"))
	if err == nil {
		// The write itself may succeed only if refresh somehow passed —
		// which would be a correctness bug, caught at commit or here.
		err = txn.Commit(ctx)
	}
	if err == nil {
		t.Fatal("transaction with invalidated read committed")
	}
	if !kvclient.IsRetryable(err) {
		t.Fatalf("expected retryable error, got %v", err)
	}
	_ = txn.Rollback(ctx)
}

// TestTxnRefreshOnCommit: the read-then-conflicting-read-elsewhere pattern
// where only the COMMIT discovers the divergence (a tsCache push on the
// write) also refreshes instead of failing.
func TestTxnRefreshScanSpans(t *testing.T) {
	tc := Start(t, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	db := tc.Nodes[0].DB()

	prefix := keys.TableDataPrefix(702)
	k := func(s string) keys.Key { return append(prefix.Clone(), s...) }
	for _, s := range []string{"a", "b", "c"} {
		if err := db.Put(ctx, k(s), []byte("v-"+s)); err != nil {
			t.Fatal(err)
		}
	}

	// Txn scans [a, c); a write lands OUTSIDE the scanned span (on c);
	// then the txn writes c: refresh of the scan span must succeed.
	txn := db.NewTxn("scanner")
	rows, err := txn.Scan(ctx, k("a"), k("c"), 0)
	if err != nil || len(rows) != 2 {
		t.Fatalf("scan: %d rows %v", len(rows), err)
	}
	if err := db.Put(ctx, k("c"), []byte("bump")); err != nil {
		t.Fatal(err)
	}
	if err := txn.Put(ctx, k("c"), []byte("mine")); err != nil {
		t.Fatalf("write after out-of-span bump: %v", err)
	}
	if err := txn.Commit(ctx); err != nil {
		t.Fatalf("commit: %v", err)
	}

	// Same shape, but the interfering write lands INSIDE the scanned span:
	// must fail retryably.
	txn2 := db.NewTxn("scanner2")
	if _, err := txn2.Scan(ctx, k("a"), k("c"), 0); err != nil {
		t.Fatal(err)
	}
	if err := db.Put(ctx, k("b"), []byte("invalidate")); err != nil {
		t.Fatal(err)
	}
	if err := db.Put(ctx, k("c"), []byte("bump2")); err != nil {
		t.Fatal(err)
	}
	err = txn2.Put(ctx, k("c"), []byte("mine2"))
	if err == nil {
		err = txn2.Commit(ctx)
	}
	if err == nil || !kvclient.IsRetryable(err) {
		t.Fatalf("expected retryable after in-span invalidation, got %v", err)
	}
	_ = txn2.Rollback(ctx)
}
