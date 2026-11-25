package testcluster

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/kvclient"
	"github.com/sthorne/datax/pkg/kvserver"
	"github.com/sthorne/datax/pkg/server"
)

// startPartialBatchCluster starts a one-node cluster whose user table
// range (split off at the returned prefix) sheds writes as overloaded
// while the returned flag is set; the range holding keys.UserKey keys
// keeps serving. A transactional batch over both then applies on one
// range and fails on the other.
func startPartialBatchCluster(t *testing.T, table uint64) (*kvclient.DB, *atomic.Bool, keys.Key) {
	t.Helper()
	overloaded := &atomic.Bool{}
	knobs := kvserver.TestingKnobs{
		OverrideOverloaded: func() (bool, string) {
			if overloaded.Load() {
				return true, "test overload"
			}
			return false, ""
		},
	}
	tc, _ := StartWithEngines(t, 1, func(c *server.Config) { c.TestingKnobs = knobs })
	db := tc.Nodes[0].DB()
	db.SetOverloadRetryBudget(200 * time.Millisecond)
	prefix := keys.TableDataPrefix(table)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if _, err := db.AdminSplit(ctx, prefix); err != nil {
		t.Fatal(err)
	}
	return db, overloaded, prefix
}

// A multi-range write batch whose anchor part (sent first, alone,
// creating the record) applies and whose other part then fails with an
// error other than a duplicate key — here a write shed under storage
// overload — used to leave the coordinator believing nothing applied:
// not anchored, its keys untracked. After a ROLLBACK TO SAVEPOINT the
// transaction wrote more, and COMMIT reported success taking it for
// read-only: the record stayed PENDING, a pusher aborted it once it
// expired, and the committed write was lost.
func TestTxnPartialBatchFailureKeepsLaterCommit(t *testing.T) {
	db, overloaded, prefix := startPartialBatchCluster(t, 978)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	anchor := keys.UserKey("partial-batch-anchor")
	k1 := append(prefix.Clone(), "k1"...)
	k2 := append(prefix.Clone(), "k2"...)

	txn := db.NewTxn("victim")
	if err := txn.Savepoint("sp"); err != nil {
		t.Fatal(err)
	}
	overloaded.Store(true)
	var wb kvclient.WriteBatch
	wb.Put(anchor, []byte("a"))
	wb.Put(k1, []byte("x"))
	err := txn.RunBatch(ctx, &wb)
	overloaded.Store(false)
	if err == nil || !kvclient.IsStorageOverloaded(err) {
		t.Fatalf("batch over a shedding range: got %v, want the shed", err)
	}
	if err := txn.RollbackToSavepoint(ctx, "sp"); err != nil {
		t.Fatal(err)
	}
	if err := txn.Put(ctx, k2, []byte("committed-value")); err != nil {
		t.Fatal(err)
	}
	if err := txn.Commit(ctx); err != nil {
		t.Fatalf("commit: %v", err)
	}

	// Read without waiting for the record to expire: the commit must have
	// ended it, so the reader finds k2 committed at once, and the rolled
	// back anchor write gone.
	reader := db.NewTxn("reader")
	defer func() { _ = reader.Rollback(ctx) }()
	rctx, rcancel := context.WithTimeout(ctx, 3*time.Second)
	defer rcancel()
	v, err := reader.Get(rctx, k2)
	if err != nil {
		t.Fatalf("read k2: %v", err)
	}
	if string(v) != "committed-value" {
		t.Fatalf("COMMIT succeeded but k2 = %q, want the committed value", v)
	}
	if v, err := reader.Get(rctx, anchor); err != nil || v != nil {
		t.Fatalf("anchor key after ROLLBACK TO SAVEPOINT: %q, %v; want absent", v, err)
	}
}

// In an anchored transaction a multi-range batch's ranges are written in
// parallel; one that applied while another shed the write was not
// tracked, so ROLLBACK TO SAVEPOINT left its intent in place and COMMIT
// made the rolled-back write visible.
func TestTxnPartialBatchFailureRolledBackBySavepoint(t *testing.T) {
	db, overloaded, prefix := startPartialBatchCluster(t, 979)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	anchor := keys.UserKey("partial-batch-sp-anchor")
	sibling := keys.UserKey("partial-batch-sp-sibling")
	k1 := append(prefix.Clone(), "k1"...)

	txn := db.NewTxn("victim")
	if err := txn.Put(ctx, anchor, []byte("kept")); err != nil {
		t.Fatal(err)
	}
	if err := txn.Savepoint("sp"); err != nil {
		t.Fatal(err)
	}
	overloaded.Store(true)
	var wb kvclient.WriteBatch
	wb.Put(sibling, []byte("rolled back"))
	wb.Put(k1, []byte("x"))
	err := txn.RunBatch(ctx, &wb)
	overloaded.Store(false)
	if err == nil || !kvclient.IsStorageOverloaded(err) {
		t.Fatalf("batch over a shedding range: got %v, want the shed", err)
	}
	if err := txn.RollbackToSavepoint(ctx, "sp"); err != nil {
		t.Fatal(err)
	}
	if err := txn.Commit(ctx); err != nil {
		t.Fatalf("commit: %v", err)
	}

	reader := db.NewTxn("reader")
	defer func() { _ = reader.Rollback(ctx) }()
	if v, err := reader.Get(ctx, anchor); err != nil || string(v) != "kept" {
		t.Fatalf("anchor = %q, %v; want the write before the savepoint", v, err)
	}
	if v, err := reader.Get(ctx, sibling); err != nil || v != nil {
		t.Fatalf("a write rolled back to the savepoint is visible after COMMIT: %q, %v", v, err)
	}
}
