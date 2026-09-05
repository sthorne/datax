package testcluster

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/kvclient"
	"github.com/sthorne/datax/pkg/sql"
	"github.com/sthorne/datax/pkg/util/hlc"
)

// TestOnePhaseCommitHonorsDeadline (issue #133): a pipelined transaction
// with a commit deadline in the past and one deferred single-range write
// takes the one-phase path — and must fail its Commit with a retryable
// error, exactly as the classic path does, leaving nothing behind.
func TestOnePhaseCommitHonorsDeadline(t *testing.T) {
	tc := Start(t, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	db := tc.Nodes[0].DB()
	prefix := keys.TableDataPrefix(908)
	// Warm the range cache: the one-phase path takes it only on a hit.
	if err := db.Put(ctx, append(prefix.Clone(), "warm"...), []byte("w")); err != nil {
		t.Fatal(err)
	}
	key := append(prefix.Clone(), "k"...)

	txn := db.NewTxn("past-deadline")
	txn.EnablePipelining()
	txn.UpdateDeadline(hlc.Timestamp{WallTime: 1}) // long past
	var wb kvclient.WriteBatch
	wb.Put(key, []byte("v"))
	if err := txn.RunBatch(ctx, &wb); err != nil {
		t.Fatal(err)
	}
	err := txn.Commit(ctx)
	if err == nil {
		t.Fatal("a one-phase commit past the transaction's deadline succeeded")
	}
	if !kvclient.IsRetryable(err) {
		t.Fatalf("commit past the deadline: %v, want a retryable error", err)
	}
	_ = txn.Rollback(ctx)
	if v, gerr := db.Get(ctx, key); gerr != nil || v != nil {
		t.Fatalf("the refused commit left %q behind (%v)", v, gerr)
	}

	// The same transaction shape without a deadline commits in one phase.
	txn = db.NewTxn("no-deadline")
	txn.EnablePipelining()
	var wb2 kvclient.WriteBatch
	wb2.Put(key, []byte("v"))
	if err := txn.RunBatch(ctx, &wb2); err != nil {
		t.Fatal(err)
	}
	if err := txn.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if v, gerr := db.Get(ctx, key); gerr != nil || string(v) != "v" {
		t.Fatalf("committed value: %q, %v", v, gerr)
	}
}

// TestOnlineCreateIndexUnderLapsedLeaseImplicit (issue #133): shape 2 of
// TestOnlineCreateIndexUnderLapsedLease with an implicit, auto-commit
// INSERT instead of a transaction block — the statement plans under a
// lease the index build has since drained, and its one-phase commit
// must be refused (and retried under the new descriptor) rather than
// land a row the index never sees.
func TestOnlineCreateIndexUnderLapsedLeaseImplicit(t *testing.T) {
	tc := Start(t, 3)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	const ttl = 2 * time.Second
	sA := leasedSession(t, tc, 0, ttl)
	sB, catB := leasedSessionWithAccessor(t, tc, 1, ttl)

	execSQL(t, ctx, sA, `CREATE TABLE kvi (id INT PRIMARY KEY, v INT)`)
	for i := 0; i < 5; i++ {
		execSQL(t, ctx, sA, fmt.Sprintf(`INSERT INTO kvi VALUES (%d, %d)`, i, i%3))
	}
	countIndex := func(name string) int {
		t.Helper()
		desc := lookupDescriptor(t, ctx, tc.Nodes[0].DB(), "kvi")
		idx, ok := desc.Index(name)
		if !ok {
			t.Fatalf("index %s missing", name)
		}
		lo, hi := keys.TableIndexSpan(desc.ID, idx.ID)
		reader := tc.Nodes[0].DB().NewTxn("index-count")
		entries, err := reader.Scan(ctx, lo, hi, 0)
		_ = reader.Rollback(ctx)
		if err != nil {
			t.Fatal(err)
		}
		return len(entries)
	}

	// B's implicit INSERT plans under its live lease (pinning the lease's
	// expiration as the commit deadline); before it commits, its renewals
	// stall and A's index build drains the lease and completes. The
	// one-phase commit is then past the deadline: refused, and the
	// implicit retry re-plans with the index.
	execSQL(t, ctx, sB, `SELECT id FROM kvi WHERE id = 0`) // adopt the lease
	var built sync.Once
	sql.TestingBeforeImplicitCommit = func() {
		built.Do(func() {
			catB.TestingPauseRenewal(true)
			start := time.Now()
			execSQL(t, ctx, sA, `CREATE INDEX by_v ON kvi (v)`)
			if waited := time.Since(start); waited < ttl/2 {
				t.Errorf("the drain should have waited on B's live lease; CREATE INDEX returned after %s", waited)
			}
		})
	}
	defer func() { sql.TestingBeforeImplicitCommit = nil }()
	if _, serr := trySQL(ctx, sB, `INSERT INTO kvi VALUES (600, 2)`); serr != nil && serr.Code != sql.CodeSerializationFailure {
		t.Fatalf("implicit INSERT across the lapsed lease: [%s] %s", serr.Code, serr.Msg)
	}
	catB.TestingPauseRenewal(false)
	if rows := execSQL(t, ctx, sA, `SELECT id FROM kvi`); len(rows.Rows) != 6 {
		t.Fatalf("table has %d rows, want 6", len(rows.Rows))
	}
	if n := countIndex("by_v"); n != 6 {
		t.Fatalf("index by_v has %d entries, want 6: the implicit INSERT's row is missing from the index", n)
	}
	rows := execSQL(t, ctx, sA, `SELECT id FROM kvi WHERE v = 2`)
	if len(rows.Rows) != 2 { // id 2 from the seed and 600
		t.Fatalf("index read: %d rows, want 2", len(rows.Rows))
	}
}
