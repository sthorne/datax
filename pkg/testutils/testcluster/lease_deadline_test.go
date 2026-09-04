package testcluster

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/sql"
	"github.com/sthorne/datax/pkg/sql/catalog"
)

// leasedSessionWithAccessor is leasedSession returning the accessor too,
// for tests that drive its lease renewal.
func leasedSessionWithAccessor(t *testing.T, tc *TestCluster, node int, ttl time.Duration) (*sql.Session, *catalog.Accessor) {
	t.Helper()
	n := tc.Nodes[node]
	cat := catalog.NewAccessor()
	if err := cat.StartLeasing(n.DB(), n.Clock(), n.Stopper(), ttl); err != nil {
		t.Fatal(err)
	}
	return sql.NewSession(n.DB(), cat), cat
}

// TestOnlineCreateIndexUnderLapsedLease (issue #110): a gateway whose
// lease renewals stall keeps running statements planned against the
// pre-index descriptor. Two shapes must both end with every row indexed:
//
//   - a write already laid down (an intent in the table's tail) when the
//     backfill runs: the backfill's chunk reads cover the whole primary
//     span, so the chunk waits for that writer and indexes its row once
//     it commits, at the writer's original timestamp;
//   - a write issued after the backfill read its key span: the timestamp
//     cache pushes it above the backfill, and the pushed commit fails the
//     transaction's lease deadline with a retryable error instead of
//     landing a row the index never sees; the retry re-plans with the
//     index.
func TestOnlineCreateIndexUnderLapsedLease(t *testing.T) {
	tc := Start(t, 3)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	const ttl = 2 * time.Second
	sA := leasedSession(t, tc, 0, ttl)
	sB, catB := leasedSessionWithAccessor(t, tc, 1, ttl)

	execSQL(t, ctx, sA, `CREATE TABLE kv (id INT PRIMARY KEY, v INT)`)
	for i := 0; i < 5; i++ {
		execSQL(t, ctx, sA, fmt.Sprintf(`INSERT INTO kv VALUES (%d, %d)`, i, i%3))
	}
	countIndex := func(name string) int {
		t.Helper()
		desc := lookupDescriptor(t, ctx, tc.Nodes[0].DB(), "kv")
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

	// Shape 1: B's intent on a tail key precedes the backfill. B's
	// gateway stalls; A's drain waits out B's lease, then the backfill
	// blocks on B's intent until B commits, and indexes the row.
	execSQL(t, ctx, sB, `BEGIN`)
	execSQL(t, ctx, sB, `INSERT INTO kv VALUES (500, 1)`)
	catB.TestingPauseRenewal(true)
	indexDone := make(chan *sql.Error, 1)
	go func() {
		_, serr := trySQL(ctx, sA, `CREATE INDEX by_v ON kv (v)`)
		indexDone <- serr
	}()
	select {
	case serr := <-indexDone:
		t.Fatalf("CREATE INDEX returned before B committed (serr=%v); the backfill did not wait on B's intent", serr)
	case <-time.After(3 * ttl):
	}
	execSQL(t, ctx, sB, `COMMIT`) // at B's original timestamp, before its lease expired: allowed
	select {
	case serr := <-indexDone:
		if serr != nil {
			t.Fatalf("CREATE INDEX: [%s] %s", serr.Code, serr.Msg)
		}
	case <-time.After(60 * time.Second):
		t.Fatal("CREATE INDEX did not complete after B committed")
	}
	if n := countIndex("by_v"); n != 6 {
		t.Fatalf("index has %d entries, want 6 (B's pre-backfill row must be indexed)", n)
	}

	// Shape 2: B plans under a lease, the lease lapses, the index build
	// completes, and only then does B write. The write is pushed above
	// the backfill's read and the pushed commit is refused.
	catB.TestingPauseRenewal(false)
	execSQL(t, ctx, sB, `SELECT id FROM kv WHERE id = 0`) // renew and re-adopt; then plan under the lease
	execSQL(t, ctx, sB, `BEGIN`)
	execSQL(t, ctx, sB, `SELECT id FROM kv WHERE id = 1`)
	catB.TestingPauseRenewal(true)
	start := time.Now()
	execSQL(t, ctx, sA, `CREATE INDEX by_v2 ON kv (v)`)
	if waited := time.Since(start); waited < ttl/2 {
		t.Fatalf("the drain should have waited on B's live lease; CREATE INDEX returned after %s", waited)
	}
	if _, serr := trySQL(ctx, sB, `INSERT INTO kv VALUES (600, 2)`); serr != nil && serr.Code != sql.CodeSerializationFailure {
		t.Fatalf("INSERT after the lapsed lease: [%s] %s", serr.Code, serr.Msg)
	}
	_, serr := trySQL(ctx, sB, `COMMIT`)
	if serr == nil {
		t.Fatal("COMMIT past the lease's expiration succeeded; the index would miss the row")
	}
	if serr.Code != sql.CodeSerializationFailure {
		t.Fatalf("COMMIT past the lease: [%s] %s, want %s", serr.Code, serr.Msg, sql.CodeSerializationFailure)
	}
	if n := countIndex("by_v2"); n != 6 {
		t.Fatalf("index by_v2 has %d entries after the refused commit, want 6", n)
	}
	if rows := execSQL(t, ctx, sA, `SELECT id FROM kv`); len(rows.Rows) != 6 {
		t.Fatalf("table has %d rows after the refused commit, want 6", len(rows.Rows))
	}

	// B recovers: renewal resumes, the lapsed entry is re-read, and the
	// retried statement maintains both indexes.
	catB.TestingPauseRenewal(false)
	execSQL(t, ctx, sB, `INSERT INTO kv VALUES (600, 2)`)
	if n := countIndex("by_v2"); n != 7 {
		t.Fatalf("index by_v2 has %d entries after B's retry, want 7", n)
	}
	rows := execSQL(t, ctx, sB, `SELECT id FROM kv WHERE v = 2`)
	if len(rows.Rows) != 2 { // id 2 from the seed and 600
		t.Fatalf("index read after the retry: %d rows, want 2", len(rows.Rows))
	}
}
