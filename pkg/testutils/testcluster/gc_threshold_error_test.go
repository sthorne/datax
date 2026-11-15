package testcluster

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/sthorne/datax/pkg/sql"
	"github.com/sthorne/datax/pkg/sql/catalog"
)

// TestBelowGCThresholdSQLState (issue #313): a read below a range's GC
// threshold used to reach the client as a bare internal error (XX000).
// With per-table windows and purge it is reachable by ordinary work, and
// what the client should do differs by who chose the timestamp:
//
//   - AS OF SYSTEM TIME names the moment, and the history at it is gone:
//     no retry can succeed. PostgreSQL's snapshot_too_old, 72000.
//   - A transaction whose timestamp the cluster chose — here one that
//     began before a purge of a table it reads — succeeds when run again
//     at a new timestamp: serialization_failure, 40001, which drivers
//     and ORMs already retry.
func TestBelowGCThresholdSQLState(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	tc := Start(t, 1)
	n := tc.Nodes[0]
	s := sql.NewSession(n.DB(), catalog.NewAccessor())
	waitForDatabases(t, ctx, s)
	execSQL(t, ctx, s, `CREATE TABLE t (id INT8 PRIMARY KEY, v TEXT)`)
	execSQL(t, ctx, s, `INSERT INTO t VALUES (1, 'a')`)
	before := strconv.FormatInt(n.Clock().Now().WallTime, 10)

	long := sql.NewSession(n.DB(), catalog.NewAccessor())
	execSQL(t, ctx, long, `BEGIN`)
	execSQL(t, ctx, long, `SELECT v FROM t`)
	// The version 'before' reads is superseded and collected: a purge
	// with nothing to collect moves no threshold.
	execSQL(t, ctx, s, `UPDATE t SET v = 'b' WHERE id = 1`)
	execSQL(t, ctx, s, `ALTER TABLE t PURGE`)

	if _, serr := trySQL(ctx, s, `SELECT v FROM t AS OF SYSTEM TIME '`+before+`'`); serr == nil || serr.Code != sql.CodeSnapshotTooOld {
		t.Fatalf("AS OF SYSTEM TIME below the purge: %v, want %s", serr, sql.CodeSnapshotTooOld)
	}
	if _, serr := trySQL(ctx, long, `SELECT v FROM t`); serr == nil || serr.Code != sql.CodeSerializationFailure {
		t.Fatalf("a transaction from before the purge: %v, want %s", serr, sql.CodeSerializationFailure)
	}
	execSQL(t, ctx, long, `ROLLBACK`)
	// Run again, it succeeds.
	execSQL(t, ctx, long, `BEGIN`)
	if res := execSQL(t, ctx, long, `SELECT v FROM t`); len(res.Rows) != 1 || res.Rows[0][0].S != "b" {
		t.Fatalf("the retried transaction: %+v", res.Rows)
	}
	execSQL(t, ctx, long, `COMMIT`)
}
