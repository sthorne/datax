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

// TestPurgeCutoffSQLStateOverPgwire (issue #320, from its report: a
// purge's cut-off reached a client as XX000): the same two refusals as
// TestBelowGCThresholdSQLState, through pgwire with a stock driver, for
// an explicit transaction that read the table before the purge and for
// one that wrote it, and for AS OF SYSTEM TIME.
func TestPurgeCutoffSQLStateOverPgwire(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	tc := Start(t, 1)
	admin := pgConn(t, ctx, tc, 0)
	for _, stmt := range []string{
		`CREATE TABLE p (id INT8 PRIMARY KEY, v TEXT)`,
		`INSERT INTO p VALUES (1, 'a'), (2, 'a')`,
	} {
		if _, err := admin.Exec(ctx, stmt); err != nil {
			t.Fatalf("%s: %v", stmt, err)
		}
	}
	before := strconv.FormatInt(tc.Nodes[0].Clock().Now().WallTime, 10)
	reader, writer := pgConn(t, ctx, tc, 0), pgConn(t, ctx, tc, 0)
	rtx, err := reader.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = rtx.Rollback(context.Background()) }()
	var v string
	if err := rtx.QueryRow(ctx, `SELECT v FROM p WHERE id = 1`).Scan(&v); err != nil {
		t.Fatal(err)
	}
	wtx, err := writer.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = wtx.Rollback(context.Background()) }()
	if err := wtx.QueryRow(ctx, `SELECT v FROM p WHERE id = 2`).Scan(&v); err != nil {
		t.Fatal(err)
	}
	// The versions both read are superseded, then collected.
	if _, err := admin.Exec(ctx, `UPDATE p SET v = 'b'`); err != nil {
		t.Fatal(err)
	}
	if _, err := admin.Exec(ctx, `ALTER TABLE p PURGE`); err != nil {
		t.Fatal(err)
	}
	if err := admin.QueryRow(ctx, `SELECT v FROM p AS OF SYSTEM TIME '`+before+`' WHERE id = 1`).Scan(&v); sqlState(err) != sql.CodeSnapshotTooOld {
		t.Fatalf("AS OF SYSTEM TIME below the purge: %v, want %s", err, sql.CodeSnapshotTooOld)
	}
	if err := rtx.QueryRow(ctx, `SELECT v FROM p WHERE id = 1`).Scan(&v); sqlState(err) != sql.CodeSerializationFailure {
		t.Fatalf("a read from before the purge: %v, want %s", err, sql.CodeSerializationFailure)
	}
	if _, err := wtx.Exec(ctx, `UPDATE p SET v = 'c' WHERE id = 2`); sqlState(err) != sql.CodeSerializationFailure {
		t.Fatalf("a write from before the purge: %v, want %s", err, sql.CodeSerializationFailure)
	}
}
