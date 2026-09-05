package testcluster

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/sthorne/datax/pkg/metrics"
	"github.com/sthorne/datax/pkg/sql"
	"github.com/sthorne/datax/pkg/sql/parser"
)

// TestStatementPanicBarrier (issue #136): a panic on the statement path
// — before execution, or while a stream produces rows — fails that
// statement with XX000 and is counted, instead of taking the node down
// with every connection on it. The connection stays usable; inside a
// transaction block the block is failed until ROLLBACK.
func TestStatementPanicBarrier(t *testing.T) {
	tc := Start(t, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	conn, err := pgx.Connect(ctx, pgURL(tc, 0))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close(ctx) }()
	fillBig(t, ctx, conn, 3000, 300)

	// Every SELECT from table "boom" panics before it executes.
	sql.TestingPanicBeforeExec = func(stmt parser.Statement) {
		if sel, ok := stmt.(*parser.Select); ok && sel.Table == "boom" {
			panic("injected: nil descriptor")
		}
	}
	defer func() { sql.TestingPanicBeforeExec = nil }()
	usable := func() {
		t.Helper()
		var one int64
		if err := conn.QueryRow(ctx, `SELECT 1`).Scan(&one); err != nil || one != 1 {
			t.Fatalf("connection after the panic: %v", err)
		}
	}
	before := metrics.CounterValue(metrics.SQLStatementPanics)

	// An implicit transaction: XX000, the connection lives on.
	_, err = conn.Exec(ctx, `SELECT * FROM boom`)
	if pgErrCode(err) != sql.CodeInternal || !strings.Contains(err.Error(), "injected") {
		t.Fatalf("panicking statement: %v", err)
	}
	if got := metrics.CounterValue(metrics.SQLStatementPanics) - before; got != 1 {
		t.Fatalf("datax_sql_statement_panics_total rose by %v, want 1", got)
	}
	usable()

	// In a block: the block is failed until ROLLBACK, then the
	// connection is fine.
	tx, err := conn.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, `INSERT INTO big VALUES (100000, 1, 'x')`); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, `SELECT * FROM boom`); pgErrCode(err) != sql.CodeInternal {
		t.Fatalf("panicking statement in a block: %v", err)
	}
	if _, err := tx.Exec(ctx, `SELECT 1`); pgErrCode(err) != sql.CodeInFailedTransaction {
		t.Fatalf("after the panic in the block: %v", err)
	}
	if err := tx.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	usable()
	var n int64
	if err := conn.QueryRow(ctx, `SELECT count(*) FROM big WHERE k = 100000`).Scan(&n); err != nil || n != 0 {
		t.Fatalf("the failed block's write: %d rows, %v", n, err)
	}

	// A panic while the stream produces rows, after some were sent: the
	// rows already sent, then XX000; the connection lives on.
	sql.TestingStreamHook = func(ctx context.Context, rows int64) error {
		if rows == 500 {
			panic("injected: mid-stream")
		}
		return nil
	}
	defer func() { sql.TestingStreamHook = nil }()
	rs, err := conn.Query(ctx, `SELECT k FROM big`)
	if err != nil {
		t.Fatal(err)
	}
	var got int64
	for rs.Next() {
		got++
	}
	if pgErrCode(rs.Err()) != sql.CodeInternal || got == 0 || got >= 500 {
		t.Fatalf("mid-stream panic: %v after %d rows", rs.Err(), got)
	}
	sql.TestingStreamHook = nil
	usable()
	if got := metrics.CounterValue(metrics.SQLStatementPanics) - before; got != 3 {
		t.Fatalf("datax_sql_statement_panics_total rose by %v, want 3", got)
	}
}
