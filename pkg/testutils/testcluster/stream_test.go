package testcluster

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgconn/ctxwatch"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/sthorne/datax/pkg/kvclient"
	"github.com/sthorne/datax/pkg/kvpb"
	"github.com/sthorne/datax/pkg/metrics"
	"github.com/sthorne/datax/pkg/sql"
)

// fillBig creates big (k INT8 PRIMARY KEY, g INT8, pad TEXT) with an index
// on g and rows rows of padBytes padding, k = 0..rows-1, g = k % 10.
func fillBig(t *testing.T, ctx context.Context, conn *pgx.Conn, rows, padBytes int) {
	t.Helper()
	if _, err := conn.Exec(ctx, `CREATE TABLE big (k INT8 PRIMARY KEY, g INT8, pad TEXT)`); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Exec(ctx, `CREATE INDEX big_g ON big (g)`); err != nil {
		t.Fatal(err)
	}
	pad := strings.Repeat("x", padBytes)
	for i := 0; i < rows; i += 500 {
		var sb strings.Builder
		sb.WriteString("INSERT INTO big VALUES ")
		end := i + 500
		if end > rows {
			end = rows
		}
		for j := i; j < end; j++ {
			if j > i {
				sb.WriteByte(',')
			}
			fmt.Fprintf(&sb, "(%d, %d, '%s')", j, j%10, pad)
		}
		if _, err := conn.Exec(ctx, sb.String()); err != nil {
			t.Fatal(err)
		}
	}
}

// TestStreamingSelect: scan-shaped SELECTs stream — the first row reaches
// the client before the server has read the table, over both protocols
// and through a gateway that does not lead the range — with the same
// answers as before for bounds, index scans, reverse order, LIMIT and
// OFFSET, residual filters and expressions; everything that sorts or
// aggregates still materializes correctly.
func TestStreamingSelect(t *testing.T) {
	tc := Start(t, 3)
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()
	root, err := pgx.Connect(ctx, pgURL(tc, 0))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = root.Close(ctx) }()
	const rows = 30000
	fillBig(t, ctx, root, rows, 300) // ~9 MiB of rows

	for _, mode := range []struct {
		name string
		mode pgx.QueryExecMode
	}{{"extended", pgx.QueryExecModeCacheStatement}, {"simple", pgx.QueryExecModeSimpleProtocol}} {
		t.Run(mode.name, func(t *testing.T) {
			cfg, err := pgx.ParseConfig(pgURL(tc, 1))
			if err != nil {
				t.Fatal(err)
			}
			cfg.DefaultQueryExecMode = mode.mode
			conn, err := pgx.ConnectConfig(ctx, cfg)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = conn.Close(ctx) }()

			// The whole table: the first row arrives while the server is
			// still scanning (its scanned-row count is far short of the
			// table), and the count and order are right.
			scannedBefore := testutil.ToFloat64(metrics.SQLRowsScanned)
			streamedBefore := testutil.ToFloat64(metrics.SQLStreamedRows)
			rs, err := conn.Query(ctx, `SELECT k, pad FROM big`)
			if err != nil {
				t.Fatal(err)
			}
			var n int64
			var scannedAtFirst float64
			for rs.Next() {
				var k int64
				var pad string
				if err := rs.Scan(&k, &pad); err != nil {
					t.Fatal(err)
				}
				if n == 0 {
					scannedAtFirst = testutil.ToFloat64(metrics.SQLRowsScanned) - scannedBefore
				}
				if k != n || len(pad) != 300 {
					t.Fatalf("row %d: k=%d pad=%d", n, k, len(pad))
				}
				n++
			}
			if err := rs.Err(); err != nil {
				t.Fatal(err)
			}
			if n != rows {
				t.Fatalf("rows: %d, want %d", n, rows)
			}
			if scannedAtFirst >= rows {
				t.Fatalf("the server had scanned %.0f rows when the first row arrived: the result was materialized", scannedAtFirst)
			}
			if d := testutil.ToFloat64(metrics.SQLStreamedRows) - streamedBefore; d != rows {
				t.Fatalf("streamed rows: %.0f, want %d", d, rows)
			}
			t.Logf("%s: first row after %.0f of %d rows scanned", mode.name, scannedAtFirst, rows)

			check := func(q string, want []int64) {
				t.Helper()
				rs, err := conn.Query(ctx, q)
				if err != nil {
					t.Fatalf("%s: %v", q, err)
				}
				var got []int64
				for rs.Next() {
					var k int64
					if err := rs.Scan(&k); err != nil {
						t.Fatal(err)
					}
					got = append(got, k)
				}
				if err := rs.Err(); err != nil {
					t.Fatalf("%s: %v", q, err)
				}
				if fmt.Sprint(got) != fmt.Sprint(want) {
					t.Fatalf("%s: got %v, want %v", q, got, want)
				}
			}
			seq := func(from, to, step int64) []int64 {
				var out []int64
				for k := from; (step > 0 && k < to) || (step < 0 && k > to); k += step {
					out = append(out, k)
				}
				return out
			}
			check(`SELECT k FROM big WHERE k >= 1000 AND k < 1300`, seq(1000, 1300, 1))
			check(`SELECT k FROM big WHERE k >= 1000 AND k < 1300 AND k % 7 = 0`, func() []int64 {
				var out []int64
				for k := int64(1000); k < 1300; k++ {
					if k%7 == 0 {
						out = append(out, k)
					}
				}
				return out
			}())
			check(`SELECT k FROM big ORDER BY k DESC LIMIT 10 OFFSET 5`, seq(rows-6, rows-16, -1))
			check(`SELECT k FROM big LIMIT 3 OFFSET 29998`, []int64{29998, 29999})
			check(`SELECT k * 2 FROM big WHERE k < 4`, []int64{0, 2, 4, 6})
			check(`SELECT k FROM big WHERE g = 7 AND k < 100`, seq(7, 100, 10))
			// Sorted by a non-key column, aggregated, distinct: materialized.
			check(`SELECT k FROM big WHERE k < 20 ORDER BY g, k`, func() []int64 {
				var out []int64
				for g := int64(0); g < 10; g++ {
					out = append(out, g, g+10)
				}
				return out
			}())
			check(`SELECT count(*) FROM big WHERE g = 3`, []int64{rows / 10})
			check(`SELECT DISTINCT g FROM big WHERE k < 5`, []int64{0, 1, 2, 3, 4})
			// Scan-shaped selects underneath a statement materialize for
			// it: derived tables, WITH members, set members, subqueries,
			// INSERT ... SELECT.
			check(`SELECT k FROM (SELECT k FROM big WHERE k < 3) AS d`, []int64{0, 1, 2})
			check(`WITH w AS (SELECT k FROM big WHERE k < 3) SELECT k FROM w`, []int64{0, 1, 2})
			check(`SELECT k FROM big WHERE k < 2 UNION ALL SELECT k FROM big WHERE k >= 2 AND k < 4`, []int64{0, 1, 2, 3})
			check(`SELECT k FROM big WHERE k IN (SELECT k FROM big WHERE k < 3)`, []int64{0, 1, 2})
			check(`SELECT (SELECT max(k) FROM big WHERE k < 3)`, []int64{2})
			if _, err := conn.Exec(ctx, `CREATE TABLE IF NOT EXISTS copy_of_big (k INT8 PRIMARY KEY)`); err != nil {
				t.Fatal(err)
			}
			if _, err := conn.Exec(ctx, `DELETE FROM copy_of_big`); err != nil {
				t.Fatal(err)
			}
			if tag, err := conn.Exec(ctx, `INSERT INTO copy_of_big SELECT k FROM big WHERE k < 700`); err != nil || tag.RowsAffected() != 700 {
				t.Fatalf("INSERT ... SELECT: %v %v", tag, err)
			}
			check(`SELECT count(*) FROM copy_of_big`, []int64{700})

			// A streamed SELECT inside a transaction block sees the
			// block's own writes and leaves it usable.
			tx, err := conn.Begin(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := tx.Exec(ctx, `INSERT INTO big VALUES (-1, 0, 'new')`); err != nil {
				t.Fatal(err)
			}
			var cnt int64
			rs, err = tx.Query(ctx, `SELECT k FROM big WHERE k < 3`)
			if err != nil {
				t.Fatal(err)
			}
			for rs.Next() {
				cnt++
			}
			if err := rs.Err(); err != nil {
				t.Fatal(err)
			}
			if cnt != 4 {
				t.Fatalf("rows inside the block: %d, want 4", cnt)
			}
			if err := tx.Rollback(ctx); err != nil {
				t.Fatal(err)
			}
			// EXPLAIN ANALYZE still accounts the scan (materialized under it).
			rs, err = conn.Query(ctx, `EXPLAIN ANALYZE SELECT k FROM big WHERE k < 100`)
			if err != nil {
				t.Fatal(err)
			}
			var lines []string
			for rs.Next() {
				var l string
				if err := rs.Scan(&l); err != nil {
					t.Fatal(err)
				}
				lines = append(lines, l)
			}
			if err := rs.Err(); err != nil {
				t.Fatal(err)
			}
			if !strings.Contains(strings.Join(lines, "\n"), "100 rows") {
				t.Fatalf("EXPLAIN ANALYZE: %q", lines)
			}
		})
	}
}

// TestStreamingPortal: a streamed portal serves MaxRows rows per Execute
// across page boundaries, suspends in between, completes, and a
// completed portal stays completed; Close and Sync release it.
func TestStreamingPortal(t *testing.T) {
	tc := Start(t, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	root, err := pgx.Connect(ctx, pgURL(tc, 0))
	if err != nil {
		t.Fatal(err)
	}
	fillBig(t, ctx, root, 1300, 20)
	_ = root.Close(ctx)

	r := dialRaw(t, tc)
	r.send(
		&pgproto3.Parse{Name: "st", Query: "SELECT k FROM big"},
		&pgproto3.Bind{DestinationPortal: "pt", PreparedStatement: "st"},
		&pgproto3.Execute{Portal: "pt", MaxRows: 600},
		&pgproto3.Flush{},
	)
	expect[*pgproto3.ParseComplete](r)
	expect[*pgproto3.BindComplete](r)
	next := int64(0)
	readRows := func(n int) {
		t.Helper()
		for i := 0; i < n; i++ {
			dr := expect[*pgproto3.DataRow](r)
			if string(dr.Values[0]) != fmt.Sprint(next) {
				t.Fatalf("row: %s, want %d", dr.Values[0], next)
			}
			next++
		}
	}
	readRows(600)
	expect[*pgproto3.PortalSuspended](r)
	r.send(&pgproto3.Execute{Portal: "pt", MaxRows: 600}, &pgproto3.Flush{})
	readRows(600)
	expect[*pgproto3.PortalSuspended](r)
	r.send(&pgproto3.Execute{Portal: "pt", MaxRows: 600}, &pgproto3.Flush{})
	readRows(100)
	if cc := expect[*pgproto3.CommandComplete](r); string(cc.CommandTag) != "SELECT 1300" {
		t.Fatalf("tag %q", cc.CommandTag)
	}
	r.send(&pgproto3.Execute{Portal: "pt", MaxRows: 600}, &pgproto3.Sync{})
	expect[*pgproto3.CommandComplete](r)
	expect[*pgproto3.ReadyForQuery](r)

	// A suspended portal closed explicitly, then one released by Sync;
	// the connection is fine afterwards.
	r.send(
		&pgproto3.Bind{DestinationPortal: "p2", PreparedStatement: "st"},
		&pgproto3.Execute{Portal: "p2", MaxRows: 5},
		&pgproto3.Close{ObjectType: 'P', Name: "p2"},
		&pgproto3.Bind{DestinationPortal: "p3", PreparedStatement: "st"},
		&pgproto3.Execute{Portal: "p3", MaxRows: 5},
		&pgproto3.Sync{},
	)
	expect[*pgproto3.BindComplete](r)
	for i := 0; i < 5; i++ {
		expect[*pgproto3.DataRow](r)
	}
	expect[*pgproto3.PortalSuspended](r)
	expect[*pgproto3.CloseComplete](r)
	expect[*pgproto3.BindComplete](r)
	for i := 0; i < 5; i++ {
		expect[*pgproto3.DataRow](r)
	}
	expect[*pgproto3.PortalSuspended](r)
	if rq := expect[*pgproto3.ReadyForQuery](r); rq.TxStatus != 'I' {
		t.Fatalf("tx status %c, want I", rq.TxStatus)
	}
	r.send(&pgproto3.Query{String: "SELECT count(*) FROM big"})
	expect[*pgproto3.RowDescription](r)
	if dr := expect[*pgproto3.DataRow](r); string(dr.Values[0]) != "1300" {
		t.Fatalf("count %s", dr.Values[0])
	}
	expect[*pgproto3.CommandComplete](r)
	expect[*pgproto3.ReadyForQuery](r)
}

// TestStreamingErrors: an error after rows have been sent reaches the
// client after those rows (a division by zero on a later row, a
// cancellation, a statement timeout); the connection stays usable, an
// implicit transaction is gone, and an explicit block is failed.
func TestStreamingErrors(t *testing.T) {
	tc := Start(t, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	// The cancel-request context handler (what pools and psql do) sends
	// a CancelRequest and waits for the server's answer instead of
	// closing the connection.
	cfg, err := pgx.ParseConfig(pgURL(tc, 0))
	if err != nil {
		t.Fatal(err)
	}
	cfg.BuildContextWatcherHandler = func(pc *pgconn.PgConn) ctxwatch.Handler {
		return &pgconn.CancelRequestContextWatcherHandler{Conn: pc, CancelRequestDelay: 50 * time.Millisecond, DeadlineDelay: 10 * time.Second}
	}
	conn, err := pgx.ConnectConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close(ctx) }()
	const rows = 3000
	fillBig(t, ctx, conn, rows, 300)

	drain := func(ctx context.Context, q string) (int64, error) {
		rs, err := conn.Query(ctx, q)
		if err != nil {
			return 0, err
		}
		var n int64
		for rs.Next() {
			n++
		}
		return n, rs.Err()
	}
	usable := func() {
		t.Helper()
		var one int64
		if err := conn.QueryRow(ctx, `SELECT 1`).Scan(&one); err != nil || one != 1 {
			t.Fatalf("connection after the error: %v", err)
		}
	}

	// A late evaluation error: rows first, then 22012.
	n, err := drain(ctx, `SELECT k, 1 / (k - 2500) FROM big`)
	if pgErrCode(err) != sql.CodeDivisionByZero {
		t.Fatalf("late error: %v (after %d rows)", err, n)
	}
	if n == 0 || n > 2500 {
		t.Fatalf("rows before the error: %d", n)
	}
	usable()

	// Inside a block the error fails the block.
	tx, err := conn.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	rs, err := tx.Query(ctx, `SELECT k, 1 / (k - 2500) FROM big`)
	if err != nil {
		t.Fatal(err)
	}
	for rs.Next() {
	}
	if pgErrCode(rs.Err()) != sql.CodeDivisionByZero {
		t.Fatalf("in block: %v", rs.Err())
	}
	if _, err := tx.Exec(ctx, `SELECT 1`); pgErrCode(err) != sql.CodeInFailedTransaction {
		t.Fatalf("after the error in the block: %v", err)
	}
	if err := tx.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	usable()

	// Cancellation mid-stream: the hook parks the stream at row 100 until
	// the statement is cancelled.
	sql.TestingStreamHook = func(ctx context.Context, rows int64) error {
		if rows == 100 {
			<-ctx.Done()
			return ctx.Err()
		}
		return nil
	}
	defer func() { sql.TestingStreamHook = nil }()
	cctx, ccancel := context.WithTimeout(ctx, 500*time.Millisecond)
	start := time.Now()
	n, err = drain(cctx, `SELECT k FROM big`)
	ccancel()
	if pgErrCode(err) != sql.CodeQueryCanceled || time.Since(start) > 10*time.Second {
		t.Fatalf("cancelled stream: %v after %s (%d rows)", err, time.Since(start), n)
	}
	usable()

	// statement_timeout applies to the pull too.
	if _, err := conn.Exec(ctx, `SET statement_timeout = '300ms'`); err != nil {
		t.Fatal(err)
	}
	start = time.Now()
	_, err = drain(ctx, `SELECT k FROM big`)
	if pgErrCode(err) != sql.CodeQueryCanceled || !strings.Contains(err.Error(), "statement timeout") || time.Since(start) > 10*time.Second {
		t.Fatalf("timed-out stream: %v after %s", err, time.Since(start))
	}
	if _, err := conn.Exec(ctx, `RESET statement_timeout`); err != nil {
		t.Fatal(err)
	}
	sql.TestingStreamHook = nil
	usable()
}

// TestStreamingRetryRule: a retryable error before anything has been
// flushed re-runs the statement invisibly; one after the first flush
// surfaces as 40001 after the rows already sent.
func TestStreamingRetryRule(t *testing.T) {
	tc := Start(t, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	conn, err := pgx.Connect(ctx, pgURL(tc, 0))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close(ctx) }()
	const rows = 3000
	fillBig(t, ctx, conn, rows, 300)

	retryAt := func(at int64) {
		fired := false
		sql.TestingStreamHook = func(ctx context.Context, rows int64) error {
			if rows == at && !fired {
				fired = true
				return &kvclient.RetryableError{Cause: kvpb.NewErrorf("injected")}
			}
			return nil
		}
	}
	defer func() { sql.TestingStreamHook = nil }()
	count := func() (int64, error) {
		rs, err := conn.Query(ctx, `SELECT k, pad FROM big`)
		if err != nil {
			return 0, err
		}
		var n int64
		for rs.Next() {
			n++
		}
		return n, rs.Err()
	}

	// Row 10 is well inside the first 64 KB: restarted, full answer.
	restartsBefore := testutil.ToFloat64(metrics.SQLStreamRestarts)
	retryAt(10)
	if n, err := count(); err != nil || n != rows {
		t.Fatalf("early retry: %d rows, %v", n, err)
	}
	if d := testutil.ToFloat64(metrics.SQLStreamRestarts) - restartsBefore; d != 1 {
		t.Fatalf("restarts: %.0f, want 1", d)
	}
	// Row 2000 (~600 KB in) is past several flushes: surfaced.
	retryAt(2000)
	n, err := count()
	if pgErrCode(err) != sql.CodeSerializationFailure {
		t.Fatalf("late retry: %v after %d rows", err, n)
	}
	if n == 0 || n >= 2000 {
		t.Fatalf("rows before the surfaced error: %d", n)
	}
	sql.TestingStreamHook = nil
	if n, err := count(); err != nil || n != rows {
		t.Fatalf("after: %d rows, %v", n, err)
	}
}

// TestStatementMemoryLimit: a sort over more rows than
// statement_memory_limit allows fails with 53200 while the same table
// streams under the limit; the setting is shown, set, reset and
// validated.
func TestStatementMemoryLimit(t *testing.T) {
	tc := Start(t, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	conn, err := pgx.Connect(ctx, pgURL(tc, 0))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close(ctx) }()
	const rows = 6000
	fillBig(t, ctx, conn, rows, 300)

	var shown string
	if err := conn.QueryRow(ctx, `SHOW statement_memory_limit`).Scan(&shown); err != nil || shown != "67108864" {
		t.Fatalf("default: %q %v", shown, err)
	}
	if _, err := conn.Exec(ctx, `SET statement_memory_limit = '1MB'`); err != nil {
		t.Fatal(err)
	}
	if err := conn.QueryRow(ctx, `SHOW statement_memory_limit`).Scan(&shown); err != nil || shown != "1048576" {
		t.Fatalf("set: %q %v", shown, err)
	}
	count := func(q string) (int64, error) {
		rs, err := conn.Query(ctx, q)
		if err != nil {
			return 0, err
		}
		var n int64
		for rs.Next() {
			n++
		}
		return n, rs.Err()
	}
	hitsBefore := testutil.ToFloat64(metrics.SQLMemoryLimitHits)
	if n, err := count(`SELECT k FROM big ORDER BY pad, k`); pgErrCode(err) != sql.CodeOutOfMemory {
		t.Fatalf("sort under a 1MB limit: %d rows, %v", n, err)
	} else if !strings.Contains(err.Error(), "statement memory limit of 1MB exceeded") {
		t.Fatalf("message: %v", err)
	}
	if n, err := count(`SELECT count(*) FROM big`); pgErrCode(err) != sql.CodeOutOfMemory {
		t.Fatalf("aggregate under a 1MB limit: %d rows, %v", n, err)
	}
	if d := testutil.ToFloat64(metrics.SQLMemoryLimitHits) - hitsBefore; d != 2 {
		t.Fatalf("limit hits: %.0f, want 2", d)
	}
	// The same rows stream under the limit.
	if n, err := count(`SELECT k, pad FROM big`); err != nil || n != rows {
		t.Fatalf("streamed under the limit: %d rows, %v", n, err)
	}
	if n, err := count(`SELECT k FROM big WHERE k < 500 ORDER BY pad, k`); err != nil || n != 500 {
		t.Fatalf("small sort under the limit: %d rows, %v", n, err)
	}
	if _, err := conn.Exec(ctx, `SET statement_memory_limit = 0`); err != nil {
		t.Fatal(err)
	}
	if n, err := count(`SELECT k FROM big ORDER BY pad, k`); err != nil || n != rows {
		t.Fatalf("unlimited: %d rows, %v", n, err)
	}
	if _, err := conn.Exec(ctx, `RESET statement_memory_limit`); err != nil {
		t.Fatal(err)
	}
	if err := conn.QueryRow(ctx, `SHOW statement_memory_limit`).Scan(&shown); err != nil || shown != "67108864" {
		t.Fatalf("reset: %q %v", shown, err)
	}
	if _, err := conn.Exec(ctx, `SET statement_memory_limit = 'lots'`); pgErrCode(err) != sql.CodeInvalidParameterValue {
		t.Fatalf("invalid value: %v", err)
	}
	// SET LOCAL scopes it to the block.
	tx, err := conn.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, `SET LOCAL statement_memory_limit = '64kB'`); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, `SELECT k FROM big ORDER BY pad, k`); pgErrCode(err) != sql.CodeOutOfMemory {
		t.Fatalf("SET LOCAL: %v", err)
	}
	_ = tx.Rollback(ctx)
	if n, err := count(`SELECT k FROM big ORDER BY pad, k`); err != nil || n != rows {
		t.Fatalf("after the block: %d rows, %v", n, err)
	}
}
