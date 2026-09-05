package testcluster

import (
	"context"
	"errors"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgconn/ctxwatch"
	"github.com/jackc/pgx/v5/pgproto3"

	"github.com/sthorne/datax/pkg/sql"
	"github.com/sthorne/datax/pkg/sql/catalog"
	"github.com/sthorne/datax/pkg/sql/types"
)

func pgErrCode(err error) string {
	var pe *pgconn.PgError
	if errors.As(err, &pe) {
		return pe.Code
	}
	return ""
}

// TestSessionVariables (issue #97): SET / RESET / SHOW over every honored
// variable, SET LOCAL and SET TRANSACTION scoped to the block, RESET
// ALL, SHOW ALL and pg_settings, the refusals (42704, 22023), read-only
// transactions (25006), the time zone applied to TIMESTAMPTZ output on
// the wire, the reported parameters announced to the client, and
// pg_backend_pid / SHOW SESSIONS / pg_stat_activity.
func TestSessionVariables(t *testing.T) {
	tc := Start(t, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	s := sql.NewSession(tc.Nodes[0].DB(), catalog.NewAccessor())
	waitForDatabases(t, ctx, s)
	show := func(name string) string {
		t.Helper()
		r := execSQL(t, ctx, s, "SHOW "+name)
		return r.Rows[0][0].Text()
	}
	refused := func(stmt, code string) {
		t.Helper()
		if _, serr := trySQL(ctx, s, stmt); serr == nil || serr.Code != code {
			t.Fatalf("%s: %v, want %s", stmt, serr, code)
		}
	}

	for _, c := range []struct{ set, name, want string }{
		{`SET application_name = 'app1'`, "application_name", "app1"},
		{`SET application_name TO app2`, "application_name", "app2"},
		{`SET search_path = public, other`, "search_path", "public, other"},
		{`SET search_path TO "$user", public`, "search_path", "$user, public"},
		{`SET TIME ZONE 'America/New_York'`, "TimeZone", "America/New_York"},
		{`SET TimeZone = '+05:30'`, "timezone", "+05:30"},
		{`SET TIME ZONE 'UTC'`, "TIME ZONE", "UTC"},
		{`SET DateStyle = 'ISO, YMD'`, "DateStyle", "ISO, YMD"},
		{`SET DateStyle TO ISO`, "datestyle", "ISO"},
		{`SET client_encoding = 'UTF8'`, "client_encoding", "UTF8"},
		{`SET NAMES 'utf-8'`, "client_encoding", "UTF8"},
		{`SET statement_timeout = 2500`, "statement_timeout", "2500"},
		{`SET statement_timeout = '2s'`, "statement_timeout", "2000"},
		{`SET statement_timeout = '1min'`, "statement_timeout", "60000"},
		{`SET statement_timeout = 0`, "statement_timeout", "0"},
		{`SET lock_timeout = '250ms'`, "lock_timeout", "250"},
		{`SET idle_in_transaction_session_timeout = '3s'`, "idle_in_transaction_session_timeout", "3000"},
		{`SET default_transaction_read_only = on`, "default_transaction_read_only", "on"},
		{`SET default_transaction_read_only = off`, "default_transaction_read_only", "off"},
		{`SET transaction_isolation = 'read committed'`, "transaction_isolation", "serializable"},
		{`SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL REPEATABLE READ`, "TRANSACTION ISOLATION LEVEL", "serializable"},
		{`SET foreign_key_cascade_limit = 5`, "foreign_key_cascade_limit", "5"},
	} {
		execSQL(t, ctx, s, c.set)
		if got := show(c.name); got != c.want {
			t.Errorf("%s; SHOW %s = %q, want %q", c.set, c.name, got, c.want)
		}
	}
	refused(`SET nosuch = 1`, sql.CodeUndefinedObject)
	refused(`SHOW nosuch`, sql.CodeUndefinedObject)
	refused(`SET statement_timeout = 'soon'`, sql.CodeInvalidParameterValue)
	refused(`SET statement_timeout = -1`, sql.CodeInvalidParameterValue)
	refused(`SET TIME ZONE 'Mars/Olympus'`, sql.CodeInvalidParameterValue)
	refused(`SET DateStyle = 'German'`, sql.CodeInvalidParameterValue)
	refused(`SET client_encoding = 'LATIN1'`, sql.CodeInvalidParameterValue)
	refused(`SET transaction_isolation = 'chaotic'`, sql.CodeInvalidParameterValue)
	refused(`SET default_transaction_read_only = maybe`, sql.CodeInvalidParameterValue)
	refused(`SET server_version = '15'`, sql.CodeInvalidParameterValue)
	refused(`SET foreign_key_cascade_limit = 0`, sql.CodeInvalidParameterValue)

	// RESET one, RESET ALL, SHOW ALL and pg_settings agree.
	execSQL(t, ctx, s, `RESET statement_timeout`)
	if show("statement_timeout") != "0" {
		t.Fatal("RESET statement_timeout")
	}
	execSQL(t, ctx, s, `SET application_name = DEFAULT`)
	if show("application_name") != "" {
		t.Fatal("SET ... DEFAULT")
	}
	execSQL(t, ctx, s, `RESET ALL`)
	if show("lock_timeout") != "0" || show("search_path") != "public" || show("foreign_key_cascade_limit") == "5" {
		t.Fatal("RESET ALL")
	}
	all := execSQL(t, ctx, s, `SHOW ALL`)
	settings := execSQL(t, ctx, s, `SELECT name, setting, vartype, unit FROM pg_settings ORDER BY name`)
	if len(all.Rows) != len(settings.Rows) || len(all.Rows) < 15 {
		t.Fatalf("SHOW ALL %d rows, pg_settings %d", len(all.Rows), len(settings.Rows))
	}
	for _, r := range settings.Rows {
		if r[0].S == "statement_timeout" && (r[2].S != "integer" || r[3].S != "ms") {
			t.Fatalf("pg_settings statement_timeout: %v", r)
		}
	}
	if r := execSQL(t, ctx, s, `SELECT current_setting('search_path'), current_setting('TimeZone')`); r.Rows[0][0].S != "public" || r.Rows[0][1].S != "UTC" {
		t.Fatalf("current_setting: %v", r.Rows)
	}

	// SET LOCAL and SET TRANSACTION last for the block; a read-only
	// transaction refuses writes; default_transaction_read_only makes
	// every implicit statement read-only.
	execSQL(t, ctx, s, `CREATE TABLE kv (k INT8 PRIMARY KEY, v TEXT)`)
	execSQL(t, ctx, s, `BEGIN`)
	execSQL(t, ctx, s, `SET LOCAL statement_timeout = '7s'`)
	execSQL(t, ctx, s, `SET TRANSACTION READ ONLY`)
	if show("statement_timeout") != "7000" || show("transaction_read_only") != "on" {
		t.Fatal("SET LOCAL / SET TRANSACTION inside the block")
	}
	refused(`INSERT INTO kv VALUES (1, 'x')`, sql.CodeReadOnlyTransaction)
	execSQL(t, ctx, s, `ROLLBACK`)
	if show("statement_timeout") != "0" || show("transaction_read_only") != "off" {
		t.Fatal("the block's end restores the session's values")
	}
	execSQL(t, ctx, s, `SET LOCAL statement_timeout = '9s'`) // outside a block: no effect
	if show("statement_timeout") != "0" {
		t.Fatal("SET LOCAL outside a block")
	}
	execSQL(t, ctx, s, `SET default_transaction_read_only = on`)
	refused(`INSERT INTO kv VALUES (1, 'x')`, sql.CodeReadOnlyTransaction)
	refused(`CREATE TABLE nope (id INT8 PRIMARY KEY)`, sql.CodeReadOnlyTransaction)
	execSQL(t, ctx, s, `BEGIN`)
	refused(`DELETE FROM kv`, sql.CodeReadOnlyTransaction)
	execSQL(t, ctx, s, `SET TRANSACTION READ WRITE`)
	execSQL(t, ctx, s, `INSERT INTO kv VALUES (1, 'x')`)
	execSQL(t, ctx, s, `COMMIT`)
	execSQL(t, ctx, s, `SET default_transaction_read_only = off`)
	execSQL(t, ctx, s, `INSERT INTO kv VALUES (2, 'y')`)
	if r := execSQL(t, ctx, s, `SELECT count(*) FROM kv`); r.Rows[0][0].I != 2 {
		t.Fatal("rows")
	}

	// Over the wire: the reported parameters, the time zone applied to
	// TIMESTAMPTZ text output (binary stays UTC), pg_backend_pid, SHOW
	// SESSIONS and pg_stat_activity.
	cfg, _ := pgx.ParseConfig(pgURL(tc, 0))
	cfg.RuntimeParams["application_name"] = "vars-test"
	cfg.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol
	conn, err := pgx.ConnectConfig(ctx, cfg)
	if err != nil {
		t.Fatalf("pgx connect: %v", err)
	}
	defer func() { _ = conn.Close(ctx) }()
	var app string
	if err := conn.QueryRow(ctx, `SHOW application_name`).Scan(&app); err != nil || app != "vars-test" {
		t.Fatalf("startup application_name: %q %v", app, err)
	}
	if _, err := conn.Exec(ctx, `SET TIME ZONE 'America/New_York'`); err != nil {
		t.Fatal(err)
	}
	if got := conn.PgConn().ParameterStatus("TimeZone"); got != "America/New_York" {
		t.Fatalf("ParameterStatus TimeZone after SET: %q", got)
	}
	if _, err := conn.Exec(ctx, `SET application_name = 'renamed'`); err != nil {
		t.Fatal(err)
	}
	if got := conn.PgConn().ParameterStatus("application_name"); got != "renamed" {
		t.Fatalf("ParameterStatus application_name after SET: %q", got)
	}
	var rendered string
	if err := conn.QueryRow(ctx, `SELECT '2024-07-04 12:00:00Z'::timestamptz::text, '2024-07-04 12:00:00Z'::timestamptz`).Scan(&rendered, new(string)); err != nil {
		t.Fatal(err)
	}
	execSQL(t, ctx, s, `CREATE TABLE stamps (id INT8 PRIMARY KEY, plain TIMESTAMP)`)
	execSQL(t, ctx, s, `INSERT INTO stamps VALUES (1, '2024-01-04 12:00:00')`)
	rows, err := conn.PgConn().Exec(ctx, `SELECT '2024-07-04 12:00:00Z'::timestamptz, '2024-07-04 12:00:00Z'::timestamptz::text; SELECT plain FROM stamps WHERE id = 1`).ReadAll()
	if err != nil || len(rows) != 2 || len(rows[0].Rows) != 1 || len(rows[1].Rows) != 1 {
		t.Fatalf("raw select: %v", err)
	}
	if got := string(rows[0].Rows[0][0]); got != "2024-07-04 08:00:00-04" {
		t.Fatalf("TIMESTAMPTZ text in America/New_York: %q", got)
	}
	if got := string(rows[1].Rows[0][0]); got != "2024-01-04 12:00:00" {
		t.Fatalf("TIMESTAMP (without time zone) is unaffected: %q", got)
	}
	bcfg, _ := pgx.ParseConfig(pgURL(tc, 0))
	bconn, err := pgx.ConnectConfig(ctx, bcfg)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = bconn.Close(ctx) }()
	if _, err := bconn.Exec(ctx, `SET TIME ZONE 'Asia/Kolkata'`); err != nil {
		t.Fatal(err)
	}
	var at time.Time
	if err := bconn.QueryRow(ctx, `SELECT '2024-07-04 12:00:00Z'::timestamptz`).Scan(&at); err != nil || !at.Equal(time.Date(2024, 7, 4, 12, 0, 0, 0, time.UTC)) {
		t.Fatalf("binary TIMESTAMPTZ under a zone: %v %v", at, err)
	}
	var pid int32
	if err := bconn.QueryRow(ctx, `SELECT pg_backend_pid()`).Scan(&pid); err != nil || pid == 0 {
		t.Fatalf("pg_backend_pid: %d %v", pid, err)
	}
	var found, state, app2 string
	if err := bconn.QueryRow(ctx, `SELECT application_name, state, query FROM pg_stat_activity WHERE pid = $1`, pid).Scan(&app2, &state, &found); err != nil || state != "active" || !strings.Contains(found, "pg_stat_activity") {
		t.Fatalf("pg_stat_activity: %q %q %q %v", app2, state, found, err)
	}
	var n int
	if err := bconn.QueryRow(ctx, `SELECT count(*) FROM pg_stat_activity WHERE application_name = 'renamed' AND state = 'idle'`).Scan(&n); err != nil || n != 1 {
		t.Fatalf("the other connection in pg_stat_activity: %d %v", n, err)
	}
	srows, err := bconn.Query(ctx, `SHOW SESSIONS`)
	if err != nil {
		t.Fatal(err)
	}
	sessions, fields := 0, len(srows.FieldDescriptions())
	for srows.Next() {
		sessions++
	}
	srows.Close()
	if sessions < 2 || fields != 10 {
		t.Fatalf("SHOW SESSIONS: %d rows, %d columns", sessions, fields)
	}
	if r := execSQL(t, ctx, s, `SHOW SESSIONS`); len(r.Rows) != 0 {
		t.Fatal("an internal session lists no wire sessions")
	}
	_ = rendered
}

// TestCancelAndTimeouts (issue #97): a CancelRequest (pgx's context
// cancellation) stops a running statement with 57014 and rolls its
// transaction back so another session's conflicting write proceeds, the
// connection stays usable; statement_timeout expires with 57014;
// lock_timeout fails a wait on a live intent with 55P03; the
// idle-in-transaction timeout ends the connection with 25P03 and
// releases its intents; pg_cancel_backend / pg_terminate_backend from an
// admin; a cross-node cancel through a second node.
func TestCancelAndTimeouts(t *testing.T) {
	tc := Start(t, 2)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	s := sql.NewSession(tc.Nodes[0].DB(), catalog.NewAccessor())
	waitForDatabases(t, ctx, s)
	execSQL(t, ctx, s, `CREATE TABLE kv (k INT8 PRIMARY KEY, v TEXT)`)

	// pgx's default context handler closes the connection on a
	// cancelled context; the cancel-request handler sends a
	// CancelRequest and waits for the server's answer, which is what
	// pools and psql do.
	connect := func() *pgx.Conn {
		t.Helper()
		cfg, _ := pgx.ParseConfig(pgURL(tc, 0))
		cfg.BuildContextWatcherHandler = func(pc *pgconn.PgConn) ctxwatch.Handler {
			return &pgconn.CancelRequestContextWatcherHandler{Conn: pc, CancelRequestDelay: 50 * time.Millisecond, DeadlineDelay: 10 * time.Second}
		}
		c, err := pgx.ConnectConfig(ctx, cfg)
		if err != nil {
			t.Fatalf("pgx connect: %v", err)
		}
		return c
	}
	conn := connect()
	defer func() { _ = conn.Close(ctx) }()
	other := connect()
	defer func() { _ = other.Close(ctx) }()

	// Context cancellation: pgx sends a CancelRequest on a fresh
	// connection; the statement fails with 57014, the transaction's
	// intent is gone (the other session's write proceeds at once), and
	// the connection serves the next statement.
	tx, err := conn.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, `INSERT INTO kv VALUES (1, 'held')`); err != nil {
		t.Fatal(err)
	}
	cctx, ccancel := context.WithTimeout(ctx, 300*time.Millisecond)
	start := time.Now()
	_, err = tx.Exec(cctx, `SELECT pg_sleep(20)`)
	ccancel()
	if pgErrCode(err) != sql.CodeQueryCanceled || time.Since(start) > 5*time.Second {
		t.Fatalf("cancelled pg_sleep: %v after %s", err, time.Since(start))
	}
	if err := tx.Rollback(ctx); err != nil {
		t.Fatalf("rollback after cancel: %v", err)
	}
	wctx, wcancel := context.WithTimeout(ctx, 5*time.Second)
	if _, err := other.Exec(wctx, `INSERT INTO kv VALUES (1, 'other')`); err != nil {
		t.Fatalf("the cancelled transaction's intent should be gone: %v", err)
	}
	wcancel()
	var v string
	if err := conn.QueryRow(ctx, `SELECT v FROM kv WHERE k = 1`).Scan(&v); err != nil || v != "other" {
		t.Fatalf("connection after cancel: %q %v", v, err)
	}

	// statement_timeout.
	if _, err := conn.Exec(ctx, `SET statement_timeout = '200ms'`); err != nil {
		t.Fatal(err)
	}
	start = time.Now()
	if _, err := conn.Exec(ctx, `SELECT pg_sleep(10)`); pgErrCode(err) != sql.CodeQueryCanceled || !strings.Contains(err.Error(), "statement timeout") || time.Since(start) > 5*time.Second {
		t.Fatalf("statement_timeout: %v after %s", err, time.Since(start))
	}
	if _, err := conn.Exec(ctx, `RESET statement_timeout`); err != nil {
		t.Fatal(err)
	}

	// lock_timeout: a wait on a live intent fails with 55P03 instead of
	// waiting the conflict budget out.
	tx, err = other.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, `UPDATE kv SET v = 'locked' WHERE k = 1`); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Exec(ctx, `SET lock_timeout = '300ms'`); err != nil {
		t.Fatal(err)
	}
	start = time.Now()
	if _, err := conn.Exec(ctx, `UPDATE kv SET v = 'mine' WHERE k = 1`); pgErrCode(err) != sql.CodeLockNotAvailable || time.Since(start) > 5*time.Second {
		t.Fatalf("lock_timeout: %v after %s", err, time.Since(start))
	}
	if err := tx.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Exec(ctx, `RESET lock_timeout`); err != nil {
		t.Fatal(err)
	}

	// idle_in_transaction_session_timeout: the idle block's connection is
	// ended with 25P03 and its intent released.
	idle, err := pgx.Connect(ctx, pgURL(tc, 0))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := idle.Exec(ctx, `SET idle_in_transaction_session_timeout = '500ms'`); err != nil {
		t.Fatal(err)
	}
	itx, err := idle.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := itx.Exec(ctx, `INSERT INTO kv VALUES (2, 'idle')`); err != nil {
		t.Fatal(err)
	}
	wctx, wcancel = context.WithTimeout(ctx, 10*time.Second)
	if _, err := conn.Exec(wctx, `INSERT INTO kv VALUES (2, 'proceeds')`); err != nil {
		t.Fatalf("the idle transaction's intent should be released by the timeout: %v", err)
	}
	wcancel()
	_, err = itx.Exec(ctx, `SELECT 1`)
	if err == nil || (pgErrCode(err) != sql.CodeIdleInTransactionTimeout && !idle.IsClosed()) {
		t.Fatalf("idle connection after the timeout: %v (closed %v)", err, idle.IsClosed())
	}
	_ = idle.Close(ctx)

	// pg_cancel_backend from an admin session; pg_terminate_backend ends
	// the connection.
	var pid int32
	if err := other.QueryRow(ctx, `SELECT pg_backend_pid()`).Scan(&pid); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		_, err := other.Exec(ctx, `SELECT pg_sleep(20)`)
		done <- err
	}()
	time.Sleep(200 * time.Millisecond)
	// The admin functions run over the wire (an internal session has no
	// connection registry).
	var ok bool
	if err := conn.QueryRow(ctx, `SELECT pg_cancel_backend($1)`, pid).Scan(&ok); err != nil || !ok {
		t.Fatalf("pg_cancel_backend: %v %v", ok, err)
	}
	select {
	case err := <-done:
		if pgErrCode(err) != sql.CodeQueryCanceled {
			t.Fatalf("pg_cancel_backend outcome: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("pg_cancel_backend did not stop pg_sleep")
	}
	if err := conn.QueryRow(ctx, `SELECT pg_cancel_backend(123456789)`).Scan(&ok); err != nil || ok {
		t.Fatalf("cancel of an unknown pid: %v %v", ok, err)
	}
	if err := conn.QueryRow(ctx, `SELECT pg_terminate_backend($1)`, pid).Scan(&ok); err != nil || !ok {
		t.Fatalf("pg_terminate_backend: %v %v", ok, err)
	}
	if r := execSQL(t, ctx, s, `SELECT pg_cancel_backend($1)`, types.NewInt(int64(pid))); r.Rows[0][0].B {
		t.Fatal("an internal session has no registry: false")
	}
	deadline := time.Now().Add(5 * time.Second)
	for !other.IsClosed() && time.Now().Before(deadline) {
		_, _ = other.Exec(ctx, `SELECT 1`)
		time.Sleep(50 * time.Millisecond)
	}
	if !other.IsClosed() {
		t.Fatal("pg_terminate_backend did not end the connection")
	}

	// Cross-node: a CancelRequest carrying node 1's key pair that lands
	// on node 2 (a load balancer) is forwarded and cancels the statement.
	conn2 := connect()
	defer func() { _ = conn2.Close(ctx) }()
	pid2, secret := conn2.PgConn().PID(), conn2.PgConn().SecretKey()
	done = make(chan error, 1)
	go func() {
		_, err := conn2.Exec(ctx, `SELECT pg_sleep(20)`)
		done <- err
	}()
	time.Sleep(200 * time.Millisecond)
	raw, err := net.Dial("tcp", tc.Nodes[1].SQLAddr())
	if err != nil {
		t.Fatal(err)
	}
	fe := pgproto3.NewFrontend(raw, raw)
	fe.Send(&pgproto3.CancelRequest{ProcessID: pid2, SecretKey: secret})
	if err := fe.Flush(); err != nil {
		t.Fatal(err)
	}
	_ = raw.Close()
	select {
	case err := <-done:
		if pgErrCode(err) != sql.CodeQueryCanceled {
			t.Fatalf("cross-node cancel outcome: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("cross-node cancel did not stop pg_sleep")
	}
	if pid2>>20 != 1 {
		t.Fatalf("pid %d should carry node 1 in its high bits", pid2)
	}
}
