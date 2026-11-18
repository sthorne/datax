package testcluster

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// TestSessionFunctionsNested (PG-F-018): a session function whose
// argument is another session function, or a subquery, resolves
// innermost first — pg_cancel_backend(pg_backend_pid()) and its kin
// used to fail with 42883 for the inner call.
func TestSessionFunctionsNested(t *testing.T) {
	tc := Start(t, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	cfg, err := pgx.ParseConfig(pgURL(tc, 0))
	if err != nil {
		t.Fatal(err)
	}
	cfg.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol // text results
	conn, err := pgx.ConnectConfig(ctx, cfg)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer conn.Close(ctx)

	// Cancelling one's own backend: PostgreSQL may report the cancel
	// (57014) or true; either way the inner call resolved.
	var ok bool
	err = conn.QueryRow(ctx, `SELECT pg_cancel_backend(pg_backend_pid())`).Scan(&ok)
	var pgErr *pgconn.PgError
	if err != nil && !(errors.As(err, &pgErr) && pgErr.Code == "57014") {
		t.Fatalf("pg_cancel_backend(pg_backend_pid()): %v", err)
	}
	if err == nil && !ok {
		t.Fatal("pg_cancel_backend(pg_backend_pid()) = false for the session's own pid")
	}

	for q, want := range map[string]string{
		`SELECT pg_cancel_backend(pg_backend_pid() + 100000)`:                          "f",
		`SELECT pg_terminate_backend(pg_backend_pid() + 100000)`:                       "f",
		`SELECT set_config('app.x', current_user, false), current_setting('app.x')`:    "root|root",
		`SELECT set_config('app.y', (SELECT 'v' || 'w'), false)`:                       "vw",
		`SELECT current_setting(current_setting('app.z', true) || 'application_name')`: "NULL",
		`SELECT pg_notify(current_user, version()) IS NOT NULL`:                        "t",
		`SELECT set_config('app.seq', 'nest_seq', false)`:                              "nest_seq",
	} {
		if q == `SELECT set_config('app.seq', 'nest_seq', false)` {
			if _, err := conn.Exec(ctx, `CREATE SEQUENCE nest_seq`); err != nil {
				t.Fatal(err)
			}
		}
		got, err := queryCells(t, ctx, conn, q)
		if err != nil || got != want {
			t.Errorf("%s = %q, %v; want %q", q, got, err, want)
		}
	}
	if got, err := queryCells(t, ctx, conn, `SELECT nextval(current_setting('app.seq'))`); err != nil || got != "1" {
		t.Errorf("nextval(current_setting(...)) = %q, %v", got, err)
	}
}
