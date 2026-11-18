package testcluster

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
)

// TestDiscard covers DISCARD (gap PG-Q-024 of
// docs/compatibility-gaps.md), the reset query of connection poolers,
// over the wire: DISCARD ALL closes cursors, resets every setting (and
// reports the reported ones changed), deallocates the SQL-level and the
// named protocol-level prepared statements, releases the session's
// advisory locks and stops its LISTENs, and is refused inside a
// transaction block (25001); DISCARD PLANS, SEQUENCES and TEMP.
func TestDiscard(t *testing.T) {
	tc := Start(t, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	cfg, err := pgconn.ParseConfig(pgURL(tc, 0))
	if err != nil {
		t.Fatal(err)
	}
	var notified atomic.Int32
	cfg.OnNotification = func(*pgconn.PgConn, *pgconn.Notification) { notified.Add(1) }
	pc, err := pgconn.ConnectConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = pc.Close(context.Background()) }()
	other := pgConn(t, ctx, tc, 0).PgConn()
	run := func(c *pgconn.PgConn, q string) ([]*pgconn.Result, error) {
		return c.Exec(ctx, q).ReadAll()
	}
	must := func(c *pgconn.PgConn, q string) []*pgconn.Result {
		t.Helper()
		res, err := run(c, q)
		if err != nil {
			t.Fatalf("%s: %v", q, err)
		}
		return res
	}
	one := func(c *pgconn.PgConn, q string) string {
		t.Helper()
		res := must(c, q)
		if len(res) != 1 || len(res[0].Rows) != 1 {
			t.Fatalf("%s: %d results", q, len(res))
		}
		return string(res[0].Rows[0][0])
	}
	code := func(err error) string {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) {
			return pgErr.Code
		}
		return ""
	}
	fails := func(c *pgconn.PgConn, q, want string) {
		t.Helper()
		if _, err := run(c, q); code(err) != want {
			t.Fatalf("%s: got %v, want SQLSTATE %s", q, err, want)
		}
	}

	must(pc, `CREATE SEQUENCE ds`)
	must(pc, `SET application_name = 'pooled'`)
	must(pc, `SET TimeZone = 'Asia/Tokyo'`)
	must(pc, `SET statement_timeout = '5s'`)
	if got := pc.ParameterStatus("application_name"); got != "pooled" {
		t.Fatalf("application_name reported %q", got)
	}
	if _, err := pc.Prepare(ctx, "proto_ps", "SELECT 1", nil); err != nil {
		t.Fatal(err)
	}
	must(pc, `PREPARE sql_ps AS SELECT 2`)
	must(pc, `BEGIN`)
	must(pc, `DECLARE held CURSOR WITH HOLD FOR SELECT 1`)
	must(pc, `COMMIT`)
	must(pc, `SELECT pg_advisory_lock(4242)`)
	must(pc, `LISTEN dchan`)
	// notifiedWithin polls until pc has seen n notifications in all.
	notifiedWithin := func(n int32, d time.Duration) bool {
		for end := time.Now().Add(d); time.Now().Before(end); time.Sleep(20 * time.Millisecond) {
			must(pc, `SELECT 1`)
			if notified.Load() >= n {
				return true
			}
		}
		return false
	}
	must(other, `NOTIFY dchan, 'before'`)
	if !notifiedWithin(1, 5*time.Second) {
		t.Fatal("LISTEN: no notification before DISCARD ALL")
	}
	must(pc, `SELECT nextval('ds')`)
	if got := one(other, `SELECT pg_try_advisory_lock(4242)`); got != "f" {
		t.Fatalf("lock not held before DISCARD: %s", got)
	}

	// Refused inside a transaction block, which it fails.
	must(pc, `BEGIN`)
	fails(pc, `DISCARD ALL`, "25001")
	must(pc, `ROLLBACK`)

	res := must(pc, `DISCARD ALL`)
	if tag := res[0].CommandTag.String(); tag != "DISCARD ALL" {
		t.Fatalf("tag %q", tag)
	}
	// Settings are back to their defaults, the reported ones announced.
	if got := pc.ParameterStatus("application_name"); got != "" {
		t.Fatalf("application_name reported %q after DISCARD ALL", got)
	}
	if got := pc.ParameterStatus("TimeZone"); got != "UTC" {
		t.Fatalf("TimeZone reported %q after DISCARD ALL", got)
	}
	if got := one(pc, `SHOW statement_timeout`); got != "0" {
		t.Fatalf("statement_timeout %q after DISCARD ALL", got)
	}
	// Prepared statements of both kinds are gone, and the cursor.
	if err := pc.ExecPrepared(ctx, "proto_ps", nil, nil, nil).Read().Err; code(err) != "26000" {
		t.Fatalf("protocol statement after DISCARD ALL: %v", err)
	}
	fails(pc, `EXECUTE sql_ps`, "26000")
	fails(pc, `FETCH held`, "34000")
	// The advisory lock is released.
	if got := one(other, `SELECT pg_try_advisory_lock(4242)`); got != "t" {
		t.Fatalf("lock still held after DISCARD ALL: %s", got)
	}
	must(other, `SELECT pg_advisory_unlock_all()`)
	// No longer listening: a notification on the channel is not
	// delivered.
	must(other, `NOTIFY dchan, 'after'`)
	if notifiedWithin(2, time.Second) {
		t.Fatal("notification delivered after DISCARD ALL")
	}
	// currval / lastval state is gone.
	fails(pc, `SELECT currval('ds')`, "55000")
	// New statements prepare as usual.
	if _, err := pc.Prepare(ctx, "proto_ps", "SELECT 3", nil); err != nil {
		t.Fatal(err)
	}

	// DISCARD PLANS, SEQUENCES and TEMP run inside a block too.
	must(pc, `SELECT nextval('ds')`)
	must(pc, `BEGIN`)
	for _, c := range []struct{ q, tag string }{
		{`DISCARD PLANS`, "DISCARD PLANS"},
		{`DISCARD SEQUENCES`, "DISCARD SEQUENCES"},
		{`DISCARD TEMP`, "DISCARD TEMP"},
		{`DISCARD TEMPORARY`, "DISCARD TEMP"},
	} {
		if tag := must(pc, c.q)[0].CommandTag.String(); tag != c.tag {
			t.Fatalf("%s: tag %q", c.q, tag)
		}
	}
	must(pc, `COMMIT`)
	fails(pc, `SELECT lastval()`, "55000")
	fails(pc, `DISCARD everything`, "42601")
}
