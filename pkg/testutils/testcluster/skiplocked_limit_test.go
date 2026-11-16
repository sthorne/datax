package testcluster

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/sthorne/datax/pkg/sql"
)

// claimQueue sets up the job queue of issue #322 (items 15 and 16): three
// pending jobs under an index that serves the claim query's filter and
// order, oldest first 3, 2, 1, and an empty table of claims. It returns
// A's connection and its open transaction, which holds job 3 locked,
// and a second connection, B.
func claimQueue(t *testing.T, ctx context.Context) (tc *TestCluster, atx pgx.Tx, a, b *pgx.Conn) {
	t.Helper()
	tc = Start(t, 1)
	connect := func() *pgx.Conn {
		t.Helper()
		c, err := pgx.Connect(ctx, pgURL(tc, 0))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = c.Close(context.Background()) })
		return c
	}
	a, b = connect(), connect()
	for _, stmt := range []string{
		`CREATE TABLE jobs (id INT8 PRIMARY KEY, queue TEXT NOT NULL, state TEXT NOT NULL, visible_at TIMESTAMPTZ NOT NULL)`,
		`CREATE INDEX jobs_claim_idx ON jobs (queue, state, visible_at)`,
		`INSERT INTO jobs VALUES (1, 'q', 'pending', now() - interval '1 minute'), (2, 'q', 'pending', now() - interval '2 minute'), (3, 'q', 'pending', now() - interval '3 minute')`,
		`CREATE TABLE claims (id INT8 PRIMARY KEY)`,
	} {
		if _, err := a.Exec(ctx, stmt); err != nil {
			t.Fatalf("%s: %v", stmt, err)
		}
	}
	atx, err := a.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = atx.Rollback(context.Background()) })
	if ids, err := queryIDs(ctx, atx, `SELECT id FROM jobs WHERE id = 3 FOR UPDATE`); err != nil || fmt.Sprint(ids) != "[3]" {
		t.Fatalf("A's lock: %v, %v", ids, err)
	}
	return tc, atx, a, b
}

// promptly runs f in a transaction of c that is then rolled back, under
// a deadline far shorter than any lock wait on A (which never lets go
// until told to), and fails the test if f fails or takes as long as
// within.
func promptly(t *testing.T, ctx context.Context, c *pgx.Conn, what string, within time.Duration, f func(ctx context.Context, tx pgx.Tx) error) {
	t.Helper()
	sctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	start := time.Now()
	tx, err := c.Begin(sctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := f(sctx, tx); err != nil {
		t.Fatalf("%s: %v (after %s)", what, err, time.Since(start))
	}
	if took := time.Since(start); took >= within {
		t.Fatalf("%s took %s: it waited for the lock holder", what, took)
	}
}

const claimWhere = `queue = 'q' AND state = 'pending' AND visible_at <= now()`

// TestSkipLockedLimitPastIndexJoin (issue #322 item 15): a LIMIT pushed
// into an index scan that joins to the primary key counts the rows the
// statement returns, not the index entries it scanned. A row passed over
// as locked behind an entry used to consume the limit: the claim query
// returned nothing with LIMIT 1 and one row with LIMIT 2. The primary
// key scan and the in-memory sort give the same answers, and NOWAIT
// still fails on the locked row when the order reaches it.
func TestSkipLockedLimitPastIndexJoin(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	_, _, _, b := claimQueue(t, ctx)

	claim := `SELECT id FROM jobs WHERE ` + claimWhere + ` ORDER BY visible_at LIMIT 1 FOR UPDATE SKIP LOCKED`
	var plan strings.Builder
	rows, err := b.Query(ctx, `EXPLAIN `+claim)
	if err != nil {
		t.Fatal(err)
	}
	lines, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		t.Fatal(err)
	}
	plan.WriteString(strings.Join(lines, "\n"))
	if !strings.Contains(plan.String(), `scan of index "jobs_claim_idx"`) || !strings.Contains(plan.String(), "limit pushed into scan") {
		t.Fatalf("the claim query no longer runs as a limited index join, so this test checks nothing:\n%s", plan.String())
	}

	for _, q := range []struct{ query, want string }{
		// The index join, the limit pushed into its scan.
		{claim, "[2]"},
		{`SELECT id FROM jobs WHERE ` + claimWhere + ` ORDER BY visible_at LIMIT 2 FOR UPDATE SKIP LOCKED`, "[2 1]"},
		{`SELECT id FROM jobs WHERE ` + claimWhere + ` ORDER BY visible_at LIMIT 3 FOR UPDATE SKIP LOCKED`, "[2 1]"},
		{`SELECT id FROM jobs WHERE ` + claimWhere + ` ORDER BY visible_at LIMIT 1 OFFSET 1 FOR UPDATE SKIP LOCKED`, "[1]"},
		{`SELECT id FROM jobs WHERE ` + claimWhere + ` ORDER BY visible_at DESC LIMIT 1 FOR UPDATE SKIP LOCKED`, "[1]"},
		{`SELECT id FROM jobs WHERE queue = 'q' AND state = 'pending' LIMIT 1 FOR UPDATE SKIP LOCKED`, "[2]"},
		// The primary key: a range scan and a full scan with a filter.
		{`SELECT id FROM jobs WHERE id >= 2 ORDER BY id DESC LIMIT 1 FOR UPDATE SKIP LOCKED`, "[2]"},
		{`SELECT id FROM jobs WHERE id >= 2 ORDER BY id DESC LIMIT 2 FOR UPDATE SKIP LOCKED`, "[2]"},
		{`SELECT id FROM jobs WHERE state = 'pending' ORDER BY id DESC LIMIT 1 FOR UPDATE SKIP LOCKED`, "[2]"},
		{`SELECT id FROM jobs WHERE state = 'pending' ORDER BY id DESC LIMIT 2 FOR UPDATE SKIP LOCKED`, "[2 1]"},
		// Sorted in memory.
		{`SELECT id FROM jobs WHERE state = 'pending' ORDER BY visible_at LIMIT 1 FOR UPDATE SKIP LOCKED`, "[2]"},
	} {
		promptly(t, ctx, b, q.query, 2*time.Second, func(sctx context.Context, tx pgx.Tx) error {
			ids, err := queryIDs(sctx, tx, q.query)
			if err != nil {
				return err
			}
			if got := fmt.Sprint(ids); got != q.want {
				return fmt.Errorf("returned %s, want %s", got, q.want)
			}
			return nil
		})
	}

	// NOWAIT fails on the locked row when the scan reaches it, through
	// the index join and the primary key alike, and does not when the
	// limit is met first.
	for _, query := range []string{
		`SELECT id FROM jobs WHERE ` + claimWhere + ` ORDER BY visible_at LIMIT 1 FOR UPDATE NOWAIT`,
		`SELECT id FROM jobs WHERE id >= 2 ORDER BY id DESC LIMIT 2 FOR UPDATE NOWAIT`,
	} {
		promptly(t, ctx, b, query, 2*time.Second, func(sctx context.Context, tx pgx.Tx) error {
			if _, err := queryIDs(sctx, tx, query); pgErrCode(err) != sql.CodeLockNotAvailable {
				return fmt.Errorf("got %v, want SQLSTATE %s", err, sql.CodeLockNotAvailable)
			}
			return nil
		})
	}
	for _, q := range []struct{ query, want string }{
		{`SELECT id FROM jobs WHERE ` + claimWhere + ` ORDER BY visible_at DESC LIMIT 2 FOR UPDATE NOWAIT`, "[1 2]"},
		{`SELECT id FROM jobs WHERE id <= 2 ORDER BY id DESC LIMIT 1 FOR UPDATE NOWAIT`, "[2]"},
	} {
		promptly(t, ctx, b, q.query, 2*time.Second, func(sctx context.Context, tx pgx.Tx) error {
			ids, err := queryIDs(sctx, tx, q.query)
			if err != nil {
				return err
			}
			if got := fmt.Sprint(ids); got != q.want {
				return fmt.Errorf("returned %s, want %s", got, q.want)
			}
			return nil
		})
	}
}

// TestForUpdateLimitWaitsPastIndexJoin (issue #322 item 15): a plain
// FOR UPDATE with a limit pushed into the index scan waits for the
// locked row that comes first, as PostgreSQL does, and returns it once
// the holder commits without having changed it.
func TestForUpdateLimitWaitsPastIndexJoin(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	_, atx, _, b := claimQueue(t, ctx)
	type result struct {
		ids []int64
		err error
	}
	done := make(chan result, 1)
	go func() {
		tx, err := b.Begin(ctx)
		if err != nil {
			done <- result{err: err}
			return
		}
		defer func() { _ = tx.Rollback(ctx) }()
		ids, err := queryIDs(ctx, tx, `SELECT id FROM jobs WHERE `+claimWhere+` ORDER BY visible_at LIMIT 1 FOR UPDATE`)
		done <- result{ids, err}
	}()
	select {
	case r := <-done:
		t.Fatalf("FOR UPDATE did not wait for the locked row: %v, %v", r.ids, r.err)
	case <-time.After(300 * time.Millisecond):
	}
	if err := atx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	r := <-done
	if r.err != nil || fmt.Sprint(r.ids) != "[3]" {
		t.Fatalf("FOR UPDATE after the holder committed: %v, %v; want [3]", r.ids, r.err)
	}
}

// TestSkipLockedIndexJoinRowsStayOutOfTheReadSet (issue #322 item 15):
// the index entry of a row an index join passed over as locked is no
// read of the transaction either. B claims job 2 past A's job 3; A then
// claims job 3, which moves its index entry out of the span B scanned,
// and commits; C reads above B's timestamp, so B's commit must refresh
// its reads. Were job 3's old entry in B's read set, A's change to it
// would fail B with 40001 over a row B never returned.
func TestSkipLockedIndexJoinRowsStayOutOfTheReadSet(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	tc, atx, _, b := claimQueue(t, ctx)
	c, err := pgx.Connect(ctx, pgURL(tc, 0))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close(context.Background()) }()

	btx, err := b.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = btx.Rollback(ctx) }()
	if ids, err := queryIDs(ctx, btx, `SELECT id FROM jobs WHERE `+claimWhere+` ORDER BY visible_at LIMIT 1 FOR UPDATE SKIP LOCKED`); err != nil || fmt.Sprint(ids) != "[2]" {
		t.Fatalf("B's claim: %v, %v", ids, err)
	}
	if _, err := atx.Exec(ctx, `UPDATE jobs SET state = 'claimed' WHERE id = 3`); err != nil {
		t.Fatal(err)
	}
	if err := atx.Commit(ctx); err != nil {
		t.Fatalf("A's commit: %v", err)
	}
	if ids, err := queryIDs(ctx, c, `SELECT id FROM jobs ORDER BY id FOR UPDATE SKIP LOCKED`); err != nil || fmt.Sprint(ids) != "[1 3]" {
		t.Fatalf("C's read: %v, %v", ids, err)
	}
	if _, err := btx.Exec(ctx, `UPDATE jobs SET state = 'claimed' WHERE id = 2`); err != nil {
		t.Fatalf("B's write: %v", err)
	}
	if err := btx.Commit(ctx); err != nil {
		t.Fatalf("B's commit (a 40001 here means a skipped row's index entry was in its read set): %v", err)
	}
}
