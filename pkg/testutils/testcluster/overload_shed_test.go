package testcluster

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/kvclient"
	"github.com/sthorne/datax/pkg/kvserver"
	"github.com/sthorne/datax/pkg/server"
	"github.com/sthorne/datax/pkg/sql"
	"github.com/sthorne/datax/pkg/sql/catalog"
)

// TestOverloadShedFailsPromptly (issue #391): under sustained storage
// backpressure a write statement fails with 40001 once the overload
// budget is spent, or as soon as its next backoff would run past its
// own deadline — before the deadline, not at it. Before, a write shed
// by the leader was retried until the statement's context ended: over
// the wire, where the context carries no client deadline, the statement
// waited past the client's deadline and the client saw a timeout, not a
// retryable error.
func TestOverloadShedFailsPromptly(t *testing.T) {
	var overloaded atomic.Bool
	knobs := kvserver.TestingKnobs{
		OverrideOverloaded: func() (bool, string) {
			if overloaded.Load() {
				return true, "test overload"
			}
			return false, ""
		},
	}
	const budget = time.Second
	tc := StartWithOptions(t, 1, func(c *server.Config) {
		c.TestingKnobs = knobs
		c.OverloadRetryBudget = budget
	})
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	n := tc.Nodes[0]
	s := sql.NewSession(n.DB(), catalog.NewAccessor())
	waitForDatabases(t, ctx, s)
	execSQL(t, ctx, s, `CREATE TABLE schema_migrations (version INT8 PRIMARY KEY)`)
	execSQL(t, ctx, s, `INSERT INTO schema_migrations VALUES (1)`)

	conn, err := pgx.Connect(ctx, pgURL(tc, 0))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(context.Background())

	overloaded.Store(true)

	// Over the wire: the server cannot see the client's deadline, so the
	// overload budget is what ends the statement.
	const clientDeadline = 6 * time.Second
	cctx, ccancel := context.WithTimeout(ctx, clientDeadline)
	start := time.Now()
	_, err = conn.Exec(cctx, `INSERT INTO schema_migrations VALUES (2)`)
	elapsed := time.Since(start)
	ccancel()
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) {
		t.Fatalf("INSERT under sustained overload over the wire: %v after %s, want SQLSTATE %s before the client's %s deadline",
			err, elapsed, sql.CodeSerializationFailure, clientDeadline)
	}
	if pgErr.Code != sql.CodeSerializationFailure || !strings.Contains(pgErr.Message, "storage overloaded") {
		t.Fatalf("INSERT under sustained overload: %s %q, want %s storage overloaded", pgErr.Code, pgErr.Message, sql.CodeSerializationFailure)
	}
	if elapsed >= clientDeadline-time.Second {
		t.Fatalf("INSERT took %s to fail, the client's deadline is %s", elapsed, clientDeadline)
	}
	if elapsed < budget/2 {
		t.Fatalf("INSERT failed after %s: the shed was not retried for the budget (%s)", elapsed, budget)
	}

	// The connection is usable afterwards.
	if _, err := conn.Exec(ctx, `SELECT 1`); err != nil {
		t.Fatalf("connection after the shed: %v", err)
	}

	// A statement whose own deadline is nearer than the budget: the
	// shed comes back before the deadline, still as 40001 — not as a
	// cancellation at the deadline.
	n.DB().SetOverloadRetryBudget(time.Minute)
	const own = 1500 * time.Millisecond
	sctx, scancel := context.WithTimeout(ctx, own)
	deadline, _ := sctx.Deadline()
	_, serr := trySQL(sctx, s, `INSERT INTO schema_migrations VALUES (3)`)
	early := time.Until(deadline)
	scancel()
	if serr == nil || serr.Code != sql.CodeSerializationFailure {
		t.Fatalf("INSERT with a %s deadline under overload: %v, want %s", own, serr, sql.CodeSerializationFailure)
	}
	if early <= 0 {
		t.Fatalf("INSERT with a %s deadline returned %s after it", own, -early)
	}

	// A non-transactional write's shed is retryable too.
	pctx, pcancel := context.WithTimeout(ctx, 3*time.Second)
	n.DB().SetOverloadRetryBudget(200 * time.Millisecond)
	perr := n.DB().Put(pctx, append(keys.TableDataPrefix(9391), "k"...), []byte("v"))
	pcancel()
	if perr == nil || !kvclient.IsRetryable(perr) || !kvclient.IsStorageOverloaded(perr) {
		t.Fatalf("non-transactional put under overload: %v, want a retryable storage-overloaded error", perr)
	}
	if serr := sql.ToSQLError(perr); serr.Code != sql.CodeSerializationFailure {
		t.Fatalf("non-transactional shed maps to %s, want %s", serr.Code, sql.CodeSerializationFailure)
	}

	// Overload clears: writes flow again.
	overloaded.Store(false)
	n.DB().SetOverloadRetryBudget(0)
	if _, err := conn.Exec(ctx, `INSERT INTO schema_migrations VALUES (4)`); err != nil {
		t.Fatalf("INSERT after the overload cleared: %v", err)
	}
}
