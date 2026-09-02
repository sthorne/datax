package testcluster

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/sthorne/datax/pkg/sql"
	"github.com/sthorne/datax/pkg/sql/catalog"
)

func denied(t *testing.T, ctx context.Context, s *sql.Session, stmt string) {
	t.Helper()
	if _, serr := trySQL(ctx, s, stmt); serr == nil || serr.Code != sql.CodeInsufficientPriv {
		t.Fatalf("%s: expected 42501, got %v", stmt, serr)
	}
}

// Grants and revokes ride descriptor leases, so another gateway may serve
// one more statement against the pre-change privileges (issue #61). These
// helpers absorb that window instead of asserting on the first attempt.

// execEventually retries stmt while it is denied (42501): the granted
// privilege may take a lease refresh to reach this session's gateway.
func execEventually(t *testing.T, ctx context.Context, s *sql.Session, stmt string) *sql.Result {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for {
		res, serr := trySQL(ctx, s, stmt)
		if serr == nil {
			return res
		}
		if serr.Code != sql.CodeInsufficientPriv || time.Now().After(deadline) {
			t.Fatalf("%s: [%s] %s", stmt, serr.Code, serr.Msg)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// awaitDenied polls a SIDE-EFFECT-FREE probe until it returns 42501 —
// the barrier after a revoke, before asserting denials of statements
// that WOULD have side effects on a stale lease.
func awaitDenied(t *testing.T, ctx context.Context, s *sql.Session, probe string) {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for {
		_, serr := trySQL(ctx, s, probe)
		if serr != nil && serr.Code == sql.CodeInsufficientPriv {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s: still not denied after revoke (last: %v)", probe, serr)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// TestPrivileges: the admin role gates DDL, user management, and grants;
// per-table privileges gate DML; grants and revokes take effect at runtime
// across gateways (they ride the descriptor leases).
func TestPrivileges(t *testing.T) {
	tc := Start(t, 3)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	// Leased accessors: grants ride descriptor versions, so a grant on one
	// gateway must drain into every other gateway's cache before returning.
	root := leasedSession(t, tc, 0, 2*time.Second)
	execSQL(t, ctx, root, `CREATE TABLE t (id INT PRIMARY KEY, v INT)`)
	execSQL(t, ctx, root, `CREATE TABLE t2 (id INT PRIMARY KEY, w INT)`)
	execSQL(t, ctx, root, `INSERT INTO t VALUES (1, 10), (2, 20)`)
	execSQL(t, ctx, root, `INSERT INTO t2 VALUES (1, 100)`)
	execSQL(t, ctx, root, `CREATE USER alice PASSWORD 'hunter22'`)

	// alice connects through a different gateway (leased cache there too).
	aliceCat := catalog.NewAccessor()
	if err := aliceCat.StartLeasing(tc.Nodes[1].DB(), tc.Nodes[1].Clock(), tc.Nodes[1].Stopper(), 2*time.Second); err != nil {
		t.Fatal(err)
	}
	alice := sql.NewSessionForUser(tc.Nodes[1].DB(), aliceCat, "alice")

	// No privileges: DML denied.
	denied(t, ctx, alice, `SELECT id FROM t`)
	denied(t, ctx, alice, `INSERT INTO t VALUES (3, 30)`)
	denied(t, ctx, alice, `UPDATE t SET v = 0 WHERE id = 1`)
	denied(t, ctx, alice, `DELETE FROM t WHERE id = 1`)
	// Not an admin: DDL, user management, and grants denied.
	denied(t, ctx, alice, `CREATE TABLE mine (id INT PRIMARY KEY)`)
	denied(t, ctx, alice, `DROP TABLE t`)
	denied(t, ctx, alice, `ALTER TABLE t ADD COLUMN x INT`)
	denied(t, ctx, alice, `CREATE INDEX by_v ON t (v)`)
	denied(t, ctx, alice, `CREATE USER mallory PASSWORD 'pw12345'`)
	denied(t, ctx, alice, `DROP USER root`)
	denied(t, ctx, alice, `GRANT SELECT ON t TO alice`)
	denied(t, ctx, alice, `GRANT ADMIN TO alice`)

	// SELECT grant: reads work, writes stay denied.
	execSQL(t, ctx, root, `GRANT SELECT ON t TO alice`)
	if res := execEventually(t, ctx, alice, `SELECT id FROM t ORDER BY id`); len(res.Rows) != 2 {
		t.Fatalf("granted select: %+v", res.Rows)
	}
	denied(t, ctx, alice, `INSERT INTO t VALUES (3, 30)`)

	// Joins and subqueries check every table they touch.
	denied(t, ctx, alice, `SELECT t.id FROM t JOIN t2 ON t.id = t2.id`)
	denied(t, ctx, alice, `SELECT id FROM t WHERE id IN (SELECT id FROM t2)`)
	execSQL(t, ctx, root, `GRANT SELECT ON t2 TO alice`)
	if res := execEventually(t, ctx, alice, `SELECT t.id FROM t JOIN t2 ON t.id = t2.id`); len(res.Rows) != 1 {
		t.Fatalf("granted join: %+v", res.Rows)
	}

	// ALL expands; revoking one privilege keeps the rest.
	execSQL(t, ctx, root, `GRANT ALL ON t TO alice`)
	execEventually(t, ctx, alice, `INSERT INTO t VALUES (3, 30)`)
	execSQL(t, ctx, alice, `UPDATE t SET v = 33 WHERE id = 3`)
	execSQL(t, ctx, alice, `DELETE FROM t WHERE id = 3`)
	execSQL(t, ctx, root, `REVOKE INSERT, DELETE ON t FROM alice`)
	// Barrier: a no-op DELETE is harmless on a stale lease and 42501 once
	// the revoke lands; only then are the real denials deterministic.
	awaitDenied(t, ctx, alice, `DELETE FROM t WHERE id = 999`)
	denied(t, ctx, alice, `INSERT INTO t VALUES (4, 40)`)
	denied(t, ctx, alice, `DELETE FROM t WHERE id = 1`)
	execSQL(t, ctx, alice, `UPDATE t SET v = 11 WHERE id = 1`)
	if res := execSQL(t, ctx, alice, `SELECT v FROM t WHERE id = 1`); res.Rows[0][0].I != 11 {
		t.Fatalf("post-revoke update: %+v", res.Rows)
	}

	// Admin role: full DDL + grant rights, revocable at runtime.
	execSQL(t, ctx, root, `GRANT ADMIN TO alice`)
	execEventually(t, ctx, alice, `CREATE TABLE hers (id INT PRIMARY KEY)`)
	execSQL(t, ctx, alice, `GRANT SELECT ON hers TO bob`)
	execSQL(t, ctx, root, `REVOKE ADMIN FROM alice`)
	// Barrier: dropping a nonexistent user is side-effect-free — "does not
	// exist" while alice's admin bit is stale, 42501 once the revoke lands.
	awaitDenied(t, ctx, alice, `DROP USER nobody_probe`)
	denied(t, ctx, alice, `CREATE TABLE more (id INT PRIMARY KEY)`)

	// root's membership is implicit and immutable.
	if _, serr := trySQL(ctx, root, `REVOKE ADMIN FROM root`); serr == nil {
		t.Fatal("revoking root's admin accepted")
	}
	// root retains everything.
	execSQL(t, ctx, root, `SELECT id FROM t`)
	execSQL(t, ctx, root, `DROP TABLE hers`)
}

// TestPrivilegesOverPgwire: 42501 surfaces over the wire for the startup
// identity; root keeps working.
func TestPrivilegesOverPgwire(t *testing.T) {
	tc := Start(t, 3)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	rootConn, err := pgx.Connect(ctx, pgURL(tc, 0))
	if err != nil {
		t.Fatalf("root connect: %v", err)
	}
	defer func() { _ = rootConn.Close(ctx) }()
	if _, err := rootConn.Exec(ctx, `CREATE TABLE secrets (id INT8 PRIMARY KEY, s TEXT)`); err != nil {
		t.Fatal(err)
	}
	if _, err := rootConn.Exec(ctx, `INSERT INTO secrets VALUES (1, 'x')`); err != nil {
		t.Fatal(err)
	}
	if _, err := rootConn.Exec(ctx, `GRANT SELECT ON secrets TO bob`); err != nil {
		t.Fatal(err)
	}

	bobConn, err := pgx.Connect(ctx, "postgres://bob@"+tc.Nodes[1].SQLAddr()+"/datax?sslmode=disable")
	if err != nil {
		t.Fatalf("bob connect: %v", err)
	}
	defer func() { _ = bobConn.Close(ctx) }()

	var s string
	if err := bobConn.QueryRow(ctx, `SELECT s FROM secrets WHERE id = 1`).Scan(&s); err != nil || s != "x" {
		t.Fatalf("granted select over wire: %v (%q)", err, s)
	}
	var pgErr *pgconn.PgError
	if _, err := bobConn.Exec(ctx, `INSERT INTO secrets VALUES (2, 'y')`); err == nil {
		t.Fatal("ungranted insert accepted")
	} else if !errors.As(err, &pgErr) || pgErr.Code != "42501" {
		t.Fatalf("insert error: %v", err)
	}
	if _, err := bobConn.Exec(ctx, `CREATE TABLE nope (id INT8 PRIMARY KEY)`); err == nil {
		t.Fatal("non-admin DDL accepted")
	} else if !errors.As(err, &pgErr) || pgErr.Code != "42501" {
		t.Fatalf("ddl error: %v", err)
	}
}
