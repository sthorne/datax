package testcluster

import (
	"bytes"
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/kvclient"
	"github.com/sthorne/datax/pkg/kvpb"
	"github.com/sthorne/datax/pkg/server"
	"github.com/sthorne/datax/pkg/sql/catalog"
)

// Schema-set leases (pkg/sql/catalog/schemacache.go): name resolution
// answers the search path's schema lookups — the "$user" entry of the
// default path above all — from a leased cache, and schema DDL drains
// it on every gateway.

// pointSelectBatches runs a prepared point SELECT reps times on conn and
// returns, over the implicit transactions that read table tableID's
// rows, how many there were, the KV batches they sent between them, and
// how many of their requests read a schema name entry.
func pointSelectBatches(t *testing.T, ctx context.Context, tc *TestCluster, conn *pgx.Conn, query string, tableID uint64, reps int) (txns, batches, schemaReads int) {
	t.Helper()
	dataLo, dataHi := keys.TableDataSpan(tableID)
	nsLo, nsHi := keys.AllSchemaNamespaceSpan()
	type txnCost struct {
		batches, schemaReads int
		readsTable           bool
	}
	var mu sync.Mutex
	perTxn := map[string]*txnCost{}
	in := func(k, lo, hi keys.Key) bool { return bytes.Compare(k, lo) >= 0 && bytes.Compare(k, hi) < 0 }
	db := tc.Nodes[0].DB()
	db.TestingSetSendObserver(func(_ context.Context, ba *kvpb.BatchRequest, _ *kvpb.BatchResponse) {
		if ba.Header.Txn == nil || ba.Header.Txn.Name != "sql-implicit" {
			return
		}
		mu.Lock()
		defer mu.Unlock()
		id := ba.Header.Txn.ID.String()
		c := perTxn[id]
		if c == nil {
			c = &txnCost{}
			perTxn[id] = c
		}
		c.batches++
		for _, ru := range ba.Requests {
			k := keys.Key(ru.GetInner().Header().Key)
			if in(k, dataLo, dataHi) {
				c.readsTable = true
			}
			if in(k, nsLo, nsHi) {
				c.schemaReads++
			}
		}
	})
	defer db.TestingSetSendObserver(nil)
	for i := 0; i < reps; i++ {
		var v string
		if err := conn.QueryRow(ctx, query, int64(i%10+1)).Scan(&v); err != nil {
			t.Fatalf("%s: %v", query, err)
		}
	}
	db.TestingSetSendObserver(nil)
	mu.Lock()
	defer mu.Unlock()
	for _, c := range perTxn {
		if c.readsTable {
			txns++
			batches += c.batches
			schemaReads += c.schemaReads
		}
	}
	return txns, batches, schemaReads
}

// TestPointSelectOneKVBatch: a prepared point SELECT does exactly one KV
// batch, the row read. Under the default search path ("$user", public)
// the role's schema is looked up on every statement, and the miss was a
// transactional read of /system/schemans in each of them; now the
// gateway's leased schema set answers it. The same holds for a schema
// named on the path and for a schema-qualified name.
func TestPointSelectOneKVBatch(t *testing.T) {
	tc := Start(t, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	conn := pgConn(t, ctx, tc, 0)
	mustExec(t, ctx, conn, `CREATE TABLE kvt (k INT8 PRIMARY KEY, v TEXT)`)
	mustExec(t, ctx, conn, `INSERT INTO kvt SELECT i, 'public' FROM generate_series(1, 10) AS g(i)`)
	mustExec(t, ctx, conn, `CREATE SCHEMA s1`)
	mustExec(t, ctx, conn, `CREATE TABLE s1.kvs (k INT8 PRIMARY KEY, v TEXT)`)
	mustExec(t, ctx, conn, `INSERT INTO s1.kvs SELECT i, 's1' FROM generate_series(1, 10) AS g(i)`)
	public := lookupDescriptor(t, ctx, tc.Nodes[0].DB(), "kvt").ID
	inS1 := lookupDescriptor(t, ctx, tc.Nodes[0].DB(), "s1.kvs").ID

	const reps = 50
	for _, c := range []struct {
		name, setup, query string
		table              uint64
	}{
		{"default path", `RESET search_path`, `SELECT v FROM kvt WHERE k = $1`, public},
		{"qualified", `RESET search_path`, `SELECT v FROM s1.kvs WHERE k = $1`, inS1},
		{"schema on the path", `SET search_path = s1, public`, `SELECT v FROM kvs WHERE k = $1`, inS1},
	} {
		t.Run(c.name, func(t *testing.T) {
			mustExec(t, ctx, conn, c.setup)
			for i := 0; i < 5; i++ { // prepare, lease the table and the schema set
				var v string
				if err := conn.QueryRow(ctx, c.query, int64(i+1)).Scan(&v); err != nil {
					t.Fatalf("%s: %v", c.query, err)
				}
			}
			txns, batches, schemaReads := pointSelectBatches(t, ctx, tc, conn, c.query, c.table, reps)
			if txns != reps {
				t.Fatalf("saw %d statements read the table, want %d", txns, reps)
			}
			if batches != reps || schemaReads != 0 {
				t.Fatalf("%d point SELECTs sent %d KV batches with %d schema name reads; want %d batches (one each) and no schema read",
					reps, batches, schemaReads, reps)
			}
		})
	}
}

// userConnOn connects to node as user (the test cluster trusts every
// role).
func userConnOn(t *testing.T, ctx context.Context, tc *TestCluster, node int, user string) *pgx.Conn {
	t.Helper()
	conn, err := pgx.Connect(ctx, fmt.Sprintf("postgres://%s@%s/datax?sslmode=disable", user, tc.Nodes[node].SQLAddr()))
	if err != nil {
		t.Fatalf("connect as %s to node %d: %v", user, node, err)
	}
	t.Cleanup(func() { _ = conn.Close(context.Background()) })
	return conn
}

// TestUserSchemaFollowsSchemaDDL: a schema named after the role is found
// through "$user" as soon as CREATE SCHEMA returns, by sessions on every
// node that had resolved the role's names without it a moment before; a
// rename or a drop takes effect the same way; a USAGE grant or revoke
// on a schema of the path does too; and a transaction that creates,
// renames or drops the schema resolves through its own change, while a
// change rolled back (to a savepoint or wholly) leaves no trace.
func TestUserSchemaFollowsSchemaDDL(t *testing.T) {
	// A renewal every two seconds: a gateway that had not adopted the
	// change by the drain would show it for up to that long.
	tc := StartWithOptions(t, 2, func(cfg *server.Config) { cfg.DescLeaseTTL = 6 * time.Second })
	ctx, cancel := context.WithTimeout(context.Background(), 240*time.Second)
	defer cancel()
	root := pgConn(t, ctx, tc, 0)
	for _, stmt := range []string{
		`CREATE USER uc_alice`, `CREATE USER uc_bob`,
		`GRANT CREATE ON DATABASE datax TO uc_alice`,
		`CREATE TABLE notes (v TEXT)`, `INSERT INTO notes VALUES ('public')`,
		`GRANT SELECT ON notes TO uc_alice, uc_bob`,
	} {
		mustExec(t, ctx, root, stmt)
	}
	alice0 := userConnOn(t, ctx, tc, 0, "uc_alice")
	alice1 := userConnOn(t, ctx, tc, 1, "uc_alice")
	other1 := userConnOn(t, ctx, tc, 1, "uc_alice")
	// Every session has resolved "$user" and missed: the schema sets are
	// leased and cached without uc_alice.
	for _, c := range []*pgx.Conn{alice0, alice1, other1} {
		schemaWant(t, ctx, c, `SELECT v FROM notes`, "public")
		schemaWant(t, ctx, c, `SELECT current_schema()`, "public")
	}

	// Created on node 0: found on node 1 at once.
	mustExec(t, ctx, alice0, `CREATE SCHEMA uc_alice`)
	schemaWant(t, ctx, alice1, `SELECT current_schema()`, "uc_alice")
	schemaWant(t, ctx, other1, `SELECT array_to_string(current_schemas(false), ',')`, "uc_alice,public")
	mustExec(t, ctx, alice0, `CREATE TABLE notes (v TEXT)`) // in uc_alice, the first schema of the path
	mustExec(t, ctx, alice0, `INSERT INTO notes VALUES ('mine')`)
	schemaWant(t, ctx, alice1, `SELECT v FROM notes`, "mine")
	schemaWant(t, ctx, other1, `SELECT v FROM notes`, "mine")

	// Renamed away and back, from node 1: node 0 follows each step.
	mustExec(t, ctx, alice1, `ALTER SCHEMA uc_alice RENAME TO uc_elsewhere`)
	schemaWant(t, ctx, alice0, `SELECT v FROM notes`, "public")
	schemaWant(t, ctx, alice0, `SELECT v FROM uc_elsewhere.notes`, "mine")
	mustExec(t, ctx, alice1, `ALTER SCHEMA uc_elsewhere RENAME TO uc_alice`)
	schemaWant(t, ctx, alice0, `SELECT v FROM notes`, "mine")

	// Dropped on node 0: gone on node 1.
	mustExec(t, ctx, alice0, `DROP SCHEMA uc_alice CASCADE`)
	schemaWant(t, ctx, alice1, `SELECT v FROM notes`, "public")
	schemaWant(t, ctx, other1, `SELECT current_schema()`, "public")

	// Created inside a transaction on node 1: the transaction resolves
	// through it from the next statement on; node 0 once it commits.
	mustExec(t, ctx, alice1, `BEGIN`)
	mustExec(t, ctx, alice1, `CREATE SCHEMA uc_alice`)
	schemaWant(t, ctx, alice1, `SELECT current_schema()`, "uc_alice")
	mustExec(t, ctx, alice1, `CREATE TABLE notes (v TEXT)`)
	mustExec(t, ctx, alice1, `INSERT INTO notes VALUES ('in txn')`)
	schemaWant(t, ctx, alice1, `SELECT v FROM notes`, "in txn")
	schemaWant(t, ctx, other1, `SELECT v FROM notes`, "public") // not committed
	mustExec(t, ctx, alice1, `COMMIT`)
	schemaWant(t, ctx, alice0, `SELECT v FROM notes`, "in txn")
	schemaWant(t, ctx, other1, `SELECT v FROM notes`, "in txn")

	// Renamed inside a transaction, then rolled back to a savepoint; and
	// dropped inside one that rolls back.
	mustExec(t, ctx, alice0, `BEGIN`)
	mustExec(t, ctx, alice0, `SAVEPOINT sp`)
	mustExec(t, ctx, alice0, `ALTER SCHEMA uc_alice RENAME TO uc_renamed`)
	schemaWant(t, ctx, alice0, `SELECT v FROM notes`, "public")
	mustExec(t, ctx, alice0, `ROLLBACK TO SAVEPOINT sp`)
	schemaWant(t, ctx, alice0, `SELECT v FROM notes`, "in txn")
	mustExec(t, ctx, alice0, `COMMIT`)
	mustExec(t, ctx, alice0, `BEGIN`)
	mustExec(t, ctx, alice0, `DROP SCHEMA uc_alice CASCADE`)
	schemaWant(t, ctx, alice0, `SELECT v FROM notes`, "public")
	mustExec(t, ctx, alice0, `ROLLBACK`)
	schemaWant(t, ctx, alice0, `SELECT v FROM notes`, "in txn")
	schemaWant(t, ctx, alice1, `SELECT v FROM notes`, "in txn")

	// USAGE: a schema named after uc_bob that root owns is skipped on
	// bob's path until root grants USAGE on it, and again once revoked,
	// on the other node at once.
	mustExec(t, ctx, root, `CREATE SCHEMA uc_bob`)
	mustExec(t, ctx, root, `CREATE TABLE uc_bob.notes (v TEXT)`)
	mustExec(t, ctx, root, `INSERT INTO uc_bob.notes VALUES ('bob''s')`)
	mustExec(t, ctx, root, `GRANT SELECT ON uc_bob.notes TO uc_bob`)
	bob1 := userConnOn(t, ctx, tc, 1, "uc_bob")
	schemaWant(t, ctx, bob1, `SELECT v FROM notes`, "public")
	mustExec(t, ctx, root, `GRANT USAGE ON SCHEMA uc_bob TO uc_bob`)
	schemaWant(t, ctx, bob1, `SELECT v FROM notes`, "bob's")
	mustExec(t, ctx, root, `REVOKE USAGE ON SCHEMA uc_bob FROM uc_bob`)
	schemaWant(t, ctx, bob1, `SELECT v FROM notes`, "public")
}

// databaseIDOf reads a database's ID.
func databaseIDOf(t *testing.T, ctx context.Context, db *kvclient.DB, name string) uint64 {
	t.Helper()
	var id uint64
	if err := db.RunTxn(ctx, "test-db", func(ctx context.Context, txn *kvclient.Txn) error {
		d, err := catalog.LookupDatabase(ctx, txn, name)
		if err == nil {
			id = d.ID
		}
		return err
	}); err != nil {
		t.Fatal(err)
	}
	return id
}

// TestSchemaSetLeaseDrain: the drain of a schema change, gateway by
// gateway. A gateway that renews adopts the new generation before the
// DDL returns, and resolves through it from its cache; a gateway whose
// renewals have stalled is waited out — its cached set expires with its
// lease, which is all the drain waits for — and resolves through the
// new schemas the moment it is used again.
func TestSchemaSetLeaseDrain(t *testing.T) {
	tc := Start(t, 2)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	const ttl = 2 * time.Second
	sA := leasedSession(t, tc, 0, ttl)
	sB, catB := leasedSessionWithAccessor(t, tc, 1, ttl)
	waitForDatabases(t, ctx, sA)
	execSQL(t, ctx, sA, `CREATE TABLE t (v TEXT)`)
	execSQL(t, ctx, sA, `INSERT INTO t VALUES ('public')`)
	dbID := databaseIDOf(t, ctx, tc.Nodes[0].DB(), catalog.DefaultDatabase)
	want := func(s string) {
		t.Helper()
		res := execSQL(t, ctx, sB, `SELECT v FROM t`)
		if len(res.Rows) != 1 || res.Rows[0][0].S != s {
			t.Fatalf("B reads %+v, want %q", res.Rows, s)
		}
	}
	want("public")
	gen0, ok := catB.TestingSchemaSetCached(dbID)
	if !ok {
		t.Fatal("B resolved an unqualified name and holds no schema set")
	}

	// Renewing: B adopts before CREATE SCHEMA returns, and keeps
	// answering from its cache. The sessions act as root, so "$user"
	// names the schema root.
	start := time.Now()
	execSQL(t, ctx, sA, `CREATE SCHEMA root`)
	if elapsed := time.Since(start); elapsed >= 2*ttl {
		t.Fatalf("CREATE SCHEMA took %v: the drain timed out instead of B adopting", elapsed)
	}
	gen1, ok := catB.TestingSchemaSetCached(dbID)
	if !ok || gen1 <= gen0 {
		t.Fatalf("after CREATE SCHEMA, B caches generation %d (cached %v), want one above %d", gen1, ok, gen0)
	}
	execSQL(t, ctx, sA, `CREATE TABLE root.t (v TEXT)`)
	execSQL(t, ctx, sA, `INSERT INTO root.t VALUES ('root')`)
	want("root")

	// Stalled: B's renewals stop. DROP SCHEMA cannot have B adopt, so it
	// waits for B's lease to lapse; by then B's cache has lapsed too.
	catB.TestingPauseRenewal(true)
	defer catB.TestingPauseRenewal(false)
	want("root") // B's live lease, at the old generation
	execSQL(t, ctx, sA, `DROP SCHEMA root CASCADE`)
	if g, ok := catB.TestingSchemaSetCached(dbID); ok {
		t.Fatalf("DROP SCHEMA returned while stalled B still caches generation %d", g)
	}
	want("public")
}

// TestRestoredSchemasReachCachedSessions: a restore that writes schemas
// moves their databases' schema generations, so a session that had
// resolved names before the restore — its gateway caching the schema
// set without them — resolves through the restored schemas as soon as
// the restore returns, on another node too.
func TestRestoredSchemasReachCachedSessions(t *testing.T) {
	src := Start(t, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 240*time.Second)
	defer cancel()
	a := pgConn(t, ctx, src, 0)
	for _, stmt := range []string{
		`CREATE SCHEMA root`, // "$user" for the root sessions below
		`CREATE TABLE root.t (v TEXT)`, `INSERT INTO root.t VALUES ('restored')`,
	} {
		mustExec(t, ctx, a, stmt)
	}
	dir := t.TempDir()
	if _, err := src.Nodes[0].RunBackup(ctx, dir, "", false, false); err != nil {
		t.Fatalf("backup: %v", err)
	}
	dst := Start(t, 2)
	r := pgConn(t, ctx, dst, 1) // the restore runs on node 0
	schemaWant(t, ctx, r, `SELECT current_schema()`, "public")
	if _, err := dst.Nodes[0].RunRestore(ctx, []string{dir}); err != nil {
		t.Fatalf("restore: %v", err)
	}
	schemaWant(t, ctx, r, `SELECT current_schema()`, "root")
	schemaWant(t, ctx, r, `SELECT v FROM t`, "restored")
}

// TestSchemaTableMissFollowsDDL: under a search path that names a schema
// before public (app, public), an unqualified name of a public table is
// first looked up in app, and the gateway's schema set answers that miss
// from its cache. Every change that files a table under app's name —
// CREATE TABLE and CREATE VIEW, ALTER TABLE ... RENAME TO, ALTER TABLE
// ... SET SCHEMA, from a session on the other node or on the same one —
// is seen by the next statement of every session — whether or not
// another schema holds the name too — as are CREATE, DROP
// and ALTER SCHEMA (RENAME, OWNER) and GRANT and REVOKE of USAGE on app;
// a transaction resolves through its own change, and a change rolled
// back leaves no trace. The same holds for "$user" naming a role that
// has its own schema.
func TestSchemaTableMissFollowsDDL(t *testing.T) {
	// A renewal every two seconds: a gateway that had not adopted a
	// change by its drain would show the old answer for that long.
	tc := StartWithOptions(t, 2, func(cfg *server.Config) { cfg.DescLeaseTTL = 6 * time.Second })
	ctx, cancel := context.WithTimeout(context.Background(), 240*time.Second)
	defer cancel()
	root0 := pgConn(t, ctx, tc, 0)
	for _, stmt := range []string{
		`CREATE USER tm_dan`,
		`CREATE TABLE t (v TEXT)`, `INSERT INTO t VALUES ('public')`,
		`GRANT SELECT ON t TO tm_dan`,
	} {
		mustExec(t, ctx, root0, stmt)
	}
	root1 := pgConn(t, ctx, tc, 1)
	other0 := pgConn(t, ctx, tc, 0)
	dan1 := userConnOn(t, ctx, tc, 1, "tm_dan")
	sessions := []*pgx.Conn{root0, root1, other0}
	for _, c := range append(sessions, dan1) {
		mustExec(t, ctx, c, `SET search_path = app, public`)
	}
	// want: every root session reads s from t.
	want := func(s string) {
		t.Helper()
		for _, c := range sessions {
			schemaWant(t, ctx, c, `SELECT v FROM t`, s)
		}
	}
	want("public") // app does not exist yet

	// CREATE SCHEMA on node 1: the path's schema now exists, holding no t.
	mustExec(t, ctx, root1, `CREATE SCHEMA app`)
	want("public")
	schemaWant(t, ctx, other0, `SELECT current_schema()`, "app")

	// CREATE TABLE on node 0: every session finds app.t at once.
	mustExec(t, ctx, root0, `CREATE TABLE app.t (v TEXT)`)
	mustExec(t, ctx, root0, `INSERT INTO app.t VALUES ('app')`)
	want("app")
	mustExec(t, ctx, root1, `DROP TABLE app.t`)
	want("public")

	// CREATE VIEW, from the other session on node 0.
	mustExec(t, ctx, other0, `CREATE VIEW app.t AS SELECT 'view' AS v`)
	want("view")
	mustExec(t, ctx, root1, `DROP VIEW app.t`)
	want("public")

	// ALTER TABLE ... RENAME TO into the name, on node 1.
	mustExec(t, ctx, root1, `CREATE TABLE app.t2 (v TEXT)`)
	mustExec(t, ctx, root1, `INSERT INTO app.t2 VALUES ('renamed')`)
	want("public")
	mustExec(t, ctx, root1, `ALTER TABLE app.t2 RENAME TO t`)
	want("renamed")
	mustExec(t, ctx, root0, `DROP TABLE app.t`)
	want("public")

	// ALTER TABLE ... SET SCHEMA into app, on node 0.
	mustExec(t, ctx, root0, `CREATE SCHEMA other`)
	mustExec(t, ctx, root0, `CREATE TABLE other.t (v TEXT)`)
	mustExec(t, ctx, root0, `INSERT INTO other.t VALUES ('moved')`)
	want("public")
	mustExec(t, ctx, root0, `ALTER TABLE other.t SET SCHEMA app`)
	want("moved")

	// A name only app holds: no drain, and every session finds it, its
	// lookup reading what the set may not hold yet. Then public files the
	// name too, and app's table still comes first everywhere; renamed in
	// app to a name public holds, and back.
	mustExec(t, ctx, root0, `CREATE TABLE app.u (v TEXT)`)
	mustExec(t, ctx, root0, `INSERT INTO app.u VALUES ('app u')`)
	for _, c := range sessions {
		schemaWant(t, ctx, c, `SELECT v FROM u`, "app u")
	}
	mustExec(t, ctx, root1, `CREATE TABLE public.u (v TEXT)`)
	mustExec(t, ctx, root1, `INSERT INTO public.u VALUES ('public u')`)
	for _, c := range sessions {
		schemaWant(t, ctx, c, `SELECT v FROM u`, "app u")
	}
	mustExec(t, ctx, root0, `CREATE TABLE app.w (v TEXT)`)
	mustExec(t, ctx, root0, `INSERT INTO app.w VALUES ('app w')`)
	mustExec(t, ctx, root1, `ALTER TABLE app.u RENAME TO u_old`)
	for _, c := range sessions {
		schemaWant(t, ctx, c, `SELECT v FROM u`, "public u")
	}
	mustExec(t, ctx, root1, `ALTER TABLE app.w RENAME TO u`)
	for _, c := range sessions {
		schemaWant(t, ctx, c, `SELECT v FROM u`, "app w")
	}

	// ALTER SCHEMA RENAME away and back, on node 1.
	mustExec(t, ctx, root1, `ALTER SCHEMA app RENAME TO app_away`)
	want("public")
	mustExec(t, ctx, root1, `ALTER SCHEMA app_away RENAME TO app`)
	want("moved")

	// Privileges: without USAGE on app, tm_dan's path skips it; a grant,
	// a revoke and an ownership change on node 0 each reach node 1.
	mustExec(t, ctx, root0, `GRANT SELECT ON app.t TO tm_dan`)
	schemaWant(t, ctx, dan1, `SELECT v FROM t`, "public")
	mustExec(t, ctx, root0, `GRANT USAGE ON SCHEMA app TO tm_dan`)
	schemaWant(t, ctx, dan1, `SELECT v FROM t`, "moved")
	mustExec(t, ctx, root0, `REVOKE USAGE ON SCHEMA app FROM tm_dan`)
	schemaWant(t, ctx, dan1, `SELECT v FROM t`, "public")
	mustExec(t, ctx, root0, `ALTER SCHEMA app OWNER TO tm_dan`)
	schemaWant(t, ctx, dan1, `SELECT v FROM t`, "moved")
	mustExec(t, ctx, root0, `ALTER SCHEMA app OWNER TO root`)
	schemaWant(t, ctx, dan1, `SELECT v FROM t`, "public")

	// DROP SCHEMA on node 0.
	mustExec(t, ctx, root0, `DROP SCHEMA app CASCADE`)
	want("public")
	mustExec(t, ctx, root0, `CREATE SCHEMA app`)
	want("public")

	// Created in a transaction on node 1: it resolves through its own
	// table; the others once it commits.
	mustExec(t, ctx, root1, `BEGIN`)
	mustExec(t, ctx, root1, `CREATE TABLE app.t (v TEXT)`)
	mustExec(t, ctx, root1, `INSERT INTO app.t VALUES ('in txn')`)
	schemaWant(t, ctx, root1, `SELECT v FROM t`, "in txn")
	schemaWant(t, ctx, root0, `SELECT v FROM t`, "public") // not committed
	mustExec(t, ctx, root1, `COMMIT`)
	want("in txn")
	mustExec(t, ctx, root1, `DROP TABLE app.t`)
	want("public")

	// Created in a transaction that rolls back, and behind a savepoint
	// rolled back to: no trace.
	mustExec(t, ctx, root0, `BEGIN`)
	mustExec(t, ctx, root0, `CREATE TABLE app.t (v TEXT)`)
	schemaWant(t, ctx, root0, `SELECT count(*)::TEXT FROM t`, "0")
	mustExec(t, ctx, root0, `ROLLBACK`)
	want("public")
	mustExec(t, ctx, root1, `BEGIN`)
	mustExec(t, ctx, root1, `SAVEPOINT sp`)
	mustExec(t, ctx, root1, `CREATE TABLE app.t (v TEXT)`)
	mustExec(t, ctx, root1, `ROLLBACK TO SAVEPOINT sp`)
	schemaWant(t, ctx, root1, `SELECT v FROM t`, "public")
	mustExec(t, ctx, root1, `COMMIT`)
	want("public")

	// "$user": tm_dan's own schema, on the default path, holds no t until
	// a table is created there from the other node.
	mustExec(t, ctx, root0, `CREATE SCHEMA tm_dan AUTHORIZATION tm_dan`)
	mustExec(t, ctx, dan1, `RESET search_path`)
	schemaWant(t, ctx, dan1, `SELECT v FROM t`, "public")
	dan0 := userConnOn(t, ctx, tc, 0, "tm_dan")
	mustExec(t, ctx, dan0, `CREATE TABLE tm_dan.t (v TEXT)`)
	mustExec(t, ctx, dan0, `INSERT INTO t VALUES ('dan''s')`)
	schemaWant(t, ctx, dan1, `SELECT v FROM t`, "dan's")
}
