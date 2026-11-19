package testcluster

import (
	"context"
	"encoding/json"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/kvclient"
	"github.com/sthorne/datax/pkg/server"
	"github.com/sthorne/datax/pkg/sql"
	"github.com/sthorne/datax/pkg/sql/catalog"
	"github.com/sthorne/datax/pkg/version"
)

// Schemas and temporary tables: the review findings on gap PG-D-001.

// TestSchemaDrainsTheExactTable: a statement that changes a table other
// than the one it names (a foreign key's parent) drains that table, by
// a name no search path can redirect. Without it, another gateway keeps
// using the parent's old descriptor for its whole lease, and deletes a
// referenced row.
func TestSchemaDrainsTheExactTable(t *testing.T) {
	tc := StartWithOptions(t, 2, func(c *server.Config) { c.DescLeaseTTL = 60 * time.Second })
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()
	a, b := pgConn(t, ctx, tc, 0), pgConn(t, ctx, tc, 1)

	// The parent in a schema off the path.
	mustExec(t, ctx, a, `CREATE SCHEMA s1`)
	mustExec(t, ctx, a, `CREATE TABLE s1.parent (id INT PRIMARY KEY)`)
	mustExec(t, ctx, a, `INSERT INTO s1.parent VALUES (1), (2)`)
	schemaWant(t, ctx, b, `SELECT count(*) FROM s1.parent`, "2") // b leases the parent
	mustExec(t, ctx, a, `CREATE TABLE s1.child (id INT PRIMARY KEY, p INT REFERENCES s1.parent (id))`)
	mustExec(t, ctx, a, `INSERT INTO s1.child VALUES (1, 1)`)
	tempWantCode(t, ctx, b, `DELETE FROM s1.parent WHERE id = 1`, "23503")

	// The parent shadowed: the path reaches another table of its name
	// first.
	mustExec(t, ctx, a, `CREATE TABLE parent (id INT PRIMARY KEY)`)
	mustExec(t, ctx, a, `SET search_path = public, s1`)
	schemaWant(t, ctx, b, `SELECT count(*) FROM s1.parent`, "2")
	mustExec(t, ctx, a, `CREATE TABLE s1.child2 (id INT PRIMARY KEY, p INT)`)
	mustExec(t, ctx, a, `ALTER TABLE s1.child2 ADD CONSTRAINT c2p FOREIGN KEY (p) REFERENCES s1.parent (id)`)
	mustExec(t, ctx, a, `INSERT INTO s1.child2 VALUES (1, 2)`)
	tempWantCode(t, ctx, b, `DELETE FROM s1.parent WHERE id = 2`, "23503")
	schemaWant(t, ctx, b, `SELECT count(*) FROM s1.parent`, "2")
}

// TestSchemaViewBindsAtCreation: a view reads the relations its names
// resolved to when it was created, even when a table created later in
// a schema earlier on its path would take the name; a view made before
// schemas resolves through public as it did.
func TestSchemaViewBindsAtCreation(t *testing.T) {
	tc := Start(t, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()
	a := pgConn(t, ctx, tc, 0)
	for _, stmt := range []string{
		`CREATE TABLE t (v TEXT)`, `INSERT INTO t VALUES ('public')`,
		`CREATE SCHEMA s1`,
		`SET search_path = s1, public`,
		`CREATE VIEW v AS SELECT v FROM t`,  // s1.v, reading public.t
		`CREATE VIEW v2 AS SELECT v FROM v`, // a view over a view
		`RESET search_path`,
		`CREATE TABLE s1.t (v TEXT)`, `INSERT INTO s1.t VALUES ('hijack')`,
	} {
		mustExec(t, ctx, a, stmt)
	}
	schemaWant(t, ctx, a, `SELECT v FROM s1.v`, "public")
	schemaWant(t, ctx, a, `SELECT v FROM s1.v2`, "public")
	mustExec(t, ctx, a, `SET search_path = s1, public`)
	schemaWant(t, ctx, a, `SELECT v FROM v`, "public")
	schemaStrings(t, ctx, a, `EXPLAIN SELECT v FROM v`)
	mustExec(t, ctx, a, `RESET search_path`)

	// A view as made before schemas: no bindings, no path. It reads
	// public's table.
	mustExec(t, ctx, a, `CREATE TABLE u (v TEXT)`)
	mustExec(t, ctx, a, `INSERT INTO u VALUES ('old public')`)
	mustExec(t, ctx, a, `CREATE VIEW old_v AS SELECT v FROM u`)
	var id uint64
	if err := tc.Nodes[0].DB().RunTxn(ctx, "strip-view", func(ctx context.Context, txn *kvclient.Txn) error {
		db, err := catalog.LookupDatabase(ctx, txn, "datax")
		if err != nil {
			return err
		}
		raw, err := txn.Get(ctx, keys.TableNamespaceKey(db.ID, "old_v"))
		if err != nil {
			return err
		}
		id, _ = strconv.ParseUint(string(raw), 10, 64)
		desc, err := txn.Get(ctx, keys.TableDescKey(id))
		if err != nil {
			return err
		}
		var m map[string]any
		if err := json.Unmarshal(desc, &m); err != nil {
			return err
		}
		delete(m, "view_bindings")
		delete(m, "view_search_path")
		out, _ := json.Marshal(m)
		return txn.Put(ctx, keys.TableDescKey(id), out)
	}); err != nil {
		t.Fatal(err)
	}
	fresh := pgConn(t, ctx, tc, 0)
	mustExec(t, ctx, fresh, `CREATE SCHEMA early`)
	mustExec(t, ctx, fresh, `CREATE TABLE early.u (v TEXT)`)
	mustExec(t, ctx, fresh, `SET search_path = early, public`)
	schemaWant(t, ctx, fresh, `SELECT v FROM public.old_v`, "old public")
}

// TestDropSchemaInViewPath: DROP SCHEMA is refused while a view of
// another schema resolves its names through it, as RENAME is.
func TestDropSchemaInViewPath(t *testing.T) {
	tc := Start(t, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	a := pgConn(t, ctx, tc, 0)
	for _, stmt := range []string{
		`CREATE TABLE t (v TEXT)`, `CREATE SCHEMA s1`,
		`SET search_path = s1, public`,
		`CREATE VIEW public.pv AS SELECT v FROM t`,
		`RESET search_path`,
	} {
		mustExec(t, ctx, a, stmt)
	}
	tempWantCode(t, ctx, a, `DROP SCHEMA s1`, "2BP01")
	tempWantCode(t, ctx, a, `DROP SCHEMA s1 CASCADE`, "2BP01")
	mustExec(t, ctx, a, `DROP VIEW pv`)
	mustExec(t, ctx, a, `DROP SCHEMA s1`)
}

// TestSearchPathSkipsSchemasWithoutUsage: a schema of the path the role
// holds no USAGE on is skipped, for resolution and as the creation
// target, as in PostgreSQL: another role cannot capture a role's
// unqualified names by creating a schema named after it.
func TestSearchPathSkipsSchemasWithoutUsage(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()
	tc := privCluster(t)
	root := pgConn(t, ctx, tc, 0)
	for _, stmt := range []string{
		`CREATE USER us_bob`, `CREATE USER us_mallory`,
		`GRANT CREATE ON DATABASE datax TO us_mallory`,
		`GRANT CREATE ON DATABASE datax TO us_bob`,
		`CREATE TABLE notes (v TEXT)`, `INSERT INTO notes VALUES ('public')`,
		`GRANT SELECT ON notes TO us_bob`,
	} {
		mustExec(t, ctx, root, stmt)
	}
	mallory := scopeConn(t, ctx, tc, "us_mallory", "datax")
	mustExec(t, ctx, mallory, `CREATE SCHEMA us_bob`) // owned by mallory
	mustExec(t, ctx, mallory, `CREATE TABLE us_bob.notes (v TEXT)`)
	mustExec(t, ctx, mallory, `INSERT INTO us_bob.notes VALUES ('captured')`)
	mustExec(t, ctx, mallory, `GRANT SELECT ON us_bob.notes TO us_bob`)

	bob := scopeConn(t, ctx, tc, "us_bob", "datax")
	schemaWant(t, ctx, bob, `SELECT v FROM notes`, "public")
	schemaWant(t, ctx, bob, `SELECT current_schema()`, "public")
	schemaWant(t, ctx, bob, `SELECT array_to_string(current_schemas(false), ',')`, "public")
	mustExec(t, ctx, bob, `CREATE TABLE mine (a INT)`)
	schemaWant(t, ctx, root, `SELECT schemaname FROM pg_tables WHERE tablename = 'mine'`, "public")
	tempWantCode(t, ctx, bob, `SELECT v FROM us_bob.notes`, "42501")
	// Granted USAGE, the schema is on bob's path again.
	mustExec(t, ctx, mallory, `GRANT USAGE ON SCHEMA us_bob TO us_bob`)
	schemaWant(t, ctx, bob, `SELECT v FROM notes`, "captured")
}

// TestTempAndUnloggedNeedV22: temporary and unlogged tables, whose
// descriptor fields a v21 node would drop, wait for v22 like schemas.
func TestTempAndUnloggedNeedV22(t *testing.T) {
	tc := StartWithOptions(t, 1, func(c *server.Config) { c.BinaryVersionOverride = version.V21 })
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	a := pgConn(t, ctx, tc, 0)
	tempWantCode(t, ctx, a, `CREATE TEMP TABLE tt (a INT)`, "0A000")
	tempWantCode(t, ctx, a, `CREATE TEMPORARY TABLE tt AS SELECT 1 AS a`, "0A000")
	tempWantCode(t, ctx, a, `CREATE UNLOGGED TABLE ut (a INT)`, "0A000")
	mustExec(t, ctx, a, `CREATE TABLE lt (a INT)`)
	tempWantCode(t, ctx, a, `ALTER TABLE lt SET UNLOGGED`, "0A000")
	mustExec(t, ctx, a, `ALTER TABLE lt SET LOGGED`) // what it is already
}

// TestRestoreOfSchemasNeedsV22: a backup holding schemas is not
// restored into a cluster below v22.
func TestRestoreOfSchemasNeedsV22(t *testing.T) {
	tc := Start(t, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()
	a := pgConn(t, ctx, tc, 0)
	mustExec(t, ctx, a, `CREATE SCHEMA bk`)
	mustExec(t, ctx, a, `CREATE TABLE bk.t (a INT)`)
	dir := t.TempDir()
	if _, err := tc.Nodes[0].RunBackup(ctx, dir, "", false, false); err != nil {
		t.Fatalf("backup: %v", err)
	}
	old := StartWithOptions(t, 1, func(c *server.Config) { c.BinaryVersionOverride = version.V21 })
	if _, err := old.Nodes[0].RunRestore(ctx, []string{dir}); err == nil || !strings.Contains(err.Error(), "need cluster version v22") {
		t.Fatalf("restore of schemas into a v21 cluster: %v", err)
	}
}

// TestTempTableDroppedFromUnderSession: when the cleanup of ended
// sessions drops a live session's temporary table, the session's next
// unqualified use of the name is refused once, rather than reaching
// the ordinary table the temporary one shadowed.
func TestTempTableDroppedFromUnderSession(t *testing.T) {
	tc := Start(t, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	a := pgConn(t, ctx, tc, 0)
	mustExec(t, ctx, a, `CREATE TABLE t (v TEXT)`)
	mustExec(t, ctx, a, `CREATE TEMP TABLE t (v TEXT)`)
	mustExec(t, ctx, a, `INSERT INTO t VALUES ('temp')`)
	// A table the session itself created and rolled back, and one it
	// dropped, are no such case.
	mustExec(t, ctx, a, `CREATE TABLE u (v TEXT)`)
	mustExec(t, ctx, a, `BEGIN`)
	mustExec(t, ctx, a, `CREATE TEMP TABLE u (v TEXT)`)
	mustExec(t, ctx, a, `ROLLBACK`)
	mustExec(t, ctx, a, `INSERT INTO u VALUES ('public u')`)
	mustExec(t, ctx, a, `CREATE TEMP TABLE w (v TEXT)`)
	mustExec(t, ctx, a, `DROP TABLE w`)
	tempWantCode(t, ctx, a, `SELECT * FROM w`, "42P01")

	n, err := sql.SweepOrphanTempTables(ctx, tc.Nodes[0].DB(), catalog.NewAccessor(), func(string) bool { return true })
	if err != nil || n != 1 {
		t.Fatalf("sweep: %d, %v", n, err)
	}
	_, err = a.Exec(ctx, `INSERT INTO t VALUES ('lost')`)
	if tempSQLState(err) != "42P01" || !strings.Contains(err.Error(), `"t"`) {
		t.Fatalf("INSERT into a temporary table dropped from under the session: %v", err)
	}
	tempWantCode(t, ctx, a, `SELECT * FROM t`, "42P01") // until the session lets it go
	schemaWant(t, ctx, a, `SELECT count(*) FROM public.t`, "0")
	schemaWant(t, ctx, a, `SELECT v FROM u`, "public u")
	// DROP TABLE lets the name go: it is the ordinary table's again.
	mustExec(t, ctx, a, `DROP TABLE t`)
	schemaWant(t, ctx, a, `SELECT count(*) FROM public.t`, "0")
	mustExec(t, ctx, a, `INSERT INTO t VALUES ('public')`)
	schemaWant(t, ctx, a, `SELECT v FROM public.t`, "public")
}

// TestSchemaChangePhasesKeepTheirTable: the phases of an online CREATE
// INDEX act on the table its first phase resolved, even when a table of
// the name is created meanwhile in a schema earlier on the path.
func TestSchemaChangePhasesKeepTheirTable(t *testing.T) {
	tc := Start(t, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	a, b := pgConn(t, ctx, tc, 0), pgConn(t, ctx, tc, 0)
	mustExec(t, ctx, a, `CREATE SCHEMA s1`)
	mustExec(t, ctx, a, `CREATE TABLE t (id INT PRIMARY KEY, v TEXT)`)
	mustExec(t, ctx, a, `INSERT INTO t VALUES (1, 'x'), (2, 'y')`)
	mustExec(t, ctx, a, `SET search_path = s1, public`)
	created := false
	sql.TestingCreateIndexHook = func(point, index string) {
		if point == sql.CreateIndexDrained && index == "t_v" && !created {
			created = true
			if _, err := b.Exec(ctx, `CREATE TABLE s1.t (id INT PRIMARY KEY, v TEXT)`); err != nil {
				t.Errorf("CREATE TABLE s1.t between phases: %v", err)
			}
		}
	}
	defer func() { sql.TestingCreateIndexHook = nil }()
	mustExec(t, ctx, a, `CREATE INDEX t_v ON t (v)`)
	sql.TestingCreateIndexHook = nil
	if !created {
		t.Fatal("the hook did not run")
	}
	schemaWant(t, ctx, b, `SELECT schemaname || '.' || tablename FROM pg_indexes WHERE indexname = 't_v'`, "public.t")
	schemaWant(t, ctx, b, `SELECT v FROM public.t WHERE v = 'y'`, "y")
}

// TestOtherSessionsTempTablesByOID: the catalog functions that take an
// OID say nothing of another session's temporary table.
func TestOtherSessionsTempTablesByOID(t *testing.T) {
	tc := Start(t, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	a, b := pgConn(t, ctx, tc, 0), pgConn(t, ctx, tc, 0)
	mustExec(t, ctx, a, `CREATE TEMP TABLE secret (id INT PRIMARY KEY, v TEXT CHECK (v <> 'x'))`)
	mustExec(t, ctx, a, `CREATE INDEX secret_v ON secret (v)`)
	mustExec(t, ctx, a, `COMMENT ON TABLE secret IS 'hush'`)
	tbl := tempString(t, ctx, a, `SELECT oid::text FROM pg_class WHERE relname = 'secret'`)
	idx := tempString(t, ctx, a, `SELECT oid::text FROM pg_class WHERE relname = 'secret_v'`)
	con := tempString(t, ctx, a, `SELECT oid::text FROM pg_constraint WHERE contype = 'c' AND conrelid = `+tbl)
	schemaWant(t, ctx, a, `SELECT pg_get_indexdef(`+idx+`) IS NOT NULL, obj_description(`+tbl+`, 'pg_class')`, "true|hush")
	schemaWant(t, ctx, b, `SELECT pg_get_indexdef(`+idx+`) IS NULL, pg_get_constraintdef(`+con+`) IS NULL, obj_description(`+tbl+`, 'pg_class') IS NULL`, "true|true|true")
	schemaWant(t, ctx, b, `SELECT has_table_privilege(`+tbl+`, 'SELECT') IS NULL`, "true")
}
