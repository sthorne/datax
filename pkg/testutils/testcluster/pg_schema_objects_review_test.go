package testcluster

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/sthorne/datax/pkg/kvclient"
	"github.com/sthorne/datax/pkg/sql/catalog"
)

// Review reproductions for c451770 (sequences and enum types in any
// schema). Each test asserts PostgreSQL's behaviour and fails on the
// commit as reviewed. Not for merging as is.

// reviewWant is schemaWant, reporting with Errorf so a test shows every
// divergence it reaches.
func objWant(t *testing.T, ctx context.Context, conn *pgx.Conn, q, want string) {
	t.Helper()
	rows, err := conn.Query(ctx, q)
	if err != nil {
		t.Errorf("%s: %v (want %q)", q, err, want)
		return
	}
	var out []string
	for rows.Next() {
		vals, err := rows.Values()
		if err != nil {
			t.Errorf("%s: %v", q, err)
			rows.Close()
			return
		}
		parts := make([]string, len(vals))
		for i, v := range vals {
			if v == nil {
				parts[i] = "NULL"
			} else if s, ok := v.(string); ok {
				parts[i] = s
			} else {
				parts[i] = fmt.Sprint(v)
			}
		}
		out = append(out, strings.Join(parts, "|"))
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		t.Errorf("%s: %v (want %q)", q, err, want)
		return
	}
	if got := strings.Join(out, ","); got != want {
		t.Errorf("%s:\n got %q\nwant %q", q, got, want)
	}
}

// objCode is tempWantCode, reporting with Errorf.
func objCode(t *testing.T, ctx context.Context, conn *pgx.Conn, q, code string) {
	t.Helper()
	_, err := conn.Exec(ctx, q)
	if got := tempSQLState(err); got != code {
		t.Errorf("%s: got %v (SQLSTATE %q), want SQLSTATE %s", q, err, got, code)
	}
}

// A column default's sequence name never resolves through the
// inserting session's search path. A name that names no sequence is
// refused when the default is set (42P01, as PostgreSQL refuses it);
// and a name a default does not bind (binding 0, which a table restore
// leaves for a sequence it does not bring back) resolves in public, so
// another role that can create a schema named after the inserting role
// ("$user") cannot redirect it to its own sequence.
func TestSchemaSequenceDefaultNeverFollowsInserterPath(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	tc := privCluster(t)
	root := pgConn(t, ctx, tc, 0)
	mustExec(t, ctx, root, `CREATE USER rv_victim`)
	mustExec(t, ctx, root, `CREATE USER rv_mallory`)
	mustExec(t, ctx, root, `GRANT CREATE ON DATABASE datax TO rv_mallory`)
	objCode(t, ctx, root, `CREATE TABLE early (id INT8 DEFAULT nextval('later'), v TEXT)`, "42P01")
	objCode(t, ctx, root, `CREATE TABLE early (id INT8, v TEXT)`, "")
	objCode(t, ctx, root, `ALTER TABLE early ALTER COLUMN id SET DEFAULT nextval('later')`, "42P01")
	objCode(t, ctx, root, `ALTER TABLE early ADD COLUMN w INT8 DEFAULT nextval('later')`, "42P01")
	for _, q := range []string{
		`CREATE SEQUENCE later`,
		`ALTER TABLE early ALTER COLUMN id SET DEFAULT nextval('later')`,
		`GRANT USAGE ON SEQUENCE later TO rv_victim`,
		`GRANT INSERT, SELECT ON early TO rv_victim`,
	} {
		mustExec(t, ctx, root, q)
	}
	// A binding of 0, as a table restore leaves one.
	setBindings(t, ctx, tc, "early", 0)
	mallory := scopeConn(t, ctx, tc, "rv_mallory", "datax")
	for _, q := range []string{
		`CREATE SCHEMA rv_victim`, // the victim's "$user"
		`GRANT USAGE ON SCHEMA rv_victim TO rv_victim`,
		`CREATE SEQUENCE rv_victim.later START 1000`,
		`GRANT USAGE ON SEQUENCE rv_victim.later TO rv_victim`,
	} {
		mustExec(t, ctx, mallory, q)
	}
	victim := scopeConn(t, ctx, tc, "rv_victim", "datax")
	objWant(t, ctx, victim, `INSERT INTO early (v) VALUES ('v') RETURNING id`, "1")
}

// setBindings sets every sequence binding of table name's column
// defaults to id (0: bound to nothing, as a table restore leaves a
// sequence it does not bring back), and waits out the gateways'
// leases (privCluster's, a second).
func setBindings(t *testing.T, ctx context.Context, tc *TestCluster, name string, id uint64) {
	t.Helper()
	acc := catalog.NewAccessor()
	if err := tc.Nodes[0].DB().RunTxn(ctx, "set-bindings", func(ctx context.Context, txn *kvclient.Txn) error {
		d, err := acc.LookupFreshIn(ctx, txn, catalog.DefaultDatabase, name)
		if err != nil {
			return err
		}
		c := d.Clone()
		for i := range c.Columns {
			if c.Columns[i].SeqBindings == nil {
				continue
			}
			b := map[string]uint64{}
			for n := range c.Columns[i].SeqBindings {
				b[n] = id
			}
			c.Columns[i].SeqBindings = b
		}
		return acc.Update(ctx, txn, c)
	}); err != nil {
		t.Fatal(err)
	}
	time.Sleep(2500 * time.Millisecond)
}

// CREATE SEQUENCE ... OWNED BY accepts another role's table
// (PostgreSQL: 55000 sequence must have same owner as table it is
// linked to; ALTER SEQUENCE checks it, CREATE does not). With the
// commit's DROP TABLE accounting, the attacker's bound default then
// blocks the table owner's DROP TABLE (2BP01), and ALTER TABLE ... SET
// SCHEMA by the table owner moves the attacker's sequence.
func TestSchemaSequenceOwnedByNeedsTableOwner(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	tc := privCluster(t)
	root := pgConn(t, ctx, tc, 0)
	for _, q := range []string{
		`CREATE USER ro_alice`,
		`CREATE USER ro_mallory`,
		`GRANT CREATE ON DATABASE datax TO ro_alice`,
		`GRANT CREATE ON DATABASE datax TO ro_mallory`,
	} {
		mustExec(t, ctx, root, q)
	}
	alice := scopeConn(t, ctx, tc, "ro_alice", "datax")
	mustExec(t, ctx, alice, `CREATE TABLE at (id INT8 PRIMARY KEY)`)
	mallory := scopeConn(t, ctx, tc, "ro_mallory", "datax")
	objCode(t, ctx, mallory, `CREATE SEQUENCE m OWNED BY at.id`, "55000")
	_, _ = mallory.Exec(ctx, `CREATE TABLE mt (id INT8 DEFAULT nextval('m'))`)
	// Alice may always drop her own table.
	objCode(t, ctx, alice, `DROP TABLE at`, "")
}

// ALTER SEQUENCE ... SET SCHEMA and ALTER SCHEMA ... RENAME
// leave a view that names the sequence by text naming the old place:
// the view breaks (42P01), and a sequence later created under the old
// name (by anyone with CREATE on that schema) silently takes it over.
// PostgreSQL binds the view to the sequence's OID; datax refuses the
// same moves for tables a view reads (refuseIfViewed).
func TestSchemaObjectMovesUnderViewsAndDefaults(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	tc := privCluster(t)
	root := pgConn(t, ctx, tc, 0)
	for _, q := range []string{
		`CREATE SCHEMA s`,
		`CREATE SCHEMA s2`,
		`CREATE SEQUENCE s.q`,
		`CREATE VIEW vq AS SELECT nextval('s.q') AS n`,
		`CREATE SCHEMA r`,
		`CREATE SEQUENCE r.rq`,
		`CREATE VIEW vr AS SELECT nextval('r.rq') AS n`,
	} {
		mustExec(t, ctx, root, q)
	}
	objWant(t, ctx, root, `SELECT n FROM vq`, "1")
	// Either refused (2BP01, as for a table) or the view follows.
	if _, err := root.Exec(ctx, `ALTER SEQUENCE s.q SET SCHEMA s2`); err == nil {
		objWant(t, ctx, root, `SELECT n FROM vq`, "2")
		mustExec(t, ctx, root, `CREATE SEQUENCE s.q START 1000`)
		objWant(t, ctx, root, `SELECT n FROM vq`, "3")
	}
	if _, err := root.Exec(ctx, `ALTER SCHEMA r RENAME TO r2`); err == nil {
		objWant(t, ctx, root, `SELECT n FROM vr`, "1")
	}
	// Refused, as for a table a view reads.
	objCode(t, ctx, root, `ALTER SEQUENCE s.q SET SCHEMA s2`, "2BP01")
	objCode(t, ctx, root, `ALTER SCHEMA r RENAME TO r2`, "2BP01")
	objWant(t, ctx, root, `SELECT n FROM vq`, "2")
	// A table owning a sequence a view names stays too.
	mustExec(t, ctx, root, `CREATE TABLE s.ser (id SERIAL)`)
	mustExec(t, ctx, root, `CREATE VIEW vs AS SELECT currval('s.ser_id_seq') AS n`)
	objCode(t, ctx, root, `ALTER TABLE s.ser SET SCHEMA s2`, "2BP01")
	// A type named by a view's cast, a column default's or a CHECK's,
	// whose text resolves the name when evaluated: refused too.
	for _, q := range []string{
		`CREATE TYPE s.mood AS ENUM ('a', 'b')`,
		`CREATE TYPE s.tone AS ENUM ('x')`,
		`CREATE TYPE s.hue AS ENUM ('r')`,
		`CREATE TYPE s.free AS ENUM ('f')`,
		`CREATE VIEW vm AS SELECT 'a'::s.mood AS m`,
		`CREATE TABLE s.d (t s.tone DEFAULT 'x'::s.tone)`,
		`CREATE TABLE s.c (h TEXT CHECK (h::s.hue = 'r'))`,
		`CREATE SCHEMA ty`,
	} {
		mustExec(t, ctx, root, q)
	}
	objCode(t, ctx, root, `ALTER TYPE s.mood SET SCHEMA ty`, "2BP01")
	objCode(t, ctx, root, `ALTER TYPE s.tone SET SCHEMA ty`, "2BP01")
	objCode(t, ctx, root, `ALTER TYPE s.hue SET SCHEMA ty`, "2BP01")
	objCode(t, ctx, root, `ALTER TYPE s.free SET SCHEMA ty`, "")
	objWant(t, ctx, root, `SELECT m FROM vm`, "a")
	mustExec(t, ctx, root, `INSERT INTO s.d DEFAULT VALUES`)
	// A schema whose type a view casts to keeps its name.
	mustExec(t, ctx, root, `CREATE SCHEMA tt`)
	mustExec(t, ctx, root, `CREATE TYPE tt.k AS ENUM ('k')`)
	mustExec(t, ctx, root, `CREATE VIEW vk AS SELECT 'k'::tt.k AS k`)
	objCode(t, ctx, root, `ALTER SCHEMA tt RENAME TO tt2`, "2BP01")
	mustExec(t, ctx, root, `DROP VIEW vk`)
	mustExec(t, ctx, root, `CREATE TABLE tk (k tt.k DEFAULT 'k'::tt.k)`)
	objCode(t, ctx, root, `ALTER SCHEMA tt RENAME TO tt2`, "2BP01")
}

// seqDefaultRefs skips other sessions' temporary tables, so
// DROP SEQUENCE succeeds while another session's temporary table has a
// default bound to it, leaving a dangling binding (PostgreSQL: 2BP01).
func TestDropSequenceCountsOtherSessionsTempTables(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	tc := privCluster(t)
	a, b := pgConn(t, ctx, tc, 0), pgConn(t, ctx, tc, 0)
	mustExec(t, ctx, a, `CREATE SEQUENCE pq`)
	mustExec(t, ctx, b, `CREATE TEMP TABLE tq (id INT8 DEFAULT nextval('public.pq'))`)
	mustExec(t, ctx, b, `INSERT INTO tq DEFAULT VALUES`)
	objCode(t, ctx, a, `DROP SEQUENCE pq`, "2BP01")
	// As reviewed: 42P01 could not open relation with OID n.
	objCode(t, ctx, b, `INSERT INTO tq DEFAULT VALUES`, "")
}

// A table (or view, or CTAS) may take the name of a sequence
// of its schema: CREATE TABLE checks only the table namespace. The
// design says the two share the relation namespace (CREATE SEQUENCE
// and SET SCHEMA check it); pg_class then lists two s.x, and regclass,
// nextval and SELECT resolve the name differently. (Already so in
// public before the commit.)
func TestRelationNamesShareTheSequenceNamespace(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	tc := privCluster(t)
	root := pgConn(t, ctx, tc, 0)
	mustExec(t, ctx, root, `CREATE SCHEMA s`)
	mustExec(t, ctx, root, `CREATE SEQUENCE s.x`)
	objCode(t, ctx, root, `CREATE TABLE s.x (a INT)`, "42P07")
	mustExec(t, ctx, root, `CREATE SEQUENCE s.y`)
	objCode(t, ctx, root, `CREATE VIEW s.y AS SELECT 1`, "42P07")
	mustExec(t, ctx, root, `CREATE TABLE s.t (a INT)`)
	mustExec(t, ctx, root, `CREATE SEQUENCE s.w`)
	objCode(t, ctx, root, `ALTER TABLE s.t RENAME TO w`, "42P07")
	objWant(t, ctx, root, `SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
		WHERE n.nspname = 's' AND c.relname IN ('x', 'y', 'w')`, "3")
}

// The type of a column of an enum in a schema off the path
// prints unqualified where psql's \d and pg_dump read it
// (format_type(atttypid, atttypmod), format_type(t.oid, a.atttypmod),
// both from the vtables' hidden __format_type), and regtype output and
// pg_type_is_visible of a regtype literal ignore the schema too.
func TestSchemaEnumTypeQualifiedInCatalogs(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	tc := privCluster(t)
	root := pgConn(t, ctx, tc, 0)
	for _, q := range []string{
		`CREATE SCHEMA app`,
		`CREATE TYPE mood AS ENUM ('x')`, // public's, of the same name
		`CREATE TYPE app.mood AS ENUM ('a', 'b')`,
		`CREATE TABLE tt (m app.mood, p mood)`,
	} {
		mustExec(t, ctx, root, q)
	}
	// psql \d tt.
	objWant(t, ctx, root, `SELECT a.attname, pg_catalog.format_type(a.atttypid, a.atttypmod)
		FROM pg_catalog.pg_attribute a WHERE a.attrelid = 'tt'::regclass AND a.attnum > 0 AND NOT a.attisdropped ORDER BY a.attnum`,
		"m|app.mood,p|mood")
	// pg_dump's column types, with its empty search_path.
	mustExec(t, ctx, root, `SET search_path = ''`)
	objWant(t, ctx, root, `SELECT format_type(t.oid, a.atttypmod) FROM pg_catalog.pg_attribute a JOIN pg_catalog.pg_type t ON a.atttypid = t.oid
		WHERE a.attrelid = 'public.tt'::regclass AND a.attnum > 0 ORDER BY a.attnum`, "app.mood,public.mood")
	mustExec(t, ctx, root, `RESET search_path`)
	objWant(t, ctx, root, `SELECT 'app.mood'::regtype::text, pg_type_is_visible('app.mood'::regtype)`, "app.mood|false")
}

// Both directions of the schema USAGE check. A default bound to a sequence of another schema takes
// USAGE on that schema from the inserting role (checkSequencePriv on
// the bound OID). PostgreSQL checks only the sequence's own privilege
// for a regclass bound by OID, so a role with INSERT on the table and
// USAGE on the sequence can insert there; datax refuses (42501).
func TestSchemaUsageForBoundDefaultsAndRegInput(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	tc := privCluster(t)
	root := pgConn(t, ctx, tc, 0)
	for _, q := range []string{
		`CREATE USER bu_alice`,
		`CREATE SCHEMA app`,
		`CREATE SEQUENCE app.ids`,
		`GRANT USAGE ON SEQUENCE app.ids TO bu_alice`,
		`CREATE TABLE pub (id INT8 PRIMARY KEY DEFAULT nextval('app.ids'), v TEXT)`,
		`GRANT INSERT, SELECT ON pub TO bu_alice`,
	} {
		mustExec(t, ctx, root, q)
	}
	alice := scopeConn(t, ctx, tc, "bu_alice", "datax")
	objWant(t, ctx, alice, `INSERT INTO pub (v) VALUES ('x') RETURNING id`, "1")
	// The other way round: regclass / regtype input of a name in a
	// schema without USAGE resolves it (PostgreSQL: 42501), disclosing
	// the object's existence and OID.
	mustExec(t, ctx, root, `CREATE TYPE app.mood AS ENUM ('a')`)
	objCode(t, ctx, alice, `SELECT 'app.ids'::regclass`, "42501")
	objCode(t, ctx, alice, `SELECT 'app.mood'::regtype`, "42501")
}

// regclass output of a sequence is not quoted, so it does
// not read back: '"Mixed"'::regclass::text is Mixed (PostgreSQL:
// "Mixed"), and "Big"."Q" prints as Big.Q. (Tables' output was already
// unquoted; sequences printed their OID before the commit.)
func TestRegclassOutputQuotesNames(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	tc := privCluster(t)
	root := pgConn(t, ctx, tc, 0)
	mustExec(t, ctx, root, `CREATE SCHEMA "Big"`)
	mustExec(t, ctx, root, `CREATE SEQUENCE "Big"."Q"`)
	mustExec(t, ctx, root, `CREATE SEQUENCE "Mixed"`)
	objWant(t, ctx, root, `SELECT '"Big"."Q"'::regclass::text, '"Mixed"'::regclass::text`, `"Big"."Q"|"Mixed"`)
}
