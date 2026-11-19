package testcluster

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/sthorne/datax/pkg/base"
	"github.com/sthorne/datax/pkg/cluster"
	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/kvclient"
	"github.com/sthorne/datax/pkg/server"
	"github.com/sthorne/datax/pkg/sql"
	"github.com/sthorne/datax/pkg/sql/catalog"
	"github.com/sthorne/datax/pkg/version"
)

// Schemas (gap PG-D-001 of docs/compatibility-gaps.md; design in
// docs/design/schemas.md).

func schemaStrings(t *testing.T, ctx context.Context, conn *pgx.Conn, q string, args ...any) []string {
	t.Helper()
	rows, err := conn.Query(ctx, q, args...)
	if err != nil {
		t.Fatalf("%s: %v", q, err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		vals, err := rows.Values()
		if err != nil {
			t.Fatalf("%s: %v", q, err)
		}
		parts := make([]string, len(vals))
		for i, v := range vals {
			switch x := v.(type) {
			case nil:
				parts[i] = "NULL"
			case string:
				parts[i] = x
			default:
				b, _ := json.Marshal(x)
				parts[i] = strings.Trim(string(b), `"`)
			}
		}
		out = append(out, strings.Join(parts, "|"))
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("%s: %v", q, err)
	}
	return strings.Split(strings.Join(out, ","), ",")
}

// sessWant runs q in a session and compares its rows, rendered as
// schemaStrings renders them.
func sessWant(t *testing.T, ctx context.Context, s *sql.Session, q, want string) {
	t.Helper()
	res := execSQL(t, ctx, s, q)
	var rows []string
	for _, r := range res.Rows {
		parts := make([]string, len(r))
		for i, d := range r {
			if d.Null {
				parts[i] = "NULL"
			} else {
				parts[i] = d.Text()
			}
		}
		rows = append(rows, strings.Join(parts, "|"))
	}
	if got := strings.Join(rows, ","); got != want {
		t.Fatalf("%s:\n got %q\nwant %q", q, got, want)
	}
}

func sessWantCode(t *testing.T, ctx context.Context, s *sql.Session, q, code string) {
	t.Helper()
	if _, serr := trySQL(ctx, s, q); serr == nil || serr.Code != code {
		t.Fatalf("%s: got %v, want SQLSTATE %s", q, serr, code)
	}
}

func schemaWant(t *testing.T, ctx context.Context, conn *pgx.Conn, q, want string, args ...any) {
	t.Helper()
	if got := strings.Join(schemaStrings(t, ctx, conn, q, args...), ","); got != want {
		t.Fatalf("%s:\n got %q\nwant %q", q, got, want)
	}
}

// TestSchemas: CREATE / DROP / ALTER SCHEMA, tables of every kind of
// statement in a schema, qualified names, and the SQLSTATEs.
func TestSchemas(t *testing.T) {
	tc := Start(t, 2)
	ctx, cancel := context.WithTimeout(context.Background(), 240*time.Second)
	defer cancel()
	a, b := pgConn(t, ctx, tc, 0), pgConn(t, ctx, tc, 1)

	mustExec(t, ctx, a, `CREATE TABLE t (id INT PRIMARY KEY, v TEXT)`)
	mustExec(t, ctx, a, `INSERT INTO t VALUES (1, 'public')`)
	mustExec(t, ctx, a, `CREATE SCHEMA s`)
	tempWantCode(t, ctx, a, `CREATE SCHEMA s`, "42P06")
	mustExec(t, ctx, a, `CREATE SCHEMA IF NOT EXISTS s`)
	tempWantCode(t, ctx, a, `CREATE SCHEMA public`, "42P06")
	tempWantCode(t, ctx, a, `CREATE SCHEMA information_schema`, "42P06")
	tempWantCode(t, ctx, a, `CREATE SCHEMA pg_mine`, "42939")
	tempWantCode(t, ctx, a, `CREATE SCHEMA s2 CREATE TABLE x (a INT)`, "0A000")
	mustExec(t, ctx, a, `BEGIN READ ONLY`)
	tempWantCode(t, ctx, a, `CREATE SCHEMA ro`, "25006")
	mustExec(t, ctx, a, `ROLLBACK`)
	// A schema made in a block that rolls back leaves nothing behind.
	mustExec(t, ctx, a, `BEGIN`)
	mustExec(t, ctx, a, `CREATE SCHEMA tx`)
	mustExec(t, ctx, a, `CREATE TABLE tx.t (a INT)`)
	mustExec(t, ctx, a, `INSERT INTO tx.t VALUES (1)`)
	schemaWant(t, ctx, a, `SELECT a FROM tx.t`, "1")
	mustExec(t, ctx, a, `ROLLBACK`)
	tempWantCode(t, ctx, b, `SELECT a FROM tx.t`, "42P01")
	tempWantCode(t, ctx, a, `DROP SCHEMA tx`, "3F000")

	// A table of the same name in each schema: each statement reaches
	// the one its name says, from either node.
	mustExec(t, ctx, b, `CREATE TABLE s.t (id INT PRIMARY KEY, v TEXT, n INT DEFAULT 0)`)
	mustExec(t, ctx, b, `INSERT INTO s.t (id, v) VALUES (1, 'schema s'), (2, 'two')`)
	mustExec(t, ctx, a, `UPDATE s.t SET n = n + 1 WHERE id = 1`)
	mustExec(t, ctx, a, `DELETE FROM s.t WHERE id = 2`)
	schemaWant(t, ctx, a, `SELECT v FROM t`, "public")
	schemaWant(t, ctx, a, `SELECT v FROM public.t`, "public")
	schemaWant(t, ctx, b, `SELECT v, n FROM s.t`, "schema s|1")
	schemaWant(t, ctx, b, `SELECT s.t.v FROM s.t`, "schema s")
	schemaWant(t, ctx, b, `SELECT t.v FROM s.t`, "schema s")
	schemaWant(t, ctx, a, `SELECT v FROM datax.s.t`, "schema s")
	schemaWant(t, ctx, a, `SELECT p.v || '/' || q.v FROM public.t p JOIN s.t q ON p.id = q.id`, "public/schema s")
	// Two tables of one name bind by that name: unaliased, they clash
	// (never one read for the other).
	tempWantCode(t, ctx, a, `SELECT * FROM public.t JOIN s.t ON public.t.id = s.t.id`, "42P07")
	if _, err := a.CopyFrom(ctx, pgx.Identifier{"s", "t"}, []string{"id", "v"}, pgx.CopyFromRows([][]any{{int32(3), "copied"}})); err != nil {
		t.Fatalf("COPY into s.t: %v", err)
	}
	schemaWant(t, ctx, a, `SELECT count(*) FROM s.t`, "2")
	schemaWant(t, ctx, a, `SELECT count(*) FROM t`, "1")

	// DDL on the schema's table: indexes, constraints, columns, a
	// foreign key across schemas, rename, truncate.
	mustExec(t, ctx, a, `CREATE INDEX t_v ON s.t (v)`)
	mustExec(t, ctx, a, `CREATE INDEX t_v ON t (v)`) // index names are per schema
	mustExec(t, ctx, a, `ALTER TABLE s.t ADD COLUMN w TEXT DEFAULT 'w'`)
	mustExec(t, ctx, a, `ALTER TABLE s.t ADD CONSTRAINT n_pos CHECK (n >= 0)`)
	tempWantCode(t, ctx, a, `INSERT INTO s.t (id, v, n) VALUES (9, 'neg', -1)`, "23514")
	mustExec(t, ctx, a, `CREATE TABLE s.child (id INT PRIMARY KEY, parent INT REFERENCES public.t (id), sib INT REFERENCES s.t (id))`)
	mustExec(t, ctx, a, `INSERT INTO s.child VALUES (1, 1, 1)`)
	tempWantCode(t, ctx, a, `INSERT INTO s.child VALUES (2, 99, 1)`, "23503")
	schemaWant(t, ctx, b, `SELECT w FROM s.t WHERE id = 1`, "w")
	mustExec(t, ctx, a, `DROP INDEX s.t_v`)
	schemaWant(t, ctx, a, `SELECT count(*) FROM pg_indexes WHERE indexname = 't_v'`, "1")
	mustExec(t, ctx, a, `ALTER TABLE s.child RENAME TO kid`)
	schemaWant(t, ctx, b, `SELECT parent FROM s.kid`, "1")
	tempWantCode(t, ctx, b, `SELECT * FROM s.child`, "42P01")
	tempWantCode(t, ctx, b, `SELECT * FROM kid`, "42P01")

	// Errors: missing schemas and relations, three-part names.
	tempWantCode(t, ctx, a, `CREATE TABLE nosuch.x (a INT)`, "3F000")
	tempWantCode(t, ctx, a, `SELECT * FROM nosuch.x`, "42P01")
	tempWantCode(t, ctx, a, `SELECT * FROM s.nosuch`, "42P01")
	tempWantCode(t, ctx, a, `SELECT * FROM datax.nosuch.x`, "42P01")
	tempWantCode(t, ctx, a, `SELECT * FROM otherdb.s.t`, "0A000")
	tempWantCode(t, ctx, a, `CREATE TABLE a.b.c.d (x INT)`, "42601")

	// DROP SCHEMA: RESTRICT refuses a schema with objects; CASCADE
	// drops them; public and the system schemas cannot go.
	tempWantCode(t, ctx, a, `DROP SCHEMA s`, "2BP01")
	tempWantCode(t, ctx, a, `DROP SCHEMA s RESTRICT`, "2BP01")
	tempWantCode(t, ctx, a, `DROP SCHEMA nosuch`, "3F000")
	mustExec(t, ctx, a, `DROP SCHEMA IF EXISTS nosuch`)
	tempWantCode(t, ctx, a, `DROP SCHEMA public`, "0A000")
	tempWantCode(t, ctx, a, `DROP SCHEMA pg_catalog`, "2BP01")
	mustExec(t, ctx, a, `CREATE SCHEMA empty`)
	mustExec(t, ctx, a, `DROP SCHEMA empty`)
	mustExec(t, ctx, a, `DROP SCHEMA s CASCADE`)
	tempWantCode(t, ctx, b, `SELECT * FROM s.t`, "42P01")
	tempWantCode(t, ctx, b, `SELECT * FROM s.kid`, "42P01")
	schemaWant(t, ctx, b, `SELECT v FROM t`, "public")
	// The name is free again, with nothing in it.
	mustExec(t, ctx, b, `CREATE SCHEMA s`)
	tempWantCode(t, ctx, a, `SELECT * FROM s.t`, "42P01")
	mustExec(t, ctx, b, `DROP SCHEMA s`)

	// ALTER SCHEMA ... RENAME TO: the tables follow, whatever node.
	mustExec(t, ctx, a, `CREATE SCHEMA r1`)
	mustExec(t, ctx, a, `CREATE TABLE r1.x (id INT PRIMARY KEY)`)
	mustExec(t, ctx, a, `INSERT INTO r1.x VALUES (7)`)
	schemaWant(t, ctx, b, `SELECT id FROM r1.x`, "7")
	mustExec(t, ctx, a, `ALTER SCHEMA r1 RENAME TO r2`)
	schemaWant(t, ctx, b, `SELECT id FROM r2.x`, "7")
	tempWantCode(t, ctx, b, `SELECT id FROM r1.x`, "42P01")
	mustExec(t, ctx, a, `CREATE SCHEMA r3`)
	tempWantCode(t, ctx, a, `ALTER SCHEMA r2 RENAME TO r3`, "42P06")
	tempWantCode(t, ctx, a, `ALTER SCHEMA r2 RENAME TO pg_x`, "42939")
	tempWantCode(t, ctx, a, `ALTER SCHEMA public RENAME TO p`, "0A000")
	tempWantCode(t, ctx, a, `ALTER SCHEMA nosuch RENAME TO p`, "3F000")

	// Sequences and types in a schema (gap PG-D-035, covered in
	// pg_schema_objects_test.go), and a SERIAL column with them; a
	// schema's table may still draw from a public sequence.
	mustExec(t, ctx, a, `CREATE SEQUENCE r2.q`)
	mustExec(t, ctx, a, `CREATE TYPE r2.mood AS ENUM ('a')`)
	mustExec(t, ctx, a, `CREATE TABLE r2.ser (id SERIAL PRIMARY KEY)`)
	mustExec(t, ctx, a, `CREATE TABLE r2.ident (id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY)`)
	mustExec(t, ctx, a, `CREATE SEQUENCE ids`)
	mustExec(t, ctx, a, `CREATE TABLE r2.withseq (id INT8 PRIMARY KEY DEFAULT nextval('public.ids'), v TEXT)`)
	mustExec(t, ctx, b, `INSERT INTO r2.withseq (v) VALUES ('a')`)
	schemaWant(t, ctx, b, `SELECT id FROM r2.withseq`, "1")
	tempWantCode(t, ctx, a, `SELECT nextval('r2.ids')`, "42P01")
}

// TestSchemaSearchPath: the default path, SET / SHOW, resolution and
// creation through the path, "$user", current_schema() and
// current_schemas(), and a SET LOCAL path in a block that changes a
// table (the drain at COMMIT must find the table the block changed).
func TestSchemaSearchPath(t *testing.T) {
	tc := Start(t, 2)
	ctx, cancel := context.WithTimeout(context.Background(), 240*time.Second)
	defer cancel()
	a, b := pgConn(t, ctx, tc, 0), pgConn(t, ctx, tc, 1)

	schemaWant(t, ctx, a, `SHOW search_path`, `"$user", public`)
	schemaWant(t, ctx, a, `SELECT current_schema()`, "public")
	schemaWant(t, ctx, a, `SELECT array_to_string(current_schemas(true), ',')`, "pg_catalog,public")
	schemaWant(t, ctx, a, `SELECT array_to_string(current_schemas(false), ',')`, "public")

	mustExec(t, ctx, a, `CREATE TABLE t (v TEXT)`)
	mustExec(t, ctx, a, `INSERT INTO t VALUES ('public')`)
	mustExec(t, ctx, a, `CREATE SCHEMA app`)
	mustExec(t, ctx, a, `CREATE TABLE app.t (v TEXT)`)
	mustExec(t, ctx, a, `INSERT INTO app.t VALUES ('app')`)

	mustExec(t, ctx, a, `SET search_path = app, public`)
	schemaWant(t, ctx, a, `SHOW search_path`, `app, public`)
	schemaWant(t, ctx, a, `SELECT current_schema()`, "app")
	schemaWant(t, ctx, a, `SELECT v FROM t`, "app")
	schemaWant(t, ctx, a, `SELECT v FROM public.t`, "public")
	schemaWant(t, ctx, b, `SELECT v FROM t`, "public") // another session's path
	// Creation goes to the first schema of the path; nonexistent ones
	// are skipped.
	mustExec(t, ctx, a, `SET search_path TO nosuch, "app", public`)
	schemaWant(t, ctx, a, `SELECT array_to_string(current_schemas(false), ',')`, "app,public")
	mustExec(t, ctx, a, `CREATE TABLE fresh (k INT)`)
	schemaWant(t, ctx, b, `SELECT count(*) FROM app.fresh`, "0")
	tempWantCode(t, ctx, b, `SELECT * FROM fresh`, "42P01")
	// DML, DDL and DROP through the path.
	mustExec(t, ctx, a, `INSERT INTO fresh VALUES (1)`)
	mustExec(t, ctx, a, `ALTER TABLE fresh ADD COLUMN j INT DEFAULT 5`)
	schemaWant(t, ctx, b, `SELECT k, j FROM app.fresh`, "1|5")
	mustExec(t, ctx, a, `DROP TABLE t`) // app.t
	schemaWant(t, ctx, b, `SELECT v FROM t`, "public")
	tempWantCode(t, ctx, b, `SELECT * FROM app.t`, "42P01")

	// A path of schemas that do not exist: nothing resolves
	// unqualified, nothing is created.
	mustExec(t, ctx, a, `SET search_path = nosuch`)
	schemaWant(t, ctx, a, `SELECT current_schema()`, "NULL")
	tempWantCode(t, ctx, a, `SELECT * FROM t`, "42P01")
	tempWantCode(t, ctx, a, `CREATE TABLE nowhere (a INT)`, "3F000")
	schemaWant(t, ctx, a, `SELECT v FROM public.t`, "public")
	schemaWant(t, ctx, a, `SELECT count(*) FROM pg_class WHERE relname = 'fresh'`, "1") // catalogs stay visible
	// An empty path (pg_dump's): an existing name still resolves
	// through public (datax renders view and constraint definitions
	// unqualified, and a dump must restore), but nothing is created
	// unqualified.
	for _, empty := range []string{`SET search_path = ''`, `SET search_path = pg_catalog`} {
		mustExec(t, ctx, a, empty)
		schemaWant(t, ctx, a, `SELECT current_schema()`, "NULL")
		schemaWant(t, ctx, a, `SELECT v FROM t`, "public")
		tempWantCode(t, ctx, a, `CREATE TABLE nowhere (a INT)`, "3F000")
	}
	mustExec(t, ctx, a, `RESET search_path`)
	schemaWant(t, ctx, a, `SHOW search_path`, `"$user", public`)

	// "$user": a schema named after the role leads the default path.
	mustExec(t, ctx, a, `CREATE USER sp_alice`)
	mustExec(t, ctx, a, `GRANT CREATE ON DATABASE datax TO sp_alice`)
	mustExec(t, ctx, a, `GRANT SELECT ON t TO sp_alice`)
	alice := scopeConn(t, ctx, tc, "sp_alice", "datax")
	schemaWant(t, ctx, alice, `SELECT v FROM t`, "public")
	mustExec(t, ctx, alice, `CREATE SCHEMA AUTHORIZATION sp_alice`)
	schemaWant(t, ctx, alice, `SELECT current_schema()`, "sp_alice")
	mustExec(t, ctx, alice, `CREATE TABLE t (v TEXT)`)
	mustExec(t, ctx, alice, `INSERT INTO t VALUES ('mine')`)
	schemaWant(t, ctx, alice, `SELECT v FROM t`, "mine")
	schemaWant(t, ctx, a, `SELECT v FROM t`, "public") // root has no schema of its own
	schemaWant(t, ctx, a, `SELECT v FROM sp_alice.t`, "mine")

	// SET LOCAL search_path in a block that alters a table: the change
	// is drained (and visible on the other node) after COMMIT.
	mustExec(t, ctx, a, `BEGIN`)
	mustExec(t, ctx, a, `SET LOCAL search_path = app`)
	mustExec(t, ctx, a, `ALTER TABLE fresh ADD COLUMN late TEXT DEFAULT 'L'`)
	mustExec(t, ctx, a, `COMMIT`)
	schemaWant(t, ctx, a, `SHOW search_path`, `"$user", public`)
	schemaWant(t, ctx, b, `SELECT late FROM app.fresh`, "L")
}

// TestSchemaTwoPartPrecedence: a two-part name is a schema of the
// current database first, and a database (the datax extension) only
// when no such schema exists; db.public.t still reaches another
// database; db.s.t does not (0A000).
func TestSchemaTwoPartPrecedence(t *testing.T) {
	tc := Start(t, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()
	a := pgConn(t, ctx, tc, 0)

	mustExec(t, ctx, a, `CREATE DATABASE shop`)
	mustExec(t, ctx, a, `CREATE TABLE shop.orders (id INT PRIMARY KEY)`)
	mustExec(t, ctx, a, `INSERT INTO shop.orders VALUES (1)`)
	schemaWant(t, ctx, a, `SELECT id FROM shop.orders`, "1")
	schemaWant(t, ctx, a, `SELECT id FROM shop.public.orders`, "1")
	mustExec(t, ctx, a, `INSERT INTO shop.public.orders VALUES (2)`)

	// A schema of the same name in the current database wins.
	mustExec(t, ctx, a, `CREATE SCHEMA shop`)
	tempWantCode(t, ctx, a, `SELECT id FROM shop.orders`, "42P01")
	mustExec(t, ctx, a, `CREATE TABLE shop.orders (id INT PRIMARY KEY)`)
	mustExec(t, ctx, a, `INSERT INTO shop.orders VALUES (100)`)
	schemaWant(t, ctx, a, `SELECT id FROM shop.orders`, "100")
	schemaWant(t, ctx, a, `SELECT id FROM shop.public.orders ORDER BY id`, "1,2")
	schemaWant(t, ctx, a, `SELECT id FROM datax.shop.orders`, "100")
	// Another database's schema other than public: not reachable.
	tempWantCode(t, ctx, a, `SELECT id FROM shop.shop.orders`, "0A000")
	// Dropping the schema brings the database back.
	mustExec(t, ctx, a, `DROP SCHEMA shop CASCADE`)
	schemaWant(t, ctx, a, `SELECT count(*) FROM shop.orders`, "2")

	// The current database's name as a two-part qualifier is the
	// database, as before.
	mustExec(t, ctx, a, `CREATE TABLE here (x INT)`)
	mustExec(t, ctx, a, `INSERT INTO datax.here VALUES (1)`)
	schemaWant(t, ctx, a, `SELECT x FROM datax.here`, "1")

	// DROP DATABASE takes its schemas along.
	mustExec(t, ctx, a, `CREATE DATABASE gone`)
	ga := scopeConn(t, ctx, tc, "root", "gone")
	mustExec(t, ctx, ga, `CREATE SCHEMA inner_s`)
	mustExec(t, ctx, ga, `CREATE TABLE inner_s.x (a INT)`)
	ga.Close(ctx)
	countSchemas := func() int {
		n := 0
		if err := tc.Nodes[0].DB().RunTxn(ctx, "count-schemas", func(ctx context.Context, txn *kvclient.Txn) error {
			lo, hi := keys.SchemaDescSpan()
			rows, err := txn.Scan(ctx, lo, hi, 0)
			n = len(rows)
			return err
		}); err != nil {
			t.Fatal(err)
		}
		return n
	}
	if n := countSchemas(); n != 1 {
		t.Fatalf("%d schema descriptors before DROP DATABASE, want 1", n)
	}
	tempWantCode(t, ctx, a, `DROP DATABASE gone`, "2BP01")
	mustExec(t, ctx, a, `DROP DATABASE gone CASCADE`)
	if n := countSchemas(); n != 0 {
		t.Fatalf("%d schema descriptors after DROP DATABASE, want 0", n)
	}
}

// TestSchemaPrivileges: USAGE and CREATE on a schema, its owner,
// AUTHORIZATION, ALTER SCHEMA ... OWNER TO, has_schema_privilege, the
// schemata view, and DROP ROLE / REASSIGN OWNED / DROP OWNED of a
// schema.
func TestSchemaPrivileges(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 240*time.Second)
	defer cancel()
	tc := privCluster(t)
	root := pgConn(t, ctx, tc, 0)
	for _, stmt := range []string{
		`CREATE USER sch_owner`, `CREATE USER sch_user`, `CREATE USER sch_none`,
		`CREATE SCHEMA owned AUTHORIZATION sch_owner`,
		`CREATE TABLE owned.t (id INT PRIMARY KEY)`,
		`INSERT INTO owned.t VALUES (1)`,
		`GRANT SELECT ON owned.t TO sch_user`,
	} {
		mustExec(t, ctx, root, stmt)
	}
	owner := scopeConn(t, ctx, tc, "sch_owner", "datax")
	user := scopeConn(t, ctx, tc, "sch_user", "datax")
	none := scopeConn(t, ctx, tc, "sch_none", "datax")

	// The table's grant is not enough without USAGE on its schema.
	_, err := user.Exec(ctx, `SELECT * FROM owned.t`)
	if tempSQLState(err) != "42501" || !strings.Contains(err.Error(), "permission denied for schema owned") {
		t.Fatalf("SELECT without USAGE: %v", err)
	}
	schemaWant(t, ctx, user, `SELECT has_schema_privilege('owned', 'USAGE')`, "false")
	schemaWant(t, ctx, user, `SELECT count(*) FROM information_schema.schemata WHERE schema_name = 'owned'`, "0")
	// The owner grants USAGE; the table's own grant then decides.
	mustExec(t, ctx, owner, `GRANT USAGE ON SCHEMA owned TO sch_user`)
	schemaWant(t, ctx, user, `SELECT id FROM owned.t`, "1")
	schemaWant(t, ctx, user, `SELECT has_schema_privilege('owned', 'USAGE')`, "true")
	schemaWant(t, ctx, user, `SELECT has_schema_privilege('owned', 'CREATE')`, "false")
	schemaWant(t, ctx, user, `SELECT count(*) FROM information_schema.schemata WHERE schema_name = 'owned'`, "1")
	tempWantCode(t, ctx, user, `INSERT INTO owned.t VALUES (2)`, "42501")
	tempWantCode(t, ctx, user, `CREATE TABLE owned.mine (a INT)`, "42501")
	// CREATE on the schema lets it create there (and own what it makes).
	mustExec(t, ctx, owner, `GRANT CREATE ON SCHEMA owned TO sch_user`)
	mustExec(t, ctx, user, `CREATE TABLE owned.mine (a INT)`)
	mustExec(t, ctx, user, `INSERT INTO owned.mine VALUES (1)`)
	// PUBLIC holds nothing on a new schema; a grant to PUBLIC reaches
	// every role.
	tempWantCode(t, ctx, none, `SELECT * FROM owned.mine`, "42501")
	mustExec(t, ctx, owner, `GRANT USAGE ON SCHEMA owned TO PUBLIC`)
	mustExec(t, ctx, user, `GRANT SELECT ON owned.mine TO sch_none`)
	schemaWant(t, ctx, none, `SELECT a FROM owned.mine`, "1")
	mustExec(t, ctx, owner, `REVOKE USAGE ON SCHEMA owned FROM PUBLIC`)
	tempWantCode(t, ctx, none, `SELECT * FROM owned.mine`, "42501")
	schemaWant(t, ctx, root, `SELECT nspacl FROM pg_namespace WHERE nspname = 'owned'`, "{sch_owner=UC/sch_owner,sch_user=UC/sch_owner}")
	// Only the owner may grant, rename, drop, or give the schema away.
	tempWantCode(t, ctx, user, `GRANT USAGE ON SCHEMA owned TO sch_none`, "42501")
	tempWantCode(t, ctx, user, `DROP SCHEMA owned CASCADE`, "42501")
	tempWantCode(t, ctx, user, `ALTER SCHEMA owned OWNER TO sch_user`, "42501")
	tempWantCode(t, ctx, owner, `ALTER SCHEMA owned RENAME TO o2`, "42501") // needs CREATE on the database too
	// CREATE SCHEMA needs CREATE on the database.
	tempWantCode(t, ctx, none, `CREATE SCHEMA nope`, "42501")
	mustExec(t, ctx, root, `GRANT CREATE ON DATABASE datax TO sch_none`)
	mustExec(t, ctx, none, `CREATE SCHEMA nope`)
	schemaWant(t, ctx, root, `SELECT schema_owner FROM information_schema.schemata WHERE schema_name = 'nope'`, "sch_none")
	tempWantCode(t, ctx, none, `CREATE SCHEMA other AUTHORIZATION sch_owner`, "42501") // not a member
	// ALTER SCHEMA ... OWNER TO, by the owner (admins give to anyone).
	mustExec(t, ctx, root, `ALTER SCHEMA nope OWNER TO sch_user`)
	schemaWant(t, ctx, root, `SELECT pg_get_userbyid(nspowner) FROM pg_namespace WHERE nspname = 'nope'`, "sch_user")
	mustExec(t, ctx, user, `CREATE TABLE nope.x (a INT)`)
	// DROP ROLE refuses an owner of a schema; REASSIGN OWNED moves it;
	// DROP OWNED drops it.
	mustExec(t, ctx, root, `REVOKE CREATE ON DATABASE datax FROM sch_none`)
	_, err = root.Exec(ctx, `DROP ROLE sch_owner`)
	if tempSQLState(err) != "2BP01" || !strings.Contains(err.Error(), "schema owned") {
		t.Fatalf("DROP ROLE of a schema owner: %v", err)
	}
	mustExec(t, ctx, root, `REASSIGN OWNED BY sch_owner TO sch_user`)
	schemaWant(t, ctx, root, `SELECT schema_owner FROM information_schema.schemata WHERE schema_name = 'owned'`, "sch_user")
	tempWantCode(t, ctx, root, `DROP OWNED BY sch_user`, "2BP01") // owned.t is root's
	mustExec(t, ctx, root, `DROP OWNED BY sch_user CASCADE`)
	schemaWant(t, ctx, root, `SELECT count(*) FROM pg_namespace WHERE nspname IN ('owned', 'nope')`, "0")
	mustExec(t, ctx, root, `DROP ROLE sch_user`)
	mustExec(t, ctx, root, `DROP ROLE sch_owner`)

	// public keeps its rules: CREATE is the database owner's and its
	// grantees' (PostgreSQL 15), USAGE is PUBLIC's.
	tempWantCode(t, ctx, none, `CREATE TABLE public.nope (a INT)`, "42501")
	schemaWant(t, ctx, none, `SELECT has_schema_privilege('public', 'USAGE'), has_schema_privilege('public', 'CREATE')`, "true|false")
}

// TestSchemaSetSchema: ALTER TABLE / VIEW / SEQUENCE ... SET SCHEMA.
func TestSchemaSetSchema(t *testing.T) {
	tc := Start(t, 2)
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()
	a, b := pgConn(t, ctx, tc, 0), pgConn(t, ctx, tc, 1)

	mustExec(t, ctx, a, `CREATE SCHEMA s`)
	mustExec(t, ctx, a, `CREATE TABLE m (id INT PRIMARY KEY, v TEXT)`)
	mustExec(t, ctx, a, `CREATE INDEX m_v ON m (v)`)
	mustExec(t, ctx, a, `INSERT INTO m VALUES (1, 'one'), (2, 'two')`)
	schemaWant(t, ctx, b, `SELECT count(*) FROM m`, "2") // b caches m
	mustExec(t, ctx, a, `ALTER TABLE m SET SCHEMA s`)
	tempWantCode(t, ctx, b, `SELECT * FROM m`, "42P01")
	schemaWant(t, ctx, b, `SELECT v FROM s.m WHERE v = 'two'`, "two")
	mustExec(t, ctx, b, `INSERT INTO s.m VALUES (3, 'three')`)
	schemaWant(t, ctx, a, `SELECT relname FROM pg_class WHERE relnamespace = 's'::regnamespace ORDER BY 1`, "m,m_pkey,m_v")
	// Back again; the same name in the target is refused.
	mustExec(t, ctx, a, `CREATE TABLE m (x INT)`)
	tempWantCode(t, ctx, a, `ALTER TABLE s.m SET SCHEMA public`, "42P07")
	mustExec(t, ctx, a, `DROP TABLE m`)
	mustExec(t, ctx, a, `ALTER TABLE s.m SET SCHEMA public`)
	schemaWant(t, ctx, b, `SELECT count(*) FROM m`, "3")
	mustExec(t, ctx, a, `ALTER TABLE m SET SCHEMA public`) // already there
	tempWantCode(t, ctx, a, `ALTER TABLE m SET SCHEMA nosuch`, "3F000")
	tempWantCode(t, ctx, a, `ALTER TABLE nosuch SET SCHEMA s`, "42P01")
	mustExec(t, ctx, a, `ALTER TABLE IF EXISTS nosuch SET SCHEMA s`)

	// Views: one may move; a relation a view reads may not.
	mustExec(t, ctx, a, `CREATE VIEW mv AS SELECT id FROM public.m`)
	tempWantCode(t, ctx, a, `ALTER TABLE m SET SCHEMA s`, "2BP01")
	tempWantCode(t, ctx, a, `ALTER VIEW m SET SCHEMA s`, "42809")
	mustExec(t, ctx, a, `ALTER VIEW mv SET SCHEMA s`)
	schemaWant(t, ctx, b, `SELECT count(*) FROM s.mv`, "3")

	// Sequences, and tables owning sequences, move too (gap PG-D-035,
	// covered in pg_schema_objects_test.go); temporary tables do not.
	mustExec(t, ctx, a, `CREATE SEQUENCE q`)
	mustExec(t, ctx, a, `ALTER SEQUENCE q SET SCHEMA s`)
	mustExec(t, ctx, a, `ALTER SEQUENCE s.q SET SCHEMA public`)
	mustExec(t, ctx, a, `ALTER SEQUENCE q SET SCHEMA public`)
	mustExec(t, ctx, a, `CREATE TABLE ser (id SERIAL PRIMARY KEY)`)
	mustExec(t, ctx, a, `ALTER TABLE ser SET SCHEMA s`)
	schemaWant(t, ctx, b, `SELECT pg_get_serial_sequence('s.ser', 'id')`, "s.ser_id_seq")
	mustExec(t, ctx, a, `CREATE TEMP TABLE tmp (a INT)`)
	tempWantCode(t, ctx, a, `ALTER TABLE tmp SET SCHEMA s`, "0A000")
}

// TestSchemaViewsResolveThroughTheirOwnPath: a view's query resolves
// its names through the creator's path, never the reader's, nor the
// reader's temporary tables — or a reader could redirect a view's
// definer-rights query to a table of its choosing.
func TestSchemaViewsResolveThroughTheirOwnPath(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()
	tc := privCluster(t)
	root := pgConn(t, ctx, tc, 0)
	for _, stmt := range []string{
		`CREATE TABLE secrets (v TEXT)`, `INSERT INTO secrets VALUES ('public row')`,
		`CREATE SCHEMA hidden`, `CREATE TABLE hidden.secrets (v TEXT)`, `INSERT INTO hidden.secrets VALUES ('hidden row')`,
		`CREATE SCHEMA app`, `CREATE TABLE app.items (v TEXT)`, `INSERT INTO app.items VALUES ('app item')`,
		`CREATE VIEW pub_v AS SELECT v FROM secrets`,
		`SET search_path = app, public`,
		`CREATE VIEW app.items_v AS SELECT v FROM items`,
		`RESET search_path`,
		`CREATE USER vreader`,
		`GRANT SELECT ON pub_v TO vreader`,
		`GRANT USAGE ON SCHEMA app TO vreader`,
		`GRANT SELECT ON app.items_v TO vreader`,
		`GRANT USAGE ON SCHEMA hidden TO vreader`,
	} {
		mustExec(t, ctx, root, stmt)
	}
	r := scopeConn(t, ctx, tc, "vreader", "datax")
	schemaWant(t, ctx, r, `SELECT v FROM pub_v`, "public row")
	// The reader points its path at another schema holding a table of
	// the view's name, and makes a temporary one too: the view still
	// reads what its creator meant.
	mustExec(t, ctx, r, `SET search_path = hidden, public`)
	schemaWant(t, ctx, r, `SELECT v FROM pub_v`, "public row")
	// Nor when the statement itself names the table the reader's path
	// reaches (the statement's lookups are not the view's).
	mustExec(t, ctx, root, `GRANT SELECT ON hidden.secrets TO vreader`)
	schemaWant(t, ctx, r, `SELECT h.v || ' / ' || p.v FROM secrets h, pub_v p`, "hidden row / public row")
	schemaWant(t, ctx, r, `SELECT p.v || ' / ' || h.v FROM pub_v p, secrets h`, "public row / hidden row")
	mustExec(t, ctx, r, `CREATE TEMP TABLE secrets (v TEXT)`)
	mustExec(t, ctx, r, `INSERT INTO secrets VALUES ('temp row')`)
	schemaWant(t, ctx, r, `SELECT v FROM pub_v`, "public row")
	// A view made under app's path reads app's table for any reader.
	schemaWant(t, ctx, r, `SELECT v FROM app.items_v`, "app item")
	mustExec(t, ctx, r, `RESET search_path`)
	schemaWant(t, ctx, r, `SELECT v FROM app.items_v`, "app item")
	// Two views of one name from two schemas cannot be read together.
	mustExec(t, ctx, root, `CREATE VIEW hidden.pub_v AS SELECT v FROM hidden.secrets`)
	tempWantCode(t, ctx, root, `SELECT * FROM pub_v, hidden.pub_v`, "0A000")
	// Renaming a schema a view reads through is refused.
	tempWantCode(t, ctx, root, `ALTER SCHEMA app RENAME TO app2`, "2BP01")
	// DROP SCHEMA CASCADE takes the views of the schema along.
	mustExec(t, ctx, root, `DROP SCHEMA app CASCADE`)
	tempWantCode(t, ctx, root, `SELECT * FROM app.items_v`, "42P01")
}

// TestSchemaCatalogs: pg_namespace, pg_class, pg_tables, pg_indexes,
// information_schema, regclass, regnamespace and psql's \dn query.
func TestSchemaCatalogs(t *testing.T) {
	tc := Start(t, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()
	a := pgConn(t, ctx, tc, 0)
	for _, stmt := range []string{
		`CREATE SCHEMA sales`,
		`CREATE TABLE sales.orders (id INT PRIMARY KEY, total DECIMAL(10,2) NOT NULL)`,
		`CREATE INDEX orders_total ON sales.orders (total)`,
		`CREATE VIEW sales.big AS SELECT id FROM sales.orders WHERE total > 100`,
		`CREATE TABLE orders (id INT PRIMARY KEY)`,
	} {
		mustExec(t, ctx, a, stmt)
	}
	schemaWant(t, ctx, a, `SELECT n.nspname || '.' || c.relname || ':' || c.relkind FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
		WHERE c.relname IN ('orders', 'orders_pkey', 'orders_total', 'big') ORDER BY 1`,
		"public.orders:r,public.orders_pkey:i,sales.big:v,sales.orders:r,sales.orders_pkey:i,sales.orders_total:i")
	// psql's \dn.
	schemaWant(t, ctx, a, `SELECT n.nspname AS "Name", pg_catalog.pg_get_userbyid(n.nspowner) AS "Owner"
		FROM pg_catalog.pg_namespace n WHERE n.nspname !~ '^pg_' AND n.nspname <> 'information_schema' ORDER BY 1`, "public|root,sales|root")
	schemaWant(t, ctx, a, `SELECT schemaname || '.' || tablename FROM pg_tables WHERE tablename = 'orders' ORDER BY 1`, "public.orders,sales.orders")
	schemaWant(t, ctx, a, `SELECT schemaname || '.' || viewname FROM pg_views WHERE viewname = 'big'`, "sales.big")
	schemaWant(t, ctx, a, `SELECT schemaname FROM pg_indexes WHERE indexname = 'orders_total'`, "sales")
	schemaWant(t, ctx, a, `SELECT table_schema || '.' || table_name || ':' || table_type FROM information_schema.tables
		WHERE table_name IN ('orders', 'big') ORDER BY 1`, "public.orders:BASE TABLE,sales.big:VIEW,sales.orders:BASE TABLE")
	schemaWant(t, ctx, a, `SELECT table_schema || '.' || column_name FROM information_schema.columns WHERE table_name = 'orders' ORDER BY 1`,
		"public.id,sales.id,sales.total")
	schemaWant(t, ctx, a, `SELECT table_schema FROM information_schema.views WHERE table_name = 'big'`, "sales")
	schemaWant(t, ctx, a, `SELECT constraint_schema || '.' || constraint_name FROM information_schema.table_constraints
		WHERE table_name = 'orders' AND constraint_type = 'PRIMARY KEY' ORDER BY 1`, "public.orders_pkey,sales.orders_pkey")
	schemaWant(t, ctx, a, `SELECT schema_name FROM information_schema.schemata ORDER BY 1`, "information_schema,pg_catalog,public,sales")
	// regclass: a table off the path prints qualified; on it, bare.
	schemaWant(t, ctx, a, `SELECT 'sales.orders'::regclass::text, 'orders'::regclass::text, 'public.orders'::regclass::text`, "sales.orders|orders|orders")
	mustExec(t, ctx, a, `SET search_path = sales, public`)
	schemaWant(t, ctx, a, `SELECT 'sales.orders'::regclass::text`, "orders")
	schemaWant(t, ctx, a, `SELECT to_regclass('orders') = 'sales.orders'::regclass`, "true")
	mustExec(t, ctx, a, `RESET search_path`)
	// pg_table_is_visible, and psql's \dt with it: the path's tables.
	dt := `SELECT n.nspname || '.' || c.relname FROM pg_catalog.pg_class c LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
		WHERE c.relkind IN ('r','p','v') AND n.nspname <> 'pg_catalog' AND n.nspname !~ '^pg_toast' AND n.nspname <> 'information_schema'
		AND pg_catalog.pg_table_is_visible(c.oid) ORDER BY 1`
	schemaWant(t, ctx, a, dt, "public.orders")
	schemaWant(t, ctx, a, `SELECT pg_table_is_visible('sales.orders'::regclass), pg_table_is_visible('orders'::regclass)`, "false|true")
	mustExec(t, ctx, a, `SET search_path = sales`)
	schemaWant(t, ctx, a, dt, "sales.big,sales.orders")
	mustExec(t, ctx, a, `RESET search_path`)
	schemaWant(t, ctx, a, `SELECT 'sales'::regnamespace::text, to_regnamespace('sales') IS NOT NULL, to_regnamespace('nosuch') IS NULL`, "sales|true|true")
	schemaWant(t, ctx, a, `SELECT c.relnamespace = 'sales'::regnamespace FROM pg_class c WHERE c.oid = 'sales.orders'::regclass`, "true")
	tempWantCode(t, ctx, a, `SELECT 'nosuch'::regnamespace`, "3F000")
	schemaWant(t, ctx, a, `SELECT has_schema_privilege('sales', 'CREATE')`, "true")
	tempWantCode(t, ctx, a, `SELECT has_schema_privilege('nosuch', 'USAGE')`, "3F000")
}

// TestSchemaPublicSkipsTempTable: public.t names the public table
// even when a temporary table t shadows it (the parser used to reduce
// public.t to t); pg_temp.t names the temporary one.
func TestSchemaPublicSkipsTempTable(t *testing.T) {
	tc := Start(t, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	a := pgConn(t, ctx, tc, 0)
	mustExec(t, ctx, a, `CREATE TABLE t (v TEXT)`)
	mustExec(t, ctx, a, `INSERT INTO t VALUES ('public')`)
	mustExec(t, ctx, a, `CREATE TEMP TABLE t (v TEXT)`)
	mustExec(t, ctx, a, `INSERT INTO t VALUES ('temp')`)
	schemaWant(t, ctx, a, `SELECT v FROM t`, "temp")
	schemaWant(t, ctx, a, `SELECT v FROM public.t`, "public")
	schemaWant(t, ctx, a, `SELECT v FROM datax.public.t`, "public")
	schemaWant(t, ctx, a, `SELECT v FROM pg_temp.t`, "temp")
	mustExec(t, ctx, a, `INSERT INTO public.t VALUES ('public 2')`)
	schemaWant(t, ctx, a, `SELECT count(*) FROM public.t`, "2")
	schemaWant(t, ctx, a, `SELECT count(*) FROM t`, "1")
	mustExec(t, ctx, a, `DROP TABLE public.t`)
	schemaWant(t, ctx, a, `SELECT v FROM t`, "temp")
}

// TestSchemasNeedV22: CREATE SCHEMA is refused until the cluster
// finalizes v22; nothing else changes.
func TestSchemasNeedV22(t *testing.T) {
	tc := StartWithOptions(t, 1, func(c *server.Config) { c.BinaryVersionOverride = version.V21 })
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	a := pgConn(t, ctx, tc, 0)
	tempWantCode(t, ctx, a, `CREATE SCHEMA s`, "0A000")
	mustExec(t, ctx, a, `CREATE TABLE t (v TEXT)`)
	mustExec(t, ctx, a, `INSERT INTO public.t VALUES ('x')`)
	schemaWant(t, ctx, a, `SELECT v FROM t`, "x")
	schemaWant(t, ctx, a, `SHOW search_path`, `"$user", public`)
}

// TestUpgradeKeepsTablesResolving: a cluster created at v21 — tables
// in the default database and in another, a view, an index, foreign
// keys, written in the layout from before schemas — is rolled onto the
// current binary and finalized: every table resolves exactly as before
// by every name it had, its descriptor and name entry are untouched,
// and schemas work afterwards.
func TestUpgradeKeepsTablesResolving(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 240*time.Second)
	defer cancel()
	asV21 := func(c *server.Config) { c.BinaryVersionOverride = version.V21 }
	tc, engines := StartWithEngines(t, 3, asV21)
	tc.LeaderIndex(1)
	sess := func(i int) *sql.Session { return sql.NewSession(tc.Nodes[i].DB(), catalog.NewAccessor()) }
	a := sess(0)
	for _, stmt := range []string{
		`CREATE TABLE accounts (id INT PRIMARY KEY, owner TEXT)`,
		`CREATE INDEX accounts_owner ON accounts (owner)`,
		`CREATE TABLE entries (id INT PRIMARY KEY, account INT REFERENCES accounts (id), amount INT)`,
		`INSERT INTO accounts VALUES (1, 'ann'), (2, 'bob')`,
		`INSERT INTO entries VALUES (10, 1, 5), (11, 2, 7)`,
		`CREATE VIEW balances AS SELECT account, sum(amount) AS total FROM entries GROUP BY account`,
		`CREATE DATABASE app`,
		`CREATE TABLE app.cfg (k TEXT PRIMARY KEY, v TEXT)`,
		`INSERT INTO app.cfg VALUES ('mode', 'old')`,
	} {
		execSQL(t, ctx, a, stmt)
	}
	// The layout from before schemas: a descriptor with no schema_id,
	// filed under its plain name.
	rawEntry := func(dbName, name string) (string, string) {
		var entry, desc string
		if err := tc.Nodes[1].DB().RunTxn(ctx, "raw-entry", func(ctx context.Context, txn *kvclient.Txn) error {
			db, err := catalog.LookupDatabase(ctx, txn, dbName)
			if err != nil {
				return err
			}
			id, err := txn.Get(ctx, keys.TableNamespaceKey(db.ID, name))
			if err != nil || id == nil {
				return err
			}
			entry = string(id)
			var tid uint64
			for _, c := range entry {
				tid = tid*10 + uint64(c-'0')
			}
			raw, err := txn.Get(ctx, keys.TableDescKey(tid))
			desc = string(raw)
			return err
		}); err != nil {
			t.Fatal(err)
		}
		return entry, desc
	}
	before := map[string][2]string{}
	for _, n := range [][2]string{{"datax", "accounts"}, {"datax", "entries"}, {"datax", "balances"}, {"app", "cfg"}} {
		e, d := rawEntry(n[0], n[1])
		if e == "" || strings.Contains(d, "schema_id") {
			t.Fatalf("%s.%s before the upgrade: entry %q, descriptor %s", n[0], n[1], e, d)
		}
		before[n[0]+"."+n[1]] = [2]string{e, d}
	}

	for i := 0; i < 3; i++ {
		tc.StopNode(i)
		tc.RestartNode(i, engines[i])
		tc.LeaderIndex(1)
	}
	waitForAdvertisedVersion(t, ctx, tc.Nodes[0].Addr(), []base.NodeID{1, 2, 3}, int(version.Current))
	a = sess(0)
	sessWantCode(t, ctx, a, `CREATE SCHEMA early`, "0A000")
	if resp := adminCall(t, ctx, tc.Nodes[0].Addr(), cluster.AdminRequest{Op: "upgrade-cluster", Version: int(version.V22)}); resp.Error != "" || resp.ClusterVersion != int(version.V22) {
		t.Fatalf("finalize v22: %+v", resp)
	}
	for n, was := range before {
		parts := strings.SplitN(n, ".", 2)
		if e, d := rawEntry(parts[0], parts[1]); e != was[0] || d != was[1] {
			t.Fatalf("%s changed on disk across the upgrade:\n entry %q -> %q\n desc %s\n   -> %s", n, was[0], e, was[1], d)
		}
	}
	for i := range tc.Nodes {
		c := sess(i)
		sessWant(t, ctx, c, `SELECT owner FROM accounts WHERE owner = 'bob'`, "bob")
		sessWant(t, ctx, c, `SELECT owner FROM public.accounts WHERE id = 1`, "ann")
		sessWant(t, ctx, c, `SELECT owner FROM datax.accounts WHERE id = 1`, "ann")
		sessWant(t, ctx, c, `SELECT owner FROM datax.public.accounts WHERE id = 2`, "bob")
		sessWant(t, ctx, c, `SELECT total FROM balances ORDER BY account`, "5,7")
		sessWant(t, ctx, c, `SELECT v FROM app.cfg`, "old")
		sessWant(t, ctx, c, `SELECT v FROM app.public.cfg`, "old")
		sessWantCode(t, ctx, c, `INSERT INTO entries VALUES (12, 99, 1)`, "23503")
	}
	execSQL(t, ctx, a, `CREATE SCHEMA ledger`)
	execSQL(t, ctx, a, `CREATE TABLE ledger.accounts (id INT PRIMARY KEY)`)
	sessWant(t, ctx, a, `SELECT count(*) FROM accounts`, "2")
	sessWant(t, ctx, a, `SELECT count(*) FROM ledger.accounts`, "0")
	// A node restarted after finalize serves both.
	tc.StopNode(2)
	tc.RestartNode(2, engines[2])
	tc.LeaderIndex(1)
	c := sess(2)
	sessWant(t, ctx, c, `SELECT count(*) FROM accounts`, "2")
	sessWant(t, ctx, c, `SELECT count(*) FROM ledger.accounts`, "0")
}

// TestSchemaBackupRestore: a cluster backup carries the schemas and
// their tables; the restored cluster resolves them, and allocates no
// descriptor ID a restored schema holds.
func TestSchemaBackupRestore(t *testing.T) {
	tc := Start(t, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 240*time.Second)
	defer cancel()
	a := pgConn(t, ctx, tc, 0)
	for _, stmt := range []string{
		`CREATE TABLE t (v TEXT)`, `INSERT INTO t VALUES ('public')`,
		`CREATE USER bk_user`,
		`CREATE SCHEMA bk AUTHORIZATION bk_user`,
		`CREATE TABLE bk.t (id INT PRIMARY KEY, v TEXT)`, `INSERT INTO bk.t VALUES (1, 'bk')`,
		`CREATE INDEX bk_t_v ON bk.t (v)`,
		`GRANT USAGE ON SCHEMA bk TO PUBLIC`,
		`CREATE SCHEMA late`, // the highest descriptor ID
	} {
		mustExec(t, ctx, a, stmt)
	}
	dir := t.TempDir()
	if _, err := tc.Nodes[0].RunBackup(ctx, dir, "", false, false); err != nil {
		t.Fatalf("backup: %v", err)
	}
	tc2 := Start(t, 1)
	if _, err := tc2.Nodes[0].RunRestore(ctx, []string{dir}); err != nil {
		t.Fatalf("restore: %v", err)
	}
	r := pgConn(t, ctx, tc2, 0)
	schemaWant(t, ctx, r, `SELECT v FROM t`, "public")
	schemaWant(t, ctx, r, `SELECT v FROM bk.t WHERE v = 'bk'`, "bk")
	schemaWant(t, ctx, r, `SELECT schema_name || ':' || schema_owner FROM information_schema.schemata WHERE schema_name IN ('bk', 'late') ORDER BY 1`, "bk:bk_user,late:root")
	schemaWant(t, ctx, r, `SELECT nspacl FROM pg_namespace WHERE nspname = 'bk'`, "{bk_user=UC/bk_user,=U/bk_user}")
	// New schemas get new IDs: the restored ones stay intact.
	mustExec(t, ctx, r, `CREATE SCHEMA after_restore`)
	mustExec(t, ctx, r, `CREATE TABLE after_restore.x (a INT)`)
	schemaWant(t, ctx, r, `SELECT count(*) FROM pg_namespace WHERE nspname IN ('bk', 'late', 'after_restore')`, "3")
	schemaWant(t, ctx, r, `SELECT v FROM bk.t`, "bk")
}
