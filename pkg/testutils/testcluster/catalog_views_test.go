package testcluster

import (
	"context"
	"os/exec"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/sthorne/datax/pkg/sql"
	"github.com/sthorne/datax/pkg/sql/catalog"
)

// TestCatalogViewsSmoke: the virtual tables answer through the session
// with the right shapes.
func TestCatalogViewsSmoke(t *testing.T) {
	tc := Start(t, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	root := sql.NewSession(tc.Nodes[0].DB(), catalog.NewAccessor())
	waitForDatabases(t, ctx, root)
	execSQL(t, ctx, root, `CREATE TABLE t (id INT8 PRIMARY KEY, name TEXT NOT NULL, v FLOAT8)`)
	execSQL(t, ctx, root, `CREATE UNIQUE INDEX t_name ON t (name)`)
	r := execSQL(t, ctx, root, `SELECT relname, relkind FROM pg_catalog.pg_class WHERE relnamespace = 2200 ORDER BY relname`)
	if len(r.Rows) != 3 || r.Rows[0][0].S != "t" || r.Rows[1][0].S != "t_name" || r.Rows[2][0].S != "t_pkey" {
		t.Fatalf("pg_class: %+v", r.Rows)
	}
	r = execSQL(t, ctx, root, `SELECT attname, attnum, attnotnull FROM pg_attribute WHERE attrelid = (SELECT oid FROM pg_class WHERE relname = 't') ORDER BY attnum`)
	if len(r.Rows) != 3 || r.Rows[1][0].S != "name" || !r.Rows[1][2].B {
		t.Fatalf("pg_attribute: %+v", r.Rows)
	}
	r = execSQL(t, ctx, root, `SELECT column_name, data_type, is_nullable FROM information_schema.columns WHERE table_name = 't' ORDER BY ordinal_position`)
	if len(r.Rows) != 3 || r.Rows[0][1].S != "bigint" || r.Rows[2][2].S != "YES" {
		t.Fatalf("information_schema.columns: %+v", r.Rows)
	}
	r = execSQL(t, ctx, root, `SELECT datname FROM pg_database ORDER BY datname`)
	if len(r.Rows) != 2 || r.Rows[0][0].S != "datax" {
		t.Fatalf("pg_database: %+v", r.Rows)
	}
	r = execSQL(t, ctx, root, `SELECT c.relname, n.nspname FROM pg_catalog.pg_class AS c LEFT JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace WHERE c.relkind = 'r' ORDER BY c.relname`)
	if len(r.Rows) != 1 || r.Rows[0][1].S != "public" {
		t.Fatalf("pg_class join pg_namespace: %+v", r.Rows)
	}
	if _, serr := trySQL(ctx, root, `INSERT INTO pg_class VALUES (1)`); serr == nil || serr.Code != sql.CodeInsufficientPriv {
		t.Fatalf("write to a catalog: %v", serr)
	}
	r = execSQL(t, ctx, root, `SELECT rolname, rolsuper FROM pg_roles`)
	if len(r.Rows) != 1 || r.Rows[0][0].S != "root" || !r.Rows[0][1].B {
		t.Fatalf("pg_roles: %+v", r.Rows)
	}
}

// psqlDescribeQueries are the statements psql 16 sends for \d t against a
// PostgreSQL-14 server, in order, with the table's OID as $oid — the
// exact text psql sends, so a parser regression shows up here first.
var psqlDescribeQueries = []string{
	`SELECT c.oid,
  n.nspname,
  c.relname
FROM pg_catalog.pg_class c
     LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
WHERE c.relname OPERATOR(pg_catalog.~) '^(t)$' COLLATE pg_catalog.default
  AND pg_catalog.pg_table_is_visible(c.oid)
ORDER BY 2, 3;`,
	`SELECT c.relchecks, c.relkind, c.relhasindex, c.relhasrules, c.relhastriggers, c.relrowsecurity, c.relforcerowsecurity, false AS relhasoids, c.relispartition, '', c.reltablespace, CASE WHEN c.reloftype = 0 THEN '' ELSE c.reloftype::pg_catalog.regtype::pg_catalog.text END, c.relpersistence, c.relreplident, am.amname
FROM pg_catalog.pg_class c
 LEFT JOIN pg_catalog.pg_class tc ON (c.reltoastrelid = tc.oid)
LEFT JOIN pg_catalog.pg_am am ON (c.relam = am.oid)
WHERE c.oid = '$oid';`,
	`SELECT a.attname,
  pg_catalog.format_type(a.atttypid, a.atttypmod),
  (SELECT pg_catalog.pg_get_expr(d.adbin, d.adrelid, true)
   FROM pg_catalog.pg_attrdef d
   WHERE d.adrelid = a.attrelid AND d.adnum = a.attnum AND a.atthasdef),
  a.attnotnull,
  (SELECT c.collname FROM pg_catalog.pg_collation c, pg_catalog.pg_type t
   WHERE c.oid = a.attcollation AND t.oid = a.atttypid AND a.attcollation <> t.typcollation) AS attcollation,
  a.attidentity,
  a.attgenerated
FROM pg_catalog.pg_attribute a
WHERE a.attrelid = '$oid' AND a.attnum > 0 AND NOT a.attisdropped
ORDER BY a.attnum;`,
	`SELECT c2.relname, i.indisprimary, i.indisunique, i.indisclustered, i.indisvalid, pg_catalog.pg_get_indexdef(i.indexrelid, 0, true),
  pg_catalog.pg_get_constraintdef(con.oid, true), contype, condeferrable, condeferred, i.indisreplident, c2.reltablespace
FROM pg_catalog.pg_class c, pg_catalog.pg_class c2, pg_catalog.pg_index i
  LEFT JOIN pg_catalog.pg_constraint con ON (conrelid = i.indrelid AND conindid = i.indexrelid AND contype IN ('p','u','x'))
WHERE c.oid = '$oid' AND c.oid = i.indrelid AND i.indexrelid = c2.oid
ORDER BY i.indisprimary DESC, c2.relname;`,
	`SELECT pol.polname, pol.polpermissive,
  CASE WHEN pol.polroles = '{0}' THEN NULL ELSE pg_catalog.array_to_string(array(select rolname from pg_catalog.pg_roles where oid = any (pol.polroles) order by 1),',') END,
  pg_catalog.pg_get_expr(pol.polqual, pol.polrelid),
  pg_catalog.pg_get_expr(pol.polwithcheck, pol.polrelid),
  CASE pol.polcmd
    WHEN 'r' THEN 'SELECT'
    WHEN 'a' THEN 'INSERT'
    WHEN 'w' THEN 'UPDATE'
    WHEN 'd' THEN 'DELETE'
    END AS cmd
FROM pg_catalog.pg_policy pol
WHERE pol.polrelid = '$oid' ORDER BY 1;`,
	`SELECT oid, stxrelid::pg_catalog.regclass, stxnamespace::pg_catalog.regnamespace::pg_catalog.text AS nsp, stxname,
pg_catalog.pg_get_statisticsobjdef_columns(oid) AS columns,
  'd' = any(stxkind) AS ndist_enabled,
  'f' = any(stxkind) AS deps_enabled,
  'm' = any(stxkind) AS mcv_enabled,
stxstattarget
FROM pg_catalog.pg_statistic_ext
WHERE stxrelid = '$oid'
ORDER BY nsp, stxname;`,
	`SELECT pubname
     , NULL
     , NULL
FROM pg_catalog.pg_publication p
JOIN pg_catalog.pg_publication_rel pr ON p.oid = pr.prpubid
JOIN pg_catalog.pg_class c ON c.oid = pr.prrelid
WHERE pr.prrelid = '$oid'
UNION ALL
SELECT pubname
     , NULL
     , NULL
FROM pg_catalog.pg_publication p
WHERE p.puballtables AND pg_catalog.pg_relation_is_publishable('$oid')
ORDER BY 1;`,
	`SELECT c.oid::pg_catalog.regclass
FROM pg_catalog.pg_class c, pg_catalog.pg_inherits i
WHERE c.oid = i.inhparent AND i.inhrelid = '$oid'
  AND c.relkind != 'p' AND c.relkind != 'I'
ORDER BY inhseqno;`,
	`SELECT c.oid::pg_catalog.regclass, c.relkind, inhdetachpending, pg_catalog.pg_get_expr(c.relpartbound, c.oid)
FROM pg_catalog.pg_class c, pg_catalog.pg_inherits i
WHERE c.oid = i.inhrelid AND i.inhparent = '$oid'
ORDER BY pg_catalog.pg_get_expr(c.relpartbound, c.oid) = 'DEFAULT', c.oid::pg_catalog.regclass::pg_catalog.text;`,
}

// psqlListQueries are the statements behind \l, \dt, \di, \du, \dn, \dp
// and \dT (psql 16, PostgreSQL-14 server), keyed by the command.
var psqlListQueries = map[string]string{
	`\l`: `SELECT
  d.datname as "Name",
  pg_catalog.pg_get_userbyid(d.datdba) as "Owner",
  pg_catalog.pg_encoding_to_char(d.encoding) as "Encoding",
  'libc' AS "Locale Provider",
  d.datcollate as "Collate",
  d.datctype as "Ctype",
  NULL as "ICU Locale",
  NULL as "ICU Rules",
  pg_catalog.array_to_string(d.datacl, E'\n') AS "Access privileges"
FROM pg_catalog.pg_database d
ORDER BY 1;`,
	`\dt`: `SELECT n.nspname as "Schema",
  c.relname as "Name",
  CASE c.relkind WHEN 'r' THEN 'table' WHEN 'v' THEN 'view' WHEN 'm' THEN 'materialized view' WHEN 'i' THEN 'index' WHEN 'S' THEN 'sequence' WHEN 't' THEN 'TOAST table' WHEN 'f' THEN 'foreign table' WHEN 'p' THEN 'partitioned table' WHEN 'I' THEN 'partitioned index' END as "Type",
  pg_catalog.pg_get_userbyid(c.relowner) as "Owner"
FROM pg_catalog.pg_class c
     LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
     LEFT JOIN pg_catalog.pg_am am ON am.oid = c.relam
WHERE c.relkind IN ('r','p','')
      AND n.nspname <> 'pg_catalog'
      AND n.nspname !~ '^pg_toast'
      AND n.nspname <> 'information_schema'
  AND pg_catalog.pg_table_is_visible(c.oid)
ORDER BY 1,2;`,
	`\di`: `SELECT n.nspname as "Schema",
  c.relname as "Name",
  CASE c.relkind WHEN 'r' THEN 'table' WHEN 'v' THEN 'view' WHEN 'm' THEN 'materialized view' WHEN 'i' THEN 'index' WHEN 'S' THEN 'sequence' WHEN 't' THEN 'TOAST table' WHEN 'f' THEN 'foreign table' WHEN 'p' THEN 'partitioned table' WHEN 'I' THEN 'partitioned index' END as "Type",
  pg_catalog.pg_get_userbyid(c.relowner) as "Owner",
  c2.relname as "Table"
FROM pg_catalog.pg_class c
     LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
     LEFT JOIN pg_catalog.pg_am am ON am.oid = c.relam
     LEFT JOIN pg_catalog.pg_index i ON i.indexrelid = c.oid
     LEFT JOIN pg_catalog.pg_class c2 ON i.indrelid = c2.oid
WHERE c.relkind IN ('i','I','')
      AND n.nspname <> 'pg_catalog'
      AND n.nspname !~ '^pg_toast'
      AND n.nspname <> 'information_schema'
  AND pg_catalog.pg_table_is_visible(c.oid)
ORDER BY 1,2;`,
	`\du`: `SELECT r.rolname, r.rolsuper, r.rolinherit,
  r.rolcreaterole, r.rolcreatedb, r.rolcanlogin,
  r.rolconnlimit, r.rolvaliduntil
, r.rolreplication
, r.rolbypassrls
FROM pg_catalog.pg_roles r
WHERE r.rolname !~ '^pg_'
ORDER BY 1;`,
	`\dn`: `SELECT n.nspname AS "Name",
  pg_catalog.pg_get_userbyid(n.nspowner) AS "Owner"
FROM pg_catalog.pg_namespace n
WHERE n.nspname !~ '^pg_' AND n.nspname <> 'information_schema'
ORDER BY 1;`,
	`\dp`: `SELECT n.nspname as "Schema",
  c.relname as "Name",
  CASE c.relkind WHEN 'r' THEN 'table' WHEN 'v' THEN 'view' WHEN 'm' THEN 'materialized view' WHEN 'S' THEN 'sequence' WHEN 'f' THEN 'foreign table' WHEN 'p' THEN 'partitioned table' END as "Type",
  pg_catalog.array_to_string(c.relacl, E'\n') AS "Access privileges",
  pg_catalog.array_to_string(ARRAY(
    SELECT attname || E':\n  ' || pg_catalog.array_to_string(attacl, E'\n  ')
    FROM pg_catalog.pg_attribute a
    WHERE attrelid = c.oid AND NOT attisdropped AND attacl IS NOT NULL
  ), E'\n') AS "Column privileges",
  pg_catalog.array_to_string(ARRAY(
    SELECT polname
    || CASE WHEN NOT polpermissive THEN
       E' (RESTRICTIVE)'
       ELSE '' END
    || CASE WHEN polcmd != '*' THEN
           E' (' || polcmd::pg_catalog.text || E'):'
       ELSE E':'
       END
    || CASE WHEN polqual IS NOT NULL THEN
           E'\n  (u): ' || pg_catalog.pg_get_expr(polqual, polrelid)
       ELSE E''
       END
    || CASE WHEN polwithcheck IS NOT NULL THEN
           E'\n  (c): ' || pg_catalog.pg_get_expr(polwithcheck, polrelid)
       ELSE E''
       END    || CASE WHEN polroles <> '{0}' THEN
           E'\n  to: ' || pg_catalog.array_to_string(
               ARRAY(
                   SELECT rolname
                   FROM pg_catalog.pg_roles
                   WHERE oid = ANY (polroles)
                   ORDER BY 1
               ), E', ')
       ELSE E''
       END
    FROM pg_catalog.pg_policy pol
    WHERE polrelid = c.oid), E'\n')
    AS "Policies"
FROM pg_catalog.pg_class c
     LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
WHERE c.relkind IN ('r','v','m','S','f','p')
      AND n.nspname <> 'pg_catalog'
      AND n.nspname <> 'information_schema'
  AND pg_catalog.pg_table_is_visible(c.oid)
ORDER BY 1, 2;`,
	`\dT`: `SELECT n.nspname as "Schema",
  pg_catalog.format_type(t.oid, NULL) AS "Name",
  pg_catalog.obj_description(t.oid, 'pg_type') as "Description"
FROM pg_catalog.pg_type t
     LEFT JOIN pg_catalog.pg_namespace n ON n.oid = t.typnamespace
WHERE (t.typrelid = 0 OR (SELECT c.relkind = 'c' FROM pg_catalog.pg_class c WHERE c.oid = t.typrelid))
  AND NOT EXISTS(SELECT 1 FROM pg_catalog.pg_type el WHERE el.oid = t.typelem AND el.typarray = t.oid)
      AND n.nspname <> 'pg_catalog'
      AND n.nspname <> 'information_schema'
  AND pg_catalog.pg_type_is_visible(t.oid)
ORDER BY 1, 2;`,
}

// TestPsqlCatalogQueries: the exact catalog queries psql sends for \d,
// \l, \dt, \di, \du, \dn, \dp and \dT run over the wire and return what
// psql needs to render — and psql itself, when it is installed, renders
// the table without an error.
func TestPsqlCatalogQueries(t *testing.T) {
	tc := Start(t, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	root := sql.NewSession(tc.Nodes[0].DB(), catalog.NewAccessor())
	waitForDatabases(t, ctx, root)
	execSQL(t, ctx, root, `CREATE TABLE t (id INT8 PRIMARY KEY, name TEXT NOT NULL, qty INT8 DEFAULT 0)`)
	execSQL(t, ctx, root, `CREATE INDEX t_qty ON t (qty)`)
	execSQL(t, ctx, root, `CREATE UNIQUE INDEX t_name ON t (name)`)
	execSQL(t, ctx, root, `CREATE USER bob PASSWORD 'x'`)
	execSQL(t, ctx, root, `GRANT SELECT ON t TO bob`)

	url := "postgres://root@" + tc.Nodes[0].SQLAddr() + "/datax?sslmode=disable"
	conn, err := pgx.Connect(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(ctx)
	rowsOf := func(q string) [][]any {
		t.Helper()
		rows, err := conn.Query(ctx, q)
		if err != nil {
			t.Fatalf("%v\n%s", err, q)
		}
		out, err := pgx.CollectRows(rows, func(r pgx.CollectableRow) ([]any, error) { return r.Values() })
		if err != nil {
			t.Fatalf("%v\n%s", err, q)
		}
		return out
	}

	// \d t, step by step.
	first := rowsOf(psqlDescribeQueries[0])
	if len(first) != 1 || first[0][1] != "public" || first[0][2] != "t" {
		t.Fatalf("relation lookup: %v", first)
	}
	oid := first[0][0].(int64)
	for i, q := range psqlDescribeQueries[1:] {
		q = strings.ReplaceAll(q, "$oid", strconv.FormatInt(oid, 10))
		got := rowsOf(q)
		switch i {
		case 0: // relation facts
			if len(got) != 1 || got[0][1] != "r" || got[0][len(got[0])-1] != "heap" {
				t.Fatalf("relation facts: %v", got)
			}
		case 1: // columns
			if len(got) != 3 || got[0][0] != "id" || got[0][1] != "bigint" || got[2][2] != "0" || got[0][3] != true {
				t.Fatalf("columns: %v", got)
			}
		case 2: // indexes: the primary key first, then by name
			if len(got) != 3 || got[0][0] != "t_pkey" || got[0][1] != true || got[1][0] != "t_name" || got[2][0] != "t_qty" {
				t.Fatalf("indexes: %v", got)
			}
			if got[0][6] != "PRIMARY KEY (id)" || got[1][6] != "UNIQUE (name)" || got[2][6] != nil {
				t.Fatalf("constraint defs: %v", got)
			}
		default: // policies, extended stats, publications, inheritance: empty
			if len(got) != 0 {
				t.Fatalf("query %d: expected no rows, got %v", i+1, got)
			}
		}
	}

	// The list commands.
	want := map[string]int{`\l`: 2, `\dt`: 1, `\di`: 3, `\du`: 2, `\dn`: 1, `\dp`: 1, `\dT`: 0}
	for cmd, q := range psqlListQueries {
		if got := rowsOf(q); len(got) != want[cmd] {
			t.Fatalf("%s: %d rows, want %d: %v", cmd, len(got), want[cmd], got)
		}
	}

	// psql itself, when available: every describe command renders
	// without an error on stderr.
	psql, err := exec.LookPath("psql")
	if err != nil {
		t.Log("psql not installed; skipping the end-to-end run")
		return
	}
	for _, cmd := range []string{`\l`, `\dt`, `\di`, `\du`, `\dn`, `\d`, `\d t`, `\d+ t`, `\d t_qty`, `\dp`, `\dT`, `\db`, `\dx`, `\dt+`, `\l+`} {
		out, err := exec.CommandContext(ctx, psql, url, "-c", cmd).CombinedOutput()
		if err != nil || strings.Contains(string(out), "ERROR") {
			t.Fatalf("psql %s: %v\n%s", cmd, err, out)
		}
		if cmd == `\d t` {
			for _, s := range []string{`"t_pkey" PRIMARY KEY, btree (id)`, `"t_name" UNIQUE CONSTRAINT, btree (name)`, `"t_qty" btree (qty)`, `qty    | bigint |           |          | 0`} {
				if !strings.Contains(string(out), s) {
					t.Fatalf("psql \\d t lacks %q:\n%s", s, out)
				}
			}
		}
	}
}

// TestSQLSurfaceForTools: the SQL that psql, drivers and ORMs lean on
// beyond the catalogs — UNION [ALL], LIKE / ILIKE, = ANY (array),
// 'name'::regclass, comparisons as values, ORDER BY expressions and
// output aliases, correlated subqueries in the select list of a join,
// FROM unnest(...), and the SHOW family.
func TestSQLSurfaceForTools(t *testing.T) {
	tc := Start(t, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	root := sql.NewSession(tc.Nodes[0].DB(), catalog.NewAccessor())
	waitForDatabases(t, ctx, root)
	execSQL(t, ctx, root, `CREATE TABLE t (id INT8 PRIMARY KEY, name TEXT NOT NULL, qty INT8 DEFAULT 0)`)
	execSQL(t, ctx, root, `CREATE INDEX t_qty ON t (qty)`)
	execSQL(t, ctx, root, `CREATE TABLE u (id INT8 PRIMARY KEY, tid INT8, tag TEXT)`)
	execSQL(t, ctx, root, `INSERT INTO t VALUES (1, 'Alice', 5), (2, 'bob', 3), (3, 'carol', 3)`)
	execSQL(t, ctx, root, `INSERT INTO u VALUES (10, 1, 'x'), (11, 1, 'y'), (12, 3, 'z')`)
	execSQL(t, ctx, root, `CREATE USER bob PASSWORD 'x'`)
	execSQL(t, ctx, root, `GRANT SELECT, INSERT ON t TO bob`)

	names := func(r *sql.Result) []string {
		var out []string
		for _, row := range r.Rows {
			out = append(out, row[0].Text())
		}
		return out
	}
	eq := func(what string, got []string, want ...string) {
		t.Helper()
		if strings.Join(got, ",") != strings.Join(want, ",") {
			t.Fatalf("%s: got %v, want %v", what, got, want)
		}
	}

	eq("UNION ALL", names(execSQL(t, ctx, root, `SELECT name FROM t WHERE id = 1 UNION ALL SELECT tag FROM u WHERE tid = 1 ORDER BY 1`)), "Alice", "x", "y")
	eq("UNION dedupes", names(execSQL(t, ctx, root, `SELECT qty FROM t UNION SELECT qty FROM t ORDER BY qty`)), "3", "5")
	if _, serr := trySQL(ctx, root, `SELECT id, name FROM t UNION SELECT id FROM u`); serr == nil {
		t.Fatal("UNION with mismatched columns was accepted")
	}
	eq("LIKE", names(execSQL(t, ctx, root, `SELECT name FROM t WHERE name LIKE 'c%' OR name NOT LIKE '%o%' ORDER BY name`)), "Alice", "carol")
	eq("ILIKE", names(execSQL(t, ctx, root, `SELECT name FROM t WHERE name ILIKE 'a_ice'`)), "Alice")
	eq("= ANY", names(execSQL(t, ctx, root, `SELECT name FROM t WHERE qty = ANY ('{3,4}') ORDER BY name`)), "bob", "carol")
	eq("<> ALL", names(execSQL(t, ctx, root, `SELECT name FROM t WHERE qty <> ALL ('{3,4}')`)), "Alice")
	eq("= ANY (subquery)", names(execSQL(t, ctx, root, `SELECT name FROM t WHERE id = ANY (SELECT tid FROM u) ORDER BY name`)), "Alice", "carol")
	r := execSQL(t, ctx, root, `SELECT 't'::regclass = (SELECT oid FROM pg_class WHERE relname = 't') AS same, 'pg_catalog.pg_class'::regclass > 0`)
	if len(r.Rows) != 1 || !r.Rows[0][0].B || !r.Rows[0][1].B || r.Columns[0].Name != "same" {
		t.Fatalf("regclass: %+v %+v", r.Columns, r.Rows)
	}
	if _, serr := trySQL(ctx, root, `SELECT 'nope'::regclass`); serr == nil || serr.Code != sql.CodeUndefinedTable {
		t.Fatalf("unknown regclass: %v", serr)
	}
	eq("comparison as value", names(execSQL(t, ctx, root, `SELECT qty > 3 FROM t ORDER BY id`)), "t", "f", "f")
	eq("boolean expression as value", names(execSQL(t, ctx, root, `SELECT (NOT qty > 3) AND EXISTS (SELECT 1 FROM u WHERE u.tid = t.id) FROM t ORDER BY id`)), "f", "f", "t")
	eq("ORDER BY expression", names(execSQL(t, ctx, root, `SELECT name FROM t ORDER BY qty = 3, name DESC`)), "Alice", "carol", "bob")
	eq("ORDER BY output alias", names(execSQL(t, ctx, root, `SELECT name AS n FROM t ORDER BY n DESC`)), "carol", "bob", "Alice")
	eq("ORDER BY alias of an expression", names(execSQL(t, ctx, root, `SELECT name, qty * 2 AS d FROM t ORDER BY d, name`)), "bob", "carol", "Alice")
	eq("correlated select-list subquery", names(execSQL(t, ctx, root, `SELECT (SELECT count(*) FROM u WHERE u.tid = t.id) FROM t ORDER BY id`)), "2", "0", "1")
	eq("array(select ...) correlated over a join", names(execSQL(t, ctx, root,
		`SELECT array_to_string(array(SELECT tag FROM u WHERE u.tid = t.id ORDER BY 1), ',') FROM t JOIN t AS t2 ON t2.id = t.id ORDER BY t.id`)), "x,y", "", "z")
	eq("correlated WHERE over a join", names(execSQL(t, ctx, root,
		`SELECT t.name FROM t JOIN u ON u.tid = t.id WHERE EXISTS (SELECT 1 FROM u AS u2 WHERE u2.id = u.id + 1) ORDER BY t.name`)), "Alice", "Alice")
	eq("ON filters", names(execSQL(t, ctx, root, `SELECT t.name FROM t LEFT JOIN u ON (u.tid = t.id AND u.tag IN ('x', 'z')) WHERE u.id IS NULL`)), "bob")
	eq("comma join", names(execSQL(t, ctx, root, `SELECT u.tag FROM t, u WHERE t.id = u.tid AND t.name = 'carol'`)), "z")
	eq("unnest", names(execSQL(t, ctx, root, `SELECT x FROM unnest('{b,a,"c d"}') AS s(x) ORDER BY x`)), "a", "b", "c d")
	eq("CAST", names(execSQL(t, ctx, root, `SELECT CAST(qty AS text) FROM t WHERE id = 1`)), "5")
	eq("subquery as predicate", names(execSQL(t, ctx, root, `SELECT name FROM t WHERE (SELECT count(*) FROM u WHERE u.tid = t.id) = 2 OR (SELECT true)`)), "Alice", "bob", "carol")
	if _, serr := trySQL(ctx, root, `SELECT nosuchfn(1)`); serr == nil || serr.Code != sql.CodeUndefinedFunction {
		t.Fatalf("unknown function: %v", serr)
	}

	// SHOW.
	eq("SHOW TABLES", names(execSQL(t, ctx, root, `SHOW TABLES`)), "t", "u")
	if r := execSQL(t, ctx, root, `SHOW TABLES FROM system`); len(r.Rows) != 0 {
		t.Fatalf("SHOW TABLES FROM system: %v", r.Rows)
	}
	r = execSQL(t, ctx, root, `SHOW COLUMNS FROM t`)
	if len(r.Rows) != 3 || r.Rows[0][0].S != "id" || r.Rows[0][1].S != "bigint" || r.Rows[0][2].B || r.Rows[2][3].Text() != "0" || r.Rows[2][4].S != "{t_qty}" {
		t.Fatalf("SHOW COLUMNS: %+v", r.Rows)
	}
	r = execSQL(t, ctx, root, `SHOW INDEXES FROM t`)
	if len(r.Rows) != 2 || r.Rows[0][1].S != "t_pkey" || r.Rows[1][1].S != "t_qty" || !r.Rows[1][2].B || r.Rows[1][4].S != "qty" {
		t.Fatalf("SHOW INDEXES: %+v", r.Rows)
	}
	r = execSQL(t, ctx, root, `SHOW CREATE TABLE t`)
	if len(r.Rows) != 1 || !strings.Contains(r.Rows[0][1].S, "INDEX t_qty (qty)") {
		t.Fatalf("SHOW CREATE TABLE: %+v", r.Rows)
	}
	r = execSQL(t, ctx, root, `SHOW USERS`)
	if len(r.Rows) != 2 || r.Rows[0][0].S != "root" || !r.Rows[0][1].B || r.Rows[1][0].S != "bob" || r.Rows[1][1].B {
		t.Fatalf("SHOW USERS: %+v", r.Rows)
	}
	r = execSQL(t, ctx, root, `SHOW GRANTS ON t FOR bob`)
	if len(r.Rows) != 2 || r.Rows[0][2].S != "bob" || r.Rows[0][3].S != "SELECT" || r.Rows[1][3].S != "INSERT" {
		t.Fatalf("SHOW GRANTS: %+v", r.Rows)
	}
	if r := execSQL(t, ctx, root, `SHOW GRANTS`); len(r.Rows) != 2 {
		t.Fatalf("SHOW GRANTS (all): %+v", r.Rows)
	}
	for q, want := range map[string]string{`SHOW TIME ZONE`: "UTC", `SHOW transaction isolation level`: "serializable", `SHOW server_version`: "14.0 datax", `SHOW search_path`: "public", `SHOW database`: "datax"} {
		if r := execSQL(t, ctx, root, q); len(r.Rows) != 1 || r.Rows[0][0].S != want {
			t.Fatalf("%s: %+v", q, r.Rows)
		}
	}
	if r := execSQL(t, ctx, root, `SHOW ALL`); len(r.Rows) < 8 || r.Columns[0].Name != "name" {
		t.Fatalf("SHOW ALL: %+v", r.Rows)
	}
	if _, serr := trySQL(ctx, root, `SHOW no_such_setting`); serr == nil || serr.Code != sql.CodeUndefinedObject {
		t.Fatalf("SHOW unknown: %v", serr)
	}
}
