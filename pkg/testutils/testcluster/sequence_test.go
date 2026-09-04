package testcluster

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/sthorne/datax/pkg/server"
	"github.com/sthorne/datax/pkg/sql"
	"github.com/sthorne/datax/pkg/sql/catalog"
	"github.com/sthorne/datax/pkg/version"
)

// TestSequencesAndDefaults: expression defaults, the DEFAULT keyword and
// DEFAULT VALUES, sequences and their functions, SERIAL, identity
// columns, ownership, the catalogs, and the error codes.
func TestSequencesAndDefaults(t *testing.T) {
	tc := Start(t, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	s := sql.NewSession(tc.Nodes[0].DB(), catalog.NewAccessor())
	waitForDatabases(t, ctx, s)
	texts := func(r *sql.Result) []string {
		var out []string
		for _, row := range r.Rows {
			var cells []string
			for _, d := range row {
				if d.Null {
					cells = append(cells, "NULL")
				} else {
					cells = append(cells, d.Text())
				}
			}
			out = append(out, strings.Join(cells, "|"))
		}
		return out
	}
	expect := func(what string, r *sql.Result, rows ...string) {
		t.Helper()
		if strings.Join(texts(r), ";") != strings.Join(rows, ";") {
			t.Fatalf("%s: got %v, want %v", what, texts(r), rows)
		}
	}
	code := func(what, q, want string) {
		t.Helper()
		if _, serr := trySQL(ctx, s, q); serr == nil || serr.Code != want {
			t.Fatalf("%s: %v, want %s", what, serr, want)
		}
	}

	// Expression defaults, per row.
	execSQL(t, ctx, s, `CREATE TABLE t (id SERIAL PRIMARY KEY, u UUID DEFAULT gen_random_uuid(), at TIMESTAMPTZ DEFAULT now(), rid INT8 DEFAULT unique_rowid(), n INT8 DEFAULT 1 + 2, name TEXT DEFAULT 'none')`)
	expect("serial returning", execSQL(t, ctx, s, `INSERT INTO t (name) VALUES ('a'), ('b') RETURNING id, n, name`), "1|3|a", "2|3|b")
	expect("default values", execSQL(t, ctx, s, `INSERT INTO t DEFAULT VALUES RETURNING id, name`), "3|none")
	expect("DEFAULT keyword", execSQL(t, ctx, s, `INSERT INTO t (id, name, n) VALUES (DEFAULT, 'c', DEFAULT), (100, 'd', 7) RETURNING id, n`), "4|3", "100|7")
	r := execSQL(t, ctx, s, `SELECT count(*), count(u), count(at), count(rid) FROM t`)
	expect("defaults filled", r, "5|5|5|5")
	r = execSQL(t, ctx, s, `SELECT count(*) FROM (SELECT u FROM t) AS d GROUP BY u`)
	if len(r.Rows) != 5 {
		t.Fatalf("uuids not distinct: %v", texts(r))
	}
	r = execSQL(t, ctx, s, `SELECT rid FROM t ORDER BY id`)
	for i := 1; i < len(r.Rows); i++ {
		if r.Rows[i][0].I <= r.Rows[i-1][0].I {
			t.Fatalf("unique_rowid not ascending: %v", texts(r))
		}
	}
	expect("SET DEFAULT", execSQL(t, ctx, s, `UPDATE t SET n = DEFAULT, name = DEFAULT WHERE id = 100 RETURNING n, name`), "3|none")
	code("default referencing a column", `CREATE TABLE bad (a INT8 PRIMARY KEY, b INT8 DEFAULT a + 1)`, sql.CodeSyntaxError)
	code("unknown default function", `CREATE TABLE bad (a INT8 PRIMARY KEY, b INT8 DEFAULT nosuch())`, sql.CodeSyntaxError)
	code("ADD COLUMN expression default", `ALTER TABLE t ADD COLUMN c INT8 DEFAULT unique_rowid()`, sql.CodeFeatureNotSupported)

	// The owned sequence: nextval/currval/lastval/setval, the relation
	// view, SHOW SEQUENCES, and that it cannot be dropped out from under
	// its column.
	expect("show sequences", execSQL(t, ctx, s, `SHOW SEQUENCES`), "t_id_seq|1|1|1|9223372036854775807|f|32|32|t.id")
	// currval/lastval are per session: this one advanced the sequence
	// through the SERIAL inserts, a fresh one has not.
	fresh := sql.NewSession(tc.Nodes[0].DB(), catalog.NewAccessor())
	if _, serr := trySQL(ctx, fresh, `SELECT currval('t_id_seq')`); serr == nil || serr.Code != sql.CodeObjectNotInState {
		t.Fatalf("currval before nextval: %v, want %s", serr, sql.CodeObjectNotInState)
	}
	if _, serr := trySQL(ctx, fresh, `SELECT lastval()`); serr == nil || serr.Code != sql.CodeObjectNotInState {
		t.Fatalf("lastval before nextval: %v, want %s", serr, sql.CodeObjectNotInState)
	}
	expect("currval after inserts", execSQL(t, ctx, s, `SELECT currval('t_id_seq'), lastval()`), "4|4")
	expect("nextval currval lastval", execSQL(t, ctx, s, `SELECT nextval('t_id_seq'), currval('t_id_seq'), lastval()`), "5|5|5")
	expect("as a relation", execSQL(t, ctx, s, `SELECT last_value, is_called FROM t_id_seq`), "32|t")
	expect("setval", execSQL(t, ctx, s, `SELECT setval('t_id_seq', 1000)`), "1000")
	expect("after setval", execSQL(t, ctx, s, `INSERT INTO t (name) VALUES ('e') RETURNING id`), "1001")
	expect("setval not called", execSQL(t, ctx, s, `SELECT setval('t_id_seq', 2000, false)`), "2000")
	expect("after setval false", execSQL(t, ctx, s, `SELECT nextval('t_id_seq')`), "2000")
	code("drop owned sequence", `DROP SEQUENCE t_id_seq`, sql.CodeDependentObjectsExist)
	code("unknown sequence", `SELECT nextval('nope')`, sql.CodeUndefinedTable)

	// A standalone sequence with options: bounds, CYCLE, ALTER, DROP.
	execSQL(t, ctx, s, `CREATE SEQUENCE s START 5 INCREMENT 5 MAXVALUE 20 CACHE 1`)
	expect("stepping", execSQL(t, ctx, s, `SELECT nextval('s'), nextval('s'), nextval('s'), nextval('s')`), "5|10|15|20")
	code("maxvalue", `SELECT nextval('s')`, sql.CodeSequenceLimit)
	execSQL(t, ctx, s, `ALTER SEQUENCE s MAXVALUE 100 RESTART WITH 50`)
	expect("after alter", execSQL(t, ctx, s, `SELECT nextval('s'), nextval('s')`), "50|55")
	execSQL(t, ctx, s, `CREATE SEQUENCE cyc MAXVALUE 3 CYCLE CACHE 1`)
	expect("cycle", execSQL(t, ctx, s, `SELECT nextval('cyc'), nextval('cyc'), nextval('cyc'), nextval('cyc')`), "1|2|3|1")
	execSQL(t, ctx, s, `CREATE SEQUENCE down INCREMENT -1 MINVALUE -3 CACHE 1`)
	expect("descending", execSQL(t, ctx, s, `SELECT nextval('down'), nextval('down')`), "-1|-2")
	code("sequence named like a table", `CREATE SEQUENCE t`, sql.CodeDuplicateTable)
	execSQL(t, ctx, s, `DROP SEQUENCE cyc`)
	execSQL(t, ctx, s, `DROP SEQUENCE IF EXISTS cyc`)
	code("dropped", `SELECT nextval('cyc')`, sql.CodeUndefinedTable)
	if r := execSQL(t, ctx, s, `SELECT relname FROM pg_class WHERE relkind = 'S' ORDER BY relname`); strings.Join(texts(r), ",") != "down,s,t_id_seq" {
		t.Fatalf("pg_class sequences: %v", texts(r))
	}
	expect("pg_sequences", execSQL(t, ctx, s, `SELECT sequencename, start_value, increment_by, max_value, cache_size FROM pg_sequences WHERE sequencename = 's'`), "s|5|5|100|1")
	expect("column_default", execSQL(t, ctx, s, `SELECT column_default FROM information_schema.columns WHERE table_name = 't' AND column_name IN ('id', 'u') ORDER BY ordinal_position`), "nextval('t_id_seq')", "gen_random_uuid()")

	// nextval is never rolled back.
	execSQL(t, ctx, s, `BEGIN`)
	expect("in txn", execSQL(t, ctx, s, `SELECT nextval('s')`), "60")
	execSQL(t, ctx, s, `ROLLBACK`)
	expect("after rollback", execSQL(t, ctx, s, `SELECT nextval('s')`), "65")

	// Identity columns.
	execSQL(t, ctx, s, `CREATE TABLE ident (k INT8 GENERATED ALWAYS AS IDENTITY (START WITH 10) PRIMARY KEY, v TEXT, d INT8 GENERATED BY DEFAULT AS IDENTITY)`)
	expect("identity", execSQL(t, ctx, s, `INSERT INTO ident (v) VALUES ('x'), ('y') RETURNING k, d`), "10|1", "11|2")
	code("generated always", `INSERT INTO ident (k, v) VALUES (5, 'z')`, sql.CodeGeneratedAlways)
	expect("overriding", execSQL(t, ctx, s, `INSERT INTO ident (k, v, d) OVERRIDING SYSTEM VALUE VALUES (5, 'z', 99) RETURNING k, d`), "5|99")
	expect("by default takes a value", execSQL(t, ctx, s, `INSERT INTO ident (v, d) VALUES ('w', 7) RETURNING k, d`), "12|7")
	expect("identity in the catalog", execSQL(t, ctx, s, `SELECT attname, attidentity FROM pg_attribute WHERE attrelid = 'ident'::regclass AND attidentity <> '' ORDER BY attnum`), "k|a", "d|d")
	expect("owned sequences", execSQL(t, ctx, s, `SELECT sequencename FROM pg_sequences WHERE sequencename IN ('ident_k_seq', 'ident_d_seq') ORDER BY 1`), "ident_d_seq", "ident_k_seq")
	execSQL(t, ctx, s, `DROP TABLE ident`)
	code("owned sequences dropped with the table", `SELECT nextval('ident_k_seq')`, sql.CodeUndefinedTable)
	if r := execSQL(t, ctx, s, `SHOW SEQUENCES`); len(r.Rows) != 3 {
		t.Fatalf("sequences after drop: %v", texts(r))
	}
	execSQL(t, ctx, s, `ALTER TABLE t DROP COLUMN rid`)
	execSQL(t, ctx, s, `CREATE TABLE ser2 (a SERIAL PRIMARY KEY, b SERIAL)`)
	execSQL(t, ctx, s, `ALTER TABLE ser2 DROP COLUMN b`)
	code("sequence dropped with its column", `SELECT nextval('ser2_b_seq')`, sql.CodeUndefinedTable)
	expect("the other survives", execSQL(t, ctx, s, `SELECT nextval('ser2_a_seq')`), "1")

	// Over the wire: SERIAL with RETURNING through pgx, and psql's \\d of
	// a sequence when psql is installed.
	conn, err := pgx.Connect(ctx, "postgres://root@"+tc.Nodes[0].SQLAddr()+"/datax?sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(ctx)
	var id int64
	if err := conn.QueryRow(ctx, `INSERT INTO t (name) VALUES ($1) RETURNING id`, "wire").Scan(&id); err != nil || id != 2001 {
		t.Fatalf("pgx serial returning: %d %v", id, err)
	}
	if psql, err := lookPsql(); err == nil {
		url := "postgres://root@" + tc.Nodes[0].SQLAddr() + "/datax?sslmode=disable"
		for _, cmd := range []string{`\ds`, `\d s`, `\d t`} {
			out, err := runPsql(ctx, psql, url, cmd)
			if err != nil || strings.Contains(out, "ERROR") {
				t.Fatalf("psql %s: %v\n%s", cmd, err, out)
			}
			if cmd == `\d t` && !strings.Contains(out, "nextval('t_id_seq')") {
				t.Fatalf("psql \\d t lacks the default:\n%s", out)
			}
		}
	}
}

// TestSequenceBlocksAcrossGateways: three gateways drawing from one
// sequence under concurrent inserts hand out unique values (each node
// takes CACHE-sized blocks; no value repeats, none is lost to a
// collision), and the count adds up.
func TestSequenceBlocksAcrossGateways(t *testing.T) {
	tc := Start(t, 3)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	root := sql.NewSession(tc.Nodes[0].DB(), catalog.NewAccessor())
	waitForDatabases(t, ctx, root)
	execSQL(t, ctx, root, `CREATE TABLE q (id SERIAL PRIMARY KEY, node INT8)`)
	// Every node reads the catalog it may not have cached yet.
	for i := range tc.Nodes {
		s := sql.NewSession(tc.Nodes[i].DB(), catalog.NewAccessor())
		execSQL(t, ctx, s, `SELECT count(*) FROM q`)
	}
	var wg sync.WaitGroup
	for i := range tc.Nodes {
		for w := 0; w < 2; w++ {
			wg.Add(1)
			go func(node int) {
				defer wg.Done()
				s := sql.NewSession(tc.Nodes[node].DB(), catalog.NewAccessor())
				for k := 0; k < 25; k++ {
					for attempt := 0; ; attempt++ {
						_, serr := trySQL(ctx, s, `INSERT INTO q (node) VALUES (`+itoa64(int64(node))+`)`)
						if serr == nil {
							break
						}
						if serr.Code != sql.CodeSerializationFailure || attempt > 20 {
							t.Errorf("node %d: %v", node, serr)
							return
						}
					}
				}
			}(i)
		}
	}
	wg.Wait()
	// id is the primary key, so every insert that succeeded holds a
	// distinct value; the count says none was lost, and the maximum that
	// no more than the nodes' partial blocks went unused.
	r := execSQL(t, ctx, root, `SELECT count(*), min(id), max(id) FROM q`)
	if len(r.Rows) != 1 || r.Rows[0][0].I != 150 || r.Rows[0][1].I != 1 || r.Rows[0][2].I > 150+3*32 {
		t.Fatalf("ids: %+v", r.Rows)
	}
}

// TestSequencesNeedV7: a cluster still at v6 refuses the DDL a v6 node
// could not evaluate, while ordinary tables keep working.
func TestSequencesNeedV7(t *testing.T) {
	tc := StartWithOptions(t, 1, func(c *server.Config) { c.BinaryVersionOverride = version.V6 })
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	s := sql.NewSession(tc.Nodes[0].DB(), catalog.NewAccessor())
	waitForDatabases(t, ctx, s)
	execSQL(t, ctx, s, `CREATE TABLE plain (id INT8 PRIMARY KEY, n INT8 DEFAULT 3)`)
	for _, q := range []string{
		`CREATE SEQUENCE s`,
		`CREATE TABLE ser (id SERIAL PRIMARY KEY)`,
		`CREATE TABLE ident (id INT8 GENERATED ALWAYS AS IDENTITY PRIMARY KEY)`,
		`CREATE TABLE ex (id INT8 PRIMARY KEY, u UUID DEFAULT gen_random_uuid())`,
	} {
		if _, serr := trySQL(ctx, s, q); serr == nil || serr.Code != sql.CodeFeatureNotSupported || !strings.Contains(serr.Msg, "v7") {
			t.Fatalf("%s: %v", q, serr)
		}
	}
	execSQL(t, ctx, s, `INSERT INTO plain (id) VALUES (1)`)
}

func itoa64(n int64) string {
	return strings.TrimSpace(strings.Repeat(" ", 0) + formatInt64(n))
}

func formatInt64(n int64) string {
	if n == 0 {
		return "0"
	}
	var b []byte
	for n > 0 {
		b = append([]byte{byte('0' + n%10)}, b...)
		n /= 10
	}
	return string(b)
}
