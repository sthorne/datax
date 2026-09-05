package testcluster

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/sthorne/datax/pkg/server"
	"github.com/sthorne/datax/pkg/sql"
	"github.com/sthorne/datax/pkg/sql/catalog"
	"github.com/sthorne/datax/pkg/sql/parser"
	"github.com/sthorne/datax/pkg/sql/types"
	"github.com/sthorne/datax/pkg/version"
)

// TestViews covers #95's views: CREATE [OR REPLACE] VIEW with and
// without a column list, reading a view everywhere a table reads (base,
// join, subquery, set operation, INSERT source, WITH, qualified,
// aliased), views over views, dependency tracking against DROP TABLE /
// DROP VIEW / RENAME / DROP COLUMN with and without CASCADE, the
// read-only refusals, the catalogs and SHOW, Describe and pgx, the
// privilege rule, a view inside a transaction, and the error codes.
func TestViews(t *testing.T) {
	tc := Start(t, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	// Leased accessors on both sessions: a GRANT drains the other's
	// lease, so the privilege checks below see it at once.
	s := leasedSession(t, tc, 0, 2*time.Second)
	waitForDatabases(t, ctx, s)
	rowsText := func(r *sql.Result) string {
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
		return strings.Join(out, ";")
	}
	expect := func(q, want string) {
		t.Helper()
		if got := rowsText(execSQL(t, ctx, s, q)); got != want {
			t.Fatalf("%s: got %q, want %q", q, got, want)
		}
	}
	expectCode := func(q, code string) *sql.Error {
		t.Helper()
		_, serr := trySQL(ctx, s, q)
		if serr == nil || serr.Code != code {
			t.Fatalf("%s: want %s, got %v", q, code, serr)
		}
		return serr
	}

	execSQL(t, ctx, s, `CREATE TABLE customers (id INT8 PRIMARY KEY, name TEXT)`)
	execSQL(t, ctx, s, `CREATE TABLE orders (id INT8 PRIMARY KEY, cust INT8 REFERENCES customers, qty INT8)`)
	execSQL(t, ctx, s, `INSERT INTO customers VALUES (1, 'ann'), (2, 'bob'), (3, 'cy')`)
	execSQL(t, ctx, s, `INSERT INTO orders VALUES (10, 1, 5), (11, 1, 50), (12, 2, 500), (13, 2, 15), (14, 3, 1)`)

	// ---- create and read ----
	execSQL(t, ctx, s, `CREATE VIEW big AS SELECT id, cust, qty FROM orders WHERE qty > 10`)
	expect(`SELECT id, qty FROM big ORDER BY id`, "11|50;12|500;13|15")
	expect(`SELECT * FROM big WHERE qty < 100 ORDER BY qty DESC`, "11|1|50;13|2|15")
	expect(`SELECT count(*), sum(qty) FROM big`, "3|565")
	expect(`SELECT b.id, c.name FROM big b JOIN customers c ON c.id = b.cust ORDER BY b.id`, "11|ann;12|bob;13|bob")
	expect(`SELECT c.name FROM customers c WHERE c.id IN (SELECT cust FROM big) ORDER BY c.name`, "ann;bob")
	expect(`SELECT name FROM customers c WHERE EXISTS (SELECT 1 FROM big WHERE big.cust = c.id AND big.qty > 100)`, "bob")
	expect(`SELECT id FROM big UNION SELECT id FROM orders WHERE qty = 1 ORDER BY id`, "11;12;13;14")
	expect(`WITH q AS (SELECT cust, max(qty) AS m FROM big GROUP BY cust) SELECT cust, m FROM q ORDER BY cust`, "1|50;2|500")
	expect(`SELECT id FROM datax.big ORDER BY id`, "11;12;13")
	expect(`SELECT (SELECT count(*) FROM big) + (SELECT count(*) FROM big WHERE qty > 400)`, "4")
	execSQL(t, ctx, s, `CREATE TABLE archive (id INT8 PRIMARY KEY, qty INT8)`)
	execSQL(t, ctx, s, `INSERT INTO archive SELECT id, qty FROM big`)
	expect(`SELECT count(*) FROM archive`, "3")
	if p := execSQL(t, ctx, s, `EXPLAIN SELECT id FROM big WHERE qty > 20`); len(p.Rows) != 1 {
		t.Fatalf("explain over a view: %+v", p.Rows)
	}

	// ---- column lists, OR REPLACE, views over views ----
	execSQL(t, ctx, s, `CREATE OR REPLACE VIEW big (order_id, customer, amount) AS SELECT id, cust, qty FROM orders WHERE qty > 4`)
	expect(`SELECT order_id, customer FROM big WHERE amount = 5`, "10|1")
	expect(`SELECT column_name FROM information_schema.columns WHERE table_name = 'big' ORDER BY ordinal_position`, "order_id;customer;amount")
	execSQL(t, ctx, s, `CREATE VIEW per_customer AS SELECT c.name, count(*) AS n, sum(b.amount) AS total FROM big b JOIN customers c ON c.id = b.customer GROUP BY c.name`)
	execSQL(t, ctx, s, `CREATE VIEW top AS SELECT name FROM per_customer WHERE total > 100`)
	expect(`SELECT name, n, total FROM per_customer ORDER BY name`, "ann|2|55;bob|2|515")
	expect(`SELECT * FROM top`, "bob")
	expect(`SELECT t.name, p.n FROM top t JOIN per_customer p ON p.name = t.name`, "bob|2")
	execSQL(t, ctx, s, `INSERT INTO orders VALUES (15, 3, 900)`)
	expect(`SELECT name FROM top ORDER BY name`, "bob;cy") // views are live
	expect(`SELECT * FROM (SELECT name FROM top) AS d ORDER BY name`, "bob;cy")

	// ---- the read-only and definition refusals ----
	expectCode(`INSERT INTO big VALUES (1, 1, 1)`, sql.CodeWrongObjectType)
	expectCode(`UPDATE big SET amount = 1`, sql.CodeWrongObjectType)
	expectCode(`DELETE FROM big`, sql.CodeWrongObjectType)
	expectCode(`DROP TABLE big`, sql.CodeWrongObjectType)
	expectCode(`DROP VIEW orders`, sql.CodeWrongObjectType)
	expectCode(`CREATE INDEX i ON big (amount)`, sql.CodeWrongObjectType)
	expectCode(`TRUNCATE big`, sql.CodeWrongObjectType)
	expectCode(`ALTER TABLE big ADD COLUMN x INT8`, sql.CodeWrongObjectType)
	expectCode(`ANALYZE big`, sql.CodeWrongObjectType)
	expectCode(`CREATE VIEW v2 AS SELECT id FROM orders WHERE qty > $1`, sql.CodeSyntaxError)
	expectCode(`CREATE VIEW v2 AS SELECT id FROM nope`, sql.CodeUndefinedTable)
	expectCode(`CREATE VIEW orders AS SELECT 1`, sql.CodeDuplicateTable)
	expectCode(`CREATE VIEW v2 (a) AS SELECT id, qty FROM orders`, sql.CodeSyntaxError)
	expectCode(`CREATE VIEW v2 (a, a) AS SELECT id, qty FROM orders`, sql.CodeDuplicateObject)
	expectCode(`CREATE VIEW v2 AS SELECT id FROM orders AS OF SYSTEM TIME '-1s'`, sql.CodeFeatureNotSupported)
	expectCode(`CREATE OR REPLACE VIEW big AS SELECT name FROM top`, sql.CodeInvalidObjectDefinition)
	expectCode(`CREATE OR REPLACE VIEW orders AS SELECT 1`, sql.CodeWrongObjectType)
	expectCode(`DROP VIEW nope`, sql.CodeUndefinedTable)
	execSQL(t, ctx, s, `DROP VIEW IF EXISTS nope`)

	// ---- dependencies ----
	expectCode(`DROP TABLE orders`, sql.CodeDependentObjectsExist)
	expectCode(`DROP VIEW big`, sql.CodeDependentObjectsExist)
	expectCode(`ALTER TABLE orders RENAME TO o2`, sql.CodeDependentObjectsExist)
	expectCode(`ALTER TABLE orders RENAME COLUMN qty TO amount`, sql.CodeDependentObjectsExist)
	expectCode(`ALTER TABLE orders DROP COLUMN qty`, sql.CodeDependentObjectsExist)
	execSQL(t, ctx, s, `ALTER TABLE orders ADD COLUMN note TEXT`) // additive changes are fine
	execSQL(t, ctx, s, `DROP VIEW per_customer CASCADE`)          // takes top with it
	expect(`SELECT viewname FROM pg_views ORDER BY viewname`, "big")
	execSQL(t, ctx, s, `DROP VIEW big, big`)
	expect(`SELECT count(*) FROM pg_views`, "0")
	execSQL(t, ctx, s, `CREATE VIEW big AS SELECT id FROM orders`)
	execSQL(t, ctx, s, `CREATE VIEW bigger AS SELECT id FROM big`)
	execSQL(t, ctx, s, `DROP TABLE orders CASCADE`) // both views go
	expect(`SELECT count(*) FROM pg_views`, "0")
	expectCode(`SELECT id FROM big`, sql.CodeUndefinedTable)

	// ---- catalogs and SHOW ----
	execSQL(t, ctx, s, `CREATE VIEW names (who) AS SELECT name FROM customers WHERE id > 1`)
	expect(`SELECT relname, relkind FROM pg_class WHERE relname = 'names'`, "names|v")
	expect(`SELECT pg_get_viewdef(c.oid, true) FROM pg_class c WHERE c.relname = 'names'`, "SELECT name FROM customers WHERE id > 1")
	expect(`SELECT attname FROM pg_attribute WHERE attrelid = (SELECT oid FROM pg_class WHERE relname = 'names') ORDER BY attnum`, "who")
	expect(`SELECT schemaname, viewname, definition FROM pg_views`, "public|names|SELECT name FROM customers WHERE id > 1")
	expect(`SELECT table_name, table_type, is_insertable_into FROM information_schema.tables WHERE table_name IN ('names', 'customers') ORDER BY table_name`, "customers|BASE TABLE|YES;names|VIEW|NO")
	expect(`SELECT table_name, view_definition FROM information_schema.views`, "names|SELECT name FROM customers WHERE id > 1")
	expect(`SHOW VIEWS`, "names|SELECT name FROM customers WHERE id > 1")
	expect(`SHOW CREATE VIEW names`, "names|CREATE VIEW names (who) AS SELECT name FROM customers WHERE id > 1")
	if r := execSQL(t, ctx, s, `SHOW TABLES`); strings.Contains(rowsText(r), "names") {
		t.Fatalf("SHOW TABLES lists a view: %s", rowsText(r))
	}
	expect(`SELECT 'names'::regclass = (SELECT oid FROM pg_class WHERE relname = 'names')`, "t")

	// ---- Describe and pgx ----
	stmts, perr := parser.Parse(`SELECT who, length(who) FROM names WHERE who > $1`)
	if perr != nil {
		t.Fatal(perr)
	}
	cols, serr := s.PlanColumns(ctx, stmts[0])
	if serr != nil || len(cols) != 2 || cols[0].Name != "who" || cols[0].Type != types.String || cols[1].Type != types.Int {
		t.Fatalf("describe a view: %v %v", cols, serr)
	}
	if fams, serr := s.PlanParams(ctx, stmts[0]); serr != nil || len(fams) != 1 || fams[0] != types.String {
		t.Fatalf("params through a view: %v %v", fams, serr)
	}
	conn, err := pgx.Connect(ctx, pgURL(tc, 0))
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer func() { _ = conn.Close(ctx) }()
	var who string
	if err := conn.QueryRow(ctx, `SELECT who FROM names WHERE who > $1 ORDER BY who LIMIT 1`, "b").Scan(&who); err != nil || who != "bob" {
		t.Fatalf("pgx through a view: %q %v", who, err)
	}

	// ---- inside a transaction ----
	execSQL(t, ctx, s, `BEGIN; CREATE VIEW tmp AS SELECT id FROM customers WHERE id = 1`)
	expect(`SELECT * FROM tmp`, "1")
	execSQL(t, ctx, s, `ROLLBACK`)
	expectCode(`SELECT * FROM tmp`, sql.CodeUndefinedTable)

	// ---- privileges: SELECT on the view and on what it reads ----
	execSQL(t, ctx, s, `CREATE USER alice PASSWORD 'pw12345'`)
	aliceCat := catalog.NewAccessor()
	if err := aliceCat.StartLeasing(tc.Nodes[0].DB(), tc.Nodes[0].Clock(), tc.Nodes[0].Stopper(), 2*time.Second); err != nil {
		t.Fatal(err)
	}
	alice := sql.NewSessionForUser(tc.Nodes[0].DB(), aliceCat, "alice")
	denied(t, ctx, alice, `SELECT who FROM names`)
	execSQL(t, ctx, s, `GRANT SELECT ON names TO alice`)
	denied(t, ctx, alice, `SELECT who FROM names`) // the query reads customers
	execSQL(t, ctx, s, `GRANT SELECT ON customers TO alice`)
	if r := execSQL(t, ctx, alice, `SELECT who FROM names ORDER BY who`); rowsText(r) != "bob;cy" {
		t.Fatalf("alice reads the view: %s", rowsText(r))
	}
	denied(t, ctx, alice, `DROP VIEW names`)
	denied(t, ctx, alice, `CREATE VIEW mine AS SELECT who FROM names`)
}

// TestViewsNeedV9: a cluster still at v8 refuses CREATE VIEW while
// everything else keeps working.
func TestViewsNeedV9(t *testing.T) {
	tc := StartWithOptions(t, 1, func(c *server.Config) { c.BinaryVersionOverride = version.V8 })
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	s := sql.NewSession(tc.Nodes[0].DB(), catalog.NewAccessor())
	waitForDatabases(t, ctx, s)
	execSQL(t, ctx, s, `CREATE TABLE plain (id INT8 PRIMARY KEY, n INT8)`)
	if _, serr := trySQL(ctx, s, `CREATE VIEW v AS SELECT id FROM plain`); serr == nil || serr.Code != sql.CodeFeatureNotSupported {
		t.Fatalf("CREATE VIEW at v8: %v, want 0A000", serr)
	}
}
