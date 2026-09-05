package testcluster

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/sthorne/datax/pkg/server"
	"github.com/sthorne/datax/pkg/sql"
	"github.com/sthorne/datax/pkg/sql/catalog"
	"github.com/sthorne/datax/pkg/sql/types"
	"github.com/sthorne/datax/pkg/version"
)

// TestCreateTableAsLikeComments covers the last DDL set of #95 minus
// the type rewrite: CREATE TABLE ... AS (the generated rowid key, an
// explicit key, column names, WITH NO DATA, a query over a view and a
// parameter, the transaction-block refusal, a failure dropping the
// table again), CREATE TABLE ... (LIKE ...) with its options, COMMENT
// ON for tables, views, columns and indexes with the catalogs and
// psql's renderings, and SELECT INTO's pointer.
func TestCreateTableAsLikeComments(t *testing.T) {
	tc := Start(t, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	s := sql.NewSession(tc.Nodes[0].DB(), catalog.NewAccessor())
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
	expectCode := func(q, code string) {
		t.Helper()
		if _, serr := trySQL(ctx, s, q); serr == nil || serr.Code != code {
			t.Fatalf("%s: want %s, got %v", q, code, serr)
		}
	}

	execSQL(t, ctx, s, `CREATE TABLE orders (id INT8 PRIMARY KEY, cust INT8 NOT NULL DEFAULT 0, qty INT8 CHECK (qty > 0), note TEXT DEFAULT 'n/a', at TIMESTAMPTZ DEFAULT now())`)
	execSQL(t, ctx, s, `CREATE INDEX orders_by_cust ON orders (cust)`)
	execSQL(t, ctx, s, `CREATE UNIQUE INDEX orders_by_note ON orders (note)`)
	var vals []string
	for i := 1; i <= 300; i++ {
		vals = append(vals, fmt.Sprintf("(%d, %d, %d, 'n%d')", i, i%7, i, i))
	}
	execSQL(t, ctx, s, `INSERT INTO orders (id, cust, qty, note) VALUES `+strings.Join(vals, ", "))

	// ---- CREATE TABLE AS ----
	if r := execSQL(t, ctx, s, `CREATE TABLE big AS SELECT id, cust, qty * 2 AS qty2 FROM orders WHERE qty > 100`); r.Tag != "CREATE TABLE AS 200" {
		t.Fatalf("tag: %q", r.Tag)
	}
	expect(`SELECT count(*), min(qty2), max(qty2) FROM big`, "200|202|600")
	expect(`SELECT column_name, data_type FROM information_schema.columns WHERE table_name = 'big' ORDER BY ordinal_position`, "id|bigint;cust|bigint;qty2|bigint")
	expect(`SELECT id FROM big WHERE cust = 3 ORDER BY id LIMIT 2`, "101;108") // no key on id: a scan, still correct
	execSQL(t, ctx, s, `INSERT INTO big VALUES (1, 1, 1), (1, 1, 1)`)          // the hidden rowid key allows duplicates
	expect(`SELECT count(*) FROM big WHERE id = 1`, "2")
	if r := execSQL(t, ctx, s, `SELECT * FROM big WHERE id = 1 LIMIT 1`); len(r.Columns) != 3 {
		t.Fatalf("SELECT * shows the rowid: %v", r.Columns)
	}
	expect(`SELECT column_name FROM information_schema.columns WHERE table_name = 'big' AND column_name = 'rowid'`, "")
	execSQL(t, ctx, s, `CREATE TABLE keyed (order_id, customer, PRIMARY KEY (order_id)) AS SELECT id, cust FROM orders WHERE qty <= 3`)
	expect(`SELECT order_id, customer FROM keyed ORDER BY order_id`, "1|1;2|2;3|3")
	if p := explainPlan(t, ctx, s, `SELECT customer FROM keyed WHERE order_id = 2`); p != "point lookup on primary key" {
		t.Fatalf("explicit key: %s", p)
	}
	expectCode(`INSERT INTO keyed VALUES (1, 9)`, sql.CodeUniqueViolation)
	execSQL(t, ctx, s, `CREATE TABLE empty_copy AS SELECT id, note FROM orders WITH NO DATA`)
	expect(`SELECT count(*) FROM empty_copy`, "0")
	expect(`SELECT column_name FROM information_schema.columns WHERE table_name = 'empty_copy' ORDER BY ordinal_position`, "id;note")
	execSQL(t, ctx, s, `CREATE VIEW small AS SELECT id, qty FROM orders WHERE qty < 5`)
	execSQL(t, ctx, s, `CREATE TABLE from_view AS SELECT * FROM small`)
	expect(`SELECT count(*) FROM from_view`, "4")
	execSQL(t, ctx, s, `CREATE TABLE IF NOT EXISTS from_view AS SELECT 1 AS x`) // exists: no-op
	expect(`SELECT count(*) FROM from_view`, "4")
	expectCode(`CREATE TABLE from_view AS SELECT 1 AS x`, sql.CodeDuplicateTable)
	expectCode(`CREATE TABLE bad AS SELECT id, id FROM orders`, sql.CodeDuplicateObject)
	expectCode(`CREATE TABLE bad (a) AS SELECT id, qty FROM orders`, sql.CodeSyntaxError)
	expectCode(`CREATE TABLE bad AS SELECT id FROM nope`, sql.CodeUndefinedTable)
	expectCode(`BEGIN; CREATE TABLE bad AS SELECT 1 AS x`, sql.CodeActiveTransaction)
	execSQL(t, ctx, s, `ROLLBACK`)
	expectCode(`SELECT id INTO t2 FROM orders`, sql.CodeSyntaxError)
	// A parameter in the query, through the session.
	if r, serr := trySQL(ctx, s, `CREATE TABLE by_param AS SELECT id FROM orders WHERE qty > $1`, types.NewInt(295)); serr != nil || r.Tag != "CREATE TABLE AS 5" {
		t.Fatalf("parameterized CTAS: %v %v", r, serr)
	}

	// ---- LIKE ----
	execSQL(t, ctx, s, `CREATE TABLE o2 (LIKE orders)`)
	expect(`SELECT column_name, is_nullable, column_default FROM information_schema.columns WHERE table_name = 'o2' ORDER BY ordinal_position`, "id|NO|NULL;cust|NO|NULL;qty|YES|NULL;note|YES|NULL;at|YES|NULL")
	if p := explainPlan(t, ctx, s, `SELECT qty FROM o2 WHERE id = 1`); p != "point lookup on primary key" {
		t.Fatalf("LIKE copies the primary key: %s", p)
	}
	expectCode(`INSERT INTO o2 (id, qty) VALUES (1, -5)`, sql.CodeNotNullViolation) // NOT NULL copied, no default
	execSQL(t, ctx, s, `INSERT INTO o2 (id, cust, qty) VALUES (1, 1, -5)`)          // no CHECK copied
	execSQL(t, ctx, s, `CREATE TABLE o3 (extra TEXT, LIKE orders INCLUDING ALL, more INT8)`)
	expect(`SELECT column_name FROM information_schema.columns WHERE table_name = 'o3' ORDER BY ordinal_position`, "extra;id;cust;qty;note;at;more")
	expect(`SELECT column_default FROM information_schema.columns WHERE table_name = 'o3' AND column_name IN ('cust', 'note') ORDER BY column_name`, "0;'n/a'")
	expectCode(`INSERT INTO o3 (id, qty) VALUES (1, -5)`, sql.CodeCheckViolation)
	execSQL(t, ctx, s, `INSERT INTO o3 (id, qty, note) VALUES (1, 5, 'x'), (2, 6, 'y')`)
	expectCode(`INSERT INTO o3 (id, qty, note) VALUES (3, 5, 'x')`, sql.CodeUniqueViolation) // the unique index came along
	if p := explainPlan(t, ctx, s, `SELECT id FROM o3 WHERE cust = 0`); !strings.Contains(p, `index "orders_by_cust"`) {
		t.Fatalf("LIKE INCLUDING INDEXES: %s", p)
	}
	execSQL(t, ctx, s, `CREATE TABLE o4 (LIKE orders INCLUDING DEFAULTS EXCLUDING INDEXES, PRIMARY KEY (id, cust))`)
	expect(`SELECT count(*) FROM pg_indexes WHERE tablename = 'o4'`, "1") // the primary key only
	expectCode(`CREATE TABLE o5 (LIKE nope)`, sql.CodeUndefinedTable)
	expectCode(`CREATE TABLE o5 (LIKE small)`, sql.CodeWrongObjectType)

	// ---- COMMENT ON ----
	execSQL(t, ctx, s, `COMMENT ON TABLE orders IS 'customer orders'`)
	execSQL(t, ctx, s, `COMMENT ON COLUMN orders.qty IS 'units ordered'`)
	execSQL(t, ctx, s, `COMMENT ON INDEX orders_by_cust IS 'per customer'`)
	execSQL(t, ctx, s, `COMMENT ON VIEW small IS 'the small ones'`)
	expect(`SELECT obj_description('orders'::regclass, 'pg_class')`, "customer orders")
	expect(`SELECT obj_description('small'::regclass, 'pg_class')`, "the small ones")
	expect(`SELECT col_description('orders'::regclass, 3)`, "units ordered")
	expect(`SELECT col_description('orders'::regclass, 1)`, "NULL")
	// Two indexes carry the name (o3's LIKE copy has no comment).
	expect(`SELECT obj_description(c.oid, 'pg_class') FROM pg_class c WHERE c.relname = 'orders_by_cust' ORDER BY c.oid`, "per customer;NULL")
	expect(`SELECT a.attname, col_description(a.attrelid, a.attnum) FROM pg_attribute a WHERE a.attrelid = 'orders'::regclass AND a.attnum = 3`, "qty|units ordered")
	expect(`SELECT objsubid, description FROM pg_description WHERE objoid = 'orders'::regclass ORDER BY objsubid`, "0|customer orders;3|units ordered")
	execSQL(t, ctx, s, `COMMENT ON COLUMN orders.qty IS NULL`)
	expect(`SELECT count(*) FROM pg_description WHERE objoid = 'orders'::regclass`, "1")
	execSQL(t, ctx, s, `CREATE TABLE o6 (LIKE orders INCLUDING COMMENTS)`) // accepted; comments are per relation
	expectCode(`COMMENT ON COLUMN orders.nope IS 'x'`, sql.CodeUndefinedColumn)
	expectCode(`COMMENT ON INDEX nope IS 'x'`, sql.CodeUndefinedObject)
	expectCode(`COMMENT ON TABLE pg_class IS 'x'`, sql.CodeInsufficientPriv)
	// The comment survives the column's other DDL.
	execSQL(t, ctx, s, `COMMENT ON COLUMN orders.note IS 'free text'`)
	execSQL(t, ctx, s, `ALTER TABLE orders ALTER COLUMN note SET DEFAULT 'none'`)
	expect(`SELECT col_description('orders'::regclass, 4)`, "free text")
	if r := execSQL(t, ctx, s, `SHOW CREATE TABLE orders`); !strings.Contains(r.Rows[0][1].S, "CREATE TABLE orders") {
		t.Fatalf("SHOW CREATE TABLE: %s", r.Rows[0][1].S)
	}
}

// TestAlterColumnType: the online rewrite — widening and text
// conversions over a table with concurrent writes, the shadow column
// invisible meanwhile, NOT NULL / defaults / comments carried over, a
// value that cannot convert reverting the change, the refusals, and the
// v9 gate.
func TestAlterColumnType(t *testing.T) {
	tc := Start(t, 3)
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()
	const ttl = 2 * time.Second
	s := leasedSession(t, tc, 0, ttl)
	sB := leasedSession(t, tc, 1, ttl)
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
	expect := func(sess *sql.Session, q, want string) {
		t.Helper()
		if got := rowsText(execSQL(t, ctx, sess, q)); got != want {
			t.Fatalf("%s: got %q, want %q", q, got, want)
		}
	}
	expectCode := func(q, code string) {
		t.Helper()
		if _, serr := trySQL(ctx, s, q); serr == nil || serr.Code != code {
			t.Fatalf("%s: want %s, got %v", q, code, serr)
		}
	}

	execSQL(t, ctx, s, `CREATE TABLE m (id INT8 PRIMARY KEY, n INT8 NOT NULL DEFAULT 7, tag TEXT, d TEXT, note TEXT)`)
	var vals []string
	for i := 1; i <= 500; i++ {
		vals = append(vals, fmt.Sprintf("(%d, %d, '%d', '%d.5', 'x')", i, i*10, i, i))
	}
	execSQL(t, ctx, s, `INSERT INTO m VALUES `+strings.Join(vals, ", "))
	execSQL(t, ctx, s, `COMMENT ON COLUMN m.n IS 'a count'`)
	execSQL(t, ctx, s, `CREATE INDEX m_by_tag ON m (tag)`)

	// INT8 → DECIMAL(12,2), with the other gateway writing throughout.
	stop := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		defer close(done)
		for i := 1000; ; i++ {
			select {
			case <-stop:
				return
			default:
			}
			if _, serr := trySQL(ctx, sB, fmt.Sprintf(`INSERT INTO m (id, n, d) VALUES (%d, %d, '%d.25')`, i, i, i)); serr != nil && serr.Code != sql.CodeSerializationFailure {
				done <- fmt.Errorf("writer: %v", serr)
				return
			}
			if _, serr := trySQL(ctx, sB, fmt.Sprintf(`UPDATE m SET n = n + 1 WHERE id = %d`, i%500+1)); serr != nil && serr.Code != sql.CodeSerializationFailure {
				done <- fmt.Errorf("updater: %v", serr)
				return
			}
		}
	}()
	execSQL(t, ctx, s, `ALTER TABLE m ALTER COLUMN n TYPE DECIMAL(12, 2)`)
	close(stop)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	expect(s, `SELECT data_type, is_nullable, column_default, numeric_precision, numeric_scale FROM information_schema.columns WHERE table_name = 'm' AND column_name = 'n'`, "numeric(12,2)|NO|'7.00'|12|2")
	expect(s, `SELECT count(*) FROM information_schema.columns WHERE table_name = 'm'`, "5")
	expect(s, `SELECT n >= 20 AND n < 100 FROM m WHERE id = 2`, "t") // the updater bumped it a few times
	expect(sB, `SELECT n FROM m WHERE id = 1000`, "1000.00")
	expect(s, `SELECT col_description('m'::regclass, 2)`, "a count")
	execSQL(t, ctx, s, `INSERT INTO m (id) VALUES (5000)`)
	expect(s, `SELECT n FROM m WHERE id = 5000`, "7.00")
	expect(sB, `SELECT count(*) FROM m WHERE n < 0`, "0")
	expectCode(`INSERT INTO m (id, n) VALUES (5001, 'x')`, sql.CodeInvalidTextRepresentation)
	// Every row of the table carries the converted value (no NULLs where
	// the source had a value).
	expect(s, `SELECT count(*) FROM m WHERE n IS NULL`, "0")

	// TEXT → DECIMAL, TEXT → INT8 (validated), INT8 → TEXT.
	execSQL(t, ctx, s, `ALTER TABLE m ALTER COLUMN d TYPE DECIMAL`)
	expect(s, `SELECT d FROM m WHERE id = 3`, "3.5")
	expect(s, `SELECT sum(d) FROM m WHERE id <= 2`, "4")
	execSQL(t, ctx, s, `UPDATE m SET note = id::text`)
	execSQL(t, ctx, s, `ALTER TABLE m ALTER COLUMN note TYPE INT8`)
	expect(s, `SELECT note + 1 FROM m WHERE id = 3`, "4")
	execSQL(t, ctx, s, `ALTER TABLE m ALTER COLUMN note TYPE TEXT`)
	expect(s, `SELECT note || '!' FROM m WHERE id = 3`, "3!")
	execSQL(t, ctx, s, `ALTER TABLE m ALTER COLUMN note TYPE TEXT`) // no change: a no-op

	// A value that cannot convert fails the statement and leaves the
	// column as it was, with nothing hidden left behind.
	execSQL(t, ctx, s, `UPDATE m SET note = 'not a number' WHERE id = 5`)
	expectCode(`ALTER TABLE m ALTER COLUMN note TYPE INT8`, sql.CodeInvalidTextRepresentation)
	expect(s, `SELECT data_type FROM information_schema.columns WHERE table_name = 'm' AND column_name = 'note'`, "text")
	if d := lookupDescriptor(t, ctx, tc.Nodes[0].DB(), "m"); len(d.Columns) != 5 {
		t.Fatalf("shadow column left behind: %+v", d.Columns)
	}
	expect(sB, `SELECT note FROM m WHERE id = 5`, "not a number")

	// Refusals.
	expectCode(`ALTER TABLE m ALTER COLUMN n TYPE INT8`, sql.CodeFeatureNotSupported)   // narrowing
	expectCode(`ALTER TABLE m ALTER COLUMN id TYPE TEXT`, sql.CodeFeatureNotSupported)  // primary key
	expectCode(`ALTER TABLE m ALTER COLUMN tag TYPE INT8`, sql.CodeFeatureNotSupported) // indexed
	expectCode(`ALTER TABLE m ALTER COLUMN nope TYPE TEXT`, sql.CodeUndefinedColumn)
	expectCode(`BEGIN; ALTER TABLE m ALTER COLUMN d TYPE TEXT`, sql.CodeActiveTransaction)
	execSQL(t, ctx, s, `ROLLBACK`)
	execSQL(t, ctx, s, `CREATE VIEW mv AS SELECT d FROM m`)
	expectCode(`ALTER TABLE m ALTER COLUMN d TYPE TEXT`, sql.CodeDependentObjectsExist)
	execSQL(t, ctx, s, `ALTER TABLE IF EXISTS nope ALTER COLUMN d TYPE TEXT`)
}

// TestRetypeNeedsV9: a cluster still at v8 refuses ALTER COLUMN TYPE.
func TestRetypeNeedsV9(t *testing.T) {
	tc := StartWithOptions(t, 1, func(c *server.Config) { c.BinaryVersionOverride = version.V8 })
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	s := sql.NewSession(tc.Nodes[0].DB(), catalog.NewAccessor())
	waitForDatabases(t, ctx, s)
	execSQL(t, ctx, s, `CREATE TABLE plain (id INT8 PRIMARY KEY, n INT8)`)
	if _, serr := trySQL(ctx, s, `ALTER TABLE plain ALTER COLUMN n TYPE TEXT`); serr == nil || serr.Code != sql.CodeFeatureNotSupported {
		t.Fatalf("ALTER COLUMN TYPE at v8: %v, want 0A000", serr)
	}
}
