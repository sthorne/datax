package testcluster

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/sthorne/datax/pkg/sql"
	"github.com/sthorne/datax/pkg/sql/catalog"
	"github.com/sthorne/datax/pkg/sql/types"
)

// nwaySchema loads the 3-level regions→customers→orders(→items) schema
// used by the N-way join tests.
func nwaySchema(t *testing.T, ctx context.Context, s *sql.Session) {
	t.Helper()
	execSQL(t, ctx, s, `CREATE TABLE regions (id INT8 PRIMARY KEY, name TEXT)`)
	execSQL(t, ctx, s, `CREATE TABLE customers (id INT8 PRIMARY KEY, region_id INT8, name TEXT)`)
	execSQL(t, ctx, s, `CREATE TABLE orders (id INT8 PRIMARY KEY, customer_id INT8, total INT8)`)
	execSQL(t, ctx, s, `CREATE TABLE items (id INT8 PRIMARY KEY, order_id INT8, qty INT8)`)
	execSQL(t, ctx, s, `CREATE INDEX by_customer ON orders (customer_id)`)
	execSQL(t, ctx, s, `INSERT INTO regions VALUES (1, 'west'), (2, 'east'), (3, 'north')`)
	execSQL(t, ctx, s, `INSERT INTO customers VALUES (10, 1, 'ann'), (11, 1, 'bob'), (12, 2, 'cat'), (13, NULL, 'dan')`)
	execSQL(t, ctx, s, `INSERT INTO orders VALUES (100, 10, 50), (101, 10, 70), (102, 11, 30), (103, 99, 10)`)
	execSQL(t, ctx, s, `INSERT INTO items VALUES (1000, 100, 2), (1001, 100, 3), (1002, 102, 5)`)
}

// TestNWayJoins: left-deep chains of three and four tables, LEFT joins in
// middle positions (NULL propagation), skip-level ON conditions, WHERE
// semantics on LEFT sides, and the structural errors.
func TestNWayJoins(t *testing.T) {
	tc := Start(t, 3)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	s := sql.NewSession(tc.Nodes[0].DB(), catalog.NewAccessor())
	nwaySchema(t, ctx, s)

	// 3-way inner: only west's customers have orders.
	res := execSQL(t, ctx, s, `SELECT r.name, c.name, o.total FROM regions r
		JOIN customers c ON c.region_id = r.id
		JOIN orders o ON o.customer_id = c.id
		ORDER BY o.total`)
	if len(res.Rows) != 3 || res.Rows[0][2].I != 30 || res.Rows[1][2].I != 50 || res.Rows[2][2].I != 70 {
		t.Fatalf("3-way inner: %+v", res.Rows)
	}
	if res.Rows[0][1].S != "bob" || res.Rows[1][1].S != "ann" {
		t.Fatalf("3-way inner names: %+v", res.Rows)
	}

	// LEFT in the middle, INNER after: a NULL-extended middle side has a
	// NULL join key, so the inner join drops the row — same 3 rows.
	res = execSQL(t, ctx, s, `SELECT r.name, o.total FROM regions r
		LEFT JOIN customers c ON c.region_id = r.id
		JOIN orders o ON o.customer_id = c.id ORDER BY o.total`)
	if len(res.Rows) != 3 {
		t.Fatalf("LEFT-then-INNER: %+v", res.Rows)
	}

	// LEFT all the way down: north (no customers) and east/cat (no
	// orders) survive NULL-extended.
	res = execSQL(t, ctx, s, `SELECT r.name, c.name, o.total FROM regions r
		LEFT JOIN customers c ON c.region_id = r.id
		LEFT JOIN orders o ON o.customer_id = c.id
		ORDER BY r.name, c.name, o.total`)
	if len(res.Rows) != 5 {
		t.Fatalf("double LEFT: %+v", res.Rows)
	}
	// east row: customer cat, NULL total; north row: both NULL.
	if res.Rows[0][0].S != "east" || !res.Rows[0][2].Null {
		t.Fatalf("east NULL extension: %+v", res.Rows[0])
	}
	if res.Rows[1][0].S != "north" || !res.Rows[1][1].Null || !res.Rows[1][2].Null {
		t.Fatalf("north NULL extension: %+v", res.Rows[1])
	}

	// WHERE on a LEFT-joined side filters AFTER NULL-extension (never
	// pushed into the side's scan): the NULL-total rows drop.
	res = execSQL(t, ctx, s, `SELECT r.name, o.total FROM regions r
		LEFT JOIN customers c ON c.region_id = r.id
		LEFT JOIN orders o ON o.customer_id = c.id
		WHERE o.total > 40 ORDER BY o.total`)
	if len(res.Rows) != 2 || res.Rows[0][1].I != 50 || res.Rows[1][1].I != 70 {
		t.Fatalf("WHERE over LEFT side: %+v", res.Rows)
	}
	// ...while IS NULL sees the extension (anti-join through two levels).
	res = execSQL(t, ctx, s, `SELECT r.name FROM regions r
		LEFT JOIN customers c ON c.region_id = r.id
		LEFT JOIN orders o ON o.customer_id = c.id
		WHERE o.id IS NULL ORDER BY r.name`)
	if len(res.Rows) != 2 || res.Rows[0][0].S != "east" || res.Rows[1][0].S != "north" {
		t.Fatalf("two-level anti-join: %+v", res.Rows)
	}

	// Skip-level ON: items joins on side 0's order id from level 2.
	res = execSQL(t, ctx, s, `SELECT o.id, i.qty FROM orders o
		JOIN customers c ON o.customer_id = c.id
		JOIN items i ON i.order_id = o.id
		ORDER BY i.qty`)
	if len(res.Rows) != 3 || res.Rows[0][1].I != 2 || res.Rows[2][1].I != 5 {
		t.Fatalf("skip-level ON: %+v", res.Rows)
	}

	// 4-way chain with a parameter.
	res = execSQL(t, ctx, s, `SELECT r.name, i.qty FROM regions r
		JOIN customers c ON c.region_id = r.id
		JOIN orders o ON o.customer_id = c.id
		JOIN items i ON i.order_id = o.id
		WHERE r.name = $1 ORDER BY i.qty`, types.NewString("west"))
	if len(res.Rows) != 3 || res.Rows[2][1].I != 5 {
		t.Fatalf("4-way with param: %+v", res.Rows)
	}

	// SELECT * expands every side's columns in join order.
	res = execSQL(t, ctx, s, `SELECT * FROM regions r
		JOIN customers c ON c.region_id = r.id
		JOIN orders o ON o.customer_id = c.id LIMIT 1`)
	if len(res.Columns) != 2+3+3 {
		t.Fatalf("* over 3-way: %d columns", len(res.Columns))
	}

	// ON must reference the joined table and an EARLIER one.
	if _, serr := trySQL(ctx, s, `SELECT r.name FROM regions r
		JOIN customers c ON c.id = o.id
		JOIN orders o ON o.customer_id = c.id`); serr == nil || serr.Code != sql.CodeUndefinedTable {
		t.Fatalf("forward ON reference: %+v", serr)
	}
	// Same-side ON still rejected at any level.
	if _, serr := trySQL(ctx, s, `SELECT r.name FROM regions r
		JOIN customers c ON c.region_id = r.id
		JOIN orders o ON c.id = c.id`); serr == nil || serr.Code != sql.CodeFeatureNotSupported {
		t.Fatalf("same-side ON at level 2: %+v", serr)
	}

	// EXPLAIN renders one line per level; the indexed side shows its path.
	p := explainPlan(t, ctx, s, `SELECT r.name, o.total FROM regions r
		JOIN customers c ON c.region_id = r.id
		JOIN orders o ON o.customer_id = c.id`)
	if !strings.Contains(p, "inner (c) per outer row") || !strings.Contains(p, `then inner (o) per row: scan of index "by_customer"`) {
		t.Fatalf("3-way plan: %s", p)
	}

	// The parser caps the chain at 8 tables.
	long := `SELECT 1 FROM t0 `
	for i := 1; i < 9; i++ {
		long += `JOIN t` + string(rune('0'+i)) + ` ON t0.a = t` + string(rune('0'+i)) + `.a `
	}
	if _, serr := trySQL(ctx, s, long); serr == nil || !strings.Contains(serr.Msg, "too many joined tables") {
		t.Fatalf("join cap: %+v", serr)
	}
}

// TestJoinAggregates: GROUP BY and aggregates over joined rows — across
// sides, under LEFT extension, with HAVING and result-name ORDER BY — and
// the grouped Describe path over pgwire.
func TestJoinAggregates(t *testing.T) {
	tc := Start(t, 3)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	s := sql.NewSession(tc.Nodes[0].DB(), catalog.NewAccessor())
	nwaySchema(t, ctx, s)

	// SUM per customer, ordered by the aggregate's result name.
	res := execSQL(t, ctx, s, `SELECT c.name, SUM(o.total) AS spent FROM customers c
		JOIN orders o ON o.customer_id = c.id
		GROUP BY c.name ORDER BY spent DESC`)
	if len(res.Rows) != 2 || res.Rows[0][0].S != "ann" || res.Rows[0][1].I != 120 ||
		res.Rows[1][0].S != "bob" || res.Rows[1][1].I != 30 {
		t.Fatalf("grouped join: %+v", res.Rows)
	}
	if res.Columns[0].Name != "name" || res.Columns[1].Name != "spent" {
		t.Fatalf("grouped join headers: %+v", res.Columns)
	}

	// HAVING over the joined aggregate.
	res = execSQL(t, ctx, s, `SELECT c.name FROM customers c
		JOIN orders o ON o.customer_id = c.id
		GROUP BY c.name HAVING SUM(o.total) > 50`)
	if len(res.Rows) != 1 || res.Rows[0][0].S != "ann" {
		t.Fatalf("grouped join HAVING: %+v", res.Rows)
	}

	// Grouping a 3-way join by a side-0 column.
	res = execSQL(t, ctx, s, `SELECT r.name, COUNT(*) AS n FROM regions r
		JOIN customers c ON c.region_id = r.id
		JOIN orders o ON o.customer_id = c.id
		GROUP BY r.name`)
	if len(res.Rows) != 1 || res.Rows[0][0].S != "west" || res.Rows[0][1].I != 3 {
		t.Fatalf("grouped 3-way: %+v", res.Rows)
	}

	// COUNT(*) over a LEFT join counts NULL-extended rows.
	res = execSQL(t, ctx, s, `SELECT r.name, COUNT(*) AS n FROM regions r
		LEFT JOIN customers c ON c.region_id = r.id
		GROUP BY r.name ORDER BY n DESC, name`)
	if len(res.Rows) != 3 || res.Rows[0][0].S != "west" || res.Rows[0][1].I != 2 ||
		res.Rows[1][1].I != 1 || res.Rows[2][1].I != 1 {
		t.Fatalf("LEFT grouped: %+v", res.Rows)
	}
	// ...while COUNT(column) skips the NULLs.
	res = execSQL(t, ctx, s, `SELECT COUNT(c.id) AS n FROM regions r
		LEFT JOIN customers c ON c.region_id = r.id`)
	if res.Rows[0][0].I != 3 {
		t.Fatalf("COUNT(col) over LEFT: %+v", res.Rows)
	}

	// A bare column that exists on two sides is ambiguous in GROUP BY.
	if _, serr := trySQL(ctx, s, `SELECT COUNT(*) FROM customers c
		JOIN orders o ON o.customer_id = c.id GROUP BY id`); serr == nil || serr.Code != sql.CodeAmbiguousColumn {
		t.Fatalf("ambiguous GROUP BY: %+v", serr)
	}
	// Non-grouped bare column still errors with the standard message.
	if _, serr := trySQL(ctx, s, `SELECT c.name, SUM(o.total) FROM customers c
		JOIN orders o ON o.customer_id = c.id GROUP BY c.id`); serr == nil || serr.Code != sql.CodeGrouping {
		t.Fatalf("non-grouped select item: %+v", serr)
	}

	// The extended protocol Describes grouped-join output correctly.
	conn, err := pgx.Connect(ctx, pgURL(tc, 0))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close(ctx) }()
	rows, err := conn.Query(ctx, `SELECT c.name, SUM(o.total) AS spent FROM customers c
		JOIN orders o ON o.customer_id = c.id
		WHERE o.total > $1 GROUP BY c.name ORDER BY spent DESC`, 20)
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	var sums []int64
	for rows.Next() {
		var n string
		var v int64
		if err := rows.Scan(&n, &v); err != nil {
			t.Fatal(err)
		}
		names = append(names, n)
		sums = append(sums, v)
	}
	if rows.Err() != nil {
		t.Fatal(rows.Err())
	}
	if len(names) != 2 || names[0] != "ann" || sums[0] != 120 || sums[1] != 30 {
		t.Fatalf("pgwire grouped join: %v %v", names, sums)
	}
}
