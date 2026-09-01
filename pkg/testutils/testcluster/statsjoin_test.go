package testcluster

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

// seedJoinTables creates users/orders/products and returns after seeding:
// orders is the big table (400 rows), users medium (20), products small
// (5), so a query written orders-first is worst-first.
func seedJoinTables(t testing.TB, ctx context.Context, conn *pgx.Conn) {
	for _, q := range []string{
		`CREATE TABLE users (id INT8 PRIMARY KEY, name TEXT NOT NULL)`,
		`CREATE TABLE products (id INT8 PRIMARY KEY, sku TEXT NOT NULL)`,
		`CREATE TABLE orders (id INT8 PRIMARY KEY, user_id INT8 NOT NULL, product_id INT8 NOT NULL, qty INT8 NOT NULL)`,
		`CREATE INDEX orders_by_user ON orders (user_id)`,
		`CREATE INDEX orders_by_product ON orders (product_id)`,
	} {
		if _, err := conn.Exec(ctx, q); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}
	batch := &pgx.Batch{}
	for i := 0; i < 20; i++ {
		batch.Queue(`INSERT INTO users VALUES ($1, $2)`, int64(i), fmt.Sprintf("user-%d", i))
	}
	for i := 0; i < 5; i++ {
		batch.Queue(`INSERT INTO products VALUES ($1, $2)`, int64(i), fmt.Sprintf("sku-%d", i))
	}
	for i := 0; i < 400; i++ {
		batch.Queue(`INSERT INTO orders VALUES ($1, $2, $3, $4)`,
			int64(i), int64(i%20), int64(i%5), int64(1+i%7))
	}
	if err := conn.SendBatch(ctx, batch).Close(); err != nil {
		t.Fatalf("seed: %v", err)
	}
}

// readAll renders every row as a line and returns the column names and
// sorted lines (joins without ORDER BY have no guaranteed row order).
func readAll(t testing.TB, ctx context.Context, conn *pgx.Conn, q string) ([]string, []string) {
	t.Helper()
	rows, err := conn.Query(ctx, q)
	if err != nil {
		t.Fatalf("%s: %v", q, err)
	}
	defer rows.Close()
	var cols []string
	for _, fd := range rows.FieldDescriptions() {
		cols = append(cols, fd.Name)
	}
	var out []string
	for rows.Next() {
		vals, err := rows.Values()
		if err != nil {
			t.Fatal(err)
		}
		out = append(out, fmt.Sprintf("%v", vals))
	}
	if rows.Err() != nil {
		t.Fatalf("%s: %v", q, rows.Err())
	}
	sort.Strings(out)
	return cols, out
}

// TestJoinReorderByCost: a worst-first 3-way join is rewritten to drive
// from the smallest side once statistics exist, with identical results
// and column order; LEFT joins and self-joins are never reordered.
// Issue #56 (SA4).
func TestJoinReorderByCost(t *testing.T) {
	tc := Start(t, 3)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	conn, err := pgx.Connect(ctx, pgURL(tc, 0))
	if err != nil {
		t.Fatalf("pgx connect: %v", err)
	}
	defer func() { _ = conn.Close(ctx) }()
	seedJoinTables(t, ctx, conn)

	explain := func(q string) string {
		t.Helper()
		var plan string
		if err := conn.QueryRow(ctx, "EXPLAIN "+q).Scan(&plan); err != nil {
			t.Fatalf("explain %q: %v", q, err)
		}
		return plan
	}

	worstFirst := `SELECT * FROM orders
		JOIN users ON users.id = orders.user_id
		JOIN products ON products.id = orders.product_id
		WHERE orders.qty >= 1`
	grouped := `SELECT users.name, COUNT(*) AS n FROM orders
		JOIN users ON users.id = orders.user_id
		JOIN products ON products.id = orders.product_id
		GROUP BY users.name ORDER BY users.name`

	// Without statistics: syntactic order, no estimates, no reorder note.
	prePlan := explain(worstFirst)
	if !strings.HasPrefix(prePlan, "nested loop inner join; outer (orders):") ||
		strings.Contains(prePlan, "reordered") || strings.Contains(prePlan, "[~") {
		t.Fatalf("pre-ANALYZE plan: %q", prePlan)
	}
	preCols, preRows := readAll(t, ctx, conn, worstFirst)
	_, preGrouped := readAll(t, ctx, conn, grouped)
	if len(preRows) != 400 {
		t.Fatalf("pre rows: %d", len(preRows))
	}

	if _, err := conn.Exec(ctx, `ANALYZE`); err != nil {
		t.Fatalf("analyze: %v", err)
	}

	// With statistics: products (5 rows) drives, the note appears, and
	// each side carries an estimate.
	postPlan := explain(worstFirst)
	if !strings.HasPrefix(postPlan, "nested loop inner join; outer (products):") ||
		!strings.HasSuffix(postPlan, "; join reordered by cost") ||
		!strings.Contains(postPlan, "[~") {
		t.Fatalf("post-ANALYZE plan: %q", postPlan)
	}

	// Identical results: same column names in the same order, same rows.
	postCols, postRows := readAll(t, ctx, conn, worstFirst)
	if fmt.Sprint(preCols) != fmt.Sprint(postCols) {
		t.Fatalf("column order changed: %v -> %v", preCols, postCols)
	}
	if fmt.Sprint(preRows) != fmt.Sprint(postRows) {
		t.Fatalf("rows changed after reorder (pre %d, post %d)", len(preRows), len(postRows))
	}

	// Grouped join over the reordered tree: same aggregates.
	if _, postGrouped := readAll(t, ctx, conn, grouped); fmt.Sprint(preGrouped) != fmt.Sprint(postGrouped) {
		t.Fatalf("grouped join changed after reorder")
	}

	// LEFT joins keep the written order (NULL-extension is order-sensitive).
	leftQ := `SELECT users.name, orders.id FROM users
		LEFT JOIN orders ON orders.user_id = users.id`
	leftPlan := explain(leftQ)
	if !strings.HasPrefix(leftPlan, "nested loop left join; outer (users):") ||
		strings.Contains(leftPlan, "reordered") {
		t.Fatalf("LEFT join plan: %q", leftPlan)
	}

	// Self-joins (duplicate table, distinct aliases) still run, unreordered.
	selfQ := `SELECT x.name FROM users AS x JOIN users AS y ON y.id = x.id WHERE y.name = 'user-3'`
	if p := explain(selfQ); strings.Contains(p, "reordered") {
		t.Fatalf("self-join reordered: %q", p)
	}
	_, selfRows := readAll(t, ctx, conn, selfQ)
	if len(selfRows) != 1 || !strings.Contains(selfRows[0], "user-3") {
		t.Fatalf("self-join rows: %v", selfRows)
	}
}

// benchThreeWayJoin measures one 3-way join per iteration over pgwire.
// The selective predicate keeps one product (80 of 400 orders), so
// driving from products is far cheaper than driving from orders.
func benchThreeWayJoin(b *testing.B, analyze bool, query string) {
	tc := Start(b, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Second)
	defer cancel()
	conn, err := pgx.Connect(ctx, pgURL(tc, 0))
	if err != nil {
		b.Fatalf("pgx connect: %v", err)
	}
	defer func() { _ = conn.Close(ctx) }()
	seedJoinTables(b, ctx, conn)
	if analyze {
		if _, err := conn.Exec(ctx, `ANALYZE`); err != nil {
			b.Fatalf("analyze: %v", err)
		}
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rows, err := conn.Query(ctx, query)
		if err != nil {
			b.Fatal(err)
		}
		n := 0
		for rows.Next() {
			n++
		}
		rows.Close()
		if rows.Err() != nil {
			b.Fatal(rows.Err())
		}
		if n != 80 {
			b.Fatalf("rows = %d, want 80", n)
		}
	}
}

const worstFirstJoin = `SELECT orders.id FROM orders
	JOIN users ON users.id = orders.user_id
	JOIN products ON products.id = orders.product_id
	WHERE products.sku = 'sku-3'`

const handOrderedJoin = `SELECT orders.id FROM products
	JOIN orders ON orders.product_id = products.id
	JOIN users ON users.id = orders.user_id
	WHERE products.sku = 'sku-3'`

// The three-way comparison: NoStats runs the written (worst) order,
// WorstFirst is the same query with statistics (the reorderer rewrites
// it), HandOrdered is the best order written by hand — the target the
// reordered query should get within a few percent of.
func Benchmark3WayJoinWorstFirstNoStats(b *testing.B) { benchThreeWayJoin(b, false, worstFirstJoin) }
func Benchmark3WayJoinWorstFirst(b *testing.B)        { benchThreeWayJoin(b, true, worstFirstJoin) }
func Benchmark3WayJoinHandOrdered(b *testing.B)       { benchThreeWayJoin(b, true, handOrderedJoin) }

// BenchmarkAnalyzeTable: one full statistics collection (row count + KMV
// sketches) over a 5000-row table per iteration.
func BenchmarkAnalyzeTable(b *testing.B) {
	tc := Start(b, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Second)
	defer cancel()
	conn, err := pgx.Connect(ctx, pgURL(tc, 0))
	if err != nil {
		b.Fatalf("pgx connect: %v", err)
	}
	defer func() { _ = conn.Close(ctx) }()
	if _, err := conn.Exec(ctx, `CREATE TABLE big (id INT8 PRIMARY KEY, v TEXT NOT NULL, grp INT8 NOT NULL)`); err != nil {
		b.Fatalf("create: %v", err)
	}
	batch := &pgx.Batch{}
	for i := 0; i < 5000; i++ {
		batch.Queue(`INSERT INTO big VALUES ($1, $2, $3)`, int64(i), fmt.Sprintf("v-%d", i), int64(i%50))
	}
	if err := conn.SendBatch(ctx, batch).Close(); err != nil {
		b.Fatalf("seed: %v", err)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := conn.Exec(ctx, `ANALYZE big`); err != nil {
			b.Fatal(err)
		}
	}
}
