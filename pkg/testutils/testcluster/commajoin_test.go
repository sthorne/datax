package testcluster

import (
	"context"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/sthorne/datax/pkg/sql"
	"github.com/sthorne/datax/pkg/sql/catalog"
)

// TestCommaJoinWhereKeys: a comma (or CROSS) join whose WHERE equates
// its sides runs as an inner join keyed by those equalities, not as a
// cross product filtered at the end. psql's \d <index> is such a join
// over pg_index, pg_class twice and pg_am; as a cross product it built
// |pg_index| x |pg_class|^2 x |pg_am| rows and rescanned the inner
// catalogs once per partial row, which took 20-35 s on a node whose
// catalogs list the system tables.
func TestCommaJoinWhereKeys(t *testing.T) {
	tc := Start(t, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	s := sql.NewSession(tc.Nodes[0].DB(), catalog.NewAccessor())
	waitForDatabases(t, ctx, s)

	execSQL(t, ctx, s, `CREATE TABLE customers (id INT PRIMARY KEY, name TEXT)`)
	execSQL(t, ctx, s, `CREATE TABLE orders (id INT PRIMARY KEY, customer_id INT, total INT)`)
	execSQL(t, ctx, s, `CREATE INDEX by_customer ON orders (customer_id)`)
	execSQL(t, ctx, s, `INSERT INTO customers VALUES (1, 'ann'), (2, 'bob'), (3, 'cy')`)
	execSQL(t, ctx, s, `INSERT INTO orders VALUES (100, 1, 50), (101, 2, 30), (102, 1, 70), (103, NULL, 10)`)
	// More catalog rows, so a cross product would show in the counts.
	for i := 0; i < 6; i++ {
		execSQL(t, ctx, s, fmt.Sprintf(`CREATE TABLE pad%d (id INT PRIMARY KEY, v INT)`, i))
		execSQL(t, ctx, s, fmt.Sprintf(`CREATE INDEX pad%d_v ON pad%d (v)`, i, i))
	}

	rowsOf := func(q string) string {
		t.Helper()
		var out []string
		for _, r := range execSQL(t, ctx, s, q).Rows {
			var cells []string
			for _, d := range r {
				cells = append(cells, d.Text())
			}
			out = append(out, strings.Join(cells, "|"))
		}
		return strings.Join(out, ",")
	}

	// The WHERE equality becomes the inner side's key: a point lookup on
	// its primary key, or a scan of its index.
	if p := explainPlan(t, ctx, s, `SELECT o.id, c.name FROM orders o, customers c WHERE o.customer_id = c.id`); p != "nested loop inner join; outer (o): full table scan; inner (c) per outer row: point lookup on primary key" {
		t.Fatalf("plan: %s", p)
	}
	if p := explainPlan(t, ctx, s, `SELECT o.id FROM customers c CROSS JOIN orders o WHERE c.name = 'ann' AND c.id = o.customer_id`); p != `nested loop inner join; outer (c): full table scan; inner (o) per outer row: scan of index "by_customer" (1 column prefix) + primary key join` {
		t.Fatalf("plan: %s", p)
	}
	// Without an equality between the sides it stays a cross join.
	if p := explainPlan(t, ctx, s, `SELECT o.id FROM customers c, orders o WHERE o.total > c.id`); !strings.Contains(p, "cross") {
		t.Fatalf("plan: %s", p)
	}

	// Results are the inner join's, NULL keys matching nothing; a
	// constant on a later side narrows that side; a LEFT side keeps its
	// NULL-extended rows for the WHERE to judge.
	for q, want := range map[string]string{
		`SELECT o.id, c.name FROM orders o, customers c WHERE o.customer_id = c.id ORDER BY o.id`:                                                "100|ann,101|bob,102|ann",
		`SELECT o.id FROM customers c, orders o WHERE c.id = o.customer_id AND o.total = 70`:                                                     "102",
		`SELECT c.name, o.id FROM customers c, orders o WHERE o.customer_id = c.id AND c.id = 1 ORDER BY o.id`:                                   "ann|100,ann|102",
		`SELECT c.name FROM customers c LEFT JOIN orders o ON o.customer_id = c.id WHERE o.id IS NULL`:                                           "cy",
		`SELECT c.name, o2.id FROM customers c LEFT JOIN orders o ON o.customer_id = c.id, orders o2 WHERE o2.customer_id = c.id AND o.id = 101`: "bob|101",
		`SELECT count(*) FROM customers c, orders o WHERE o.customer_id = c.id`:                                                                  "3",
	} {
		if got := rowsOf(q); got != want {
			t.Fatalf("%s:\n got %q\nwant %q", q, got, want)
		}
	}

	// psql's \d <index> join: every level holds the one matching row,
	// and each inner catalog is read once per partial row, not once per
	// row of a cross product.
	oid := execSQL(t, ctx, s, `SELECT oid FROM pg_class WHERE relname = 'by_customer'`).Rows[0][0].I
	q := `SELECT i.indisunique, i.indisprimary, a.amname, c2.relname
FROM pg_catalog.pg_index i, pg_catalog.pg_class c, pg_catalog.pg_class c2, pg_catalog.pg_am a
WHERE i.indexrelid = c.oid AND c.oid = '` + strconv.FormatInt(oid, 10) + `' AND c.relam = a.oid
AND i.indrelid = c2.oid`
	if got := rowsOf(q); got != "f|f|btree|orders" {
		t.Fatalf("\\d by_customer's join: %q", got)
	}
	var lines []string
	for _, r := range execSQL(t, ctx, s, "EXPLAIN ANALYZE "+q).Rows {
		lines = append(lines, r[0].Text())
	}
	analyzed := strings.Join(lines, "\n")
	if strings.Contains(lines[0], "cross") {
		t.Fatalf("still a cross join:\n%s", analyzed)
	}
	level := regexp.MustCompile(`^join level (\d) \((\w+), (\w+)\): (\d+) rows$`)
	levels, amScans := 0, 0
	for _, l := range lines {
		l = strings.TrimSpace(l)
		if strings.HasPrefix(l, "scan pg_am:") {
			amScans++
		}
		m := level.FindStringSubmatch(l)
		if m == nil {
			continue
		}
		levels++
		if n, _ := strconv.Atoi(m[4]); n != 1 || m[3] != "inner" {
			t.Fatalf("join level %s (%s): %s rows, %s:\n%s", m[1], m[2], m[4], m[3], analyzed)
		}
	}
	if levels != 3 || amScans != 1 {
		t.Fatalf("%d join levels, %d pg_am scans (want 3, 1):\n%s", levels, amScans, analyzed)
	}
}
