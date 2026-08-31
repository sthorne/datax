package testcluster

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/sthorne/datax/pkg/metrics"
	"github.com/sthorne/datax/pkg/sql"
	"github.com/sthorne/datax/pkg/sql/catalog"
)

// TestPKRangeScans: range predicates on primary-key columns become bounded
// KV scans (EXPLAIN-asserted) and return exactly the matching rows.
func TestPKRangeScans(t *testing.T) {
	tc := Start(t, 3)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	s := sql.NewSession(tc.Nodes[0].DB(), catalog.NewAccessor())

	execSQL(t, ctx, s, `CREATE TABLE m (a INT, b INT, v TEXT, PRIMARY KEY (a, b))`)
	for a := 1; a <= 3; a++ {
		for b := 1; b <= 10; b++ {
			execSQL(t, ctx, s, fmt.Sprintf(`INSERT INTO m VALUES (%d, %d, 'r%d%d')`, a, b, a, b))
		}
	}

	// Pinned prefix + range on the next PK column.
	q := `SELECT a, b FROM m WHERE a = 2 AND b > 3 AND b <= 7`
	if p := explainPlan(t, ctx, s, q); p != "range scan of primary key (a = 2, b > 3, b <= 7)" {
		t.Fatalf("plan: %s", p)
	}
	res := execSQL(t, ctx, s, q)
	if len(res.Rows) != 4 {
		t.Fatalf("rows: %+v", res.Rows)
	}
	for i, want := range []int64{4, 5, 6, 7} {
		if res.Rows[i][0].I != 2 || res.Rows[i][1].I != want {
			t.Fatalf("row %d = %+v, want (2, %d)", i, res.Rows[i], want)
		}
	}

	// Range on the first PK column alone.
	q = `SELECT a, b FROM m WHERE a > 2`
	if p := explainPlan(t, ctx, s, q); p != "range scan of primary key (a > 2)" {
		t.Fatalf("plan: %s", p)
	}
	if res := execSQL(t, ctx, s, q); len(res.Rows) != 10 {
		t.Fatalf("rows: %d", len(res.Rows))
	}

	// A range on b without pinning a cannot bound the scan.
	q = `SELECT a, b FROM m WHERE b > 8`
	if p := explainPlan(t, ctx, s, q); p != "full table scan" {
		t.Fatalf("plan: %s", p)
	}
	if res := execSQL(t, ctx, s, q); len(res.Rows) != 6 {
		t.Fatalf("rows: %d", len(res.Rows))
	}

	// A PK range scan preserves primary-key order.
	q = `SELECT a, b FROM m WHERE a = 2 AND b >= 5 ORDER BY a, b`
	if p := explainPlan(t, ctx, s, q); p != "range scan of primary key (a = 2, b >= 5); order satisfied by access path" {
		t.Fatalf("plan: %s", p)
	}

	// UPDATE and DELETE ride the same bounded path.
	execSQL(t, ctx, s, `UPDATE m SET v = 'upd' WHERE a = 1 AND b >= 9`)
	if res := execSQL(t, ctx, s, `SELECT b FROM m WHERE v = 'upd'`); len(res.Rows) != 2 {
		t.Fatalf("updated rows: %+v", res.Rows)
	}
	execSQL(t, ctx, s, `DELETE FROM m WHERE a = 3 AND b < 6`)
	if res := execSQL(t, ctx, s, `SELECT b FROM m WHERE a = 3`); len(res.Rows) != 5 {
		t.Fatalf("remaining a=3 rows: %+v", res.Rows)
	}
}

// TestIndexRangeScans: range predicates on the trailing constrained index
// column become bounded index scans, including string bounds.
func TestIndexRangeScans(t *testing.T) {
	tc := Start(t, 3)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	s := sql.NewSession(tc.Nodes[0].DB(), catalog.NewAccessor())

	execSQL(t, ctx, s, `CREATE TABLE ev (id INT PRIMARY KEY, kind TEXT NOT NULL, score INT NOT NULL, note TEXT)`)
	execSQL(t, ctx, s, `CREATE INDEX by_kind_score ON ev (kind, score)`)
	for i := 1; i <= 20; i++ {
		kind := "alpha"
		if i%2 == 0 {
			kind = "beta"
		}
		execSQL(t, ctx, s, fmt.Sprintf(`INSERT INTO ev VALUES (%d, '%s', %d, NULL)`, i, kind, i*10))
	}

	q := `SELECT id FROM ev WHERE kind = 'beta' AND score >= 40 AND score < 120`
	if p := explainPlan(t, ctx, s, q); p != `scan of index "by_kind_score" (kind = 'beta', score >= 40, score < 120) + primary key join` {
		t.Fatalf("plan: %s", p)
	}
	res := execSQL(t, ctx, s, q)
	if len(res.Rows) != 4 { // ids 4, 6, 8, 10 → scores 40, 60, 80, 100
		t.Fatalf("rows: %+v", res.Rows)
	}

	// String range on the first index column.
	q = `SELECT id FROM ev WHERE kind >= 'b' AND kind < 'c'`
	if p := explainPlan(t, ctx, s, q); p != `scan of index "by_kind_score" (kind >= 'b', kind < 'c') + primary key join` {
		t.Fatalf("plan: %s", p)
	}
	if res := execSQL(t, ctx, s, q); len(res.Rows) != 10 {
		t.Fatalf("rows: %d", len(res.Rows))
	}
}

// TestIsNullPredicates: IS [NOT] NULL parse, filter correctly, and force a
// full scan when an indexed column is involved (NULL rows have no index
// entry, so an index scan would silently miss them).
func TestIsNullPredicates(t *testing.T) {
	tc := Start(t, 3)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	s := sql.NewSession(tc.Nodes[0].DB(), catalog.NewAccessor())

	execSQL(t, ctx, s, `CREATE TABLE p (id INT PRIMARY KEY, c INT, d INT)`)
	execSQL(t, ctx, s, `CREATE INDEX by_cd ON p (c, d)`)
	execSQL(t, ctx, s, `INSERT INTO p VALUES (1, 5, 1), (2, 5, NULL), (3, NULL, 1), (4, 7, 2)`)

	// The NULL row in c must be found — only a full scan can see it.
	q := `SELECT id FROM p WHERE c IS NULL`
	if p := explainPlan(t, ctx, s, q); p != "full table scan" {
		t.Fatalf("plan: %s", p)
	}
	res := execSQL(t, ctx, s, q)
	if len(res.Rows) != 1 || res.Rows[0][0].I != 3 {
		t.Fatalf("c IS NULL rows: %+v", res.Rows)
	}
	if res := execSQL(t, ctx, s, `SELECT id FROM p WHERE c IS NOT NULL`); len(res.Rows) != 3 {
		t.Fatalf("c IS NOT NULL rows: %+v", res.Rows)
	}

	// c = 5 alone cannot use by_cd (row (2, 5, NULL) has no index entry) —
	// and the result must include id 2.
	q = `SELECT id FROM p WHERE c = 5`
	if p := explainPlan(t, ctx, s, q); p != "full table scan" {
		t.Fatalf("plan: %s", p)
	}
	if res := execSQL(t, ctx, s, q); len(res.Rows) != 2 {
		t.Fatalf("c = 5 rows: %+v", res.Rows)
	}

	// Adding d IS NOT NULL makes the index a complete row source again.
	q = `SELECT id FROM p WHERE c = 5 AND d IS NOT NULL`
	if p := explainPlan(t, ctx, s, q); p != `scan of index "by_cd" (1 column prefix) + primary key join` {
		t.Fatalf("plan: %s", p)
	}
	res = execSQL(t, ctx, s, q)
	if len(res.Rows) != 1 || res.Rows[0][0].I != 1 {
		t.Fatalf("rows: %+v", res.Rows)
	}
}

// TestLimitPushdown: with no residual filter the LIMIT rides into the KV
// scan — observable as scanned-row counts (datax_sql_rows_scanned_total).
func TestLimitPushdown(t *testing.T) {
	tc := Start(t, 3)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	s := sql.NewSession(tc.Nodes[0].DB(), catalog.NewAccessor())

	execSQL(t, ctx, s, `CREATE TABLE big (id INT PRIMARY KEY, v TEXT)`)
	for i := 1; i <= 100; i++ {
		execSQL(t, ctx, s, fmt.Sprintf(`INSERT INTO big VALUES (%d, 'v%d')`, i, i))
	}

	scanned := func(query string) float64 {
		before := testutil.ToFloat64(metrics.SQLRowsScanned)
		execSQL(t, ctx, s, query)
		return testutil.ToFloat64(metrics.SQLRowsScanned) - before
	}

	// Bare LIMIT: pushed.
	if p := explainPlan(t, ctx, s, `SELECT id FROM big LIMIT 5`); p != "full table scan; limit pushed into scan" {
		t.Fatalf("plan: %s", p)
	}
	if n := scanned(`SELECT id FROM big LIMIT 5`); n != 5 {
		t.Fatalf("scanned %v rows, want 5", n)
	}

	// PK range + LIMIT, fully absorbed WHERE: pushed.
	q := `SELECT id FROM big WHERE id > 10 LIMIT 3`
	if p := explainPlan(t, ctx, s, q); p != "range scan of primary key (id > 10); limit pushed into scan" {
		t.Fatalf("plan: %s", p)
	}
	if n := scanned(q); n != 3 {
		t.Fatalf("scanned %v rows, want 3", n)
	}

	// ORDER BY satisfied by the access path keeps the pushdown.
	q = `SELECT id FROM big ORDER BY id LIMIT 7`
	if p := explainPlan(t, ctx, s, q); p != "full table scan; order satisfied by access path; limit pushed into scan" {
		t.Fatalf("plan: %s", p)
	}
	if n := scanned(q); n != 7 {
		t.Fatalf("scanned %v rows, want 7", n)
	}

	// A residual filter blocks the pushdown (the scan must see everything).
	q = `SELECT id FROM big WHERE v != 'v1' LIMIT 5`
	if p := explainPlan(t, ctx, s, q); p != "full table scan" {
		t.Fatalf("plan: %s", p)
	}
	if n := scanned(q); n != 100 {
		t.Fatalf("scanned %v rows, want 100", n)
	}

	// An ORDER BY needing a sort disables it too (all rows feed the sort).
	q = `SELECT id FROM big ORDER BY v LIMIT 5`
	if p := explainPlan(t, ctx, s, q); p != "full table scan; in-memory sort" {
		t.Fatalf("plan: %s", p)
	}
	if n := scanned(q); n != 100 {
		t.Fatalf("scanned %v rows, want 100", n)
	}

	// Correctness under pushdown: the right rows, in order.
	res := execSQL(t, ctx, s, `SELECT id FROM big WHERE id >= 42 LIMIT 4`)
	if len(res.Rows) != 4 {
		t.Fatalf("rows: %+v", res.Rows)
	}
	for i, want := range []int64{42, 43, 44, 45} {
		if res.Rows[i][0].I != want {
			t.Fatalf("row %d = %+v, want %d", i, res.Rows[i], want)
		}
	}
}
