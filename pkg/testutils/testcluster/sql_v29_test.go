package testcluster

import (
	"context"
	"testing"
	"time"

	"github.com/sthorne/datax/pkg/sql"
	"github.com/sthorne/datax/pkg/sql/catalog"
	"github.com/sthorne/datax/pkg/sql/types"
)

func TestOrderBy(t *testing.T) {
	tc := Start(t, 3)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	s := sql.NewSession(tc.Nodes[0].DB(), catalog.NewAccessor())

	execSQL(t, ctx, s, `CREATE TABLE items (id INT PRIMARY KEY, price INT, name TEXT)`)
	execSQL(t, ctx, s, `INSERT INTO items VALUES (1, 30, 'c'), (2, 10, 'a'), (3, 20, NULL), (4, 10, 'b')`)

	// Sort by a non-key column, DESC, with a NULL (PG default: NULLS FIRST
	// on DESC applies to the sorted column only when it is the sort key).
	res := execSQL(t, ctx, s, `SELECT id FROM items ORDER BY price DESC, id ASC`)
	want := []int64{1, 3, 2, 4}
	if len(res.Rows) != 4 {
		t.Fatalf("rows: %+v", res.Rows)
	}
	for i, w := range want {
		if res.Rows[i][0].I != w {
			t.Fatalf("order: got %+v, want %v", res.Rows, want)
		}
	}
	// NULLS LAST on ASC.
	res = execSQL(t, ctx, s, `SELECT id FROM items ORDER BY name`)
	if got := res.Rows[len(res.Rows)-1][0].I; got != 3 {
		t.Fatalf("NULL name should sort last: %+v", res.Rows)
	}

	// ORDER BY primary key on a full scan needs no sort; EXPLAIN shows it.
	if p := explainPlan(t, ctx, s, `SELECT id FROM items ORDER BY id`); p != "full table scan; order satisfied by access path" {
		t.Fatalf("plan: %s", p)
	}
	if p := explainPlan(t, ctx, s, `SELECT id FROM items ORDER BY price`); p != "full table scan; in-memory sort" {
		t.Fatalf("plan: %s", p)
	}
	// Index order fast path.
	execSQL(t, ctx, s, `CREATE INDEX by_price ON items (price)`)
	if p := explainPlan(t, ctx, s, `SELECT id FROM items WHERE price = 10 ORDER BY price, id`); p != `scan of index "by_price" (1 column prefix) + primary key join; order satisfied by access path` {
		t.Fatalf("plan: %s", p)
	}

	// LIMIT applies after the sort.
	res = execSQL(t, ctx, s, `SELECT id FROM items ORDER BY price DESC, id LIMIT 2`)
	if len(res.Rows) != 2 || res.Rows[0][0].I != 1 || res.Rows[1][0].I != 3 {
		t.Fatalf("limit after sort: %+v", res.Rows)
	}
}

func TestAggregates(t *testing.T) {
	tc := Start(t, 3)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	s := sql.NewSession(tc.Nodes[0].DB(), catalog.NewAccessor())

	execSQL(t, ctx, s, `CREATE TABLE m (id INT PRIMARY KEY, v INT, f FLOAT, s TEXT)`)
	execSQL(t, ctx, s, `INSERT INTO m VALUES (1, 10, 1.5, 'b'), (2, 20, 2.5, 'a'), (3, NULL, NULL, 'c'), (4, 30, 4.0, NULL)`)

	res := execSQL(t, ctx, s, `SELECT COUNT(*), COUNT(v), SUM(v), AVG(v), MIN(v), MAX(v) FROM m`)
	r := res.Rows[0]
	if r[0].I != 4 || r[1].I != 3 || r[2].I != 60 || r[3].Fam != types.Decimal || r[3].Text() != "20" || r[4].I != 10 || r[5].I != 30 {
		t.Fatalf("aggregates: %+v", r)
	}
	res = execSQL(t, ctx, s, `SELECT SUM(f), MIN(s), MAX(s) FROM m`)
	r = res.Rows[0]
	if r[0].F != 8.0 || r[1].S != "a" || r[2].S != "c" {
		t.Fatalf("aggregates: %+v", r)
	}
	// Aggregates respect WHERE.
	res = execSQL(t, ctx, s, `SELECT COUNT(*), SUM(v) FROM m WHERE id <= 2`)
	if res.Rows[0][0].I != 2 || res.Rows[0][1].I != 30 {
		t.Fatalf("filtered aggregates: %+v", res.Rows[0])
	}
	// Empty input: COUNT 0, others NULL.
	res = execSQL(t, ctx, s, `SELECT COUNT(*), SUM(v), MIN(v) FROM m WHERE id > 100`)
	if res.Rows[0][0].I != 0 || !res.Rows[0][1].Null || !res.Rows[0][2].Null {
		t.Fatalf("empty aggregates: %+v", res.Rows[0])
	}
	// Mixing aggregates and plain columns errors.
	if _, serr := trySQL(ctx, s, `SELECT id, COUNT(*) FROM m`); serr == nil {
		t.Fatal("mixed aggregate accepted")
	}
	// SUM over TEXT errors.
	if _, serr := trySQL(ctx, s, `SELECT SUM(s) FROM m`); serr == nil {
		t.Fatal("SUM over TEXT accepted")
	}
}

func TestAlterTable(t *testing.T) {
	tc := Start(t, 3)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	s := sql.NewSession(tc.Nodes[0].DB(), catalog.NewAccessor())

	execSQL(t, ctx, s, `CREATE TABLE t (id INT PRIMARY KEY, a TEXT)`)
	execSQL(t, ctx, s, `INSERT INTO t VALUES (1, 'old')`)

	// ADD COLUMN: old rows read the new column as NULL.
	execSQL(t, ctx, s, `ALTER TABLE t ADD COLUMN b INT`)
	res := execSQL(t, ctx, s, `SELECT a, b FROM t WHERE id = 1`)
	if res.Rows[0][0].S != "old" || !res.Rows[0][1].Null {
		t.Fatalf("old row after ADD: %+v", res.Rows[0])
	}
	execSQL(t, ctx, s, `INSERT INTO t VALUES (2, 'new', 7)`)
	res = execSQL(t, ctx, s, `SELECT b FROM t WHERE id = 2`)
	if res.Rows[0][0].I != 7 {
		t.Fatalf("new row: %+v", res.Rows[0])
	}
	// NOT NULL adds are refused.
	if _, serr := trySQL(ctx, s, `ALTER TABLE t ADD COLUMN c INT NOT NULL`); serr == nil {
		t.Fatal("NOT NULL ADD COLUMN accepted")
	}

	// DROP COLUMN: lazy — old bytes ignored; re-add gets a fresh column ID,
	// so the old values must NOT resurrect.
	execSQL(t, ctx, s, `ALTER TABLE t DROP COLUMN b`)
	if _, serr := trySQL(ctx, s, `SELECT b FROM t`); serr == nil {
		t.Fatal("dropped column still selectable")
	}
	execSQL(t, ctx, s, `ALTER TABLE t ADD COLUMN b INT`)
	res = execSQL(t, ctx, s, `SELECT b FROM t WHERE id = 2`)
	if !res.Rows[0][0].Null {
		t.Fatalf("dropped value resurrected: %+v", res.Rows[0])
	}

	// Guard rails: PK and indexed columns cannot be dropped.
	if _, serr := trySQL(ctx, s, `ALTER TABLE t DROP COLUMN id`); serr == nil {
		t.Fatal("PK column drop accepted")
	}
	execSQL(t, ctx, s, `CREATE INDEX by_a ON t (a)`)
	if _, serr := trySQL(ctx, s, `ALTER TABLE t DROP COLUMN a`); serr == nil {
		t.Fatal("indexed column drop accepted")
	}
}
