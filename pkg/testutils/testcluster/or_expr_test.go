package testcluster

import (
	"context"
	"testing"
	"time"

	"github.com/sthorne/datax/pkg/sql"
	"github.com/sthorne/datax/pkg/sql/catalog"
	"github.com/sthorne/datax/pkg/sql/types"
)

// TestOrAndExpressions: OR/NOT filtering with SQL NULL semantics, OR under
// joins, arithmetic with precedence and exactness, and the builtin
// functions — the ORM-compatibility surface of issue #51.
func TestOrAndExpressions(t *testing.T) {
	tc := Start(t, 3)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	s := sql.NewSession(tc.Nodes[0].DB(), catalog.NewAccessor())

	execSQL(t, ctx, s, `CREATE TABLE p (id INT8 PRIMARY KEY, name TEXT, cat TEXT, price DECIMAL, qty INT8)`)
	execSQL(t, ctx, s, `INSERT INTO p VALUES
		(1, 'anvil', 'tools',  99.99, 3),
		(2, 'rope',  'gear',    5.25, 40),
		(3, 'tent',  'gear',  120.5,  2),
		(4, 'flint', NULL,      2.5,  7),
		(5, 'Stove', 'tools',  45,    0)`)

	ids := func(q string, params ...types.Datum) []int64 {
		t.Helper()
		res := execSQL(t, ctx, s, q, params...)
		var out []int64
		for _, r := range res.Rows {
			out = append(out, r[0].I)
		}
		return out
	}
	eq := func(got []int64, want ...int64) {
		t.Helper()
		if len(got) != len(want) {
			t.Fatalf("ids = %v, want %v", got, want)
		}
		for i := range want {
			if got[i] != want[i] {
				t.Fatalf("ids = %v, want %v", got, want)
			}
		}
	}

	// Plain OR across columns; NULL cat rows match neither disjunct.
	eq(ids(`SELECT id FROM p WHERE cat = 'tools' OR qty > 10 ORDER BY id`), 1, 2, 5)
	// Parenthesized grouping with AND.
	eq(ids(`SELECT id FROM p WHERE (cat = 'gear' OR cat IS NULL) AND price < 100 ORDER BY id`), 2, 4)
	// NOT over a group (De Morgan; UNKNOWN rows stay excluded).
	eq(ids(`SELECT id FROM p WHERE NOT (cat = 'gear' OR qty = 3) ORDER BY id`), 5)
	// Same-column ORs run as IN — including through parameters.
	eq(ids(`SELECT id FROM p WHERE id = $1 OR id = $2 ORDER BY id`, types.NewInt(2), types.NewInt(4)), 2, 4)
	// OR still narrows an indexed plan's RESULTS correctly (filter-only:
	// the enclosing AND's equality drives the plan, OR filters).
	execSQL(t, ctx, s, `CREATE INDEX by_cat ON p (cat)`)
	eq(ids(`SELECT id FROM p WHERE cat = 'gear' AND (qty > 10 OR price > 100) ORDER BY id`), 2, 3)

	// OR under a join.
	execSQL(t, ctx, s, `CREATE TABLE o (oid INT8 PRIMARY KEY, pid INT8, n INT8)`)
	execSQL(t, ctx, s, `INSERT INTO o VALUES (10, 1, 5), (11, 2, 1), (12, 4, 9)`)
	eq(ids(`SELECT o.oid FROM o AS o JOIN p AS p ON p.id = o.pid WHERE p.cat = 'tools' OR o.n > 8 ORDER BY o.oid`), 10, 12)

	// Arithmetic: precedence, integer division truncation, decimal
	// exactness, division by zero.
	res := execSQL(t, ctx, s, `SELECT qty + qty * 2 FROM p WHERE id = 1`)
	if res.Rows[0][0].I != 9 {
		t.Fatalf("precedence: %+v", res.Rows[0][0])
	}
	res = execSQL(t, ctx, s, `SELECT (qty + qty) * 2 FROM p WHERE id = 1`)
	if res.Rows[0][0].I != 12 {
		t.Fatalf("grouping: %+v", res.Rows[0][0])
	}
	res = execSQL(t, ctx, s, `SELECT qty / 2 FROM p WHERE id = 1`)
	if res.Rows[0][0].I != 1 { // 3 / 2 truncates
		t.Fatalf("int division: %+v", res.Rows[0][0])
	}
	res = execSQL(t, ctx, s, `SELECT price * 3 FROM p WHERE id = 4`)
	if d := res.Rows[0][0]; d.Fam != types.Decimal || d.S != "7.5" {
		t.Fatalf("decimal mul: %+v", d)
	}
	if _, serr := trySQL(ctx, s, `SELECT qty / 0 FROM p WHERE id = 1`); serr == nil || serr.Code != sql.CodeDivisionByZero {
		t.Fatalf("division by zero: %+v", serr)
	}
	// Arithmetic in WHERE and in UPDATE SET.
	eq(ids(`SELECT id FROM p WHERE qty * 2 > 10 ORDER BY id`), 2, 4)
	execSQL(t, ctx, s, `UPDATE p SET qty = qty * 10 WHERE id = 3`)
	if res = execSQL(t, ctx, s, `SELECT qty FROM p WHERE id = 3`); res.Rows[0][0].I != 20 {
		t.Fatalf("update arith: %+v", res.Rows[0][0])
	}

	// Functions.
	res = execSQL(t, ctx, s, `SELECT lower(name), upper(name), length(name) FROM p WHERE id = 5`)
	if res.Rows[0][0].S != "stove" || res.Rows[0][1].S != "STOVE" || res.Rows[0][2].I != 5 {
		t.Fatalf("string funcs: %+v", res.Rows[0])
	}
	res = execSQL(t, ctx, s, `SELECT coalesce(cat, 'uncategorized'), abs(0 - qty) FROM p WHERE id = 4`)
	if res.Rows[0][0].S != "uncategorized" || res.Rows[0][1].I != 7 {
		t.Fatalf("coalesce/abs: %+v", res.Rows[0])
	}
	eq(ids(`SELECT id FROM p WHERE lower(name) = 'stove'`), 5)

	// now(): a timestamp bracketed by the test's own clock reads.
	before := time.Now().Add(-time.Minute).UnixNano()
	res = execSQL(t, ctx, s, `SELECT now()`)
	if d := res.Rows[0][0]; d.Fam != types.Timestamp || d.I < before || d.I > time.Now().Add(time.Minute).UnixNano() {
		t.Fatalf("now(): %+v", d)
	}
	execSQL(t, ctx, s, `CREATE TABLE ev (id INT8 PRIMARY KEY, at TIMESTAMPTZ)`)
	execSQL(t, ctx, s, `INSERT INTO ev VALUES (1, now())`)
	if res = execSQL(t, ctx, s, `SELECT COUNT(*) FROM ev WHERE at <= now()`); res.Rows[0][0].I != 1 {
		t.Fatalf("now in where: %+v", res.Rows)
	}
}
