package parser

import "testing"

// TestParseWith: WITH [RECURSIVE] members with column lists attach to
// the statement they scope over (SELECT, INSERT, UPDATE, DELETE), a
// member may be a data-modifying statement, WITH works inside a
// subquery and as a set-operation member, INSERT takes a query source,
// and a derived table may be joined.
func TestParseWith(t *testing.T) {
	sel := parseOne(t, `WITH RECURSIVE a (x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM a WHERE x < 3), b AS (DELETE FROM t RETURNING id) SELECT x FROM a JOIN b ON b.id = a.x`).(*Select)
	if len(sel.With) != 2 || sel.With[0].Name != "a" || !sel.With[0].Recursive || sel.With[0].Columns[0] != "x" || sel.With[1].Name != "b" {
		t.Fatalf("with: %+v", sel.With)
	}
	if q, ok := sel.With[0].Query.(*Select); !ok || q.Union == nil || !q.UnionAll {
		t.Fatalf("recursive member: %+v", sel.With[0].Query)
	}
	if _, ok := sel.With[1].Query.(*Delete); !ok {
		t.Fatalf("data-modifying member: %T", sel.With[1].Query)
	}
	if sel.Table != "a" || len(sel.Joins) != 1 {
		t.Fatalf("statement: %+v", sel)
	}
	ins := parseOne(t, `WITH s AS (SELECT 1 AS v) INSERT INTO t (a) SELECT v FROM s ON CONFLICT DO NOTHING RETURNING a`).(*Insert)
	if len(ins.With) != 1 || ins.Select == nil || ins.Select.Table != "s" || len(ins.Rows) != 0 || ins.OnConflict == nil || len(ins.Returning) != 1 {
		t.Fatalf("insert select: %+v", ins)
	}
	if ins := parseOne(t, `INSERT INTO t SELECT * FROM u WHERE x > 1`).(*Insert); ins.Select == nil || len(ins.Select.Where) != 1 {
		t.Fatalf("insert select where: %+v", ins)
	}
	if ins := parseOne(t, `INSERT INTO t (a) (SELECT 1 UNION SELECT 2)`).(*Insert); ins.Select == nil || ins.Select.Derived == nil {
		t.Fatalf("insert parenthesized query: %+v", ins)
	}
	upd := parseOne(t, `WITH s AS (SELECT id FROM t) UPDATE t SET a = 1 WHERE id IN (SELECT id FROM s)`).(*Update)
	if len(upd.With) != 1 {
		t.Fatalf("update with: %+v", upd)
	}
	del := parseOne(t, `WITH s AS (SELECT 1) DELETE FROM t WHERE a = 1`).(*Delete)
	if len(del.With) != 1 {
		t.Fatalf("delete with: %+v", del)
	}
	sel = parseOne(t, `SELECT id FROM t WHERE id IN (WITH q AS (SELECT 1 AS id) SELECT id FROM q) AND (WITH z AS (SELECT 2 AS v) SELECT v FROM z) = 2`).(*Select)
	if len(sel.Where) != 2 || sel.Where[0].Sub == nil || len(sel.Where[0].Sub.With) != 1 {
		t.Fatalf("with in IN: %+v", sel.Where)
	}
	sel = parseOne(t, `WITH a AS (SELECT 1 AS v) SELECT v FROM a UNION (WITH b AS (SELECT 2 AS v) SELECT v FROM b) ORDER BY 1`).(*Select)
	if len(sel.With) != 1 || sel.Union == nil || sel.Union.Derived == nil || len(sel.Union.Derived.With) != 1 {
		t.Fatalf("with as member: %+v", sel)
	}
	sel = parseOne(t, `SELECT d.n, t.x FROM (SELECT 1 AS n) AS d LEFT JOIN t ON t.n = d.n WHERE t.x > 0 ORDER BY 1`).(*Select)
	if sel.Derived == nil || sel.Alias != "d" || len(sel.Joins) != 1 || !sel.Joins[0].Left || len(sel.Where) != 1 {
		t.Fatalf("joined derived table: %+v", sel)
	}
	for _, bad := range []string{
		`WITH a AS (SELECT 1) SHOW TABLES`,
		`WITH a AS (CREATE TABLE x (id INT PRIMARY KEY)) SELECT 1`,
		`WITH a AS SELECT 1 SELECT 1`,
		`SELECT (WITH a AS (SELECT 1) DELETE FROM t) FROM t`,
	} {
		if _, err := Parse(bad); err == nil {
			t.Errorf("%s: parsed", bad)
		}
	}
}
