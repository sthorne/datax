package parser

import (
	"strings"
	"testing"
)

// TestParseQueryShapes: OFFSET / FETCH / LIMIT ALL, NULLS FIRST | LAST,
// set operations as a flat member list with ORDER BY / LIMIT / OFFSET
// carried by the head, parenthesized members and VALUES, aggregates in
// ORDER BY, and the RIGHT / FULL / NATURAL / USING join forms.
func TestParseQueryShapes(t *testing.T) {
	sel := parseOne(t, `SELECT a FROM t ORDER BY a DESC NULLS LAST, b NULLS FIRST LIMIT 5 OFFSET 10`).(*Select)
	if len(sel.OrderBy) != 2 || sel.OrderBy[0].Nulls != "last" || !sel.OrderBy[0].Desc || sel.OrderBy[1].Nulls != "first" {
		t.Fatalf("nulls: %+v", sel.OrderBy)
	}
	if sel.Limit != 5 || sel.Offset != 10 {
		t.Fatalf("limit/offset: %d/%d", sel.Limit, sel.Offset)
	}
	sel = parseOne(t, `SELECT a FROM t OFFSET 2 ROWS FETCH FIRST 3 ROWS ONLY`).(*Select)
	if sel.Limit != 3 || sel.Offset != 2 {
		t.Fatalf("fetch first: %d/%d", sel.Limit, sel.Offset)
	}
	sel = parseOne(t, `SELECT a FROM t FETCH NEXT ROW ONLY`).(*Select)
	if sel.Limit != 1 {
		t.Fatalf("fetch next row: %d", sel.Limit)
	}
	sel = parseOne(t, `SELECT a FROM t LIMIT ALL OFFSET 1`).(*Select)
	if sel.Limit != -1 || sel.Offset != 1 {
		t.Fatalf("limit all: %d/%d", sel.Limit, sel.Offset)
	}
	if sel := parseOne(t, `SELECT a FROM t LIMIT 0`).(*Select); sel.Limit != 0 {
		t.Fatalf("limit 0: %d", sel.Limit)
	}

	// A flat member list; the operator sits on the link to the next
	// member; ORDER BY / LIMIT / OFFSET after the last member move to
	// the head, and a position is rewritten to the last member's name.
	sel = parseOne(t, `SELECT a FROM t UNION SELECT b FROM u INTERSECT ALL SELECT c FROM v EXCEPT SELECT d FROM w ORDER BY 1 LIMIT 3 OFFSET 1`).(*Select)
	var ops []string
	for m := sel; m != nil; m = m.Union {
		ops = append(ops, m.SetOp)
		if m != sel && (len(m.OrderBy) > 0 || m.Limit >= 0 || m.Offset > 0) {
			t.Fatalf("member carries ordering: %+v", m)
		}
	}
	if strings.Join(ops, ",") != "UNION,INTERSECT,EXCEPT," || !sel.Union.UnionAll || sel.UnionAll {
		t.Fatalf("set ops: %v all=%v/%v", ops, sel.UnionAll, sel.Union.UnionAll)
	}
	if len(sel.OrderBy) != 1 || sel.OrderBy[0].Column != "d" || sel.Limit != 3 || sel.Offset != 1 {
		t.Fatalf("head ordering: %+v %d %d", sel.OrderBy, sel.Limit, sel.Offset)
	}
	// Parenthesized members keep their own ORDER BY / LIMIT as derived
	// tables; a position after the parentheses is kept as a position.
	sel = parseOne(t, `(SELECT a FROM t ORDER BY a LIMIT 1) UNION ALL (SELECT b FROM u) ORDER BY 1 DESC`).(*Select)
	if sel.Derived == nil || sel.Derived.Limit != 1 || len(sel.Derived.OrderBy) != 1 || !sel.UnionAll || sel.Union == nil || sel.Union.Derived == nil {
		t.Fatalf("parenthesized members: %+v", sel)
	}
	if len(sel.OrderBy) != 1 || sel.OrderBy[0].Position != 1 || !sel.OrderBy[0].Desc {
		t.Fatalf("position kept: %+v", sel.OrderBy)
	}
	sel = parseOne(t, `VALUES (1), (2) UNION SELECT 3 ORDER BY 1 DESC`).(*Select)
	// The rows chain UNION ALL; the link from the last row to SELECT 3 is
	// the written UNION; the position names the last member's output.
	if sel.Union == nil || !sel.UnionAll || sel.Union.SetOp != "UNION" || sel.Union.UnionAll || sel.Union.Union == nil || sel.OrderBy[0].Column != "?column?" {
		t.Fatalf("values head: %+v", sel)
	}
	sel = parseOne(t, `SELECT 1 EXCEPT ALL SELECT 2`).(*Select)
	if sel.SetOp != "EXCEPT" || !sel.UnionAll {
		t.Fatalf("except all: %+v", sel)
	}

	sel = parseOne(t, `SELECT g, count(*) AS n FROM t GROUP BY g ORDER BY count(*) DESC, max(x), 2`).(*Select)
	if len(sel.OrderBy) != 3 || sel.OrderBy[0].Agg == nil || sel.OrderBy[0].Agg.Agg != "COUNT" || !sel.OrderBy[0].Desc ||
		sel.OrderBy[1].Agg == nil || sel.OrderBy[1].Agg.AggCol != "x" || sel.OrderBy[2].Column != "n" {
		t.Fatalf("aggregates in ORDER BY: %+v", sel.OrderBy)
	}

	sel = parseOne(t, `SELECT * FROM a RIGHT OUTER JOIN b ON a.id = b.aid FULL JOIN c USING (id, k) NATURAL LEFT JOIN d`).(*Select)
	if len(sel.Joins) != 3 || !sel.Joins[0].Right || sel.Joins[0].Full || len(sel.Joins[0].On) != 1 {
		t.Fatalf("right join: %+v", sel.Joins)
	}
	if !sel.Joins[1].Full || strings.Join(sel.Joins[1].Using, ",") != "id,k" {
		t.Fatalf("full join using: %+v", sel.Joins[1])
	}
	if !sel.Joins[2].Natural || !sel.Joins[2].Left || sel.Joins[2].Table != "d" {
		t.Fatalf("natural left join: %+v", sel.Joins[2])
	}

	for _, bad := range []string{
		`SELECT a FROM t LIMIT 1 LIMIT 2`,
		`SELECT a FROM t OFFSET 1 OFFSET 2`,
		`SELECT a FROM t ORDER BY a NULLS MIDDLE`,
		`SELECT a FROM t FETCH FIRST 2 ROWS`,
		`SELECT a FROM t ORDER BY a UNION SELECT b FROM u`,
		`SELECT a FROM t FOR UPDATE INTERSECT SELECT b FROM u`,
		`SELECT * FROM a NATURAL CROSS JOIN b`,
		`SELECT a FROM t OFFSET -1`,
	} {
		if _, err := Parse(bad); err == nil {
			t.Errorf("%s: parsed", bad)
		}
	}
}

// TestParseExplainAnalyze: EXPLAIN ANALYZE precedes a query; a bare
// EXPLAIN ANALYZE t still explains the ANALYZE statement.
func TestParseExplainAnalyze(t *testing.T) {
	ex := parseOne(t, `EXPLAIN ANALYZE SELECT 1 FROM t`).(*Explain)
	if !ex.Analyze {
		t.Fatalf("analyze flag: %+v", ex)
	}
	if _, ok := ex.Stmt.(*Select); !ok {
		t.Fatalf("inner: %T", ex.Stmt)
	}
	ex = parseOne(t, `EXPLAIN ANALYZE WITH q AS (SELECT 1) SELECT * FROM q`).(*Explain)
	if !ex.Analyze || len(ex.Stmt.(*Select).With) != 1 {
		t.Fatalf("analyze with: %+v", ex)
	}
	if ex := parseOne(t, `EXPLAIN SELECT 1 FROM t`).(*Explain); ex.Analyze {
		t.Fatalf("plain explain: %+v", ex)
	}
	if ex := parseOne(t, `EXPLAIN ANALYZE t`).(*Explain); ex.Analyze {
		t.Fatalf("EXPLAIN of ANALYZE: %+v", ex)
	}
}
