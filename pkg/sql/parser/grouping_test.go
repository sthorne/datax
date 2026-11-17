package parser

import (
	"fmt"
	"strings"
	"testing"
)

func parseOneSelect(t *testing.T, sql string) *Select {
	t.Helper()
	stmts, err := Parse(sql)
	if err != nil {
		t.Fatalf("%s: %v", sql, err)
	}
	sel, ok := stmts[0].(*Select)
	if !ok || len(stmts) != 1 {
		t.Fatalf("%s: parsed as %T", sql, stmts[0])
	}
	return sel
}

// TestAggregateExpressionsParse covers issue #335: aggregate calls are
// operands of expressions in the select list, HAVING and ORDER BY.
func TestAggregateExpressionsParse(t *testing.T) {
	sel := parseOneSelect(t, `SELECT SUM(a) / COUNT(*), count(*) AS n, ROUND(AVG(x), 2),
		SUM(DISTINCT a) FILTER (WHERE a > 1) + 1, count(*)::int8 FROM t`)
	e := sel.Exprs[0]
	if e.Agg != "" || e.Expr.Agg == nil || e.Expr.Agg.Agg != "SUM" || e.Expr.BinOp != "/" || e.Expr.Right == nil ||
		e.Expr.Right.Agg == nil || !e.Expr.Right.Agg.AggStar {
		t.Fatalf("SUM(a) / COUNT(*): %+v", e)
	}
	if got := FormatExpr(e.Expr); got != "sum(a) / count(*)" {
		t.Fatalf("format: %q", got)
	}
	if n := sel.Exprs[1]; n.Agg != "COUNT" || !n.AggStar || n.Alias != "n" {
		t.Fatalf("a lone aggregate keeps the item form: %+v", n)
	}
	if r := sel.Exprs[2].Expr; r.Func != "round" || len(r.Args) != 2 || r.Args[0].Agg == nil || r.Args[0].Agg.AggCol != "x" {
		t.Fatalf("ROUND(AVG(x), 2): %+v", r)
	}
	if d := sel.Exprs[3].Expr; d.Agg == nil || !d.Agg.AggDistinct || len(d.Agg.AggFilter) != 1 || d.BinOp != "+" {
		t.Fatalf("DISTINCT and FILTER inside an expression: %+v", d)
	}
	if got := OutputName(sel.Exprs[4]); got != "count" {
		t.Fatalf("output name of count(*)::int8: %q", got)
	}

	sel = parseOneSelect(t, `SELECT g FROM t GROUP BY g HAVING (COUNT(*) > 1 OR SUM(a) / COUNT(a) < 2) AND bool_or(b)
		ORDER BY SUM(a) * 2 DESC, count(*), g`)
	if len(sel.Having) != 2 || sel.Having[0].Op != "OR" || len(sel.Having[0].Or) != 2 {
		t.Fatalf("HAVING: %+v", sel.Having)
	}
	if b := sel.Having[1]; b.Expr == nil || b.Expr.Agg == nil || b.Expr.Agg.Agg != "BOOL_OR" {
		t.Fatalf("bare boolean aggregate in HAVING: %+v", b)
	}
	ob := sel.OrderBy
	if len(ob) != 3 || ob[0].Expr == nil || ob[0].Expr.Agg == nil || !ob[0].Desc || ob[1].Agg == nil || ob[2].Column != "g" {
		t.Fatalf("ORDER BY: %+v", ob)
	}

	// An aggregate in WHERE parses; the executor refuses it (42803).
	parseOneSelect(t, `SELECT a FROM t WHERE COUNT(*) > 1`)
}

// TestGroupByParse covers issue #336: GROUP BY expressions and
// positions of expression items, and ROLLUP, CUBE and GROUPING SETS
// expanded to grouping sets over the distinct keys.
func TestGroupByParse(t *testing.T) {
	keys := func(sel *Select) string {
		var out []string
		for _, k := range sel.GroupBy {
			out = append(out, FormatExpr(k))
		}
		return strings.Join(out, ", ")
	}
	sets := func(sel *Select) string { return fmt.Sprint(sel.GroupingSets) }
	for _, c := range []struct{ sql, keys, sets string }{
		{`SELECT x, lower(y) FROM t GROUP BY a + 1, 2, a + 1`, "a + 1, lower(y)", "[]"},
		{`SELECT 1 FROM t GROUP BY (a * 2), date_trunc('day', ts)`, "a * 2, date_trunc('day', ts)", "[]"},
		{`SELECT 1 FROM t GROUP BY (a, b), rollup`, "a, b, rollup", "[]"},
		{`SELECT 1 FROM t GROUP BY ROLLUP (a, (b, c))`, "a, b, c", "[[0 1 2] [0] []]"},
		{`SELECT 1 FROM t GROUP BY CUBE (a, b % 2)`, "a, b % 2", "[[0 1] [0] [1] []]"},
		{`SELECT 1 FROM t GROUP BY GROUPING SETS ((a, b), a, (), ROLLUP (c))`, "a, b, c", "[[0 1] [0] [] [2] []]"},
		{`SELECT 1 FROM t GROUP BY a, ROLLUP (b, a)`, "a, b", "[[0 1] [0 1] [0]]"},
		{`SELECT 1 FROM t GROUP BY ()`, "", "[[]]"},
		{`SELECT a, b FROM t GROUP BY CUBE (1, 2)`, "a, b", "[[0 1] [0] [1] []]"},
	} {
		sel := parseOneSelect(t, c.sql)
		if got := keys(sel); got != c.keys {
			t.Errorf("%s: keys %q, want %q", c.sql, got, c.keys)
		}
		if got := sets(sel); got != c.sets {
			t.Errorf("%s: sets %s, want %s", c.sql, got, c.sets)
		}
	}
	if sel := parseOneSelect(t, `SELECT 1 FROM t GROUP BY (a + b) * 2`); len(sel.GroupBy) != 1 || sel.GroupBy[0].BinOp != "*" || sel.GroupBy[0].Left == nil {
		t.Errorf("parenthesized grouping expression: %+v", sel.GroupBy)
	}
	for _, q := range []string{
		`SELECT 1 FROM t GROUP BY CUBE (a, b, c, d, e, f, g, h, i, j, k, l, m)`,
		`SELECT a FROM t GROUP BY 2`,
		`SELECT a FROM t GROUP BY ROLLUP (a`,
	} {
		if _, err := Parse(q); err == nil {
			t.Errorf("%s parsed", q)
		}
	}
}

// TestDistinctOnParse covers issue #337: DISTINCT ON (exprs), its
// positions and output names resolved to the select items' expressions.
func TestDistinctOnParse(t *testing.T) {
	sel := parseOneSelect(t, `SELECT DISTINCT ON (a, 2, p, lower(c), t.d) a, b % 2, c AS p FROM t ORDER BY a`)
	if !sel.Distinct || len(sel.DistinctOn) != 5 {
		t.Fatalf("DISTINCT ON: %+v", sel)
	}
	var got []string
	for _, e := range sel.DistinctOn {
		got = append(got, FormatExpr(e))
	}
	if s := strings.Join(got, ", "); s != "a, b % 2, c, lower(c), t.d" {
		t.Fatalf("DISTINCT ON expressions: %s", s)
	}
	if sel := parseOneSelect(t, `SELECT DISTINCT a FROM t`); !sel.Distinct || sel.DistinctOn != nil {
		t.Fatalf("plain DISTINCT: %+v", sel)
	}
	for _, q := range []string{
		`SELECT DISTINCT ON (3) a, b FROM t`,
		`SELECT DISTINCT ON a, b FROM t`,
		`SELECT DISTINCT ON () a FROM t`,
	} {
		if _, err := Parse(q); err == nil {
			t.Errorf("%s parsed", q)
		}
	}
}
