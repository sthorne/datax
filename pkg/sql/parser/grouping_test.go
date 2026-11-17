package parser

import "testing"

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
