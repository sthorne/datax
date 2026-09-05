package parser

import "testing"

// TestParseWindows: OVER on aggregates and window-only functions, with
// PARTITION BY, ORDER BY (aggregates included), ROWS / RANGE frames,
// named windows (WINDOW clause, extended inline), window calls inside
// expressions and predicates, derived tables as join members, and the
// refused forms.
func TestParseWindows(t *testing.T) {
	sel := parseOne(t, `SELECT id, row_number() OVER (PARTITION BY region ORDER BY amount DESC NULLS LAST) AS rn,
		sum(amount) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING),
		lag(amount, 2, 0) OVER w, count(*) OVER (),
		rank() OVER (ORDER BY sum(amount) DESC),
		first_value(amount) OVER (w RANGE UNBOUNDED PRECEDING)
		FROM sales WINDOW w AS (PARTITION BY region ORDER BY id) ORDER BY rn`).(*Select)
	if len(sel.Exprs) != 7 || len(sel.Windows) != 1 || sel.Windows[0].Name != "w" || len(sel.Windows[0].Spec.PartitionBy) != 1 {
		t.Fatalf("select: %+v", sel)
	}
	rn := sel.Exprs[1]
	if rn.Agg != "ROW_NUMBER" || rn.Window == nil || len(rn.Window.PartitionBy) != 1 || len(rn.Window.OrderBy) != 1 || !rn.Window.OrderBy[0].Desc || rn.Window.OrderBy[0].Nulls != "last" || rn.Alias != "rn" {
		t.Fatalf("row_number: %+v", rn)
	}
	sum := sel.Exprs[2]
	if sum.Agg != "SUM" || sum.AggCol != "amount" || sum.Window.Frame == nil || sum.Window.Frame.Mode != "ROWS" ||
		sum.Window.Frame.Start != (FrameBound{Kind: "preceding", Offset: 1}) || sum.Window.Frame.End != (FrameBound{Kind: "following", Offset: 1}) {
		t.Fatalf("sum frame: %+v", sum.Window.Frame)
	}
	lag := sel.Exprs[3]
	if lag.Agg != "LAG" || lag.AggCol != "amount" || len(lag.AggArgs) != 2 || lag.Window.Name != "w" {
		t.Fatalf("lag: %+v", lag)
	}
	if cnt := sel.Exprs[4]; cnt.Agg != "COUNT" || !cnt.AggStar || cnt.Window == nil || len(cnt.Window.OrderBy) != 0 {
		t.Fatalf("count(*) over (): %+v", cnt)
	}
	if rk := sel.Exprs[5]; rk.Window == nil || len(rk.Window.OrderBy) != 1 || rk.Window.OrderBy[0].Agg == nil || rk.Window.OrderBy[0].Agg.Agg != "SUM" {
		t.Fatalf("rank over aggregate: %+v", rk)
	}
	if fv := sel.Exprs[6]; fv.Window == nil || fv.Window.Name != "w" || fv.Window.Frame == nil || fv.Window.Frame.Mode != "RANGE" || fv.Window.Frame.Start.Kind != "unbounded preceding" || fv.Window.Frame.End.Kind != "current row" {
		t.Fatalf("extended named window: %+v", fv.Window)
	}

	// Inside expressions and predicates.
	sel = parseOne(t, `SELECT amount - lag(amount) OVER (ORDER BY id) AS delta, count(*) OVER (ORDER BY id) > 3 AS late,
		CASE WHEN row_number() OVER (ORDER BY id) = 1 THEN 'first' ELSE 'rest' END FROM sales`).(*Select)
	if d := sel.Exprs[0]; d.Window != nil || d.Expr.BinOp != "-" || d.Expr.Right == nil || d.Expr.Right.Window == nil || d.Expr.Right.Window.Agg != "LAG" {
		t.Fatalf("window inside arithmetic: %+v", d.Expr)
	}
	if l := sel.Exprs[1]; l.Expr.Cmp == nil || l.Expr.Cmp.Expr == nil || l.Expr.Cmp.Expr.Window == nil {
		t.Fatalf("window inside predicate: %+v", l.Expr)
	}
	if c := sel.Exprs[2]; c.Expr.Case == nil || len(c.Expr.Case.Whens[0].Cond) != 1 || c.Expr.Case.Whens[0].Cond[0].Expr == nil || c.Expr.Case.Whens[0].Cond[0].Expr.Window == nil {
		t.Fatalf("window inside CASE: %+v", c.Expr)
	}

	// A derived table as a join member.
	sel = parseOne(t, `SELECT s.id, r.n FROM sales s JOIN (SELECT region, count(*) AS n FROM sales GROUP BY region) AS r ON r.region = s.region`).(*Select)
	if len(sel.Joins) != 1 || sel.Joins[0].Derived == nil || sel.Joins[0].Alias != "r" || sel.Joins[0].Table != "r" || len(sel.Joins[0].On) != 1 {
		t.Fatalf("derived join member: %+v", sel.Joins)
	}

	for _, bad := range []string{
		`SELECT rank() FROM sales`,
		`SELECT sum() OVER () FROM sales`,
		`SELECT sum(x) OVER (ROWS BETWEEN 1 PRECEDING) FROM sales`,
		`SELECT sum(x) OVER (ORDER BY x NULLS MIDDLE) FROM sales`,
		`SELECT sum(x) OVER (ROWS UNBOUNDED) FROM sales`,
		`SELECT s.id FROM sales s JOIN (SELECT 1) ON true`,
	} {
		if _, err := Parse(bad); err == nil {
			t.Errorf("%s: parsed", bad)
		}
	}
}
