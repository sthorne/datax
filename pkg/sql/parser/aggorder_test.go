package parser

import (
	"strings"
	"testing"
)

// TestAggregateOrderBy covers an ORDER BY among an aggregate call's
// arguments (issue #356) beside WITHIN GROUP: the terms, their
// directions and NULLS placement, a number kept as a constant, and the
// combinations PostgreSQL's grammar refuses.
func TestAggregateOrderBy(t *testing.T) {
	sel := parseOne(t, `SELECT string_agg(DISTINCT b, ',' ORDER BY b DESC NULLS LAST, 1, x + 1 NULLS FIRST) FILTER (WHERE x > 0) FROM t`).(*Select)
	se := sel.Exprs[0]
	if se.Agg != "STRING_AGG" || !se.AggDistinct || se.AggWithinGroup || len(se.AggArgs) != 1 || len(se.AggFilter) != 1 {
		t.Fatalf("call: %+v", se)
	}
	if len(se.AggOrder) != 3 {
		t.Fatalf("order: %+v", se.AggOrder)
	}
	o := se.AggOrder
	if o[0].Expr == nil || o[0].Expr.Column != "b" || !o[0].Desc || o[0].Nulls != "last" ||
		o[1].Expr == nil || o[1].Expr.Lit == nil || o[1].Position != 0 || o[1].Desc ||
		o[2].Expr == nil || o[2].Expr.BinOp != "+" || o[2].Nulls != "first" {
		t.Fatalf("order terms: %+v %+v %+v", o[0], o[1], o[2])
	}
	if got := FormatExpr(Expr{Agg: &se}); !strings.Contains(got, "ORDER BY b DESC NULLS LAST, 1, ") {
		t.Fatalf("format: %s", got)
	}

	sel = parseOne(t, `SELECT mode() WITHIN GROUP (ORDER BY y DESC), percentile_cont(0.5) WITHIN GROUP (ORDER BY x) FROM t`).(*Select)
	if m := sel.Exprs[0]; m.Agg != "MODE" || !m.AggWithinGroup || len(m.AggOrder) != 1 || m.AggOrder[0].Column != "y" || !m.AggOrder[0].Desc {
		t.Fatalf("mode: %+v", m)
	}
	if p := sel.Exprs[1]; !p.AggWithinGroup || p.AggArg == nil || p.AggOrder[0].Column != "x" {
		t.Fatalf("percentile: %+v", p)
	}

	for _, bad := range []string{
		`SELECT string_agg(b, ',' ORDER BY b) WITHIN GROUP (ORDER BY b) FROM t`,
		`SELECT percentile_cont(DISTINCT 0.5) WITHIN GROUP (ORDER BY x) FROM t`,
		`SELECT count(* ORDER BY x) FROM t`,
		`SELECT array_agg(x ORDER BY y NULLS) FROM t`,
	} {
		if _, err := Parse(bad); err == nil {
			t.Errorf("%s: parsed", bad)
		}
	}
}
