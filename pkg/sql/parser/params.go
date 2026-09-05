package parser

// CountParams returns the highest parameter ordinal ($N) referenced by the
// statement — the number of parameter values a Bind must supply.
func CountParams(stmt Statement) int {
	max := 0
	var visit func(e Expr)
	visit = func(e Expr) {
		if e.Param > max {
			max = e.Param
		}
		if e.Left != nil {
			visit(*e.Left)
		}
		if e.Right != nil {
			visit(*e.Right)
		}
		for _, a := range e.Args {
			visit(a)
		}
	}
	var visitWhere func(where []Comparison)
	visitWhere = func(where []Comparison) {
		for _, c := range where {
			if c.Expr != nil {
				visit(*c.Expr)
			}
			visit(c.Value)
			for _, v := range c.Values {
				visit(v)
			}
			for _, d := range c.Or {
				visitWhere(d)
			}
		}
	}
	visitReturning := func(exprs []SelectExpr) {
		for _, se := range exprs {
			if !se.Star {
				visit(se.Expr)
			}
		}
	}
	withMax := func(ctes []CTE) {
		for _, c := range ctes {
			if n := CountParams(c.Query); n > max {
				max = n
			}
		}
	}
	switch t := stmt.(type) {
	case *Insert:
		withMax(t.With)
		if t.Select != nil {
			if n := CountParams(t.Select); n > max {
				max = n
			}
		}
		for _, row := range t.Rows {
			for _, e := range row {
				visit(e)
			}
		}
		if oc := t.OnConflict; oc != nil {
			for _, set := range oc.Set {
				visit(set.Value)
			}
			visitWhere(oc.Where)
		}
		visitReturning(t.Returning)
	case *Select:
		withMax(t.With)
		for m := t; m != nil; m = m.Union {
			if m.Derived != nil {
				if n := CountParams(m.Derived); n > max {
					max = n
				}
			}
			for _, se := range m.Exprs {
				if !se.Star {
					visit(se.Expr)
				}
			}
			visitWhere(m.Where)
			if m.LimitParam > max {
				max = m.LimitParam
			}
			if m.OffsetParam > max {
				max = m.OffsetParam
			}
		}
	case *Update:
		withMax(t.With)
		for _, set := range t.Set {
			visit(set.Value)
		}
		visitWhere(t.Where)
		visitReturning(t.Returning)
	case *Delete:
		withMax(t.With)
		visitWhere(t.Where)
		visitReturning(t.Returning)
	}
	return max
}
