package parser

// CountParams returns the highest parameter ordinal ($N) referenced by the
// statement — the number of parameter values a Bind must supply.
func CountParams(stmt Statement) int {
	max := 0
	visit := func(e Expr) {
		for {
			if e.Param > max {
				max = e.Param
			}
			if e.Right == nil {
				return
			}
			e = *e.Right
		}
	}
	switch t := stmt.(type) {
	case *Insert:
		for _, row := range t.Rows {
			for _, e := range row {
				visit(e)
			}
		}
	case *Select:
		for _, se := range t.Exprs {
			if !se.Star {
				visit(se.Expr)
			}
		}
		for _, c := range t.Where {
			visit(c.Value)
		}
	case *Update:
		for _, set := range t.Set {
			visit(set.Value)
		}
		for _, c := range t.Where {
			visit(c.Value)
		}
	case *Delete:
		for _, c := range t.Where {
			visit(c.Value)
		}
	}
	return max
}
