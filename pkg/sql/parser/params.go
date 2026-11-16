package parser

// CountParams returns the highest parameter ordinal ($N) referenced by the
// statement — the number of parameter values a Bind must supply. Every
// position counts, subqueries and the statement under EXPLAIN included
// (see Walker).
func CountParams(stmt Statement) int {
	max := 0
	note := func(n int) {
		if n > max {
			max = n
		}
	}
	w := Walker{
		OnExpr: func(e *Expr) bool {
			note(e.Param)
			return true
		},
		OnParam: note,
	}
	w.Statement(stmt)
	return max
}
