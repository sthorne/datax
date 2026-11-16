package parser

// Walker visits every expression position of a statement: select lists,
// WHERE / ON / HAVING conjuncts, ORDER BY and window terms, aggregate
// arguments and filters, LIMIT / OFFSET, VALUES rows, SET and ON
// CONFLICT clauses, RETURNING lists, and — recursively — WITH members,
// derived and joined subqueries, set-operation members, scalar, IN and
// EXISTS subqueries, and the statement under EXPLAIN. Every pass that
// must see all of a statement's parameters or calls (counting $n,
// inferring their types) goes through it, so a new position is added
// once, here.
//
// Each callback is optional. OnSelect, OnExpr and OnCond are called before
// the node's children; returning false skips them. A nested select
// (subquery, member, derived table) is reported to OnSelect like the
// statement's own.
type Walker struct {
	OnSelect func(s *Select) bool
	OnExpr   func(e *Expr) bool
	OnCond   func(c *Comparison) bool
	// OnSelectExpr and OnOrderCol see each select item, aggregate call
	// and ORDER BY term (the names they carry outside an expression:
	// AggCol, Column) before their expressions are walked.
	OnSelectExpr func(se *SelectExpr)
	OnOrderCol   func(oc *OrderCol)
	// OnParam receives the parameters that stand outside an expression:
	// LIMIT $n and OFFSET $n.
	OnParam func(n int)
}

// Statement walks stmt.
func (w *Walker) Statement(stmt Statement) {
	switch t := stmt.(type) {
	case *Select:
		w.SelectStmt(t)
	case *Insert:
		w.ctes(t.With)
		for _, row := range t.Rows {
			for i := range row {
				w.WalkExpr(&row[i])
			}
		}
		if t.Select != nil {
			w.SelectStmt(t.Select)
		}
		if oc := t.OnConflict; oc != nil {
			for i := range oc.Set {
				w.WalkExpr(&oc.Set[i].Value)
			}
			w.Conds(oc.Where)
		}
		w.selectExprs(t.Returning)
	case *Update:
		w.ctes(t.With)
		for i := range t.Set {
			w.WalkExpr(&t.Set[i].Value)
		}
		w.Conds(t.Where)
		w.selectExprs(t.Returning)
	case *Delete:
		w.ctes(t.With)
		w.Conds(t.Where)
		w.selectExprs(t.Returning)
	case *Explain:
		w.Statement(t.Stmt)
	case *CreateTable:
		if t.As != nil {
			w.SelectStmt(t.As)
		}
	case *CreateView:
		if t.Query != nil {
			w.SelectStmt(t.Query)
		}
	case *AlterTable:
		for _, tuple := range t.SplitAt {
			for i := range tuple {
				w.WalkExpr(&tuple[i])
			}
		}
		if t.SplitExpiration != nil {
			w.WalkExpr(t.SplitExpiration)
		}
		for _, tuple := range t.UnsplitAt {
			for i := range tuple {
				w.WalkExpr(&tuple[i])
			}
		}
	}
}

func (w *Walker) ctes(with []CTE) {
	for _, c := range with {
		w.Statement(c.Query)
	}
}

// SelectStmt walks a select and every set-operation member chained to it.
func (w *Walker) SelectStmt(s *Select) {
	for m := s; m != nil; m = m.Union {
		w.selectMember(m)
	}
}

func (w *Walker) selectMember(m *Select) {
	if w.OnSelect != nil && !w.OnSelect(m) {
		return
	}
	w.ctes(m.With)
	if m.Derived != nil {
		w.SelectStmt(m.Derived)
	}
	if m.FuncTable != nil {
		w.WalkExpr(m.FuncTable)
	}
	for i := range m.Joins {
		j := &m.Joins[i]
		if j.Derived != nil {
			w.SelectStmt(j.Derived)
		}
		if j.FuncTable != nil {
			w.WalkExpr(j.FuncTable)
		}
		w.Conds(j.Filter)
	}
	w.selectExprs(m.Exprs)
	w.Conds(m.Where)
	for i := range m.Having {
		h := &m.Having[i]
		if h.Agg != nil {
			w.selectExpr(h.Agg)
		}
		w.WalkExpr(&h.Value)
	}
	w.orderCols(m.OrderBy)
	for i := range m.Windows {
		w.windowSpec(&m.Windows[i].Spec)
	}
	if w.OnParam != nil {
		if m.LimitParam > 0 {
			w.OnParam(m.LimitParam)
		}
		if m.OffsetParam > 0 {
			w.OnParam(m.OffsetParam)
		}
	}
}

func (w *Walker) selectExprs(exprs []SelectExpr) {
	for i := range exprs {
		if !exprs[i].Star {
			w.selectExpr(&exprs[i])
		}
	}
}

func (w *Walker) selectExpr(se *SelectExpr) {
	if w.OnSelectExpr != nil {
		w.OnSelectExpr(se)
	}
	w.WalkExpr(&se.Expr)
	if se.AggArg != nil {
		w.WalkExpr(se.AggArg)
	}
	for i := range se.AggArgs {
		w.WalkExpr(&se.AggArgs[i])
	}
	w.Conds(se.AggFilter)
	w.orderCols(se.AggOrder)
	if se.Window != nil {
		w.windowSpec(se.Window)
	}
}

func (w *Walker) windowSpec(ws *WindowSpec) {
	for i := range ws.PartitionBy {
		w.WalkExpr(&ws.PartitionBy[i])
	}
	w.orderCols(ws.OrderBy)
}

func (w *Walker) orderCols(order []OrderCol) {
	for i := range order {
		if w.OnOrderCol != nil {
			w.OnOrderCol(&order[i])
		}
		if order[i].Expr != nil {
			w.WalkExpr(order[i].Expr)
		}
		if order[i].Agg != nil {
			w.selectExpr(order[i].Agg)
		}
	}
}

// WalkExpr walks one expression and everything inside it.
func (w *Walker) WalkExpr(e *Expr) {
	if w.OnExpr != nil && !w.OnExpr(e) {
		return
	}
	if e.Left != nil {
		w.WalkExpr(e.Left)
	}
	if e.Right != nil {
		w.WalkExpr(e.Right)
	}
	for i := range e.Args {
		w.WalkExpr(&e.Args[i])
	}
	if e.Sub != nil {
		w.SelectStmt(e.Sub)
	}
	if c := e.Case; c != nil {
		if c.Operand != nil {
			w.WalkExpr(c.Operand)
		}
		for i := range c.Whens {
			wh := &c.Whens[i]
			if wh.Value != nil {
				w.WalkExpr(wh.Value)
			}
			w.Conds(wh.Cond)
			w.WalkExpr(&wh.Result)
		}
		if c.Else != nil {
			w.WalkExpr(c.Else)
		}
	}
	if e.Cmp != nil {
		w.Cond(e.Cmp)
	}
	if e.Window != nil {
		w.selectExpr(e.Window)
	}
}

// Conds walks a conjunction.
func (w *Walker) Conds(conds []Comparison) {
	for i := range conds {
		w.Cond(&conds[i])
	}
}

// Cond walks one conjunct: its computed left-hand side, its value or
// value list, its subquery and its OR groups.
func (w *Walker) Cond(c *Comparison) {
	if w.OnCond != nil && !w.OnCond(c) {
		return
	}
	if c.Expr != nil {
		w.WalkExpr(c.Expr)
	}
	w.WalkExpr(&c.Value)
	for i := range c.Values {
		w.WalkExpr(&c.Values[i])
	}
	if c.Sub != nil {
		w.SelectStmt(c.Sub)
	}
	for _, d := range c.Or {
		w.Conds(d)
	}
}
