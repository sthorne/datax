package sql

import (
	"github.com/sthorne/datax/pkg/sql/catalog"
	"github.com/sthorne/datax/pkg/sql/parser"
	"github.com/sthorne/datax/pkg/sql/types"
)

// Aggregates and GROUP BY over joins ride the derived-table template: the
// joined rows materialize under a synthetic descriptor whose columns are
// the qualified "alias.column" names, every reference in the select is
// canonicalized to that spelling, and the grouped executor runs unchanged
// over the wrapped rows.

// canonJoinName renders a resolved reference in the synthetic descriptor's
// namespace.
func canonJoinName(sides []joinSide, ref joinRef) string {
	return sides[ref.side].alias + "." + ref.col.Name
}

// groupedJoinDesc builds the synthetic descriptor: each side's visible
// columns in join order, named alias.column, IDs 1..n.
func groupedJoinDesc(sides []joinSide) (*catalog.TableDescriptor, []joinRef) {
	desc := &catalog.TableDescriptor{Name: ""}
	var refs []joinRef
	id := catalog.ColumnID(1)
	for i, js := range sides {
		for _, c := range js.desc.VisibleColumns() {
			desc.Columns = append(desc.Columns, catalog.Column{
				ID: id, Name: js.alias + "." + c.Name, Type: c.Type,
			})
			refs = append(refs, joinRef{side: i, col: c})
			id++
		}
	}
	return desc, refs
}

// groupedJoinSelect returns a copy of the select with every column
// reference canonicalized to alias.column (so it resolves in the
// synthetic descriptor), keeping the user-visible output names bare.
// ORDER BY is rewritten to bare result names — the grouped executor
// orders by output column, the same rule single-table GROUP BY follows.
func groupedJoinSelect(sides []joinSide, t *parser.Select) (*parser.Select, error) {
	canon := func(name string) (string, error) {
		ref, err := resolveJoinRef(sides, name)
		if err != nil {
			return "", err
		}
		return canonJoinName(sides, ref), nil
	}
	c := *t
	c.Exprs = append([]parser.SelectExpr(nil), t.Exprs...)
	for i := range c.Exprs {
		se := &c.Exprs[i]
		if se.Expr.Column != "" && se.Expr.BinOp == "" {
			name, err := canon(se.Expr.Column)
			if err != nil {
				return nil, err
			}
			if se.Alias == "" {
				_, bare := splitQualified(se.Expr.Column)
				se.Alias = bare
			}
			se.Expr.Column = name
		}
		if se.AggCol != "" && se.AggCol != "*" {
			name, err := canon(se.AggCol)
			if err != nil {
				return nil, err
			}
			se.AggCol = name
		}
		if err := canonAggExprs(se, canon); err != nil {
			return nil, err
		}
	}
	c.GroupBy = append([]string(nil), t.GroupBy...)
	for i := range c.GroupBy {
		name, err := canon(c.GroupBy[i])
		if err != nil {
			return nil, err
		}
		c.GroupBy[i] = name
	}
	c.Having = append([]parser.HavingCond(nil), t.Having...)
	for i := range c.Having {
		hc := &c.Having[i]
		if hc.Agg != nil {
			agg := *hc.Agg
			if agg.AggCol != "" && agg.AggCol != "*" {
				name, err := canon(agg.AggCol)
				if err != nil {
					return nil, err
				}
				agg.AggCol = name
			}
			if err := canonAggExprs(&agg, canon); err != nil {
				return nil, err
			}
			hc.Agg = &agg
		} else if hc.Column != "" {
			// A grouped column reference canonicalizes; anything else may
			// be an output name (aggregate alias) — leave it for the
			// grouped resolver's output-name fallback.
			if name, err := canon(hc.Column); err == nil {
				hc.Column = name
			}
		}
	}
	// ORDER BY over grouped output resolves by RESULT name: strip any
	// qualifier that resolves to a join column (its output name is the
	// bare form set above); leave aggregate output names untouched.
	c.OrderBy = append([]parser.OrderCol(nil), t.OrderBy...)
	for i := range c.OrderBy {
		oc := &c.OrderBy[i]
		if oc.Agg != nil {
			agg := *oc.Agg
			if agg.AggCol != "" && agg.AggCol != "*" {
				name, err := canon(agg.AggCol)
				if err != nil {
					return nil, err
				}
				agg.AggCol = name
			}
			if err := canonAggExprs(&agg, canon); err != nil {
				return nil, err
			}
			oc.Agg = &agg
			continue
		}
		if _, err := resolveJoinRef(sides, oc.Column); err == nil {
			_, bare := splitQualified(oc.Column)
			oc.Column = bare
		}
	}
	// WHERE was already applied to the joined rows; the grouped executor
	// ignores it (derived-table precedent), but clear it so no path can
	// double-resolve un-canonicalized names.
	c.Where = nil
	return &c, nil
}

// canonAggExprs rewrites the column references inside an aggregate's
// expression argument, extra arguments, FILTER and WITHIN GROUP to the
// synthetic descriptor's names.
func canonAggExprs(se *parser.SelectExpr, canon func(string) (string, error)) error {
	var err error
	rename := func(name string) string {
		if err != nil {
			return name
		}
		n, e := canon(name)
		if e != nil {
			err = e
			return name
		}
		return n
	}
	if se.AggArg != nil {
		e := renameExprColumns(*se.AggArg, rename)
		se.AggArg = &e
	}
	if len(se.AggArgs) > 0 {
		args := make([]parser.Expr, len(se.AggArgs))
		for i, a := range se.AggArgs {
			args[i] = renameExprColumns(a, rename)
		}
		se.AggArgs = args
	}
	if len(se.AggFilter) > 0 {
		se.AggFilter = renameCondColumns(se.AggFilter, rename)
	}
	if len(se.AggOrder) > 0 {
		order := make([]parser.OrderCol, len(se.AggOrder))
		for i, oc := range se.AggOrder {
			order[i] = oc
			if oc.Expr != nil {
				e := renameExprColumns(*oc.Expr, rename)
				order[i].Expr = &e
			} else if oc.Column != "" {
				order[i].Column = rename(oc.Column)
			}
		}
		se.AggOrder = order
	}
	return err
}

// renameExprColumns returns a copy of e with every column reference
// renamed.
func renameExprColumns(e parser.Expr, rename func(string) string) parser.Expr {
	out := e
	if e.Column != "" {
		out.Column = rename(e.Column)
	}
	if e.Left != nil {
		l := renameExprColumns(*e.Left, rename)
		out.Left = &l
	}
	if e.Right != nil {
		r := renameExprColumns(*e.Right, rename)
		out.Right = &r
	}
	if len(e.Args) > 0 {
		out.Args = make([]parser.Expr, len(e.Args))
		for i, a := range e.Args {
			out.Args[i] = renameExprColumns(a, rename)
		}
	}
	if e.Case != nil {
		ce := *e.Case
		if ce.Operand != nil {
			op := renameExprColumns(*ce.Operand, rename)
			ce.Operand = &op
		}
		ce.Whens = make([]parser.CaseWhen, len(e.Case.Whens))
		for i, w := range e.Case.Whens {
			nw := w
			if w.Value != nil {
				v := renameExprColumns(*w.Value, rename)
				nw.Value = &v
			}
			if len(w.Cond) > 0 {
				nw.Cond = renameCondColumns(w.Cond, rename)
			}
			nw.Result = renameExprColumns(w.Result, rename)
			ce.Whens[i] = nw
		}
		if ce.Else != nil {
			el := renameExprColumns(*ce.Else, rename)
			ce.Else = &el
		}
		out.Case = &ce
	}
	if e.Cmp != nil {
		c := renameCondColumns([]parser.Comparison{*e.Cmp}, rename)[0]
		out.Cmp = &c
	}
	return out
}

// renameCondColumns is renameExprColumns over a conjunction.
func renameCondColumns(conds []parser.Comparison, rename func(string) string) []parser.Comparison {
	out := make([]parser.Comparison, len(conds))
	for i, c := range conds {
		nc := c
		if c.Column != "" {
			nc.Column = rename(c.Column)
		}
		if c.Expr != nil {
			e := renameExprColumns(*c.Expr, rename)
			nc.Expr = &e
		}
		nc.Value = renameExprColumns(c.Value, rename)
		if len(c.Values) > 0 {
			nc.Values = make([]parser.Expr, len(c.Values))
			for j, v := range c.Values {
				nc.Values[j] = renameExprColumns(v, rename)
			}
		}
		if len(c.Or) > 0 {
			nc.Or = make([][]parser.Comparison, len(c.Or))
			for j, d := range c.Or {
				nc.Or[j] = renameCondColumns(d, rename)
			}
		}
		out[i] = nc
	}
	return out
}

// groupedJoinQuery canonicalizes the select and resolves it against the
// synthetic descriptor — the validation half, shared with EXPLAIN and
// PlanColumns.
func groupedJoinQuery(sides []joinSide, t *parser.Select) (*catalog.TableDescriptor, []joinRef, *parser.Select, error) {
	desc, refs := groupedJoinDesc(sides)
	sel, err := groupedJoinSelect(sides, t)
	if err != nil {
		return nil, nil, nil, err
	}
	if _, err := resolveGrouped(desc, sel); err != nil {
		return nil, nil, nil, err
	}
	return desc, refs, sel, nil
}

// execGroupedJoin wraps the joined rows as synthetic-descriptor rows and
// hands them to the grouped executor.
func (s *Session) execGroupedJoin(sides []joinSide, joined []joinedRow, t *parser.Select, params []types.Datum) (*Result, error) {
	desc, refs, sel, err := groupedJoinQuery(sides, t)
	if err != nil {
		return nil, err
	}
	rows := make([]fetchedRow, 0, len(joined))
	for _, jr := range joined {
		row := make(map[catalog.ColumnID]types.Datum, len(refs))
		for i, ref := range refs {
			row[catalog.ColumnID(i+1)] = jr.datum(ref)
		}
		rows = append(rows, fetchedRow{row: row})
	}
	return s.execGroupedOver(desc, rows, sel, params)
}
