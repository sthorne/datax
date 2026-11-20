package sql

import (
	"context"
	"strings"

	"github.com/sthorne/datax/pkg/kvclient"
	"github.com/sthorne/datax/pkg/sql/catalog"
	"github.com/sthorne/datax/pkg/sql/parser"
	"github.com/sthorne/datax/pkg/sql/types"
)

// projCol is one resolved output column of a SELECT.
type projCol struct {
	col  catalog.Column
	expr *parser.Expr // non-nil for computed expressions (rendered as TEXT)
	name string
}

func resolveProjection(desc *catalog.TableDescriptor, exprs []parser.SelectExpr) ([]projCol, error) {
	var proj []projCol
	for _, se := range exprs {
		if se.Star {
			for _, c := range desc.VisibleColumns() {
				proj = append(proj, projCol{col: c, name: c.Name})
			}
			continue
		}
		if se.Expr.Column != "" && se.Expr.BinOp == "" && se.Expr.Cast == "" {
			c, ok := desc.Col(se.Expr.Column)
			if !ok {
				return nil, newErrf(CodeUndefinedColumn, "column %q does not exist", se.Expr.Column)
			}
			name := se.Alias
			if name == "" {
				name = c.Name
			}
			if len(se.Expr.Path) > 0 {
				// col -> 'k' / ->> 'k': a computed column typed by the chain.
				if c.Type != types.Jsonb {
					return nil, newErrf(CodeFeatureNotSupported, "cannot extract path from type %s (-> and ->> require jsonb)", c.Type)
				}
				e := se.Expr
				if se.Alias == "" {
					name = "?column?"
				}
				proj = append(proj, projCol{expr: &e, name: name, col: catalog.Column{Type: pathResultType(se.Expr.Path)}})
				continue
			}
			proj = append(proj, projCol{col: c, name: name})
			continue
		}
		e := se.Expr
		name := se.Alias
		if name == "" {
			name = "?column?"
			if e.Column != "" && e.BinOp == "" && len(e.Path) == 0 {
				name = stripQualifier(e.Column) // col::type is still "col"
			}
		}
		typ := exprFamily(e, func(n string) (types.Family, bool) {
			c, ok := desc.Col(stripQualifier(n))
			return c.Type, ok
		})
		if typ == types.Unknown {
			typ = types.String
		}
		noTZ := typ == types.Timestamp && exprNoTZ(e, func(n string) bool {
			c, ok := desc.Col(stripQualifier(n))
			return ok && c.NoTZ
		})
		proj = append(proj, projCol{expr: &e, name: name, col: catalog.Column{Type: typ, NoTZ: noTZ}})
	}
	return proj, nil
}

// stripQualifier drops a table or alias qualifier from a column name.
func stripQualifier(name string) string {
	if i := strings.LastIndexByte(name, '.'); i >= 0 {
		return name[i+1:]
	}
	return name
}

// lookupForPlan resolves a descriptor for planning: inside the session's
// open transaction if any, else in a throwaway one.
func (s *Session) lookupForPlan(ctx context.Context, name string) (*catalog.TableDescriptor, error) {
	if s.state == StateOpen {
		return s.lookup(ctx, s.txn, name)
	}
	var desc *catalog.TableDescriptor
	err := s.db.RunTxn(ctx, "plan", func(ctx context.Context, txn *kvclient.Txn) error {
		var err error
		desc, err = s.lookup(ctx, txn, name)
		return err
	})
	return desc, err
}

// PlanColumns returns the output columns a statement will produce, without
// executing it (nil for row-less statements). The wire protocol's Describe
// needs this before Execute.
func (s *Session) PlanColumns(ctx context.Context, stmt parser.Statement) ([]ResultColumn, *Error) {
	cols, serr := s.planColumns(ctx, stmt)
	if sel, ok := stmt.(*parser.Select); ok && serr == nil && len(cols) == len(sel.Exprs) {
		// A catalog function's call is named as execution names it
		// (resolveSelectSubs): pg_get_indexdef, not ?column?.
		for i, se := range sel.Exprs {
			if !se.Star && se.Alias == "" && catalogRenderingFuncs[se.Expr.Func] && cols[i].Name == "?column?" {
				cols[i].Name = se.Expr.Func
			}
		}
	}
	return cols, serr
}

func (s *Session) planColumns(ctx context.Context, stmt parser.Statement) ([]ResultColumn, *Error) {
	expanded, err := s.expandViews(ctx, nil, stmt)
	if err != nil {
		return nil, ToSQLError(err)
	}
	stmt = expanded
	if with := stmtWith(stmt); len(with) > 0 {
		restore, err := s.bindWith(ctx, nil, with, nil, true, nil)
		if err != nil {
			return nil, ToSQLError(err)
		}
		defer restore()
		stmt = stmtWithout(stmt)
	}
	cols, serr := s.planBoundColumns(ctx, stmt)
	if sel, ok := stmt.(*parser.Select); ok && serr == nil {
		// A scalar subquery item is described as its own output column
		// (execSelect types it the same), while the WITH list is bound.
		s.typeScalarSubItems(ctx, sel, cols, nil)
	}
	return cols, serr
}

// planBoundColumns is planColumns once the statement's WITH list is
// bound.
func (s *Session) planBoundColumns(ctx context.Context, stmt parser.Statement) ([]ResultColumn, *Error) {
	switch t := stmt.(type) {
	case *parser.Select:
		if hasDerivedJoin(t) {
			bound, restore, err := s.bindJoinedDerived(ctx, nil, t, nil, true)
			if err != nil {
				return nil, ToSQLError(err)
			}
			defer restore()
			t = bound
		}
		// Execution addresses a single table's columns by bare name.
		if err := stripTableAlias(t); err != nil {
			return nil, ToSQLError(err)
		}
		if err := checkDistinctOn(t); err != nil {
			return nil, ToSQLError(err)
		}
		if hasWindows(t.Exprs) {
			wp, err := windowPlanFor(t)
			if err != nil {
				return nil, ToSQLError(err)
			}
			innerCols, serr := s.PlanColumns(ctx, wp.inner)
			if serr != nil {
				return nil, serr
			}
			if err := wp.windowTypes(innerCols); err != nil {
				return nil, ToSQLError(err)
			}
			return wp.outputColumns(innerCols), nil
		}
		if t.Derived != nil && len(t.Joins) > 0 {
			innerCols, err := s.PlanColumns(ctx, t.Derived)
			if err != nil {
				return nil, err
			}
			name := strings.ToLower(t.Alias)
			desc, derr := relationDesc(name, innerCols, nil)
			if derr != nil {
				return nil, ToSQLError(derr)
			}
			prev, had := s.bindRelation(name, &relation{desc: desc})
			defer func() {
				if had {
					s.rels[name] = prev
				} else {
					delete(s.rels, name)
				}
			}()
			c := *t
			c.Derived, c.Table = nil, name
			return s.PlanColumns(ctx, &c)
		}
		if fromlessGrouped(t) {
			return s.PlanColumns(ctx, oneRowFrom(t))
		}
		if t.Union != nil {
			// A set operation's output is named by its head and typed by
			// unifying every member (the numeric families lift, anything
			// else meets as text); a VALUES list's as execution types it
			// (resolveValuesTypes).
			head := *t
			head.Union, head.OrderBy, head.Limit, head.Offset = nil, nil, -1, 0
			cols, err := s.PlanColumns(ctx, &head)
			if err != nil {
				return nil, err
			}
			asts := []*parser.Select{t}
			memberCols := [][]ResultColumn{cols}
			for m := t.Union; m != nil; m = m.Union {
				one := *m
				one.Union = nil
				mc, err := s.PlanColumns(ctx, &one)
				if err != nil {
					return nil, err
				}
				if len(mc) != len(cols) {
					return nil, newErrf(CodeSyntaxError, "each set operation member must have the same number of columns")
				}
				asts, memberCols = append(asts, m), append(memberCols, mc)
			}
			if valuesList(asts) {
				for i := range cols {
					fam, verr := valuesColumnType(asts, i, func(mi int) types.Family { return memberCols[mi][i].Type })
					if verr != nil {
						return nil, ToSQLError(verr)
					}
					cols[i].Type = fam
				}
				return cols, nil
			}
			for _, mc := range memberCols[1:] {
				for i := range cols {
					cols[i].Type = unifyFamily(cols[i].Type, mc[i].Type)
				}
			}
			return cols, nil
		}
		if t.Derived != nil || t.FuncTable != nil {
			var desc *catalog.TableDescriptor
			if t.FuncTable != nil {
				d, err := funcTableDesc(t)
				if err != nil {
					return nil, ToSQLError(err)
				}
				desc = d
			} else {
				innerCols, err := s.PlanColumns(ctx, t.Derived)
				if err != nil {
					return nil, err
				}
				desc = derivedDesc(t.Alias, innerCols)
			}
			td := derivedSelect(t)
			if hasAggregates(td.Exprs) || len(td.GroupBy) > 0 {
				gq, aerr := resolveGrouped(desc, td)
				if aerr != nil {
					return nil, ToSQLError(aerr)
				}
				cols := make([]ResultColumn, len(gq.outs))
				for i, oc := range gq.outs {
					cols[i] = ResultColumn{Name: oc.name, Type: oc.typ, NoTZ: oc.noTZ}
				}
				return cols, nil
			}
			proj, perr := resolveProjection(desc, td.Exprs)
			if perr != nil {
				return nil, ToSQLError(perr)
			}
			cols := make([]ResultColumn, len(proj))
			for i, p := range proj {
				cols[i] = colResult(p.name, p.col)
			}
			return cols, nil
		}
		if t.Table == "" {
			var cols []ResultColumn
			for _, se := range t.Exprs {
				if se.Star {
					return nil, newErrf(CodeSyntaxError, "SELECT * requires a FROM clause")
				}
				name := se.Alias
				if name == "" {
					name = "?column?"
				}
				fam := exprFamily(se.Expr, nil)
				if fn, ok := selectSRF(se.Expr); ok {
					// Named by the function, as execution names it.
					if se.Alias == "" {
						name = fn
					}
					if fn == "unnest" {
						fam = exprFamily(se.Expr.Args[0], nil).Elem()
					}
				}
				if fam == types.Unknown {
					fam = types.String
				}
				if se.Expr.Sub != nil && se.Expr.BinOp == "" {
					if sc, serr := s.PlanColumns(ctx, se.Expr.Sub); serr == nil && len(sc) == 1 {
						fam = sc[0].Type
					}
				}
				cols = append(cols, ResultColumn{Name: name, Type: fam, NoTZ: exprNoTZ(se.Expr, nil)})
			}
			return cols, nil
		}
		desc, err := s.lookupForPlan(ctx, t.Table)
		if err != nil {
			return nil, ToSQLError(err)
		}
		if len(t.Joins) > 0 {
			sides, jerr := s.planJoinSides(ctx, desc, t)
			if jerr != nil {
				return nil, ToSQLError(jerr)
			}
			// Subqueries in the select list are evaluated per row at
			// execution: describe them by their columns' types.
			t = s.maskSubqueryExprs(ctx, t)
			if hasAggregates(t.Exprs) || len(t.GroupBy) > 0 {
				jdesc, _, sel, gerr := groupedJoinQuery(sides, t)
				if gerr != nil {
					return nil, ToSQLError(gerr)
				}
				gq, aerr := resolveGrouped(jdesc, sel)
				if aerr != nil {
					return nil, ToSQLError(aerr)
				}
				cols := make([]ResultColumn, len(gq.outs))
				for i, oc := range gq.outs {
					cols[i] = ResultColumn{Name: oc.name, Type: oc.typ, NoTZ: oc.noTZ}
				}
				return cols, nil
			}
			proj, perr := resolveJoinProjection(sides, t.Exprs)
			if perr != nil {
				return nil, ToSQLError(perr)
			}
			cols := make([]ResultColumn, len(proj))
			for i, p := range proj {
				cols[i] = exprResult(p.name, p.typ, p.ref.col)
			}
			return cols, nil
		}
		if hasAggregates(t.Exprs) || len(t.GroupBy) > 0 {
			gq, aerr := resolveGrouped(desc, t)
			if aerr != nil {
				return nil, ToSQLError(aerr)
			}
			cols := make([]ResultColumn, len(gq.outs))
			for i, oc := range gq.outs {
				cols[i] = ResultColumn{Name: oc.name, Type: oc.typ, NoTZ: oc.noTZ}
			}
			return cols, nil
		}
		proj, perr := resolveProjection(desc, s.maskSubqueryExprs(ctx, t).Exprs)
		if perr != nil {
			return nil, ToSQLError(perr)
		}
		cols := make([]ResultColumn, len(proj))
		for i, p := range proj {
			cols[i] = colResult(p.name, p.col)
		}
		return cols, nil

	case *parser.Insert:
		return s.planReturning(ctx, t.Table, t.Returning)
	case *parser.Update:
		if len(t.From) > 0 {
			return s.dmlReturningColumns(ctx, t.Table, t.Alias, t.From, t.Returning)
		}
		t, err := stripUpdateAlias(t)
		if err != nil {
			return nil, ToSQLError(err)
		}
		return s.planReturning(ctx, t.Table, t.Returning)
	case *parser.Delete:
		if len(t.Using) > 0 {
			return s.dmlReturningColumns(ctx, t.Table, t.Alias, t.Using, t.Returning)
		}
		t, err := stripDeleteAlias(t)
		if err != nil {
			return nil, ToSQLError(err)
		}
		return s.planReturning(ctx, t.Table, t.Returning)

	case *parser.Explain:
		return []ResultColumn{{Name: "plan", Type: types.String}}, nil

	case *parser.ShowTables:
		return showTablesColumns(), nil
	case *parser.Show:
		return showColumns(t), nil
	case *parser.ShowDatabases:
		return showDatabasesColumns(), nil
	case *parser.ShowPlacement:
		return showPlacementColumns(), nil
	case *parser.AlterTable:
		return alterTableColumns(t), nil

	case *parser.ShowFunctions:
		return execShowFunctions().Columns, nil
	case *parser.ShowSequences:
		return showSequencesColumns(), nil
	case *parser.ShowStats:
		return showStatsColumns(), nil

	case *parser.Analyze, *parser.Vacuum, *parser.LockTable, *parser.Prepare, *parser.Deallocate, *parser.DeclareCursor, *parser.CloseCursor, *parser.SetTransactionSnapshot, *parser.SetTransactionAsOf:
		return nil, nil
	case *parser.Fetch:
		if c, ok := s.cursors[t.Name]; ok && !t.Move {
			return c.cols, nil
		}
		return nil, nil
	case *parser.ExecutePrepared:
		inner, _, serr := s.executePrepared(t, nil)
		if serr != nil {
			return nil, serr
		}
		return s.PlanColumns(ctx, inner)

	case *parser.ShowEncryptionKeys:
		return showEncryptionKeysColumns(), nil
	case *parser.ShowClusterSetting:
		return []ResultColumn{{Name: t.Name, Type: types.String}}, nil

	case *parser.SetVar:
		if len(t.Name) > 5 && t.Name[:5] == "show:" {
			name := t.Name[5:]
			if canonical, _, ok := s.setting(name); ok {
				name = canonical
			}
			return []ResultColumn{{Name: name, Type: types.String}}, nil
		}
		return nil, nil

	default:
		return nil, nil
	}
}

// planJoinSides builds the join sides for planning (Describe/parameter
// inference) without privilege checks — mirrors resolveJoinQuery's shape.
func (s *Session) planJoinSides(ctx context.Context, baseDesc *catalog.TableDescriptor, t *parser.Select) ([]joinSide, error) {
	inner := make([]*catalog.TableDescriptor, len(t.Joins))
	for i := range t.Joins {
		d, err := s.lookupForPlan(ctx, t.Joins[i].Table)
		if err != nil {
			return nil, err
		}
		inner[i] = d
	}
	sides, err := makeJoinSides(baseDesc, inner, t)
	if err != nil {
		return nil, err
	}
	if _, err := expandUsing(sides, t); err != nil {
		return nil, err
	}
	return sides, nil
}

// maskSubqueryExprs returns t with every select-list expression that
// carries a subquery replaced by a NULL placeholder (the original is
// untouched), for describing a statement without evaluating it. Each
// scalar subquery stands as a NULL of its own column's type, so the
// expression around it (or the bare subquery) types as execution
// types it; an expression with another kind of subquery is masked
// whole, as text.
func (s *Session) maskSubqueryExprs(ctx context.Context, t *parser.Select) *parser.Select {
	var out *parser.Select
	for i, se := range t.Exprs {
		if se.Star || !exprHasSubquery(se.Expr) {
			continue
		}
		if out == nil {
			c := *t
			c.Exprs = append([]parser.SelectExpr(nil), t.Exprs...)
			out = &c
		}
		if typed, ok := s.typeSubqueryOperands(ctx, se.Expr); ok {
			out.Exprs[i].Expr = typed
			continue
		}
		null := types.DNull
		out.Exprs[i].Expr = parser.Expr{Lit: &null}
	}
	if out == nil {
		return t
	}
	return out
}

// planReturning describes a write's RETURNING columns (nil without one).
func (s *Session) planReturning(ctx context.Context, table string, exprs []parser.SelectExpr) ([]ResultColumn, *Error) {
	if exprs == nil {
		return nil, nil
	}
	desc, err := s.lookupForPlan(ctx, table)
	if err != nil {
		return nil, ToSQLError(err)
	}
	ret, rerr := s.returningProjection(desc, table, exprs)
	if rerr != nil {
		return nil, ToSQLError(rerr)
	}
	return ret.columns(), nil
}

// stmtWith is a statement's WITH list (nil for statements without one).
func stmtWith(stmt parser.Statement) []parser.CTE {
	switch t := stmt.(type) {
	case *parser.Select:
		return t.With
	case *parser.Insert:
		return t.With
	case *parser.Update:
		return t.With
	case *parser.Delete:
		return t.With
	}
	return nil
}

// stmtWithout is the statement with its WITH list detached (a copy).
func stmtWithout(stmt parser.Statement) parser.Statement {
	switch t := stmt.(type) {
	case *parser.Select:
		c := *t
		c.With = nil
		return &c
	case *parser.Insert:
		c := *t
		c.With = nil
		return &c
	case *parser.Update:
		c := *t
		c.With = nil
		return &c
	case *parser.Delete:
		c := *t
		c.With = nil
		return &c
	}
	return stmt
}
