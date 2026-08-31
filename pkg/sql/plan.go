package sql

import (
	"context"

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
		if se.Expr.Column != "" && se.Expr.BinOp == "" {
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
		}
		proj = append(proj, projCol{expr: &e, name: name, col: catalog.Column{Type: types.String}})
	}
	return proj, nil
}

// lookupForPlan resolves a descriptor for planning: inside the session's
// open transaction if any, else in a throwaway one.
func (s *Session) lookupForPlan(ctx context.Context, name string) (*catalog.TableDescriptor, error) {
	if s.state == StateOpen {
		return s.cat.Lookup(ctx, s.txn, name)
	}
	var desc *catalog.TableDescriptor
	err := s.db.RunTxn(ctx, "plan", func(ctx context.Context, txn *kvclient.Txn) error {
		var err error
		desc, err = s.cat.Lookup(ctx, txn, name)
		return err
	})
	return desc, err
}

// PlanParams infers the type of each $N parameter from its column context
// (WHERE col = $1, SET col = $1, INSERT VALUES ($1)). Unknown stays Unknown
// and is treated as TEXT on the wire.
func (s *Session) PlanParams(ctx context.Context, stmt parser.Statement) ([]types.Family, *Error) {
	n := parser.CountParams(stmt)
	if n == 0 {
		return nil, nil
	}
	fams := make([]types.Family, n)
	var assign func(e parser.Expr, fam types.Family)
	assign = func(e parser.Expr, fam types.Family) {
		if e.Param > 0 && fams[e.Param-1] == types.Unknown {
			fams[e.Param-1] = fam
		}
		if e.Left != nil {
			assign(*e.Left, fam)
		}
		if e.Right != nil {
			assign(*e.Right, fam)
		}
		for _, a := range e.Args {
			assign(a, fam)
		}
	}
	var fromWhere func(desc *catalog.TableDescriptor, where []parser.Comparison)
	fromWhere = func(desc *catalog.TableDescriptor, where []parser.Comparison) {
		for _, cmp := range where {
			if col, ok := desc.Col(cmp.Column); ok {
				typ := col.Type
				if len(cmp.Path) > 0 {
					typ = pathResultType(cmp.Path)
				}
				assign(cmp.Value, typ)
				for _, v := range cmp.Values {
					assign(v, typ)
				}
			}
			for _, d := range cmp.Or {
				fromWhere(desc, d)
			}
		}
	}
	switch t := stmt.(type) {
	case *parser.Insert:
		desc, err := s.lookupForPlan(ctx, t.Table)
		if err != nil {
			return nil, ToSQLError(err)
		}
		target := desc.VisibleColumns()
		if len(t.Columns) > 0 {
			target = nil
			for _, name := range t.Columns {
				if col, ok := desc.Col(name); ok {
					target = append(target, col)
				} else {
					target = append(target, catalog.Column{})
				}
			}
		}
		for _, row := range t.Rows {
			for i, e := range row {
				if i < len(target) {
					assign(e, target[i].Type)
				}
			}
		}
	case *parser.Select:
		if t.Table != "" {
			desc, err := s.lookupForPlan(ctx, t.Table)
			if err != nil {
				return nil, ToSQLError(err)
			}
			fromWhere(desc, t.Where)
			if len(t.Joins) > 0 {
				// Qualified references resolve against any join side.
				if sides, jerr := s.planJoinSides(ctx, desc, t); jerr == nil {
					for _, cmp := range t.Where {
						if ref, rerr := resolveJoinRef(sides, cmp.Column); rerr == nil {
							typ := ref.col.Type
							if len(cmp.Path) > 0 {
								typ = pathResultType(cmp.Path)
							}
							assign(cmp.Value, typ)
						}
					}
				}
			}
		}
	case *parser.Update:
		desc, err := s.lookupForPlan(ctx, t.Table)
		if err != nil {
			return nil, ToSQLError(err)
		}
		for _, set := range t.Set {
			if col, ok := desc.Col(set.Column); ok {
				assign(set.Value, col.Type)
			}
		}
		fromWhere(desc, t.Where)
	case *parser.Delete:
		desc, err := s.lookupForPlan(ctx, t.Table)
		if err != nil {
			return nil, ToSQLError(err)
		}
		fromWhere(desc, t.Where)
	}
	return fams, nil
}

// PlanColumns returns the output columns a statement will produce, without
// executing it (nil for row-less statements). The wire protocol's Describe
// needs this before Execute.
func (s *Session) PlanColumns(ctx context.Context, stmt parser.Statement) ([]ResultColumn, *Error) {
	switch t := stmt.(type) {
	case *parser.Select:
		if t.Derived != nil {
			innerCols, err := s.PlanColumns(ctx, t.Derived)
			if err != nil {
				return nil, err
			}
			desc := derivedDesc(t.Alias, innerCols)
			td := derivedSelect(t)
			if hasAggregates(td.Exprs) || len(td.GroupBy) > 0 {
				gq, aerr := resolveGrouped(desc, td)
				if aerr != nil {
					return nil, ToSQLError(aerr)
				}
				cols := make([]ResultColumn, len(gq.outs))
				for i, oc := range gq.outs {
					cols[i] = ResultColumn{Name: oc.name, Type: oc.typ}
				}
				return cols, nil
			}
			proj, perr := resolveProjection(desc, td.Exprs)
			if perr != nil {
				return nil, ToSQLError(perr)
			}
			cols := make([]ResultColumn, len(proj))
			for i, p := range proj {
				cols[i] = ResultColumn{Name: p.name, Type: p.col.Type, Typmod: colTypmod(p.col)}
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
				fam := types.String
				if se.Expr.Lit != nil && !se.Expr.Lit.Null {
					fam = se.Expr.Lit.Fam
				}
				if se.Expr.Sub != nil && se.Expr.BinOp == "" {
					if sc, serr := s.PlanColumns(ctx, se.Expr.Sub); serr == nil && len(sc) == 1 {
						fam = sc[0].Type
					}
				}
				cols = append(cols, ResultColumn{Name: name, Type: fam})
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
					cols[i] = ResultColumn{Name: oc.name, Type: oc.typ}
				}
				return cols, nil
			}
			proj, perr := resolveJoinProjection(sides, t.Exprs)
			if perr != nil {
				return nil, ToSQLError(perr)
			}
			cols := make([]ResultColumn, len(proj))
			for i, p := range proj {
				cols[i] = ResultColumn{Name: p.name, Type: p.typ, Typmod: colTypmod(p.ref.col)}
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
				cols[i] = ResultColumn{Name: oc.name, Type: oc.typ}
			}
			return cols, nil
		}
		proj, perr := resolveProjection(desc, t.Exprs)
		if perr != nil {
			return nil, ToSQLError(perr)
		}
		cols := make([]ResultColumn, len(proj))
		for i, p := range proj {
			cols[i] = ResultColumn{Name: p.name, Type: p.col.Type, Typmod: colTypmod(p.col)}
		}
		return cols, nil

	case *parser.Explain:
		return []ResultColumn{{Name: "plan", Type: types.String}}, nil

	case *parser.ShowTables:
		return []ResultColumn{{Name: "table_name", Type: types.String}}, nil

	case *parser.SetVar:
		if len(t.Name) > 5 && t.Name[:5] == "show:" {
			return []ResultColumn{{Name: t.Name[5:], Type: types.String}}, nil
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
	return makeJoinSides(baseDesc, inner, t)
}
