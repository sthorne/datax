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
			for _, c := range desc.Columns {
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
	assign := func(e parser.Expr, fam types.Family) {
		for {
			if e.Param > 0 && fams[e.Param-1] == types.Unknown {
				fams[e.Param-1] = fam
			}
			if e.Right == nil {
				return
			}
			e = *e.Right
		}
	}
	fromWhere := func(desc *catalog.TableDescriptor, where []parser.Comparison) {
		for _, cmp := range where {
			if col, ok := desc.Col(cmp.Column); ok {
				assign(cmp.Value, col.Type)
			}
		}
	}
	switch t := stmt.(type) {
	case *parser.Insert:
		desc, err := s.lookupForPlan(ctx, t.Table)
		if err != nil {
			return nil, ToSQLError(err)
		}
		target := desc.Columns
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
				cols = append(cols, ResultColumn{Name: name, Type: fam})
			}
			return cols, nil
		}
		var proj []projCol
		lookup := func(ctx context.Context, txn *kvclient.Txn) error {
			desc, err := s.cat.Lookup(ctx, txn, t.Table)
			if err != nil {
				return err
			}
			var perr error
			proj, perr = resolveProjection(desc, t.Exprs)
			return perr
		}
		var err error
		if s.state == StateOpen {
			err = lookup(ctx, s.txn)
		} else {
			err = s.db.RunTxn(ctx, "plan", lookup)
		}
		if err != nil {
			return nil, ToSQLError(err)
		}
		cols := make([]ResultColumn, len(proj))
		for i, p := range proj {
			cols[i] = ResultColumn{Name: p.name, Type: p.col.Type}
		}
		return cols, nil

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
