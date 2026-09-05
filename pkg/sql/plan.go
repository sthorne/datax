package sql

import (
	"context"
	"strings"

	"github.com/sthorne/datax/pkg/kvclient"
	"github.com/sthorne/datax/pkg/sql/builtins"
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
		proj = append(proj, projCol{expr: &e, name: name, col: catalog.Column{Type: typ}})
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

// PlanParams infers the type of each $N parameter from its column context
// (WHERE col = $1, SET col = $1, INSERT VALUES ($1)). Unknown stays Unknown
// and is treated as TEXT on the wire.
func (s *Session) PlanParams(ctx context.Context, stmt parser.Statement) ([]types.Family, *Error) {
	n := parser.CountParams(stmt)
	if n == 0 {
		return nil, nil
	}
	fams := make([]types.Family, n)
	if expanded, err := s.expandViews(ctx, nil, stmt); err != nil {
		return nil, ToSQLError(err)
	} else {
		stmt = expanded
	}
	if with := stmtWith(stmt); len(with) > 0 {
		// The members bind (columns only) so lookups resolve, and each
		// member's own parameters type as in its query once it and the
		// members before it are visible.
		merge := func(cte parser.CTE) error {
			sub, err := s.PlanParams(ctx, cte.Query)
			if err != nil {
				return err
			}
			for i := range sub {
				if i < len(fams) && fams[i] == types.Unknown {
					fams[i] = sub[i]
				}
			}
			return nil
		}
		restore, err := s.bindWith(ctx, nil, with, nil, true, merge)
		if err != nil {
			return nil, ToSQLError(err)
		}
		defer restore()
		stmt = stmtWithout(stmt)
	}
	if ins, ok := stmt.(*parser.Insert); ok && ins.Select != nil {
		sub, err := s.PlanParams(ctx, ins.Select)
		if err != nil {
			return nil, err
		}
		for i := range sub {
			if i < len(fams) && fams[i] == types.Unknown {
				fams[i] = sub[i]
			}
		}
	}
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
				if strings.Contains(cmp.Op, " ANY") || strings.Contains(cmp.Op, " ALL") || cmp.Op == "@>" || cmp.Op == "NOT @>" || cmp.Op == "&&" || cmp.Op == "NOT &&" {
					// = ANY($1), @> $1: the parameter is an array of the
					// column's type (of the column's array type for @>).
					if !typ.IsArray() && typ != types.Jsonb {
						typ = types.ArrayOf(typ)
					}
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
		if oc := t.OnConflict; oc != nil {
			for _, set := range oc.Set {
				if col, ok := desc.Col(set.Column); ok {
					assign(set.Value, col.Type)
				}
			}
			fromWhere(desc, oc.Where)
		}
	case *parser.Select:
		for _, idx := range []int{t.LimitParam, t.OffsetParam} {
			if idx > 0 && fams[idx-1] == types.Unknown {
				fams[idx-1] = types.Int
			}
		}
		// A parameter that is a function's argument takes the declared
		// family (pg_cancel_backend($1) binds an integer).
		var fromCalls func(e parser.Expr)
		fromCalls = func(e parser.Expr) {
			if e.Func != "" {
				for i, a := range e.Args {
					fam := types.Unknown
					switch e.Func {
					case "pg_cancel_backend", "pg_terminate_backend":
						fam = types.Int
					case "pg_sleep":
						fam = types.Float
					default:
						if b, ok := builtins.Lookup(e.Func); ok {
							if f := b.ArgFamily(i); f != builtins.Any {
								fam = f
							}
						}
					}
					if fam != types.Unknown && a.Param > 0 && fams[a.Param-1] == types.Unknown {
						fams[a.Param-1] = fam
					}
					fromCalls(a)
				}
			}
			if e.Left != nil {
				fromCalls(*e.Left)
			}
			if e.Right != nil {
				fromCalls(*e.Right)
			}
		}
		for _, se := range t.Exprs {
			fromCalls(se.Expr)
		}
		// Every set-operation member types its own WHERE; a derived table
		// types as a statement of its own.
		if hasDerivedJoin(t) {
			bound, restore, err := s.bindJoinedDerived(ctx, nil, t, nil, true)
			if err != nil {
				return nil, ToSQLError(err)
			}
			defer restore()
			t = bound
		}
		for m := t; m != nil; m = m.Union {
			if m.Derived != nil {
				sub, err := s.PlanParams(ctx, m.Derived)
				if err != nil {
					return nil, err
				}
				for i := range sub {
					if i < len(fams) && fams[i] == types.Unknown {
						fams[i] = sub[i]
					}
				}
			}
			if m.Table == "" {
				continue
			}
			desc, err := s.lookupForPlan(ctx, m.Table)
			if err != nil {
				return nil, ToSQLError(err)
			}
			fromWhere(desc, m.Where)
			if len(m.Joins) > 0 {
				// Qualified references resolve against any join side.
				if sides, jerr := s.planJoinSides(ctx, desc, m); jerr == nil {
					for _, cmp := range m.Where {
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
		if t.Union != nil {
			// A set operation's output is named by its head and typed by
			// unifying every member (the numeric families lift, anything
			// else meets as text).
			head := *t
			head.Union, head.OrderBy, head.Limit, head.Offset = nil, nil, -1, 0
			cols, err := s.PlanColumns(ctx, &head)
			if err != nil {
				return nil, err
			}
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
				for i := range cols {
					cols[i].Type = unifyFamily(cols[i].Type, mc[i].Type)
				}
			}
			return cols, nil
		}
		if t.Derived != nil || t.FuncTable != nil {
			var desc *catalog.TableDescriptor
			if t.FuncTable != nil {
				desc = funcTableDesc(t)
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
				if fam == types.Unknown {
					fam = types.String
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
			// Subqueries in the select list are evaluated per row at
			// execution (text on the wire): describe them as such.
			t = maskSubqueryExprs(t)
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
				cols[i] = ResultColumn{Name: oc.name, Type: oc.typ}
			}
			return cols, nil
		}
		// Execution addresses a single table's columns by bare name.
		stripTableAlias(t)
		proj, perr := resolveProjection(desc, maskSubqueryExprs(t).Exprs)
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
		return s.planReturning(ctx, t.Table, t.Returning)
	case *parser.Delete:
		return s.planReturning(ctx, t.Table, t.Returning)

	case *parser.Explain:
		return []ResultColumn{{Name: "plan", Type: types.String}}, nil

	case *parser.ShowTables:
		return []ResultColumn{{Name: "table_name", Type: types.String}}, nil

	case *parser.Show:
		names := map[string][]string{
			"columns":  {"column_name", "data_type", "is_nullable", "column_default", "indices"},
			"indexes":  {"table_name", "index_name", "non_unique", "seq_in_index", "column_name"},
			"create":   {"table_name", "create_statement"},
			"views":    {"view_name", "definition"},
			"users":    {"username", "is_admin", "member_of"},
			"roles":    {"role_name", "can_login", "is_admin", "member_of"},
			"grants":   {"database_name", "schema_name", "relation_name", "grantee", "privilege_type", "is_grantable"},
			"all":      {"name", "setting"},
			"sessions": {"pid", "user_name", "database", "application_name", "client_addr", "state", "query", "backend_start", "query_start", "xact_start"},
		}[t.Kind]
		cols := make([]ResultColumn, len(names))
		for i, n := range names {
			cols[i] = ResultColumn{Name: n, Type: types.String}
		}
		if t.Kind == "sessions" {
			cols[0].Type = types.Int
			for i := 7; i < 10; i++ {
				cols[i].Type = types.Timestamp
			}
		}
		return cols, nil

	case *parser.ShowFunctions:
		return execShowFunctions().Columns, nil
	case *parser.ShowSequences:
		return []ResultColumn{
			{Name: "sequence_name", Type: types.String}, {Name: "start", Type: types.Int}, {Name: "increment", Type: types.Int},
			{Name: "min_value", Type: types.Int}, {Name: "max_value", Type: types.Int}, {Name: "cycle", Type: types.Bool},
			{Name: "cache", Type: types.Int}, {Name: "last_value", Type: types.Int}, {Name: "owned_by", Type: types.String},
		}, nil

	case *parser.ShowStats:
		return []ResultColumn{
			{Name: "table_name", Type: types.String},
			{Name: "row_count", Type: types.Int},
			{Name: "collected_at", Type: types.Timestamp},
			{Name: "column_name", Type: types.String},
			{Name: "distinct_count", Type: types.Int},
			{Name: "null_count", Type: types.Int},
		}, nil

	case *parser.Analyze:
		return nil, nil

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
// untouched), for describing a statement without evaluating it.
func maskSubqueryExprs(t *parser.Select) *parser.Select {
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
