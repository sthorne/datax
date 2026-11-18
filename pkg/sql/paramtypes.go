package sql

import (
	"context"
	"strings"

	"github.com/sthorne/datax/pkg/sql/builtins"
	"github.com/sthorne/datax/pkg/sql/catalog"
	"github.com/sthorne/datax/pkg/sql/parser"
	"github.com/sthorne/datax/pkg/sql/types"
)

// PlanParams infers the type of each $N parameter from where it stands,
// for the wire's ParameterDescription: compared with a column (WHERE
// col = $1, a.at < $1 in a join, inside OR groups, CASE conditions,
// subqueries of any depth, HAVING), assigned to one (SET col = $1,
// INSERT VALUES ($1), ON CONFLICT ... SET), cast ($1::int8), a LIMIT
// or OFFSET count, a function's declared argument, or a vector
// distance's operand. A parameter nothing types stays Unknown and is
// described as TEXT.
//
// As in PostgreSQL, a parameter used in several places has one type:
// two positions that each fix a different type (an INT8 column and a
// TEXT column, say) are an error, 42P08. Positions that only suggest a
// type (a function argument, an operand inside arithmetic) never
// override one a column fixed, and numeric (or date and timestamp)
// columns of different widths agree on the first.
func (s *Session) PlanParams(ctx context.Context, stmt parser.Statement) ([]types.Family, *Error) {
	n := parser.CountParams(stmt)
	if n == 0 {
		return nil, nil
	}
	expanded, err := s.expandViews(ctx, nil, stmt)
	if err != nil {
		return nil, ToSQLError(err)
	}
	pt := newParamTyper(n, func(name string) (*catalog.TableDescriptor, error) {
		return s.lookupForPlan(ctx, name)
	})
	pt.describe = func(q parser.Statement) ([]ResultColumn, error) {
		cols, serr := s.PlanColumns(ctx, q)
		if serr != nil {
			return nil, serr
		}
		return cols, nil
	}
	pt.bind = func(name string, cols []ResultColumn, rename []string) (func(), error) {
		desc, err := relationDesc(name, cols, rename)
		if err != nil {
			return nil, err
		}
		prev, had := s.bindRelation(name, &relation{desc: desc})
		return func() {
			if had {
				s.restoreRelations(map[string]*relation{name: prev})
			} else {
				s.restoreRelations(map[string]*relation{name: nil})
			}
		}, nil
	}
	pt.bindMember = func(sel *parser.Select, i int) (func(), error) {
		jc := sel.Joins[i]
		var cols []ResultColumn
		var lateral *parser.Select
		if isLateralMember(jc) {
			scope, err := s.lateralScope(ctx, nil, sel, i)
			if err != nil {
				return nil, err
			}
			lateral = lateralSub(jc)
			if cols, err = s.describeLateral(ctx, nil, lateral, []scopeLevel{scope}); err != nil {
				return nil, err
			}
		} else {
			var err error
			if cols, err = tableFuncColumns(*jc.FuncTable, jc.Alias, jc.FuncCols); err != nil {
				return nil, err
			}
		}
		name := strings.ToLower(jc.Alias)
		desc, err := relationDesc(name, cols, nil)
		if err != nil {
			return nil, err
		}
		prev, had := s.bindRelation(name, &relation{desc: desc, lateral: lateral})
		if !had {
			prev = nil
		}
		return func() { s.restoreRelations(map[string]*relation{name: prev}) }, nil
	}
	if err := pt.statement(expanded, nil, true); err != nil {
		return nil, ToSQLError(err)
	}
	return pt.fams, nil
}

// paramTyper carries the inference of one statement's parameter types.
// Its hooks reach the catalog: lookup resolves a table (or a bound
// relation), describe a query's output columns, and bind makes a WITH
// member or a joined subquery visible by name while the statement is
// typed. describe and bind may be nil (a derived table's columns then
// type nothing).
type paramTyper struct {
	fams []types.Family
	// fixed marks a family a column fixed (see PlanParams); a suggested
	// family can be replaced by a fixed one, never the reverse.
	fixed    []bool
	lookup   func(name string) (*catalog.TableDescriptor, error)
	describe func(q parser.Statement) ([]ResultColumn, error)
	bind     func(name string, cols []ResultColumn, rename []string) (func(), error)
	// bindMember binds a select's join member i — a table function
	// call, or a lateral member reading the members to its left — as a
	// relation named by its alias (nil: such members type nothing).
	bindMember func(sel *parser.Select, i int) (func(), error)
	err        error
}

func newParamTyper(n int, lookup func(string) (*catalog.TableDescriptor, error)) *paramTyper {
	return &paramTyper{fams: make([]types.Family, n), fixed: make([]bool, n), lookup: lookup}
}

// paramScope resolves column references for one query level, falling
// back to the enclosing level's (a correlated reference).
type paramScope struct {
	parent  *paramScope
	resolve func(name string) (types.Family, bool)
}

func (sc *paramScope) column(name string) (types.Family, bool) {
	for s := sc; s != nil; s = s.parent {
		if s.resolve == nil {
			continue
		}
		if f, ok := s.resolve(name); ok {
			return f, true
		}
	}
	return types.Unknown, false
}

// tableScope resolves a single table's columns, bare or qualified by
// its alias or its (database-qualified or bare) name.
func tableScope(parent *paramScope, desc *catalog.TableDescriptor, table, alias string) *paramScope {
	_, bare := catalog.SplitTableName(table)
	return &paramScope{parent: parent, resolve: func(name string) (types.Family, bool) {
		q, col := "", name
		if i := strings.LastIndexByte(name, '.'); i >= 0 {
			q, col = name[:i], name[i+1:]
		}
		if q != "" && q != alias && q != table && q != bare && q != desc.Name && q != "excluded" {
			return types.Unknown, false
		}
		if c, ok := desc.Col(col); ok {
			return c.Type, true
		}
		return types.Unknown, false
	}}
}

// columnsScope resolves the output columns of a described query (a
// derived table) by name, bare or qualified by alias.
func columnsScope(parent *paramScope, cols []ResultColumn, alias string) *paramScope {
	return &paramScope{parent: parent, resolve: func(name string) (types.Family, bool) {
		q, col := splitQualified(name)
		if q != "" && q != alias {
			return types.Unknown, false
		}
		if q == "" {
			col = name
		}
		for _, c := range cols {
			if strings.EqualFold(c.Name, col) {
				return c.Type, true
			}
		}
		return types.Unknown, false
	}}
}

// set records family fam for parameter n (1-based). fixed is a column's
// type; otherwise the family is only a suggestion.
func (pt *paramTyper) set(n int, fam types.Family, fixed bool) {
	if n <= 0 || n > len(pt.fams) || fam == types.Unknown || pt.err != nil {
		return
	}
	i := n - 1
	cur := pt.fams[i]
	switch {
	case cur == types.Unknown:
		pt.fams[i], pt.fixed[i] = fam, fixed
	case cur == fam:
		pt.fixed[i] = pt.fixed[i] || fixed
	case !fixed:
		// A suggestion never changes a family already chosen.
	case !pt.fixed[i]:
		pt.fams[i], pt.fixed[i] = fam, true
	case paramFamiliesAgree(cur, fam):
		// The first column's type stands; the comparison converts.
	default:
		pt.err = newErrf(CodeAmbiguousParameter, "inconsistent types deduced for parameter $%d: %s versus %s",
			n, strings.ToLower(cur.String()), strings.ToLower(fam.String()))
	}
}

// paramFamiliesAgree reports whether one parameter can stand for both
// families: numbers of any width, a date and a timestamp, text and an
// enum label.
func paramFamiliesAgree(a, b types.Family) bool {
	group := func(f types.Family) int {
		switch f {
		case types.Int, types.Float, types.Decimal:
			return 1
		case types.Timestamp, types.Date:
			return 2
		case types.String, types.Enum:
			return 3
		}
		return 0
	}
	return group(a) != 0 && group(a) == group(b)
}

// value types a value expression standing where fam is expected: a bare
// parameter takes it (fixed when fixed), a parameter inside arithmetic,
// CASE or a call is only suggested it.
func (pt *paramTyper) value(e parser.Expr, fam types.Family, fixed bool) {
	if fam == types.Unknown {
		return
	}
	if e.Param > 0 && e.Cast != "" {
		return // $1::type: the cast fixed it (see expr)
	}
	if e.Param > 0 && e.BinOp == "" && e.Func == "" && len(e.Path) == 0 {
		pt.set(e.Param, fam, fixed)
		return
	}
	var suggest func(e parser.Expr)
	suggest = func(e parser.Expr) {
		if e.Param > 0 && e.Cast == "" {
			pt.set(e.Param, fam, false)
		}
		if e.Left != nil {
			suggest(*e.Left)
		}
		if e.Right != nil {
			suggest(*e.Right)
		}
		for _, a := range e.Args {
			suggest(a)
		}
	}
	suggest(e)
}

// statement types one statement; top marks the statement itself, whose
// own table must exist (a nested query's lookup failure only leaves its
// columns untyped — execution reports it).
func (pt *paramTyper) statement(stmt parser.Statement, parent *paramScope, top bool) error {
	switch t := stmt.(type) {
	case *parser.Select:
		pt.selectStmt(t, parent, top)
	case *parser.Insert:
		restore := pt.with(t.With, parent)
		defer restore()
		desc, err := pt.lookup(t.Table)
		if err != nil {
			if top {
				return err
			}
			pt.walk(t, parent)
			break
		}
		target := desc.VisibleColumns()
		if len(t.Columns) > 0 {
			target = make([]catalog.Column, len(t.Columns))
			for i, name := range t.Columns {
				target[i], _ = desc.Col(name)
			}
		}
		for _, row := range t.Rows {
			for i, e := range row {
				if i < len(target) {
					pt.value(e, target[i].Type, true)
				}
			}
		}
		if t.Select != nil {
			// INSERT ... SELECT $1, $2: a select item is a value for its
			// target column, as a VALUES item is.
			for m := t.Select; m != nil; m = m.Union {
				for i, se := range m.Exprs {
					if i < len(target) && !se.Star && se.Agg == "" {
						pt.value(se.Expr, target[i].Type, true)
					}
				}
			}
			pt.selectStmt(t.Select, parent, false)
		}
		sc := tableScope(parent, desc, t.Table, "")
		if oc := t.OnConflict; oc != nil {
			for _, set := range oc.Set {
				if col, ok := desc.Col(set.Column); ok {
					pt.value(set.Value, col.Type, true)
				}
			}
		}
		c := *t
		c.With, c.Select = nil, nil
		pt.walk(&c, sc)
	case *parser.Update:
		restore := pt.with(t.With, parent)
		defer restore()
		desc, err := pt.lookup(t.Table)
		if err != nil {
			if top {
				return err
			}
			pt.walk(t, parent)
			break
		}
		for _, set := range t.Set {
			if col, ok := desc.Col(set.Column); ok {
				pt.value(set.Value, col.Type, true)
			}
		}
		if len(t.From) > 0 {
			// The join of the target with the FROM list types the WHERE,
			// the SET values' subqueries and RETURNING, as a select's.
			var exprs []parser.SelectExpr
			for _, set := range t.Set {
				if !set.Value.IsDefault {
					exprs = append(exprs, parser.SelectExpr{Expr: set.Value})
				}
			}
			exprs = append(exprs, t.Returning...)
			pt.selectStmt(dmlFromSelect(t.Table, t.Alias, t.From, t.Where, exprs, true), parent, false)
			break
		}
		c := *t
		c.With = nil
		pt.walk(&c, tableScope(parent, desc, t.Table, t.Alias))
	case *parser.Delete:
		restore := pt.with(t.With, parent)
		defer restore()
		desc, err := pt.lookup(t.Table)
		if err != nil {
			if top {
				return err
			}
			pt.walk(t, parent)
			break
		}
		if len(t.Using) > 0 {
			exprs := t.Returning
			if len(exprs) == 0 {
				one := types.NewInt(1)
				exprs = []parser.SelectExpr{{Expr: parser.Expr{Lit: &one}}}
			}
			pt.selectStmt(dmlFromSelect(t.Table, t.Alias, t.Using, t.Where, exprs, true), parent, false)
			break
		}
		c := *t
		c.With = nil
		pt.walk(&c, tableScope(parent, desc, t.Table, t.Alias))
	case *parser.Explain:
		return pt.statement(t.Stmt, parent, top)
	case *parser.CreateTable:
		if t.As != nil {
			pt.selectStmt(t.As, parent, false)
		}
	case *parser.AlterTable:
		// PURGE WHERE key = $1: the parameter takes the key column's
		// type.
		if t.PurgeWhere != nil {
			if desc, err := pt.lookup(t.Table); err == nil {
				pt.walk(t, tableScope(parent, desc, t.Table, ""))
				break
			}
		}
		pt.walk(stmt, parent)
	default:
		pt.walk(stmt, parent)
	}
	return pt.err
}

// with types a statement's WITH members in order and binds each (by
// shape) so the members after it and the statement see its columns. The
// returned function removes the bindings.
func (pt *paramTyper) with(ctes []parser.CTE, parent *paramScope) func() {
	var undo []func()
	restore := func() {
		for i := len(undo) - 1; i >= 0; i-- {
			undo[i]()
		}
	}
	for _, cte := range ctes {
		name := strings.ToLower(cte.Name)
		shape := cte.Query
		if sel, ok := cte.Query.(*parser.Select); ok && cte.Recursive && sel.Union != nil {
			seed := *sel
			seed.Union, seed.SetOp, seed.UnionAll = nil, "", false
			shape = &seed
			// The recursive step sees the member itself.
			if r := pt.bindDescribed(name, shape, cte.Columns); r != nil {
				undo = append(undo, r)
			}
			_ = pt.statement(cte.Query, parent, false)
			continue
		}
		_ = pt.statement(cte.Query, parent, false)
		if r := pt.bindDescribed(name, shape, cte.Columns); r != nil {
			undo = append(undo, r)
		}
	}
	return restore
}

// bindDescribed binds name to q's output columns; nil when it cannot.
func (pt *paramTyper) bindDescribed(name string, q parser.Statement, rename []string) func() {
	if pt.describe == nil || pt.bind == nil {
		return nil
	}
	cols, err := pt.describe(q)
	if err != nil || cols == nil {
		return nil
	}
	r, err := pt.bind(name, cols, rename)
	if err != nil {
		return nil
	}
	return r
}

// selectStmt types a select and its set-operation members.
func (pt *paramTyper) selectStmt(sel *parser.Select, parent *paramScope, top bool) {
	restore := pt.with(sel.With, parent)
	defer restore()
	for m := sel; m != nil; m = m.Union {
		pt.member(m, parent, top && m == sel)
	}
}

// member types one select member: it builds the member's scope (its
// table, join sides or derived table), then visits every expression
// position of the member, nested queries typing with this scope as
// their enclosing one.
func (pt *paramTyper) member(m *parser.Select, parent *paramScope, top bool) {
	for _, n := range []int{m.LimitParam, m.OffsetParam} {
		pt.set(n, types.Int, true)
	}
	c := *m
	c.With, c.Union, c.Derived = nil, nil, nil
	var undo []func()
	defer func() {
		for i := len(undo) - 1; i >= 0; i-- {
			undo[i]()
		}
	}()
	sc := &paramScope{parent: parent}
	if len(m.Joins) > 0 {
		c.Joins = append([]parser.JoinClause(nil), m.Joins...)
		for i := range c.Joins {
			jc := &c.Joins[i]
			if jc.Derived == nil || isLateralMember(*jc) {
				continue
			}
			pt.selectStmt(jc.Derived, parent, false)
			name := strings.ToLower(jc.Alias)
			if r := pt.bindDescribed(name, jc.Derived, nil); r != nil {
				undo = append(undo, r)
			}
			jc.Derived, jc.Table = nil, name
		}
	}
	switch {
	case m.Derived != nil:
		pt.selectStmt(m.Derived, parent, false)
		if pt.describe != nil {
			if cols, err := pt.describe(m.Derived); err == nil {
				if len(m.Joins) > 0 && pt.bind != nil {
					// A joined derived table runs as a relation named
					// by its alias.
					name := strings.ToLower(m.Alias)
					if r, err := pt.bind(name, cols, nil); err == nil {
						undo = append(undo, r)
						c.Table = name
					}
				} else {
					sc = columnsScope(parent, cols, m.Alias)
				}
			}
		}
	}
	// Table functions and lateral members bind in order, once the
	// members to their left are; their queries stay in place for the
	// walk below, which types a lateral subquery's parameters with the
	// join's scope enclosing it.
	for i := range c.Joins {
		jc := &c.Joins[i]
		if pt.bindMember == nil || c.Table == "" || jc.FuncTable == nil && !isLateralMember(*jc) {
			continue
		}
		if r, err := pt.bindMember(&c, i); err == nil {
			undo = append(undo, r)
			jc.Table = strings.ToLower(jc.Alias)
		}
	}
	if c.Table != "" {
		if desc, err := pt.lookup(c.Table); err == nil {
			if len(c.Joins) == 0 {
				sc = tableScope(parent, desc, c.Table, c.Alias)
			} else if sides := pt.joinSides(desc, &c); sides != nil {
				sc = &paramScope{parent: parent, resolve: func(name string) (types.Family, bool) {
					ref, err := resolveJoinRef(sides, name)
					if err != nil {
						return types.Unknown, false
					}
					return ref.col.Type, true
				}}
			}
		} else if top && pt.err == nil {
			pt.err = err
		}
	}
	// HAVING's conjuncts type like WHERE's, an aggregate call on the
	// left by its result type (exprFamily).
	pt.walkSelect(&c, sc)
}

// joinSides resolves a join member's sides for column lookup; nil when
// a side cannot be resolved.
func (pt *paramTyper) joinSides(base *catalog.TableDescriptor, t *parser.Select) []joinSide {
	inner := make([]*catalog.TableDescriptor, len(t.Joins))
	for i := range t.Joins {
		if t.Joins[i].Table == "" {
			return nil
		}
		d, err := pt.lookup(t.Joins[i].Table)
		if err != nil {
			return nil
		}
		inner[i] = d
	}
	sides, err := makeJoinSides(base, inner, t)
	if err != nil {
		return nil
	}
	if _, err := expandUsing(sides, t); err != nil {
		return nil
	}
	return sides
}

// walkSelect visits the expression positions of one member (already
// stripped of its WITH, set-operation and derived parts).
func (pt *paramTyper) walkSelect(m *parser.Select, sc *paramScope) {
	w := pt.walker(sc, m)
	w.SelectStmt(m)
}

// walk visits a whole statement's expression positions with scope sc.
func (pt *paramTyper) walk(stmt parser.Statement, sc *paramScope) {
	w := pt.walker(sc, nil)
	w.Statement(stmt)
}

// walker returns the visitor that types each position in scope sc;
// self is the member being walked (a nested select types as a query of
// its own, enclosed by sc).
func (pt *paramTyper) walker(sc *paramScope, self *parser.Select) *parser.Walker {
	return &parser.Walker{
		OnSelect: func(s *parser.Select) bool {
			if s == self {
				return true
			}
			restore := pt.with(s.With, sc)
			pt.member(s, sc, false)
			restore()
			return false
		},
		OnCond: func(c *parser.Comparison) bool {
			pt.cond(*c, sc)
			return true
		},
		OnExpr: func(e *parser.Expr) bool {
			pt.expr(*e, sc)
			return true
		},
	}
}

// cond types the values a conjunct compares with its left-hand side:
// the column's type (an array of it for = ANY / @>, the other
// text-search type for @@), or a computed left-hand side's.
func (pt *paramTyper) cond(c parser.Comparison, sc *paramScope) {
	fam, fixed := types.Unknown, false
	switch {
	case c.Column != "":
		f, ok := sc.column(c.Column)
		if !ok {
			return
		}
		fam, fixed = f, true
		if len(c.Path) > 0 {
			fam = pathResultType(c.Path)
		}
	case c.Expr != nil:
		lhs := *c.Expr
		if lhs.Param > 0 && lhs.BinOp == "" && lhs.Func == "" {
			// $1 = col: the parameter takes the other side's type.
			if f := exprFamily(c.Value, sc.column); f != types.Unknown {
				pt.set(lhs.Param, f, c.Value.Column != "" && c.Value.BinOp == "")
			}
			return
		}
		fam = exprFamily(lhs, sc.column)
		fixed = lhs.Column != "" && lhs.BinOp == "" && lhs.Func == ""
	default:
		return
	}
	if fam == types.Unknown {
		return
	}
	op := c.Op
	if strings.Contains(op, " ANY") || strings.Contains(op, " ALL") || op == "@>" || op == "NOT @>" || op == "&&" || op == "NOT &&" {
		if !fam.IsArray() && fam != types.Jsonb {
			fam = types.ArrayOf(fam)
		}
	}
	switch op {
	case "@@", "NOT @@":
		switch fam {
		case types.Tsvector:
			fam = types.Tsquery
		case types.Tsquery:
			fam = types.Tsvector
		}
	case "~", "!~", "~*", "!~*", "LIKE", "NOT LIKE", "ILIKE", "NOT ILIKE", "SIMILAR TO", "NOT SIMILAR TO":
		// A pattern is text whatever the column.
		fam, fixed = types.String, false
	}
	pt.value(c.Value, fam, fixed)
	for _, v := range c.Values {
		pt.value(v, fam, fixed)
	}
}

// expr types the parameters an expression node itself places: a
// function's declared argument families and a vector distance's
// operands. These are suggestions; a column comparison overrides them.
func (pt *paramTyper) expr(e parser.Expr, sc *paramScope) {
	if e.Param > 0 && e.Cast != "" {
		// $1::int8: the parameter is of the type it is cast to, as in
		// PostgreSQL.
		pt.set(e.Param, builtins.CastFamily(e.Cast, types.Unknown), true)
	}
	if e.Func != "" {
		for i, a := range e.Args {
			if a.Param == 0 {
				continue
			}
			fam := types.Unknown
			switch e.Func {
			case "pg_cancel_backend", "pg_terminate_backend":
				fam = types.Int
			case "pg_sleep":
				fam = types.Float
			case destroyTenantKeyFunc, rotateTenantKeyFunc, destroyOldKeyVersionsFunc, removeTenantRowsFunc:
				// (table text, tenant [, keep_old bool]): the tenant is
				// coerced to the table's tenant column, so it binds as
				// text.
				fam = types.String
				if i == 2 {
					fam = types.Bool
				}
			case "datax_key_wrap", "datax_key_unwrap", "datax_encrypt", "datax_decrypt":
				// (name text, data bytea, context bytea)
				fam = types.Bytes
				if i == 0 {
					fam = types.String
				}
			default:
				if b, ok := builtins.Lookup(e.Func); ok {
					if f := b.ArgFamily(i); f != builtins.Any {
						fam = f
					}
				}
			}
			pt.set(a.Param, fam, false)
		}
	}
	// A parameter that is an operand of arithmetic with a number takes
	// the number's type, as PostgreSQL resolves an unknown operand
	// (ORDER BY abs(x - $1) inside an aggregate, SELECT price * $1).
	if e.Right != nil && (e.BinOp == "+" || e.BinOp == "-" || e.BinOp == "*" || e.BinOp == "/" || builtins.IsBitwiseOp(e.BinOp)) {
		lhs := e
		lhs.BinOp, lhs.Right, lhs.Cast = "", nil, ""
		bareParam := func(x parser.Expr) bool {
			return x.Param > 0 && x.BinOp == "" && x.Func == "" && x.Cast == "" && len(x.Path) == 0 && x.Left == nil
		}
		numeric := func(f types.Family) bool { return f == types.Int || f == types.Float || f == types.Decimal }
		if bareParam(*e.Right) {
			if f := exprFamily(lhs, sc.column); numeric(f) {
				pt.set(e.Right.Param, f, false)
			}
		}
		if bareParam(lhs) {
			if f := exprFamily(*e.Right, sc.column); numeric(f) {
				pt.set(lhs.Param, f, false)
			}
		}
	}
	// A parameter that is an operand of a vector distance is a vector
	// (ORDER BY embedding <-> $1), wherever the distance is written: a
	// client that knows the type then sends it in binary.
	if builtins.IsVectorOp(e.BinOp) && e.Right != nil {
		pt.set(e.Param, types.Vector, false)
		pt.set(e.Right.Param, types.Vector, false)
	}
}
