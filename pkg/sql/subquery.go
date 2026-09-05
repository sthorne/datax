package sql

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/sthorne/datax/pkg/kvclient"
	"github.com/sthorne/datax/pkg/sql/catalog"
	"github.com/sthorne/datax/pkg/sql/parser"
	"github.com/sthorne/datax/pkg/sql/types"
	"github.com/sthorne/datax/pkg/sql/vtable"
	"github.com/sthorne/datax/pkg/version"
)

// Uncorrelated subqueries: the inner SELECT is evaluated first, within the
// same transaction, and its result is spliced in as a literal (scalar), a
// value list ([NOT] IN), or a constant conjunct ([NOT] EXISTS). The parsed
// AST is never mutated — resolution builds modified copies — so a prepared
// statement re-evaluates its subqueries on every execution.

// execSubSelect executes a subquery. A column that fails to resolve inside
// the subquery's own scope is (almost always) a correlated reference,
// which is not supported.
func (s *Session) execSubSelect(ctx context.Context, txn *kvclient.Txn, sub *parser.Select, params []types.Datum) (*Result, error) {
	res, err := s.execSelect(ctx, txn, sub, params)
	if err != nil {
		if serr, ok := err.(*Error); ok && serr.Code == CodeUndefinedColumn {
			return nil, newErrf(CodeFeatureNotSupported, "correlated subqueries are not supported (%s)", serr.Msg)
		}
		return nil, err
	}
	return res, nil
}

// execScalarSub evaluates (SELECT ...) used as a value: exactly one output
// column; zero rows is NULL; more than one row is an error.
func (s *Session) execScalarSub(ctx context.Context, txn *kvclient.Txn, sub *parser.Select, params []types.Datum) (types.Datum, error) {
	res, err := s.execSubSelect(ctx, txn, sub, params)
	if err != nil {
		return types.Datum{}, err
	}
	if len(res.Columns) != 1 {
		return types.Datum{}, newErrf(CodeSyntaxError, "subquery must return only one column")
	}
	switch len(res.Rows) {
	case 0:
		return types.DNull, nil
	case 1:
		return res.Rows[0][0], nil
	}
	return types.Datum{}, newErrf(CodeCardinality, "more than one row returned by a subquery used as an expression")
}

// resolveValueExpr splices the parts of a value expression that must be
// evaluated once, before row-by-row execution: scalar subqueries and
// now() (the statement timestamp).
func (s *Session) resolveValueExpr(ctx context.Context, txn *kvclient.Txn, e parser.Expr, params []types.Datum) (parser.Expr, error) {
	return s.resolveValueExprOpts(ctx, txn, e, params, false)
}

// resolveValueExprOpts is resolveValueExpr; keepVolatile leaves the
// volatile row functions (nextval, ...) in place for a per-row caller.
func (s *Session) resolveValueExprOpts(ctx context.Context, txn *kvclient.Txn, e parser.Expr, params []types.Datum, keepVolatile bool) (parser.Expr, error) {
	out := e
	if e.Func != "" && volatileFuncs[e.Func] && !keepVolatile {
		// nextval() and friends outside a write's per-row path (SELECT
		// nextval('s')): once per statement.
		return s.spliceVolatile(ctx, txn, e, params)
	}
	if e.Sub != nil {
		d, err := s.execScalarSub(ctx, txn, e.Sub, params)
		if err != nil {
			return e, err
		}
		out.Sub, out.Lit = nil, &d
	}
	switch e.Func {
	case "now", "current_timestamp", "localtimestamp", "statement_timestamp", "transaction_timestamp":
		d := types.NewTimestamp(s.statementTime())
		out.Func, out.Args, out.Lit = "", nil, &d
	case "current_date":
		d := types.NewDate(s.statementTime() / (86400 * 1e9))
		out.Func, out.Args, out.Lit = "", nil, &d
	case "current_database":
		d := types.NewString(s.database)
		out.Func, out.Lit = "", &d
	case "current_schema":
		d := types.NewString(catalog.PublicSchema)
		out.Func, out.Lit = "", &d
	case "current_user", "session_user":
		d := types.NewString(s.user)
		out.Func, out.Lit = "", &d
	case "version":
		d := types.NewString("PostgreSQL 14.0 datax " + version.Release)
		out.Func, out.Lit = "", &d
	case "pg_backend_pid":
		d := types.NewInt(0)
		out.Func, out.Lit = "", &d
	case "pg_get_userbyid":
		// Every object is owned by root (there is no ownership yet).
		d := types.NewString("root")
		out.Func, out.Args, out.Lit = "", nil, &d
	case "pg_table_is_visible":
		d := types.NewBool(true)
		out.Func, out.Args, out.Lit = "", nil, &d
	case "pg_partition_ancestors":
		// No partitions: a relation's only ancestor is itself. The
		// argument is an OID, as a number or a regclass literal.
		if len(e.Args) == 1 && e.Args[0].Lit != nil && !e.Args[0].Lit.Null {
			oid, err := s.regclassOID(ctx, txn, e.Args[0].Lit.Text())
			if err != nil {
				return e, err
			}
			d := types.NewInt(oid)
			out.Func, out.Args, out.Lit = "", nil, &d
		}
	case "pg_encoding_to_char":
		d := types.NewString("UTF8")
		out.Func, out.Args, out.Lit = "", nil, &d
	case "obj_description", "col_description", "shobj_description", "pg_get_statisticsobjdef_columns", "pg_get_triggerdef":
		d := types.DNull
		out.Func, out.Args, out.Lit = "", nil, &d
	case "pg_relation_is_publishable":
		d := types.NewBool(true)
		out.Func, out.Args, out.Lit = "", nil, &d
	case "array":
		// array(SELECT ...): the subquery's single column rendered as a
		// text array literal.
		if len(e.Args) == 1 && e.Args[0].Sub != nil {
			res, err := s.execSubSelect(ctx, txn, e.Args[0].Sub, params)
			if err != nil {
				return e, err
			}
			if len(res.Columns) != 1 {
				return e, newErrf(CodeSyntaxError, "subquery must return only one column")
			}
			elems := make([]string, len(res.Rows))
			for i, r := range res.Rows {
				elems[i] = arrayElemText(r[0])
			}
			d := types.NewString("{" + strings.Join(elems, ",") + "}")
			out.Func, out.Args, out.Lit = "", nil, &d
		}
	case "pg_get_viewdef":
		// pg_get_viewdef(oid [, pretty]): a literal OID (psql's \d+ view)
		// resolves to the view's stored query here; a column reference
		// reads pg_class's hidden rendering like the functions below.
		if len(e.Args) > 0 && e.Args[0].Lit != nil && !e.Args[0].Lit.Null {
			oid, err := s.regclassOID(ctx, txn, e.Args[0].Lit.Text())
			if err != nil {
				return e, err
			}
			d := types.DNull
			if desc, rerr := catalog.ReadTable(ctx, txn, uint64(oid)); rerr == nil && desc != nil && desc.IsView() {
				d = types.NewString(desc.ViewQuery)
			}
			out.Func, out.Args, out.Lit = "", nil, &d
			break
		}
		fallthrough
	case "format_type", "pg_get_indexdef", "pg_get_constraintdef", "pg_get_expr":
		// Row-dependent catalog renderings: the virtual tables carry them
		// as hidden columns beside the OID the function takes, so the
		// call becomes a column reference on the same row.
		if len(e.Args) > 0 && e.Args[0].Column != "" && (e.Func != "format_type" || len(e.Args) == 2 && e.Args[1].Column != "") {
			prefix := ""
			if i := strings.LastIndexByte(e.Args[0].Column, '.'); i >= 0 {
				prefix = e.Args[0].Column[:i+1]
			}
			out.Func, out.Args, out.Column = "", nil, prefix+vtable.HiddenColumnFor(e.Func)
		}
	}
	if e.Left != nil {
		l, err := s.resolveValueExprOpts(ctx, txn, *e.Left, params, keepVolatile)
		if err != nil {
			return e, err
		}
		out.Left = &l
	}
	if e.Right != nil {
		r, err := s.resolveValueExprOpts(ctx, txn, *e.Right, params, keepVolatile)
		if err != nil {
			return e, err
		}
		out.Right = &r
	}
	if len(out.Args) > 0 {
		args := make([]parser.Expr, len(out.Args))
		for i, a := range out.Args {
			ra, err := s.resolveValueExprOpts(ctx, txn, a, params, keepVolatile)
			if err != nil {
				return e, err
			}
			args[i] = ra
		}
		out.Args = args
	}
	if e.Cmp != nil {
		// A boolean value: its EXISTS / IN / scalar subqueries resolve
		// like a WHERE conjunct's.
		resolved, err := s.resolveWhereSubs(ctx, txn, []parser.Comparison{*e.Cmp}, params)
		if err != nil {
			return e, err
		}
		out.Cmp = &resolved[0]
	}
	if out.Cast == "regclass" && out.Lit != nil && !out.Lit.Null && out.Lit.Fam == types.String {
		// 'name'::regclass: the table's OID (a real table's, or a catalog
		// view's), 42P01 when nothing is so named; '4'::regclass is the
		// OID itself.
		oid, err := s.regclassOID(ctx, txn, out.Lit.Text())
		if err != nil {
			return e, err
		}
		d := types.NewInt(oid)
		out.Lit, out.Cast = &d, ""
	} else if out.Cast == "regclass" && out.Lit == nil {
		// oid::regclass on a column or expression (conrelid::regclass in
		// psql's constraint queries): the table's name per row, as a
		// CASE over every OID the session can see.
		names, err := s.regclassNames(ctx, txn)
		if err != nil {
			return e, err
		}
		inner := out
		inner.Cast = ""
		ce := &parser.CaseExpr{Operand: &inner, Else: &inner}
		for _, on := range names {
			oid, name := types.NewInt(on.oid), types.NewString(on.name)
			ce.Whens = append(ce.Whens, parser.CaseWhen{Value: &parser.Expr{Lit: &oid}, Result: parser.Expr{Lit: &name}})
		}
		out = parser.Expr{Case: ce}
	}
	if e.Case != nil {
		ce := *e.Case
		if ce.Operand != nil {
			op, err := s.resolveValueExprOpts(ctx, txn, *ce.Operand, params, keepVolatile)
			if err != nil {
				return e, err
			}
			ce.Operand = &op
		}
		whens := make([]parser.CaseWhen, len(ce.Whens))
		for i, w := range ce.Whens {
			nw := w
			if w.Value != nil {
				v, err := s.resolveValueExprOpts(ctx, txn, *w.Value, params, keepVolatile)
				if err != nil {
					return e, err
				}
				nw.Value = &v
			}
			if len(w.Cond) > 0 {
				c, err := s.resolveWhereSubs(ctx, txn, w.Cond, params)
				if err != nil {
					return e, err
				}
				nw.Cond = c
			}
			r, err := s.resolveValueExprOpts(ctx, txn, w.Result, params, keepVolatile)
			if err != nil {
				return e, err
			}
			nw.Result = r
			whens[i] = nw
		}
		ce.Whens = whens
		if ce.Else != nil {
			el, err := s.resolveValueExprOpts(ctx, txn, *ce.Else, params, keepVolatile)
			if err != nil {
				return e, err
			}
			ce.Else = &el
		}
		out.Case = &ce
	}
	return out, nil
}

// exprHasSub reports whether an expression needs the pre-execution
// resolve pass: a scalar subquery or a now() call anywhere inside it.
func exprHasSub(e parser.Expr) bool {
	return exprHas(e, func(x parser.Expr) bool {
		return x.Sub != nil || x.Cast != "" || (x.Func != "" && splicedFuncs[x.Func])
	})
}

// exprHasSubquery reports whether an actual (SELECT ...) appears
// anywhere inside the expression (spliced functions do not count).
func exprHasSubquery(e parser.Expr) bool {
	return exprHas(e, func(x parser.Expr) bool { return x.Sub != nil })
}

// condHas is exprHas over a conjunct: its subquery, its expressions,
// and (recursively) an OR group's disjuncts.
func condHas(c parser.Comparison, pred func(parser.Expr) bool) bool {
	if c.Sub != nil && pred(parser.Expr{Sub: c.Sub}) || exprHas(c.Value, pred) || c.Expr != nil && exprHas(*c.Expr, pred) {
		return true
	}
	for _, v := range c.Values {
		if exprHas(v, pred) {
			return true
		}
	}
	for _, d := range c.Or {
		for _, inner := range d {
			if condHas(inner, pred) {
				return true
			}
		}
	}
	return false
}

func exprHas(e parser.Expr, pred func(parser.Expr) bool) bool {
	if pred(e) {
		return true
	}
	if e.Cmp != nil && condHas(*e.Cmp, pred) {
		return true
	}
	if e.Case != nil {
		if e.Case.Operand != nil && exprHas(*e.Case.Operand, pred) {
			return true
		}
		for _, w := range e.Case.Whens {
			if w.Value != nil && exprHas(*w.Value, pred) || exprHas(w.Result, pred) {
				return true
			}
			for _, c := range w.Cond {
				if condHas(c, pred) {
					return true
				}
			}
		}
		if e.Case.Else != nil && exprHas(*e.Case.Else, pred) {
			return true
		}
	}
	if e.Left != nil && exprHas(*e.Left, pred) {
		return true
	}
	if e.Right != nil && exprHas(*e.Right, pred) {
		return true
	}
	for _, a := range e.Args {
		if exprHas(a, pred) {
			return true
		}
	}
	return false
}

// cmpNeedsResolve reports whether a conjunct (or, recursively, an OR
// group) contains anything the pre-execution resolve pass must handle.
func cmpNeedsResolve(cmp parser.Comparison) bool {
	if cmp.Sub != nil || exprHasSub(cmp.Value) {
		return true
	}
	if cmp.Expr != nil && exprHasSub(*cmp.Expr) {
		return true
	}
	for _, ve := range cmp.Values {
		if exprHasSub(ve) {
			return true
		}
	}
	for _, d := range cmp.Or {
		for _, inner := range d {
			if cmpNeedsResolve(inner) {
				return true
			}
		}
	}
	return false
}

// resolveWhereSubs evaluates every subquery in a WHERE conjunction,
// returning a spliced copy (or the original slice when nothing changed).
func (s *Session) resolveWhereSubs(ctx context.Context, txn *kvclient.Txn, where []parser.Comparison, params []types.Datum) ([]parser.Comparison, error) {
	changed := false
	for _, cmp := range where {
		if cmpNeedsResolve(cmp) {
			changed = true
			break
		}
	}
	if !changed {
		return where, nil
	}
	out := make([]parser.Comparison, len(where))
	copy(out, where)
	for i := range out {
		cmp := &out[i]
		if len(cmp.Or) > 0 {
			// OR groups may hold scalar subqueries and now() calls:
			// resolve each disjunct recursively.
			or := make([][]parser.Comparison, len(cmp.Or))
			for j, d := range cmp.Or {
				rd, err := s.resolveWhereSubs(ctx, txn, d, params)
				if err != nil {
					return nil, err
				}
				or[j] = rd
			}
			cmp.Or = or
			continue
		}
		if exprHasSub(cmp.Value) {
			v, err := s.resolveValueExpr(ctx, txn, cmp.Value, params)
			if err != nil {
				return nil, err
			}
			cmp.Value = v
		}
		if cmp.Expr != nil && exprHasSub(*cmp.Expr) {
			v, err := s.resolveValueExpr(ctx, txn, *cmp.Expr, params)
			if err != nil {
				return nil, err
			}
			cmp.Expr = &v
		}
		switch cmp.Op {
		case "IN", "NOT IN":
			for j, ve := range cmp.Values {
				if !exprHasSub(ve) {
					continue
				}
				if &cmp.Values[0] == &where[i].Values[0] {
					cmp.Values = append([]parser.Expr(nil), where[i].Values...)
				}
				v, err := s.resolveValueExpr(ctx, txn, ve, params)
				if err != nil {
					return nil, err
				}
				cmp.Values[j] = v
			}
			if cmp.Sub != nil {
				res, err := s.execSubSelect(ctx, txn, cmp.Sub, params)
				if err != nil {
					return nil, err
				}
				if len(res.Columns) != 1 {
					return nil, newErrf(CodeSyntaxError, "subquery must return only one column")
				}
				vals := make([]parser.Expr, len(res.Rows))
				for j, r := range res.Rows {
					d := r[0]
					vals[j] = parser.Expr{Lit: &d}
				}
				cmp.Values, cmp.Sub = vals, nil
			}
		case "EXISTS", "NOT EXISTS":
			probe := *cmp.Sub
			probe.Limit = 1
			res, err := s.execSubSelect(ctx, txn, &probe, params)
			if err != nil {
				return nil, err
			}
			exists := len(res.Rows) > 0
			if cmp.Op == "NOT EXISTS" {
				exists = !exists
			}
			op := "FALSE"
			if exists {
				op = "TRUE"
			}
			*cmp = parser.Comparison{Op: op}
		}
	}
	return out, nil
}

// resolveSelectSubs evaluates the uncorrelated subqueries of a SELECT
// (select list, WHERE, HAVING values) into a spliced copy. The derived
// table (FROM subquery) is left alone — execDerivedSelect materializes it.
func (s *Session) resolveSelectSubs(ctx context.Context, txn *kvclient.Txn, t *parser.Select, params []types.Datum) (*parser.Select, error) {
	out := t
	cloned := func() *parser.Select {
		if out == t {
			c := *t
			out = &c
		}
		return out
	}

	where, err := s.resolveWhereSubs(ctx, txn, t.Where, params)
	if err != nil {
		return nil, err
	}
	if len(where) > 0 && &where[0] != &t.Where[0] {
		cloned().Where = where
	}

	for i, se := range t.Exprs {
		if !exprHasSub(se.Expr) {
			continue
		}
		v, err := s.resolveValueExpr(ctx, txn, se.Expr, params)
		if err != nil {
			return nil, err
		}
		c := cloned()
		if &c.Exprs[0] == &t.Exprs[0] {
			c.Exprs = append([]parser.SelectExpr(nil), t.Exprs...)
		}
		c.Exprs[i].Expr = v
	}

	for i, hc := range t.Having {
		if !exprHasSub(hc.Value) {
			continue
		}
		v, err := s.resolveValueExpr(ctx, txn, hc.Value, params)
		if err != nil {
			return nil, err
		}
		c := cloned()
		if &c.Having[0] == &t.Having[0] {
			c.Having = append([]parser.HavingCond(nil), t.Having...)
		}
		c.Having[i].Value = v
	}
	for i, oc := range t.OrderBy {
		if oc.Expr == nil || !exprHasSub(*oc.Expr) {
			continue
		}
		v, err := s.resolveValueExpr(ctx, txn, *oc.Expr, params)
		if err != nil {
			return nil, err
		}
		c := cloned()
		if &c.OrderBy[0] == &t.OrderBy[0] {
			c.OrderBy = append([]parser.OrderCol(nil), t.OrderBy...)
		}
		c.OrderBy[i].Expr = &v
	}
	return out, nil
}

// resolveUpdateSubs evaluates subqueries in an UPDATE's SET values and
// WHERE clause into a spliced copy.
func (s *Session) resolveUpdateSubs(ctx context.Context, txn *kvclient.Txn, t *parser.Update, params []types.Datum) (*parser.Update, error) {
	out := t
	where, err := s.resolveWhereSubs(ctx, txn, t.Where, params)
	if err != nil {
		return nil, err
	}
	if len(where) > 0 && &where[0] != &t.Where[0] {
		c := *t
		c.Where = where
		out = &c
	}
	for i, set := range t.Set {
		if !exprHasSub(set.Value) {
			continue
		}
		v, err := s.resolveValueExpr(ctx, txn, set.Value, params)
		if err != nil {
			return nil, err
		}
		if out == t {
			c := *t
			out = &c
		}
		if &out.Set[0] == &t.Set[0] {
			out.Set = append([]parser.SetClause(nil), t.Set...)
		}
		out.Set[i].Value = v
	}
	return out, nil
}

// resolveDeleteSubs evaluates subqueries in a DELETE's WHERE clause.
func (s *Session) resolveDeleteSubs(ctx context.Context, txn *kvclient.Txn, t *parser.Delete, params []types.Datum) (*parser.Delete, error) {
	where, err := s.resolveWhereSubs(ctx, txn, t.Where, params)
	if err != nil {
		return nil, err
	}
	if len(where) > 0 && &where[0] != &t.Where[0] {
		c := *t
		c.Where = where
		return &c, nil
	}
	return t, nil
}

// resolveInsertSubs evaluates scalar subqueries in INSERT values.
func (s *Session) resolveInsertSubs(ctx context.Context, txn *kvclient.Txn, t *parser.Insert, params []types.Datum) (*parser.Insert, error) {
	out := t
	for ri, row := range t.Rows {
		for ci, e := range row {
			if !exprHasSub(e) {
				continue
			}
			v, err := s.resolveValueExpr(ctx, txn, e, params)
			if err != nil {
				return nil, err
			}
			if out == t {
				c := *t
				c.Rows = make([][]parser.Expr, len(t.Rows))
				copy(c.Rows, t.Rows)
				out = &c
			}
			if &out.Rows[ri][0] == &t.Rows[ri][0] {
				out.Rows[ri] = append([]parser.Expr(nil), t.Rows[ri]...)
			}
			out.Rows[ri][ci] = v
		}
	}
	return out, nil
}

// ---------------------------------------------------------------------------
// Derived tables: FROM (SELECT ...) AS alias.

// stripAliasQualifier rewrites "alias.c" to "c" for the derived table's
// alias in a possibly-qualified column name.
func stripAliasQualifier(name, alias string) string {
	if q, col := splitQualified(name); q == alias {
		return col
	}
	return name
}

// derivedSelect returns a copy of the outer select with the derived
// table's alias stripped from every column reference, so the synthetic
// descriptor's bare column names resolve.
func derivedSelect(t *parser.Select) *parser.Select {
	c := *t
	c.Exprs = append([]parser.SelectExpr(nil), t.Exprs...)
	for i := range c.Exprs {
		if c.Exprs[i].Expr.Column != "" {
			c.Exprs[i].Expr.Column = stripAliasQualifier(c.Exprs[i].Expr.Column, t.Alias)
		}
		if c.Exprs[i].AggCol != "" {
			c.Exprs[i].AggCol = stripAliasQualifier(c.Exprs[i].AggCol, t.Alias)
		}
	}
	c.Where = append([]parser.Comparison(nil), t.Where...)
	for i := range c.Where {
		c.Where[i].Column = stripAliasQualifier(c.Where[i].Column, t.Alias)
	}
	c.GroupBy = append([]string(nil), t.GroupBy...)
	for i := range c.GroupBy {
		c.GroupBy[i] = stripAliasQualifier(c.GroupBy[i], t.Alias)
	}
	c.Having = append([]parser.HavingCond(nil), t.Having...)
	for i := range c.Having {
		if c.Having[i].Column != "" {
			c.Having[i].Column = stripAliasQualifier(c.Having[i].Column, t.Alias)
		}
	}
	c.OrderBy = append([]parser.OrderCol(nil), t.OrderBy...)
	for i := range c.OrderBy {
		c.OrderBy[i].Column = stripAliasQualifier(c.OrderBy[i].Column, t.Alias)
	}
	return &c
}

// derivedDesc builds the synthetic single-table descriptor for a
// materialized subquery result.
func derivedDesc(alias string, cols []ResultColumn) *catalog.TableDescriptor {
	desc := &catalog.TableDescriptor{Name: alias}
	for i, rc := range cols {
		desc.Columns = append(desc.Columns, catalog.Column{
			ID: catalog.ColumnID(i + 1), Name: strings.ToLower(rc.Name), Type: rc.Type,
		})
	}
	return desc
}

// execDerivedSelect materializes the FROM subquery and runs the outer
// select's pipeline (WHERE filter, grouping or projection, ORDER BY,
// DISTINCT, LIMIT) over the in-memory rows.
func (s *Session) execDerivedSelect(ctx context.Context, txn *kvclient.Txn, t *parser.Select, params []types.Datum) (*Result, error) {
	if t.ForUpdate {
		return nil, newErrf(CodeFeatureNotSupported, "FOR UPDATE is not allowed on a subquery")
	}
	inner, err := s.execSubSelect(ctx, txn, t.Derived, params)
	if err != nil {
		return nil, err
	}
	return s.execMaterialized(ctx, txn, derivedDesc(t.Alias, inner.Columns), inner.Rows, t, params)
}

// funcTableDesc is the one-column descriptor of a FROM table function.
func funcTableDesc(t *parser.Select) *catalog.TableDescriptor {
	return &catalog.TableDescriptor{Name: t.Alias, Columns: []catalog.Column{{ID: 1, Name: strings.ToLower(t.FuncCol), Type: types.String}}}
}

// execFuncTableSelect materializes FROM unnest(array): one text row per
// array element (NULL when the array is NULL), then runs the select's
// pipeline over them.
func (s *Session) execFuncTableSelect(ctx context.Context, txn *kvclient.Txn, t *parser.Select, params []types.Datum) (*Result, error) {
	e, err := s.resolveValueExpr(ctx, txn, *t.FuncTable, params)
	if err != nil {
		return nil, err
	}
	if e.Func != "unnest" || len(e.Args) != 1 {
		return nil, newErrf(CodeFeatureNotSupported, "table function %s is not supported", e.Func)
	}
	arg, err := evalExpr(e.Args[0], nil, nil, params)
	if err != nil {
		return nil, err
	}
	var rows [][]types.Datum
	if !arg.Null {
		for _, el := range arrayElems(arg.Text()) {
			d := types.NewString(el)
			if el == "NULL" {
				d = types.DNull
			}
			rows = append(rows, []types.Datum{d})
		}
	}
	return s.execMaterialized(ctx, txn, funcTableDesc(t), rows, t, params)
}

// execMaterialized runs a select's pipeline (WHERE filter, grouping or
// projection, ORDER BY, DISTINCT, LIMIT) over in-memory rows shaped by
// desc, for derived tables and table functions.
func (s *Session) execMaterialized(ctx context.Context, txn *kvclient.Txn, desc *catalog.TableDescriptor, inRows [][]types.Datum, t *parser.Select, params []types.Datum) (*Result, error) {
	t = derivedSelect(t)
	rows := make([]fetchedRow, 0, len(inRows))
	for _, r := range inRows {
		row := make(map[catalog.ColumnID]types.Datum, len(r))
		for i, d := range r {
			row[catalog.ColumnID(i+1)] = d
		}
		match, err := matchesWhere(t.Where, desc, row, params)
		if err != nil {
			return nil, err
		}
		if match {
			rows = append(rows, fetchedRow{row: row})
		}
	}

	if hasAggregates(t.Exprs) || len(t.GroupBy) > 0 {
		return s.execGroupedOver(desc, rows, t, params)
	}
	if len(t.Having) > 0 {
		return nil, newErrf(CodeGrouping, "HAVING requires GROUP BY or aggregate functions")
	}

	if len(t.OrderBy) > 0 {
		if err := sortRows(desc, rows, t.OrderBy, params); err != nil {
			return nil, err
		}
	}
	proj, perr := resolveProjection(desc, t.Exprs)
	if perr != nil {
		return nil, perr
	}
	res := &Result{}
	for _, p := range proj {
		res.Columns = append(res.Columns, ResultColumn{Name: p.name, Type: p.col.Type, Typmod: colTypmod(p.col)})
	}
	for _, fr := range rows {
		out := make([]types.Datum, len(proj))
		for i, p := range proj {
			if p.expr != nil {
				d, err := evalExpr(*p.expr, desc, fr.row, params)
				if err != nil {
					return nil, err
				}
				out[i] = d
				continue
			}
			d, ok := fr.row[p.col.ID]
			if !ok {
				d = types.DNull
			}
			out[i] = d
		}
		res.Rows = append(res.Rows, out)
	}
	if t.Distinct {
		res.Rows = dedupeRows(res.Rows)
	}
	res.Rows = trimRows(res.Rows, t)
	res.Tag = fmt.Sprintf("SELECT %d", len(res.Rows))
	return res, nil
}

// splicedFuncs are resolved by the session before execution: they
// depend on the session, the clock, or the catalog rather than the row.
var splicedFuncs = map[string]bool{
	"now": true, "current_timestamp": true, "localtimestamp": true, "statement_timestamp": true, "transaction_timestamp": true, "current_date": true,
	"current_database": true, "current_schema": true, "current_user": true, "session_user": true,
	"version": true, "pg_backend_pid": true, "pg_get_userbyid": true, "pg_table_is_visible": true, "pg_partition_ancestors": true,
	"pg_encoding_to_char": true, "obj_description": true, "col_description": true, "shobj_description": true,
	"array_to_string": true, "pg_get_viewdef": true, "current_schemas": true, "current_setting": true, "pg_get_triggerdef": true,
	"format_type": true, "pg_get_indexdef": true, "pg_get_constraintdef": true, "pg_get_expr": true,
	"pg_get_statisticsobjdef_columns": true, "pg_relation_is_publishable": true, "array": true,
	"nextval": true, "currval": true, "lastval": true, "setval": true, "unique_rowid": true, "gen_random_uuid": true,
}

// arrayElemText renders one datum as a text array element.
func arrayElemText(d types.Datum) string {
	if d.Null {
		return "NULL"
	}
	t := d.Text()
	if t == "" || strings.ContainsAny(t, ",{}\"\\ ") {
		return "\"" + strings.NewReplacer("\\", "\\\\", "\"", "\\\"").Replace(t) + "\""
	}
	return t
}

// regclassOID resolves a regclass literal: a number is the OID itself,
// a name the table's OID (42P01 when nothing is so named).
func (s *Session) regclassOID(ctx context.Context, txn *kvclient.Txn, text string) (int64, error) {
	if n, err := strconv.ParseInt(text, 10, 64); err == nil {
		return n, nil
	}
	desc, err := s.lookup(ctx, txn, text)
	if err != nil {
		return 0, newErrf(CodeUndefinedTable, "relation %q does not exist", text)
	}
	return vtable.TableOID(desc), nil
}

// regclassNames lists the (OID, name) of every table the session can
// see — real tables of every database, then the catalogs.
func (s *Session) regclassNames(ctx context.Context, txn *kvclient.Txn) ([]oidName, error) {
	all, err := s.cat.List(ctx, txn)
	if err != nil {
		return nil, err
	}
	out := make([]oidName, 0, len(all))
	for _, d := range all {
		out = append(out, oidName{oid: vtable.TableOID(d), name: d.Name})
	}
	vtable.EachTable(func(t *vtable.Table) {
		out = append(out, oidName{oid: vtable.TableOID(t.Descriptor()), name: t.Name})
	})
	return out, nil
}

type oidName struct {
	oid  int64
	name string
}
