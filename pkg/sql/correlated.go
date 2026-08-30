package sql

import (
	"context"
	"fmt"
	"strings"

	"github.com/sthorne/datax/pkg/kvclient"
	"github.com/sthorne/datax/pkg/sql/catalog"
	"github.com/sthorne/datax/pkg/sql/parser"
	"github.com/sthorne/datax/pkg/sql/types"
)

// Correlated subqueries (v1 scope: scalar / [NOT] IN / [NOT] EXISTS in
// the WHERE clause of a single-table SELECT, UPDATE, or DELETE, one
// correlation level, plain single-table inner selects).
//
// Correlation is detected STRUCTURALLY — every name in the inner select
// is resolved against the inner scope first, then the outer (PG scoping:
// an inner column shadows an outer one) — replacing the old trick of
// sniffing CodeUndefinedColumn out of the inner execution, which also
// misreported genuine typos as "correlated". A name resolving in neither
// scope is now a proper 42703.
//
// Execution is a nested loop: correlated conjuncts are stripped from the
// WHERE clause before access planning (the planner must never see them —
// it evaluates bound values without a row), the outer rows are fetched
// with the remaining conjuncts, and each fetched row then evaluates its
// correlated conjuncts by cloning the inner select with the outer
// references spliced in as literals (the parsed AST is never mutated) and
// running it through the ordinary subquery machinery. O(outer × inner),
// stated in EXPLAIN; a per-statement memo keyed by the substituted values
// makes repeated correlation keys cheap.

// corrScope is the two-scope name-resolution context.
type corrScope struct {
	outerDesc  *catalog.TableDescriptor
	outerAlias string // alias if given, else the table name
	innerDesc  *catalog.TableDescriptor
	innerAlias string
}

func (sc *corrScope) matchesOuter(q string) bool {
	return q == sc.outerAlias || q == sc.outerDesc.Name
}

func (sc *corrScope) matchesInner(q string) bool {
	return q == sc.innerAlias || q == sc.innerDesc.Name
}

// classify resolves one name: inner=false means a correlated (outer)
// reference. Inner wins for bare names present in both scopes.
func (sc *corrScope) classify(name string) (inner bool, err error) {
	q, col := splitQualified(name)
	if q != "" {
		switch {
		case sc.matchesInner(q):
			if _, ok := sc.innerDesc.Col(col); !ok {
				return false, newErrf(CodeUndefinedColumn, "column %q does not exist", name)
			}
			return true, nil
		case sc.matchesOuter(q):
			if _, ok := sc.outerDesc.Col(col); !ok {
				return false, newErrf(CodeUndefinedColumn, "column %q does not exist", name)
			}
			return false, nil
		}
		return false, newErrf(CodeUndefinedTable, "missing FROM-clause entry for table %q (a correlated reference may only reach the immediately enclosing query)", q)
	}
	if _, ok := sc.innerDesc.Col(name); ok {
		return true, nil
	}
	if _, ok := sc.outerDesc.Col(name); ok {
		return false, nil
	}
	return false, newErrf(CodeUndefinedColumn, "column %q does not exist", name)
}

// outerDatum evaluates a correlated reference against an outer row.
func (sc *corrScope) outerDatum(name string, row map[catalog.ColumnID]types.Datum) types.Datum {
	_, colName := splitQualified(name)
	col, _ := sc.outerDesc.Col(colName)
	d, ok := row[col.ID]
	if !ok {
		d = types.DNull
	}
	return d
}

// correlatedConjunct is one WHERE conjunct whose subquery references the
// outer row; it is evaluated per fetched row instead of being planned.
type correlatedConjunct struct {
	cmp   parser.Comparison
	scope corrScope
	refs  []string // correlated names, deduplicated, in first-seen order (the memo key)
	idx   int
}

// exprColumns walks a value-expression chain's column references.
func exprColumns(e parser.Expr, visit func(string)) {
	for {
		if e.Column != "" {
			visit(e.Column)
		}
		if e.Right == nil {
			return
		}
		e = *e.Right
	}
}

// innerSubShape reports whether the inner select is a shape correlation
// detection covers: a plain single-table select with no nested
// subqueries of its own (one correlation level).
func innerSubShape(sub *parser.Select) bool {
	if sub.Table == "" || len(sub.Joins) > 0 || sub.Derived != nil {
		return false
	}
	for _, cmp := range sub.Where {
		if cmp.Sub != nil || exprHasSub(cmp.Value) {
			return false
		}
		for _, ve := range cmp.Values {
			if exprHasSub(ve) {
				return false
			}
		}
	}
	for _, se := range sub.Exprs {
		if exprHasSub(se.Expr) {
			return false
		}
	}
	for _, hc := range sub.Having {
		if exprHasSub(hc.Value) {
			return false
		}
	}
	return true
}

// analyzeSub classifies every name of a covered-shape inner select,
// returning the correlated ones (nil = uncorrelated). Positions that
// cannot carry an outer reference (GROUP BY, ORDER BY, aggregate
// arguments, HAVING left-hand sides) reject with a clear error.
func (s *Session) analyzeSub(ctx context.Context, txn *kvclient.Txn, sub *parser.Select, outerDesc *catalog.TableDescriptor, outerAlias string) ([]string, *corrScope, error) {
	innerDesc, err := s.cat.Lookup(ctx, txn, sub.Table)
	if err != nil {
		return nil, nil, err
	}
	innerAlias := sub.Alias
	if innerAlias == "" {
		innerAlias = sub.Table
	}
	scope := &corrScope{outerDesc: outerDesc, outerAlias: outerAlias, innerDesc: innerDesc, innerAlias: innerAlias}

	var refs []string
	seen := map[string]bool{}
	var firstErr error
	record := func(name string, allowedOuter bool, position string) {
		if firstErr != nil {
			return
		}
		inner, err := scope.classify(name)
		if err != nil {
			firstErr = err
			return
		}
		if inner {
			return
		}
		if !allowedOuter {
			firstErr = newErrf(CodeFeatureNotSupported, "correlated reference %q is not supported in %s", name, position)
			return
		}
		if !seen[name] {
			seen[name] = true
			refs = append(refs, name)
		}
	}

	for _, cmp := range sub.Where {
		if cmp.Column != "" {
			record(cmp.Column, true, "WHERE")
		}
		exprColumns(cmp.Value, func(n string) { record(n, true, "WHERE") })
		for _, ve := range cmp.Values {
			exprColumns(ve, func(n string) { record(n, true, "WHERE") })
		}
	}
	for _, se := range sub.Exprs {
		if se.AggCol != "" && se.AggCol != "*" {
			record(se.AggCol, false, "an aggregate argument")
		}
		exprColumns(se.Expr, func(n string) { record(n, true, "the select list") })
	}
	for _, g := range sub.GroupBy {
		record(g, false, "GROUP BY")
	}
	for _, hc := range sub.Having {
		if hc.Column != "" {
			record(hc.Column, false, "HAVING")
		}
		exprColumns(hc.Value, func(n string) { record(n, true, "HAVING") })
	}
	for _, oc := range sub.OrderBy {
		record(oc.Column, false, "ORDER BY")
	}
	if firstErr != nil {
		return nil, nil, firstErr
	}
	return refs, scope, nil
}

// conjunctSubs collects the subqueries a conjunct carries (cmp.Sub plus
// any inside the value expression chain).
func conjunctSubs(cmp parser.Comparison) []*parser.Select {
	var subs []*parser.Select
	if cmp.Sub != nil {
		subs = append(subs, cmp.Sub)
	}
	for e := &cmp.Value; e != nil; e = e.Right {
		if e.Sub != nil {
			subs = append(subs, e.Sub)
		}
	}
	for _, ve := range cmp.Values {
		for e := &ve; e != nil; e = e.Right {
			if e.Sub != nil {
				subs = append(subs, e.Sub)
			}
		}
	}
	return subs
}

// splitCorrelatedWhere partitions a WHERE clause into plannable conjuncts
// and correlated ones. It must run before subquery resolution AND before
// access planning: a correlated conjunct reaching either would fail (the
// planner evaluates bound values with no row in scope).
func (s *Session) splitCorrelatedWhere(ctx context.Context, txn *kvclient.Txn, where []parser.Comparison, outerDesc *catalog.TableDescriptor, outerAlias string) (plannable []parser.Comparison, corr []correlatedConjunct, err error) {
	if outerAlias == "" {
		outerAlias = outerDesc.Name
	}
	for _, cmp := range where {
		subs := conjunctSubs(cmp)
		correlated := false
		if len(subs) > 0 {
			var refs []string
			var scope *corrScope
			covered := 0
			for _, sub := range subs {
				if !innerSubShape(sub) {
					continue // uncovered shape: leave to the eager path (and its conservative error)
				}
				covered++
				r, sc, aerr := s.analyzeSub(ctx, txn, sub, outerDesc, outerAlias)
				if aerr != nil {
					return nil, nil, aerr
				}
				if len(r) > 0 {
					refs, scope = r, sc
				}
			}
			if len(refs) > 0 {
				if len(subs) > 1 {
					return nil, nil, newErrf(CodeFeatureNotSupported, "a conjunct may carry only one correlated subquery")
				}
				if covered != len(subs) {
					return nil, nil, newErrf(CodeFeatureNotSupported, "correlated subqueries are only supported over plain single-table selects")
				}
				// The correlated scalar subquery must head the value chain
				// (col = (SELECT ...) [+ tail]); deeper positions are out of
				// scope.
				if cmp.Sub == nil && cmp.Value.Sub == nil {
					return nil, nil, newErrf(CodeFeatureNotSupported, "a correlated scalar subquery must be the comparison's right-hand side")
				}
				corr = append(corr, correlatedConjunct{cmp: cmp, scope: *scope, refs: refs, idx: len(corr)})
				correlated = true
			}
		}
		if !correlated {
			plannable = append(plannable, cmp)
		}
	}
	return plannable, corr, nil
}

// splitCorrelatedUpdate strips correlated conjuncts from an UPDATE's
// WHERE clause (the outer scope is the bare table name).
func (s *Session) splitCorrelatedUpdate(ctx context.Context, txn *kvclient.Txn, t *parser.Update) ([]correlatedConjunct, *parser.Update, error) {
	desc, err := s.cat.Lookup(ctx, txn, t.Table)
	if err != nil {
		return nil, t, nil // let the ordinary path report the missing table
	}
	plannable, corr, err := s.splitCorrelatedWhere(ctx, txn, t.Where, desc, "")
	if err != nil {
		return nil, nil, err
	}
	if len(corr) == 0 {
		return nil, t, nil
	}
	c := *t
	c.Where = plannable
	return corr, &c, nil
}

// splitCorrelatedDelete is splitCorrelatedUpdate for DELETE.
func (s *Session) splitCorrelatedDelete(ctx context.Context, txn *kvclient.Txn, t *parser.Delete) ([]correlatedConjunct, *parser.Delete, error) {
	desc, err := s.cat.Lookup(ctx, txn, t.Table)
	if err != nil {
		return nil, t, nil
	}
	plannable, corr, err := s.splitCorrelatedWhere(ctx, txn, t.Where, desc, "")
	if err != nil {
		return nil, nil, err
	}
	if len(corr) == 0 {
		return nil, t, nil
	}
	c := *t
	c.Where = plannable
	return corr, &c, nil
}

// mirrorOp flips a comparison for operand exchange (a op b == b mirror(op) a).
func mirrorOp(op string) string {
	switch op {
	case "<":
		return ">"
	case "<=":
		return ">="
	case ">":
		return "<"
	case ">=":
		return "<="
	}
	return op // = and != are symmetric
}

// substituteExpr replaces correlated column references in a value chain
// with literals from the outer row.
func substituteExpr(e parser.Expr, sc *corrScope, row map[catalog.ColumnID]types.Datum) parser.Expr {
	out := e
	if out.Column != "" {
		if inner, _ := sc.classify(out.Column); !inner {
			d := sc.outerDatum(out.Column, row)
			out.Column, out.Lit = "", &d
		}
	}
	if out.Right != nil {
		r := substituteExpr(*out.Right, sc, row)
		out.Right = &r
	}
	return out
}

// substituteSub clones the inner select with every correlated reference
// replaced by the outer row's value. The source AST is never modified.
func substituteSub(sub *parser.Select, sc *corrScope, row map[catalog.ColumnID]types.Datum, params []types.Datum) (*parser.Select, error) {
	c := *sub
	c.Where = append([]parser.Comparison(nil), sub.Where...)
	for i := range c.Where {
		cmp := &c.Where[i]
		cmp.Value = substituteExpr(cmp.Value, sc, row)
		if len(cmp.Values) > 0 {
			cmp.Values = append([]parser.Expr(nil), cmp.Values...)
			for j := range cmp.Values {
				cmp.Values[j] = substituteExpr(cmp.Values[j], sc, row)
			}
		}
		if cmp.Column == "" {
			continue
		}
		inner, err := sc.classify(cmp.Column)
		if err != nil {
			return nil, err
		}
		if inner {
			continue
		}
		// The conjunct's LEFT side is the outer reference (outer.x op rhs).
		lhs := sc.outerDatum(cmp.Column, row)
		switch cmp.Op {
		case "IS NULL", "IS NOT NULL":
			truth := lhs.Null == (cmp.Op == "IS NULL")
			*cmp = parser.Comparison{Op: constOp(truth)}
		case "=", "!=", "<", "<=", ">", ">=":
			if rhsCol := cmp.Value.Column; rhsCol != "" && cmp.Value.BinOp == "" {
				// outer.x op inner_col  →  inner_col mirror(op) <literal>
				*cmp = parser.Comparison{Column: rhsCol, Op: mirrorOp(cmp.Op), Value: parser.Expr{Lit: &lhs}}
				break
			}
			// Fully-substituted right side: the conjunct is a constant.
			rhs, err := evalExpr(cmp.Value, nil, nil, params)
			if err != nil {
				return nil, err
			}
			match, err := compareDatums(lhs, cmp.Op, rhs)
			if err != nil {
				return nil, err
			}
			*cmp = parser.Comparison{Op: constOp(match)}
		default:
			return nil, newErrf(CodeFeatureNotSupported, "correlated reference on the left of %s is not supported", cmp.Op)
		}
	}
	// Select-list expressions (already vetted: no outer refs in aggregate
	// arguments).
	c.Exprs = append([]parser.SelectExpr(nil), sub.Exprs...)
	for i := range c.Exprs {
		c.Exprs[i].Expr = substituteExpr(c.Exprs[i].Expr, sc, row)
	}
	if len(sub.Having) > 0 {
		c.Having = append([]parser.HavingCond(nil), sub.Having...)
		for i := range c.Having {
			c.Having[i].Value = substituteExpr(c.Having[i].Value, sc, row)
		}
	}
	return &c, nil
}

func constOp(b bool) string {
	if b {
		return "TRUE"
	}
	return "FALSE"
}

// compareDatums applies a scalar comparison with SQL NULL semantics
// (NULL never matches).
func compareDatums(l types.Datum, op string, r types.Datum) (bool, error) {
	if l.Null || r.Null {
		return false, nil
	}
	c, err := l.Compare(r)
	if err != nil {
		return false, err
	}
	switch op {
	case "=":
		return c == 0, nil
	case "!=":
		return c != 0, nil
	case "<":
		return c < 0, nil
	case "<=":
		return c <= 0, nil
	case ">":
		return c > 0, nil
	case ">=":
		return c >= 0, nil
	}
	return false, newErrf(CodeInternal, "unknown comparison %q", op)
}

// corrMemo caches inner-query results per statement, keyed by conjunct
// and the substituted outer values — repeated correlation keys run the
// inner query once.
type corrMemo map[string]memoEntry

type memoEntry struct {
	exists bool
	scalar types.Datum
	values []parser.Expr
}

func (cc *correlatedConjunct) memoKey(row map[catalog.ColumnID]types.Datum) string {
	var b strings.Builder
	fmt.Fprintf(&b, "%d", cc.idx)
	for _, name := range cc.refs {
		d := cc.scope.outerDatum(name, row)
		fmt.Fprintf(&b, "|%t,%d,%d,%g,%q,%t", d.Null, d.Fam, d.I, d.F, d.S, d.B)
	}
	return b.String()
}

// evalCorrelated evaluates every correlated conjunct against one fetched
// outer row (AND semantics).
func (s *Session) evalCorrelated(ctx context.Context, txn *kvclient.Txn, corr []correlatedConjunct, desc *catalog.TableDescriptor, row map[catalog.ColumnID]types.Datum, params []types.Datum, memo corrMemo) (bool, error) {
	for i := range corr {
		cc := &corr[i]
		match, err := s.evalCorrelatedOne(ctx, txn, cc, desc, row, params, memo)
		if err != nil {
			return false, err
		}
		if !match {
			return false, nil
		}
	}
	return true, nil
}

func (s *Session) evalCorrelatedOne(ctx context.Context, txn *kvclient.Txn, cc *correlatedConjunct, desc *catalog.TableDescriptor, row map[catalog.ColumnID]types.Datum, params []types.Datum, memo corrMemo) (bool, error) {
	key := cc.memoKey(row)
	entry, cached := memo[key]

	switch cc.cmp.Op {
	case "EXISTS", "NOT EXISTS":
		if !cached {
			probe, err := substituteSub(cc.cmp.Sub, &cc.scope, row, params)
			if err != nil {
				return false, err
			}
			probe.Limit = 1
			res, err := s.execSelect(ctx, txn, probe, params)
			if err != nil {
				return false, err
			}
			entry = memoEntry{exists: len(res.Rows) > 0}
			memo[key] = entry
		}
		if cc.cmp.Op == "NOT EXISTS" {
			return !entry.exists, nil
		}
		return entry.exists, nil

	case "IN", "NOT IN":
		if !cached {
			sub, err := substituteSub(cc.cmp.Sub, &cc.scope, row, params)
			if err != nil {
				return false, err
			}
			res, err := s.execSelect(ctx, txn, sub, params)
			if err != nil {
				return false, err
			}
			if len(res.Columns) != 1 {
				return false, newErrf(CodeSyntaxError, "subquery must return only one column")
			}
			vals := make([]parser.Expr, len(res.Rows))
			for j, r := range res.Rows {
				d := r[0]
				vals[j] = parser.Expr{Lit: &d}
			}
			entry = memoEntry{values: vals}
			memo[key] = entry
		}
		spliced := parser.Comparison{Column: cc.cmp.Column, Op: cc.cmp.Op, Values: entry.values}
		return matchesWhere([]parser.Comparison{spliced}, desc, row, params)

	default: // scalar comparison: col op (SELECT ...) [+ tail]
		if !cached {
			sub, err := substituteSub(cc.cmp.Value.Sub, &cc.scope, row, params)
			if err != nil {
				return false, err
			}
			res, err := s.execSelect(ctx, txn, sub, params)
			if err != nil {
				return false, err
			}
			if len(res.Columns) != 1 {
				return false, newErrf(CodeSyntaxError, "subquery must return only one column")
			}
			switch len(res.Rows) {
			case 0:
				entry = memoEntry{scalar: types.DNull}
			case 1:
				entry = memoEntry{scalar: res.Rows[0][0]}
			default:
				return false, newErrf(CodeCardinality, "more than one row returned by a subquery used as an expression")
			}
			memo[key] = entry
		}
		spliced := cc.cmp
		v := spliced.Value
		d := entry.scalar
		v.Sub, v.Lit = nil, &d
		spliced.Value = v
		return matchesWhere([]parser.Comparison{spliced}, desc, row, params)
	}
}
