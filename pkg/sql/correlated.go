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

// Correlated subqueries (scalar / [NOT] IN / [NOT] EXISTS in the WHERE
// clause of a single-table SELECT, UPDATE, or DELETE; plain single-table
// inner selects; up to maxCorrDepth nesting levels).
//
// Correlation is detected STRUCTURALLY — every name in a subquery resolves
// against its own scope first, then each enclosing scope nearest-first (PG
// scoping: a nearer column shadows a farther one). A name resolving
// nowhere is a proper 42703.
//
// Execution is a nested loop, one level at a time: correlated conjuncts
// are stripped from the WHERE clause before access planning, the outer
// rows are fetched with the remaining conjuncts, and each fetched row
// evaluates its correlated conjuncts by cloning the subquery TREE with
// every reference to THIS query's row spliced in as a literal (the parsed
// AST is never mutated) and running it through the ordinary machinery.
// References to intermediate levels stay symbolic in the clone: when the
// substituted subquery executes, its own splitCorrelatedWhere re-detects
// them as ordinary one-level correlation — multi-level correlation is the
// single-level machinery composing with itself. O(∏ level row counts),
// stated in EXPLAIN; a per-statement memo keyed by the substituted values
// makes repeated correlation keys cheap at every level.

// maxCorrDepth caps subquery nesting below a correlated statement: each
// level multiplies the work of the one above.
const maxCorrDepth = 4

// scopeLevel is one query's name-resolution scope.
type scopeLevel struct {
	desc  *catalog.TableDescriptor
	alias string // alias if given, else the table name
}

func (sl scopeLevel) matches(q string) bool {
	return q == sl.alias || q == sl.desc.Name
}

// corrScope is a subquery's resolution context: its own scope plus every
// enclosing scope, nearest first.
type corrScope struct {
	inner  scopeLevel
	outers []scopeLevel // [0] = immediately enclosing
}

// classify resolves one name to a level: 0 is the subquery's own scope,
// k >= 1 is outers[k-1]. Nearer scopes shadow farther ones.
func (sc *corrScope) classify(name string) (int, error) {
	q, col := splitQualified(name)
	if q != "" {
		if sc.inner.matches(q) {
			if _, ok := sc.inner.desc.Col(col); !ok {
				return 0, newErrf(CodeUndefinedColumn, "column %q does not exist", name)
			}
			return 0, nil
		}
		for i, sl := range sc.outers {
			if !sl.matches(q) {
				continue
			}
			if _, ok := sl.desc.Col(col); !ok {
				return 0, newErrf(CodeUndefinedColumn, "column %q does not exist", name)
			}
			return i + 1, nil
		}
		return 0, newErrf(CodeUndefinedTable, "missing FROM-clause entry for table %q", q)
	}
	if _, ok := sc.inner.desc.Col(name); ok {
		return 0, nil
	}
	for i, sl := range sc.outers {
		if _, ok := sl.desc.Col(name); ok {
			return i + 1, nil
		}
	}
	return 0, newErrf(CodeUndefinedColumn, "column %q does not exist", name)
}

// bindLevel is the classify index of the scope currently being bound (the
// statement whose row is in hand): the outermost scope this node knows.
func (sc *corrScope) bindLevel() int { return len(sc.outers) }

// bindDatum evaluates a bind-level reference against the bound row.
func (sc *corrScope) bindDatum(name string, row map[catalog.ColumnID]types.Datum) types.Datum {
	_, colName := splitQualified(name)
	col, _ := sc.outers[len(sc.outers)-1].desc.Col(colName)
	d, ok := row[col.ID]
	if !ok {
		d = types.DNull
	}
	return d
}

// scopeNode is the analysis result for one subquery: its scope, plus a
// child node per nested subquery (keyed by AST pointer — stable for the
// statement execution the split belongs to).
type scopeNode struct {
	scope    corrScope
	children map[*parser.Select]*scopeNode
}

// correlatedConjunct is one WHERE conjunct whose subquery tree references
// the current statement's row; it is evaluated per fetched row instead of
// being planned.
type correlatedConjunct struct {
	cmp  parser.Comparison
	node *scopeNode // analysis tree of the conjunct's subquery
	refs []string   // bind-level names, deduplicated, first-seen order (the memo key)
	idx  int
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

// innerSubShape reports whether a subquery is a shape correlation
// detection covers: a plain single-table select whose own nested
// subqueries (WHERE positions only) are recursively covered too.
func innerSubShape(sub *parser.Select) bool {
	if sub.Table == "" || len(sub.Joins) > 0 || sub.Derived != nil {
		return false
	}
	for _, cmp := range sub.Where {
		for _, nested := range conjunctSubs(cmp) {
			if !innerSubShape(nested) {
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

// analyzeSub classifies every name of a covered-shape subquery (and,
// recursively, its nested subqueries), returning the names that resolve
// to the OUTERMOST scope — the statement being bound — and the scope
// tree. Positions that cannot carry an outer reference (GROUP BY, ORDER
// BY, aggregate arguments, HAVING left-hand sides) reject with a clear
// error at every level.
func (s *Session) analyzeSub(ctx context.Context, txn *kvclient.Txn, sub *parser.Select, outers []scopeLevel) ([]string, *scopeNode, error) {
	if len(outers) > maxCorrDepth {
		return nil, nil, newErrf(CodeFeatureNotSupported, "correlated subqueries nest deeper than %d levels", maxCorrDepth)
	}
	innerDesc, err := s.lookup(ctx, txn, sub.Table)
	if err != nil {
		return nil, nil, err
	}
	innerAlias := sub.Alias
	if innerAlias == "" {
		innerAlias = sub.Table
	}
	node := &scopeNode{scope: corrScope{
		inner:  scopeLevel{desc: innerDesc, alias: innerAlias},
		outers: outers,
	}}
	bind := node.scope.bindLevel()

	var refs []string
	seen := map[string]bool{}
	var firstErr error
	record := func(name string, allowedOuter bool, position string) {
		if firstErr != nil {
			return
		}
		level, err := node.scope.classify(name)
		if err != nil {
			firstErr = err
			return
		}
		if level == 0 {
			return
		}
		if !allowedOuter {
			firstErr = newErrf(CodeFeatureNotSupported, "correlated reference %q is not supported in %s", name, position)
			return
		}
		// Only bind-level refs bubble up; intermediate levels are the
		// intermediate statements' business when they execute.
		if level == bind && !seen[name] {
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
		// Nested subqueries extend the scope stack by this level.
		for _, nested := range conjunctSubs(cmp) {
			if firstErr != nil {
				break
			}
			nrefs, nnode, nerr := s.analyzeSub(ctx, txn, nested,
				append([]scopeLevel{node.scope.inner}, outers...))
			if nerr != nil {
				return nil, nil, nerr
			}
			if node.children == nil {
				node.children = map[*parser.Select]*scopeNode{}
			}
			node.children[nested] = nnode
			// A nested bind-level ref is, in this node's coordinates, still
			// a bind-level ref: the stack grew by one at the near end.
			for _, n := range nrefs {
				if !seen[n] {
					seen[n] = true
					refs = append(refs, n)
				}
			}
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
	return refs, node, nil
}

// conjunctSubs collects the subqueries a conjunct carries (cmp.Sub plus
// any inside the value expression chains).
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
	outer := scopeLevel{desc: outerDesc, alias: outerAlias}
	for _, cmp := range where {
		subs := conjunctSubs(cmp)
		correlated := false
		if len(subs) > 0 {
			var refs []string
			var node *scopeNode
			covered := 0
			for _, sub := range subs {
				if !innerSubShape(sub) {
					continue // uncovered shape: leave to the eager path (and its conservative error)
				}
				covered++
				r, n, aerr := s.analyzeSub(ctx, txn, sub, []scopeLevel{outer})
				if aerr != nil {
					return nil, nil, aerr
				}
				if len(r) > 0 {
					refs, node = r, n
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
				corr = append(corr, correlatedConjunct{cmp: cmp, node: node, refs: refs, idx: len(corr)})
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
	desc, err := s.lookup(ctx, txn, t.Table)
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
	desc, err := s.lookup(ctx, txn, t.Table)
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

// substituteExpr replaces bind-level column references in a value chain
// with literals from the bound row, and rewrites nested subqueries
// through their scope nodes.
func substituteExpr(e parser.Expr, node *scopeNode, row map[catalog.ColumnID]types.Datum, params []types.Datum) (parser.Expr, error) {
	out := e
	if out.Column != "" {
		switch level, _ := node.scope.classify(out.Column); level {
		case node.scope.bindLevel():
			d := node.scope.bindDatum(out.Column, row)
			out.Column, out.Lit = "", &d
		case 0:
			// The clone executes as a plain single-table select, whose
			// resolver knows bare names only: strip the sub's own alias.
			_, bare := splitQualified(out.Column)
			out.Column = bare
		}
	}
	if out.Sub != nil {
		if child := node.children[out.Sub]; child != nil {
			sub, err := substituteSub(out.Sub, child, row, params)
			if err != nil {
				return out, err
			}
			out.Sub = sub
		}
	}
	if out.Right != nil {
		r, err := substituteExpr(*out.Right, node, row, params)
		if err != nil {
			return out, err
		}
		out.Right = &r
	}
	return out, nil
}

// substituteSub clones the subquery tree with every BIND-LEVEL reference
// replaced by the bound row's value. References to intermediate enclosing
// levels stay symbolic — the substituted select re-enters the correlation
// machinery when it executes, one level at a time. The source AST is
// never modified.
func substituteSub(sub *parser.Select, node *scopeNode, row map[catalog.ColumnID]types.Datum, params []types.Datum) (*parser.Select, error) {
	bind := node.scope.bindLevel()
	c := *sub
	c.Where = append([]parser.Comparison(nil), sub.Where...)
	for i := range c.Where {
		cmp := &c.Where[i]
		var err error
		cmp.Value, err = substituteExpr(cmp.Value, node, row, params)
		if err != nil {
			return nil, err
		}
		if len(cmp.Values) > 0 {
			cmp.Values = append([]parser.Expr(nil), cmp.Values...)
			for j := range cmp.Values {
				if cmp.Values[j], err = substituteExpr(cmp.Values[j], node, row, params); err != nil {
					return nil, err
				}
			}
		}
		if cmp.Sub != nil {
			if child := node.children[cmp.Sub]; child != nil {
				nsub, err := substituteSub(cmp.Sub, child, row, params)
				if err != nil {
					return nil, err
				}
				cmp.Sub = nsub
			}
		}
		if cmp.Column == "" {
			continue
		}
		level, err := node.scope.classify(cmp.Column)
		if err != nil {
			return nil, err
		}
		if level == 0 {
			// Own scope: strip the alias qualifier for the plain
			// single-table resolver the clone will execute under.
			_, bare := splitQualified(cmp.Column)
			cmp.Column = bare
			continue
		}
		if level != bind {
			continue // intermediate level: stays symbolic
		}
		// The conjunct's LEFT side is a bind-level reference (outer.x op rhs).
		lhs := node.scope.bindDatum(cmp.Column, row)
		switch cmp.Op {
		case "IS NULL", "IS NOT NULL":
			truth := lhs.Null == (cmp.Op == "IS NULL")
			*cmp = parser.Comparison{Op: constOp(truth)}
		case "=", "!=", "<", "<=", ">", ">=":
			if rhsCol := cmp.Value.Column; rhsCol != "" && cmp.Value.BinOp == "" {
				// outer.x op other_col  →  other_col mirror(op) <literal>
				*cmp = parser.Comparison{Column: rhsCol, Op: mirrorOp(cmp.Op), Value: parser.Expr{Lit: &lhs}}
				break
			}
			if cmp.Value.Sub != nil {
				// outer.x op (SELECT ...): keep as a comparison against the
				// (possibly still correlated) subquery with the literal on
				// the left folded to the right via the mirror.
				return nil, newErrf(CodeFeatureNotSupported, "a correlated reference cannot compare against another subquery")
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
		var err error
		if c.Exprs[i].Expr, err = substituteExpr(c.Exprs[i].Expr, node, row, params); err != nil {
			return nil, err
		}
	}
	if len(sub.Having) > 0 {
		c.Having = append([]parser.HavingCond(nil), sub.Having...)
		for i := range c.Having {
			var err error
			if c.Having[i].Value, err = substituteExpr(c.Having[i].Value, node, row, params); err != nil {
				return nil, err
			}
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
// inner query once. Each nesting level's statement execution carries its
// own memo.
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
		d := cc.node.scope.bindDatum(name, row)
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
			probe, err := substituteSub(cc.cmp.Sub, cc.node, row, params)
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
			sub, err := substituteSub(cc.cmp.Sub, cc.node, row, params)
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
			sub, err := substituteSub(cc.cmp.Value.Sub, cc.node, row, params)
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
