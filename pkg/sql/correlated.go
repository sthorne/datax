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

// scopeLevel is one query's name-resolution scope: a table, or (aliases
// set) the merged sides of a join, whose descriptor carries every side's
// columns under qualified names ("c.oid") plus the unambiguous bare ones.
type scopeLevel struct {
	desc    *catalog.TableDescriptor
	alias   string   // alias if given, else the table name
	aliases []string // further qualifiers naming this level (join sides)
}

// matches reports whether q names this level. An aliased table answers
// to its alias only (PostgreSQL scoping: FROM u AS u2 hides "u", so a
// "u.x" inside the subquery reaches an enclosing u).
func (sl scopeLevel) matches(q string) bool {
	if q == sl.alias {
		return true
	}
	for _, a := range sl.aliases {
		if a == q {
			return true
		}
	}
	return false
}

// col resolves a (possibly qualified) column within this level.
func (sl scopeLevel) col(q, name string) (catalog.Column, bool) {
	if q != "" && len(sl.aliases) > 0 {
		if c, ok := sl.desc.Col(q + "." + name); ok {
			return c, true
		}
	}
	return sl.desc.Col(name)
}

// corrScope is a subquery's resolution context: its own scope plus every
// enclosing scope, nearest first.
type corrScope struct {
	inner  scopeLevel
	more   []scopeLevel // further own-scope sides when the subquery joins
	outers []scopeLevel // [0] = immediately enclosing
}

// own reports whether q names one of the subquery's own sides, and
// whether that side has column col ("" = any column).
func (sc *corrScope) own(q, col string) (matched, hasCol bool) {
	for _, sl := range append([]scopeLevel{sc.inner}, sc.more...) {
		if q != "" && !sl.matches(q) {
			continue
		}
		if col == "" {
			return true, true
		}
		if _, ok := sl.col(q, col); ok {
			return true, true
		}
		if q != "" {
			return true, false
		}
	}
	return false, false
}

// classify resolves one name to a level: 0 is the subquery's own scope,
// k >= 1 is outers[k-1]. Nearer scopes shadow farther ones.
func (sc *corrScope) classify(name string) (int, error) {
	q, col := splitQualified(name)
	if q != "" {
		if matched, hasCol := sc.own(q, col); matched {
			if !hasCol {
				return 0, newErrf(CodeUndefinedColumn, "column %q does not exist", name)
			}
			return 0, nil
		}
		for i, sl := range sc.outers {
			if !sl.matches(q) {
				continue
			}
			if _, ok := sl.col(q, col); !ok {
				return 0, newErrf(CodeUndefinedColumn, "column %q does not exist", name)
			}
			return i + 1, nil
		}
		return 0, newErrf(CodeUndefinedTable, "missing FROM-clause entry for table %q", q)
	}
	if _, ok := sc.own("", name); ok {
		return 0, nil
	}
	for i, sl := range sc.outers {
		if _, ok := sl.desc.Col(name); ok {
			return i + 1, nil
		}
	}
	return 0, newErrf(CodeUndefinedColumn, "column %q does not exist", name)
}

// owns lists every own-scope level (the inner side first).
func (sc *corrScope) owns() []scopeLevel {
	return append([]scopeLevel{sc.inner}, sc.more...)
}

// bindLevel is the classify index of the scope currently being bound (the
// statement whose row is in hand): the outermost scope this node knows.
func (sc *corrScope) bindLevel() int { return len(sc.outers) }

// bindDatum evaluates a bind-level reference against the bound row.
func (sc *corrScope) bindDatum(name string, row map[catalog.ColumnID]types.Datum) types.Datum {
	q, colName := splitQualified(name)
	col, _ := sc.outers[len(sc.outers)-1].col(q, colName)
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
	cmp   parser.Comparison
	node  *scopeNode                    // any analyzed subquery's node: all share the bound scope
	nodes map[*parser.Select]*scopeNode // analysis tree per subquery of the conjunct
	refs  []string                      // bind-level names, deduplicated, first-seen order (the memo key)
	idx   int
}

// exprColumns walks a value expression's column references (operands,
// function arguments, CASE arms).
func exprColumns(e parser.Expr, visit func(string)) {
	if e.Column != "" {
		visit(e.Column)
	}
	if e.Left != nil {
		exprColumns(*e.Left, visit)
	}
	for _, a := range e.Args {
		exprColumns(a, visit)
	}
	if e.Case != nil {
		if e.Case.Operand != nil {
			exprColumns(*e.Case.Operand, visit)
		}
		for _, w := range e.Case.Whens {
			if w.Value != nil {
				exprColumns(*w.Value, visit)
			}
			for _, c := range w.Cond {
				if c.Column != "" {
					visit(c.Column)
				}
				exprColumns(c.Value, visit)
			}
			exprColumns(w.Result, visit)
		}
		if e.Case.Else != nil {
			exprColumns(*e.Case.Else, visit)
		}
	}
	if e.Cmp != nil {
		condColumns(*e.Cmp, visit)
	}
	if e.Right != nil {
		exprColumns(*e.Right, visit)
	}
}

// condColumns walks a conjunct's column references (OR groups included).
func condColumns(c parser.Comparison, visit func(string)) {
	if c.Column != "" {
		visit(c.Column)
	}
	if c.Expr != nil {
		exprColumns(*c.Expr, visit)
	}
	exprColumns(c.Value, visit)
	for _, v := range c.Values {
		exprColumns(v, visit)
	}
	for _, d := range c.Or {
		for _, inner := range d {
			condColumns(inner, visit)
		}
	}
}

// innerSubShape reports whether a subquery is a shape correlation
// detection covers: a plain single-table select whose own nested
// subqueries (WHERE positions only) are recursively covered too.
func innerSubShape(sub *parser.Select) bool {
	if sub.Derived != nil || sub.Union != nil {
		return false
	}
	for _, cmp := range sub.Where {
		for _, nested := range conjunctSubs(cmp) {
			if !innerSubShape(nested) {
				return false
			}
		}
	}
	for _, jc := range sub.Joins {
		for _, cmp := range jc.Filter {
			for _, nested := range condSubqueries(cmp) {
				if !innerSubShape(nested) {
					return false
				}
			}
		}
	}
	for _, se := range sub.Exprs {
		for _, nested := range exprSubqueries(se.Expr) {
			if !innerSubShape(nested) {
				return false
			}
		}
	}
	for _, hc := range sub.Having {
		if exprHasSubquery(hc.Value) {
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
	if len(sub.With) > 0 {
		// The subquery's own WITH members bind by shape for the analysis
		// (execution binds them with rows when the subquery runs).
		restore, err := s.bindWith(ctx, txn, sub.With, nil, true, nil)
		if err != nil {
			return nil, nil, err
		}
		defer restore()
	}
	if hasDerivedJoin(sub) {
		bound, restore, err := s.bindJoinedDerived(ctx, txn, sub, nil, true)
		if err != nil {
			return nil, nil, err
		}
		defer restore()
		sub = bound
	}
	var innerDesc *catalog.TableDescriptor
	switch {
	case sub.FuncTable != nil:
		innerDesc = funcTableDesc(sub)
	case sub.Table == "":
		// FROM-less: no columns of its own; every name is an outer's.
		innerDesc = &catalog.TableDescriptor{}
	default:
		var err error
		if innerDesc, err = s.lookup(ctx, txn, sub.Table); err != nil {
			return nil, nil, err
		}
	}
	inner := scopeLevel{desc: innerDesc, alias: sub.Alias}
	if inner.alias == "" {
		// Unaliased: the bare name, and the qualified spelling if the
		// query used one (pg_catalog.pg_class).
		inner.alias = innerDesc.Name
		if sub.Table != "" && sub.Table != innerDesc.Name {
			inner.aliases = []string{sub.Table}
		}
	}
	node := &scopeNode{scope: corrScope{inner: inner, outers: outers}}
	for _, jc := range sub.Joins {
		jd, err := s.lookup(ctx, txn, jc.Table)
		if err != nil {
			return nil, nil, err
		}
		side := scopeLevel{desc: jd, alias: jc.Alias}
		if side.alias == "" {
			side.alias = jd.Name
			if jc.Table != jd.Name {
				side.aliases = []string{jc.Table}
			}
		}
		node.scope.more = append(node.scope.more, side)
	}
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

	// analyzeNested analyzes subqueries one level down; a nested
	// bind-level ref is, in this node's coordinates, still a bind-level
	// ref: the stack grew at the near end.
	analyzeNested := func(nested []*parser.Select) error {
		for _, n := range nested {
			if firstErr != nil {
				return nil
			}
			nrefs, nnode, nerr := s.analyzeSub(ctx, txn, n, append(node.scope.owns(), outers...))
			if nerr != nil {
				return nerr
			}
			if node.children == nil {
				node.children = map[*parser.Select]*scopeNode{}
			}
			node.children[n] = nnode
			for _, r := range nrefs {
				if !seen[r] {
					seen[r] = true
					refs = append(refs, r)
				}
			}
		}
		return nil
	}
	if sub.FuncTable != nil {
		// unnest(outer.col): the argument may reference enclosing rows.
		exprColumns(*sub.FuncTable, func(n string) { record(n, true, "FROM") })
	}
	conds := append([]parser.Comparison(nil), sub.Where...)
	for _, jc := range sub.Joins {
		conds = append(conds, jc.Filter...)
	}
	for _, cmp := range conds {
		condColumns(cmp, func(n string) { record(n, true, "WHERE") })
		// Nested subqueries extend the scope stack by this level.
		if err := analyzeNested(condSubqueries(cmp)); err != nil {
			return nil, nil, err
		}
	}
	for _, se := range sub.Exprs {
		if se.AggCol != "" && se.AggCol != "*" {
			record(se.AggCol, false, "an aggregate argument")
		}
		exprColumns(se.Expr, func(n string) { record(n, true, "the select list") })
		if err := analyzeNested(exprSubqueries(se.Expr)); err != nil {
			return nil, nil, err
		}
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

// condSubqueries lists every subquery a conjunct carries: its own Sub,
// those inside its value expressions, and (recursively) those of an OR
// group's disjuncts.
func condSubqueries(cmp parser.Comparison) []*parser.Select {
	var subs []*parser.Select
	if cmp.Sub != nil {
		subs = append(subs, cmp.Sub)
	}
	if cmp.Expr != nil {
		subs = append(subs, exprSubqueries(*cmp.Expr)...)
	}
	subs = append(subs, exprSubqueries(cmp.Value)...)
	for _, v := range cmp.Values {
		subs = append(subs, exprSubqueries(v)...)
	}
	for _, d := range cmp.Or {
		for _, inner := range d {
			subs = append(subs, condSubqueries(inner)...)
		}
	}
	return subs
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
	return s.splitCorrelatedWhereScope(ctx, txn, where, scopeLevel{desc: outerDesc, alias: outerAlias})
}

// splitCorrelatedWhereScope is splitCorrelatedWhere against an explicit
// outer scope (a table, or a join's merged sides).
func (s *Session) splitCorrelatedWhereScope(ctx context.Context, txn *kvclient.Txn, where []parser.Comparison, outer scopeLevel) (plannable []parser.Comparison, corr []correlatedConjunct, err error) {
	for _, cmp := range where {
		subs := condSubqueries(cmp)
		correlated := false
		if len(subs) > 0 {
			cc := correlatedConjunct{cmp: cmp, nodes: map[*parser.Select]*scopeNode{}, idx: len(corr)}
			seen := map[string]bool{}
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
				cc.nodes[sub] = n
				if cc.node == nil {
					cc.node = n
				}
				for _, name := range r {
					if !seen[name] {
						seen[name] = true
						cc.refs = append(cc.refs, name)
					}
				}
			}
			if len(cc.refs) > 0 {
				if covered != len(subs) {
					return nil, nil, newErrf(CodeFeatureNotSupported, "correlated subqueries are only supported over plain selects")
				}
				corr = append(corr, cc)
				correlated = true
			}
		}
		if !correlated {
			plannable = append(plannable, cmp)
		}
	}
	return plannable, corr, nil
}

// substituteCond clones a conjunct with every subquery it carries (OR
// groups included) substituted for the bound row.
func substituteCond(cmp parser.Comparison, nodes map[*parser.Select]*scopeNode, row map[catalog.ColumnID]types.Datum, params []types.Datum) (parser.Comparison, error) {
	subst := func(sub *parser.Select) (*parser.Select, error) {
		node := nodes[sub]
		if node == nil {
			return sub, nil
		}
		return substituteSub(sub, node, row, params)
	}
	out := cmp
	var err error
	if out.Sub != nil {
		if out.Sub, err = subst(cmp.Sub); err != nil {
			return out, err
		}
	}
	if out.Expr != nil {
		e, err := mapExprSubqueries(*cmp.Expr, subst)
		if err != nil {
			return out, err
		}
		out.Expr = &e
	}
	if out.Value, err = mapExprSubqueries(cmp.Value, subst); err != nil {
		return out, err
	}
	if len(cmp.Values) > 0 {
		out.Values = make([]parser.Expr, len(cmp.Values))
		for i, v := range cmp.Values {
			if out.Values[i], err = mapExprSubqueries(v, subst); err != nil {
				return out, err
			}
		}
	}
	if len(cmp.Or) > 0 {
		out.Or = make([][]parser.Comparison, len(cmp.Or))
		for i, d := range cmp.Or {
			out.Or[i] = make([]parser.Comparison, len(d))
			for j, inner := range d {
				if out.Or[i][j], err = substituteCond(inner, nodes, row, params); err != nil {
					return out, err
				}
			}
		}
	}
	return out, nil
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
			// A single-table clone executes under the plain resolver,
			// which knows bare names only: strip the sub's own alias.
			// A joined clone resolves qualifiers per side.
			if len(node.scope.more) == 0 {
				_, bare := splitQualified(out.Column)
				out.Column = bare
			}
		}
	}
	if out.Left != nil {
		l, err := substituteExpr(*out.Left, node, row, params)
		if err != nil {
			return out, err
		}
		out.Left = &l
	}
	if len(out.Args) > 0 {
		out.Args = append([]parser.Expr(nil), out.Args...)
		for i := range out.Args {
			a, err := substituteExpr(out.Args[i], node, row, params)
			if err != nil {
				return out, err
			}
			out.Args[i] = a
		}
	}
	if out.Case != nil {
		cs := *out.Case
		if cs.Operand != nil {
			o, err := substituteExpr(*cs.Operand, node, row, params)
			if err != nil {
				return out, err
			}
			cs.Operand = &o
		}
		cs.Whens = append([]parser.CaseWhen(nil), cs.Whens...)
		for i := range cs.Whens {
			w := &cs.Whens[i]
			if w.Value != nil {
				v, err := substituteExpr(*w.Value, node, row, params)
				if err != nil {
					return out, err
				}
				w.Value = &v
			}
			if len(w.Cond) > 0 {
				c, err := substituteConds(w.Cond, node, row, params)
				if err != nil {
					return out, err
				}
				w.Cond = c
			}
			r, err := substituteExpr(w.Result, node, row, params)
			if err != nil {
				return out, err
			}
			w.Result = r
		}
		if cs.Else != nil {
			e, err := substituteExpr(*cs.Else, node, row, params)
			if err != nil {
				return out, err
			}
			cs.Else = &e
		}
		out.Case = &cs
	}
	if out.Cmp != nil {
		cs, err := substituteConds([]parser.Comparison{*out.Cmp}, node, row, params)
		if err != nil {
			return out, err
		}
		out.Cmp = &cs[0]
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
	c := *sub
	var err error
	if sub.FuncTable != nil {
		ft, err := substituteExpr(*sub.FuncTable, node, row, params)
		if err != nil {
			return nil, err
		}
		c.FuncTable = &ft
	}
	if c.Where, err = substituteConds(sub.Where, node, row, params); err != nil {
		return nil, err
	}
	if len(sub.Joins) > 0 {
		c.Joins = append([]parser.JoinClause(nil), sub.Joins...)
		for i := range c.Joins {
			if len(c.Joins[i].Filter) == 0 {
				continue
			}
			if c.Joins[i].Filter, err = substituteConds(sub.Joins[i].Filter, node, row, params); err != nil {
				return nil, err
			}
		}
	}
	// Select-list expressions (already vetted: no outer refs in aggregate
	// arguments).
	c.Exprs = append([]parser.SelectExpr(nil), sub.Exprs...)
	for i := range c.Exprs {
		if c.Exprs[i].Expr, err = substituteExpr(c.Exprs[i].Expr, node, row, params); err != nil {
			return nil, err
		}
	}
	if len(sub.Having) > 0 {
		c.Having = append([]parser.HavingCond(nil), sub.Having...)
		for i := range c.Having {
			if c.Having[i].Value, err = substituteExpr(c.Having[i].Value, node, row, params); err != nil {
				return nil, err
			}
		}
	}
	return &c, nil
}

// substituteConds is substituteSub's per-conjunct step over one
// conjunction (WHERE, an ON filter, a CASE arm's condition).
func substituteConds(conds []parser.Comparison, node *scopeNode, row map[catalog.ColumnID]types.Datum, params []types.Datum) ([]parser.Comparison, error) {
	bind := node.scope.bindLevel()
	out := append([]parser.Comparison(nil), conds...)
	for i := range out {
		cmp := &out[i]
		if len(cmp.Or) > 0 {
			cmp.Or = append([][]parser.Comparison(nil), cmp.Or...)
			for j := range cmp.Or {
				d, err := substituteConds(cmp.Or[j], node, row, params)
				if err != nil {
					return nil, err
				}
				cmp.Or[j] = d
			}
			continue
		}
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
			// single-table resolver the clone will execute under (a
			// joined clone keeps its qualifiers).
			if len(node.scope.more) == 0 {
				_, bare := splitQualified(cmp.Column)
				cmp.Column = bare
			}
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
	return out, nil
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
	if !plainCmpOp(op) {
		return applyCmpOp(op, l, r)
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
	scalar types.Datum
	cmp    *parser.Comparison // a correlated conjunct, substituted and resolved
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
	if !cached {
		// Substitute this row's values into every subquery, then let the
		// ordinary resolve pass run them: EXISTS becomes a constant, IN a
		// value list, a scalar subquery a literal — per row, memoized.
		sub, err := substituteCond(cc.cmp, cc.nodes, row, params)
		if err != nil {
			return false, err
		}
		resolved, err := s.resolveWhereSubs(ctx, txn, []parser.Comparison{sub}, params)
		if err != nil {
			return false, err
		}
		entry = memoEntry{cmp: &resolved[0]}
		memo[key] = entry
	}
	return matchesWhere([]parser.Comparison{*entry.cmp}, desc, row, params)
}

// corrProj is one select-list expression containing a subquery that
// references the statement's own row ((SELECT d.x FROM d WHERE d.k = t.k)
// AS x, array(SELECT ... WHERE ... = t.k)): it is evaluated per fetched
// row — subqueries substituted and run, then the expression — instead
// of being spliced once before execution.
type corrProj struct {
	idx   int // select-list position
	expr  parser.Expr
	nodes map[*parser.Select]*scopeNode // analysis of each (covered) subquery
	refs  []string                      // bind-level names across all subqueries (the memo key)
	bind  *scopeNode                    // any node: all share the bound scope
}

// exprSubqueries lists the subqueries an expression carries, anywhere
// inside it.
func exprSubqueries(e parser.Expr) []*parser.Select {
	var subs []*parser.Select
	var walk func(e parser.Expr)
	var walkConds func(conds []parser.Comparison)
	walkConds = func(conds []parser.Comparison) {
		for _, c := range conds {
			if c.Sub != nil {
				subs = append(subs, c.Sub)
			}
			if c.Expr != nil {
				walk(*c.Expr)
			}
			walk(c.Value)
			for _, v := range c.Values {
				walk(v)
			}
			for _, d := range c.Or {
				walkConds(d)
			}
		}
	}
	walk = func(e parser.Expr) {
		if e.Sub != nil {
			subs = append(subs, e.Sub)
		}
		if e.Left != nil {
			walk(*e.Left)
		}
		if e.Right != nil {
			walk(*e.Right)
		}
		for _, a := range e.Args {
			walk(a)
		}
		if e.Case != nil {
			if e.Case.Operand != nil {
				walk(*e.Case.Operand)
			}
			for _, w := range e.Case.Whens {
				if w.Value != nil {
					walk(*w.Value)
				}
				walkConds(w.Cond)
				walk(w.Result)
			}
			if e.Case.Else != nil {
				walk(*e.Case.Else)
			}
		}
		if e.Cmp != nil {
			walkConds([]parser.Comparison{*e.Cmp})
		}
	}
	walk(e)
	return subs
}

// mapExprSubqueries returns a copy of e with every subquery replaced by
// f(sub) (the source AST is never modified).
func mapExprSubqueries(e parser.Expr, f func(*parser.Select) (*parser.Select, error)) (parser.Expr, error) {
	var walk func(e parser.Expr) (parser.Expr, error)
	var walkConds func(conds []parser.Comparison) ([]parser.Comparison, error)
	walkConds = func(conds []parser.Comparison) ([]parser.Comparison, error) {
		out := append([]parser.Comparison(nil), conds...)
		for i := range out {
			c := &out[i]
			var err error
			if c.Sub != nil {
				if c.Sub, err = f(c.Sub); err != nil {
					return nil, err
				}
			}
			if len(c.Or) > 0 {
				c.Or = append([][]parser.Comparison(nil), c.Or...)
				for j := range c.Or {
					if c.Or[j], err = walkConds(c.Or[j]); err != nil {
						return nil, err
					}
				}
			}
			if c.Expr != nil {
				l, err := walk(*c.Expr)
				if err != nil {
					return nil, err
				}
				c.Expr = &l
			}
			if c.Value, err = walk(c.Value); err != nil {
				return nil, err
			}
			if len(c.Values) > 0 {
				c.Values = append([]parser.Expr(nil), c.Values...)
				for j := range c.Values {
					if c.Values[j], err = walk(c.Values[j]); err != nil {
						return nil, err
					}
				}
			}
		}
		return out, nil
	}
	walk = func(e parser.Expr) (parser.Expr, error) {
		out := e
		var err error
		if e.Sub != nil {
			if out.Sub, err = f(e.Sub); err != nil {
				return out, err
			}
		}
		if e.Left != nil {
			l, err := walk(*e.Left)
			if err != nil {
				return out, err
			}
			out.Left = &l
		}
		if e.Right != nil {
			r, err := walk(*e.Right)
			if err != nil {
				return out, err
			}
			out.Right = &r
		}
		if len(e.Args) > 0 {
			out.Args = make([]parser.Expr, len(e.Args))
			for i, a := range e.Args {
				if out.Args[i], err = walk(a); err != nil {
					return out, err
				}
			}
		}
		if e.Case != nil {
			cs := *e.Case
			if cs.Operand != nil {
				o, err := walk(*cs.Operand)
				if err != nil {
					return out, err
				}
				cs.Operand = &o
			}
			cs.Whens = append([]parser.CaseWhen(nil), cs.Whens...)
			for i := range cs.Whens {
				w := &cs.Whens[i]
				if w.Value != nil {
					v, err := walk(*w.Value)
					if err != nil {
						return out, err
					}
					w.Value = &v
				}
				if w.Cond, err = walkConds(w.Cond); err != nil {
					return out, err
				}
				if w.Result, err = walk(w.Result); err != nil {
					return out, err
				}
			}
			if cs.Else != nil {
				el, err := walk(*cs.Else)
				if err != nil {
					return out, err
				}
				cs.Else = &el
			}
			out.Case = &cs
		}
		if e.Cmp != nil {
			cs, err := walkConds([]parser.Comparison{*e.Cmp})
			if err != nil {
				return out, err
			}
			out.Cmp = &cs[0]
		}
		return out, nil
	}
	return walk(e)
}

// splitCorrelatedProj finds the output expressions of a plain
// single-table select whose subqueries reference the select's own row,
// and returns a clone with each replaced by a NULL placeholder (typed
// after evaluation).
func (s *Session) splitCorrelatedProj(ctx context.Context, txn *kvclient.Txn, t *parser.Select, outer scopeLevel) ([]corrProj, *parser.Select, error) {
	var projs []corrProj
	var clone *parser.Select
	for i, se := range t.Exprs {
		subs := exprSubqueries(se.Expr)
		if len(subs) == 0 {
			continue
		}
		cp := corrProj{idx: i, expr: se.Expr, nodes: map[*parser.Select]*scopeNode{}}
		seen := map[string]bool{}
		for _, sub := range subs {
			if !innerSubShape(sub) {
				continue // uncovered shape: the eager path handles (or refuses) it
			}
			refs, node, err := s.analyzeSub(ctx, txn, sub, []scopeLevel{outer})
			if err != nil {
				return nil, nil, err
			}
			cp.nodes[sub] = node
			if cp.bind == nil {
				cp.bind = node
			}
			for _, r := range refs {
				if !seen[r] {
					seen[r] = true
					cp.refs = append(cp.refs, r)
				}
			}
		}
		if len(cp.refs) == 0 {
			continue
		}
		if clone == nil {
			c := *t
			c.Exprs = append([]parser.SelectExpr(nil), t.Exprs...)
			clone = &c
		}
		null := types.DNull
		clone.Exprs[i].Expr = parser.Expr{Lit: &null}
		projs = append(projs, cp)
	}
	if clone == nil {
		return nil, t, nil
	}
	return projs, clone, nil
}

// evalCorrProj evaluates one correlated output expression against a
// row, memoized per correlation key: the subqueries are substituted with
// the row's values and run, then the expression is evaluated.
func (s *Session) evalCorrProj(ctx context.Context, txn *kvclient.Txn, cp *corrProj, desc *catalog.TableDescriptor, row map[catalog.ColumnID]types.Datum, params []types.Datum, memo corrMemo) (types.Datum, error) {
	var b strings.Builder
	fmt.Fprintf(&b, "p%d", cp.idx)
	for _, name := range cp.refs {
		d := cp.bind.scope.bindDatum(name, row)
		fmt.Fprintf(&b, "|%t,%d,%d,%g,%q,%t", d.Null, d.Fam, d.I, d.F, d.S, d.B)
	}
	key := b.String()
	if entry, ok := memo[key]; ok {
		return entry.scalar, nil
	}
	e, err := mapExprSubqueries(cp.expr, func(sub *parser.Select) (*parser.Select, error) {
		node := cp.nodes[sub]
		if node == nil {
			return sub, nil
		}
		return substituteSub(sub, node, row, params)
	})
	if err != nil {
		return types.Datum{}, err
	}
	if e, err = s.resolveValueExpr(ctx, txn, e, params); err != nil {
		return types.Datum{}, err
	}
	d, err := evalExpr(e, desc, row, params)
	if err != nil {
		return types.Datum{}, err
	}
	memo[key] = memoEntry{scalar: d}
	return d, nil
}

// joinCorr carries a join select's correlated conjuncts and output
// expressions, bound against the merged scope of its sides.
type joinCorr struct {
	conds []correlatedConjunct
	projs []corrProj
	desc  *catalog.TableDescriptor // the merged descriptor
	rowOf func(jr joinedRow) map[catalog.ColumnID]types.Datum
}

// joinScope merges a join's sides into one scope level: every column
// appears under each qualifier that names its side ("c.oid", and
// "pg_class.oid" when the alias differs), and under its bare name when
// no other side has a column so named. rowOf projects a joined row onto
// the merged column IDs.
func joinScope(sides []joinSide) (scopeLevel, func(jr joinedRow) map[catalog.ColumnID]types.Datum) {
	const stride = 1 << 16
	desc := &catalog.TableDescriptor{Name: sides[0].desc.Name}
	count := map[string]int{}
	for _, js := range sides {
		for _, c := range js.desc.Columns {
			count[c.Name]++
		}
	}
	var aliases []string
	for i, js := range sides {
		quals := []string{js.alias}
		if js.desc.Name != js.alias && !js.aliased {
			quals = append(quals, js.desc.Name)
		}
		aliases = append(aliases, quals...)
		for _, c := range js.desc.Columns {
			id := catalog.ColumnID(i*stride) + c.ID
			for _, q := range quals {
				mc := c
				mc.ID, mc.Name = id, q+"."+c.Name
				desc.Columns = append(desc.Columns, mc)
			}
			if count[c.Name] == 1 {
				mc := c
				mc.ID = id
				desc.Columns = append(desc.Columns, mc)
			}
		}
	}
	rowOf := func(jr joinedRow) map[catalog.ColumnID]types.Datum {
		row := map[catalog.ColumnID]types.Datum{}
		for i, js := range sides {
			for _, c := range js.desc.Columns {
				row[catalog.ColumnID(i*stride)+c.ID] = jr.datum(joinRef{side: i, col: c})
			}
		}
		return row
	}
	return scopeLevel{desc: desc, alias: sides[0].alias, aliases: aliases}, rowOf
}
