package sql

import (
	"github.com/sthorne/datax/pkg/sql/catalog"
	"github.com/sthorne/datax/pkg/sql/parser"
)

// Cost-based join reordering (INNER joins only). With statistics present
// for every joined table, the greedy planner rewrites the syntactic join
// order: the side with the fewest estimated rows (after its side-local
// WHERE conjuncts) drives the nested loop, and each level extends with
// the cheapest side connected to the already-placed ones by at least one
// ON equality. The rewrite is a pure AST transformation returning a
// CLONE — prepared statements re-plan per execution and the original
// Select is never mutated — with `SELECT *` pre-expanded into qualified
// column references in the ORIGINAL side order, so output columns are
// byte-identical to the unreordered query, and every pooled ON conjunct
// re-attached (fully qualified) to the level where its later side is
// placed, so resolveOn's earlier-side rule keeps holding.
//
// reorderJoins declines (ok = false, caller keeps the syntactic order)
// whenever the rewrite could change semantics or has nothing to work
// with: any LEFT join (NULL-extension is order-sensitive), a derived
// base table, missing statistics for any side, an ambiguous qualifier
// (two sides sharing a table name, or an alias shadowing another side's
// table name — resolution is first-match and must stay order-free), a
// JSONB containment conjunct, an ON condition that does not resolve
// against the full side list, or a join graph that is not connected by
// ON equalities. Without statistics nothing runs at all: the syntactic
// order is byte-identical to the pre-statistics executor.

// pooledCond is one ON equality resolved against the full side list and
// rewritten to fully-qualified references.
type pooledCond struct {
	a, b int // side indices (a < b not required)
	cond parser.JoinCond
}

// reorderJoins returns the reordered clone, the permutation (new
// position → original side index), whether the order actually changed,
// and whether the rewrite applies at all. sides and stats are parallel;
// stats entries must all be non-nil.
func reorderJoins(t *parser.Select, sides []joinSide, stats []*catalog.TableStatistics) (*parser.Select, []int, bool, bool) {
	if len(sides) < 2 || t.Derived != nil || t.Table == "" {
		return nil, nil, false, false
	}
	for _, js := range sides {
		if js.left || js.right {
			return nil, nil, false, false
		}
	}
	for _, jc := range t.Joins {
		if len(jc.Filter) > 0 || jc.Cross {
			return nil, nil, false, false // ON filters stay bound to their level
		}
	}
	// Qualifier uniqueness: every alias and table name must name exactly
	// one side, or qualified references would bind by position.
	quals := map[string]int{}
	for i, js := range sides {
		for _, q := range []string{js.alias, js.desc.Name} {
			if prev, dup := quals[q]; dup && prev != i {
				return nil, nil, false, false
			}
			quals[q] = i
		}
	}
	for _, cmp := range t.Where {
		if cmp.Op == "@>" || cmp.Op == "NOT @>" {
			return nil, nil, false, false // refused downstream; keep that path pristine
		}
	}

	// Pool every ON conjunct, resolved against the FULL side list and
	// rewritten fully qualified.
	var pool []pooledCond
	for _, jc := range t.Joins {
		for _, cond := range jc.On {
			l, err := resolveJoinRef(sides, cond.L.String())
			if err != nil {
				return nil, nil, false, false
			}
			r, err := resolveJoinRef(sides, cond.R.String())
			if err != nil {
				return nil, nil, false, false
			}
			if l.side == r.side {
				return nil, nil, false, false
			}
			pool = append(pool, pooledCond{a: l.side, b: r.side, cond: parser.JoinCond{
				L: parser.ColumnRef{Table: sides[l.side].alias, Column: l.col.Name},
				R: parser.ColumnRef{Table: sides[r.side].alias, Column: r.col.Name},
			}})
		}
	}

	// Side-local row estimates: table rows scaled by the selectivity of
	// each WHERE conjunct that binds only that side (same selectivities
	// as single-table costing).
	est := make([]float64, len(sides))
	for i := range sides {
		est[i] = sideEstimate(i, sides, stats[i], t.Where)
	}

	// Greedy: cheapest side drives; extend with the cheapest side
	// connected to the placed set by at least one pooled equality.
	order := make([]int, 0, len(sides))
	placed := make([]bool, len(sides))
	pick := func(candidate func(int) bool) int {
		bestIdx := -1
		for i := range sides {
			if placed[i] || !candidate(i) {
				continue
			}
			if bestIdx < 0 || est[i] < est[bestIdx] {
				bestIdx = i
			}
		}
		return bestIdx
	}
	first := pick(func(int) bool { return true })
	order = append(order, first)
	placed[first] = true
	for len(order) < len(sides) {
		next := pick(func(i int) bool {
			for _, pc := range pool {
				if (pc.a == i && placed[pc.b]) || (pc.b == i && placed[pc.a]) {
					return true
				}
			}
			return false
		})
		if next < 0 {
			return nil, nil, false, false // disconnected join graph
		}
		order = append(order, next)
		placed[next] = true
	}
	reordered := false
	for p, si := range order {
		if p != si {
			reordered = true
			break
		}
	}
	if !reordered {
		return t, order, false, true
	}

	// Original parser-level (table, alias) per side.
	type srcRef struct{ table, alias string }
	srcs := make([]srcRef, len(sides))
	srcs[0] = srcRef{table: t.Table, alias: t.Alias}
	for i := range t.Joins {
		srcs[i+1] = srcRef{table: t.Joins[i].Table, alias: t.Joins[i].Alias}
	}

	clone := *t
	clone.Table, clone.Alias = srcs[order[0]].table, srcs[order[0]].alias
	pos := make([]int, len(sides)) // original side index → new position
	for p, si := range order {
		pos[si] = p
	}
	clone.Joins = make([]parser.JoinClause, len(sides)-1)
	for p := 1; p < len(order); p++ {
		si := order[p]
		clone.Joins[p-1] = parser.JoinClause{Table: srcs[si].table, Alias: srcs[si].alias}
	}
	// Each pooled conjunct attaches to the level where its later side is
	// placed; exactly one endpoint is that side, so resolveOn accepts it.
	for _, pc := range pool {
		p := pos[pc.a]
		if pos[pc.b] > p {
			p = pos[pc.b]
		}
		clone.Joins[p-1].On = append(clone.Joins[p-1].On, pc.cond)
	}

	// Pre-expand * into qualified references in the ORIGINAL side order,
	// so the output column order survives the permutation.
	hasStar := false
	for _, se := range t.Exprs {
		if se.Star {
			hasStar = true
			break
		}
	}
	if hasStar {
		exprs := make([]parser.SelectExpr, 0, len(t.Exprs))
		for _, se := range t.Exprs {
			if !se.Star {
				exprs = append(exprs, se)
				continue
			}
			for _, js := range sides {
				for _, c := range js.desc.VisibleColumns() {
					exprs = append(exprs, parser.SelectExpr{
						Expr: parser.Expr{Column: js.alias + "." + c.Name},
					})
				}
			}
		}
		clone.Exprs = exprs
	}
	return &clone, order, true, true
}

// exprRefsColumns reports whether the expression references any column
// or subquery — such a comparison value cannot be treated as a constant
// constraint for selectivity.
func exprRefsColumns(e parser.Expr) bool {
	if e.Column != "" || e.Sub != nil {
		return true
	}
	if e.Left != nil && exprRefsColumns(*e.Left) {
		return true
	}
	if e.Right != nil && exprRefsColumns(*e.Right) {
		return true
	}
	for _, a := range e.Args {
		if exprRefsColumns(a) {
			return true
		}
	}
	return false
}

// sideEstimate estimates the rows of side i surviving its side-local
// WHERE conjuncts: table rows times per-conjunct selectivity (equality
// by 1/distinct, ranges by the fixed fraction — the single-table
// constants), floored at one row.
func sideEstimate(i int, sides []joinSide, st *catalog.TableStatistics, where []parser.Comparison) float64 {
	rows := float64(1)
	if st.RowCount > 0 {
		rows = float64(st.RowCount)
	}
	for _, cmp := range where {
		if cmp.Column == "" || cmp.Expr != nil || len(cmp.Path) > 0 {
			continue
		}
		switch cmp.Op {
		case "=", "<", "<=", ">", ">=":
		default:
			continue
		}
		if exprRefsColumns(cmp.Value) {
			continue
		}
		ref, err := resolveJoinRef(sides, cmp.Column)
		if err != nil || ref.side != i {
			continue
		}
		if cmp.Op == "=" {
			sel := unknownEqSelectivity
			if cs, ok := st.Column(ref.col.ID); ok && cs.Distinct > 0 {
				sel = 1 / float64(cs.Distinct)
			}
			rows *= sel
		} else {
			rows *= rangeSelectivity
		}
	}
	if rows < 1 {
		rows = 1
	}
	return rows
}
