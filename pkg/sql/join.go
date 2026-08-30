package sql

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/sthorne/datax/pkg/kvclient"
	"github.com/sthorne/datax/pkg/sql/catalog"
	"github.com/sthorne/datax/pkg/sql/parser"
	"github.com/sthorne/datax/pkg/sql/types"
)

// N-way nested-loop joins (INNER and LEFT OUTER), left-deep in syntactic
// order: side 0 is the FROM table, each JOIN clause adds one side. The base
// side is fetched through the normal access-path planner with the
// base-only WHERE conjuncts; for each partial row the next side's ON
// equalities become synthetic equality predicates, so every join step
// reuses pickPlan — a join key on that side's PK or an index turns into a
// point/index lookup per row. The full WHERE clause evaluates on complete
// rows only (PostgreSQL semantics: on a LEFT JOIN it filters after
// NULL-extension). Aggregates and GROUP BY over the join run the joined
// rows through the grouped executor under a synthetic descriptor whose
// columns are the qualified "alias.column" names.

// joinSide names one side of the join.
type joinSide struct {
	desc  *catalog.TableDescriptor
	alias string // alias if given, else the table name
	left  bool   // this side was LEFT-joined (side 0: false)
}

func (js joinSide) matches(qualifier string) bool {
	return qualifier == js.alias || qualifier == js.desc.Name
}

// joinRef is a column resolved to a side.
type joinRef struct {
	side int
	col  catalog.Column
}

// splitQualified splits "t.c" into ("t", "c"); no dot → ("", name).
func splitQualified(name string) (table, column string) {
	if i := strings.IndexByte(name, '.'); i >= 0 {
		return name[:i], name[i+1:]
	}
	return "", name
}

// resolveJoinRef resolves a possibly-qualified column name against the
// join sides. Qualified names bind to the first matching side (in join
// order); unqualified names must be unambiguous across all sides.
func resolveJoinRef(sides []joinSide, name string) (joinRef, error) {
	q, colName := splitQualified(name)
	if q != "" {
		for i, js := range sides {
			if !js.matches(q) {
				continue
			}
			if col, ok := js.desc.Col(colName); ok {
				return joinRef{side: i, col: col}, nil
			}
			return joinRef{}, newErrf(CodeUndefinedColumn, "column %q does not exist", name)
		}
		return joinRef{}, newErrf(CodeUndefinedTable, "missing FROM-clause entry for table %q", q)
	}
	found := -1
	var fcol catalog.Column
	for i, js := range sides {
		if col, ok := js.desc.Col(colName); ok {
			if found >= 0 {
				return joinRef{}, newErrf(CodeAmbiguousColumn, "column reference %q is ambiguous", name)
			}
			found, fcol = i, col
		}
	}
	if found < 0 {
		return joinRef{}, newErrf(CodeUndefinedColumn, "column %q does not exist", name)
	}
	return joinRef{side: found, col: fcol}, nil
}

// joinedRow holds one row per side; a nil entry is a LEFT JOIN's NULL
// extension.
type joinedRow struct {
	rows []map[catalog.ColumnID]types.Datum
}

func (jr joinedRow) datum(ref joinRef) types.Datum {
	if ref.side >= len(jr.rows) || jr.rows[ref.side] == nil {
		return types.DNull
	}
	d, ok := jr.rows[ref.side][ref.col.ID]
	if !ok {
		return types.DNull
	}
	return d
}

// extend returns jr plus one more side (row may be nil for a LEFT miss).
func (jr joinedRow) extend(row map[catalog.ColumnID]types.Datum) joinedRow {
	out := make([]map[catalog.ColumnID]types.Datum, len(jr.rows)+1)
	copy(out, jr.rows)
	out[len(jr.rows)] = row
	return joinedRow{rows: out}
}

// onPair is one resolved ON equality for joining side k: a column of an
// earlier side (left) equated with a column of side k itself (right).
type onPair struct {
	leftSide int
	leftCol  catalog.Column
	rightCol catalog.Column
}

// resolveOn resolves side k's ON conjuncts against sides[:k+1]; each must
// equate a column of the newly joined side with a column of an earlier one.
func resolveOn(sides []joinSide, k int, conds []parser.JoinCond) ([]onPair, error) {
	scope := sides[:k+1]
	pairs := make([]onPair, 0, len(conds))
	for _, jc := range conds {
		l, err := resolveJoinRef(scope, jc.L.String())
		if err != nil {
			return nil, err
		}
		r, err := resolveJoinRef(scope, jc.R.String())
		if err != nil {
			return nil, err
		}
		if (l.side == k) == (r.side == k) {
			return nil, newErrf(CodeFeatureNotSupported,
				"ON conditions must equate a column of the joined table with one from an earlier table")
		}
		p := onPair{leftSide: l.side, leftCol: l.col, rightCol: r.col}
		if l.side == k {
			p = onPair{leftSide: r.side, leftCol: r.col, rightCol: l.col}
		}
		pairs = append(pairs, p)
	}
	return pairs, nil
}

// joinProj is one output column of a join select.
type joinProj struct {
	name string
	ref  joinRef
}

// resolveJoinProjection resolves the select list: * (each side's columns
// in join order, PostgreSQL order) or plain possibly-qualified column
// references. Expressions over joins are not supported; aggregates take
// the grouped path and never reach here.
func resolveJoinProjection(sides []joinSide, exprs []parser.SelectExpr) ([]joinProj, error) {
	var proj []joinProj
	for _, se := range exprs {
		switch {
		case se.Star:
			for i, js := range sides {
				for _, c := range js.desc.VisibleColumns() {
					proj = append(proj, joinProj{name: c.Name, ref: joinRef{side: i, col: c}})
				}
			}
		case se.Agg != "":
			return nil, newErrf(CodeInternal, "aggregate reached the join projection")
		case se.Expr.Column != "" && se.Expr.BinOp == "":
			ref, err := resolveJoinRef(sides, se.Expr.Column)
			if err != nil {
				return nil, err
			}
			name := se.Alias
			if name == "" {
				name = ref.col.Name
			}
			proj = append(proj, joinProj{name: name, ref: ref})
		default:
			return nil, newErrf(CodeFeatureNotSupported, "join SELECT items must be columns or *")
		}
	}
	return proj, nil
}

// evalJoinWhere evaluates the full WHERE conjunction against a complete
// joined row, mirroring matchesWhere (NULL comparisons never match; IS
// [NOT] NULL sees the LEFT JOIN's NULL extension).
func evalJoinWhere(sides []joinSide, where []parser.Comparison, jr joinedRow, params []types.Datum) (bool, error) {
	for _, cmp := range where {
		if cmp.Op == "TRUE" {
			continue
		}
		if cmp.Op == "FALSE" {
			return false, nil
		}
		ref, err := resolveJoinRef(sides, cmp.Column)
		if err != nil {
			return false, err
		}
		lhs := jr.datum(ref)
		if cmp.Op == "IS NULL" || cmp.Op == "IS NOT NULL" {
			if lhs.Null != (cmp.Op == "IS NULL") {
				return false, nil
			}
			continue
		}
		if cmp.Op == "IN" || cmp.Op == "NOT IN" {
			match, err := matchesIn(cmp, ref.col, lhs, nil, nil, params)
			if err != nil || !match {
				return false, err
			}
			continue
		}
		rhs, err := evalExpr(cmp.Value, nil, nil, params)
		if err != nil {
			return false, err
		}
		if lhs.Null || rhs.Null {
			return false, nil
		}
		rhs, cerr := rhs.Coerce(ref.col.Type)
		if cerr != nil {
			return false, newErrf(CodeInternal, "WHERE %s: %v", cmp.Column, cerr)
		}
		c, err := lhs.Compare(rhs)
		if err != nil {
			return false, nil
		}
		ok := false
		switch cmp.Op {
		case "=":
			ok = c == 0
		case "!=":
			ok = c != 0
		case "<":
			ok = c < 0
		case "<=":
			ok = c <= 0
		case ">":
			ok = c > 0
		case ">=":
			ok = c >= 0
		}
		if !ok {
			return false, nil
		}
	}
	return true, nil
}

// baseOnlyWhere extracts the WHERE conjuncts that reference only the base
// side, rewritten to bare column names so the single-table planner can
// absorb them into the base scan. Safe under LEFT joins: side 0 is always
// the preserved side, so filtering its rows early never changes the
// result. Conjuncts on later sides are NEVER pushed — pushing into a
// LEFT-joined side would filter before NULL-extension.
func baseOnlyWhere(sides []joinSide, where []parser.Comparison) ([]parser.Comparison, error) {
	var out []parser.Comparison
	for _, cmp := range where {
		if cmp.Column == "" {
			continue // constant conjuncts stay in the post-join filter
		}
		ref, err := resolveJoinRef(sides, cmp.Column)
		if err != nil {
			return nil, err
		}
		if ref.side != 0 {
			continue
		}
		bare := cmp
		bare.Column = ref.col.Name
		out = append(out, bare)
	}
	return out, nil
}

// makeJoinSides pairs every descriptor with its alias and checks for
// duplicates across all sides.
func makeJoinSides(baseDesc *catalog.TableDescriptor, innerDescs []*catalog.TableDescriptor, t *parser.Select) ([]joinSide, error) {
	sides := make([]joinSide, 0, 1+len(t.Joins))
	base := joinSide{desc: baseDesc, alias: baseDesc.Name}
	if t.Alias != "" {
		base.alias = t.Alias
	}
	sides = append(sides, base)
	for i := range t.Joins {
		js := joinSide{desc: innerDescs[i], alias: innerDescs[i].Name, left: t.Joins[i].Left}
		if t.Joins[i].Alias != "" {
			js.alias = t.Joins[i].Alias
		}
		sides = append(sides, js)
	}
	for i := range sides {
		for j := i + 1; j < len(sides); j++ {
			if sides[i].alias == sides[j].alias {
				return nil, newErrf(CodeDuplicateTable, "table name %q specified more than once", sides[i].alias)
			}
		}
	}
	return sides, nil
}

// resolveJoinQuery looks up and privilege-checks every joined table and
// resolves each level's ON conditions.
func (s *Session) resolveJoinQuery(ctx context.Context, txn *kvclient.Txn, baseDesc *catalog.TableDescriptor, t *parser.Select) ([]joinSide, [][]onPair, error) {
	innerDescs := make([]*catalog.TableDescriptor, len(t.Joins))
	for i := range t.Joins {
		desc, err := s.cat.Lookup(ctx, txn, t.Joins[i].Table)
		if err != nil {
			return nil, nil, err
		}
		if err := s.checkTablePriv(ctx, txn, desc, "SELECT"); err != nil {
			return nil, nil, err
		}
		innerDescs[i] = desc
	}
	sides, err := makeJoinSides(baseDesc, innerDescs, t)
	if err != nil {
		return nil, nil, err
	}
	ons := make([][]onPair, len(t.Joins))
	for i := range t.Joins {
		on, err := resolveOn(sides, i+1, t.Joins[i].On)
		if err != nil {
			return nil, nil, err
		}
		ons[i] = on
	}
	return sides, ons, nil
}

func (s *Session) execJoinSelect(ctx context.Context, txn *kvclient.Txn, baseDesc *catalog.TableDescriptor, t *parser.Select, params []types.Datum) (*Result, error) {
	if t.ForUpdate {
		return nil, newErrf(CodeFeatureNotSupported, "FOR UPDATE is not allowed with joins")
	}
	sides, ons, err := s.resolveJoinQuery(ctx, txn, baseDesc, t)
	if err != nil {
		return nil, err
	}
	grouped := hasAggregates(t.Exprs) || len(t.GroupBy) > 0 || len(t.Having) > 0
	var proj []joinProj
	if !grouped {
		if proj, err = resolveJoinProjection(sides, t.Exprs); err != nil {
			return nil, err
		}
	}
	baseWhere, err := baseOnlyWhere(sides, t.Where)
	if err != nil {
		return nil, err
	}

	baseRows, _, err := s.fetchRows(ctx, txn, sides[0].desc, baseWhere, params, 0)
	if err != nil {
		return nil, err
	}
	joined := make([]joinedRow, 0, len(baseRows))
	for _, fr := range baseRows {
		joined = append(joined, joinedRow{rows: []map[catalog.ColumnID]types.Datum{fr.row}})
	}

	// One left-deep level at a time: every partial row either fans out
	// over that side's matches, NULL-extends (LEFT), or drops (INNER).
	for k := 1; k < len(sides); k++ {
		var next []joinedRow
		for _, jr := range joined {
			// Synthetic equality predicate from the partial row's join
			// keys. A NULL key (including a NULL-extended earlier side)
			// matches nothing (SQL equality).
			nullKey := false
			synth := make([]parser.Comparison, 0, len(ons[k-1]))
			for _, p := range ons[k-1] {
				d := jr.datum(joinRef{side: p.leftSide, col: p.leftCol})
				if d.Null {
					nullKey = true
					break
				}
				lit := d
				synth = append(synth, parser.Comparison{
					Column: p.rightCol.Name, Op: "=", Value: parser.Expr{Lit: &lit},
				})
			}
			var matches []fetchedRow
			if !nullKey {
				matches, err = s.fetchRowsInner(ctx, txn, sides[k].desc, synth)
				if err != nil {
					return nil, err
				}
			}
			if len(matches) == 0 {
				if sides[k].left {
					next = append(next, jr.extend(nil))
				}
				continue
			}
			for _, ifr := range matches {
				next = append(next, jr.extend(ifr.row))
			}
		}
		joined = next
	}

	// The full WHERE filters complete rows only (LEFT JOIN semantics:
	// after NULL-extension).
	filtered := joined[:0]
	for _, jr := range joined {
		match, err := evalJoinWhere(sides, t.Where, jr, params)
		if err != nil {
			return nil, err
		}
		if match {
			filtered = append(filtered, jr)
		}
	}
	joined = filtered

	if grouped {
		return s.execGroupedJoin(sides, joined, t, params)
	}

	if len(t.OrderBy) > 0 {
		if err := sortJoinedRows(sides, joined, t.OrderBy); err != nil {
			return nil, err
		}
	}

	res := &Result{}
	for _, p := range proj {
		res.Columns = append(res.Columns, ResultColumn{Name: p.name, Type: p.ref.col.Type})
	}
	for _, jr := range joined {
		out := make([]types.Datum, len(proj))
		for i, p := range proj {
			out[i] = jr.datum(p.ref)
		}
		res.Rows = append(res.Rows, out)
	}
	if t.Distinct {
		res.Rows = dedupeRows(res.Rows)
	}
	if t.Limit > 0 && int64(len(res.Rows)) > t.Limit {
		res.Rows = res.Rows[:t.Limit]
	}
	res.Tag = fmt.Sprintf("SELECT %d", len(res.Rows))
	return res, nil
}

// fetchRowsInner runs the per-row inner lookup through the normal planner
// (point/index lookups when the join key hits that side's PK or an index;
// full scan otherwise).
func (s *Session) fetchRowsInner(ctx context.Context, txn *kvclient.Txn, desc *catalog.TableDescriptor, synth []parser.Comparison) ([]fetchedRow, error) {
	rows, _, err := s.fetchRows(ctx, txn, desc, synth, nil, 0)
	return rows, err
}

// sortJoinedRows sorts joined rows by possibly-qualified ORDER BY columns
// (PostgreSQL NULL ordering: NULLS LAST ascending, NULLS FIRST descending).
func sortJoinedRows(sides []joinSide, rows []joinedRow, order []parser.OrderCol) error {
	refs := make([]joinRef, len(order))
	for i, oc := range order {
		ref, err := resolveJoinRef(sides, oc.Column)
		if err != nil {
			return err
		}
		refs[i] = ref
	}
	var sortErr error
	sort.SliceStable(rows, func(a, b int) bool {
		for i, oc := range order {
			da, db := rows[a].datum(refs[i]), rows[b].datum(refs[i])
			if da.Null || db.Null {
				if da.Null == db.Null {
					continue
				}
				return db.Null != oc.Desc
			}
			c, err := da.Compare(db)
			if err != nil {
				if sortErr == nil {
					sortErr = err
				}
				return false
			}
			if c == 0 {
				continue
			}
			if oc.Desc {
				return c > 0
			}
			return c < 0
		}
		return false
	})
	if sortErr != nil {
		return newErrf(CodeInternal, "ORDER BY: %v", sortErr)
	}
	return nil
}

// innerPathDesc renders a join side's access path for EXPLAIN without
// bound values (per-row join keys are not literals).
func innerPathDesc(plan accessPlan) string {
	switch plan.kind {
	case planPKPoint:
		return "point lookup on primary key"
	case planUniquePoint:
		return fmt.Sprintf("point lookup via unique index %q", plan.idx.Name)
	case planPKScan:
		return fmt.Sprintf("range scan of primary key (%d column prefix)", len(plan.idxVals))
	case planIndexScan:
		return fmt.Sprintf("scan of index %q (%d column prefix) + primary key join", plan.idx.Name, len(plan.idxVals))
	}
	return "full table scan"
}

// explainJoin renders the join plan: the base access path (with real
// bounds) and each level's per-row path implied by its ON keys.
func (s *Session) explainJoin(ctx context.Context, txn *kvclient.Txn, baseDesc *catalog.TableDescriptor, t *parser.Select, params []types.Datum) (string, error) {
	sides, ons, err := s.resolveJoinQuery(ctx, txn, baseDesc, t)
	if err != nil {
		return "", err
	}
	grouped := hasAggregates(t.Exprs) || len(t.GroupBy) > 0 || len(t.Having) > 0
	if grouped {
		if _, _, _, err := groupedJoinQuery(sides, t); err != nil {
			return "", err
		}
	} else if _, err := resolveJoinProjection(sides, t.Exprs); err != nil {
		return "", err
	}
	baseWhere, err := baseOnlyWhere(sides, t.Where)
	if err != nil {
		return "", err
	}
	basePlan, err := pickPlan(sides[0].desc, baseWhere, params)
	if err != nil {
		return "", err
	}
	kind := "inner"
	if sides[1].left {
		kind = "left"
	}
	var b strings.Builder
	fmt.Fprintf(&b, "nested loop %s join; outer (%s): %s", kind, sides[0].alias, basePlan.String())
	for k := 1; k < len(sides); k++ {
		// Placeholder non-NULL datums of the join-key columns' own types
		// give pickPlan the same shape the per-row lookups will use.
		synth := make([]parser.Comparison, 0, len(ons[k-1]))
		for _, p := range ons[k-1] {
			lit := types.Datum{Fam: p.rightCol.Type}
			synth = append(synth, parser.Comparison{
				Column: p.rightCol.Name, Op: "=", Value: parser.Expr{Lit: &lit},
			})
		}
		plan, err := pickPlan(sides[k].desc, synth, nil)
		if err != nil {
			return "", err
		}
		if k == 1 {
			fmt.Fprintf(&b, "; inner (%s) per outer row: %s", sides[k].alias, innerPathDesc(plan))
			continue
		}
		lk := "inner"
		if sides[k].left {
			lk = "left"
		}
		fmt.Fprintf(&b, "; then %s (%s) per row: %s", lk, sides[k].alias, innerPathDesc(plan))
	}
	if grouped {
		b.WriteString("; then group/aggregate over the joined rows")
	}
	return b.String(), nil
}
