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
	desc    *catalog.TableDescriptor
	alias   string // alias if given, else the table name
	aliased bool   // an explicit alias was written
	left    bool   // this side was LEFT-joined (side 0: false)
	// right: this side's unmatched rows are kept too (RIGHT or FULL
	// JOIN), NULL-extended on every earlier side.
	right bool
	// merged maps a column name this side joined USING (or NATURALly) to
	// the earlier side it was equated with: it shows once in SELECT *,
	// resolves unqualified to the earlier side, and reads as
	// COALESCE(earlier, this) so an outer join's unmatched rows keep it.
	merged map[string]int
}

func (js joinSide) matches(qualifier string) bool {
	return qualifier == js.alias || qualifier == js.desc.Name
}

// joinRef is a column resolved to a side; alt, when set, is the later
// side's copy of a USING column, read when this side's value is NULL.
type joinRef struct {
	side int
	col  catalog.Column
	alt  *joinRef
}

// sideRef makes the reference to side i's column col, attaching the
// later side that merged the same column USING it, if any.
func sideRef(sides []joinSide, i int, col catalog.Column) joinRef {
	ref := joinRef{side: i, col: col}
	for k := i + 1; k < len(sides); k++ {
		if m, ok := sides[k].merged[col.Name]; ok && m == i {
			if c, ok := sides[k].desc.Col(col.Name); ok {
				alt := sideRef(sides, k, c)
				ref.alt = &alt
			}
			break
		}
	}
	return ref
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
		if _, merged := js.merged[colName]; merged {
			continue // a USING column: the earlier side owns the name
		}
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
	return sideRef(sides, found, fcol), nil
}

// joinedRow holds one row per side; a nil entry is a LEFT JOIN's NULL
// extension.
type joinedRow struct {
	rows []map[catalog.ColumnID]types.Datum
}

func (jr joinedRow) datum(ref joinRef) types.Datum {
	d := types.DNull
	if ref.side < len(jr.rows) && jr.rows[ref.side] != nil {
		if v, ok := jr.rows[ref.side][ref.col.ID]; ok {
			d = v
		}
	}
	if d.Null && ref.alt != nil {
		return jr.datum(*ref.alt)
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

// joinEnv resolves side-qualified column references against one joined
// row — the join-side exprEnv. A NULL-extended LEFT side yields DNull for
// its columns, which then flows through paths and arithmetic as SQL NULL.
type joinEnv struct {
	sides []joinSide
	jr    joinedRow
}

func (j joinEnv) col(name string) (types.Datum, error) {
	ref, err := resolveJoinRef(j.sides, name)
	if err != nil {
		return types.Datum{}, err
	}
	return j.jr.datum(ref), nil
}

// joinProj is one output column of a join select: a plain column (ref),
// or a computed expression (expr, typed typ) evaluated per joined row —
// mirroring the single-table projCol shapes exactly.
type joinProj struct {
	name string
	ref  joinRef
	expr *parser.Expr
	typ  types.Family
}

// walkExprRefs resolves every column reference inside an expression at
// projection-resolve time, so bad names error before execution (and
// EXPLAIN/Describe see them without evaluating a row).
func walkExprRefs(sides []joinSide, e parser.Expr) error {
	if e.Column != "" {
		if _, err := resolveJoinRef(sides, e.Column); err != nil {
			return err
		}
	}
	if e.Left != nil {
		if err := walkExprRefs(sides, *e.Left); err != nil {
			return err
		}
	}
	if e.Right != nil {
		if err := walkExprRefs(sides, *e.Right); err != nil {
			return err
		}
	}
	for _, a := range e.Args {
		if err := walkExprRefs(sides, a); err != nil {
			return err
		}
	}
	if e.Cmp != nil {
		if e.Cmp.Expr != nil {
			if err := walkExprRefs(sides, *e.Cmp.Expr); err != nil {
				return err
			}
		} else if e.Cmp.Column != "" {
			if _, err := resolveJoinRef(sides, e.Cmp.Column); err != nil {
				return err
			}
		}
		if err := walkExprRefs(sides, e.Cmp.Value); err != nil {
			return err
		}
	}
	if e.Sub != nil {
		return newErrf(CodeFeatureNotSupported, "subqueries in join SELECT lists are not supported")
	}
	return nil
}

// resolveJoinProjection resolves the select list: * (each side's columns
// in join order, PostgreSQL order), plain possibly-qualified columns,
// ->/->> paths on jsonb columns, and computed expressions (rendered as
// TEXT, the single-table precedent). Aggregates take the grouped path and
// never reach here.
func resolveJoinProjection(sides []joinSide, exprs []parser.SelectExpr) ([]joinProj, error) {
	var proj []joinProj
	for _, se := range exprs {
		switch {
		case se.Star:
			for i, js := range sides {
				for _, c := range js.desc.VisibleColumns() {
					if _, merged := js.merged[c.Name]; merged {
						continue // shown once, from the side it was merged with
					}
					proj = append(proj, joinProj{name: c.Name, ref: sideRef(sides, i, c), typ: c.Type})
				}
			}
		case se.Agg != "":
			return nil, newErrf(CodeInternal, "aggregate reached the join projection")
		case se.Expr.Column != "" && se.Expr.BinOp == "" && se.Expr.Cast == "":
			ref, err := resolveJoinRef(sides, se.Expr.Column)
			if err != nil {
				return nil, err
			}
			name := se.Alias
			if name == "" {
				name = ref.col.Name
			}
			if len(se.Expr.Path) > 0 {
				// col -> 'k' / ->> 'k': a computed column typed by the chain.
				if ref.col.Type != types.Jsonb {
					return nil, newErrf(CodeFeatureNotSupported, "cannot extract path from type %s (-> and ->> require jsonb)", ref.col.Type)
				}
				e := se.Expr
				if se.Alias == "" {
					name = "?column?"
				}
				proj = append(proj, joinProj{name: name, expr: &e, typ: pathResultType(se.Expr.Path)})
				continue
			}
			proj = append(proj, joinProj{name: name, ref: ref, typ: ref.col.Type})
		default:
			if err := walkExprRefs(sides, se.Expr); err != nil {
				return nil, err
			}
			e := se.Expr
			name := se.Alias
			if name == "" {
				name = "?column?"
			}
			typ := exprFamily(e, func(n string) (types.Family, bool) {
				ref, err := resolveJoinRef(sides, n)
				if err != nil {
					return types.Unknown, false
				}
				return ref.col.Type, true
			})
			if typ == types.Unknown {
				typ = types.String
			}
			proj = append(proj, joinProj{name: name, expr: &e, typ: typ})
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
		if cmp.Op == "@>" || cmp.Op == "NOT @>" {
			// Containment is single-table only. This must stay an explicit
			// refusal: an unhandled operator below would silently drop
			// every row otherwise.
			return false, newErrf(CodeFeatureNotSupported, "JSONB containment @> is not supported in join queries")
		}
		if cmp.Expr != nil {
			// Computed left-hand side: evaluate both sides against the
			// joined row and compare raw, mirroring matchesWhere.
			lhs, err := evalExprEnv(*cmp.Expr, joinEnv{sides, jr}, params)
			if err != nil {
				return false, err
			}
			rhs, err := evalExprEnv(cmp.Value, joinEnv{sides, jr}, params)
			if err != nil {
				return false, err
			}
			if lhs.Null || rhs.Null {
				return false, nil
			}
			ok, err := applyCmpOp(cmp.Op, lhs, rhs)
			if err != nil || !ok {
				return false, nil
			}
			continue
		}
		if cmp.Op == "OR" {
			matched := false
			for _, disjunct := range cmp.Or {
				ok, err := evalJoinWhere(sides, disjunct, jr, params)
				if err != nil {
					return false, err
				}
				if ok {
					matched = true
					break
				}
			}
			if !matched {
				return false, nil
			}
			continue
		}
		ref, err := resolveJoinRef(sides, cmp.Column)
		if err != nil {
			return false, err
		}
		lhs := jr.datum(ref)
		// A ->/->> path replaces the column value and its comparison type
		// — applied before IS NULL and IN, mirroring matchesWhere, so a
		// missing key (or a NULL-extended LEFT side) is seen as NULL.
		cmpType := ref.col.Type
		if len(cmp.Path) > 0 {
			var perr error
			lhs, perr = applyPath(lhs, cmp.Path)
			if perr != nil {
				return false, perr
			}
			cmpType = pathResultType(cmp.Path)
		}
		if cmp.Op == "IS NULL" || cmp.Op == "IS NOT NULL" {
			if lhs.Null != (cmp.Op == "IS NULL") {
				return false, nil
			}
			continue
		}
		if cmp.Op == "IN" || cmp.Op == "NOT IN" {
			match, err := matchesIn(cmp, cmpType, lhs, nil, nil, params)
			if err != nil || !match {
				return false, err
			}
			continue
		}
		// The right side may reference joined columns too (a = b across
		// sides).
		rhs, err := evalExprEnv(cmp.Value, joinEnv{sides, jr}, params)
		if err != nil {
			return false, err
		}
		if lhs.Null || rhs.Null {
			return false, nil
		}
		if !plainCmpOp(cmp.Op) {
			ok, err := applyCmpOp(cmp.Op, lhs, rhs)
			if err != nil {
				return false, err
			}
			if !ok {
				return false, nil
			}
			continue
		}
		if cmpType == types.Enum {
			cmpType = rhs.Fam // an enum compares with a label by text
		}
		rhs, cerr := rhs.Coerce(cmpType)
		if cerr != nil {
			return false, newErrf(CodeInternal, "WHERE %s: %v", cmp.Column, cerr)
		}
		c, err := lhs.Compare(rhs)
		if err != nil {
			return false, nil
		}
		if !cmpHolds(cmp.Op, c) {
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
		if cmp.Op == "@>" || cmp.Op == "NOT @>" {
			// Refused before the base scan (so EXPLAIN errors too), and
			// never pushed — see evalJoinWhere.
			return nil, newErrf(CodeFeatureNotSupported, "JSONB containment @> is not supported in join queries")
		}
		if len(cmp.Path) > 0 {
			// Path conjuncts filter extracted values: never pushed as base
			// bounds (even for side 0) — the post-join filter re-evaluates
			// the full WHERE, so this only costs the pushdown.
			continue
		}
		ref, err := resolveJoinRef(sides, cmp.Column)
		if err != nil {
			return nil, err
		}
		if ref.side != 0 {
			continue
		}
		// A row-dependent right side (c.oid = i.indrelid) is a join
		// predicate, not a base bound: the post-join filter applies it.
		if exprHasColumn(cmp.Value) {
			continue
		}
		bare := cmp
		bare.Column = ref.col.Name
		out = append(out, bare)
	}
	return out, nil
}

// exprHasColumn reports whether a value expression references any column.
func exprHasColumn(e parser.Expr) bool {
	found := false
	exprColumns(e, func(string) { found = true })
	return found
}

// makeJoinSides pairs every descriptor with its alias and checks for
// duplicates across all sides.
func makeJoinSides(baseDesc *catalog.TableDescriptor, innerDescs []*catalog.TableDescriptor, t *parser.Select) ([]joinSide, error) {
	sides := make([]joinSide, 0, 1+len(t.Joins))
	base := joinSide{desc: baseDesc, alias: baseDesc.Name}
	if t.Alias != "" {
		base.alias, base.aliased = t.Alias, true
	}
	sides = append(sides, base)
	for i := range t.Joins {
		jc := t.Joins[i]
		js := joinSide{desc: innerDescs[i], alias: innerDescs[i].Name, left: jc.Left || jc.Full, right: jc.Right || jc.Full}
		if jc.Alias != "" {
			js.alias, js.aliased = jc.Alias, true
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

// joinQuery is a resolved join: the sides in EXECUTION order, each
// level's ON pairs, and the Select to execute — the caller's own, or a
// cost-reordered clone (reordered = true), in which case everything
// downstream (projection, WHERE, ORDER BY) must read sel, never the
// original.
type joinQuery struct {
	sides     []joinSide
	ons       [][]onPair
	sel       *parser.Select
	reordered bool
}

// resolveJoinQuery looks up and privilege-checks every joined table,
// applies cost-based join reordering when statistics allow it, and
// resolves each level's ON conditions.
func (s *Session) resolveJoinQuery(ctx context.Context, txn *kvclient.Txn, baseDesc *catalog.TableDescriptor, t *parser.Select, noReorder bool) (*joinQuery, error) {
	innerDescs := make([]*catalog.TableDescriptor, len(t.Joins))
	for i := range t.Joins {
		desc, err := s.lookup(ctx, txn, t.Joins[i].Table)
		if err != nil {
			return nil, err
		}
		if err := s.checkTablePriv(ctx, txn, desc, "SELECT"); err != nil {
			return nil, err
		}
		innerDescs[i] = desc
	}
	sides, err := makeJoinSides(baseDesc, innerDescs, t)
	if err != nil {
		return nil, err
	}
	if t, err = expandUsing(sides, t); err != nil {
		return nil, err
	}
	jq := &joinQuery{sides: sides, sel: t}
	// Reorder only with statistics for EVERY side: their absence keeps
	// the syntactic order byte-identical to the pre-statistics executor.
	stats := make([]*catalog.TableStatistics, len(sides))
	haveAll := true
	for i, js := range sides {
		st, ok := s.cat.Stats(ctx, js.desc.ID)
		if !ok {
			haveAll = false
			break
		}
		stats[i] = st
	}
	if haveAll && !noReorder {
		if clone, order, changed, ok := reorderJoins(t, sides, stats); ok && changed {
			jq.sel, jq.reordered = clone, true
			permuted := make([]joinSide, len(sides))
			for p, si := range order {
				permuted[p] = sides[si]
			}
			jq.sides = permuted
		}
	}
	jq.ons = make([][]onPair, len(jq.sel.Joins))
	for i := range jq.sel.Joins {
		on, err := resolveOn(jq.sides, i+1, jq.sel.Joins[i].On)
		if err != nil {
			return nil, err
		}
		jq.ons[i] = on
	}
	return jq, nil
}

func (s *Session) execJoinSelect(ctx context.Context, txn *kvclient.Txn, baseDesc *catalog.TableDescriptor, t *parser.Select, params []types.Datum, jc *joinCorr) (*Result, error) {
	if t.ForUpdate {
		return nil, newErrf(CodeFeatureNotSupported, "FOR UPDATE is not allowed with joins")
	}
	// Correlated subqueries bind against the syntactic side order.
	jq, err := s.resolveJoinQuery(ctx, txn, baseDesc, t, jc != nil)
	if err != nil {
		return nil, err
	}
	// From here on t is the (possibly cost-reordered) clone; sides are in
	// execution order and the star, if any, was pre-expanded to preserve
	// the original output column order.
	sides, ons := jq.sides, jq.ons
	t = jq.sel
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
	if anyRightSide(sides) {
		// A RIGHT/FULL join keeps rows the base never matched, whose base
		// columns are NULL: WHERE must see those after the join, not
		// prune the base first.
		baseWhere = nil
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
		var matchedKeys map[string]bool // RIGHT/FULL: side k's rows that found a partner
		if sides[k].right {
			matchedKeys = map[string]bool{}
		}
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
			// Non-key ON conjuncts (tc.relkind = 't', NOT a.attisdropped)
			// decide matching, not the final row set: a candidate that
			// fails them is a miss, so a LEFT side NULL-extends.
			filter := t.Joins[k-1].Filter
			matched := 0
			for _, ifr := range matches {
				ext := jr.extend(ifr.row)
				if len(filter) > 0 {
					ok, err := evalJoinWhere(sides, filter, ext, params)
					if err != nil {
						return nil, err
					}
					if !ok {
						continue
					}
				}
				next = append(next, ext)
				matched++
				if matchedKeys != nil {
					matchedKeys[string(ifr.key)] = true
				}
			}
			if matched == 0 && sides[k].left {
				next = append(next, jr.extend(nil))
			}
		}
		if sides[k].right {
			// The joined side's unmatched rows, NULL-extended on every
			// earlier side.
			all, _, err := s.fetchRows(ctx, txn, sides[k].desc, nil, params, 0)
			if err != nil {
				return nil, err
			}
			for _, fr := range all {
				if matchedKeys[string(fr.key)] {
					continue
				}
				ext := joinedRow{rows: make([]map[catalog.ColumnID]types.Datum, k+1)}
				ext.rows[k] = fr.row
				next = append(next, ext)
			}
		}
		joined = next
		s.note("join level %d (%s, %s): %d rows", k, sides[k].alias, joinKindName(sides[k], t.Joins[k-1]), len(joined))
	}

	// The full WHERE filters complete rows only (LEFT JOIN semantics:
	// after NULL-extension). Correlated conjuncts evaluate per row
	// against the merged scope.
	memo := corrMemo{}
	filtered := joined[:0]
	for _, jr := range joined {
		match, err := evalJoinWhere(sides, t.Where, jr, params)
		if err != nil {
			return nil, err
		}
		if match && jc != nil && len(jc.conds) > 0 {
			if match, err = s.evalCorrelated(ctx, txn, jc.conds, jc.desc, jc.rowOf(jr), params, memo); err != nil {
				return nil, err
			}
		}
		if match {
			filtered = append(filtered, jr)
		}
	}
	joined = filtered

	if grouped {
		if jc != nil && len(jc.projs) > 0 {
			return nil, newErrf(CodeFeatureNotSupported, "correlated subqueries in the select list are not supported with GROUP BY")
		}
		return s.execGroupedJoin(sides, joined, t, params)
	}

	if len(t.OrderBy) > 0 {
		if err := sortJoinedRows(sides, joined, resolveOrderAliases(t), params); err != nil {
			return nil, err
		}
		s.note("sort: %d joined rows in memory", len(joined))
	}

	res := &Result{}
	for _, p := range proj {
		res.Columns = append(res.Columns, exprResult(p.name, p.typ, p.ref.col))
	}
	// Output positions of the correlated expressions (a star ahead of
	// them expands to every side's visible columns).
	var projAt []int
	if jc != nil && len(jc.projs) > 0 {
		pos, n := make([]int, len(t.Exprs)), 0
		for i, se := range t.Exprs {
			pos[i] = n
			if se.Star {
				for _, js := range sides {
					n += len(js.desc.VisibleColumns())
				}
			} else {
				n++
			}
		}
		for _, cp := range jc.projs {
			projAt = append(projAt, pos[cp.idx])
		}
	}
	projMemo := corrMemo{}
	for _, jr := range joined {
		out := make([]types.Datum, len(proj))
		for i, p := range proj {
			if p.expr != nil {
				d, err := evalExprEnv(*p.expr, joinEnv{sides, jr}, params)
				if err != nil {
					return nil, err
				}
				out[i] = conformTo(d, p.typ)
				continue
			}
			out[i] = jr.datum(p.ref)
		}
		for i := range projAt {
			d, err := s.evalCorrProj(ctx, txn, &jc.projs[i], jc.desc, jc.rowOf(jr), params, projMemo)
			if err != nil {
				return nil, err
			}
			out[projAt[i]] = d
			if !d.Null {
				res.Columns[projAt[i]].Type = d.Fam
			}
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

// fetchRowsInner runs the per-row inner lookup through the normal planner
// (point/index lookups when the join key hits that side's PK or an index;
// full scan otherwise).
func (s *Session) fetchRowsInner(ctx context.Context, txn *kvclient.Txn, desc *catalog.TableDescriptor, synth []parser.Comparison) ([]fetchedRow, error) {
	rows, _, err := s.fetchRows(ctx, txn, desc, synth, nil, 0)
	return rows, err
}

// sortJoinedRows sorts joined rows by possibly-qualified ORDER BY columns
// or computed expressions (PostgreSQL NULL ordering: NULLS LAST
// ascending, NULLS FIRST descending).
func sortJoinedRows(sides []joinSide, rows []joinedRow, order []parser.OrderCol, params []types.Datum) error {
	refs := make([]joinRef, len(order))
	for i, oc := range order {
		if oc.Expr != nil {
			continue
		}
		if oc.Agg != nil {
			return newErrf(CodeGrouping, "aggregate functions in ORDER BY require GROUP BY or an aggregated select list")
		}
		if oc.Position > 0 {
			return newErrf(CodeFeatureNotSupported, "ORDER BY position after a parenthesized join query is not supported")
		}
		ref, err := resolveJoinRef(sides, oc.Column)
		if err != nil {
			return err
		}
		refs[i] = ref
	}
	// Sort keys are materialized per row up front (computed expressions
	// evaluate once), then the rows are permuted to match.
	keys := make([][]types.Datum, len(rows))
	for r := range rows {
		keys[r] = make([]types.Datum, len(order))
		for i, oc := range order {
			if oc.Expr == nil {
				keys[r][i] = rows[r].datum(refs[i])
				continue
			}
			d, err := evalExprEnv(*oc.Expr, joinEnv{sides, rows[r]}, params)
			if err != nil {
				return err
			}
			keys[r][i] = d
		}
	}
	perm := make([]int, len(rows))
	for i := range perm {
		perm[i] = i
	}
	var sortErr error
	sort.SliceStable(perm, func(a, b int) bool {
		ka, kb := keys[perm[a]], keys[perm[b]]
		for i, oc := range order {
			da, db := ka[i], kb[i]
			if da.Null || db.Null {
				if da.Null == db.Null {
					continue
				}
				return da.Null == nullsFirst(oc)
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
	sorted := make([]joinedRow, len(rows))
	for i, p := range perm {
		sorted[i] = rows[p]
	}
	copy(rows, sorted)
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
	jq, err := s.resolveJoinQuery(ctx, txn, baseDesc, t, false)
	if err != nil {
		return "", err
	}
	sides, ons := jq.sides, jq.ons
	t = jq.sel
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
	// Statistics are threaded exactly as execution threads them (fetchRows
	// does the same lookup), so EXPLAIN shows the paths execution takes;
	// a statistics-based estimate annotates each side that has one.
	baseStats, _ := s.cat.Stats(ctx, sides[0].desc.ID)
	basePlan, err := pickPlanWithStats(sides[0].desc, baseStats, baseWhere, params)
	if err != nil {
		return "", err
	}
	estimate := func(plan accessPlan) string {
		if plan.estRows > 0 {
			return fmt.Sprintf(" [~%.0f rows]", plan.estRows)
		}
		return ""
	}
	kind := joinKindName(sides[1], t.Joins[0])
	var b strings.Builder
	fmt.Fprintf(&b, "nested loop %s join; outer (%s): %s%s", kind, sides[0].alias, basePlan.String(), estimate(basePlan))
	for k := 1; k < len(sides); k++ {
		// Placeholder non-NULL datums of the join-key columns' own types
		// give the planner the same shape the per-row lookups will use.
		synth := make([]parser.Comparison, 0, len(ons[k-1]))
		for _, p := range ons[k-1] {
			lit := types.Datum{Fam: p.rightCol.Type}
			synth = append(synth, parser.Comparison{
				Column: p.rightCol.Name, Op: "=", Value: parser.Expr{Lit: &lit},
			})
		}
		sideStats, _ := s.cat.Stats(ctx, sides[k].desc.ID)
		plan, err := pickPlanWithStats(sides[k].desc, sideStats, synth, nil)
		if err != nil {
			return "", err
		}
		if k == 1 {
			fmt.Fprintf(&b, "; inner (%s) per outer row: %s%s", sides[k].alias, innerPathDesc(plan), estimate(plan))
			if sides[k].right {
				fmt.Fprintf(&b, " + unmatched %s rows appended", sides[k].alias)
			}
			continue
		}
		lk := joinKindName(sides[k], t.Joins[k-1])
		fmt.Fprintf(&b, "; then %s (%s) per row: %s%s", lk, sides[k].alias, innerPathDesc(plan), estimate(plan))
		if sides[k].right {
			fmt.Fprintf(&b, " + unmatched %s rows appended", sides[k].alias)
		}
	}
	if grouped {
		b.WriteString("; then group/aggregate over the joined rows")
	}
	if jq.reordered {
		b.WriteString("; join reordered by cost")
	}
	return b.String(), nil
}

// joinKindName names a join level for EXPLAIN.
func joinKindName(js joinSide, jc parser.JoinClause) string {
	switch {
	case js.left && js.right:
		return "full"
	case js.right:
		return "right"
	case js.left:
		return "left"
	case jc.Cross:
		return "cross"
	}
	return "inner"
}

// anyRightSide reports whether a RIGHT or FULL join is in the chain.
func anyRightSide(sides []joinSide) bool {
	for _, js := range sides {
		if js.right {
			return true
		}
	}
	return false
}

// expandUsing rewrites JOIN ... USING (cols) and NATURAL JOIN into ON
// equalities on a copy of t (the caller's statement, possibly cached,
// is never changed), marking each merged column on its side so it shows
// once and resolves to the earlier side. A NATURAL join with no common
// column is a cross join.
func expandUsing(sides []joinSide, t *parser.Select) (*parser.Select, error) {
	var out *parser.Select
	for i := range t.Joins {
		jc := t.Joins[i]
		if !jc.Natural && len(jc.Using) == 0 {
			continue
		}
		if out == nil {
			c := *t
			c.Joins = append([]parser.JoinClause(nil), t.Joins...)
			out = &c
		}
		k := i + 1
		names := jc.Using
		if jc.Natural {
			names = nil
			for _, col := range sides[k].desc.VisibleColumns() {
				for j := 0; j < k; j++ {
					if _, merged := sides[j].merged[col.Name]; merged {
						continue
					}
					if _, ok := sides[j].desc.Col(col.Name); ok {
						names = append(names, col.Name)
						break
					}
				}
			}
			if len(names) == 0 {
				out.Joins[i].Cross, out.Joins[i].Natural = true, false
				continue
			}
		}
		on := append([]parser.JoinCond(nil), out.Joins[i].On...)
		for _, name := range names {
			name = strings.ToLower(name)
			rcol, ok := sides[k].desc.Col(name)
			if !ok {
				return nil, newErrf(CodeUndefinedColumn, "column %q specified in USING clause does not exist in right table", name)
			}
			from := -1
			for j := 0; j < k; j++ {
				if _, merged := sides[j].merged[name]; merged {
					continue
				}
				if _, ok := sides[j].desc.Col(name); ok {
					from = j
					break
				}
			}
			if from < 0 {
				return nil, newErrf(CodeUndefinedColumn, "column %q specified in USING clause does not exist in left table", name)
			}
			on = append(on, parser.JoinCond{
				L: parser.ColumnRef{Table: sides[from].alias, Column: name},
				R: parser.ColumnRef{Table: sides[k].alias, Column: rcol.Name},
			})
			if sides[k].merged == nil {
				sides[k].merged = map[string]int{}
			}
			sides[k].merged[name] = from
		}
		out.Joins[i].On, out.Joins[i].Using, out.Joins[i].Natural = on, nil, false
	}
	if out == nil {
		return t, nil
	}
	return out, nil
}
