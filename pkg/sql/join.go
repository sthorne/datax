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

// Two-table nested-loop joins (INNER and LEFT OUTER). The outer side is
// fetched through the normal access-path planner with the outer-only WHERE
// conjuncts; for each outer row the ON equalities become synthetic equality
// predicates on the inner table, so the inner side reuses pickPlan — a join
// key on the inner PK or an index turns into a point/index lookup per
// outer row. The full WHERE clause re-evaluates on every joined row
// (PostgreSQL semantics: on a LEFT JOIN it filters after NULL-extension).

// joinSide names one side of the join.
type joinSide struct {
	desc  *catalog.TableDescriptor
	alias string // alias if given, else the table name
}

func (js joinSide) matches(qualifier string) bool {
	return qualifier == js.alias || qualifier == js.desc.Name
}

// joinRef is a column resolved to a side.
type joinRef struct {
	inner bool
	col   catalog.Column
}

// splitQualified splits "t.c" into ("t", "c"); no dot → ("", name).
func splitQualified(name string) (table, column string) {
	if i := strings.IndexByte(name, '.'); i >= 0 {
		return name[:i], name[i+1:]
	}
	return "", name
}

// resolveJoinRef resolves a possibly-qualified column name against the two
// join sides. Unqualified names must be unambiguous.
func resolveJoinRef(outer, inner joinSide, name string) (joinRef, error) {
	q, colName := splitQualified(name)
	if q != "" {
		switch {
		case outer.matches(q):
			if col, ok := outer.desc.Col(colName); ok {
				return joinRef{col: col}, nil
			}
			return joinRef{}, newErrf(CodeUndefinedColumn, "column %q does not exist", name)
		case inner.matches(q):
			if col, ok := inner.desc.Col(colName); ok {
				return joinRef{inner: true, col: col}, nil
			}
			return joinRef{}, newErrf(CodeUndefinedColumn, "column %q does not exist", name)
		}
		return joinRef{}, newErrf(CodeUndefinedTable, "missing FROM-clause entry for table %q", q)
	}
	oc, inOuter := outer.desc.Col(colName)
	ic, inInner := inner.desc.Col(colName)
	switch {
	case inOuter && inInner:
		return joinRef{}, newErrf(CodeAmbiguousColumn, "column reference %q is ambiguous", name)
	case inOuter:
		return joinRef{col: oc}, nil
	case inInner:
		return joinRef{inner: true, col: ic}, nil
	}
	return joinRef{}, newErrf(CodeUndefinedColumn, "column %q does not exist", name)
}

// joinedRow pairs one outer row with one inner row; a nil inner side is a
// LEFT JOIN's NULL extension.
type joinedRow struct {
	outer map[catalog.ColumnID]types.Datum
	inner map[catalog.ColumnID]types.Datum
}

func (jr joinedRow) datum(ref joinRef) types.Datum {
	row := jr.outer
	if ref.inner {
		row = jr.inner
		if row == nil {
			return types.DNull
		}
	}
	d, ok := row[ref.col.ID]
	if !ok {
		return types.DNull
	}
	return d
}

// onPair is one resolved ON equality: outer column = inner column.
type onPair struct {
	outerCol, innerCol catalog.Column
}

// resolveOn resolves the ON conjuncts; each must equate one outer column
// with one inner column.
func resolveOn(outer, inner joinSide, conds []parser.JoinCond) ([]onPair, error) {
	pairs := make([]onPair, 0, len(conds))
	for _, jc := range conds {
		l, err := resolveJoinRef(outer, inner, jc.L.String())
		if err != nil {
			return nil, err
		}
		r, err := resolveJoinRef(outer, inner, jc.R.String())
		if err != nil {
			return nil, err
		}
		if l.inner == r.inner {
			return nil, newErrf(CodeFeatureNotSupported, "ON conditions must equate a column from each table")
		}
		p := onPair{outerCol: l.col, innerCol: r.col}
		if l.inner {
			p = onPair{outerCol: r.col, innerCol: l.col}
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

// resolveJoinProjection resolves the select list: * (outer columns then
// inner columns, PostgreSQL order) or plain possibly-qualified column
// references. Expressions and aggregates over joins are not supported.
func resolveJoinProjection(outer, inner joinSide, exprs []parser.SelectExpr) ([]joinProj, error) {
	var proj []joinProj
	for _, se := range exprs {
		switch {
		case se.Star:
			for _, c := range outer.desc.VisibleColumns() {
				proj = append(proj, joinProj{name: c.Name, ref: joinRef{col: c}})
			}
			for _, c := range inner.desc.VisibleColumns() {
				proj = append(proj, joinProj{name: c.Name, ref: joinRef{inner: true, col: c}})
			}
		case se.Agg != "":
			return nil, newErrf(CodeFeatureNotSupported, "aggregates over joins are not supported")
		case se.Expr.Column != "" && se.Expr.BinOp == "":
			ref, err := resolveJoinRef(outer, inner, se.Expr.Column)
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

// evalJoinWhere evaluates the full WHERE conjunction against a joined row,
// mirroring matchesWhere (NULL comparisons never match; IS [NOT] NULL sees
// the LEFT JOIN's NULL extension).
func evalJoinWhere(outer, inner joinSide, where []parser.Comparison, jr joinedRow, params []types.Datum) (bool, error) {
	for _, cmp := range where {
		if cmp.Op == "TRUE" {
			continue
		}
		if cmp.Op == "FALSE" {
			return false, nil
		}
		ref, err := resolveJoinRef(outer, inner, cmp.Column)
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

// outerOnlyWhere extracts the WHERE conjuncts that reference only the outer
// table, rewritten to bare column names so the single-table planner can
// absorb them into the outer scan. Safe for LEFT JOIN too: outer columns
// are unchanged by NULL extension, so filtering outer rows early never
// changes the result.
func outerOnlyWhere(outer, inner joinSide, where []parser.Comparison) ([]parser.Comparison, error) {
	var out []parser.Comparison
	for _, cmp := range where {
		if cmp.Column == "" {
			continue // constant conjuncts stay in the post-join filter
		}
		ref, err := resolveJoinRef(outer, inner, cmp.Column)
		if err != nil {
			return nil, err
		}
		if ref.inner {
			continue
		}
		bare := cmp
		bare.Column = ref.col.Name
		out = append(out, bare)
	}
	return out, nil
}

// makeJoinSides pairs the two descriptors with their aliases.
func makeJoinSides(outerDesc, innerDesc *catalog.TableDescriptor, t *parser.Select) (outer, inner joinSide, err error) {
	outer = joinSide{desc: outerDesc, alias: outerDesc.Name}
	if t.Alias != "" {
		outer.alias = t.Alias
	}
	inner = joinSide{desc: innerDesc, alias: innerDesc.Name}
	if t.Join.Alias != "" {
		inner.alias = t.Join.Alias
	}
	if outer.alias == inner.alias {
		return joinSide{}, joinSide{}, newErrf(CodeDuplicateTable, "table name %q specified more than once", outer.alias)
	}
	return outer, inner, nil
}

func (s *Session) execJoinSelect(ctx context.Context, txn *kvclient.Txn, outerDesc *catalog.TableDescriptor, t *parser.Select, params []types.Datum) (*Result, error) {
	switch {
	case t.ForUpdate:
		return nil, newErrf(CodeFeatureNotSupported, "FOR UPDATE is not allowed with joins")
	case hasAggregates(t.Exprs) || len(t.GroupBy) > 0 || len(t.Having) > 0:
		return nil, newErrf(CodeFeatureNotSupported, "aggregates and GROUP BY over joins are not supported")
	}
	innerDesc, err := s.cat.Lookup(ctx, txn, t.Join.Table)
	if err != nil {
		return nil, err
	}
	if err := s.checkTablePriv(ctx, txn, innerDesc, "SELECT"); err != nil {
		return nil, err
	}
	outer, inner, err := makeJoinSides(outerDesc, innerDesc, t)
	if err != nil {
		return nil, err
	}
	on, err := resolveOn(outer, inner, t.Join.On)
	if err != nil {
		return nil, err
	}
	proj, err := resolveJoinProjection(outer, inner, t.Exprs)
	if err != nil {
		return nil, err
	}
	outerWhere, err := outerOnlyWhere(outer, inner, t.Where)
	if err != nil {
		return nil, err
	}

	outerRows, _, err := s.fetchRows(ctx, txn, outer.desc, outerWhere, params, 0)
	if err != nil {
		return nil, err
	}

	var joined []joinedRow
	for _, ofr := range outerRows {
		// Build the synthetic inner predicate from this outer row's join
		// keys. A NULL join key matches nothing (SQL equality).
		nullKey := false
		synth := make([]parser.Comparison, 0, len(on))
		for _, p := range on {
			d, ok := ofr.row[p.outerCol.ID]
			if !ok || d.Null {
				nullKey = true
				break
			}
			lit := d
			synth = append(synth, parser.Comparison{
				Column: p.innerCol.Name, Op: "=", Value: parser.Expr{Lit: &lit},
			})
		}
		var matches []fetchedRow
		if !nullKey {
			matches, err = s.fetchRowsInner(ctx, txn, inner.desc, synth)
			if err != nil {
				return nil, err
			}
		}
		if len(matches) == 0 {
			if t.Join.Left {
				jr := joinedRow{outer: ofr.row}
				match, err := evalJoinWhere(outer, inner, t.Where, jr, params)
				if err != nil {
					return nil, err
				}
				if match {
					joined = append(joined, jr)
				}
			}
			continue
		}
		for _, ifr := range matches {
			jr := joinedRow{outer: ofr.row, inner: ifr.row}
			match, err := evalJoinWhere(outer, inner, t.Where, jr, params)
			if err != nil {
				return nil, err
			}
			if match {
				joined = append(joined, jr)
			}
		}
	}

	if len(t.OrderBy) > 0 {
		if err := sortJoinedRows(outer, inner, joined, t.OrderBy); err != nil {
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

// fetchRowsInner runs the per-outer-row inner lookup through the normal
// planner (point/index lookups when the join key hits the inner PK or an
// index; full scan otherwise).
func (s *Session) fetchRowsInner(ctx context.Context, txn *kvclient.Txn, desc *catalog.TableDescriptor, synth []parser.Comparison) ([]fetchedRow, error) {
	rows, _, err := s.fetchRows(ctx, txn, desc, synth, nil, 0)
	return rows, err
}

// sortJoinedRows sorts joined rows by possibly-qualified ORDER BY columns
// (PostgreSQL NULL ordering: NULLS LAST ascending, NULLS FIRST descending).
func sortJoinedRows(outer, inner joinSide, rows []joinedRow, order []parser.OrderCol) error {
	refs := make([]joinRef, len(order))
	for i, oc := range order {
		ref, err := resolveJoinRef(outer, inner, oc.Column)
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

// innerPathDesc renders the inner side's access path for EXPLAIN without
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

// explainJoin renders the join plan: the outer access path (with real
// bounds) and the per-outer-row inner path implied by the ON keys.
func (s *Session) explainJoin(ctx context.Context, txn *kvclient.Txn, outerDesc *catalog.TableDescriptor, t *parser.Select, params []types.Datum) (string, error) {
	innerDesc, err := s.cat.Lookup(ctx, txn, t.Join.Table)
	if err != nil {
		return "", err
	}
	outer, inner, err := makeJoinSides(outerDesc, innerDesc, t)
	if err != nil {
		return "", err
	}
	on, err := resolveOn(outer, inner, t.Join.On)
	if err != nil {
		return "", err
	}
	if _, err := resolveJoinProjection(outer, inner, t.Exprs); err != nil {
		return "", err
	}
	outerWhere, err := outerOnlyWhere(outer, inner, t.Where)
	if err != nil {
		return "", err
	}
	outerPlan, err := pickPlan(outer.desc, outerWhere, params)
	if err != nil {
		return "", err
	}
	// Placeholder non-NULL datums of the join-key columns' own types give
	// pickPlan the same shape the per-row lookups will use.
	synth := make([]parser.Comparison, 0, len(on))
	for _, p := range on {
		lit := types.Datum{Fam: p.innerCol.Type}
		synth = append(synth, parser.Comparison{
			Column: p.innerCol.Name, Op: "=", Value: parser.Expr{Lit: &lit},
		})
	}
	innerPlan, err := pickPlan(inner.desc, synth, nil)
	if err != nil {
		return "", err
	}
	kind := "inner"
	if t.Join.Left {
		kind = "left"
	}
	return fmt.Sprintf("nested loop %s join; outer (%s): %s; inner (%s) per outer row: %s",
		kind, outer.alias, outerPlan.String(), inner.alias, innerPathDesc(innerPlan)), nil
}
