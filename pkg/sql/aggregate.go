package sql

import (
	"context"
	"encoding/binary"
	"fmt"
	"math"
	"sort"
	"strings"

	"github.com/sthorne/datax/pkg/kvclient"
	"github.com/sthorne/datax/pkg/sql/catalog"
	"github.com/sthorne/datax/pkg/sql/parser"
	"github.com/sthorne/datax/pkg/sql/types"
	"github.com/sthorne/datax/pkg/util/decimal"
)

// Aggregates (COUNT/SUM/AVG/MIN/MAX, no GROUP BY) and ORDER BY.

func hasAggregates(exprs []parser.SelectExpr) bool {
	for _, se := range exprs {
		if se.Agg != "" {
			return true
		}
	}
	return false
}

// aggSpec is one resolved aggregate output.
type aggSpec struct {
	fn   string // COUNT SUM AVG MIN MAX
	star bool
	col  catalog.Column // zero for COUNT(*)
	name string
}

// resolveAggSpec validates and resolves one aggregate select item.
func resolveAggSpec(desc *catalog.TableDescriptor, se parser.SelectExpr) (aggSpec, error) {
	sp := aggSpec{fn: se.Agg, star: se.AggStar, name: se.Alias}
	if sp.name == "" {
		sp.name = strings.ToLower(se.Agg)
	}
	if !se.AggStar {
		col, ok := desc.Col(se.AggCol)
		if !ok {
			return sp, newErrf(CodeUndefinedColumn, "column %q does not exist", se.AggCol)
		}
		if (se.Agg == "SUM" || se.Agg == "AVG") && col.Type != types.Int && col.Type != types.Float && col.Type != types.Decimal {
			return sp, newErrf(CodeFeatureNotSupported, "%s over %s is not supported", se.Agg, col.Type)
		}
		if (se.Agg == "MIN" || se.Agg == "MAX") && col.Type == types.Jsonb {
			return sp, newErrf(CodeFeatureNotSupported, "%s over %s is not supported (JSONB has no order)", se.Agg, col.Type)
		}
		sp.col = col
	}
	return sp, nil
}

// sameSpec reports whether two aggregate computations are identical (so a
// HAVING aggregate can reuse a projected one's state).
func sameSpec(a, b aggSpec) bool {
	return a.fn == b.fn && a.star == b.star && a.col.ID == b.col.ID
}

func (sp aggSpec) resultType() types.Family {
	switch sp.fn {
	case "COUNT":
		return types.Int
	case "AVG":
		if sp.col.Type == types.Decimal {
			return types.Decimal // exact, quantized to 6 fractional digits
		}
		return types.Float
	}
	return sp.col.Type
}

// aggState is the streaming accumulator for one group's aggregates.
type aggState struct {
	counts []int64
	sumI   []int64
	sumF   []float64
	sumD   []decimal.Dec // exact register for DECIMAL SUM/AVG
	best   []types.Datum // MIN/MAX candidate; zero = none yet
	seen   []bool
}

func newAggState(n int) *aggState {
	return &aggState{
		counts: make([]int64, n),
		sumI:   make([]int64, n),
		sumF:   make([]float64, n),
		sumD:   make([]decimal.Dec, n),
		best:   make([]types.Datum, n),
		seen:   make([]bool, n),
	}
}

func (st *aggState) accumulate(specs []aggSpec, row map[catalog.ColumnID]types.Datum) error {
	for i, sp := range specs {
		if sp.star {
			st.counts[i]++
			continue
		}
		d, ok := row[sp.col.ID]
		if !ok || d.Null {
			continue
		}
		d, cerr := d.Coerce(sp.col.Type)
		if cerr != nil {
			return newErrf(CodeInternal, "column %q: %v", sp.col.Name, cerr)
		}
		st.counts[i]++
		switch sp.fn {
		case "SUM", "AVG":
			switch sp.col.Type {
			case types.Int:
				st.sumI[i] += d.I
			case types.Decimal:
				v, err := d.DecimalVal()
				if err != nil {
					return newErrf(CodeInternal, "column %q: %v", sp.col.Name, err)
				}
				st.sumD[i] = decimal.Add(st.sumD[i], v)
			default:
				st.sumF[i] += d.F
			}
		case "MIN", "MAX":
			if !st.seen[i] {
				st.best[i], st.seen[i] = d, true
				continue
			}
			c, err := d.Compare(st.best[i])
			if err != nil {
				return newErrf(CodeInternal, "%v", err)
			}
			if (sp.fn == "MIN" && c < 0) || (sp.fn == "MAX" && c > 0) {
				st.best[i] = d
			}
		}
	}
	return nil
}

func (st *aggState) finish(specs []aggSpec) []types.Datum {
	out := make([]types.Datum, len(specs))
	for i, sp := range specs {
		switch sp.fn {
		case "COUNT":
			out[i] = types.NewInt(st.counts[i])
		case "SUM":
			switch {
			case st.counts[i] == 0:
				out[i] = types.DNull
			case sp.col.Type == types.Int:
				out[i] = types.NewInt(st.sumI[i])
			case sp.col.Type == types.Decimal:
				out[i] = types.NewDecimal(st.sumD[i].String())
			default:
				out[i] = types.NewFloat(st.sumF[i])
			}
		case "AVG":
			switch {
			case st.counts[i] == 0:
				out[i] = types.DNull
			case sp.col.Type == types.Decimal:
				q, err := decimal.DivQuantize(st.sumD[i], decimal.FromInt(st.counts[i]), 6)
				if err != nil {
					out[i] = types.DNull
				} else {
					out[i] = types.NewDecimal(q.String())
				}
			default:
				total := st.sumF[i]
				if sp.col.Type == types.Int {
					total = float64(st.sumI[i])
				}
				out[i] = types.NewFloat(total / float64(st.counts[i]))
			}
		case "MIN", "MAX":
			if !st.seen[i] {
				out[i] = types.DNull
			} else {
				out[i] = st.best[i]
			}
		}
	}
	return out
}

// groupedOut is one output column of a grouped SELECT: either a group key
// position or an aggregate spec position.
type groupedOut struct {
	name     string
	typ      types.Family
	groupPos int // ≥0 → group key position; else -1
	aggPos   int // ≥0 → aggregate spec position; else -1
}

// havingRef is one resolved HAVING conjunct.
type havingRef struct {
	op       string
	value    parser.Expr
	groupPos int
	aggPos   int
}

// groupedQuery is the resolved shape of a grouped/aggregate SELECT.
type groupedQuery struct {
	groupCols []catalog.Column
	specs     []aggSpec // projected aggregates first, then HAVING-only ones
	outs      []groupedOut
	having    []havingRef
}

// resolveGrouped resolves the select list, GROUP BY, and HAVING of a
// grouped/aggregate SELECT (standard rule: every non-aggregate output must
// appear in GROUP BY).
func resolveGrouped(desc *catalog.TableDescriptor, t *parser.Select) (*groupedQuery, error) {
	gq := &groupedQuery{}
	groupIdx := map[string]int{} // column name → group key position
	for _, name := range t.GroupBy {
		col, ok := desc.Col(name)
		if !ok {
			return nil, newErrf(CodeUndefinedColumn, "column %q does not exist", name)
		}
		if _, dup := groupIdx[name]; !dup {
			groupIdx[name] = len(gq.groupCols)
			gq.groupCols = append(gq.groupCols, col)
		}
	}

	for _, se := range t.Exprs {
		switch {
		case se.Star:
			return nil, newErrf(CodeGrouping, "SELECT * is not allowed with GROUP BY or aggregates")
		case se.Agg != "":
			sp, err := resolveAggSpec(desc, se)
			if err != nil {
				return nil, err
			}
			gq.outs = append(gq.outs, groupedOut{name: sp.name, typ: sp.resultType(), groupPos: -1, aggPos: len(gq.specs)})
			gq.specs = append(gq.specs, sp)
		default:
			if se.Expr.Column == "" || se.Expr.BinOp != "" || len(se.Expr.Path) > 0 {
				return nil, newErrf(CodeFeatureNotSupported, "grouped SELECT items must be plain columns or aggregates")
			}
			pos, ok := groupIdx[se.Expr.Column]
			if !ok {
				return nil, newErrf(CodeGrouping, "column %q must appear in the GROUP BY clause or be used in an aggregate function", se.Expr.Column)
			}
			name := se.Alias
			if name == "" {
				name = se.Expr.Column
			}
			col, _ := desc.Col(se.Expr.Column)
			gq.outs = append(gq.outs, groupedOut{name: name, typ: col.Type, groupPos: pos, aggPos: -1})
		}
	}

	for _, hc := range t.Having {
		ref := havingRef{op: hc.Op, value: hc.Value, groupPos: -1, aggPos: -1}
		if hc.Agg != nil {
			sp, err := resolveAggSpec(desc, *hc.Agg)
			if err != nil {
				return nil, err
			}
			for i := range gq.specs {
				if sameSpec(gq.specs[i], sp) {
					ref.aggPos = i
					break
				}
			}
			if ref.aggPos < 0 {
				ref.aggPos = len(gq.specs)
				gq.specs = append(gq.specs, sp) // computed, not projected
			}
		} else if pos, ok := groupIdx[hc.Column]; ok {
			ref.groupPos = pos
		} else {
			// An output name (alias or default aggregate name).
			for _, oc := range gq.outs {
				if oc.name == hc.Column {
					ref.groupPos, ref.aggPos = oc.groupPos, oc.aggPos
					break
				}
			}
			if ref.groupPos < 0 && ref.aggPos < 0 {
				return nil, newErrf(CodeGrouping, "column %q must appear in the GROUP BY clause or be used in an aggregate function", hc.Column)
			}
		}
		gq.having = append(gq.having, ref)
	}
	return gq, nil
}

// encodeGroupKey builds a collision-free hash key from group datums. NULLs
// are their own value, so NULL keys group together (SQL semantics).
func encodeGroupKey(ds []types.Datum) string {
	var b []byte
	for _, d := range ds {
		switch {
		case d.Null:
			b = append(b, 'n')
		case d.Fam == types.Int, d.Fam == types.Timestamp, d.Fam == types.Date:
			b = append(b, 'i', byte(d.Fam))
			b = binary.BigEndian.AppendUint64(b, uint64(d.I))
		case d.Fam == types.Float:
			b = append(b, 'f')
			b = binary.BigEndian.AppendUint64(b, math.Float64bits(d.F))
		case d.Fam == types.Bool:
			if d.B {
				b = append(b, 'b', 1)
			} else {
				b = append(b, 'b', 0)
			}
		default:
			b = append(b, 's')
			b = binary.AppendUvarint(b, uint64(len(d.S)))
			b = append(b, d.S...)
		}
	}
	return string(b)
}

// execGroupedSelect executes a SELECT with aggregates and/or GROUP BY:
// hash-group over the fetched rows on the group columns' datums, streaming
// aggregate state per group, HAVING post-aggregation, then ORDER BY/LIMIT
// over the output rows.
func (s *Session) execGroupedSelect(ctx context.Context, txn *kvclient.Txn, desc *catalog.TableDescriptor, t *parser.Select, params []types.Datum, corr []correlatedConjunct) (*Result, error) {
	rows, _, err := s.fetchRows(ctx, txn, desc, t.Where, params, 0)
	if err != nil {
		return nil, err
	}
	if len(corr) > 0 {
		// Correlated conjuncts filter the input rows before grouping —
		// COUNT(*) WHERE EXISTS (...) counts the survivors.
		memo := corrMemo{}
		kept := rows[:0]
		for _, fr := range rows {
			match, cerr := s.evalCorrelated(ctx, txn, corr, desc, fr.row, params, memo)
			if cerr != nil {
				return nil, cerr
			}
			if match {
				kept = append(kept, fr)
			}
		}
		rows = kept
	}
	return s.execGroupedOver(desc, rows, t, params)
}

// execGroupedOver runs the grouping/aggregation pipeline over already
// fetched (or materialized) rows.
func (s *Session) execGroupedOver(desc *catalog.TableDescriptor, rows []fetchedRow, t *parser.Select, params []types.Datum) (*Result, error) {
	gq, err := resolveGrouped(desc, t)
	if err != nil {
		return nil, err
	}

	type aggGroup struct {
		key []types.Datum
		st  *aggState
	}
	groups := map[string]*aggGroup{}
	var order []string // first-seen group order
	for _, fr := range rows {
		key := make([]types.Datum, len(gq.groupCols))
		for i, col := range gq.groupCols {
			d, ok := fr.row[col.ID]
			if !ok {
				d = types.DNull
			}
			key[i] = d
		}
		k := encodeGroupKey(key)
		g, ok := groups[k]
		if !ok {
			g = &aggGroup{key: key, st: newAggState(len(gq.specs))}
			groups[k] = g
			order = append(order, k)
		}
		if err := g.st.accumulate(gq.specs, fr.row); err != nil {
			return nil, err
		}
	}
	// Without GROUP BY, aggregates over zero rows still produce one row.
	if len(gq.groupCols) == 0 && len(order) == 0 {
		k := encodeGroupKey(nil)
		groups[k] = &aggGroup{st: newAggState(len(gq.specs))}
		order = append(order, k)
	}

	res := &Result{}
	for _, oc := range gq.outs {
		res.Columns = append(res.Columns, ResultColumn{Name: oc.name, Type: oc.typ})
	}
	for _, k := range order {
		g := groups[k]
		aggVals := g.st.finish(gq.specs)
		keep := true
		for _, ref := range gq.having {
			var lhs types.Datum
			if ref.aggPos >= 0 {
				lhs = aggVals[ref.aggPos]
			} else {
				lhs = g.key[ref.groupPos]
			}
			match, err := compareDatum(lhs, ref.op, ref.value, params)
			if err != nil {
				return nil, err
			}
			if !match {
				keep = false
				break
			}
		}
		if !keep {
			continue
		}
		out := make([]types.Datum, len(gq.outs))
		for i, oc := range gq.outs {
			if oc.groupPos >= 0 {
				out[i] = g.key[oc.groupPos]
			} else {
				out[i] = aggVals[oc.aggPos]
			}
		}
		res.Rows = append(res.Rows, out)
	}

	if t.Distinct {
		res.Rows = dedupeRows(res.Rows)
	}
	if len(t.OrderBy) > 0 {
		if err := sortResultRows(res.Columns, res.Rows, t.OrderBy); err != nil {
			return nil, err
		}
	}
	if t.Limit > 0 && int64(len(res.Rows)) > t.Limit {
		res.Rows = res.Rows[:t.Limit]
	}
	res.Tag = fmt.Sprintf("SELECT %d", len(res.Rows))
	return res, nil
}

// compareDatum mirrors matchesWhere's comparison semantics for one value:
// NULLs never match, the RHS coerces to the LHS's family.
func compareDatum(lhs types.Datum, op string, value parser.Expr, params []types.Datum) (bool, error) {
	rhs, err := evalExpr(value, nil, nil, params)
	if err != nil {
		return false, err
	}
	if lhs.Null || rhs.Null {
		return false, nil
	}
	rhs, cerr := rhs.Coerce(lhs.Fam)
	if cerr != nil {
		return false, newErrf(CodeInternal, "HAVING: %v", cerr)
	}
	c, err := lhs.Compare(rhs)
	if err != nil {
		return false, nil
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
	return false, nil
}

// dedupeRows removes duplicate output rows, keeping first occurrences in
// order (SELECT DISTINCT).
func dedupeRows(rows [][]types.Datum) [][]types.Datum {
	seen := map[string]bool{}
	out := rows[:0]
	for _, r := range rows {
		k := encodeGroupKey(r)
		if seen[k] {
			continue
		}
		seen[k] = true
		out = append(out, r)
	}
	return out
}

// sortResultRows sorts output rows by result-column names (grouped and
// DISTINCT selects order by what they produce, not by table columns). NULL
// ordering matches sortRows: NULLS LAST ascending, NULLS FIRST descending.
func sortResultRows(cols []ResultColumn, rows [][]types.Datum, order []parser.OrderCol) error {
	idx := make([]int, len(order))
	for i, oc := range order {
		found := -1
		for j, c := range cols {
			if c.Name == oc.Column {
				found = j
				break
			}
		}
		if found < 0 {
			return newErrf(CodeUndefinedColumn, "ORDER BY column %q is not in the select list", oc.Column)
		}
		idx[i] = found
	}
	var sortErr error
	sort.SliceStable(rows, func(a, b int) bool {
		for i, oc := range order {
			da, db := rows[a][idx[i]], rows[b][idx[i]]
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

// ---------------------------------------------------------------------------
// ORDER BY.

// orderSatisfiedByPlan reports whether the access path already returns rows
// in the requested order: an all-ascending ORDER BY whose columns are a
// prefix of the path's natural order (primary key for primary paths,
// indexed columns — then the primary key for non-unique indexes — for index
// scans).
func orderSatisfiedByPlan(desc *catalog.TableDescriptor, plan accessPlan, order []parser.OrderCol) bool {
	for _, oc := range order {
		if oc.Desc {
			return false
		}
	}
	var natural []catalog.ColumnID
	switch plan.kind {
	case planFullScan, planPKPoint, planPKScan:
		natural = desc.PrimaryKey
	case planUniquePoint:
		return true // at most one row
	case planIndexScan:
		natural = append([]catalog.ColumnID(nil), plan.idx.ColumnIDs...)
		if !plan.idx.Unique {
			natural = append(natural, desc.PrimaryKey...)
		}
	}
	if len(order) > len(natural) {
		return false
	}
	for i, oc := range order {
		col, ok := desc.Col(oc.Column)
		if !ok || natural[i] != col.ID {
			return false
		}
	}
	return true
}

// sortRows sorts in place by the ORDER BY terms. NULL ordering follows
// PostgreSQL's default: NULLS LAST ascending, NULLS FIRST descending.
func sortRows(desc *catalog.TableDescriptor, rows []fetchedRow, order []parser.OrderCol) error {
	cols := make([]catalog.Column, len(order))
	for i, oc := range order {
		col, ok := desc.Col(oc.Column)
		if !ok {
			return newErrf(CodeUndefinedColumn, "column %q does not exist", oc.Column)
		}
		cols[i] = col
	}
	var sortErr error
	sort.SliceStable(rows, func(a, b int) bool {
		for i, oc := range order {
			da, okA := rows[a].row[cols[i].ID]
			db, okB := rows[b].row[cols[i].ID]
			nullA := !okA || da.Null
			nullB := !okB || db.Null
			if nullA || nullB {
				if nullA == nullB {
					continue
				}
				// ASC: NULLS LAST → null sorts after; DESC: NULLS FIRST.
				return nullB != oc.Desc
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
