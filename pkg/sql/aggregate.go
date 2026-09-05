package sql

import (
	"context"
	"encoding/binary"
	"fmt"
	"github.com/sthorne/datax/pkg/sql/builtins"
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
		if se.Agg != "" && se.Window == nil {
			return true
		}
	}
	return false
}

// aggSpec is one resolved aggregate output.
type aggSpec struct {
	fn   string // COUNT SUM AVG MIN MAX STRING_AGG ARRAY_AGG BOOL_AND ...
	star bool
	col  catalog.Column // the plain-column argument (zero otherwise)
	arg  *parser.Expr   // the expression argument (nil for a column or *)
	// argType is the argument's family (the column's, or inferred).
	argType  types.Family
	extra    []parser.Expr // string_agg's separator, jsonb_object_agg's value
	distinct bool
	filter   []parser.Comparison
	order    *parser.OrderCol // WITHIN GROUP (ORDER BY ...): the ordered-set input
	name     string
	key      string // identity, for HAVING reuse
}

// resolveAggSpec validates and resolves one aggregate select item.
func resolveAggSpec(desc *catalog.TableDescriptor, se parser.SelectExpr) (aggSpec, error) {
	sp := aggSpec{fn: se.Agg, star: se.AggStar, name: se.Alias, distinct: se.AggDistinct, extra: se.AggArgs, filter: se.AggFilter}
	if sp.name == "" {
		sp.name = strings.ToLower(se.Agg)
	}
	colType := func(n string) (types.Family, bool) {
		c, ok := desc.Col(stripQualifier(n))
		return c.Type, ok
	}
	switch {
	case se.AggStar:
	case se.AggCol != "":
		col, ok := desc.Col(se.AggCol)
		if !ok {
			return sp, newErrf(CodeUndefinedColumn, "column %q does not exist", se.AggCol)
		}
		sp.col, sp.argType = col, col.Type
	case se.AggArg != nil:
		sp.arg, sp.argType = se.AggArg, exprFamily(*se.AggArg, colType)
	default:
		return sp, newErrf(CodeSyntaxError, "%s() requires an argument", se.Agg)
	}
	switch sp.fn {
	case "PERCENTILE_CONT", "PERCENTILE_DISC":
		if len(se.AggOrder) != 1 {
			return sp, newErrf(CodeSyntaxError, "%s requires WITHIN GROUP (ORDER BY ...)", strings.ToLower(sp.fn))
		}
		oc := se.AggOrder[0]
		sp.order = &oc
		if oc.Expr != nil {
			sp.argType = exprFamily(*oc.Expr, colType)
		} else if c, ok := desc.Col(oc.Column); ok {
			sp.argType = c.Type
		} else {
			return sp, newErrf(CodeUndefinedColumn, "column %q does not exist", oc.Column)
		}
	case "STRING_AGG":
		if len(sp.extra) != 1 {
			return sp, newErrf(CodeSyntaxError, "string_agg() takes a value and a separator")
		}
	case "JSONB_OBJECT_AGG", "JSON_OBJECT_AGG":
		if len(sp.extra) != 1 {
			return sp, newErrf(CodeSyntaxError, "%s() takes a key and a value", strings.ToLower(sp.fn))
		}
	case "SUM", "AVG", "STDDEV", "STDDEV_SAMP", "STDDEV_POP", "VARIANCE", "VAR_SAMP", "VAR_POP":
		if sp.argType != types.Unknown && sp.argType != types.Int && sp.argType != types.Float && sp.argType != types.Decimal &&
			!(sp.argType == types.IntervalFam && (se.Agg == "SUM" || se.Agg == "AVG")) {
			return sp, newErrf(CodeFeatureNotSupported, "%s over %s is not supported", se.Agg, sp.argType)
		}
	case "MIN", "MAX":
		if sp.argType == types.Jsonb {
			return sp, newErrf(CodeFeatureNotSupported, "%s over %s is not supported (JSONB has no order)", se.Agg, sp.argType)
		}
	case "BOOL_AND", "BOOL_OR", "EVERY":
		if sp.argType != types.Unknown && sp.argType != types.Bool {
			return sp, newErrf(CodeFeatureNotSupported, "%s over %s is not supported", se.Agg, sp.argType)
		}
	}
	var kb strings.Builder
	kb.WriteString(sp.fn)
	if sp.star {
		kb.WriteString("(*)")
	} else if sp.arg != nil {
		kb.WriteString("(" + parser.FormatExpr(*sp.arg) + ")")
	} else {
		fmt.Fprintf(&kb, "(#%d)", sp.col.ID)
	}
	if sp.distinct {
		kb.WriteString(" distinct")
	}
	for _, e := range sp.extra {
		kb.WriteString("," + parser.FormatExpr(e))
	}
	if sp.order != nil {
		if sp.order.Expr != nil {
			kb.WriteString(" order " + parser.FormatExpr(*sp.order.Expr))
		} else {
			kb.WriteString(" order " + sp.order.Column)
		}
	}
	fmt.Fprintf(&kb, " filter%d", len(sp.filter))
	sp.key = kb.String()
	return sp, nil
}

// sameSpec reports whether two aggregate computations are identical (so a
// HAVING aggregate can reuse a projected one's state).
func sameSpec(a, b aggSpec) bool {
	return a.key == b.key
}

func (sp aggSpec) resultType() types.Family {
	switch sp.fn {
	case "COUNT":
		return types.Int
	case "AVG":
		if sp.argType == types.Decimal || sp.argType == types.Int {
			return types.Decimal // exact, quantized to 6 fractional digits
		}
		if sp.argType == types.IntervalFam {
			return types.IntervalFam
		}
		return types.Float
	case "SUM":
		if sp.argType == types.Unknown {
			return types.Decimal
		}
		return sp.argType
	case "STRING_AGG", "ARRAY_AGG":
		return types.String
	case "BOOL_AND", "BOOL_OR", "EVERY":
		return types.Bool
	case "STDDEV", "STDDEV_SAMP", "STDDEV_POP", "VARIANCE", "VAR_SAMP", "VAR_POP", "PERCENTILE_CONT":
		return types.Float
	case "JSON_AGG", "JSONB_AGG", "JSON_OBJECT_AGG", "JSONB_OBJECT_AGG":
		return types.Jsonb
	}
	if sp.argType == types.Unknown {
		return types.String
	}
	return sp.argType
}

// aggState is the streaming accumulator for one group's aggregates.
type aggState struct {
	counts []int64
	sumI   []int64
	sumF   []float64
	sumD   []decimal.Dec    // exact register for DECIMAL SUM/AVG
	sumIv  []types.Interval // INTERVAL SUM/AVG
	best   []types.Datum    // MIN/MAX candidate; zero = none yet
	seen   []bool
	// vals collects the inputs of the aggregates that need them all
	// (string_agg, array_agg, the json ones, percentiles, stddev);
	// keys pairs them for the object aggregates; distinct dedupes.
	vals     [][]types.Datum
	keys     [][]types.Datum
	distinct []map[string]bool
	all      []bool // bool_and / every
	any      []bool // bool_or
}

func newAggState(n int) *aggState {
	return &aggState{
		counts:   make([]int64, n),
		sumI:     make([]int64, n),
		sumF:     make([]float64, n),
		sumD:     make([]decimal.Dec, n),
		sumIv:    make([]types.Interval, n),
		best:     make([]types.Datum, n),
		seen:     make([]bool, n),
		vals:     make([][]types.Datum, n),
		keys:     make([][]types.Datum, n),
		distinct: make([]map[string]bool, n),
		all:      make([]bool, n),
		any:      make([]bool, n),
	}
}

// collects reports whether the aggregate keeps every input value.
func (sp aggSpec) collects() bool {
	switch sp.fn {
	case "STRING_AGG", "ARRAY_AGG", "JSON_AGG", "JSONB_AGG", "JSON_OBJECT_AGG", "JSONB_OBJECT_AGG",
		"PERCENTILE_CONT", "PERCENTILE_DISC", "STDDEV", "STDDEV_SAMP", "STDDEV_POP", "VARIANCE", "VAR_SAMP", "VAR_POP":
		return true
	}
	return false
}

// input evaluates the aggregate's argument for a row.
func (sp aggSpec) input(desc *catalog.TableDescriptor, row map[catalog.ColumnID]types.Datum, params []types.Datum) (types.Datum, error) {
	switch {
	case sp.order != nil:
		if sp.order.Expr != nil {
			return evalExpr(*sp.order.Expr, desc, row, params)
		}
		c, _ := desc.Col(sp.order.Column)
		d, ok := row[c.ID]
		if !ok {
			return types.DNull, nil
		}
		return d, nil
	case sp.arg != nil:
		return evalExpr(*sp.arg, desc, row, params)
	}
	d, ok := row[sp.col.ID]
	if !ok {
		return types.DNull, nil
	}
	return d, nil
}

func (st *aggState) accumulate(specs []aggSpec, desc *catalog.TableDescriptor, row map[catalog.ColumnID]types.Datum, params []types.Datum) error {
	for i, sp := range specs {
		if len(sp.filter) > 0 {
			ok, err := matchesWhere(sp.filter, desc, row, params)
			if err != nil {
				return err
			}
			if !ok {
				continue
			}
		}
		if sp.star {
			st.counts[i]++
			continue
		}
		d, err := sp.input(desc, row, params)
		if err != nil {
			return err
		}
		// The object aggregates pair a key with a value; the json
		// aggregates keep NULL inputs (as JSON null).
		if sp.fn == "JSON_OBJECT_AGG" || sp.fn == "JSONB_OBJECT_AGG" {
			if d.Null {
				return newErrf(CodeInvalidParameterValue, "field name must not be null")
			}
			v, err := evalExpr(sp.extra[0], desc, row, params)
			if err != nil {
				return err
			}
			st.keys[i] = append(st.keys[i], d)
			st.vals[i] = append(st.vals[i], v)
			continue
		}
		if d.Null {
			if sp.fn == "JSON_AGG" || sp.fn == "JSONB_AGG" || sp.fn == "ARRAY_AGG" {
				st.vals[i] = append(st.vals[i], d)
			}
			continue
		}
		if sp.argType != types.Unknown && d.Fam != sp.argType {
			if c, cerr := d.Coerce(sp.argType); cerr == nil {
				d = c
			}
		}
		if sp.distinct {
			if st.distinct[i] == nil {
				st.distinct[i] = map[string]bool{}
			}
			k := encodeGroupKey([]types.Datum{d})
			if st.distinct[i][k] {
				continue
			}
			st.distinct[i][k] = true
		}
		st.counts[i]++
		if sp.collects() {
			st.vals[i] = append(st.vals[i], d)
			continue
		}
		switch sp.fn {
		case "SUM", "AVG":
			switch d.Fam {
			case types.Int:
				s := st.sumI[i] + d.I
				if (s > st.sumI[i]) != (d.I > 0) && d.I != 0 {
					return newErrf(CodeNumericValueOutOfRange, "bigint out of range in %s", strings.ToLower(sp.fn))
				}
				st.sumI[i] = s
				st.sumD[i] = decimal.Add(st.sumD[i], decimal.FromInt(d.I))
			case types.Decimal:
				v, err := d.DecimalVal()
				if err != nil {
					return newErrf(CodeInternal, "%s: %v", sp.name, err)
				}
				st.sumD[i] = decimal.Add(st.sumD[i], v)
			case types.IntervalFam:
				st.sumIv[i] = st.sumIv[i].Add(d.IntervalVal())
			default:
				f, err := d.Coerce(types.Float)
				if err != nil {
					return newErrf(CodeFeatureNotSupported, "%s over %s is not supported", sp.fn, d.Fam)
				}
				st.sumF[i] += f.F
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
		case "BOOL_AND", "EVERY", "BOOL_OR":
			b, err := d.Coerce(types.Bool)
			if err != nil {
				return newErrf(CodeFeatureNotSupported, "%s over %s is not supported", sp.fn, d.Fam)
			}
			if !st.seen[i] {
				st.all[i], st.any[i], st.seen[i] = true, false, true
			}
			st.all[i] = st.all[i] && b.B
			st.any[i] = st.any[i] || b.B
		}
	}
	return nil
}

func (st *aggState) finish(specs []aggSpec, params []types.Datum) ([]types.Datum, error) {
	out := make([]types.Datum, len(specs))
	for i, sp := range specs {
		switch sp.fn {
		case "COUNT":
			out[i] = types.NewInt(st.counts[i])
		case "SUM":
			switch {
			case st.counts[i] == 0:
				out[i] = types.DNull
			case sp.argType == types.Int:
				out[i] = types.NewInt(st.sumI[i])
			case sp.argType == types.Decimal, sp.argType == types.Unknown:
				out[i] = types.NewDecimal(st.sumD[i].String())
			case sp.argType == types.IntervalFam:
				out[i] = types.NewInterval(st.sumIv[i])
			default:
				out[i] = types.NewFloat(st.sumF[i])
			}
		case "AVG":
			switch {
			case st.counts[i] == 0:
				out[i] = types.DNull
			case sp.argType == types.IntervalFam:
				out[i] = types.NewInterval(st.sumIv[i].Scale(1 / float64(st.counts[i])))
			case sp.argType == types.Decimal || sp.argType == types.Int:
				q, err := decimal.DivQuantize(st.sumD[i], decimal.FromInt(st.counts[i]), 6)
				if err != nil {
					out[i] = types.DNull
				} else {
					out[i] = types.NewDecimal(q.String())
				}
			default:
				out[i] = types.NewFloat(st.sumF[i] / float64(st.counts[i]))
			}
		case "MIN", "MAX":
			if !st.seen[i] {
				out[i] = types.DNull
			} else {
				out[i] = st.best[i]
			}
		case "BOOL_AND", "EVERY":
			if !st.seen[i] {
				out[i] = types.DNull
			} else {
				out[i] = types.NewBool(st.all[i])
			}
		case "BOOL_OR":
			if !st.seen[i] {
				out[i] = types.DNull
			} else {
				out[i] = types.NewBool(st.any[i])
			}
		case "STRING_AGG":
			if len(st.vals[i]) == 0 {
				out[i] = types.DNull
				continue
			}
			sep, err := evalExpr(sp.extra[0], nil, nil, params)
			if err != nil {
				return nil, err
			}
			parts := make([]string, len(st.vals[i]))
			for j, v := range st.vals[i] {
				parts[j] = v.Text()
			}
			out[i] = types.NewString(strings.Join(parts, sep.Text()))
		case "ARRAY_AGG":
			if len(st.vals[i]) == 0 {
				out[i] = types.DNull
				continue
			}
			elems := make([]string, len(st.vals[i]))
			nulls := make([]bool, len(st.vals[i]))
			for j, v := range st.vals[i] {
				elems[j], nulls[j] = v.Text(), v.Null
			}
			out[i] = types.NewString(builtins.TextArrayLiteral(elems, nulls))
		case "JSON_AGG", "JSONB_AGG":
			if len(st.vals[i]) == 0 {
				out[i] = types.DNull
				continue
			}
			d, err := builtins.JSONArrayOf(st.vals[i])
			if err != nil {
				return nil, builtinErr(err)
			}
			out[i] = d
		case "JSON_OBJECT_AGG", "JSONB_OBJECT_AGG":
			if len(st.vals[i]) == 0 {
				out[i] = types.DNull
				continue
			}
			d, err := builtins.JSONObjectOf(st.keys[i], st.vals[i])
			if err != nil {
				return nil, builtinErr(err)
			}
			out[i] = d
		case "STDDEV", "STDDEV_SAMP", "STDDEV_POP", "VARIANCE", "VAR_SAMP", "VAR_POP":
			out[i] = statistic(sp.fn, st.vals[i])
		case "PERCENTILE_CONT", "PERCENTILE_DISC":
			frac, err := aggFraction(sp, params)
			if err != nil {
				return nil, err
			}
			d, err := percentile(sp, frac, st.vals[i])
			if err != nil {
				return nil, err
			}
			out[i] = d
		}
	}
	return out, nil
}

// aggFraction evaluates a percentile's constant fraction argument.
func aggFraction(sp aggSpec, params []types.Datum) (float64, error) {
	var d types.Datum
	var err error
	if sp.arg != nil {
		d, err = evalExpr(*sp.arg, nil, nil, params)
	} else {
		return 0, newErrf(CodeFeatureNotSupported, "%s takes a constant fraction", strings.ToLower(sp.fn))
	}
	if err != nil {
		return 0, err
	}
	f, err := d.Coerce(types.Float)
	if err != nil || f.F < 0 || f.F > 1 {
		return 0, newErrf(CodeInvalidParameterValue, "percentile value %s is not between 0 and 1", d.Text())
	}
	return f.F, nil
}

// percentile computes the ordered-set aggregates over the collected
// values: _cont interpolates between neighbors, _disc takes the first
// value at or past the fraction.
func percentile(sp aggSpec, frac float64, vals []types.Datum) (types.Datum, error) {
	if len(vals) == 0 {
		return types.DNull, nil
	}
	sorted := append([]types.Datum(nil), vals...)
	var sortErr error
	sort.SliceStable(sorted, func(a, b int) bool {
		c, err := sorted[a].Compare(sorted[b])
		if err != nil {
			sortErr = err
		}
		if sp.order.Desc {
			return c > 0
		}
		return c < 0
	})
	if sortErr != nil {
		return types.Datum{}, newErrf(CodeFeatureNotSupported, "%s: %v", strings.ToLower(sp.fn), sortErr)
	}
	n := len(sorted)
	if sp.fn == "PERCENTILE_DISC" {
		idx := int(math.Ceil(frac*float64(n))) - 1
		if idx < 0 {
			idx = 0
		}
		return sorted[idx], nil
	}
	pos := frac * float64(n-1)
	lo := int(math.Floor(pos))
	hi := int(math.Ceil(pos))
	lf, err := sorted[lo].Coerce(types.Float)
	if err != nil {
		return types.Datum{}, newErrf(CodeFeatureNotSupported, "percentile_cont requires numeric input, got %s", sorted[lo].Fam)
	}
	if lo == hi {
		return lf, nil
	}
	hf, err := sorted[hi].Coerce(types.Float)
	if err != nil {
		return types.Datum{}, newErrf(CodeFeatureNotSupported, "percentile_cont requires numeric input, got %s", sorted[hi].Fam)
	}
	return types.NewFloat(lf.F + (pos-float64(lo))*(hf.F-lf.F)), nil
}

// statistic computes the variance family over the collected values.
func statistic(fn string, vals []types.Datum) types.Datum {
	n := float64(len(vals))
	sample := fn == "STDDEV" || fn == "STDDEV_SAMP" || fn == "VARIANCE" || fn == "VAR_SAMP"
	if len(vals) == 0 || (sample && len(vals) < 2) {
		return types.DNull
	}
	var mean float64
	fs := make([]float64, len(vals))
	for i, v := range vals {
		f, err := v.Coerce(types.Float)
		if err != nil {
			return types.DNull
		}
		fs[i] = f.F
		mean += f.F
	}
	mean /= n
	var ss float64
	for _, f := range fs {
		ss += (f - mean) * (f - mean)
	}
	div := n
	if sample {
		div = n - 1
	}
	v := ss / div
	if strings.HasPrefix(fn, "STDDEV") {
		v = math.Sqrt(v)
	}
	return types.NewFloat(v)
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
	specs     []aggSpec // projected aggregates first, then HAVING/ORDER BY-only ones
	outs      []groupedOut
	having    []havingRef
	// hidden are ORDER BY keys the output does not carry (an aggregate
	// call, an unprojected grouping column), computed after outs and
	// dropped after the sort; order is the ORDER BY rewritten onto them.
	hidden []groupedOut
	order  []parser.OrderCol
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

	// ORDER BY terms the output does not carry — an aggregate call, or a
	// grouping column that is not projected — become hidden sort keys:
	// computed with the group, sorted by, then dropped.
	gq.order = append([]parser.OrderCol(nil), t.OrderBy...)
	outName := func(name string) bool {
		for _, oc := range gq.outs {
			if oc.name == name {
				return true
			}
		}
		return false
	}
	for i, oc := range t.OrderBy {
		hidden := groupedOut{name: fmt.Sprintf("__order%d", i), groupPos: -1, aggPos: -1}
		switch {
		case oc.Agg != nil:
			sp, err := resolveAggSpec(desc, *oc.Agg)
			if err != nil {
				return nil, err
			}
			for j := range gq.specs {
				if sameSpec(gq.specs[j], sp) {
					hidden.aggPos = j
					break
				}
			}
			if hidden.aggPos < 0 {
				hidden.aggPos = len(gq.specs)
				gq.specs = append(gq.specs, sp)
			}
			hidden.typ = sp.resultType()
		case oc.Column != "" && !outName(oc.Column):
			pos, ok := groupIdx[oc.Column]
			if !ok {
				continue // an unknown name: the sorter reports it
			}
			hidden.groupPos, hidden.typ = pos, gq.groupCols[pos].Type
		default:
			continue
		}
		gq.hidden = append(gq.hidden, hidden)
		gq.order[i] = parser.OrderCol{Column: hidden.name, Desc: oc.Desc, Nulls: oc.Nulls}
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
		case d.Fam == types.Int, d.Fam == types.Timestamp, d.Fam == types.Date, d.Fam == types.Time:
			b = append(b, 'i', byte(d.Fam))
			b = binary.BigEndian.AppendUint64(b, uint64(d.I))
		case d.Fam == types.IntervalFam:
			// Equal intervals ('30 days', '1 month') are one group, as
			// they compare equal.
			b = append(b, 'v')
			b = binary.BigEndian.AppendUint64(b, uint64(d.IntervalVal().CmpValue()))
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
		if err := g.st.accumulate(gq.specs, desc, fr.row, params); err != nil {
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
	for _, oc := range gq.hidden {
		res.Columns = append(res.Columns, ResultColumn{Name: oc.name, Type: oc.typ})
	}
	visible := len(gq.outs)
	for _, k := range order {
		g := groups[k]
		aggVals, err := g.st.finish(gq.specs, params)
		if err != nil {
			return nil, err
		}
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
		out := make([]types.Datum, 0, len(res.Columns))
		for _, oc := range append(append([]groupedOut(nil), gq.outs...), gq.hidden...) {
			if oc.groupPos >= 0 {
				out = append(out, g.key[oc.groupPos])
			} else {
				out = append(out, aggVals[oc.aggPos])
			}
		}
		res.Rows = append(res.Rows, out)
	}

	s.note("group/aggregate: %d groups from %d rows", len(res.Rows), len(rows))
	if t.Distinct {
		res.Rows = dedupeRowsPrefix(res.Rows, visible)
	}
	if len(gq.order) > 0 {
		if err := sortResultRows(res.Columns, res.Rows, gq.order, params); err != nil {
			return nil, err
		}
	}
	if len(gq.hidden) > 0 {
		res.Columns = res.Columns[:visible]
		for i := range res.Rows {
			res.Rows[i] = res.Rows[i][:visible]
		}
	}
	res.Rows = trimRows(res.Rows, t)
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
	if !plainCmpOp(op) {
		return applyCmpOp(op, lhs, rhs)
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
	return dedupeRowsPrefix(rows, -1)
}

// dedupeRowsPrefix is dedupeRows keyed on the first n datums of each row
// (every datum when n is negative): hidden sort keys do not distinguish
// rows.
func dedupeRowsPrefix(rows [][]types.Datum, n int) [][]types.Datum {
	seen := map[string]bool{}
	out := rows[:0]
	for _, r := range rows {
		key := r
		if n >= 0 && n < len(r) {
			key = r[:n]
		}
		k := encodeGroupKey(key)
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
func sortResultRows(cols []ResultColumn, rows [][]types.Datum, order []parser.OrderCol, params []types.Datum) error {
	idx := make([]int, len(order))
	var desc *catalog.TableDescriptor
	for i, oc := range order {
		switch {
		case oc.Agg != nil:
			return newErrf(CodeGrouping, "aggregate functions in ORDER BY require GROUP BY or an aggregated select list")
		case oc.Expr != nil:
			idx[i] = -1
			if desc == nil {
				desc = derivedDesc("", cols)
			}
			continue
		case oc.Position > 0:
			if oc.Position > len(cols) {
				return newErrf(CodeUndefinedColumn, "ORDER BY position %d is not in the select list", oc.Position)
			}
			idx[i] = oc.Position - 1
			continue
		}
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
	// Sort keys: the referenced output values, or expressions over the
	// output columns evaluated once per row.
	keys := make([][]types.Datum, len(rows))
	for r, row := range rows {
		keys[r] = make([]types.Datum, len(order))
		var env map[catalog.ColumnID]types.Datum
		for i, oc := range order {
			if idx[i] >= 0 {
				keys[r][i] = row[idx[i]]
				continue
			}
			if env == nil {
				env = make(map[catalog.ColumnID]types.Datum, len(row))
				for j, d := range row {
					env[catalog.ColumnID(j+1)] = d
				}
			}
			d, err := evalExpr(*oc.Expr, desc, env, params)
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
	sorted := make([][]types.Datum, len(rows))
	for i, p := range perm {
		sorted[i] = rows[p]
	}
	copy(rows, sorted)
	return nil
}

// nullsFirst reports where a term's NULLs sort: as written (NULLS FIRST
// | LAST), else PostgreSQL's default — last ascending, first descending.
func nullsFirst(oc parser.OrderCol) bool {
	switch oc.Nulls {
	case "first":
		return true
	case "last":
		return false
	}
	return oc.Desc
}

// ---------------------------------------------------------------------------
// ORDER BY.

// orderDecision is how (and whether) an access path delivers the
// requested ORDER BY without an in-memory sort.
type orderDecision struct {
	satisfied bool // no in-memory sort needed
	// mergeFan: a sharded fan-out delivers the order via a K-way merge of
	// the per-bucket scans (each bucket's scan is in logical-PK order).
	mergeFan bool
	// reverse: the scan(s) run reversed (an all-descending ORDER BY).
	// Set only when the caller allowed reverse scans (the cluster-version
	// gate); without it a descending order falls back to the sort.
	reverse bool
}

// orderPlan decides whether the access path already returns rows in the
// requested order: skipping columns the plan pins to a single value by
// equality (constants order nothing), the remaining ORDER BY columns
// must be, in sequence, the path's natural order after that pinned
// prefix (primary key for primary paths — the LOGICAL primary key for a
// sharded fan-out, restored by a K-way merge; indexed columns, then the
// primary key for non-unique indexes, for index scans), with every
// non-pinned term ascending — or every one descending when reverse scans
// are available (reverseOK). Mixed directions always sort.
func orderPlan(desc *catalog.TableDescriptor, plan accessPlan, order []parser.OrderCol, reverseOK bool) orderDecision {
	if len(order) == 0 {
		return orderDecision{satisfied: true}
	}
	for _, oc := range order {
		if oc.Expr != nil || oc.Agg != nil || oc.Position > 0 {
			return orderDecision{} // computed keys always sort in memory
		}
		if oc.Nulls != "" && (oc.Nulls == "first") != oc.Desc {
			return orderDecision{} // the key order puts NULLs the other way
		}
	}

	var natural []catalog.ColumnID
	fanned := false
	switch plan.kind {
	case planFullScan, planPKPoint:
		// Physical order — on a sharded table that leads with the hidden
		// shard column, which no ORDER BY names, so it stays unsatisfied.
		natural = desc.PrimaryKey
	case planPKScan:
		natural = desc.PrimaryKey
		if plan.fanBuckets > 0 {
			// Each bucket's scan is in logical-PK order; the executor's
			// K-way merge restores the global order.
			natural = natural[1:]
			fanned = true
		}
	case planUniquePoint:
		return orderDecision{satisfied: true} // at most one row
	case planIndexScan:
		natural = append([]catalog.ColumnID(nil), plan.idx.ColumnIDs...)
		if !plan.idx.Unique {
			natural = append(natural, desc.PrimaryKey...)
		}
	}

	// The plan's equality prefix pins natural[:len(idxVals)] to single
	// values (the dashboard shape: WHERE series = 'x' ORDER BY ts).
	pinned := map[catalog.ColumnID]bool{}
	for i := 0; i < len(plan.idxVals) && i < len(natural); i++ {
		pinned[natural[i]] = true
	}
	rest := natural[len(pinned):]

	dir, dirSet := false, false
	ri := 0
	for _, oc := range order {
		col, ok := desc.Col(oc.Column)
		if !ok {
			return orderDecision{}
		}
		if pinned[col.ID] {
			continue // a constant: any direction, any position
		}
		if !dirSet {
			dir, dirSet = oc.Desc, true
		} else if oc.Desc != dir {
			return orderDecision{} // mixed directions among ordering columns
		}
		if ri >= len(rest) || rest[ri] != col.ID {
			return orderDecision{}
		}
		ri++
	}
	if !dirSet {
		return orderDecision{satisfied: true} // every term pinned: any order holds
	}
	if dir && !reverseOK {
		return orderDecision{}
	}
	return orderDecision{satisfied: true, mergeFan: fanned, reverse: dir}
}

// sortRows sorts in place by the ORDER BY terms. NULL ordering follows
// PostgreSQL's default: NULLS LAST ascending, NULLS FIRST descending.
func sortRows(desc *catalog.TableDescriptor, rows []fetchedRow, order []parser.OrderCol, params []types.Datum) error {
	cols := make([]catalog.Column, len(order))
	for i, oc := range order {
		if oc.Expr != nil {
			continue
		}
		if oc.Agg != nil {
			return newErrf(CodeGrouping, "aggregate functions in ORDER BY require GROUP BY or an aggregated select list")
		}
		if oc.Position > 0 {
			visible := desc.VisibleColumns()
			if oc.Position > len(visible) {
				return newErrf(CodeUndefinedColumn, "ORDER BY position %d is not in the select list", oc.Position)
			}
			cols[i] = visible[oc.Position-1]
			continue
		}
		col, ok := desc.Col(oc.Column)
		if !ok {
			return newErrf(CodeUndefinedColumn, "column %q does not exist", oc.Column)
		}
		cols[i] = col
	}
	// Sort keys are materialized per row up front (computed expressions
	// evaluate once), then the rows are permuted to match.
	keys := make([][]types.Datum, len(rows))
	for r := range rows {
		keys[r] = make([]types.Datum, len(order))
		for i, oc := range order {
			if oc.Expr == nil {
				d, ok := rows[r].row[cols[i].ID]
				if !ok {
					d = types.DNull
				}
				keys[r][i] = d
				continue
			}
			d, err := evalExpr(*oc.Expr, desc, rows[r].row, params)
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
	sorted := make([]fetchedRow, len(rows))
	for i, p := range perm {
		sorted[i] = rows[p]
	}
	copy(rows, sorted)
	return nil
}
