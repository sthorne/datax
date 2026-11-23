package sql

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"github.com/sthorne/datax/pkg/sql/builtins"
	"math"
	"reflect"
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
		switch {
		case se.Star || se.Window != nil:
		case se.Agg != "":
			return true
		case exprHasAgg(se.Expr):
			// An aggregate call inside an expression (SUM(a) / COUNT(*)).
			return true
		}
	}
	return false
}

// fromlessGrouped reports a select without FROM that groups: one with
// an aggregate call in its select list, HAVING or ORDER BY, or a GROUP
// BY.
func fromlessGrouped(t *parser.Select) bool {
	if t.Table != "" || t.Derived != nil || t.FuncTable != nil || len(t.Joins) > 0 || t.Union != nil {
		return false
	}
	if hasAggregates(t.Exprs) || len(t.GroupBy) > 0 || len(t.Having) > 0 {
		return true
	}
	for _, oc := range t.OrderBy {
		if oc.Agg != nil || oc.Expr != nil && exprHasAgg(*oc.Expr) {
			return true
		}
	}
	return false
}

// oneRowFrom is a select without FROM rewritten to read the one row it
// stands for from a derived table: the grouped executor then runs it.
// A HAVING without an aggregate anywhere is the grouped query's one
// group kept or not, which is a WHERE over that row.
func oneRowFrom(t *parser.Select) *parser.Select {
	one := types.NewInt(1)
	c := *t
	c.Derived = &parser.Select{Limit: -1, Exprs: []parser.SelectExpr{{Expr: parser.Expr{Lit: &one}, Alias: "?column?"}}}
	c.Alias = "*SELECT*"
	if len(c.Having) > 0 && !hasAggregates(c.Exprs) && len(c.GroupBy) == 0 && !condsHaveAgg(c.Having) {
		c.Where = append(append([]parser.Comparison(nil), c.Where...), c.Having...)
		c.Having = nil
	}
	return &c
}

// condsHaveAgg reports an aggregate call anywhere in conditions.
func condsHaveAgg(conds []parser.Comparison) bool {
	for _, c := range conds {
		if condHas(c, func(x parser.Expr) bool { return x.Agg != nil }) {
			return true
		}
	}
	return false
}

// exprHasAgg reports whether an aggregate call (not a window call)
// appears in an expression, outside its subqueries.
func exprHasAgg(e parser.Expr) bool {
	return exprHas(e, func(x parser.Expr) bool { return x.Agg != nil })
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
	// sortBy is the order the aggregate consumes its input in: the
	// ORDER BY written among its arguments, then (with DISTINCT) the
	// arguments themselves, as PostgreSQL sorts a DISTINCT aggregate's
	// input. Only the aggregates whose result shows the order use it.
	sortBy []parser.OrderCol
	// sortWritten is how many of sortBy were written (the rest are the
	// DISTINCT arguments, which order as far as their type allows).
	sortWritten int
	name        string
	key         string // identity, for HAVING reuse
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
	// An ordered-set aggregate takes its direct arguments, then exactly
	// one WITHIN GROUP argument (none of them for mode); a plain
	// aggregate takes no WITHIN GROUP. Anything else names no function
	// PostgreSQL has (42883, with the argument types it looked for).
	orderedSet := sp.fn == "PERCENTILE_CONT" || sp.fn == "PERCENTILE_DISC" || sp.fn == "MODE"
	direct := len(aggArgExprs(se))
	if orderedSet != se.AggWithinGroup || (orderedSet && (len(se.AggOrder) != 1 || (sp.fn == "MODE") != (direct == 0) || direct > 1)) {
		return sp, newErrf(CodeUndefinedFunction, "function %s does not exist", aggCallSignature(desc, se))
	}
	switch {
	case se.AggStar:
	case sp.fn == "MODE":
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
	if !se.AggWithinGroup && (len(se.AggOrder) > 0 || sp.distinct) {
		if err := resolveAggSort(desc, se, &sp); err != nil {
			return sp, err
		}
	}
	switch sp.fn {
	case "PERCENTILE_CONT", "PERCENTILE_DISC", "MODE":
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
	case "JSONB_OBJECT_AGG", "JSON_OBJECT_AGG", "JSON_OBJECT_AGG_STRICT", "JSONB_OBJECT_AGG_STRICT",
		"JSON_OBJECT_AGG_UNIQUE", "JSONB_OBJECT_AGG_UNIQUE", "JSON_OBJECT_AGG_UNIQUE_STRICT", "JSONB_OBJECT_AGG_UNIQUE_STRICT":
		if len(sp.extra) != 1 {
			return sp, newErrf(CodeSyntaxError, "%s() takes a key and a value", strings.ToLower(sp.fn))
		}
	case "CORR", "COVAR_POP", "COVAR_SAMP", "REGR_AVGX", "REGR_AVGY", "REGR_COUNT", "REGR_INTERCEPT",
		"REGR_R2", "REGR_SLOPE", "REGR_SXX", "REGR_SXY", "REGR_SYY":
		// (Y, X) pairs of double precision values.
		if len(sp.extra) != 1 || !numericOrUnknown(sp.argType) || !numericOrUnknown(exprFamily(sp.extra[0], colType)) {
			return sp, newErrf(CodeUndefinedFunction, "function %s does not exist", aggCallSignature(desc, se))
		}
	case "BIT_AND", "BIT_OR", "BIT_XOR":
		if sp.argType != types.Unknown && sp.argType != types.Int {
			return sp, newErrf(CodeUndefinedFunction, "function %s does not exist", aggCallSignature(desc, se))
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
	if len(sp.filter) > 0 || len(se.AggOrder) > 0 {
		// Two calls differing only in their FILTER (or WITHIN GROUP
		// direction) are different aggregates: the key carries the
		// whole call.
		call := se
		call.Alias = ""
		if js, err := json.Marshal(call); err == nil {
			sp.key += " " + string(js)
		}
	}
	return sp, nil
}

// aggCallSignature renders an aggregate call as PostgreSQL names the
// function it looked up: name(argument types), the WITHIN GROUP
// arguments included (an untyped string literal is "unknown").
func aggCallSignature(desc *catalog.TableDescriptor, se parser.SelectExpr) string {
	colType := func(n string) (types.Family, bool) {
		c, ok := desc.Col(stripQualifier(n))
		return c.Type, ok
	}
	var names []string
	add := func(e parser.Expr) {
		if e.Lit != nil && e.Lit.Fam == types.String && !e.Lit.Null && e.Cast == "" && e.BinOp == "" && e.Func == "" {
			names = append(names, "unknown")
			return
		}
		names = append(names, pgTypeName(exprFamily(e, colType)))
	}
	if se.AggStar {
		names = append(names, "*")
	}
	for _, a := range aggArgExprs(se) {
		add(a)
	}
	if se.AggWithinGroup {
		for _, oc := range se.AggOrder {
			if oc.Expr != nil {
				add(*oc.Expr)
			} else {
				add(parser.Expr{Column: oc.Column})
			}
		}
	}
	return strings.ToLower(se.Agg) + "(" + strings.Join(names, ", ") + ")"
}

// pgTypeName is PostgreSQL's name of a family's type, for messages.
func pgTypeName(f types.Family) string {
	if f.IsArray() {
		return pgTypeName(f.Elem()) + "[]"
	}
	switch f {
	case types.Int:
		return "bigint"
	case types.Float:
		return "double precision"
	case types.String:
		return "text"
	case types.Bool:
		return "boolean"
	case types.Decimal:
		return "numeric"
	case types.Timestamp:
		return "timestamp with time zone"
	case types.Bytes:
		return "bytea"
	case types.Unknown:
		return "unknown"
	}
	return strings.ToLower(f.String())
}

// orderShown reports whether an aggregate's result shows the order of
// its input: the ones that list their inputs.
func (sp aggSpec) orderShown() bool {
	switch sp.fn {
	case "STRING_AGG", "ARRAY_AGG", "JSON_AGG", "JSONB_AGG", "JSON_OBJECT_AGG", "JSONB_OBJECT_AGG":
		return true
	}
	return strings.HasPrefix(sp.fn, "JSON")
}

// aggArgExprs lists an aggregate call's arguments as expressions.
func aggArgExprs(se parser.SelectExpr) []parser.Expr {
	var out []parser.Expr
	switch {
	case se.AggCol != "":
		out = append(out, parser.Expr{Column: se.AggCol})
	case se.AggArg != nil:
		out = append(out, *se.AggArg)
	}
	return append(out, se.AggArgs...)
}

// resolveAggSort resolves an aggregate's ORDER BY (written among its
// arguments) into sp.sortBy. With DISTINCT, each ORDER BY expression
// must be one of the arguments (42P10, as in PostgreSQL), and the
// arguments follow as further keys: PostgreSQL sorts a DISTINCT
// aggregate's input on them, so array_agg(DISTINCT x) lists x ascending.
func resolveAggSort(desc *catalog.TableDescriptor, se parser.SelectExpr, sp *aggSpec) error {
	nested := func(x parser.Expr) bool { return x.Agg != nil || x.Window != nil }
	for _, oc := range se.AggOrder {
		if oc.Expr != nil && exprHas(*oc.Expr, nested) {
			return newErrf(CodeGrouping, "aggregate function calls cannot be nested")
		}
	}
	canon := func(e parser.Expr) parser.Expr {
		return normalizeExpr(renameExprColumns(e, func(n string) string {
			if c, ok := desc.Col(n); ok {
				return fmt.Sprintf("#%d", c.ID)
			}
			if c, ok := desc.Col(stripQualifier(n)); ok {
				return fmt.Sprintf("#%d", c.ID)
			}
			return n
		}))
	}
	toExpr := func(oc parser.OrderCol) parser.Expr {
		if oc.Expr != nil {
			return *oc.Expr
		}
		return parser.Expr{Column: oc.Column}
	}
	args := aggArgExprs(se)
	if sp.distinct {
		canonArgs := make([]parser.Expr, len(args))
		for i, a := range args {
			canonArgs[i] = canon(a)
		}
		for _, oc := range se.AggOrder {
			ce := canon(toExpr(oc))
			found := false
			for _, a := range canonArgs {
				if reflect.DeepEqual(a, ce) {
					found = true
					break
				}
			}
			if !found {
				return newErrf(CodeInvalidColumnReference, "in an aggregate with DISTINCT, ORDER BY expressions must appear in argument list")
			}
		}
	}
	if !sp.orderShown() {
		// The order cannot change the result (count, sum, max, ...).
		return nil
	}
	for _, oc := range se.AggOrder {
		e := toExpr(oc)
		sp.sortBy = append(sp.sortBy, parser.OrderCol{Expr: &e, Desc: oc.Desc, Nulls: oc.Nulls})
	}
	sp.sortWritten = len(sp.sortBy)
	if sp.distinct {
		for _, a := range args {
			e := a
			sp.sortBy = append(sp.sortBy, parser.OrderCol{Expr: &e})
		}
	}
	return nil
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
	case "STRING_AGG":
		return types.String
	case "ARRAY_AGG":
		if sp.argType == types.Unknown || sp.argType.IsArray() {
			return types.ArrayOf(types.String)
		}
		return types.ArrayOf(sp.argType)
	case "BOOL_AND", "BOOL_OR", "EVERY":
		return types.Bool
	case "STDDEV", "STDDEV_SAMP", "STDDEV_POP", "VARIANCE", "VAR_SAMP", "VAR_POP", "PERCENTILE_CONT":
		return types.Float
	case "JSON_AGG", "JSONB_AGG", "JSON_OBJECT_AGG", "JSONB_OBJECT_AGG":
		return types.Jsonb
	case "REGR_COUNT", "BIT_AND", "BIT_OR", "BIT_XOR":
		return types.Int
	}
	if strings.HasPrefix(sp.fn, "JSON") {
		return types.Jsonb // the _strict and _unique forms
	}
	if pairedStat(sp.fn) {
		return types.Float
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
	sumD   []decimal.Dec // exact register for DECIMAL SUM/AVG
	// dscale is the largest display scale among a DECIMAL AVG's inputs
	// (PostgreSQL's sum carries it into the division's result scale).
	dscale []int32
	sumIv  []types.Interval // INTERVAL SUM/AVG
	// decSpecial is the NaN or infinity a DECIMAL SUM/AVG has taken (0
	// when none): it decides the result, as in PostgreSQL.
	decSpecial []float64
	best       []types.Datum // MIN/MAX candidate; zero = none yet
	seen       []bool
	// vals collects the inputs of the aggregates that need them all
	// (string_agg, array_agg, the json ones, percentiles, stddev);
	// keys pairs them for the object aggregates; distinct dedupes.
	vals     [][]types.Datum
	keys     [][]types.Datum
	distinct []map[string]bool
	all      []bool // bool_and / every
	any      []bool // bool_or
	// sortKeys pairs each collected input with its sort key, for the
	// aggregates with an input order (aggSpec.sortBy).
	sortKeys [][][]types.Datum
}

func newAggState(n int) *aggState {
	return &aggState{
		counts:     make([]int64, n),
		sumI:       make([]int64, n),
		sumF:       make([]float64, n),
		sumD:       make([]decimal.Dec, n),
		dscale:     make([]int32, n),
		sumIv:      make([]types.Interval, n),
		decSpecial: make([]float64, n),
		best:       make([]types.Datum, n),
		seen:       make([]bool, n),
		vals:       make([][]types.Datum, n),
		keys:       make([][]types.Datum, n),
		distinct:   make([]map[string]bool, n),
		all:        make([]bool, n),
		any:        make([]bool, n),
		sortKeys:   make([][][]types.Datum, n),
	}
}

// keepSortKey records the sort key of the input aggregate i just
// collected, when the aggregate orders its input.
func (st *aggState) keepSortKey(i int, sp aggSpec, desc *catalog.TableDescriptor, row map[catalog.ColumnID]types.Datum, params []types.Datum) error {
	if len(sp.sortBy) == 0 {
		return nil
	}
	key := make([]types.Datum, len(sp.sortBy))
	for j, oc := range sp.sortBy {
		d, err := evalExpr(*oc.Expr, desc, row, params)
		if err != nil {
			return err
		}
		key[j] = d
	}
	st.sortKeys[i] = append(st.sortKeys[i], key)
	return nil
}

// sortInputs puts aggregate i's collected inputs (and keys, for the
// object aggregates) in its ORDER BY order: stable, so inputs that tie
// keep their arrival order; NULLs last ascending and first descending
// unless NULLS FIRST | LAST says otherwise.
func (st *aggState) sortInputs(i int, sp aggSpec) error {
	keys := st.sortKeys[i]
	if len(sp.sortBy) == 0 || len(keys) < 2 {
		return nil
	}
	perm := make([]int, len(keys))
	for j := range perm {
		perm[j] = j
	}
	var sortErr error
	sort.SliceStable(perm, func(a, b int) bool {
		ka, kb := keys[perm[a]], keys[perm[b]]
		for j, oc := range sp.sortBy {
			da, db := ka[j], kb[j]
			if da.Null || db.Null {
				if da.Null == db.Null {
					continue
				}
				return da.Null == nullsFirst(oc)
			}
			c, err := da.Compare(db)
			if err != nil {
				if j < sp.sortWritten && sortErr == nil {
					sortErr = err
				}
				// A DISTINCT argument of a type without an order
				// (jsonb) keeps the arrival order.
				continue
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
		return newErrf(CodeFeatureNotSupported, "%s ORDER BY: %v", strings.ToLower(sp.fn), sortErr)
	}
	vals := make([]types.Datum, len(perm))
	for j, p := range perm {
		vals[j] = st.vals[i][p]
	}
	st.vals[i] = vals
	if len(st.keys[i]) == len(perm) {
		ks := make([]types.Datum, len(perm))
		for j, p := range perm {
			ks[j] = st.keys[i][p]
		}
		st.keys[i] = ks
	}
	return nil
}

// collects reports whether the aggregate keeps every input value.
func (sp aggSpec) collects() bool {
	switch sp.fn {
	case "STRING_AGG", "ARRAY_AGG", "JSON_AGG", "JSONB_AGG", "JSON_OBJECT_AGG", "JSONB_OBJECT_AGG",
		"PERCENTILE_CONT", "PERCENTILE_DISC", "MODE", "STDDEV", "STDDEV_SAMP", "STDDEV_POP", "VARIANCE", "VAR_SAMP", "VAR_POP":
		return true
	}
	return strings.HasPrefix(sp.fn, "JSON") || pairedStat(sp.fn)
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

// seenDistinct reports whether aggregate i (a DISTINCT one) already
// took these argument values, recording them otherwise.
func (st *aggState) seenDistinct(i int, args ...types.Datum) bool {
	if st.distinct[i] == nil {
		st.distinct[i] = map[string]bool{}
	}
	k := encodeGroupKey(args)
	if st.distinct[i][k] {
		return true
	}
	st.distinct[i][k] = true
	return false
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
		if pairedStat(sp.fn) {
			// (Y, X): a pair with a NULL is skipped.
			x, err := evalExpr(sp.extra[0], desc, row, params)
			if err != nil {
				return err
			}
			if d.Null || x.Null {
				continue
			}
			yf, yerr := d.Coerce(types.Float)
			xf, xerr := x.Coerce(types.Float)
			if yerr != nil || xerr != nil {
				return newErrf(CodeUndefinedFunction, "function %s(%s, %s) does not exist", strings.ToLower(sp.fn), pgTypeName(d.Fam), pgTypeName(x.Fam))
			}
			if sp.distinct && st.seenDistinct(i, yf, xf) {
				continue
			}
			st.counts[i]++
			st.vals[i] = append(st.vals[i], yf)
			st.keys[i] = append(st.keys[i], xf)
			continue
		}
		if strings.Contains(sp.fn, "OBJECT_AGG") {
			if d.Null {
				return newErrf(CodeInvalidParameterValue, "field name must not be null")
			}
			v, err := evalExpr(sp.extra[0], desc, row, params)
			if err != nil {
				return err
			}
			if v.Null && strings.HasSuffix(sp.fn, "_STRICT") {
				continue
			}
			if sp.distinct && st.seenDistinct(i, d, v) {
				continue
			}
			if err := st.keepSortKey(i, sp, desc, row, params); err != nil {
				return err
			}
			st.keys[i] = append(st.keys[i], d)
			st.vals[i] = append(st.vals[i], v)
			continue
		}
		if d.Null {
			if sp.fn == "JSON_AGG" || sp.fn == "JSONB_AGG" || sp.fn == "ARRAY_AGG" {
				// (json_agg_strict skips them, as every other aggregate.)
				if sp.distinct && st.seenDistinct(i, d) {
					continue
				}
				if err := st.keepSortKey(i, sp, desc, row, params); err != nil {
					return err
				}
				st.vals[i] = append(st.vals[i], d)
			}
			continue
		}
		if sp.argType != types.Unknown && d.Fam != sp.argType {
			if c, cerr := d.Coerce(sp.argType); cerr == nil {
				d = c
			}
		}
		if sp.distinct && st.seenDistinct(i, d) {
			continue
		}
		st.counts[i]++
		if sp.collects() {
			if err := st.keepSortKey(i, sp, desc, row, params); err != nil {
				return err
			}
			st.vals[i] = append(st.vals[i], d)
			continue
		}
		switch sp.fn {
		case "SUM", "AVG":
			switch d.Fam {
			case types.Int:
				// AVG totals in numeric (sumD), as PostgreSQL's
				// int8_avg_accum does, so it cannot overflow; only SUM's
				// bigint total can.
				if sp.fn == "SUM" {
					s := st.sumI[i] + d.I
					if (s > st.sumI[i]) != (d.I > 0) && d.I != 0 {
						return newErrf(CodeNumericValueOutOfRange, "bigint out of range in %s", strings.ToLower(sp.fn))
					}
					st.sumI[i] = s
				}
				st.sumD[i] = decimal.Add(st.sumD[i], decimal.FromInt(d.I))
			case types.Decimal:
				if f, ok := types.DecimalSpecial(d); ok {
					// NaN wins; Infinity and -Infinity together are NaN.
					if st.decSpecial[i] == 0 {
						st.decSpecial[i] = f
					} else {
						st.decSpecial[i] += f
					}
					continue
				}
				v, err := d.DecimalVal()
				if err != nil {
					return newErrf(CodeInternal, "%s: %v", sp.name, err)
				}
				st.sumD[i] = decimal.Add(st.sumD[i], v)
				st.dscale[i] = max(st.dscale[i], v.Scale(), d.Dscale)
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
		case "ANY_VALUE":
			if !st.seen[i] {
				st.best[i], st.seen[i] = d, true
			}
		case "BIT_AND", "BIT_OR", "BIT_XOR":
			v, err := d.Coerce(types.Int)
			if err != nil {
				return newErrf(CodeFeatureNotSupported, "%s over %s is not supported", sp.fn, d.Fam)
			}
			switch {
			case !st.seen[i]:
				st.sumI[i], st.seen[i] = v.I, true
			case sp.fn == "BIT_AND":
				st.sumI[i] &= v.I
			case sp.fn == "BIT_OR":
				st.sumI[i] |= v.I
			default:
				st.sumI[i] ^= v.I
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
		if err := st.sortInputs(i, sp); err != nil {
			return nil, err
		}
		switch sp.fn {
		case "COUNT":
			out[i] = types.NewInt(st.counts[i])
		case "SUM":
			if d, ok := types.DecimalFromSpecial(st.decSpecial[i]); ok && st.decSpecial[i] != 0 {
				out[i] = d
				continue
			}
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
			if d, ok := types.DecimalFromSpecial(st.decSpecial[i]); ok && st.decSpecial[i] != 0 {
				out[i] = d
				continue
			}
			switch {
			case st.counts[i] == 0:
				out[i] = types.DNull
			case sp.argType == types.IntervalFam:
				out[i] = types.NewInterval(st.sumIv[i].Scale(1 / float64(st.counts[i])))
			case sp.argType == types.Decimal || sp.argType == types.Int:
				// sum / count through numeric division, as
				// PostgreSQL's numeric_avg does.
				sum := st.sumD[i]
				q, err := divideDecimal(sum, decimal.FromInt(st.counts[i]), max(sum.Scale(), st.dscale[i]), 0)
				if err != nil {
					out[i] = types.DNull
				} else {
					out[i] = q
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
		case "ANY_VALUE":
			out[i] = types.DNull
			if st.seen[i] {
				out[i] = st.best[i]
			}
		case "BIT_AND", "BIT_OR", "BIT_XOR":
			out[i] = types.DNull
			if st.seen[i] {
				out[i] = types.NewInt(st.sumI[i])
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
			elem := sp.resultType().Elem()
			elems := make([]types.Datum, len(st.vals[i]))
			for j, v := range st.vals[i] {
				if v.Null {
					elems[j] = v
					continue
				}
				c, err := v.Coerce(elem)
				if err != nil {
					c = types.NewString(v.Text())
				}
				elems[j] = c
			}
			out[i] = types.NewArray(elem, elems)
		case "JSON_AGG", "JSONB_AGG", "JSON_AGG_STRICT", "JSONB_AGG_STRICT":
			if len(st.vals[i]) == 0 {
				out[i] = types.DNull
				continue
			}
			d, err := builtins.JSONArrayOf(st.vals[i])
			if err != nil {
				return nil, builtinErr(err)
			}
			out[i] = d
		case "JSON_OBJECT_AGG", "JSONB_OBJECT_AGG", "JSON_OBJECT_AGG_STRICT", "JSONB_OBJECT_AGG_STRICT",
			"JSON_OBJECT_AGG_UNIQUE", "JSONB_OBJECT_AGG_UNIQUE", "JSON_OBJECT_AGG_UNIQUE_STRICT", "JSONB_OBJECT_AGG_UNIQUE_STRICT":
			if len(st.vals[i]) == 0 {
				out[i] = types.DNull
				continue
			}
			if strings.Contains(sp.fn, "_UNIQUE") {
				seen := make(map[string]bool, len(st.keys[i]))
				for _, k := range st.keys[i] {
					if seen[k.Text()] {
						return nil, newErrf(CodeDuplicateJSONKey, "duplicate JSON object key value: %q", k.Text())
					}
					seen[k.Text()] = true
				}
			}
			d, err := builtins.JSONObjectOf(st.keys[i], st.vals[i])
			if err != nil {
				return nil, builtinErr(err)
			}
			out[i] = d
		case "STDDEV", "STDDEV_SAMP", "STDDEV_POP", "VARIANCE", "VAR_SAMP", "VAR_POP":
			out[i] = statistic(sp.fn, st.vals[i])
		case "CORR", "COVAR_POP", "COVAR_SAMP", "REGR_AVGX", "REGR_AVGY", "REGR_COUNT", "REGR_INTERCEPT",
			"REGR_R2", "REGR_SLOPE", "REGR_SXX", "REGR_SXY", "REGR_SYY":
			out[i] = pairedStatistic(sp.fn, st.vals[i], st.keys[i])
		case "MODE":
			d, err := modeOf(sp, st.vals[i])
			if err != nil {
				return nil, err
			}
			out[i] = d
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

// sortedInputs sorts an ordered-set aggregate's collected (non-NULL)
// values in its WITHIN GROUP order.
func sortedInputs(sp aggSpec, vals []types.Datum) ([]types.Datum, error) {
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
		return nil, newErrf(CodeFeatureNotSupported, "%s: %v", strings.ToLower(sp.fn), sortErr)
	}
	return sorted, nil
}

// modeOf computes mode(): the most frequent value, the first in the
// WITHIN GROUP order among equally frequent ones (as PostgreSQL's
// mode_final picks).
func modeOf(sp aggSpec, vals []types.Datum) (types.Datum, error) {
	if len(vals) == 0 {
		return types.DNull, nil
	}
	sorted, err := sortedInputs(sp, vals)
	if err != nil {
		return types.Datum{}, err
	}
	best, bestN := sorted[0], 0
	for i := 0; i < len(sorted); {
		j := i + 1
		for j < len(sorted) {
			if c, err := sorted[j].Compare(sorted[i]); err != nil || c != 0 {
				break
			}
			j++
		}
		if j-i > bestN {
			best, bestN = sorted[i], j-i
		}
		i = j
	}
	return best, nil
}

// percentile computes the ordered-set aggregates over the collected
// values: _cont interpolates between neighbors, _disc takes the first
// value at or past the fraction.
func percentile(sp aggSpec, frac float64, vals []types.Datum) (types.Datum, error) {
	if len(vals) == 0 {
		return types.DNull, nil
	}
	sorted, err := sortedInputs(sp, vals)
	if err != nil {
		return types.Datum{}, err
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

// pairedStat reports whether fn is one of the two-argument (Y, X)
// statistical aggregates.
func pairedStat(fn string) bool {
	switch fn {
	case "CORR", "COVAR_POP", "COVAR_SAMP", "REGR_AVGX", "REGR_AVGY", "REGR_COUNT", "REGR_INTERCEPT",
		"REGR_R2", "REGR_SLOPE", "REGR_SXX", "REGR_SXY", "REGR_SYY":
		return true
	}
	return false
}

func numericOrUnknown(f types.Family) bool {
	return f == types.Unknown || f == types.Int || f == types.Float || f == types.Decimal || f == types.String
}

// pairedStatistic computes corr, covar_* and regr_* over the collected
// (Y, X) pairs, with PostgreSQL's NULL rules: NULL without a pair
// (regr_count is 0), covar_samp below two pairs, and corr, regr_r2,
// regr_slope and regr_intercept when X (or for corr, Y) does not vary;
// regr_r2 is 1 when only Y does not.
func pairedStatistic(fn string, ys, xs []types.Datum) types.Datum {
	n := float64(len(ys))
	if fn == "REGR_COUNT" {
		return types.NewInt(int64(len(ys)))
	}
	if len(ys) == 0 || fn == "COVAR_SAMP" && len(ys) < 2 {
		return types.DNull
	}
	var sx, sy float64
	for i := range ys {
		sx += xs[i].F
		sy += ys[i].F
	}
	mx, my := sx/n, sy/n
	var sxx, syy, sxy float64
	for i := range ys {
		dx, dy := xs[i].F-mx, ys[i].F-my
		sxx += dx * dx
		syy += dy * dy
		sxy += dx * dy
	}
	f := func(v float64) types.Datum { return types.NewFloat(v) }
	switch fn {
	case "REGR_AVGX":
		return f(mx)
	case "REGR_AVGY":
		return f(my)
	case "REGR_SXX":
		return f(sxx)
	case "REGR_SYY":
		return f(syy)
	case "REGR_SXY":
		return f(sxy)
	case "COVAR_POP":
		return f(sxy / n)
	case "COVAR_SAMP":
		return f(sxy / (n - 1))
	case "CORR":
		if sxx == 0 || syy == 0 {
			return types.DNull
		}
		return f(sxy / math.Sqrt(sxx*syy))
	}
	if sxx == 0 {
		return types.DNull
	}
	switch fn {
	case "REGR_R2":
		if syy == 0 {
			return f(1)
		}
		return f(sxy * sxy / (sxx * syy))
	case "REGR_SLOPE":
		return f(sxy / sxx)
	}
	// REGR_INTERCEPT
	return f(my - mx*sxy/sxx)
}

// encodeGroupKey builds a collision-free hash key from group datums. NULLs
// are their own value, so NULL keys group together (SQL semantics).
func encodeGroupKey(ds []types.Datum) string {
	var b []byte
	for _, d := range ds {
		switch {
		case d.Null:
			b = append(b, 'n')
		case d.Fam == types.Int, d.Fam == types.Timestamp, d.Fam == types.Date, d.Fam == types.Time, d.Fam == types.Enum:
			b = append(b, 'i', byte(d.Fam))
			b = binary.BigEndian.AppendUint64(b, uint64(d.I))
		case d.Fam == types.IntervalFam:
			// Equal intervals ('30 days', '1 month') are one group, as
			// they compare equal.
			b = append(b, 'v')
			b = binary.BigEndian.AppendUint64(b, uint64(d.IntervalVal().CmpValue()))
		case d.Fam.IsArray():
			t := d.Text()
			b = append(b, 'a')
			b = binary.AppendUvarint(b, uint64(len(t)))
			b = append(b, t...)
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
func (s *Session) execGroupedSelect(ctx context.Context, txn *kvclient.Txn, desc *catalog.TableDescriptor, t *parser.Select, params []types.Datum, corr []correlatedConjunct, gc *groupedCorr) (*Result, error) {
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
	return s.execGroupedOver(desc, rows, t, params, gc)
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
	case planFullScan, planPKPoint, planInvertedScan, planPKPoints:
		// Physical order (an inverted scan sorts the primary keys it
		// found before fetching the rows, and point lookups are made in
		// key order) — on a sharded table that leads with the hidden
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
	ok, err := newOrderKeyer(desc, order, params)
	if err != nil {
		return err
	}
	// Sort keys are materialized per row up front (computed expressions
	// evaluate once), then the rows are permuted to match.
	keys := make([][]types.Datum, len(rows))
	for r := range rows {
		if keys[r], err = ok.key(rows[r].row); err != nil {
			return err
		}
	}
	perm := make([]int, len(rows))
	for i := range perm {
		perm[i] = i
	}
	var sortErr error
	sort.SliceStable(perm, func(a, b int) bool {
		c, err := ok.compare(keys[perm[a]], keys[perm[b]])
		if err != nil {
			if sortErr == nil {
				sortErr = err
			}
			return false
		}
		return c < 0
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

// orderKeyer computes and compares the ORDER BY keys of a table's rows:
// what the in-memory sort and the top-k heap (topk.go) both order by.
type orderKeyer struct {
	desc   *catalog.TableDescriptor
	order  []parser.OrderCol
	cols   []catalog.Column // the column behind each plain term
	params []types.Datum
}

func newOrderKeyer(desc *catalog.TableDescriptor, order []parser.OrderCol, params []types.Datum) (*orderKeyer, error) {
	cols := make([]catalog.Column, len(order))
	for i, oc := range order {
		if oc.Expr != nil {
			continue
		}
		if oc.Agg != nil {
			return nil, newErrf(CodeGrouping, "aggregate functions in ORDER BY require GROUP BY or an aggregated select list")
		}
		if oc.Position > 0 {
			visible := desc.VisibleColumns()
			if oc.Position > len(visible) {
				return nil, newErrf(CodeUndefinedColumn, "ORDER BY position %d is not in the select list", oc.Position)
			}
			cols[i] = visible[oc.Position-1]
			continue
		}
		col, ok := desc.Col(oc.Column)
		if !ok {
			return nil, newErrf(CodeUndefinedColumn, "column %q does not exist", oc.Column)
		}
		cols[i] = col
	}
	return &orderKeyer{desc: desc, order: order, cols: cols, params: params}, nil
}

// key evaluates the row's sort key, one datum per ORDER BY term.
func (k *orderKeyer) key(row map[catalog.ColumnID]types.Datum) ([]types.Datum, error) {
	out := make([]types.Datum, len(k.order))
	for i, oc := range k.order {
		if oc.Expr == nil {
			d, ok := row[k.cols[i].ID]
			if !ok {
				d = types.DNull
			}
			out[i] = d
			continue
		}
		d, err := evalExpr(*oc.Expr, k.desc, row, k.params)
		if err != nil {
			return nil, err
		}
		out[i] = d
	}
	return out, nil
}

// compare orders two sort keys: negative when ka sorts first.
func (k *orderKeyer) compare(ka, kb []types.Datum) (int, error) {
	for i, oc := range k.order {
		da, db := ka[i], kb[i]
		if da.Null || db.Null {
			if da.Null == db.Null {
				continue
			}
			if da.Null == nullsFirst(oc) {
				return -1, nil
			}
			return 1, nil
		}
		c, err := da.Compare(db)
		if err != nil {
			return 0, err
		}
		if c == 0 {
			continue
		}
		if oc.Desc {
			return -c, nil
		}
		return c, nil
	}
	return 0, nil
}
