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

// Window functions run as a stage over the query's materialized rows:
// the statement executes with every window input — the function's
// arguments, the partition keys, the ordering keys — projected as hidden
// trailing columns and without its own DISTINCT / ORDER BY / LIMIT;
// each window item then partitions the rows on its keys, orders each
// partition, and computes its value per row (ranking functions from the
// position and peer group, offset functions from neighbours, value and
// aggregate functions over the row's frame, the aggregates through the
// grouping machinery over a synthetic descriptor of the hidden
// columns). The output splices the window values into the select list's
// positions, and DISTINCT, ORDER BY and LIMIT / OFFSET apply last.

// hasWindows reports whether a select list has a window function: as
// an item, or inside an item's expression.
func hasWindows(exprs []parser.SelectExpr) bool {
	for _, se := range exprs {
		if se.Window != nil {
			return true
		}
		if !se.Star && se.Agg == "" && exprHas(se.Expr, func(x parser.Expr) bool { return x.Window != nil }) {
			return true
		}
	}
	return false
}

// windowItem is one window call in the select list, resolved onto the
// inner query's hidden columns.
type windowItem struct {
	idx  int // position in the select list
	se   parser.SelectExpr
	spec parser.WindowSpec
	name string
	// Hidden column positions (in the inner result) of the first
	// argument, the partition keys and the ordering keys; orderTerms
	// carries direction and NULL placement for the keys. extra are the
	// further arguments: a hidden column each when they read the row, a
	// constant otherwise (a separator, an offset, a default).
	args       []int
	extra      []windowArg
	parts      []int
	orders     []int
	orderTerms []parser.OrderCol
	// aggSpec is set for an aggregate computed as a window.
	agg *aggSpec
	typ types.Family
}

// windowArg is one further argument of a window call.
type windowArg struct {
	hidden int          // hidden column, or -1
	expr   *parser.Expr // the constant expression when hidden is -1
	val    types.Datum  // its value, evaluated once
}

// windowOut is one select-list item of a windowed select: a star or a
// plain item passed through from the inner query, a window item's
// value, or an expression over window values and hidden columns.
type windowOut struct {
	star  bool
	item  int          // window item index (kind window)
	expr  *parser.Expr // kind computed: over the eval descriptor's names
	name  string
	typ   types.Family
	plain bool
}

// windowPlan is a windowed select rewritten for execution.
type windowPlan struct {
	inner  *parser.Select
	items  []windowItem
	outs   []windowOut
	hidden int // hidden columns appended to inner's output
	// orderHidden holds, per outer ORDER BY term, the hidden column that
	// carries it (-1 when the term names an output or a position), so
	// the result can sort by a column the select list does not show.
	orderHidden []int
}

// windowPlanFor rewrites t: window items leave the inner select list and
// their inputs join it as hidden columns; an expression around a window
// call keeps the call's value and the columns it reads as hidden
// columns too, and evaluates after the window stage.
func windowPlanFor(t *parser.Select) (*windowPlan, error) {
	inner := *t
	inner.Exprs = nil
	inner.Distinct, inner.OrderBy, inner.Limit, inner.Offset, inner.LimitParam, inner.OffsetParam = false, nil, -1, 0, 0, 0
	inner.Windows = nil
	wp := &windowPlan{}
	var hidden []parser.SelectExpr
	hiddenByCol := map[string]int{}
	addHidden := func(se parser.SelectExpr) int {
		se.Alias = fmt.Sprintf("__w%d", len(hidden))
		hidden = append(hidden, se)
		return len(hidden) - 1
	}
	addExpr := func(e parser.Expr) int { return addHidden(parser.SelectExpr{Expr: e}) }
	addCol := func(name string) int {
		if h, ok := hiddenByCol[name]; ok {
			return h
		}
		h := addExpr(parser.Expr{Column: name})
		hiddenByCol[name] = h
		return h
	}
	addItem := func(se parser.SelectExpr) (int, error) {
		spec, err := resolveWindowSpec(t, *se.Window)
		if err != nil {
			return 0, err
		}
		if se.AggDistinct || len(se.AggFilter) > 0 || len(se.AggOrder) > 0 {
			return 0, newErrf(CodeFeatureNotSupported, "DISTINCT, FILTER and WITHIN GROUP are not supported on window functions")
		}
		if se.AggStar && se.Agg != "COUNT" {
			return 0, newErrf(CodeSyntaxError, "%s(*) is not supported", se.Agg)
		}
		item := windowItem{idx: len(wp.items), se: se, spec: spec, name: strings.ToLower(se.Agg)}
		if se.Alias != "" {
			item.name = se.Alias
		}
		switch {
		case se.AggStar:
		case se.AggCol != "":
			item.args = append(item.args, addExpr(parser.Expr{Column: se.AggCol}))
		case se.AggArg != nil:
			item.args = append(item.args, addExpr(*se.AggArg))
		}
		for _, a := range se.AggArgs {
			if exprHasColumn(a) {
				item.extra = append(item.extra, windowArg{hidden: addExpr(a)})
			} else {
				e := a
				item.extra = append(item.extra, windowArg{hidden: -1, expr: &e})
			}
		}
		for _, e := range spec.PartitionBy {
			item.parts = append(item.parts, addExpr(e))
		}
		for _, oc := range spec.OrderBy {
			switch {
			case oc.Agg != nil:
				item.orders = append(item.orders, addHidden(*oc.Agg))
			case oc.Expr != nil:
				item.orders = append(item.orders, addExpr(*oc.Expr))
			case oc.Position > 0:
				return 0, newErrf(CodeSyntaxError, "a window ORDER BY cannot use a position")
			default:
				item.orders = append(item.orders, addExpr(parser.Expr{Column: oc.Column}))
			}
			item.orderTerms = append(item.orderTerms, oc)
		}
		if !windowFuncNames[se.Agg] {
			// An aggregate over the frame, computed through the grouping
			// machinery on the hidden columns; the spec resolves once the
			// inner columns are known (windowTypes).
			item.agg = &aggSpec{}
		}
		wp.items = append(wp.items, item)
		return len(wp.items) - 1, nil
	}
	hasWindowCall := func(e parser.Expr) bool {
		return exprHas(e, func(x parser.Expr) bool { return x.Window != nil })
	}
	for _, se := range t.Exprs {
		switch {
		case se.Star:
			inner.Exprs = append(inner.Exprs, se)
			wp.outs = append(wp.outs, windowOut{star: true})
		case se.Window != nil:
			idx, err := addItem(se)
			if err != nil {
				return nil, err
			}
			wp.outs = append(wp.outs, windowOut{item: idx})
		case se.Agg == "" && hasWindowCall(se.Expr):
			// Window calls become value columns; the expression's own
			// column references become hidden columns; the expression
			// evaluates over both after the window stage.
			var ierr error
			rewritten := replaceWindowCalls(se.Expr, func(call *parser.SelectExpr) parser.Expr {
				idx, err := addItem(*call)
				if err != nil && ierr == nil {
					ierr = err
				}
				return parser.Expr{Column: fmt.Sprintf("__wv%d", idx)}
			})
			if ierr != nil {
				return nil, ierr
			}
			if hasWindowCall(rewritten) {
				return nil, newErrf(CodeFeatureNotSupported, "window functions inside predicates or subqueries are not supported")
			}
			rewritten = renameExprColumns(rewritten, func(name string) string {
				if strings.HasPrefix(name, "__wv") {
					return name
				}
				return fmt.Sprintf("__w%d", addCol(name))
			})
			name := se.Alias
			if name == "" {
				name = "?column?"
			}
			e := rewritten
			wp.outs = append(wp.outs, windowOut{expr: &e, name: name})
		default:
			inner.Exprs = append(inner.Exprs, se)
			wp.outs = append(wp.outs, windowOut{plain: true})
		}
	}
	// Output names the outer ORDER BY can use directly (a * expands to
	// table columns, which the inner query resolves anyway).
	outputNames := map[string]bool{}
	for _, se := range t.Exprs {
		switch {
		case se.Star:
		case se.Alias != "":
			outputNames[se.Alias] = true
		case se.Window != nil:
			outputNames[strings.ToLower(se.Agg)] = true
		default:
			outputNames[parser.OutputName(se)] = true
		}
	}
	for _, oc := range t.OrderBy {
		h := -1
		switch {
		case oc.Agg != nil:
			h = addHidden(*oc.Agg)
		case oc.Expr != nil:
			h = addExpr(*oc.Expr)
		case oc.Column != "" && !outputNames[oc.Column]:
			h = addCol(oc.Column)
		}
		wp.orderHidden = append(wp.orderHidden, h)
	}
	inner.Exprs = append(inner.Exprs, hidden...)
	wp.inner, wp.hidden = &inner, len(hidden)
	return wp, nil
}

// replaceWindowCalls returns e with every window call replaced by
// fn's expression (walking arithmetic, function arguments and CASE
// arms; a call inside a predicate or subquery stays).
func replaceWindowCalls(e parser.Expr, fn func(*parser.SelectExpr) parser.Expr) parser.Expr {
	if e.Window != nil {
		out := fn(e.Window)
		out.Cast = e.Cast
		return out
	}
	out := e
	if e.Left != nil {
		l := replaceWindowCalls(*e.Left, fn)
		out.Left = &l
	}
	if e.Right != nil {
		r := replaceWindowCalls(*e.Right, fn)
		out.Right = &r
	}
	if len(e.Args) > 0 {
		out.Args = make([]parser.Expr, len(e.Args))
		for i, a := range e.Args {
			out.Args[i] = replaceWindowCalls(a, fn)
		}
	}
	if e.Case != nil {
		ce := *e.Case
		if ce.Operand != nil {
			op := replaceWindowCalls(*ce.Operand, fn)
			ce.Operand = &op
		}
		ce.Whens = make([]parser.CaseWhen, len(e.Case.Whens))
		for i, w := range e.Case.Whens {
			nw := w
			if w.Value != nil {
				v := replaceWindowCalls(*w.Value, fn)
				nw.Value = &v
			}
			if len(w.Cond) > 0 {
				nw.Cond = replaceCondWindowCalls(w.Cond, fn)
			}
			nw.Result = replaceWindowCalls(w.Result, fn)
			ce.Whens[i] = nw
		}
		if ce.Else != nil {
			el := replaceWindowCalls(*ce.Else, fn)
			ce.Else = &el
		}
		out.Case = &ce
	}
	if e.Cmp != nil {
		// A predicate used as a value.
		c := replaceCondWindowCalls([]parser.Comparison{*e.Cmp}, fn)[0]
		out.Cmp = &c
	}
	return out
}

// replaceCondWindowCalls is replaceWindowCalls over a conjunction: each
// conjunct's left expression, right value, IN list and OR groups (a
// subquery stays).
func replaceCondWindowCalls(conds []parser.Comparison, fn func(*parser.SelectExpr) parser.Expr) []parser.Comparison {
	out := make([]parser.Comparison, len(conds))
	for i, c := range conds {
		nc := c
		if c.Expr != nil {
			l := replaceWindowCalls(*c.Expr, fn)
			nc.Expr = &l
		}
		nc.Value = replaceWindowCalls(c.Value, fn)
		if len(c.Values) > 0 {
			nc.Values = make([]parser.Expr, len(c.Values))
			for j, v := range c.Values {
				nc.Values[j] = replaceWindowCalls(v, fn)
			}
		}
		if len(c.Or) > 0 {
			nc.Or = make([][]parser.Comparison, len(c.Or))
			for j, d := range c.Or {
				nc.Or[j] = replaceCondWindowCalls(d, fn)
			}
		}
		out[i] = nc
	}
	return out
}

// evalDesc is the descriptor a computed output evaluates against: the
// inner query's hidden columns (__wN) followed by the window values
// (__wvK).
func (wp *windowPlan) evalDesc(cols []ResultColumn) *catalog.TableDescriptor {
	vis := len(cols) - wp.hidden
	var ecols []ResultColumn
	ecols = append(ecols, cols[vis:]...)
	for i, it := range wp.items {
		ecols = append(ecols, ResultColumn{Name: fmt.Sprintf("__wv%d", i), Type: it.typ})
	}
	return derivedDesc("", ecols)
}

// windowFuncNames are the window-only functions.
var windowFuncNames = map[string]bool{
	"ROW_NUMBER": true, "RANK": true, "DENSE_RANK": true, "PERCENT_RANK": true, "CUME_DIST": true, "NTILE": true,
	"LAG": true, "LEAD": true, "FIRST_VALUE": true, "LAST_VALUE": true, "NTH_VALUE": true,
}

// resolveWindowSpec expands a named window (OVER w, or OVER (w ...)).
func resolveWindowSpec(t *parser.Select, spec parser.WindowSpec) (parser.WindowSpec, error) {
	if spec.Name == "" {
		return spec, nil
	}
	for _, nw := range t.Windows {
		if strings.EqualFold(nw.Name, spec.Name) {
			base, err := resolveWindowSpec(t, nw.Spec)
			if err != nil {
				return spec, err
			}
			out := base
			out.Name = ""
			if len(spec.PartitionBy) > 0 {
				return spec, newErrf(CodeSyntaxError, "cannot override PARTITION BY of window %q", spec.Name)
			}
			if len(spec.OrderBy) > 0 {
				if len(base.OrderBy) > 0 {
					return spec, newErrf(CodeSyntaxError, "cannot override ORDER BY of window %q", spec.Name)
				}
				out.OrderBy = spec.OrderBy
			}
			if spec.Frame != nil {
				out.Frame = spec.Frame
			}
			return out, nil
		}
	}
	return spec, newErrf(CodeUndefinedObject, "window %q does not exist", spec.Name)
}

// windowTypes settles each item's output type (and an aggregate's spec)
// from the inner query's columns.
func (wp *windowPlan) windowTypes(cols []ResultColumn) error {
	vis := len(cols) - wp.hidden
	if vis < 0 {
		return newErrf(CodeInternal, "window plan lost its hidden columns")
	}
	desc := derivedDesc("", cols)
	hiddenName := func(h int) string { return fmt.Sprintf("__w%d", h) }
	for i := range wp.items {
		it := &wp.items[i]
		argType := types.Unknown
		if len(it.args) > 0 {
			argType = cols[vis+it.args[0]].Type
		}
		switch it.se.Agg {
		case "ROW_NUMBER", "RANK", "DENSE_RANK", "NTILE":
			it.typ = types.Int
		case "PERCENT_RANK", "CUME_DIST":
			it.typ = types.Float
		case "LAG", "LEAD", "FIRST_VALUE", "LAST_VALUE", "NTH_VALUE":
			it.typ = argType
			if it.typ == types.Unknown {
				it.typ = types.String
			}
		default:
			se := parser.SelectExpr{Agg: it.se.Agg, AggStar: it.se.AggStar}
			if len(it.args) > 0 {
				se.AggCol = hiddenName(it.args[0])
			}
			for _, x := range it.extra {
				if x.hidden >= 0 {
					se.AggArgs = append(se.AggArgs, parser.Expr{Column: hiddenName(x.hidden)})
				} else {
					se.AggArgs = append(se.AggArgs, *x.expr)
				}
			}
			sp, err := resolveAggSpec(desc, se)
			if err != nil {
				return err
			}
			it.agg = &sp
			it.typ = sp.resultType()
		}
	}
	return nil
}

// perStar is how many inner columns each * expanded to.
func (wp *windowPlan) perStar(cols []ResultColumn) int {
	vis := len(cols) - wp.hidden
	stars, plain := 0, 0
	for _, o := range wp.outs {
		switch {
		case o.star:
			stars++
		case o.plain:
			plain++
		}
	}
	if stars == 0 {
		return 0
	}
	return (vis - plain) / stars
}

// outputColumns lays the window and computed items into the inner
// columns' visible prefix, at their select-list positions, typing the
// computed ones over the evaluation descriptor.
func (wp *windowPlan) outputColumns(cols []ResultColumn) []ResultColumn {
	per := wp.perStar(cols)
	edesc := wp.evalDesc(cols)
	var out []ResultColumn
	next := 0
	for i := range wp.outs {
		o := &wp.outs[i]
		switch {
		case o.star:
			out = append(out, cols[next:next+per]...)
			next += per
		case o.plain:
			out = append(out, cols[next])
			next++
		case o.expr != nil:
			o.typ = exprFamily(*o.expr, func(n string) (types.Family, bool) {
				c, ok := edesc.Col(n)
				if !ok {
					return types.Unknown, false
				}
				return c.Type, true
			})
			if o.typ == types.Unknown {
				o.typ = types.String
			}
			out = append(out, ResultColumn{Name: o.name, Type: o.typ})
		default:
			it := wp.items[o.item]
			out = append(out, ResultColumn{Name: it.name, Type: it.typ})
		}
	}
	return out
}

// execWindowed runs a select with window functions.
func (s *Session) execWindowed(ctx context.Context, txn *kvclient.Txn, t *parser.Select, params []types.Datum) (*Result, error) {
	wp, err := windowPlanFor(t)
	if err != nil {
		return nil, err
	}
	inner, err := s.execSelect(ctx, txn, wp.inner, params)
	if err != nil {
		return nil, err
	}
	if err := wp.windowTypes(inner.Columns); err != nil {
		return nil, err
	}
	vis := len(inner.Columns) - wp.hidden
	s.note("window: %d function(s) over %d rows", len(wp.items), len(inner.Rows))
	values := make([][]types.Datum, len(wp.items))
	for i := range wp.items {
		vals, err := wp.items[i].compute(inner, vis, params)
		if err != nil {
			return nil, err
		}
		values[i] = vals
	}
	// Assemble: the visible columns in select-list order with the window
	// values spliced in and the computed items evaluated over the hidden
	// columns and the window values.
	cols := wp.outputColumns(inner.Columns)
	per := wp.perStar(inner.Columns)
	edesc := wp.evalDesc(inner.Columns)
	res := &Result{Columns: cols}
	for r, row := range inner.Rows {
		out := make([]types.Datum, 0, len(cols))
		var env map[catalog.ColumnID]types.Datum
		next := 0
		for _, o := range wp.outs {
			switch {
			case o.star:
				out = append(out, row[next:next+per]...)
				next += per
			case o.plain:
				out = append(out, row[next])
				next++
			case o.expr != nil:
				if env == nil {
					env = make(map[catalog.ColumnID]types.Datum, wp.hidden+len(wp.items))
					for j, d := range row[vis:] {
						env[catalog.ColumnID(j+1)] = d
					}
					for j := range wp.items {
						env[catalog.ColumnID(wp.hidden+j+1)] = values[j][r]
					}
				}
				d, err := evalExpr(*o.expr, edesc, env, params)
				if err != nil {
					return nil, err
				}
				out = append(out, conformTo(d, o.typ))
			default:
				out = append(out, conformTo(values[o.item][r], wp.items[o.item].typ))
			}
		}
		if err := s.chargeDatums(out); err != nil {
			return nil, err
		}
		res.Rows = append(res.Rows, out)
	}
	if t.Distinct {
		res.Rows = dedupeRows(res.Rows)
	}
	if len(t.OrderBy) > 0 {
		// A term naming an output column (bare, or qualified where the
		// projection dropped the qualifier) sorts by it; any other term
		// sorts by the hidden column that carries it, appended for the
		// sort and dropped after.
		order := append([]parser.OrderCol(nil), t.OrderBy...)
		visibleCols := len(res.Columns)
		var extra []int
		for i, oc := range order {
			h := -1
			if i < len(wp.orderHidden) {
				h = wp.orderHidden[i]
			}
			if oc.Column != "" {
				if columnIndex(res.Columns, oc.Column) >= 0 {
					continue
				}
				if _, bare := splitQualified(oc.Column); columnIndex(res.Columns[:visibleCols], bare) >= 0 {
					order[i].Column = bare
					continue
				}
			}
			if h < 0 {
				continue // a position, or a name the sorter will report
			}
			name := fmt.Sprintf("__s%d", i)
			res.Columns = append(res.Columns, ResultColumn{Name: name, Type: inner.Columns[vis+h].Type})
			extra = append(extra, h)
			order[i] = parser.OrderCol{Column: name, Desc: oc.Desc, Nulls: oc.Nulls}
		}
		if len(extra) > 0 {
			for r := range res.Rows {
				for _, h := range extra {
					res.Rows[r] = append(res.Rows[r], inner.Rows[r][vis+h])
				}
			}
		}
		if err := sortResultRows(res.Columns, res.Rows, order, params); err != nil {
			return nil, err
		}
		if len(extra) > 0 {
			res.Columns = res.Columns[:visibleCols]
			for r := range res.Rows {
				res.Rows[r] = res.Rows[r][:visibleCols]
			}
		}
	}
	res.Rows = trimRows(res.Rows, t)
	res.Tag = fmt.Sprintf("SELECT %d", len(res.Rows))
	return res, nil
}

// compute evaluates the item for every row of the inner result, in
// row order.
func (it *windowItem) compute(inner *Result, vis int, params []types.Datum) ([]types.Datum, error) {
	rows := inner.Rows
	out := make([]types.Datum, len(rows))
	for i := range it.extra {
		if it.extra[i].hidden < 0 {
			d, err := evalExpr(*it.extra[i].expr, nil, nil, params)
			if err != nil {
				return nil, err
			}
			it.extra[i].val = d
		}
	}
	// Partition on the key values, keeping first-seen order.
	partitions := map[string][]int{}
	var order []string
	for r, row := range rows {
		key := make([]types.Datum, len(it.parts))
		for i, h := range it.parts {
			key[i] = row[vis+h]
		}
		k := encodeGroupKey(key)
		if _, ok := partitions[k]; !ok {
			order = append(order, k)
		}
		partitions[k] = append(partitions[k], r)
	}
	cmpKeys := func(a, b int) int {
		for i, h := range it.orders {
			oc := it.orderTerms[i]
			da, db := rows[a][vis+h], rows[b][vis+h]
			if da.Null || db.Null {
				if da.Null == db.Null {
					continue
				}
				if da.Null == nullsFirst(oc) {
					return -1
				}
				return 1
			}
			c, err := da.Compare(db)
			if err != nil || c == 0 {
				continue
			}
			if oc.Desc {
				c = -c
			}
			return c
		}
		return 0
	}
	var desc *catalog.TableDescriptor
	var rowMaps []map[catalog.ColumnID]types.Datum
	if it.agg != nil {
		desc = derivedDesc("", inner.Columns)
		rowMaps = make([]map[catalog.ColumnID]types.Datum, len(rows))
		for r, row := range rows {
			m := make(map[catalog.ColumnID]types.Datum, len(row))
			for j, d := range row {
				m[catalog.ColumnID(j+1)] = d
			}
			rowMaps[r] = m
		}
	}
	for _, k := range order {
		idx := partitions[k]
		if len(it.orders) > 0 {
			sort.SliceStable(idx, func(a, b int) bool { return cmpKeys(idx[a], idx[b]) < 0 })
		}
		n := len(idx)
		// Peer groups: peerStart[i] / peerEnd[i] are the first and last
		// positions with the same ordering keys as position i (the whole
		// partition when there is no ORDER BY).
		peerStart := make([]int, n)
		peerEnd := make([]int, n)
		for i := 0; i < n; {
			j := i
			for j+1 < n && (len(it.orders) == 0 || cmpKeys(idx[i], idx[j+1]) == 0) {
				j++
			}
			for p := i; p <= j; p++ {
				peerStart[p], peerEnd[p] = i, j
			}
			i = j + 1
		}
		distinctBefore := make([]int, n) // dense rank - 1
		for i := 1; i < n; i++ {
			distinctBefore[i] = distinctBefore[i-1]
			if peerStart[i] == i {
				distinctBefore[i]++
			}
		}
		// arg(pos, 0) is the first argument at position pos; arg(pos,
		// k) for k > 0 the k-th further argument (a constant, or a
		// hidden column's value at pos).
		arg := func(pos, a int) types.Datum {
			if a == 0 {
				if len(it.args) == 0 {
					return types.DNull
				}
				return rows[idx[pos]][vis+it.args[0]]
			}
			if a-1 >= len(it.extra) {
				return types.DNull
			}
			x := it.extra[a-1]
			if x.hidden >= 0 {
				return rows[idx[pos]][vis+x.hidden]
			}
			return x.val
		}
		intArg := func(pos, a int, def int64) (int64, error) {
			if a-1 >= len(it.extra) {
				return def, nil
			}
			d := arg(pos, a)
			if d.Null {
				return 0, nil
			}
			v, err := d.Coerce(types.Int)
			if err != nil {
				return 0, newErrf(CodeInvalidTextRepresentation, "%s: argument %d must be an integer", it.se.Agg, a+1)
			}
			return v.I, nil
		}
		for i := 0; i < n; i++ {
			r := idx[i]
			var v types.Datum
			switch it.se.Agg {
			case "ROW_NUMBER":
				v = types.NewInt(int64(i + 1))
			case "RANK":
				v = types.NewInt(int64(peerStart[i] + 1))
			case "DENSE_RANK":
				v = types.NewInt(int64(distinctBefore[i] + 1))
			case "PERCENT_RANK":
				if n == 1 {
					v = types.NewFloat(0)
				} else {
					v = types.NewFloat(float64(peerStart[i]) / float64(n-1))
				}
			case "CUME_DIST":
				v = types.NewFloat(float64(peerEnd[i]+1) / float64(n))
			case "NTILE":
				k, err := intArg(i, 0, 1)
				if err != nil {
					return nil, err
				}
				if k <= 0 {
					return nil, newErrf(CodeInvalidParameterValue, "ntile: argument must be greater than zero")
				}
				v = types.NewInt(ntile(int64(i), int64(n), k))
			case "LAG", "LEAD":
				off, err := intArg(i, 1, 1)
				if err != nil {
					return nil, err
				}
				target := i - int(off)
				if it.se.Agg == "LEAD" {
					target = i + int(off)
				}
				if target >= 0 && target < n {
					v = arg(target, 0)
				} else {
					v = arg(i, 2)
				}
			case "FIRST_VALUE", "LAST_VALUE", "NTH_VALUE":
				lo, hi, err := it.frame(i, n, peerStart, peerEnd)
				if err != nil {
					return nil, err
				}
				switch {
				case lo > hi:
					v = types.DNull
				case it.se.Agg == "FIRST_VALUE":
					v = arg(lo, 0)
				case it.se.Agg == "LAST_VALUE":
					v = arg(hi, 0)
				default:
					nth, err := intArg(i, 1, 1)
					if err != nil {
						return nil, err
					}
					if nth <= 0 {
						return nil, newErrf(CodeInvalidParameterValue, "nth_value: argument must be greater than zero")
					}
					if p := lo + int(nth) - 1; p <= hi {
						v = arg(p, 0)
					} else {
						v = types.DNull
					}
				}
			default:
				lo, hi, err := it.frame(i, n, peerStart, peerEnd)
				if err != nil {
					return nil, err
				}
				st := newAggState(1)
				specs := []aggSpec{*it.agg}
				for p := lo; p <= hi; p++ {
					if err := st.accumulate(specs, desc, rowMaps[idx[p]], params); err != nil {
						return nil, err
					}
				}
				vals, err := st.finish(specs, params)
				if err != nil {
					return nil, err
				}
				v = vals[0]
			}
			out[r] = v
		}
	}
	return out, nil
}

// frame is the item's frame around position i of a partition of n rows:
// the written ROWS / RANGE frame, else the default — the partition up to
// the current row's last peer when ordered, the whole partition when
// not.
func (it *windowItem) frame(i, n int, peerStart, peerEnd []int) (lo, hi int, err error) {
	f := it.spec.Frame
	if f == nil {
		if len(it.orders) == 0 {
			return 0, n - 1, nil
		}
		return 0, peerEnd[i], nil
	}
	bound := func(b parser.FrameBound, start bool) (int, error) {
		switch b.Kind {
		case "unbounded preceding":
			return 0, nil
		case "unbounded following":
			return n - 1, nil
		case "current row":
			if f.Mode == "RANGE" {
				if start {
					return peerStart[i], nil
				}
				return peerEnd[i], nil
			}
			return i, nil
		case "preceding", "following":
			if f.Mode == "RANGE" {
				return 0, newErrf(CodeFeatureNotSupported, "RANGE frames with an offset are not supported; use ROWS")
			}
			p := i - int(b.Offset)
			if b.Kind == "following" {
				p = i + int(b.Offset)
			}
			if p < 0 {
				p = 0
			}
			if p > n-1 {
				p = n - 1
			}
			return p, nil
		}
		return 0, newErrf(CodeSyntaxError, "unknown frame bound %q", b.Kind)
	}
	if lo, err = bound(f.Start, true); err != nil {
		return 0, 0, err
	}
	if hi, err = bound(f.End, false); err != nil {
		return 0, 0, err
	}
	return lo, hi, nil
}

// ntile is PostgreSQL's bucket for the 0-based position i of n rows
// split into k buckets: the first n mod k buckets take one row more.
func ntile(i, n, k int64) int64 {
	if k > n {
		k = n
	}
	base, extra := n/k, n%k
	if i < extra*(base+1) {
		return i/(base+1) + 1
	}
	return extra + (i-extra*(base+1))/base + 1
}
