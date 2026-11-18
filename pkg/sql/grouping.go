package sql

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/sthorne/datax/pkg/sql/catalog"
	"github.com/sthorne/datax/pkg/sql/parser"
	"github.com/sthorne/datax/pkg/sql/types"
)

// Grouped selects: GROUP BY and aggregates.
//
// A grouped select is planned as two stages. The first reads the input
// rows and, per group, computes the grouping keys and the aggregate
// calls. The second evaluates everything the statement shows or tests
// — select items, HAVING, ORDER BY — over each group's "environment": a
// synthetic row whose columns are the group's keys (__gN) and aggregate
// values (__aN). Every such expression is rewritten onto that row first
// (groupRewriter): a subexpression equal to a grouping key becomes that
// key's column, an aggregate call becomes its value's column, and what
// remains may only be constants and functions of those. A column
// reference left over is neither grouped nor aggregated, which is
// 42803, as in PostgreSQL. So SUM(a) / COUNT(*), ROUND(AVG(x), 2),
// CASE WHEN COUNT(*) > 1 ..., upper(region) || ':' || COUNT(*) and
// HAVING SUM(a) * 100 / MAX(b) > 50 are all the same mechanism.

// groupedOut is one output column of a grouped SELECT: a group key, an
// aggregate value, or an expression over the group's environment.
type groupedOut struct {
	name     string
	typ      types.Family
	noTZ     bool         // a TIMESTAMP without time zone, as the ungrouped paths mark it
	groupPos int          // ≥0 → group key position; else -1
	aggPos   int          // ≥0 → aggregate spec position; else -1
	expr     *parser.Expr // otherwise: evaluated over the environment
}

// groupedQuery is the resolved shape of a grouped/aggregate SELECT.
type groupedQuery struct {
	// keys are the grouping expressions over the input rows; keyCols
	// holds a plain column key's column (ID 0 for a computed key).
	keys     []parser.Expr
	keyCols  []catalog.Column
	keyTypes []types.Family
	// keyNoTZ marks a timestamp key that is without time zone, and
	// colNoTZ tells that of an input column (result descriptions).
	keyNoTZ []bool
	colNoTZ func(name string) bool
	// sets are the grouping sets, as key positions (GROUP BY without
	// ROLLUP, CUBE or GROUPING SETS is the one set of every key).
	sets  [][]int
	specs []aggSpec // aggregates, in first-reference order
	// groupings are the GROUPING() calls: the key positions of their
	// arguments.
	groupings [][]int
	outs      []groupedOut
	having    []parser.Comparison // over the environment
	// hidden are ORDER BY keys the output does not carry (an aggregate
	// call, an unprojected grouping column, an expression), computed
	// after outs and dropped after the sort; order is the ORDER BY
	// rewritten onto them.
	hidden []groupedOut
	order  []parser.OrderCol
	// distinctOn are the positions (among outs, then hidden) of the
	// DISTINCT ON expressions, which are hidden columns too.
	distinctOn []int
	// env describes the per-group environment the rewritten
	// expressions read: the keys, then the aggregate values.
	env *catalog.TableDescriptor
}

func groupKeyName(i int) string      { return fmt.Sprintf("__g%d", i) }
func groupAggName(i int) string      { return fmt.Sprintf("__a%d", i) }
func groupGroupingName(i int) string { return fmt.Sprintf("__x%d", i) }

// resolveGrouped resolves the select list, GROUP BY, HAVING and ORDER BY
// of a grouped/aggregate SELECT against the input descriptor.
func resolveGrouped(desc *catalog.TableDescriptor, t *parser.Select) (*groupedQuery, error) {
	gq := &groupedQuery{}
	colType := func(n string) (types.Family, bool) {
		c, ok := desc.Col(n)
		return c.Type, ok
	}
	gq.colNoTZ = func(n string) bool {
		c, ok := desc.Col(stripQualifier(n))
		return ok && c.NoTZ
	}
	for _, k := range t.GroupBy {
		if isPlainRef(k) {
			if _, ok := desc.Col(k.Column); !ok {
				// A name that is no input column may be an output name
				// (GROUP BY parity): the item's expression. An input
				// column of the name wins, as in PostgreSQL.
				for _, se := range t.Exprs {
					if se.Alias == k.Column && !se.Star {
						k = se.Expr
						if se.Agg != "" || se.Window != nil {
							call := se
							call.Alias = ""
							k = parser.Expr{Agg: &call}
						}
						break
					}
				}
			}
		}
		if exprHas(k, func(x parser.Expr) bool { return x.Agg != nil || x.Window != nil }) {
			return nil, newErrf(CodeGrouping, "aggregate functions are not allowed in GROUP BY")
		}
		var bad error
		exprColumns(k, func(n string) {
			if _, ok := desc.Col(n); !ok && bad == nil {
				bad = newErrf(CodeUndefinedColumn, "column %q does not exist", n)
			}
		})
		if bad != nil {
			return nil, bad
		}
		var col catalog.Column
		if isPlainRef(k) {
			col, _ = desc.Col(k.Column)
		}
		typ := exprFamily(k, colType)
		if typ == types.Unknown {
			typ = types.String
		}
		gq.keys = append(gq.keys, k)
		gq.keyCols = append(gq.keyCols, col)
		gq.keyTypes = append(gq.keyTypes, typ)
		gq.keyNoTZ = append(gq.keyNoTZ, typ == types.Timestamp && exprNoTZ(k, gq.colNoTZ))
	}
	for _, set := range t.GroupingSets {
		for _, k := range set {
			if k < 0 || k >= len(gq.keys) {
				return nil, newErrf(CodeInternal, "grouping set names key %d of %d", k, len(gq.keys))
			}
		}
	}
	gq.sets = t.GroupingSets
	if gq.sets == nil {
		all := make([]int, len(gq.keys))
		for i := range all {
			all[i] = i
		}
		gq.sets = [][]int{all}
	}
	rw := newGroupRewriter(desc, gq)

	for _, se := range t.Exprs {
		switch {
		case se.Star:
			return nil, newErrf(CodeGrouping, "SELECT * is not allowed with GROUP BY or aggregates")
		case se.Agg != "" && se.Window == nil:
			pos, err := rw.aggSlot(se)
			if err != nil {
				return nil, err
			}
			name := se.Alias
			if name == "" {
				name = strings.ToLower(se.Agg)
			}
			gq.outs = append(gq.outs, groupedOut{name: name, typ: gq.specs[pos].resultType(), noTZ: gq.specNoTZ(pos), groupPos: -1, aggPos: pos})
		case se.Window != nil:
			return nil, newErrf(CodeFeatureNotSupported, "window function %s() is not supported here", strings.ToLower(se.Agg))
		default:
			name := se.Alias
			if name == "" {
				name = parser.OutputName(se)
				if se.Expr.Column != "" && se.Expr.BinOp == "" && len(se.Expr.Path) == 0 {
					name = stripQualifier(se.Expr.Column) // col::type is still "col"
				}
			}
			e, err := rw.rewrite(se.Expr, false)
			if err != nil {
				return nil, err
			}
			gq.outs = append(gq.outs, rw.out(name, e))
		}
	}
	for _, o := range gq.outs {
		if _, dup := rw.aliases[o.name]; !dup {
			rw.aliases[o.name] = o
		}
	}

	var err error
	if gq.having, err = rw.rewriteConds(t.Having); err != nil {
		return nil, err
	}

	// ORDER BY: a bare name of an output column (or a position) sorts by
	// that output; any other term — an aggregate call, a grouping column
	// the output does not show, an expression — becomes a hidden column,
	// computed with the group, sorted by, then dropped.
	gq.order = append([]parser.OrderCol(nil), t.OrderBy...)
	for i, oc := range t.OrderBy {
		var e parser.Expr
		switch {
		case oc.Agg != nil:
			e = parser.Expr{Agg: oc.Agg}
		case oc.Expr != nil:
			e = *oc.Expr
		case oc.Position > 0:
			continue
		case oc.Column != "":
			if _, ok := rw.aliases[oc.Column]; ok {
				continue
			}
			e = parser.Expr{Column: oc.Column}
		default:
			continue
		}
		r, err := rw.rewrite(e, true)
		if err != nil {
			return nil, err
		}
		h := rw.out(fmt.Sprintf("__order%d", i), r)
		gq.hidden = append(gq.hidden, h)
		gq.order[i] = parser.OrderCol{Column: h.name, Desc: oc.Desc, Nulls: oc.Nulls}
	}

	for i, e := range t.DistinctOn {
		r, err := rw.rewrite(e, true)
		if err != nil {
			return nil, err
		}
		gq.distinctOn = append(gq.distinctOn, len(gq.outs)+len(gq.hidden))
		gq.hidden = append(gq.hidden, rw.out(fmt.Sprintf("__distinct%d", i), r))
	}

	gq.env = rw.envDesc()
	return gq, nil
}

// groupRewriter rewrites expressions over the input rows of a grouped
// select onto its per-group environment.
type groupRewriter struct {
	desc     *catalog.TableDescriptor
	gq       *groupedQuery
	normKeys []parser.Expr
	// aliases are the outputs by name: HAVING and ORDER BY may name one
	// (HAVING n = 2) where no grouping key or input column is meant.
	aliases map[string]groupedOut
}

func newGroupRewriter(desc *catalog.TableDescriptor, gq *groupedQuery) *groupRewriter {
	rw := &groupRewriter{desc: desc, gq: gq, aliases: map[string]groupedOut{}}
	for _, k := range gq.keys {
		rw.normKeys = append(rw.normKeys, normalizeExpr(k))
	}
	return rw
}

// envColType types the environment's columns, for exprFamily.
func (rw *groupRewriter) envColType(name string) (types.Family, bool) {
	var i int
	switch {
	case strings.HasPrefix(name, "__g"):
		if _, err := fmt.Sscanf(name, "__g%d", &i); err == nil && i < len(rw.gq.keyTypes) {
			return rw.gq.keyTypes[i], true
		}
	case strings.HasPrefix(name, "__a"):
		if _, err := fmt.Sscanf(name, "__a%d", &i); err == nil && i < len(rw.gq.specs) {
			return rw.gq.specs[i].resultType(), true
		}
	case strings.HasPrefix(name, "__x"):
		return types.Int, true
	}
	return types.Unknown, false
}

// envDesc builds the environment's descriptor: keys, then aggregates.
func (rw *groupRewriter) envDesc() *catalog.TableDescriptor {
	d := &catalog.TableDescriptor{}
	for i, f := range rw.gq.keyTypes {
		d.Columns = append(d.Columns, catalog.Column{ID: catalog.ColumnID(i + 1), Name: groupKeyName(i), Type: f})
	}
	for i, sp := range rw.gq.specs {
		d.Columns = append(d.Columns, catalog.Column{ID: catalog.ColumnID(len(rw.gq.keyTypes) + i + 1), Name: groupAggName(i), Type: sp.resultType()})
	}
	for i := range rw.gq.groupings {
		d.Columns = append(d.Columns, catalog.Column{ID: catalog.ColumnID(len(rw.gq.keyTypes) + len(rw.gq.specs) + i + 1), Name: groupGroupingName(i), Type: types.Int})
	}
	return d
}

// out makes an output column of a rewritten expression: a bare key or
// aggregate reads its value directly; anything else evaluates.
func (rw *groupRewriter) out(name string, e parser.Expr) groupedOut {
	o := groupedOut{name: name, groupPos: -1, aggPos: -1}
	if isPlainRef(e) {
		var i int
		if _, err := fmt.Sscanf(e.Column, "__g%d", &i); err == nil && strings.HasPrefix(e.Column, "__g") {
			o.groupPos, o.typ, o.noTZ = i, rw.gq.keyTypes[i], rw.gq.keyNoTZ[i]
			return o
		}
		if _, err := fmt.Sscanf(e.Column, "__a%d", &i); err == nil && strings.HasPrefix(e.Column, "__a") {
			o.aggPos, o.typ, o.noTZ = i, rw.gq.specs[i].resultType(), rw.gq.specNoTZ(i)
			return o
		}
	}
	o.expr = &e
	o.typ = exprFamily(e, rw.envColType)
	if o.typ == types.Unknown {
		o.typ = types.String
	}
	o.noTZ = o.typ == types.Timestamp && exprNoTZ(e, rw.envNoTZ)
	return o
}

// envNoTZ tells whether an environment column is a timestamp without
// time zone: a key that is one, or a MIN / MAX over one.
func (rw *groupRewriter) envNoTZ(name string) bool {
	var i int
	switch {
	case strings.HasPrefix(name, "__g"):
		if _, err := fmt.Sscanf(name, "__g%d", &i); err == nil && i < len(rw.gq.keyNoTZ) {
			return rw.gq.keyNoTZ[i]
		}
	case strings.HasPrefix(name, "__a"):
		if _, err := fmt.Sscanf(name, "__a%d", &i); err == nil && i < len(rw.gq.specs) {
			return rw.gq.specNoTZ(i)
		}
	}
	return false
}

// specNoTZ tells whether aggregate i yields a timestamp without time
// zone: MIN or MAX over one.
func (gq *groupedQuery) specNoTZ(i int) bool {
	sp := gq.specs[i]
	if sp.resultType() != types.Timestamp || (sp.fn != "MIN" && sp.fn != "MAX") {
		return false
	}
	if sp.arg != nil {
		return exprNoTZ(*sp.arg, gq.colNoTZ)
	}
	return sp.col.NoTZ
}

// isPlainRef reports whether e is a bare column reference.
func isPlainRef(e parser.Expr) bool {
	return e.Column != "" && e.BinOp == "" && e.Cast == "" && len(e.Path) == 0 && e.Left == nil
}

// aggSlot resolves an aggregate call and returns its position among the
// specs, reusing an identical earlier call's.
func (rw *groupRewriter) aggSlot(se parser.SelectExpr) (int, error) {
	nested := func(x parser.Expr) bool { return x.Agg != nil || x.Window != nil }
	if se.AggArg != nil && exprHas(*se.AggArg, nested) {
		return 0, newErrf(CodeGrouping, "aggregate function calls cannot contain aggregate or window function calls")
	}
	for _, a := range se.AggArgs {
		if exprHas(a, nested) {
			return 0, newErrf(CodeGrouping, "aggregate function calls cannot contain aggregate or window function calls")
		}
	}
	sp, err := resolveAggSpec(rw.desc, se)
	if err != nil {
		return 0, err
	}
	for i := range rw.gq.specs {
		if sameSpec(rw.gq.specs[i], sp) {
			return i, nil
		}
	}
	rw.gq.specs = append(rw.gq.specs, sp)
	return len(rw.gq.specs) - 1, nil
}

// rewrite rewrites one expression onto the environment. aliases lets a
// column reference that is no grouping key name an output (HAVING and
// ORDER BY).
func (rw *groupRewriter) rewrite(e parser.Expr, aliases bool) (parser.Expr, error) {
	return rw.rw(normalizeExpr(e), aliases)
}

func (rw *groupRewriter) rw(e parser.Expr, aliases bool) (parser.Expr, error) {
	for i, k := range rw.normKeys {
		if reflect.DeepEqual(k, e) {
			return parser.Expr{Column: groupKeyName(i)}, nil
		}
	}
	switch {
	case e.Agg != nil:
		pos, err := rw.aggSlot(*e.Agg)
		if err != nil {
			return e, err
		}
		return parser.Expr{Column: groupAggName(pos)}, nil
	case e.Window != nil:
		return e, newErrf(CodeFeatureNotSupported, "window function %s() is not supported here", strings.ToLower(e.Window.Agg))
	case e.Sub != nil:
		return e, nil
	case e.Func == "grouping":
		// GROUPING(k, ...): a bit per argument, set where the group's
		// grouping set leaves that key out (the first argument the most
		// significant bit). Each argument must be a grouping key.
		if len(e.Args) == 0 {
			return e, newErrf(CodeSyntaxError, "GROUPING() requires an argument")
		}
		if len(e.Args) > 31 {
			return e, newErrf(CodeProgramLimitExceeded, "GROUPING must have fewer than 32 arguments")
		}
		var keys []int
		for _, a := range e.Args {
			k := -1
			for i, nk := range rw.normKeys {
				if reflect.DeepEqual(nk, a) {
					k = i
					break
				}
			}
			if k < 0 {
				return e, newErrf(CodeGrouping, "arguments to GROUPING must be grouping expressions of the associated query level")
			}
			keys = append(keys, k)
		}
		for i, g := range rw.gq.groupings {
			if reflect.DeepEqual(g, keys) {
				return parser.Expr{Column: groupGroupingName(i)}, nil
			}
		}
		rw.gq.groupings = append(rw.gq.groupings, keys)
		return parser.Expr{Column: groupGroupingName(len(rw.gq.groupings) - 1)}, nil
	case e.Column != "":
		if aliases {
			if o, ok := rw.aliases[e.Column]; ok {
				switch {
				case o.groupPos >= 0:
					return parser.Expr{Column: groupKeyName(o.groupPos)}, nil
				case o.aggPos >= 0:
					return parser.Expr{Column: groupAggName(o.aggPos)}, nil
				}
				return *o.expr, nil
			}
		}
		if _, ok := rw.desc.Col(e.Column); ok {
			return e, newErrf(CodeGrouping, "column %q must appear in the GROUP BY clause or be used in an aggregate function", e.Column)
		}
		return e, newErrf(CodeUndefinedColumn, "column %q does not exist", e.Column)
	}
	out := e
	var err error
	if e.Left != nil {
		l, lerr := rw.rw(*e.Left, aliases)
		if lerr != nil {
			return e, lerr
		}
		out.Left = &l
	}
	if e.Right != nil {
		r, rerr := rw.rw(*e.Right, aliases)
		if rerr != nil {
			return e, rerr
		}
		out.Right = &r
	}
	if len(e.Args) > 0 {
		out.Args = make([]parser.Expr, len(e.Args))
		for i, a := range e.Args {
			if out.Args[i], err = rw.rw(a, aliases); err != nil {
				return e, err
			}
		}
	}
	if e.Case != nil {
		ce := *e.Case
		if ce.Operand != nil {
			op, err := rw.rw(*ce.Operand, aliases)
			if err != nil {
				return e, err
			}
			ce.Operand = &op
		}
		ce.Whens = make([]parser.CaseWhen, len(e.Case.Whens))
		for i, w := range e.Case.Whens {
			nw := w
			if w.Value != nil {
				v, err := rw.rw(*w.Value, aliases)
				if err != nil {
					return e, err
				}
				nw.Value = &v
			}
			if len(w.Cond) > 0 {
				if nw.Cond, err = rw.conds(w.Cond, aliases); err != nil {
					return e, err
				}
			}
			if nw.Result, err = rw.rw(w.Result, aliases); err != nil {
				return e, err
			}
			ce.Whens[i] = nw
		}
		if ce.Else != nil {
			el, err := rw.rw(*ce.Else, aliases)
			if err != nil {
				return e, err
			}
			ce.Else = &el
		}
		out.Case = &ce
	}
	if e.Cmp != nil {
		cs, err := rw.conds([]parser.Comparison{*e.Cmp}, aliases)
		if err != nil {
			return e, err
		}
		out.Cmp = &cs[0]
	}
	return out, nil
}

// rewriteConds rewrites a HAVING conjunction onto the environment.
func (rw *groupRewriter) rewriteConds(conds []parser.Comparison) ([]parser.Comparison, error) {
	return rw.conds(normalizeConds(conds), true)
}

func (rw *groupRewriter) conds(conds []parser.Comparison, aliases bool) ([]parser.Comparison, error) {
	if len(conds) == 0 {
		return nil, nil
	}
	out := make([]parser.Comparison, len(conds))
	for i, c := range conds {
		nc := c
		var err error
		if len(c.Or) > 0 {
			nc.Or = make([][]parser.Comparison, len(c.Or))
			for j, d := range c.Or {
				if nc.Or[j], err = rw.conds(d, aliases); err != nil {
					return nil, err
				}
			}
		}
		if c.Column != "" {
			lhs, err := rw.rw(normalizeExpr(parser.Expr{Column: c.Column, Path: c.Path}), aliases)
			if err != nil {
				return nil, err
			}
			nc.Column, nc.Path = "", nil
			if isPlainRef(lhs) {
				// A key or an aggregate on its own keeps the column form,
				// whose typed comparison coerces the other side.
				nc.Column = lhs.Column
			} else {
				nc.Expr = &lhs
			}
		}
		if c.Expr != nil {
			lhs, err := rw.rw(*c.Expr, aliases)
			if err != nil {
				return nil, err
			}
			if isPlainRef(lhs) {
				nc.Expr, nc.Column = nil, lhs.Column
			} else {
				nc.Expr = &lhs
			}
		}
		if nc.Value, err = rw.rw(c.Value, aliases); err != nil {
			return nil, err
		}
		if len(c.Values) > 0 {
			nc.Values = make([]parser.Expr, len(c.Values))
			for j, v := range c.Values {
				if nc.Values[j], err = rw.rw(v, aliases); err != nil {
					return nil, err
				}
			}
		}
		out[i] = nc
	}
	return out, nil
}

// normalizeExpr returns e in a canonical shape, so that equal
// expressions compare equal however they were written: a node applying
// an operator, a ->/->> path or a cast to a value of its own (the
// parser's flat shape, {Column: a, BinOp: +, Right: 1}) becomes a chain
// of nodes, each applying one of them to the node below
// ({Left: {Column: a}, BinOp: +, Right: 1}), and a grouping node
// ({Left: x} alone) is x. Evaluation and typing are unchanged.
func normalizeExpr(e parser.Expr) parser.Expr {
	out := e
	// The session's TimeZone marking is not part of an expression's
	// identity: a grouped expression and its select item match whether
	// or not one of them was marked.
	out.Zone = nil
	if out.Lit != nil && out.Lit.Loc != nil {
		d := *out.Lit
		d.Loc = nil
		out.Lit = &d
	}
	if e.Left != nil {
		l := normalizeExpr(*e.Left)
		out.Left = &l
	}
	if e.Right != nil {
		r := normalizeExpr(*e.Right)
		out.Right = &r
	}
	if len(e.Args) > 0 {
		out.Args = make([]parser.Expr, len(e.Args))
		for i, a := range e.Args {
			out.Args[i] = normalizeExpr(a)
		}
	}
	if e.Case != nil {
		ce := *e.Case
		if ce.Operand != nil {
			op := normalizeExpr(*ce.Operand)
			ce.Operand = &op
		}
		ce.Whens = make([]parser.CaseWhen, len(e.Case.Whens))
		for i, w := range e.Case.Whens {
			nw := w
			if w.Value != nil {
				v := normalizeExpr(*w.Value)
				nw.Value = &v
			}
			nw.Cond = normalizeConds(w.Cond)
			nw.Result = normalizeExpr(w.Result)
			ce.Whens[i] = nw
		}
		if ce.Else != nil {
			el := normalizeExpr(*ce.Else)
			ce.Else = &el
		}
		out.Case = &ce
	}
	if e.Cmp != nil {
		c := normalizeConds([]parser.Comparison{*e.Cmp})[0]
		out.Cmp = &c
	}
	if out.BinOp == "" && out.Cast == "" && len(out.Path) == 0 {
		if out.Left != nil {
			return *out.Left // a bare grouping
		}
		return out
	}
	// Split the node's own value from the operations applied to it, in
	// evaluation order: path, operator, cast.
	cur := out
	cur.BinOp, cur.Right, cur.Cast, cur.Path = "", nil, "", nil
	if cur.Left != nil {
		cur = *cur.Left
	}
	if len(out.Path) > 0 {
		inner := cur
		cur = parser.Expr{Left: &inner, Path: out.Path}
	}
	if out.BinOp != "" {
		inner := cur
		cur = parser.Expr{Left: &inner, BinOp: out.BinOp, Right: out.Right}
	}
	if out.Cast != "" {
		inner := cur
		cur = parser.Expr{Left: &inner, Cast: out.Cast}
	}
	return cur
}

// normalizeConds is normalizeExpr over a conjunction's expressions.
func normalizeConds(conds []parser.Comparison) []parser.Comparison {
	if len(conds) == 0 {
		return conds
	}
	out := make([]parser.Comparison, len(conds))
	for i, c := range conds {
		nc := c
		if c.Expr != nil {
			e := normalizeExpr(*c.Expr)
			nc.Expr = &e
		}
		nc.Value = normalizeExpr(c.Value)
		if len(c.Values) > 0 {
			nc.Values = make([]parser.Expr, len(c.Values))
			for j, v := range c.Values {
				nc.Values[j] = normalizeExpr(v)
			}
		}
		if len(c.Or) > 0 {
			nc.Or = make([][]parser.Comparison, len(c.Or))
			for j, d := range c.Or {
				nc.Or[j] = normalizeConds(d)
			}
		}
		out[i] = nc
	}
	return out
}

// execGroupedOver runs the grouping/aggregation pipeline over already
// fetched (or materialized) rows: hash-group on the keys, streaming
// aggregate state per group; then, per group, HAVING, the outputs and
// the hidden sort keys over the group's environment; then DISTINCT,
// ORDER BY and LIMIT over the output rows.
//
// With gc, the select has correlated subqueries in its per-group
// positions: t is its shape (fixing keys, aggregates and outputs), and
// each group's HAVING, outputs and hidden keys come from the select gc
// substitutes for the group's keys.
func (s *Session) execGroupedOver(desc *catalog.TableDescriptor, rows []fetchedRow, t *parser.Select, params []types.Datum, gc *groupedCorr) (*Result, error) {
	gq, err := resolveGrouped(desc, t)
	if err != nil {
		return nil, err
	}

	type aggGroup struct {
		key []types.Datum // the keys outside the group's set are NULL
		set int
		st  *aggState
	}
	groups := map[string]*aggGroup{}
	var order []string // first-seen group order
	keys := make([]types.Datum, len(gq.keys))
	multi := len(gq.sets) > 1
	for _, fr := range rows {
		for i, col := range gq.keyCols {
			if col.ID != 0 {
				d, ok := fr.row[col.ID]
				if !ok {
					d = types.DNull
				}
				keys[i] = d
				continue
			}
			d, err := evalExpr(gq.keys[i], desc, fr.row, params)
			if err != nil {
				return nil, err
			}
			keys[i] = d
		}
		// A row counts once in each grouping set, in the group of its
		// keys that the set keeps. The set's position is part of the
		// group's identity: a rolled-up NULL is not a NULL key.
		for si, set := range gq.sets {
			key := make([]types.Datum, len(keys))
			for i := range key {
				key[i] = types.DNull
			}
			for _, k := range set {
				key[k] = keys[k]
			}
			ident := key
			if multi {
				ident = append([]types.Datum{types.NewInt(int64(si))}, key...)
			}
			k := encodeGroupKey(ident)
			g, ok := groups[k]
			if !ok {
				g = &aggGroup{key: key, set: si, st: newAggState(len(gq.specs))}
				groups[k] = g
				order = append(order, k)
			}
			if err := g.st.accumulate(gq.specs, desc, fr.row, params); err != nil {
				return nil, err
			}
		}
	}
	// An empty grouping set — no GROUP BY at all, or () in ROLLUP, CUBE
	// or GROUPING SETS — has its row over zero input rows too (the
	// aggregates of nothing: COUNT 0, SUM NULL).
	if len(rows) == 0 {
		for si, set := range gq.sets {
			if len(set) > 0 {
				continue
			}
			key := make([]types.Datum, len(gq.keys))
			for i := range key {
				key[i] = types.DNull
			}
			k := fmt.Sprintf("empty%d", si)
			groups[k] = &aggGroup{key: key, set: si, st: newAggState(len(gq.specs))}
			order = append(order, k)
		}
	}
	inSet := make([][]bool, len(gq.sets))
	for si, set := range gq.sets {
		inSet[si] = make([]bool, len(gq.keys))
		for _, k := range set {
			inSet[si][k] = true
		}
	}

	res := &Result{}
	for _, oc := range gq.outs {
		res.Columns = append(res.Columns, ResultColumn{Name: oc.name, Type: oc.typ, NoTZ: oc.noTZ})
	}
	for _, oc := range gq.hidden {
		res.Columns = append(res.Columns, ResultColumn{Name: oc.name, Type: oc.typ, NoTZ: oc.noTZ})
	}
	visible := len(gq.outs)
	all := append(append([]groupedOut(nil), gq.outs...), gq.hidden...)
	nk := len(gq.keys)
	for _, k := range order {
		g := groups[k]
		aggVals, err := g.st.finish(gq.specs, params)
		if err != nil {
			return nil, err
		}
		// The group's environment: keys, then aggregate values.
		env := make(map[catalog.ColumnID]types.Datum, nk+len(aggVals))
		for i, d := range g.key {
			env[catalog.ColumnID(i+1)] = d
		}
		for i, d := range aggVals {
			env[catalog.ColumnID(nk+i+1)] = d
		}
		for i, args := range gq.groupings {
			var bits int64
			for _, k := range args {
				bits <<= 1
				if !inSet[g.set][k] {
					bits |= 1
				}
			}
			env[catalog.ColumnID(nk+len(aggVals)+i+1)] = types.NewInt(bits)
		}
		cur, curAll := gq, all
		if gc != nil {
			if cur, err = groupCorrQuery(desc, gq, gc, g.key); err != nil {
				return nil, err
			}
			curAll = append(append([]groupedOut(nil), cur.outs...), cur.hidden...)
		}
		if len(cur.having) > 0 {
			keep, err := matchesWhere(cur.having, cur.env, env, params)
			if err != nil {
				return nil, err
			}
			if !keep {
				continue
			}
		}
		out := make([]types.Datum, 0, len(curAll))
		for i, oc := range curAll {
			switch {
			case oc.groupPos >= 0:
				out = append(out, g.key[oc.groupPos])
			case oc.aggPos >= 0:
				out = append(out, aggVals[oc.aggPos])
			default:
				d, err := evalExpr(*oc.expr, cur.env, env, params)
				if err != nil {
					return nil, err
				}
				d = conformTo(d, oc.typ)
				if gc != nil && !d.Null && oc.typ != all[i].typ {
					// A correlated subquery's value: the shape could
					// not type it (typed after evaluation, as an
					// ungrouped select's are).
					res.Columns[i].Type = d.Fam
				}
				out = append(out, d)
			}
		}
		if err := s.chargeDatums(out); err != nil {
			return nil, err
		}
		res.Rows = append(res.Rows, out)
	}

	s.note("group/aggregate: %d groups from %d rows", len(res.Rows), len(rows))
	if t.Distinct && len(gq.distinctOn) == 0 {
		res.Rows = dedupeRowsPrefix(res.Rows, visible)
	}
	if len(gq.order) > 0 {
		if err := sortResultRows(res.Columns, res.Rows, gq.order, params); err != nil {
			return nil, err
		}
	}
	if len(gq.distinctOn) > 0 {
		if res.Rows, err = keepFirstDistinct(res.Rows, func(r []types.Datum) ([]types.Datum, error) {
			k := make([]types.Datum, len(gq.distinctOn))
			for i, c := range gq.distinctOn {
				k[i] = r[c]
			}
			return k, nil
		}); err != nil {
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

// groupCorrQuery resolves the select gc substitutes for one group's
// keys, checking that it groups and aggregates exactly as the shape gq
// the input rows were accumulated by does.
func groupCorrQuery(desc *catalog.TableDescriptor, gq *groupedQuery, gc *groupedCorr, keys []types.Datum) (*groupedQuery, error) {
	t, err := gc.selectFor(keys)
	if err != nil {
		return nil, err
	}
	cur, err := resolveGrouped(desc, t)
	if err != nil {
		return nil, err
	}
	same := reflect.DeepEqual(cur.keys, gq.keys) && reflect.DeepEqual(cur.groupings, gq.groupings) &&
		len(cur.specs) == len(gq.specs) && len(cur.outs) == len(gq.outs) && len(cur.hidden) == len(gq.hidden)
	for i := 0; same && i < len(cur.specs); i++ {
		same = cur.specs[i].fn == gq.specs[i].fn && cur.specs[i].key == gq.specs[i].key
	}
	if !same {
		return nil, newErrf(CodeInternal, "a correlated subquery changed the grouping of its select")
	}
	return cur, nil
}
