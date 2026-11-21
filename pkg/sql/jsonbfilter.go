package sql

import (
	"github.com/sthorne/datax/pkg/sql/catalog"
	"github.com/sthorne/datax/pkg/sql/jsonbcontain"
	"github.com/sthorne/datax/pkg/sql/parser"
	"github.com/sthorne/datax/pkg/sql/types"
)

// whereMemo holds what a statement's filter computes once rather than
// per row: the compiled right operands of its jsonb @> conjuncts. A scan
// owns one for the life of the statement (it is not safe for concurrent
// use).
type whereMemo struct {
	jsonb []jsonbMemoEntry
	// env is the environment a computed conjunct is evaluated in, held
	// here so that passing it boxes nothing per row.
	env tableEnv
}

type jsonbMemoEntry struct {
	cmp  *parser.Comparison
	pat  *jsonbcontain.Pattern
	null bool
}

// jsonbPattern returns the compiled right operand of the @> conjunct cmp
// (whose value is row-independent), evaluating, coercing and compiling
// it on first use. null reports a NULL operand.
func (m *whereMemo) jsonbPattern(cmp *parser.Comparison, desc *catalog.TableDescriptor, row map[catalog.ColumnID]types.Datum, params []types.Datum) (*jsonbcontain.Pattern, bool, error) {
	for i := range m.jsonb {
		if e := &m.jsonb[i]; e.cmp == cmp {
			return e.pat, e.null, nil
		}
	}
	rhs, err := evalExpr(cmp.Value, desc, row, params)
	if err != nil {
		return nil, false, err
	}
	e := jsonbMemoEntry{cmp: cmp, null: rhs.Null}
	if !rhs.Null {
		rhs, err = rhs.Coerce(types.Jsonb)
		if err != nil {
			return nil, false, newErrf(CodeInvalidTextRepresentation, "WHERE %s @>: %v", cmp.Column, err)
		}
		if e.pat, err = jsonbcontain.Compile(rhs.S); err != nil {
			return nil, false, newErrf(CodeInternal, "malformed stored jsonb: %v", err)
		}
	}
	m.jsonb = append(m.jsonb, e)
	return e.pat, e.null, nil
}

// constExpr reports whether a value expression reads no column: a
// literal, a parameter, a cast of one, or nothing (the absent operand of
// IS NULL).
func constExpr(e parser.Expr) bool {
	return e.Column == "" && len(e.Path) == 0 && e.Sub == nil &&
		e.Func == "" && len(e.Args) == 0 && e.BinOp == "" && e.Left == nil && e.Right == nil &&
		e.Case == nil && e.Cmp == nil && !e.IsDefault && e.Window == nil && e.Agg == nil
}

// rowIndependent reports whether a value expression is a literal or a
// parameter (possibly cast): one whose value is the same for every row
// of the statement.
func rowIndependent(e parser.Expr) bool {
	return (e.Lit != nil || e.Param > 0) && constExpr(e)
}

// scanNeededCols returns the columns a streaming scan's filter and
// projection read, as a membership test, or nil (decode every column)
// when there is nothing to skip or it cannot be told: an expression
// with a subquery, a window or aggregate call, DEFAULT, or a name that
// is no column of the table (evaluating it fails as it always has).
func scanNeededCols(desc *catalog.TableDescriptor, proj []projCol, where []parser.Comparison) func(catalog.ColumnID) bool {
	need := make(map[catalog.ColumnID]bool, len(proj)+len(where))
	if !projReadCols(desc, proj, need) || !whereReadCols(desc, where, need) {
		return nil
	}
	for i := range desc.Columns {
		if !need[desc.Columns[i].ID] {
			return func(id catalog.ColumnID) bool { return need[id] }
		}
	}
	return nil
}

// projReadCols adds the columns a select list reads to need; false when
// that cannot be told.
func projReadCols(desc *catalog.TableDescriptor, proj []projCol, need map[catalog.ColumnID]bool) bool {
	for _, p := range proj {
		if p.expr == nil {
			need[p.col.ID] = true
		} else if !exprReadCols(desc, p.expr, need) {
			return false
		}
	}
	return true
}

// whereReadCols adds the columns a conjunction reads to need: every
// column a conjunct names, on either side, in its value list, inside
// an OR, a CASE or a function's arguments. False when that cannot be
// told (see exprReadCols), or for a subquery not yet spliced.
func whereReadCols(desc *catalog.TableDescriptor, where []parser.Comparison, need map[catalog.ColumnID]bool) bool {
	for i := range where {
		c := &where[i]
		if c.Sub != nil {
			return false
		}
		for _, d := range c.Or {
			if !whereReadCols(desc, d, need) {
				return false
			}
		}
		if c.Column != "" {
			col, ok := desc.Col(c.Column)
			if !ok {
				return false
			}
			need[col.ID] = true
		}
		if !exprReadCols(desc, c.Expr, need) || !exprReadCols(desc, &c.Value, need) {
			return false
		}
		for j := range c.Values {
			if !exprReadCols(desc, &c.Values[j], need) {
				return false
			}
		}
	}
	return true
}

// exprReadCols adds the columns an expression reads to need (evaluation
// reads a column only where an Expr or a Comparison names it). False
// when that cannot be told: a subquery, a window or aggregate call,
// DEFAULT, or a name that is no column of desc.
func exprReadCols(desc *catalog.TableDescriptor, e *parser.Expr, need map[catalog.ColumnID]bool) bool {
	if e == nil {
		return true
	}
	if e.Sub != nil || e.Window != nil || e.Agg != nil || e.IsDefault {
		return false
	}
	if e.Column != "" {
		col, ok := desc.Col(e.Column)
		if !ok {
			return false
		}
		need[col.ID] = true
	}
	for i := range e.Args {
		if !exprReadCols(desc, &e.Args[i], need) {
			return false
		}
	}
	if !exprReadCols(desc, e.Left, need) || !exprReadCols(desc, e.Right, need) {
		return false
	}
	if e.Cmp != nil && !whereReadCols(desc, []parser.Comparison{*e.Cmp}, need) {
		return false
	}
	if ce := e.Case; ce != nil {
		if !exprReadCols(desc, ce.Operand, need) || !exprReadCols(desc, ce.Else, need) {
			return false
		}
		for i := range ce.Whens {
			w := &ce.Whens[i]
			if !exprReadCols(desc, w.Value, need) || !whereReadCols(desc, w.Cond, need) || !exprReadCols(desc, &w.Result, need) {
				return false
			}
		}
	}
	return true
}
