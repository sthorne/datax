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
// when there is nothing to skip or it cannot be told simply: a
// projected expression, or a conjunct other than a column compared with
// constants.
func scanNeededCols(desc *catalog.TableDescriptor, proj []projCol, where []parser.Comparison) func(catalog.ColumnID) bool {
	need := make(map[catalog.ColumnID]bool, len(proj)+len(where))
	for _, p := range proj {
		if p.expr != nil {
			return nil
		}
		need[p.col.ID] = true
	}
	if !whereColumnIDs(desc, where, need) {
		return nil
	}
	for i := range desc.Columns {
		if !need[desc.Columns[i].ID] {
			return func(id catalog.ColumnID) bool { return need[id] }
		}
	}
	return nil
}

// whereColumnIDs adds the columns of a conjunction of column-versus-
// constant conjuncts to need; false when some conjunct is not one.
func whereColumnIDs(desc *catalog.TableDescriptor, where []parser.Comparison, need map[catalog.ColumnID]bool) bool {
	for _, c := range where {
		switch {
		case c.Op == "TRUE" || c.Op == "FALSE":
			continue
		case c.Op == "OR":
			for _, d := range c.Or {
				if !whereColumnIDs(desc, d, need) {
					return false
				}
			}
			continue
		case c.Expr != nil || c.Sub != nil || c.Column == "":
			return false
		}
		col, ok := desc.Col(c.Column)
		if !ok || !constExpr(c.Value) {
			return false
		}
		for _, v := range c.Values {
			if !constExpr(v) {
				return false
			}
		}
		need[col.ID] = true
	}
	return true
}
