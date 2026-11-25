package sql

import (
	"context"
	"fmt"

	"github.com/sthorne/datax/pkg/kvclient"
	"github.com/sthorne/datax/pkg/sql/parser"
	"github.com/sthorne/datax/pkg/sql/types"
)

// Set operations: UNION, INTERSECT and EXCEPT, each [ALL], over the flat
// member list the parser builds. Every member runs on its own and is
// materialized; INTERSECT binds tighter than the other two, which
// associate left to right (PostgreSQL's precedence), so the INTERSECTs
// reduce first and the rest fold in order. Distinct forms hash rows on
// their canonical group key; ALL forms keep multiplicities (UNION ALL
// concatenates, INTERSECT ALL keeps the smaller count, EXCEPT ALL
// subtracts the right count). The head names the output; each column's
// type unifies the members' (numeric families lift, anything else meets
// as text) and every value is conformed to it. The total materialized
// row count is capped (54000) so a runaway operand fails rather than
// exhausting the gateway.

// setOpRowCap bounds the rows a set operation may hold at once.
const setOpRowCap = 1_000_000

// setOperand is one member's result, with the operator joining it to
// the operand before it.
type setOperand struct {
	rows [][]types.Datum
	op   string // "UNION", "INTERSECT", "EXCEPT" ("" for the head)
	all  bool
}

func (s *Session) execSetOps(ctx context.Context, txn *kvclient.Txn, t *parser.Select, params []types.Datum) (*Result, error) {
	var (
		operands []setOperand
		head     *Result
		members  []*Result // in list order, for ORDER BY name lookup
		asts     []*parser.Select
		total    int
	)
	op, all := "", false
	for m := t; m != nil; m = m.Union {
		one := *m
		one.Union, one.SetOp, one.UnionAll = nil, "", false
		if m == t {
			one.OrderBy, one.Limit, one.Offset = nil, -1, 0
		}
		res, err := s.execSelect(ctx, txn, &one, params)
		if err != nil {
			return nil, err
		}
		if head == nil {
			head = res
		} else if len(res.Columns) != len(head.Columns) {
			return nil, newErrf(CodeSyntaxError, "each %s query must have the same number of columns", op)
		}
		total += len(res.Rows)
		if total > setOpRowCap {
			return nil, newErrf(CodeProgramLimitExceeded, "set operation materializes more than %d rows", setOpRowCap)
		}
		operands = append(operands, setOperand{rows: res.Rows, op: op, all: all})
		members = append(members, res)
		asts = append(asts, m)
		op, all = setOpName(m.SetOp), m.UnionAll
	}

	// Unify the column types across members and conform every value.
	cols := make([]ResultColumn, len(head.Columns))
	copy(cols, head.Columns)
	if valuesList(asts) {
		if err := resolveValuesTypes(asts, members, cols); err != nil {
			return nil, err
		}
	}
	for _, res := range members[1:] {
		for i := range cols {
			cols[i].Type = unifyFamily(cols[i].Type, res.Columns[i].Type)
		}
	}
	for i := range cols {
		ci := make([]ResultColumn, len(members))
		for mi, res := range members {
			ci[mi] = res.Columns[i]
		}
		cols[i].Width = unifyWidth(cols[i].Type, ci)
	}
	for mi, res := range members {
		for i := range cols {
			if res.Columns[i].Type == cols[i].Type {
				continue
			}
			for _, row := range operands[mi].rows {
				if !row[i].Null {
					row[i] = conformTo(row[i], cols[i].Type)
				}
			}
		}
	}

	// INTERSECT first, then the rest left to right.
	var reduced []setOperand
	for _, o := range operands {
		if o.op == "INTERSECT" && len(reduced) > 0 {
			last := &reduced[len(reduced)-1]
			last.rows = intersectRows(last.rows, o.rows, o.all)
			continue
		}
		reduced = append(reduced, o)
	}
	rows := reduced[0].rows
	for _, o := range reduced[1:] {
		switch o.op {
		case "EXCEPT":
			rows = exceptRows(rows, o.rows, o.all)
		default:
			rows = append(rows, o.rows...)
			if !o.all {
				rows = dedupeRows(rows)
			}
		}
	}

	s.note("set operation: %d member(s), %d rows", len(members), len(rows))
	if len(t.OrderBy) > 0 {
		// A name written after the last member was rewritten to that
		// member's output name; map it onto the head's column at the
		// same position.
		order := append([]parser.OrderCol(nil), t.OrderBy...)
		for i, oc := range order {
			if oc.Column == "" || columnIndex(cols, oc.Column) >= 0 {
				continue
			}
			for mi := len(members) - 1; mi > 0; mi-- {
				if j := columnIndex(members[mi].Columns, oc.Column); j >= 0 {
					order[i].Column = cols[j].Name
					break
				}
			}
		}
		if err := sortResultRows(cols, rows, order, params); err != nil {
			return nil, err
		}
	}
	rows = trimRows(rows, t)
	return &Result{Columns: cols, Rows: rows, Tag: fmt.Sprintf("SELECT %d", len(rows))}, nil
}

// valuesList reports set members that are the rows of one VALUES list.
func valuesList(asts []*parser.Select) bool {
	for _, m := range asts {
		if !m.Values {
			return false
		}
	}
	return true
}

// untypedLiteral reports a VALUES item whose type is PostgreSQL's
// "unknown": a bare string literal or NULL, which takes the type the
// list's other rows give the column.
func untypedLiteral(e parser.Expr) bool {
	return e.Lit != nil && (e.Lit.Null || e.Lit.Fam == types.String) && e.Cast == "" && e.BinOp == "" &&
		e.Left == nil && len(e.Path) == 0
}

// resolveValuesTypes gives each column of a VALUES list one type, as
// PostgreSQL's select_common_type does: the first typed row's, lifted
// within the numeric (or date and time) category by a later row's, the
// untyped literals ('...', NULL) converted to it (22P02 when one is not
// a valid input of it), and text when no row is typed; rows of types
// of different categories are refused (42804). Every value is
// converted to its column's type, and members and cols record it.
func resolveValuesTypes(asts []*parser.Select, members []*Result, cols []ResultColumn) error {
	for i := range cols {
		pick, err := valuesColumnType(asts, i, func(mi int) types.Family { return members[mi].Columns[i].Type })
		if err != nil {
			return err
		}
		for mi, res := range members {
			for _, row := range res.Rows {
				d := row[i]
				if d.Null || d.Fam == pick {
					continue
				}
				c, err := d.Coerce(pick)
				if err != nil {
					return newErrf(CodeInvalidTextRepresentation, "invalid input syntax for type %s: %q", pgTypeName(pick), d.Text())
				}
				row[i] = c
			}
			members[mi].Columns[i].Type = pick
		}
		cols[i].Type = pick
	}
	return nil
}

// valuesColumnType is the type of column i of a VALUES list whose rows
// are asts, row mi's value of type typeOf(mi) (see resolveValuesTypes).
func valuesColumnType(asts []*parser.Select, i int, typeOf func(mi int) types.Family) (types.Family, error) {
	pick := types.Unknown
	for mi, m := range asts {
		if i >= len(m.Exprs) || untypedLiteral(m.Exprs[i].Expr) {
			continue
		}
		f := typeOf(mi)
		switch {
		case f == types.Unknown || f == pick:
		case pick == types.Unknown:
			pick = f
		case numericFam(f) && numericFam(pick):
			pick = unifyFamily(pick, f)
		case (f == types.Date || f == types.Timestamp) && (pick == types.Date || pick == types.Timestamp):
			pick = types.Timestamp
		default:
			return types.Unknown, newErrf(CodeDatatypeMismatch, "VALUES types %s and %s cannot be matched", pgTypeName(pick), pgTypeName(f))
		}
	}
	if pick == types.Unknown {
		pick = types.String
	}
	return pick, nil
}

// setOpName normalizes a member's SetOp ("" is UNION).
func setOpName(op string) string {
	if op == "" {
		return "UNION"
	}
	return op
}

func columnIndex(cols []ResultColumn, name string) int {
	for i, c := range cols {
		if c.Name == name {
			return i
		}
	}
	return -1
}

// unifyWidth is the integer width (2, 4, 0 = 8) of a set operation's
// output column whose members' columns are cols, once their families
// have met at fam: the widest member's (int2 UNION int8 is int8, as in
// PostgreSQL), a member of unknown type (NULL) deferring; 0 when fam is
// not an integer type or a member's family differs from it.
func unifyWidth(fam types.Family, cols []ResultColumn) int32 {
	if fam != types.Int && fam != types.ArrayOf(types.Int) {
		return 0
	}
	w := int32(-1)
	for _, c := range cols {
		switch {
		case c.Type == types.Unknown:
			continue
		case c.Type != fam:
			return 0
		}
		cw := c.Width
		if cw != 2 && cw != 4 {
			cw = 8
		}
		w = max(w, cw)
	}
	if w == 8 || w < 0 {
		return 0
	}
	return w
}

// unifyFamily is the type two set-operation members' columns meet at:
// the same family stays, the numeric families lift (INT8 < DECIMAL <
// FLOAT8), an unknown side yields to the other, anything else is text.
func unifyFamily(a, b types.Family) types.Family {
	switch {
	case a == b:
		return a
	case a == types.Unknown:
		return b
	case b == types.Unknown:
		return a
	}
	rank := func(f types.Family) int {
		switch f {
		case types.Int:
			return 1
		case types.Decimal:
			return 2
		case types.Float:
			return 3
		}
		return 0
	}
	if ra, rb := rank(a), rank(b); ra > 0 && rb > 0 {
		if ra > rb {
			return a
		}
		return b
	}
	return types.String
}

// intersectRows keeps the left rows that also occur on the right: each
// distinct row once, or (all) as many times as both sides have it.
func intersectRows(left, right [][]types.Datum, all bool) [][]types.Datum {
	counts := map[string]int{}
	for _, r := range right {
		counts[encodeGroupKey(r)]++
	}
	out := left[:0]
	seen := map[string]bool{}
	for _, r := range left {
		k := encodeGroupKey(r)
		if counts[k] == 0 {
			continue
		}
		if all {
			counts[k]--
		} else {
			if seen[k] {
				continue
			}
			seen[k] = true
		}
		out = append(out, r)
	}
	return out
}

// exceptRows keeps the left rows that do not occur on the right: each
// distinct row once, or (all) the left count less the right count.
func exceptRows(left, right [][]types.Datum, all bool) [][]types.Datum {
	counts := map[string]int{}
	for _, r := range right {
		counts[encodeGroupKey(r)]++
	}
	out := left[:0]
	seen := map[string]bool{}
	for _, r := range left {
		k := encodeGroupKey(r)
		if all {
			if counts[k] > 0 {
				counts[k]--
				continue
			}
		} else {
			if counts[k] > 0 || seen[k] {
				continue
			}
			seen[k] = true
		}
		out = append(out, r)
	}
	return out
}
