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
		op, all = setOpName(m.SetOp), m.UnionAll
	}

	// Unify the column types across members and conform every value.
	cols := make([]ResultColumn, len(head.Columns))
	copy(cols, head.Columns)
	for _, res := range members[1:] {
		for i := range cols {
			cols[i].Type = unifyFamily(cols[i].Type, res.Columns[i].Type)
		}
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
