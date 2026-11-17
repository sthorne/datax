package sql

import (
	"reflect"

	"github.com/sthorne/datax/pkg/sql/parser"
	"github.com/sthorne/datax/pkg/sql/types"
)

// SELECT DISTINCT ON (exprs): of each group of rows equal on the
// expressions (NULLs equal), the first in ORDER BY order is kept. Each
// path applies it where its rows are sorted but not yet cut by LIMIT:
// the single-table, join and derived-table paths over the input rows,
// before projection; the grouped path over the groups, through hidden
// columns. The parser has resolved positions and output names to the
// select items' expressions.

// checkDistinctOn refuses a DISTINCT ON whose expressions do not lead
// the ORDER BY (42P10, as in PostgreSQL): the ORDER BY may begin with
// them in any order, and continue past them, but no other term may come
// before all of them have.
func checkDistinctOn(t *parser.Select) error {
	if len(t.DistinctOn) == 0 || len(t.OrderBy) == 0 {
		return nil
	}
	on := make([]parser.Expr, len(t.DistinctOn))
	for i, e := range t.DistinctOn {
		on[i] = distinctOnCanon(e)
	}
	matched := make([]bool, len(on))
	left := len(on)
	for _, oc := range t.OrderBy {
		if left == 0 {
			break
		}
		var e parser.Expr
		switch {
		case oc.Agg != nil:
			e = parser.Expr{Agg: oc.Agg}
		case oc.Expr != nil:
			e = *oc.Expr
		case oc.Position > 0:
			return nil // a position into a list not known here
		default:
			e = parser.Expr{Column: oc.Column}
			for _, se := range t.Exprs {
				if !se.Star && se.Alias == oc.Column {
					e = se.Expr
					if se.Agg != "" {
						call := se
						call.Alias = ""
						e = parser.Expr{Agg: &call}
					}
					break
				}
			}
		}
		e = distinctOnCanon(e)
		found := false
		for i := range on {
			if reflect.DeepEqual(on[i], e) {
				found = true
				if !matched[i] {
					matched[i] = true
					left--
				}
			}
		}
		if !found {
			return newErrf(CodeInvalidColumnReference, "SELECT DISTINCT ON expressions must match initial ORDER BY expressions")
		}
	}
	return nil
}

// distinctOnCanon is the shape DISTINCT ON and ORDER BY terms compare
// in: normalized, with qualifiers dropped (s.player and player name the
// same column wherever only one table has it).
func distinctOnCanon(e parser.Expr) parser.Expr {
	return normalizeExpr(renameExprColumns(e, stripQualifier))
}

// keepFirstDistinct keeps, of rows in order, the first of each run of
// equal keys anywhere in the sequence (DISTINCT ON over sorted rows).
func keepFirstDistinct[T any](rows []T, key func(T) ([]types.Datum, error)) ([]T, error) {
	seen := map[string]bool{}
	out := rows[:0]
	for _, r := range rows {
		k, err := key(r)
		if err != nil {
			return nil, err
		}
		ek := encodeGroupKey(k)
		if seen[ek] {
			continue
		}
		seen[ek] = true
		out = append(out, r)
	}
	return out, nil
}

// evalExprList evaluates expressions against one row's environment.
func evalExprList(exprs []parser.Expr, env exprEnv, params []types.Datum) ([]types.Datum, error) {
	out := make([]types.Datum, len(exprs))
	for i, e := range exprs {
		d, err := evalExprEnv(e, env, params)
		if err != nil {
			return nil, err
		}
		out[i] = d
	}
	return out, nil
}
