package sql

import (
	"context"
	"sort"
	"strings"

	"github.com/sthorne/datax/pkg/kvclient"
	"github.com/sthorne/datax/pkg/sql/catalog"
	"github.com/sthorne/datax/pkg/sql/parser"
	"github.com/sthorne/datax/pkg/sql/types"
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

// resolveAggregates validates and resolves an all-aggregate SELECT list
// (mixing aggregates with plain columns is not supported without GROUP BY).
func resolveAggregates(desc *catalog.TableDescriptor, exprs []parser.SelectExpr) ([]aggSpec, error) {
	var specs []aggSpec
	for _, se := range exprs {
		if se.Agg == "" {
			return nil, newErrf(CodeFeatureNotSupported, "cannot mix aggregates with plain columns (no GROUP BY)")
		}
		sp := aggSpec{fn: se.Agg, star: se.AggStar, name: se.Alias}
		if sp.name == "" {
			sp.name = strings.ToLower(se.Agg)
		}
		if !se.AggStar {
			col, ok := desc.Col(se.AggCol)
			if !ok {
				return nil, newErrf(CodeUndefinedColumn, "column %q does not exist", se.AggCol)
			}
			if (se.Agg == "SUM" || se.Agg == "AVG") && col.Type != types.Int && col.Type != types.Float {
				return nil, newErrf(CodeFeatureNotSupported, "%s over %s is not supported", se.Agg, col.Type)
			}
			sp.col = col
		}
		specs = append(specs, sp)
	}
	return specs, nil
}

func (sp aggSpec) resultType() types.Family {
	switch sp.fn {
	case "COUNT":
		return types.Int
	case "AVG":
		return types.Float
	}
	return sp.col.Type
}

func (s *Session) execAggSelect(ctx context.Context, txn *kvclient.Txn, desc *catalog.TableDescriptor, t *parser.Select, params []types.Datum) (*Result, error) {
	if len(t.OrderBy) > 0 {
		return nil, newErrf(CodeFeatureNotSupported, "ORDER BY with aggregates is not supported")
	}
	specs, err := resolveAggregates(desc, t.Exprs)
	if err != nil {
		return nil, err
	}
	rows, _, err := s.fetchRows(ctx, txn, desc, t.Where, params, 0)
	if err != nil {
		return nil, err
	}

	// Streaming state per aggregate.
	counts := make([]int64, len(specs))
	sumI := make([]int64, len(specs))
	sumF := make([]float64, len(specs))
	best := make([]types.Datum, len(specs)) // MIN/MAX candidate; zero = none yet
	seen := make([]bool, len(specs))
	for _, fr := range rows {
		for i, sp := range specs {
			if sp.star {
				counts[i]++
				continue
			}
			d, ok := fr.row[sp.col.ID]
			if !ok || d.Null {
				continue
			}
			d, cerr := d.Coerce(sp.col.Type)
			if cerr != nil {
				return nil, newErrf(CodeInternal, "column %q: %v", sp.col.Name, cerr)
			}
			counts[i]++
			switch sp.fn {
			case "SUM", "AVG":
				if sp.col.Type == types.Int {
					sumI[i] += d.I
				} else {
					sumF[i] += d.F
				}
			case "MIN", "MAX":
				if !seen[i] {
					best[i], seen[i] = d, true
					continue
				}
				c, err := d.Compare(best[i])
				if err != nil {
					return nil, newErrf(CodeInternal, "%v", err)
				}
				if (sp.fn == "MIN" && c < 0) || (sp.fn == "MAX" && c > 0) {
					best[i] = d
				}
			}
		}
	}

	res := &Result{Tag: "SELECT 1"}
	out := make([]types.Datum, len(specs))
	for i, sp := range specs {
		res.Columns = append(res.Columns, ResultColumn{Name: sp.name, Type: sp.resultType()})
		switch sp.fn {
		case "COUNT":
			out[i] = types.NewInt(counts[i])
		case "SUM":
			if counts[i] == 0 {
				out[i] = types.DNull
			} else if sp.col.Type == types.Int {
				out[i] = types.NewInt(sumI[i])
			} else {
				out[i] = types.NewFloat(sumF[i])
			}
		case "AVG":
			if counts[i] == 0 {
				out[i] = types.DNull
			} else {
				total := sumF[i]
				if sp.col.Type == types.Int {
					total = float64(sumI[i])
				}
				out[i] = types.NewFloat(total / float64(counts[i]))
			}
		case "MIN", "MAX":
			if !seen[i] {
				out[i] = types.DNull
			} else {
				out[i] = best[i]
			}
		}
	}
	res.Rows = [][]types.Datum{out}
	return res, nil
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
	case planFullScan, planPKPoint:
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
