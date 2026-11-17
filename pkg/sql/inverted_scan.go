package sql

import (
	"context"
	"strings"

	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/kvclient"
	"github.com/sthorne/datax/pkg/metrics"
	"github.com/sthorne/datax/pkg/sql/catalog"
	"github.com/sthorne/datax/pkg/sql/inverted"
	"github.com/sthorne/datax/pkg/sql/parser"
	"github.com/sthorne/datax/pkg/sql/rowenc"
	"github.com/sthorne/datax/pkg/sql/tsearch"
	"github.com/sthorne/datax/pkg/sql/types"
)

// Reading through an inverted index (issue #300).
//
// The planner asks each public inverted index of the table what the
// WHERE clause says about its column, as an inverted.Expr: a tree of
// AND/OR over element lookups. The operators that can say anything are
// the index kind's business (for JSONB: @>, ?, ?| and ?&; for text
// search: @@), with the column alone on the left — or, for a kind that
// indexes a function of its column, that function of it (invertedLHS) —
// and a value known before any row is read on the right; the shape of
// the clause is handled here, the same for every kind:
//
//   - the top-level conjuncts are ANDed, and a conjunct the index cannot
//     use is simply left out (fewer terms admit more rows, never fewer);
//   - an OR conjunct is the union of its branches, each branch the AND
//     of what its own conjuncts say — and is usable only when EVERY
//     branch says something, because a branch the index knows nothing
//     about could match any row.
//
// The executor evaluates the expression to a set of primary keys (see
// inverted.Expr.Eval for how a conjunction chooses which elements to
// read), fetches those rows, and filters each with the complete WHERE
// clause. That recheck is not an optimization to be skipped when the
// expression looks exact: the encoding is lossy by design (long
// elements are digests, array positions are dropped, an empty container
// is not an element), and the recheck is what turns "may match" into
// "matches".

// invertedMode is the inverted_index_scan session setting.
type invertedMode int

const (
	// invertedAuto lets the planner choose (the default).
	invertedAuto invertedMode = iota
	// invertedOn takes an inverted index whenever one can serve the
	// WHERE clause, whatever else could.
	invertedOn
	// invertedOff never reads an inverted index.
	invertedOff
)

func (m invertedMode) String() string {
	switch m {
	case invertedOn:
		return "on"
	case invertedOff:
		return "off"
	}
	return "auto"
}

// invertedSelectivity is the fraction of a table an inverted lookup is
// assumed to select when statistics exist: there are none per element,
// and a containment or key test is usually asked because it is
// selective.
const invertedSelectivity = 1.0 / 100

// invertedExprFor builds what the conjunction says about the index's
// column; nil when it says nothing.
func invertedExprFor(desc *catalog.TableDescriptor, idx *catalog.IndexDescriptor, kind inverted.Kind, where []parser.Comparison, params []types.Datum) (*inverted.Expr, error) {
	var terms []*inverted.Expr
	for _, cmp := range where {
		if cmp.Op == "OR" {
			branches := make([]*inverted.Expr, 0, len(cmp.Or))
			for _, branch := range cmp.Or {
				be, err := invertedExprFor(desc, idx, kind, branch, params)
				if err != nil {
					return nil, err
				}
				branches = append(branches, be)
			}
			terms = append(terms, inverted.Or(branches...))
			continue
		}
		if !invertedLHS(desc, idx, kind, cmp) {
			continue
		}
		// The value must be known without a row: a column reference on
		// the right-hand side fails to evaluate here and the conjunct is
		// left to the filter.
		rhs, err := evalExpr(cmp.Value, nil, nil, params)
		if err != nil {
			continue
		}
		e, err := kind.Query(cmp.Op, rhs)
		if err != nil {
			return nil, newErrf(CodeInternal, "index %q: %v", idx.Name, err)
		}
		terms = append(terms, e)
	}
	return inverted.And(terms...), nil
}

// invertedLHS reports whether the conjunct's left-hand side is what the
// index holds the elements of: the indexed column itself, or — for a
// kind that indexes a function of the column — exactly that function of
// it, with its constant argument (to_tsvector('english', body); a call
// that named no configuration has had the session's default written in
// by now).
func invertedLHS(desc *catalog.TableDescriptor, idx *catalog.IndexDescriptor, kind inverted.Kind, cmp parser.Comparison) bool {
	column := cmp.Column
	if c, ok := kind.(inverted.Computed); ok {
		if fn, arg := c.Computes(); fn != "" {
			e := cmp.Expr
			if e == nil || e.Func != fn || len(e.Args) != 2 || e.Left != nil || e.BinOp != "" || e.Cast != "" || len(e.Path) > 0 {
				return false
			}
			cfg, col := e.Args[0], e.Args[1]
			if cfg.Lit == nil || cfg.Lit.Null || cfg.BinOp != "" || !sameTextSearchConfig(cfg.Lit.Text(), arg) {
				return false
			}
			if col.Column == "" || col.BinOp != "" || col.Cast != "" || len(col.Path) > 0 || col.Left != nil {
				return false
			}
			column = col.Column
			if i := strings.LastIndexByte(column, '.'); i >= 0 {
				column = column[i+1:]
			}
			c, ok := desc.Col(column)
			return ok && c.ID == idx.ColumnIDs[0]
		}
	}
	if column == "" || cmp.Expr != nil || len(cmp.Path) > 0 {
		return false
	}
	col, ok := desc.Col(column)
	return ok && col.ID == idx.ColumnIDs[0]
}

// sameTextSearchConfig: the name a statement wrote resolves to the
// configuration the index was built with.
func sameTextSearchConfig(written, indexed string) bool {
	cfg, err := tsearch.LookupConfig(written)
	return err == nil && cfg.Name == indexed
}

// pickInverted finds the inverted index that serves the WHERE clause:
// the first public one whose column the clause constrains.
func pickInverted(desc *catalog.TableDescriptor, where []parser.Comparison, params []types.Datum) (*catalog.IndexDescriptor, *inverted.Expr, error) {
	for i := range desc.Indexes {
		idx := &desc.Indexes[i]
		if !idx.IsInverted() || !idx.Public() || len(idx.ColumnIDs) != 1 {
			continue
		}
		if idx.IsPartial() && !predicateImplied(desc, idx, where) {
			continue // no entries for the rows outside its predicate
		}
		kind, ok := inverted.Lookup(idx.Inverted)
		if !ok {
			continue
		}
		e, err := invertedExprFor(desc, idx, kind, where, params)
		if err != nil {
			return nil, nil, err
		}
		if e != nil {
			return idx, e, nil
		}
	}
	return nil, nil, nil
}

// invertedPlanString renders the plan for EXPLAIN. The lookups are
// named but cut short: a containment over a large document has as many
// of them as the document has values.
func invertedPlanString(p accessPlan) string {
	const maxLen = 160
	text := "none"
	if kind, ok := inverted.Lookup(p.idx.Inverted); ok {
		text = p.inv.Format(kind.Describe)
	}
	if len(text) > maxLen {
		text = text[:maxLen] + "…"
	}
	return "inverted scan of index \"" + p.idx.Name + "\" (" + text + ") + primary key join, rows rechecked"
}

// invertedScanPage is how many entries one read of an element's span
// takes.
const invertedScanPage = 4096

// executeInvertedScan evaluates the plan's expression against the index
// and fetches, rechecks and returns the rows, in primary-key order.
func (s *Session) executeInvertedScan(ctx context.Context, txn *kvclient.Txn, desc *catalog.TableDescriptor, plan accessPlan, where []parser.Comparison, params []types.Datum, limit int64) ([]fetchedRow, error) {
	noteIndexScan(desc, plan.idx.ID)
	entries := 0
	scan := func(element []byte, prefix bool, max int) ([][]byte, bool, error) {
		start, end := rowenc.InvertedElementSpan(desc, plan.idx, element)
		if prefix {
			start, end = rowenc.InvertedPrefixSpan(desc, plan.idx, element)
		}
		// Every key in an element's span is start followed by the row's
		// encoded primary key. A prefix span holds many elements, so
		// each key is split where its own element ends.
		at := len(start)
		var refs [][]byte
		cursor := start
		for {
			page := int64(invertedScanPage)
			if max > 0 && int64(max+1-len(refs)) < page {
				// One past the limit tells "exactly max" from "more".
				page = int64(max + 1 - len(refs))
			}
			kvs, err := txn.Scan(ctx, cursor, end, page)
			if err != nil {
				return nil, false, err
			}
			for _, kv := range kvs {
				if !prefix {
					refs = append(refs, kv.Key[at:])
					continue
				}
				_, ref, err := rowenc.InvertedEntryElement(desc, plan.idx, kv.Key)
				if err != nil {
					return nil, false, err
				}
				refs = append(refs, ref)
			}
			entries += len(kvs)
			// The references are held until the rows are fetched: they
			// count against the statement's memory like rows do.
			if err := s.charge(int64(len(kvs)) * int64(24+len(desc.PrimaryKey)*9)); err != nil {
				return nil, false, err
			}
			more := max > 0 && len(refs) > max
			if int64(len(kvs)) < page || more {
				if more {
					refs = refs[:max]
				}
				if prefix {
					refs = inverted.SortDedup(refs)
				}
				return refs, more, nil
			}
			cursor = kvs[len(kvs)-1].Key.Next()
		}
	}
	refs, err := plan.inv.Eval(scan)
	if err != nil {
		return nil, err
	}
	metrics.SQLRowsScanned.Add(float64(entries))
	s.AddScanned(int64(entries))
	noteIndexRows(desc, plan.idx.ID, entries)

	prefix := rowenc.PrimaryKeyPrefixFor(desc)
	pks := make([]keys.Key, len(refs))
	for i, ref := range refs {
		pk := make(keys.Key, 0, len(prefix)+len(ref))
		pks[i] = append(append(pk, prefix...), ref...)
	}
	var out []fetchedRow
	batches := 0
	err = fetchPrimaryRows(ctx, txn, pks, func(first int, raws [][]byte) (bool, error) {
		batches++
		for i, raw := range raws {
			pk := pks[first+i]
			if raw == nil {
				if kvclient.LockWaitFrom(ctx) == kvclient.LockWaitSkip {
					continue // locked by another transaction: passed over
				}
				return false, newErrf(CodeInternal, "index %q entry points at a missing row", plan.idx.Name)
			}
			row, err := decodeFullRow(ctx, s.rowSealer(), desc, pk, raw)
			if err != nil {
				return false, err
			}
			match, err := matchesWhere(where, desc, row, params)
			if err != nil {
				return false, err
			}
			if !match {
				continue
			}
			if err := s.chargeRow(row); err != nil {
				return false, err
			}
			out = append(out, fetchedRow{key: pk, row: row})
			if limit > 0 && int64(len(out)) == limit {
				return false, nil
			}
		}
		return true, nil
	})
	if err != nil {
		return nil, err
	}
	if s.explain != nil {
		s.note("inverted scan: %d index entries read over %d element lookups, %d candidate rows fetched in %d batches, %d kept by the recheck", entries, plan.inv.Leaves(), len(pks), batches, len(out))
	}
	return out, nil
}
