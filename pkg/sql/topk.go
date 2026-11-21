package sql

import (
	"container/heap"
	"context"
	"reflect"
	"sync/atomic"

	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/kvclient"
	"github.com/sthorne/datax/pkg/metrics"
	"github.com/sthorne/datax/pkg/sql/catalog"
	"github.com/sthorne/datax/pkg/sql/parser"
	"github.com/sthorne/datax/pkg/sql/rowenc"
	"github.com/sthorne/datax/pkg/sql/types"
	"github.com/sthorne/datax/pkg/sql/vector"
)

// ORDER BY ... LIMIT k over a scan whose order the access path does not
// deliver (issue #304: ORDER BY embedding <-> $1 LIMIT 10, the exact
// nearest-neighbour search, is the shape that made it matter).
//
// Sorting needs every matching row in memory before the first k leave.
// Only the k best ever can leave, so the scan here keeps just those: it
// reads the span a page at a time and holds a heap of the k best rows
// seen so far, worst on top, where a new row either displaces the worst
// or is dropped on the spot. Memory is k rows and one page whatever the
// table's size — a table of 1536-dimension vectors is 6 KiB a row, and
// sorting a million of them to return ten would hold 6 GiB.
//
// The result is exactly the sort's: ties keep scan order (the sort is
// stable), which the heap reproduces by ordering equal keys by the
// sequence in which the scan produced them.

// topKPage is how many rows one read of the span takes.
const topKPage = 512

// topKEligible reports whether the select's fetch can run as a top-k
// scan: it sorts in memory, it has a limit, nothing between the fetch
// and the sort needs the whole row set (DISTINCT deduplicates after
// projection, a correlated filter runs per fetched row, FOR UPDATE
// locks what was fetched), and the plan walks one span of a real
// table's primary index.
func topKEligible(desc *catalog.TableDescriptor, plan accessPlan, t *parser.Select, needSort bool, corr []correlatedConjunct) bool {
	if !needSort || keepCount(t) <= 0 || t.Distinct || t.ForUpdate || len(corr) > 0 || desc.Virtual != "" {
		return false
	}
	switch plan.kind {
	case planFullScan:
		return true
	case planPKScan:
		return plan.fanBuckets == 0
	}
	return false
}

// topKSpec is the directive execSelect stamps on a plan
// (accessPlan.top): keep the first `keep` rows under order.
type topKSpec struct {
	order []parser.OrderCol
	keep  int64
	// vec, when a vector index serves the order (pickVectorIndex),
	// makes the fetch a probe of that index instead of a scan.
	vec *vectorPlan
}

// topKItem is a row in the heap with its sort key.
type topKItem struct {
	key   []types.Datum
	seq   int64 // scan position: the order among equal keys
	fr    fetchedRow
	bytes int64 // what the row was charged
}

// topKHeap keeps the worst kept row on top.
type topKHeap struct {
	items []topKItem
	keyer *orderKeyer
	err   error
}

// worse reports whether a sorts after b in the final order.
func (h *topKHeap) worse(a, b *topKItem) bool {
	c, err := h.keyer.compare(a.key, b.key)
	if err != nil && h.err == nil {
		h.err = err
	}
	if c != 0 {
		return c > 0
	}
	return a.seq > b.seq
}

func (h *topKHeap) Len() int           { return len(h.items) }
func (h *topKHeap) Less(i, j int) bool { return h.worse(&h.items[i], &h.items[j]) }
func (h *topKHeap) Swap(i, j int)      { h.items[i], h.items[j] = h.items[j], h.items[i] }
func (h *topKHeap) Push(x any)         { h.items = append(h.items, x.(topKItem)) }
func (h *topKHeap) Pop() any {
	it := h.items[len(h.items)-1]
	h.items = h.items[:len(h.items)-1]
	return it
}

// scanTopK scans the plan's span and returns the keep rows that sort
// first under order, in that order.
func (s *Session) scanTopK(ctx context.Context, txn *kvclient.Txn, desc *catalog.TableDescriptor, plan accessPlan, where []parser.Comparison, order []parser.OrderCol, params []types.Datum, keep int64) ([]fetchedRow, error) {
	keyer, err := newOrderKeyer(desc, order, params)
	if err != nil {
		return nil, err
	}
	start, end := rowenc.PrimarySpanFor(desc)
	if plan.kind == planPKScan {
		spans, err := pkScanSpans(desc, plan)
		if err != nil {
			return nil, err
		}
		start, end = spans[0].start, spans[0].end
	}
	noteIndexScan(desc, desc.LivePrimaryIndex())
	h := &topKHeap{keyer: keyer}
	dist := topKDistanceOf(desc, order)
	sealer := s.rowSealer()
	var probe [1]types.Datum
	memo := &whereMemo{}
	var scanned, matched int64
	cursor := keys.Key(start)
	for {
		kvs, err := txn.Scan(ctx, cursor, end, topKPage)
		if err != nil {
			return nil, err
		}
		for _, kv := range kvs {
			var key []types.Datum
			var row map[catalog.ColumnID]types.Datum
			if dist != nil && len(where) == 0 {
				// The distance straight off the stored bytes; the row is
				// decoded only if it makes the heap, which over a large
				// table few rows do.
				value, err := sealer.OpenValue(ctx, desc, kv.Key, kv.Value)
				if err != nil {
					return nil, err
				}
				if stored, ok := rowenc.VectorColumn(desc, value, dist.col); ok && len(stored) == len(dist.query) {
					matched++
					probe[0] = types.NewFloat(dist.metric.StoredDistanceBytes(stored, dist.query))
					if int64(h.Len()) == keep {
						cand := topKItem{key: probe[:], seq: matched}
						if !h.worse(&h.items[0], &cand) {
							if h.err != nil {
								return nil, newErrf(CodeInternal, "ORDER BY: %v", h.err)
							}
							continue
						}
					}
					key = []types.Datum{probe[0]}
				}
				if row, err = decodeOpenedRow(desc, kv.Key, value); err != nil {
					return nil, err
				}
			} else {
				if row, err = decodeFullRow(ctx, sealer, desc, kv.Key, kv.Value); err != nil {
					return nil, err
				}
			}
			if key == nil {
				match, err := matchesWhereMemo(where, desc, row, params, memo)
				if err != nil {
					return nil, err
				}
				if !match {
					continue
				}
				matched++
				if d, ok := dist.keyOf(row); ok {
					probe[0] = d
					key = probe[:]
				} else if key, err = keyer.key(row); err != nil {
					return nil, err
				}
			}
			it := topKItem{key: key, seq: matched, fr: fetchedRow{key: kv.Key, row: row}}
			if int64(h.Len()) == keep {
				// Full: the row is kept only if it sorts before the worst
				// kept one, which then leaves. It arrived later, so on a
				// tie it loses.
				if !h.worse(&h.items[0], &it) {
					if h.err != nil {
						return nil, newErrf(CodeInternal, "ORDER BY: %v", h.err)
					}
					continue
				}
				s.memUsed -= h.items[0].bytes
				heap.Pop(h)
			}
			if len(it.key) > 0 && &it.key[0] == &probe[0] {
				it.key = []types.Datum{probe[0]}
			}
			it.bytes = rowBytes(row)
			if err := s.charge(it.bytes); err != nil {
				return nil, err
			}
			heap.Push(h, it)
			if h.err != nil {
				return nil, newErrf(CodeInternal, "ORDER BY: %v", h.err)
			}
		}
		scanned += int64(len(kvs))
		if len(kvs) < topKPage {
			break
		}
		cursor = kvs[len(kvs)-1].Key.Next()
	}
	metrics.SQLRowsScanned.Add(float64(scanned))
	s.AddScanned(scanned)
	noteIndexRows(desc, desc.LivePrimaryIndex(), int(scanned))
	// The heap gives the rows worst first.
	out := make([]fetchedRow, h.Len())
	for i := len(out) - 1; i >= 0; i-- {
		out[i] = heap.Pop(h).(topKItem).fr
	}
	if h.err != nil {
		return nil, newErrf(CodeInternal, "ORDER BY: %v", h.err)
	}
	s.note("top-k: %d rows scanned, %d matched, %d kept in a heap of %d", scanned, matched, len(out), keep)
	return out, nil
}

// TestingNoStoredDistance turns off the distance-off-the-stored-bytes
// paths — the exact scan's (topKDistance) and the ivfflat candidate
// ranking's bounded heap — so a test can hold them to the general
// evaluation they replace.
var TestingNoStoredDistance atomic.Bool

// topKDistance is a sort key the scan computes off the stored row: the
// single ORDER BY term col <op> q, op a distance and q a vector constant,
// as foldDistanceConstants leaves the exact nearest-neighbour search.
// Its value for a row is the float the expression evaluates to there
// (builtins.VectorOp's two-stored-vectors case, the same arithmetic on
// the same bytes), so rows rank exactly as under the keyer; a row whose
// column is not a vector of q's dimension — NULL, or a dimension
// mismatch — takes the keyer, which gives its NULL or its error.
type topKDistance struct {
	col    catalog.ColumnID
	metric vector.Metric
	query  string // q in the stored form
}

// topKDistanceOf recognises the folded distance term; nil for any other
// order.
func topKDistanceOf(desc *catalog.TableDescriptor, order []parser.OrderCol) *topKDistance {
	if len(order) != 1 || TestingNoStoredDistance.Load() {
		return nil
	}
	e := order[0].Expr
	if e == nil || e.Right == nil || e.Right.Lit == nil {
		return nil
	}
	// Exactly a column, the operator and a literal: nothing else on the
	// node that evaluation would apply.
	if !reflect.DeepEqual(*e, parser.Expr{Column: e.Column, BinOp: e.BinOp, Right: e.Right}) ||
		!reflect.DeepEqual(*e.Right, parser.Expr{Lit: e.Right.Lit}) {
		return nil
	}
	m, ok := vector.MetricForOperator(e.BinOp)
	q := *e.Right.Lit
	if !ok || q.Null || q.Fam != types.Vector || e.Column == "" {
		return nil
	}
	col, ok := desc.Col(e.Column)
	if !ok || col.Type != types.Vector {
		return nil
	}
	return &topKDistance{col: col.ID, metric: m, query: q.S}
}

// keyOf is the term's value over a decoded row; ok is false when the
// keyer must evaluate it.
func (d *topKDistance) keyOf(row map[catalog.ColumnID]types.Datum) (types.Datum, bool) {
	if d == nil {
		return types.Datum{}, false
	}
	v, ok := row[d.col]
	if !ok || v.Null || v.Fam != types.Vector || len(v.S) != len(d.query) {
		return types.Datum{}, false
	}
	return types.NewFloat(d.metric.StoredDistance(v.S, d.query)), true
}

// rowBytes is what chargeRow charges for a row.
func rowBytes(row map[catalog.ColumnID]types.Datum) int64 {
	var n int64 = 64
	for _, d := range row {
		n += datumBytes(d)
	}
	return n
}
