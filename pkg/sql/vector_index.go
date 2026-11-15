package sql

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"math"
	mrand "math/rand"
	"sort"
	"strconv"

	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/kvclient"
	"github.com/sthorne/datax/pkg/metrics"
	"github.com/sthorne/datax/pkg/sql/catalog"
	"github.com/sthorne/datax/pkg/sql/parser"
	"github.com/sthorne/datax/pkg/sql/rowenc"
	"github.com/sthorne/datax/pkg/sql/types"
	"github.com/sthorne/datax/pkg/sql/vector"
	"github.com/sthorne/datax/pkg/util/encoding"
)

// Approximate nearest-neighbour search through an IVF index (issue
// #304): CREATE INDEX ... USING ivfflat (col vector_l2_ops) WITH (lists
// = n), pgvector's spelling.
//
// The index clusters the column's vectors around `lists` centres and
// files each row under the centre nearest its vector (see
// catalog.VectorIndex for the keys). A query — ORDER BY col <-> $1
// LIMIT k — ranks the centres by distance to $1, reads the lists of
// the nearest `ivfflat.probes` of them, ranks the entries it read by
// their true distance, and fetches the rows of the best in that order.
// What it returns is exact for the lists it read and blind to the
// rest: a row nearer than the k-th returned one may sit in a list that
// was not probed. That is the whole approximation, and probes is the
// dial — every list probed is the exact answer, at the cost of reading
// the whole index.
//
// Why this shape and not a graph (HNSW). On an ordered, range-
// partitioned store a list is a contiguous key span: a probe is one
// scan, lists split and merge and rebalance as ranges like any other
// data, and a row's entry is one key written in the row's own
// transaction. A graph index answers a query with hundreds of
// dependent point reads scattered over ranges and maintains itself by
// rewriting the neighbour lists of other rows on every insert — writes
// to keys the inserting transaction never read, which under
// serializable isolation makes unrelated inserts conflict. USING hnsw
// is therefore refused, by name, rather than accepted and built as
// something else.
//
// The centres are trained once, when the index is created, from a
// sample of the table, and never move afterwards: an UPDATE or DELETE
// finds a row's entry by recomputing which list its old vector was
// filed under, so a centre that moved would strand every entry filed
// before. The price is drift — data that arrives later and does not
// resemble the sample is filed under centres that fit it poorly, the
// lists grow unevenly, and recall at a given probes falls. The remedy
// is the one pgvector documents: build the index after the table has
// representative data, and drop and recreate it when the data has
// changed character. An index built on an empty table has a single
// list, so it is exact and no faster than a scan until rebuilt.

const (
	// defaultVectorLists is pgvector's default for ivfflat.
	defaultVectorLists = 100
	// maxVectorLists is pgvector's upper bound for lists.
	maxVectorLists = 32768
	// vectorSamplePerList is how many sampled rows the training wants
	// per list (pgvector samples 50).
	vectorSamplePerList = 50
	// vectorSampleMin keeps a small lists setting from training on a
	// handful of rows.
	vectorSampleMin = 2000
	// vectorScanPage is how many entries one read of a list takes.
	vectorScanPage = 1024
)

// vectorIndexSpec is what CREATE INDEX ... USING ivfflat asked for.
type vectorIndexSpec struct {
	metric vector.Metric
	lists  int
}

// vectorSpecFor validates the access method, operator class and WITH
// parameters of a CREATE INDEX; nil when the statement asks for an
// ordinary or inverted index.
func (s *Session) vectorSpecFor(t *parser.CreateIndex) (*vectorIndexSpec, error) {
	switch t.Using {
	case "ivfflat":
	case "hnsw":
		return nil, newErrf(CodeFeatureNotSupported, "access method \"hnsw\" is not supported: a graph index needs many dependent point reads per query and rewrites other rows' neighbour lists on every insert, which a transactional range-partitioned store serves poorly. Use USING ivfflat, which serves the same ORDER BY ... LIMIT queries (tune recall with SET ivfflat.probes)")
	default:
		if len(t.With) > 0 {
			return nil, newErrf(CodeFeatureNotSupported, "index storage parameters (WITH) are only taken by USING ivfflat")
		}
		return nil, nil
	}
	if err := s.requireV21("CREATE INDEX ... USING ivfflat"); err != nil {
		return nil, err
	}
	if t.Unique {
		return nil, newErrf(CodeFeatureNotSupported, "an ivfflat index cannot be UNIQUE: it files vectors by nearness, not by value")
	}
	if len(t.Columns) != 1 {
		return nil, newErrf(CodeFeatureNotSupported, "an ivfflat index takes exactly one column")
	}
	spec := &vectorIndexSpec{metric: vector.L2, lists: defaultVectorLists}
	if len(t.OpClasses) > 0 && t.OpClasses[0] != "" {
		m, ok := vector.MetricForOpClass(t.OpClasses[0])
		if !ok {
			return nil, newErrf(CodeUndefinedObject, "operator class %q does not exist for access method \"ivfflat\" (vector_l2_ops, vector_cosine_ops and vector_ip_ops do)", t.OpClasses[0])
		}
		spec.metric = m
	}
	for name, value := range t.With {
		if name != "lists" {
			return nil, newErrf(CodeInvalidParameterValue, "unrecognized parameter %q for an ivfflat index (lists is the one it takes)", name)
		}
		n, err := strconv.Atoi(value)
		if err != nil || n < 1 || n > maxVectorLists {
			return nil, newErrf(CodeInvalidParameterValue, "lists must be between 1 and %d, not %q", maxVectorLists, value)
		}
		spec.lists = n
	}
	return spec, nil
}

// vectorIndexColumn checks that the named column can carry an IVF
// index: a vector with a declared dimension within the index limit.
func vectorIndexColumn(desc *catalog.TableDescriptor, name string) (catalog.Column, error) {
	col, ok := desc.Col(name)
	if !ok {
		return col, newErrf(CodeUndefinedColumn, "column %q does not exist", name)
	}
	if col.Type != types.Vector {
		return col, newErrf(CodeFeatureNotSupported, "column %q of type %s cannot have an ivfflat index (VECTOR columns can)", name, col.Type)
	}
	if col.MaxLen == 0 {
		return col, newErrf(CodeFeatureNotSupported, "column %q does not have dimensions: declare it vector(n) to index it", name)
	}
	if col.MaxLen > vector.MaxIndexDims {
		return col, newErrf(CodeProgramLimitExceeded, "column %q cannot have more than %d dimensions for an ivfflat index", name, vector.MaxIndexDims)
	}
	return col, nil
}

// newVectorBuildID names a training run.
func newVectorBuildID() (string, error) {
	var b [12]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(b[:]), nil
}

// trainVectorIndex samples the table, trains the centres and stores
// them, before the index exists in the descriptor: by the time any
// writer can see the index, the centres it files rows by are committed
// and final. The sample is read at one timestamp without a
// transaction — it only has to be representative — and reservoir
// sampling decodes just the rows it keeps.
func (s *Session) trainVectorIndex(ctx context.Context, t *parser.CreateIndex, spec *vectorIndexSpec) (*catalog.VectorIndex, uint64, error) {
	var desc *catalog.TableDescriptor
	var col catalog.Column
	if err := s.db.RunTxn(ctx, "create-index-check", func(ctx context.Context, txn *kvclient.Txn) error {
		shared, err := s.lookup(ctx, txn, t.Table)
		if err != nil {
			return err
		}
		if err := mustBeReal(shared); err != nil {
			return err
		}
		if _, taken := shared.Index(t.Name); taken {
			// The publish step reports it (or honours IF NOT EXISTS);
			// there is nothing to train for.
			desc = nil
			return nil
		}
		if col, err = vectorIndexColumn(shared, t.Columns[0]); err != nil {
			return err
		}
		desc = shared
		return nil
	}); err != nil {
		return nil, 0, err
	}
	if desc == nil {
		return nil, 0, nil
	}
	dims := int(col.MaxLen)
	want := spec.lists * vectorSamplePerList
	if want < vectorSampleMin {
		want = vectorSampleMin
	}
	rng := mrand.New(mrand.NewSource(int64(desc.ID)<<20 ^ int64(len(t.Name))))
	sample := make([][]float32, 0, want)
	vecOf := func(key keys.Key, value []byte) ([]float32, error) {
		row, err := decodeFullRow(desc, key, value)
		if err != nil {
			return nil, err
		}
		d, ok := row[col.ID]
		if !ok || d.Null || d.Fam != types.Vector || d.VectorDims() != dims {
			return nil, nil
		}
		return d.VectorVal(), nil
	}
	boundary := s.db.Clock().Now()
	cursor, end := rowenc.PrimarySpanFor(desc)
	seen := 0
	for {
		kvs, err := s.db.ScanAt(ctx, cursor, end, vectorScanPage, boundary)
		if err != nil {
			return nil, 0, err
		}
		for _, kv := range kvs {
			seen++
			slot := len(sample)
			if slot >= want {
				if slot = rng.Intn(seen); slot >= want {
					continue
				}
			}
			v, err := vecOf(kv.Key, kv.Value)
			if err != nil {
				return nil, 0, err
			}
			if v == nil {
				continue
			}
			if slot == len(sample) {
				sample = append(sample, v)
			} else {
				sample[slot] = v
			}
		}
		if len(kvs) < vectorScanPage {
			break
		}
		cursor = kvs[len(kvs)-1].Key.Next()
	}
	cents := vector.Train(sample, dims, spec.lists, spec.metric, int64(desc.ID), func() bool { return ctx.Err() != nil })
	if err := ctx.Err(); err != nil {
		return nil, 0, err
	}
	build, err := newVectorBuildID()
	if err != nil {
		return nil, 0, newErrf(CodeInternal, "naming the index build: %v", err)
	}
	// A centre is up to 8 KiB; a few dozen to a transaction keeps each
	// raft entry small.
	const perTxn = 64
	for lo := 0; lo < len(cents.Centers); lo += perTxn {
		hi := lo + perTxn
		if hi > len(cents.Centers) {
			hi = len(cents.Centers)
		}
		if err := s.db.RunTxn(ctx, "create-index-centroids", func(ctx context.Context, txn *kvclient.Txn) error {
			var wb kvclient.WriteBatch
			for i := lo; i < hi; i++ {
				wb.Put(keys.VectorCentroidKey(desc.ID, build, i), vector.Encode(cents.Centers[i]))
			}
			return txn.RunBatch(ctx, &wb)
		}); err != nil {
			s.dropVectorCentroids(ctx, desc.ID, build)
			return nil, 0, err
		}
	}
	vector.Register(build, cents)
	return &catalog.VectorIndex{Method: "ivfflat", Metric: string(spec.metric), Lists: len(cents.Centers), Dims: dims, Build: build}, desc.ID, nil
}

// dropVectorCentroids removes a build's centres (best effort: without
// an index naming the build they are unreachable, and DROP TABLE takes
// them with the rest of the table's keys).
func (s *Session) dropVectorCentroids(ctx context.Context, tableID uint64, build string) {
	dropVectorCentroids(ctx, s.db, tableID, build)
}

func dropVectorCentroids(ctx context.Context, db *kvclient.DB, tableID uint64, build string) {
	vector.Forget(build)
	prefix := keys.VectorCentroidPrefix(tableID, build)
	_ = db.RunTxn(ctx, "drop-index-centroids", func(ctx context.Context, txn *kvclient.Txn) error {
		kvs, err := txn.Scan(ctx, prefix, prefix.PrefixEnd(), 0)
		if err != nil {
			return err
		}
		var wb kvclient.WriteBatch
		for _, kv := range kvs {
			wb.Delete(kv.Key)
		}
		return txn.RunBatch(ctx, &wb)
	})
}

// loadVectorCentroids makes sure the centres of every vector index of
// the table are in this process's registry, where the row encoder
// (rowenc.IndexEntries) finds them. They are immutable and were
// committed before the index was published, so they are read outside
// the caller's transaction and kept. A build whose centres cannot be
// read whole is left unregistered: writes to the table then fail by
// name of the index rather than file rows under a partial set, while
// reads that do not use the index, and DROP INDEX, still work.
func (s *Session) loadVectorCentroids(ctx context.Context, desc *catalog.TableDescriptor) error {
	for i := range desc.Indexes {
		v := desc.Indexes[i].Vector
		if v == nil {
			continue
		}
		if _, ok := vector.Lookup(v.Build); ok {
			continue
		}
		prefix := keys.VectorCentroidPrefix(desc.ID, v.Build)
		kvs, err := s.db.Scan(ctx, prefix, prefix.PrefixEnd(), 0)
		if err != nil {
			return err
		}
		if len(kvs) != v.Lists {
			continue
		}
		cents := &vector.Centroids{Metric: vector.Metric(v.Metric), Centers: make([][]float32, len(kvs))}
		whole := true
		for j, kv := range kvs {
			rest, list, err := encoding.DecodeUint64(kv.Key[len(prefix):])
			if err != nil || len(rest) != 0 || int(list) != j || len(kv.Value) != 4*v.Dims {
				whole = false
				break
			}
			cents.Centers[j] = vector.DecodeBytes(kv.Value, nil)
		}
		if whole {
			vector.Register(v.Build, cents)
		}
	}
	return nil
}

// vectorPlan is a nearest-neighbour query a vector index can answer.
type vectorPlan struct {
	idx    *catalog.IndexDescriptor
	metric vector.Metric
	query  []float32
	probes int
}

// probesFor resolves ivfflat.probes against an index: the setting, or
// by default the square root of the number of lists (the usual starting
// point for a good recall; pgvector's own default of 1 reads a single
// list).
func (s *Session) probesFor(v *catalog.VectorIndex) int {
	p := s.vars.ivfProbes
	if p == 0 {
		p = int(math.Ceil(math.Sqrt(float64(v.Lists))))
	}
	if p > v.Lists {
		p = v.Lists
	}
	if p < 1 {
		p = 1
	}
	return p
}

// distanceTerm recognises col <op> value or value <op> col, where op
// is a distance operator and value does not depend on the row.
func distanceTerm(desc *catalog.TableDescriptor, e *parser.Expr, params []types.Datum) (col catalog.Column, metric vector.Metric, query types.Datum, ok bool) {
	if e == nil || e.Right == nil || e.Cast != "" || len(e.Path) > 0 {
		return col, "", query, false
	}
	metric, isDist := vector.MetricForOperator(e.BinOp)
	if !isDist {
		return col, "", query, false
	}
	left := *e
	left.BinOp, left.Right = "", nil
	colSide, valSide := left, *e.Right
	if !plainColumn(colSide) {
		colSide, valSide = *e.Right, left
	}
	if !plainColumn(colSide) || colSide.Cast != "" {
		return col, "", query, false
	}
	col, found := desc.Col(colSide.Column)
	if !found || col.Type != types.Vector {
		return col, "", query, false
	}
	q, err := evalExpr(valSide, nil, nil, params)
	if err != nil || q.Null {
		return col, "", query, false
	}
	return col, metric, q, true
}

// foldDistanceConstants reads the constant operand of each distance
// term once, as a vector, so that the per-row evaluation of the sort
// key compares two stored vectors instead of parsing the same text
// literal or parameter for every row. A term whose constant is not a
// vector is left as written, for the evaluation to refuse.
func foldDistanceConstants(desc *catalog.TableDescriptor, order []parser.OrderCol, params []types.Datum) []parser.OrderCol {
	var out []parser.OrderCol
	for i, oc := range order {
		col, metric, q, ok := distanceTerm(desc, oc.Expr, params)
		if !ok {
			continue
		}
		qv, err := q.Coerce(types.Vector)
		if err != nil {
			continue
		}
		if out == nil {
			out = append([]parser.OrderCol(nil), order...)
		}
		// All three distances are symmetric: the column goes on the left
		// whichever side it was written on.
		out[i].Expr = &parser.Expr{Column: col.Name, BinOp: metric.Operator(), Right: &parser.Expr{Lit: &qv}}
	}
	if out == nil {
		return order
	}
	return out
}

// pickVectorIndex finds the vector index that answers the select's
// ORDER BY ... LIMIT: one ascending distance term over an indexed
// column, by the metric the index was built for, against a vector of
// the index's dimension. The WHERE clause, if any, must leave the
// planner nothing better than a full scan: a filter an ordinary index
// or the primary key can serve is answered exactly through that
// instead, over the rows it selects.
func (s *Session) pickVectorIndex(desc *catalog.TableDescriptor, plan accessPlan, order []parser.OrderCol, params []types.Datum) *vectorPlan {
	if s.vars.noVectorScan || plan.kind != planFullScan || len(order) != 1 {
		return nil
	}
	oc := order[0]
	if oc.Desc || oc.Nulls == "first" {
		return nil
	}
	col, metric, q, ok := distanceTerm(desc, oc.Expr, params)
	if !ok {
		return nil
	}
	for i := range desc.Indexes {
		idx := &desc.Indexes[i]
		if !idx.IsVector() || !idx.Public() || idx.ColumnIDs[0] != col.ID || idx.Vector.Metric != string(metric) {
			continue
		}
		qv, err := q.Coerce(types.Vector)
		if err != nil || qv.VectorDims() != idx.Vector.Dims {
			return nil // the exact path reports what is wrong with it
		}
		if _, loaded := vector.Lookup(idx.Vector.Build); !loaded {
			return nil
		}
		return &vectorPlan{idx: idx, metric: metric, query: qv.VectorVal(), probes: s.probesFor(idx.Vector)}
	}
	return nil
}

func (vp *vectorPlan) String() string {
	return fmt.Sprintf("vector scan of index %q (ivfflat, %s: the nearest %d of %d lists probed, approximate) + primary key join, rows rechecked",
		vp.idx.Name, vp.metric.Operator(), vp.probes, vp.idx.Vector.Lists)
}

// vectorCandidate is an index entry ranked for the query.
type vectorCandidate struct {
	dist float64
	pk   []byte // the encoded primary key at the end of the entry's key
}

// executeVectorScan answers the plan: it reads the probed lists,
// ranks their entries by distance to the query, and fetches rows in
// that order through the ordinary primary-row path — decoding each and
// rechecking the full WHERE clause — until keep rows have passed. The
// filter is applied to candidates nearest first, so a selective filter
// costs fetches, not answers: every row of the probed lists that
// passes is a candidate, not only the nearest keep of them.
func (s *Session) executeVectorScan(ctx context.Context, txn *kvclient.Txn, desc *catalog.TableDescriptor, vp *vectorPlan, where []parser.Comparison, params []types.Datum, keep int64) ([]fetchedRow, error) {
	cents, ok := vector.Lookup(vp.idx.Vector.Build)
	if !ok {
		return nil, newErrf(CodeInternal, "%v", &rowenc.ErrCentroidsNotLoaded{Index: vp.idx.Name})
	}
	noteIndexScan(desc, vp.idx.ID)
	lists := cents.Probe(vp.query, vp.probes)
	var cands []vectorCandidate
	var buf []float32
	entries := 0
	for _, list := range lists {
		start := rowenc.VectorListPrefix(desc.ID, vp.idx.ID, list)
		end := start.PrefixEnd()
		at := len(start)
		cursor := start
		for {
			kvs, err := txn.Scan(ctx, cursor, end, vectorScanPage)
			if err != nil {
				return nil, err
			}
			for _, kv := range kvs {
				if len(kv.Value) != 4*len(vp.query) {
					return nil, newErrf(CodeInternal, "index %q entry holds %d bytes, want a %d-dimension vector", vp.idx.Name, len(kv.Value), len(vp.query))
				}
				buf = vector.DecodeBytes(kv.Value, buf)
				cands = append(cands, vectorCandidate{dist: vp.metric.Distance(vp.query, buf), pk: kv.Key[at:]})
			}
			entries += len(kvs)
			// The candidates are held until the rows are fetched: they
			// count against the statement's memory like rows do.
			if err := s.charge(int64(len(kvs)) * int64(40+len(desc.PrimaryKey)*9)); err != nil {
				return nil, err
			}
			if len(kvs) < vectorScanPage {
				break
			}
			cursor = kvs[len(kvs)-1].Key.Next()
		}
	}
	metrics.SQLRowsScanned.Add(float64(entries))
	s.AddScanned(int64(entries))
	noteIndexRows(desc, vp.idx.ID, entries)
	// Nearest first; NaN (the cosine distance to a zero vector) after
	// every number; ties in primary-key order, as the exact scan breaks
	// them.
	sort.Slice(cands, func(i, j int) bool {
		a, b := cands[i], cands[j]
		if a.dist != b.dist {
			an, bn := math.IsNaN(a.dist), math.IsNaN(b.dist)
			if an || bn {
				if an != bn {
					return bn
				}
			} else {
				return a.dist < b.dist
			}
		}
		return string(a.pk) < string(b.pk)
	})
	prefix := rowenc.PrimaryKeyPrefixFor(desc)
	var out []fetchedRow
	fetched, batches := 0, 0
	for first := 0; first < len(cands) && (keep <= 0 || int64(len(out)) < keep); {
		// Without a filter the first keep candidates are the answer; with
		// one, each round fetches as many as are still wanted.
		n := primaryFetchPage
		if keep > 0 && keep-int64(len(out)) < int64(n) {
			n = int(keep - int64(len(out)))
		}
		if first+n > len(cands) {
			n = len(cands) - first
		}
		pks := make([]keys.Key, n)
		for i := range pks {
			ref := cands[first+i].pk
			pk := make(keys.Key, 0, len(prefix)+len(ref))
			pks[i] = append(append(pk, prefix...), ref...)
		}
		err := fetchPrimaryRows(ctx, txn, pks, func(at int, raws [][]byte) (bool, error) {
			batches++
			for i, raw := range raws {
				pk := pks[at+i]
				if raw == nil {
					return false, newErrf(CodeInternal, "index %q entry points at a missing row", vp.idx.Name)
				}
				row, err := decodeFullRow(desc, pk, raw)
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
			}
			return true, nil
		})
		if err != nil {
			return nil, err
		}
		fetched += n
		first += n
	}
	s.note("vector scan: %d of %d lists probed, %d index entries ranked, %d candidate rows fetched in %d batches, %d kept by the recheck", len(lists), vp.idx.Vector.Lists, entries, fetched, batches, len(out))
	return out, nil
}
