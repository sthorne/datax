package sql

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"math"
	"sort"

	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/kvclient"
	"github.com/sthorne/datax/pkg/metrics"
	"github.com/sthorne/datax/pkg/sql/catalog"
	"github.com/sthorne/datax/pkg/sql/parser"
	"github.com/sthorne/datax/pkg/sql/rowenc"
	"github.com/sthorne/datax/pkg/sql/types"
)

// Statistics collection: one paced sweep of the primary index at a frozen
// timestamp (the CREATE INDEX backfill planning pattern — a frozen
// boundary makes termination independent of concurrent ingest), counting
// rows exactly and estimating per-column distinct counts with a KMV
// (k-minimum-values) sketch. Chunked reads bound KV batch sizes; the
// sweep never opens a transaction, so it takes no locks and leaves no
// intents.
const (
	statsChunkSize = 1024 // rows per ScanAt (wipeChunk precedent)
	kmvK           = 256  // sketch capacity: exact below K, ±~6% beyond
)

// kmvSketch estimates the number of distinct values from the K smallest
// 64-bit hashes: if K uniform hashes occupy [0, kthMin], the full
// population is ≈ (K−1)·2^64/kthMin. Below capacity the count is exact.
type kmvSketch struct {
	hashes map[uint64]struct{}
	// sorted kept lazily; we only need the kth-largest bound at capacity.
	max uint64 // current largest retained hash (valid when saturated)
}

func newKMV() *kmvSketch {
	return &kmvSketch{hashes: make(map[uint64]struct{}, kmvK)}
}

func (s *kmvSketch) add(h uint64) {
	if _, ok := s.hashes[h]; ok {
		return
	}
	if len(s.hashes) < kmvK {
		s.hashes[h] = struct{}{}
		if h > s.max {
			s.max = h
		}
		return
	}
	if h >= s.max {
		return
	}
	// Replace the current maximum with the smaller newcomer.
	delete(s.hashes, s.max)
	s.hashes[h] = struct{}{}
	s.max = 0
	for v := range s.hashes {
		if v > s.max {
			s.max = v
		}
	}
}

func (s *kmvSketch) distinct() int64 {
	n := len(s.hashes)
	if n < kmvK {
		return int64(n)
	}
	// kthMin = s.max (the largest of the K smallest hashes).
	if s.max == 0 {
		return int64(n)
	}
	est := float64(kmvK-1) * math.Pow(2, 64) / float64(s.max)
	return int64(est)
}

// mix64 is the splitmix64 finalizer: KMV reads the hash's ORDER
// statistics, and raw fnv-1a output is measurably non-uniform on short
// similar inputs (a 60%+ estimate skew in testing); the finalizer
// restores uniformity.
func mix64(h uint64) uint64 {
	h ^= h >> 30
	h *= 0xbf58476d1ce4e5b9
	h ^= h >> 27
	h *= 0x94d049bb133111eb
	h ^= h >> 31
	return h
}

// statsHashDatum hashes a non-NULL datum over a purpose-built canonical
// byte form: representations that compare equal MUST hash equal. In
// particular DECIMAL hashes its canonical text (Datum.S), never the
// display-scale padding, so 1.0 and 1.00 collide as one value.
func statsHashDatum(d types.Datum) uint64 {
	h := fnv.New64a()
	var kind [1]byte
	kind[0] = byte(d.Fam)
	_, _ = h.Write(kind[:])
	if d.Fam.IsArray() {
		_, _ = h.Write([]byte(d.Text()))
		return h.Sum64()
	}
	switch d.Fam {
	case types.Int, types.Timestamp, types.Date, types.Time, types.Enum:
		var b [8]byte
		binary.BigEndian.PutUint64(b[:], uint64(d.I))
		_, _ = h.Write(b[:])
	case types.IntervalFam:
		var b [8]byte
		binary.BigEndian.PutUint64(b[:], uint64(d.IntervalVal().CmpValue()))
		_, _ = h.Write(b[:])
	case types.Float:
		var b [8]byte
		binary.BigEndian.PutUint64(b[:], math.Float64bits(d.F))
		_, _ = h.Write(b[:])
	case types.Bool:
		if d.B {
			_, _ = h.Write([]byte{1})
		} else {
			_, _ = h.Write([]byte{0})
		}
	default:
		// String, Bytes, Uuid, Decimal (canonical text), Jsonb (normalized
		// text): S is the identity representation.
		_, _ = h.Write([]byte(d.S))
	}
	return mix64(h.Sum64())
}

// CollectTableStats sweeps the table's primary index at a frozen
// timestamp and returns fresh statistics. Exported for the background
// sampler in pkg/server; ANALYZE uses it too.
func CollectTableStats(ctx context.Context, db *kvclient.DB, desc *catalog.TableDescriptor) (*catalog.TableStatistics, error) {
	boundary := db.Clock().Now()
	cursor, end := rowenc.PrimarySpanFor(desc)

	cols := desc.VisibleColumns()
	sketches := make(map[catalog.ColumnID]*kmvSketch, len(cols))
	nulls := make(map[catalog.ColumnID]int64, len(cols))
	for _, c := range cols {
		sketches[c.ID] = newKMV()
	}

	var rows int64
	for {
		kvs, err := db.ScanAt(ctx, cursor, end, statsChunkSize, boundary)
		if err != nil {
			return nil, err
		}
		if len(kvs) == 0 {
			break
		}
		for _, kv := range kvs {
			row, derr := decodeFullRow(desc, kv.Key, kv.Value)
			if derr != nil {
				// A row we cannot decode (concurrent schema change edge)
				// still counts; its column values are skipped.
				rows++
				continue
			}
			rows++
			for _, c := range cols {
				d, ok := row[c.ID]
				if !ok || d.Null {
					nulls[c.ID]++
					continue
				}
				sketches[c.ID].add(statsHashDatum(d))
			}
		}
		cursor = kvs[len(kvs)-1].Key.Next()
	}

	st := &catalog.TableStatistics{
		TableID:     desc.ID,
		RowCount:    rows,
		CollectedAt: boundary.WallTime,
	}
	for _, c := range cols {
		st.Columns = append(st.Columns, catalog.ColumnStatistics{
			ID: c.ID, Name: c.Name,
			Distinct: sketches[c.ID].distinct(),
			Nulls:    nulls[c.ID],
		})
	}
	sort.Slice(st.Columns, func(i, j int) bool { return st.Columns[i].ID < st.Columns[j].ID })
	return st, nil
}

// execAnalyze collects and stores statistics for one table (or all).
// Reached only through the executeData interception: not inside a
// transaction block, admin already checked.
func (s *Session) execAnalyze(ctx context.Context, an *parser.Analyze) (*Result, *Error) {
	var descs []*catalog.TableDescriptor
	if err := s.db.RunTxn(ctx, "analyze-plan", func(ctx context.Context, txn *kvclient.Txn) error {
		if an.Table != "" {
			d, derr := s.lookup(ctx, txn, an.Table)
			if derr != nil {
				return derr
			}
			if derr := mustBeReal(d); derr != nil {
				return derr
			}
			descs = []*catalog.TableDescriptor{d}
			return nil
		}
		db, derr := s.cat.Database(ctx, txn, s.database)
		if derr != nil {
			return derr
		}
		all, lerr := s.cat.ListIn(ctx, txn, db)
		if lerr != nil {
			return lerr
		}
		for _, d := range all {
			if !d.IsView() {
				descs = append(descs, d)
			}
		}
		return nil
	}); err != nil {
		return nil, ToSQLError(err)
	}

	for _, desc := range descs {
		st, err := CollectTableStats(ctx, s.db, desc)
		if err != nil {
			return nil, ToSQLError(err)
		}
		raw, err := json.Marshal(st)
		if err != nil {
			return nil, newErrf(CodeInternal, "encoding statistics: %v", err)
		}
		if err := s.db.Put(ctx, keys.TableStatsKey(desc.ID), raw); err != nil {
			return nil, ToSQLError(err)
		}
		s.cat.InvalidateStats(desc.ID)
		metrics.StatsRefreshes.Inc()
		metrics.StatsRowsScanned.Add(float64(st.RowCount))
	}
	return &Result{Tag: fmt.Sprintf("ANALYZE %d", len(descs))}, nil
}

// execShowStats renders the stored statistics for a table: one row per
// column, the table row count and collection time repeated (read-only,
// no admin needed).
func (s *Session) execShowStats(ctx context.Context, txn *kvclient.Txn, t *parser.ShowStats) (*Result, error) {
	desc, err := s.lookup(ctx, txn, t.Table)
	if err != nil {
		return nil, err
	}
	res := &Result{Columns: []ResultColumn{
		{Name: "table_name", Type: types.String},
		{Name: "row_count", Type: types.Int},
		{Name: "collected_at", Type: types.Timestamp},
		{Name: "column_name", Type: types.String},
		{Name: "distinct_count", Type: types.Int},
		{Name: "null_count", Type: types.Int},
	}}
	raw, err := txn.Get(ctx, keys.TableStatsKey(desc.ID))
	if err != nil {
		return nil, err
	}
	if raw != nil {
		var st catalog.TableStatistics
		if jerr := json.Unmarshal(raw, &st); jerr == nil {
			for _, c := range st.Columns {
				res.Rows = append(res.Rows, []types.Datum{
					types.NewString(desc.Name),
					types.NewInt(st.RowCount),
					{Fam: types.Timestamp, I: st.CollectedAt},
					types.NewString(c.Name),
					types.NewInt(c.Distinct),
					types.NewInt(c.Nulls),
				})
			}
		}
	}
	res.Tag = fmt.Sprintf("SHOW STATS %d", len(res.Rows))
	return res, nil
}
