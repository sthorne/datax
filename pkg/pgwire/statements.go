package pgwire

import (
	"container/list"
	"sort"
	"sync"
	"time"

	"github.com/sthorne/datax/pkg/sql/parser"
)

// Statement fingerprint accounting (issue #157).
//
// The rings above answer "what is running now" and "what was slow
// recently". Neither answers the question that drives optimisation work:
// which statement shape costs this cluster the most. A statement that
// takes 8ms and runs forty thousand times an hour never enters a
// slow-statement ring, and is usually the thing worth fixing.
//
// So executions are grouped by the shape the parser normalises them to
// and counted: how often, how long in total, how long typically, how
// many rows returned and read, and how often the transaction had to be
// retried. Total time is the figure that ranks them, because it is the
// one that says where the cluster's time actually goes.
//
// The table is bounded by an LRU. A pathological client can generate
// unbounded distinct shapes — a driver that inlines its parameters is
// enough — and an accounting table that grows with what clients send is
// a leak with a nice name.
const (
	// stmtShapeMax is the most shapes kept. Past it the least recently
	// executed is evicted, and the eviction is counted so the view can
	// say the list is a window rather than the whole truth.
	stmtShapeMax = 500
	// stmtLatencyRing bounds the per-shape latency samples the
	// percentiles are taken over. It is deliberately far smaller than
	// the connection-wide ring: one of those per shape, at the bound
	// above, would be megabytes of samples for a console column.
	stmtLatencyRing = 128
)

// shapeLatency is a small fixed ring of durations, one per shape.
type shapeLatency struct {
	buf  [stmtLatencyRing]time.Duration
	n    int
	next int
}

func (r *shapeLatency) add(d time.Duration) {
	r.buf[r.next] = d
	r.next = (r.next + 1) % stmtLatencyRing
	if r.n < stmtLatencyRing {
		r.n++
	}
}

func (r *shapeLatency) percentiles() (p50, p99 int64) {
	if r.n == 0 {
		return 0, 0
	}
	vals := make([]time.Duration, r.n)
	copy(vals, r.buf[:r.n])
	sort.Slice(vals, func(i, j int) bool { return vals[i] < vals[j] })
	at := func(p int) int64 {
		idx := (len(vals)*p + 99) / 100
		if idx >= len(vals) {
			idx = len(vals) - 1
		}
		return vals[idx].Microseconds()
	}
	return at(50), at(99)
}

// StatementStat is one shape's accounting, as the API reports it.
type StatementStat struct {
	// Fingerprint is the shape's stable hash; Shape the normalised text
	// it stands for. Both are derived from the parsed statement, not
	// from the text a client sent.
	Fingerprint string `json:"fingerprint"`
	Shape       string `json:"shape"`
	// Kind is the statement kind (select, insert, ...); Tables the
	// relations the shape names.
	Kind   string   `json:"kind"`
	Tables []string `json:"tables,omitempty"`
	// Representative is one statement text this shape was built from,
	// truncated. It can carry data, which is why this document is
	// admin-gated.
	Representative string `json:"representative,omitempty"`

	Count        uint64 `json:"count"`
	Errors       uint64 `json:"errors,omitempty"`
	Retries      uint64 `json:"retries,omitempty"`
	TotalMicros  int64  `json:"total_us"`
	MeanMicros   int64  `json:"mean_us"`
	P50Micros    int64  `json:"p50_us"`
	P99Micros    int64  `json:"p99_us"`
	MaxMicros    int64  `json:"max_us"`
	RowsReturned uint64 `json:"rows_returned"`
	RowsScanned  uint64 `json:"rows_scanned"`
	// LastAt is when this shape last ran.
	LastAt time.Time `json:"last_at"`
	// NodeID is set when the statistics are aggregated across the
	// cluster, so a row can say where its time was spent.
	NodeID int `json:"node_id,omitempty"`
}

// shapeStat is the live accounting for one fingerprint.
type shapeStat struct {
	fingerprint    string
	shape          string
	kind           string
	tables         []string
	representative string
	count          uint64
	errors         uint64
	retries        uint64
	totalMicros    int64
	maxMicros      int64
	rowsReturned   uint64
	rowsScanned    uint64
	lastAt         time.Time
	lat            shapeLatency
	// el is this shape's node in the LRU list, newest at the front.
	el *list.Element
}

// statements is the bounded fingerprint table.
type statements struct {
	mu      sync.Mutex
	byPrint map[string]*shapeStat
	lru     *list.List // *shapeStat, most recently executed at the front
	// evicted counts shapes dropped to stay within the bound, so the
	// console can say the list is a window over the busiest shapes
	// rather than every shape that ever ran.
	evicted uint64
}

func newStatements() *statements {
	return &statements{byPrint: map[string]*shapeStat{}, lru: list.New()}
}

// record charges one execution to its shape.
func (s *statements) record(shape parser.Shape, kind, text string, d time.Duration, rows, scanned int64, failed, retry bool) {
	if s == nil || shape.Hash == "" {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	st := s.byPrint[shape.Hash]
	if st == nil {
		if len(s.byPrint) >= stmtShapeMax {
			s.evictLocked()
		}
		st = &shapeStat{
			fingerprint: shape.Hash, shape: shape.Text, kind: kind,
			tables: shape.Tables, representative: truncateStmt(text),
		}
		s.byPrint[shape.Hash] = st
		st.el = s.lru.PushFront(st)
	} else {
		s.lru.MoveToFront(st.el)
	}
	us := d.Microseconds()
	st.count++
	st.totalMicros += us
	if us > st.maxMicros {
		st.maxMicros = us
	}
	if rows > 0 {
		st.rowsReturned += uint64(rows)
	}
	if scanned > 0 {
		st.rowsScanned += uint64(scanned)
	}
	if failed {
		st.errors++
	}
	if retry {
		st.retries++
	}
	st.lastAt = time.Now()
	st.lat.add(d)
}

// evictLocked drops the least recently executed shape.
func (s *statements) evictLocked() {
	back := s.lru.Back()
	if back == nil {
		return
	}
	st := back.Value.(*shapeStat)
	s.lru.Remove(back)
	delete(s.byPrint, st.fingerprint)
	s.evicted++
}

// Top returns the heaviest shapes by total time, and how many shapes
// have been evicted to stay within the bound.
//
// Total time is the ranking because it is the figure that answers "where
// does this cluster's time go": a fast statement run often outranks a
// slow one run rarely, which is the whole point.
func (s *statements) Top(limit int) (out []StatementStat, evicted uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out = make([]StatementStat, 0, len(s.byPrint))
	for _, st := range s.byPrint {
		p50, p99 := st.lat.percentiles()
		mean := int64(0)
		if st.count > 0 {
			mean = st.totalMicros / int64(st.count)
		}
		out = append(out, StatementStat{
			Fingerprint: st.fingerprint, Shape: st.shape, Kind: st.kind,
			Tables: append([]string(nil), st.tables...), Representative: st.representative,
			Count: st.count, Errors: st.errors, Retries: st.retries,
			TotalMicros: st.totalMicros, MeanMicros: mean,
			P50Micros: p50, P99Micros: p99, MaxMicros: st.maxMicros,
			RowsReturned: st.rowsReturned, RowsScanned: st.rowsScanned,
			LastAt: st.lastAt,
		})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].TotalMicros != out[j].TotalMicros {
			return out[i].TotalMicros > out[j].TotalMicros
		}
		return out[i].Fingerprint < out[j].Fingerprint
	})
	if limit > 0 && len(out) > limit {
		out = out[:limit]
	}
	return out, s.evicted
}

// Representative returns one statement text recorded for a fingerprint,
// for the console's EXPLAIN. Empty when the shape is not (or no longer)
// held.
func (s *statements) Representative(fingerprint string) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if st := s.byPrint[fingerprint]; st != nil {
		return st.representative
	}
	return ""
}
