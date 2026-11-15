package server

import (
	"hash/fnv"
	"sort"
	"sync"
	"time"
)

// Writing the per-table and per-index series only when they change
// (issue #316).
//
// The recorder wrote, every tick, a table.ranges row per table (and a
// table.rows row per analyzed table) and an index.scans and index.rows
// row per index the node had read through: rows that grow with the
// catalog and, for a quiet table, repeat the last tick's values. From
// cluster version v22 those series are sparse: a row is written when the
// value differs from the last one the recorder wrote, and otherwise once
// every metricsKeyframeTicks ticks (a keyframe; which tick depends on
// the series' name, so the keyframes of thousands of series spread over
// the ticks instead of landing on one). A series that stops being
// sampled — its table dropped or renamed, its index dropped — gets one
// row with a NULL value, which ends it. And every tick writes a marker
// row, metricsTickSeries, for each node it records for (the node, and
// node 0 on the leader of range 1), which says "the recorder wrote at
// this tick".
//
// The reader (/api/metrics, fillSparse) puts the values back: at each
// marker tick a sparse series without a row of its own takes the last
// row written at or before it, if that row has a value and is no older
// than the keyframe period. An idle table therefore reads exactly as it
// did when every tick wrote it — its last value at every tick the
// recorder ran, a gap where it did not — and a dropped one ends where it
// did. What the reader cannot know is a series that ended while no
// recorder could write its NULL (the leader of range 1 changed between
// the table's drop and the next tick): that one runs on for at most a
// keyframe period.
//
// A plain SQL query of datax_metrics sees the sparse rows as written;
// docs/user/operations.md says how to read them.

// metricsKeyframeTicks is how many ticks a sparse series may go unwritten.
const metricsKeyframeTicks = 60

// metricsTickSeries is the marker series. It is not in metricSeriesDefs:
// nothing charts it.
const metricsTickSeries = "recorder.tick"

// sparseSeries reports whether a recorded name is written only on change.
func sparseSeries(name string) bool {
	base, _ := splitSeriesName(name)
	d, ok := metricSeriesByName[base]
	return ok && (d.Label == "table" || d.Label == "index")
}

type metricKey struct {
	node int64
	name string
}

type metricWritten struct {
	v     float64
	at    int64
	table uint64
}

// metricsSparseState is what the recorder last wrote of each sparse
// series it records. Only the recorder's tick touches it; the mutex is
// for a test driving ticks beside a running loop.
type metricsSparseState struct {
	mu   sync.Mutex
	last map[metricKey]metricWritten
}

// reset forgets everything: the next tick writes every series.
func (s *metricsSparseState) reset() {
	s.mu.Lock()
	s.last = nil
	s.mu.Unlock()
}

// keyframeDue reports whether a series unchanged since prevAt is written
// at tick at anyway.
func keyframeDue(name string, at, prevAt int64, interval time.Duration) bool {
	iv := interval.Nanoseconds()
	if at-prevAt >= metricsKeyframeTicks*iv {
		return true
	}
	h := fnv.New32a()
	_, _ = h.Write([]byte(name))
	return (uint64(at/iv)+uint64(h.Sum32()))%metricsKeyframeTicks == 0
}

// thin returns the rows of a tick to write: every dense row, the sparse
// rows that changed or whose keyframe is due, a NULL row for each sparse
// series written before and not sampled now — unless its table is one of
// unknown, the tables this tick could not name, whose series are neither
// written nor ended — and the markers. commit
// records what was written; the caller calls it once the write
// succeeded, and a failed write leaves the state as it was, so the next
// tick writes the same changes again.
func (s *metricsSparseState) thin(rows []metricRow, unknown map[uint64]bool, at int64, interval time.Duration, self int64, leader bool) ([]metricRow, func()) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.last == nil {
		s.last = map[metricKey]metricWritten{}
	}
	if !leader {
		// Another node writes the cluster series now; what this one
		// wrote is no guide to what is in the table when it leads again.
		for k := range s.last {
			if k.node == 0 {
				delete(s.last, k)
			}
		}
	}
	out := make([]metricRow, 0, len(rows))
	seen := make(map[metricKey]bool)
	written := map[metricKey]metricWritten{}
	for _, r := range rows {
		if !sparseSeries(r.name) {
			out = append(out, r)
			continue
		}
		k := metricKey{r.node, r.name}
		if seen[k] {
			continue // two tables of one name in different databases: the first
		}
		seen[k] = true
		prev, ok := s.last[k]
		if ok && prev.v == r.value && !keyframeDue(r.name, at, prev.at, interval) {
			continue
		}
		out = append(out, r)
		written[k] = metricWritten{v: r.value, at: at, table: r.table}
	}
	var ended []metricKey
	for k, w := range s.last {
		if (k.node == self || (leader && k.node == 0)) && !seen[k] && !(w.table != 0 && unknown[w.table]) {
			ended = append(ended, k)
		}
	}
	sort.Slice(ended, func(i, j int) bool {
		if ended[i].node != ended[j].node {
			return ended[i].node < ended[j].node
		}
		return ended[i].name < ended[j].name
	})
	for _, k := range ended {
		out = append(out, metricRow{node: k.node, name: k.name, null: true})
	}
	out = append(out, metricRow{node: self, name: metricsTickSeries, value: 1})
	if leader {
		out = append(out, metricRow{node: 0, name: metricsTickSeries, value: 1})
	}
	return out, func() {
		s.mu.Lock()
		defer s.mu.Unlock()
		if s.last == nil {
			s.last = map[metricKey]metricWritten{}
		}
		for k, w := range written {
			s.last[k] = w
		}
		for _, k := range ended {
			delete(s.last, k)
		}
	}
}

// sparseRow is one row of a sparse series as read back: null is the row
// that ends the series.
type sparseRow struct {
	t    int64
	v    float64
	null bool
}

// fillSparse turns the rows of a sparse series (in time order, from
// before `from` by up to horizon) and the marker ticks of the node that
// records it (in time order, within [from, to)) into the samples a dense
// recording would have held in [from, to): every row with a value at or
// after from, and at each marker tick without a row the last row's
// value, if it has one and is no older than horizon.
func fillSparse(rows []sparseRow, ticks []int64, from, horizon int64) []tsample {
	out := make([]tsample, 0, len(ticks)+len(rows))
	i := 0
	var last *sparseRow
	for _, m := range ticks {
		for i < len(rows) && rows[i].t < m {
			if r := rows[i]; !r.null && r.t >= from {
				out = append(out, tsample{t: r.t, v: r.v})
			}
			last = &rows[i]
			i++
		}
		if i < len(rows) && rows[i].t == m {
			continue // a row of its own: appended by the loop above next time round
		}
		if last != nil && !last.null && m-last.t <= horizon {
			out = append(out, tsample{t: m, v: last.v})
		}
	}
	for ; i < len(rows); i++ {
		if r := rows[i]; !r.null && r.t >= from {
			out = append(out, tsample{t: r.t, v: r.v})
		}
	}
	return out
}
