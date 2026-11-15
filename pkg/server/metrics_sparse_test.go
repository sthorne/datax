package server

import (
	"fmt"
	"reflect"
	"testing"
	"time"
)

// TestSparseSeriesReadAsDense: the rows the recorder writes for its
// sparse series (#316), filled in by the reader, give back the samples
// a recorder writing every series every tick would have written — for an
// idle series, a changing one, one that ends, one that starts late, a
// tick the recorder skipped, and a window that opens after a series'
// last row.
func TestSparseSeriesReadAsDense(t *testing.T) {
	const interval = 10 * time.Second
	iv := interval.Nanoseconds()
	const self = 3
	idle := labelledSeries("table.ranges", "table", "idle")
	busy := labelledSeries("index.scans", "index", "busy.primary")
	ends := labelledSeries("table.rows", "table", "dropped")
	late := labelledSeries("index.rows", "index", "late.primary")
	dense := "node.ranges"
	const ticks = 3*metricsKeyframeTicks + 7
	skipped := map[int]bool{70: true, 71: true, 150: true}
	sample := func(i int) []metricRow {
		rows := []metricRow{
			{node: self, name: dense, value: float64(i % 5)},
			{node: self, name: idle, value: 4},
			{node: self, name: busy, value: float64(i / 3)},
		}
		if i < 100 {
			rows = append(rows, metricRow{node: self, name: ends, value: 7})
		}
		if i >= 40 {
			rows = append(rows, metricRow{node: self, name: late, value: float64(i / 50)})
		}
		return rows
	}

	want := map[string][]tsample{}      // what a dense recorder writes
	written := map[string][]sparseRow{} // what the sparse one writes
	var marks []int64
	var st metricsSparseState
	base := int64(1_800_000_000) * int64(time.Second)
	base -= base % iv
	total := 0
	for i := 0; i < ticks; i++ {
		at := base + int64(i)*iv
		if skipped[i] {
			continue
		}
		rows := sample(i)
		for _, r := range rows {
			want[r.name] = append(want[r.name], tsample{t: at, v: r.value})
		}
		out, commit := st.thin(rows, nil, at, interval, self, false)
		commit()
		total += len(out)
		for _, r := range out {
			if r.node != self {
				t.Fatalf("a non-leader wrote a row for node %d", r.node)
			}
			if r.name == metricsTickSeries {
				marks = append(marks, at)
				continue
			}
			written[r.name] = append(written[r.name], sparseRow{t: at, v: r.value, null: r.null})
		}
	}
	if len(written[dense]) != ticks-len(skipped) {
		t.Fatalf("the dense series has %d rows, want one per tick (%d)", len(written[dense]), ticks-len(skipped))
	}
	if n := len(written[idle]); n > ticks/metricsKeyframeTicks+2 {
		t.Fatalf("the idle series has %d rows over %d ticks", n, ticks)
	}
	if last := written[ends][len(written[ends])-1]; !last.null || last.t != base+100*iv {
		t.Fatalf("the series that ended has no NULL row at its end: %+v", last)
	}
	horizon := (metricsKeyframeTicks + 1) * iv
	for _, from := range []int64{base, base + 37*iv, base + 101*iv, base + 155*iv} {
		to := base + ticks*iv
		var m []int64
		for _, x := range marks {
			if x >= from && x < to {
				m = append(m, x)
			}
		}
		for _, name := range []string{idle, busy, ends, late} {
			var rows []sparseRow
			for _, r := range written[name] {
				if r.t >= from-horizon && r.t < to {
					rows = append(rows, r)
				}
			}
			got := fillSparse(rows, m, from, horizon)
			var exp []tsample
			for _, p := range want[name] {
				if p.t >= from && p.t < to {
					exp = append(exp, p)
				}
			}
			if len(got) == 0 && len(exp) == 0 {
				continue
			}
			if !reflect.DeepEqual(got, exp) {
				t.Fatalf("%s from tick %d: filled\n%v\nwant\n%v", name, (from-base)/iv, brief(got, base, iv), brief(exp, base, iv))
			}
		}
	}
	t.Logf("%d ticks: %d rows written, %d for a dense recorder", ticks, total, func() int {
		n := 0
		for _, p := range want {
			n += len(p)
		}
		return n
	}())
}

func brief(pts []tsample, base, iv int64) string {
	s := ""
	for _, p := range pts {
		s += fmt.Sprintf("%d:%g ", (p.t-base)/iv, p.v)
	}
	return s
}

// TestSparseStateLeaderAndFailure: the cluster series are forgotten when
// the node stops leading range 1, so it writes them all when it leads
// again; a write that failed (commit not called) is written again.
func TestSparseStateLeaderAndFailure(t *testing.T) {
	const interval = 10 * time.Second
	iv := interval.Nanoseconds()
	var st metricsSparseState
	tab := labelledSeries("table.ranges", "table", "t")
	rows := []metricRow{{node: 0, name: tab, value: 1}}
	count := func(out []metricRow, name string) int {
		n := 0
		for _, r := range out {
			if r.name == name {
				n++
			}
		}
		return n
	}
	at := int64(1_800_000_000) * int64(time.Second)
	at -= at % iv
	// Pick a starting tick that is no series' keyframe for a while.
	for keyframeDue(tab, at, at, interval) || keyframeDue(tab, at+iv, at, interval) || keyframeDue(tab, at+2*iv, at, interval) || keyframeDue(tab, at+3*iv, at, interval) {
		at += iv
	}
	out, commit := st.thin(rows, nil, at, interval, 1, true)
	commit()
	if count(out, tab) != 1 || count(out, metricsTickSeries) != 2 {
		t.Fatalf("first tick: %+v", out)
	}
	out, _ = st.thin(rows, nil, at+iv, interval, 1, true) // a failed write: no commit
	if count(out, tab) != 0 {
		t.Fatalf("an unchanged series was written: %+v", out)
	}
	changed := []metricRow{{node: 0, name: tab, value: 2}}
	out, _ = st.thin(changed, nil, at+iv, interval, 1, true)
	if count(out, tab) != 1 {
		t.Fatalf("a changed series was not written: %+v", out)
	}
	out, _ = st.thin(changed, nil, at+2*iv, interval, 1, true) // its write failed: again
	if count(out, tab) != 1 {
		t.Fatalf("a change whose write failed was not written again: %+v", out)
	}
	out, commit = st.thin(nil, nil, at+2*iv, interval, 1, false)
	commit()
	if count(out, tab) != 0 || count(out, metricsTickSeries) != 1 {
		t.Fatalf("a non-leader wrote cluster rows: %+v", out)
	}
	out, _ = st.thin(rows, nil, at+3*iv, interval, 1, true)
	if count(out, tab) != 1 {
		t.Fatalf("leading again, the cluster series was not written: %+v", out)
	}
}

// TestSparseStateUnknownTable: an index series whose table the node could
// not name this tick (its descriptor cache let it go) is held, not
// ended; one whose table is gone is ended with a NULL row.
func TestSparseStateUnknownTable(t *testing.T) {
	const interval = 10 * time.Second
	iv := interval.Nanoseconds()
	var st metricsSparseState
	a := labelledSeries("index.scans", "index", "a.primary")
	b := labelledSeries("index.scans", "index", "b.primary")
	at := int64(1_800_000_000) * int64(time.Second)
	at -= at % iv
	out, commit := st.thin([]metricRow{{node: 1, name: a, value: 1, table: 10}, {node: 1, name: b, value: 1, table: 11}}, nil, at, interval, 1, false)
	commit()
	if len(out) != 3 {
		t.Fatalf("first tick: %+v", out)
	}
	out, commit = st.thin(nil, map[uint64]bool{10: true}, at+iv, interval, 1, false)
	commit()
	var nulls []string
	for _, r := range out {
		if r.null {
			nulls = append(nulls, r.name)
		}
	}
	if !reflect.DeepEqual(nulls, []string{b}) {
		t.Fatalf("ended %v, want only %s", nulls, b)
	}
}
