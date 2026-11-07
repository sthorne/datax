package server

import (
	"testing"

	"github.com/sthorne/datax/pkg/pgwire"
)

// TestShapeSeriesRowsAreFourCountersPerShape (issue #209): a tick's rows
// are the four cumulative counters of each shape keyed by its
// fingerprint, in the order the activity table gave them; a stat without
// a fingerprint (nothing to key a series by) is skipped rather than
// written under an empty label.
func TestShapeSeriesRowsAreFourCountersPerShape(t *testing.T) {
	rows := shapeSeriesRows(3, []pgwire.StatementStat{
		{Fingerprint: "abc", Count: 7, TotalMicros: 900, RowsScanned: 42, Retries: 1},
		{Fingerprint: ""},
		{Fingerprint: "def", Count: 1},
	})
	want := []metricRow{
		{node: 3, name: "stmt.count{fp=abc}", value: 7},
		{node: 3, name: "stmt.total_us{fp=abc}", value: 900},
		{node: 3, name: "stmt.rows_scanned{fp=abc}", value: 42},
		{node: 3, name: "stmt.retries{fp=abc}", value: 1},
		{node: 3, name: "stmt.count{fp=def}", value: 1},
		{node: 3, name: "stmt.total_us{fp=def}", value: 0},
		{node: 3, name: "stmt.rows_scanned{fp=def}", value: 0},
		{node: 3, name: "stmt.retries{fp=def}", value: 0},
	}
	if len(rows) != len(want) {
		t.Fatalf("rows: %+v", rows)
	}
	for i := range want {
		if rows[i] != want[i] {
			t.Errorf("row %d: got %+v want %+v", i, rows[i], want[i])
		}
	}
	for _, s := range []string{"stmt.count", "stmt.total_us", "stmt.rows_scanned", "stmt.retries"} {
		def, ok := metricSeriesByName[s]
		if !ok || def.Label != "fp" || def.Kind != SeriesCounter {
			t.Errorf("%s: not a counter labelled by fp: %+v", s, def)
		}
		if seriesTable(s) != "datax_metrics_shapes" {
			t.Errorf("%s: served from %s", s, seriesTable(s))
		}
	}
	if seriesTable("sql.statements") != "datax_metrics" {
		t.Errorf("sql.statements: served from %s", seriesTable("sql.statements"))
	}
}
