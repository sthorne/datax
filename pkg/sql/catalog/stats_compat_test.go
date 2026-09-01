package catalog

import (
	"encoding/json"
	"testing"
)

// Golden compatibility test: the frozen JSON encoding of TableStatistics.
// Stats blobs persist at keys.TableStatsKey and are read by every future
// binary, so per pkg/version rule 5 the shape may only grow additively
// (omitempty, safe zero values) and this decode must work forever.
const goldenTableStatistics = `{"table_id":42,"row_count":12345,"collected_at":1693526400000000000,"columns":[{"id":1,"name":"id","distinct":12345},{"id":2,"name":"city","distinct":50,"nulls":7},{"id":3}]}`

func TestGoldenTableStatistics(t *testing.T) {
	var st TableStatistics
	if err := json.Unmarshal([]byte(goldenTableStatistics), &st); err != nil {
		t.Fatal(err)
	}
	if st.TableID != 42 || st.RowCount != 12345 || st.CollectedAt != 1693526400000000000 ||
		len(st.Columns) != 3 || st.Columns[0].Distinct != 12345 ||
		st.Columns[1].Name != "city" || st.Columns[1].Nulls != 7 ||
		st.Columns[2].Distinct != 0 {
		t.Fatalf("decoded %+v", st)
	}
	if c, ok := st.Column(2); !ok || c.Distinct != 50 {
		t.Fatalf("Column(2): %+v, %v", c, ok)
	}
	if _, ok := st.Column(9); ok {
		t.Fatal("Column(9) found")
	}
	raw, err := json.Marshal(st)
	if err != nil {
		t.Fatal(err)
	}
	if string(raw) != goldenTableStatistics {
		t.Fatalf("encoding changed:\n got %s\nwant %s", raw, goldenTableStatistics)
	}
	// A minimal (columns-free) blob decodes with safe zeros.
	var min TableStatistics
	if err := json.Unmarshal([]byte(`{"table_id":7,"row_count":1,"collected_at":2}`), &min); err != nil || len(min.Columns) != 0 {
		t.Fatalf("minimal: %+v, %v", min, err)
	}
}
