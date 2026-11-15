package server

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/sthorne/datax/pkg/kvpb"
	"github.com/sthorne/datax/pkg/sql/catalog"
	"github.com/sthorne/datax/pkg/util/hlc"
)

// TestCommitTimeExpiryBands (issue #316): a row whose commit time is at
// or below the chain's end less the retention is left out, whatever its
// timestamp column says, and the rest fall into bands of commit time in
// order, the newest in the last.
func TestCommitTimeExpiryBands(t *testing.T) {
	r := 16 * time.Second
	asOf := hlc.Timestamp{WallTime: 100 * time.Second.Nanoseconds()}
	e := &commitTimeExpiry{horizon: asOf.AddNanos(-r.Nanoseconds()), width: r.Nanoseconds() / restoreExpiryBands, bands: true}
	at := func(sec float64) kvpb.ExportRecord {
		return kvpb.ExportRecord{Timestamp: hlc.Timestamp{WallTime: int64(sec * float64(time.Second))}}
	}
	if e.keep(at(84)) || e.keep(at(10)) {
		t.Fatal("a row committed at or before the horizon was kept")
	}
	if !e.keep(kvpb.ExportRecord{Timestamp: hlc.Timestamp{WallTime: e.horizon.WallTime, Logical: 1}}) || !e.keep(at(100)) {
		t.Fatal("a row committed after the horizon was left out")
	}
	prev := -1
	for sec := 84.5; sec <= 100; sec += 0.5 {
		b := e.band(at(sec).Timestamp)
		if b < prev || b < 0 || b >= restoreExpiryBands {
			t.Fatalf("commit time %.1fs: band %d after %d", sec, b, prev)
		}
		prev = b
	}
	if prev != restoreExpiryBands-1 {
		t.Fatalf("the chain's end falls in band %d", prev)
	}
}

// TestDescriptorWithRestoredRows: a restore of everything writes a
// descriptor byte for byte unless it carries bands, the source's going
// and this restore's coming in their place.
func TestDescriptorWithRestoredRows(t *testing.T) {
	plain := json.RawMessage(`{"id":60,"name":"m","timeseries":true}`)
	got, err := descriptorWithRestoredRows(plain, nil)
	if err != nil || string(got) != string(plain) {
		t.Fatalf("a descriptor without bands was rewritten: %s, %v", got, err)
	}
	withSource := json.RawMessage(`{"id":60,"name":"m","restored_rows":[{"through":{"wall":5,"logical":0},"written_by":3}]}`)
	got, err = descriptorWithRestoredRows(withSource, nil)
	if err != nil || strings.Contains(string(got), "restored_rows") {
		t.Fatalf("the source's bands were kept: %s, %v", got, err)
	}
	bands := []catalog.RestoredRowsBand{{Through: hlc.Timestamp{WallTime: 9}, WrittenBy: 7}}
	got, err = descriptorWithRestoredRows(withSource, bands)
	if err != nil {
		t.Fatal(err)
	}
	var d catalog.TableDescriptor
	if err := json.Unmarshal(got, &d); err != nil || d.ID != 60 || d.Name != "m" || len(d.RestoredRows) != 1 || d.RestoredRows[0] != bands[0] {
		t.Fatalf("bands written as %s (%+v, %v)", got, d, err)
	}
	if catalog.RestoredWrittenAt(d.RestoredRows, hlc.Timestamp{WallTime: 9}) != 7 || catalog.RestoredWrittenAt(d.RestoredRows, hlc.Timestamp{WallTime: 10}) != 10 {
		t.Fatal("a version was aged by the wrong band")
	}
}
