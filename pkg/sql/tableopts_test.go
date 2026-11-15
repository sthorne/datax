package sql

import (
	"strings"
	"testing"

	"github.com/sthorne/datax/pkg/sql/catalog"
	"github.com/sthorne/datax/pkg/sql/types"
)

func optsDesc() *catalog.TableDescriptor {
	return &catalog.TableDescriptor{
		ID: 9, Name: "q",
		Columns: []catalog.Column{
			{ID: 1, Name: "id", Type: types.Int, NotNull: true},
			{ID: 2, Name: "created_at", Type: types.Timestamp},
			{ID: 3, Name: "expires_at", Type: types.Timestamp},
			{ID: 4, Name: "body", Type: types.String},
		},
		PrimaryKey: []catalog.ColumnID{1},
	}
}

// TestApplyGeneralOptions: what the gc_ttl and row expiry options accept,
// what they write, and what they refuse — the same function serves CREATE
// TABLE and ALTER TABLE ... SET, which differ only in whether half of a
// ttl_column/ttl pair may stand alone.
func TestApplyGeneralOptions(t *testing.T) {
	d := optsDesc()
	changed, err := applyGeneralOptions(d, map[string]string{"gc_ttl": "10m", "ttl_expiration": "expires_at"}, false)
	if err != nil || !changed || d.GCTTLSeconds != 600 || d.RowTTL == nil || d.RowTTL.ColumnID != 3 || d.RowTTL.AfterSeconds != 0 {
		t.Fatalf("gc_ttl + ttl_expiration: %v %v %+v %+v", changed, err, d, d.RowTTL)
	}
	addTTLIndex(d)
	if len(d.Indexes) != 1 || d.Indexes[0].Name != "q_ttl_idx" || d.Indexes[0].ColumnIDs[0] != 3 || d.Indexes[0].ID != 2 || d.NextIndexID != 3 {
		t.Fatalf("expiry index: %+v", d.Indexes)
	}
	addTTLIndex(d) // already there: nothing added
	if len(d.Indexes) != 1 {
		t.Fatalf("a second expiry index was added: %+v", d.Indexes)
	}

	d = optsDesc()
	if _, err := applyGeneralOptions(d, map[string]string{"ttl_column": "created_at", "ttl": "7 days"}, false); err != nil ||
		d.RowTTL.ColumnID != 2 || d.RowTTL.AfterSeconds != 7*24*3600 {
		t.Fatalf("ttl_column + ttl: %v %+v", err, d.RowTTL)
	}
	// On an existing pair, either half alone changes that half.
	if changed, err := applyGeneralOptions(d, map[string]string{"ttl": "1h"}, true); err != nil || changed ||
		d.RowTTL.ColumnID != 2 || d.RowTTL.AfterSeconds != 3600 {
		t.Fatalf("ttl alone on a pair: %v %v %+v", changed, err, d.RowTTL)
	}
	if changed, err := applyGeneralOptions(d, map[string]string{"ttl_column": "expires_at"}, true); err != nil || !changed ||
		d.RowTTL.ColumnID != 3 || d.RowTTL.AfterSeconds != 3600 {
		t.Fatalf("ttl_column alone on a pair: %v %v %+v", changed, err, d.RowTTL)
	}

	for name, c := range map[string]struct {
		opts  map[string]string
		alter bool
		want  string
	}{
		"ttl without a column":         {map[string]string{"ttl": "7 days"}, false, "ttl_column"},
		"ttl_column without ttl":       {map[string]string{"ttl_column": "created_at"}, false, "needs ttl"},
		"both spellings":               {map[string]string{"ttl_expiration": "expires_at", "ttl": "1h"}, false, "cannot be combined"},
		"a column that is not a time":  {map[string]string{"ttl_expiration": "body"}, false, "TIMESTAMP"},
		"a column that does not exist": {map[string]string{"ttl_expiration": "nope"}, false, "does not exist"},
		"a window of nothing":          {map[string]string{"gc_ttl": "0s"}, true, "gc_ttl"},
		"a window that is not a time":  {map[string]string{"gc_ttl": "soon"}, true, "gc_ttl"},
	} {
		if _, err := applyGeneralOptions(optsDesc(), c.opts, c.alter); err == nil || !strings.Contains(err.Error(), c.want) {
			t.Errorf("%s: %v, want an error naming %q", name, err, c.want)
		}
	}

	// A time-series table has its own expiry, and a retention is its
	// GC window.
	ts := optsDesc()
	ts.Timeseries, ts.RetentionSeconds = true, 3600
	if _, err := applyGeneralOptions(ts, map[string]string{"ttl_expiration": "expires_at"}, true); err == nil {
		t.Error("a row TTL on a time-series table was accepted")
	}
	if _, err := applyGeneralOptions(ts, map[string]string{"gc_ttl": "1h"}, true); err == nil {
		t.Error("a gc_ttl beside a retention was accepted")
	}
}
