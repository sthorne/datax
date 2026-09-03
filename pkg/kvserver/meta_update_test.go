package kvserver

import (
	"testing"

	"github.com/sthorne/datax/pkg/kvpb"
)

// TestMetaUpdateApplies: the ordering rule for range-addressing repairs.
// A record is replaced only by a newer generation (or an idempotent
// repeat of itself), so a split's or merge's repair landing after a later
// operation's cannot resurrect a range that no longer exists; a delete
// applies only while the record still names the range it meant to remove,
// at that generation or older. Issue #74.
func TestMetaUpdateApplies(t *testing.T) {
	r6 := &kvpb.RangeDescriptor{RangeID: 6, Generation: 7}
	r6newer := &kvpb.RangeDescriptor{RangeID: 6, Generation: 9}
	r5merged := &kvpb.RangeDescriptor{RangeID: 5, Generation: 8}
	put := func(d *kvpb.RangeDescriptor) *kvpb.UpdateMetaRequest { return &kvpb.UpdateMetaRequest{Desc: d} }
	del := func(d *kvpb.RangeDescriptor) *kvpb.UpdateMetaRequest {
		return &kvpb.UpdateMetaRequest{IfRangeID: d.RangeID, IfGeneration: d.Generation}
	}
	cases := []struct {
		name     string
		existing *kvpb.RangeDescriptor
		req      *kvpb.UpdateMetaRequest
		want     bool
	}{
		{"put onto empty", nil, put(r6), true},
		{"put newer generation over older", r6, put(r5merged), true},
		{"put older generation over newer (late split repair)", r5merged, put(r6), false},
		{"idempotent repeat", r6, put(r6), true},
		{"same generation, different range", &kvpb.RangeDescriptor{RangeID: 9, Generation: 7}, put(r6), false},
		{"delete the record it names", r6, del(r6), true},
		{"delete an older generation of the same range", &kvpb.RangeDescriptor{RangeID: 6, Generation: 5}, del(r6), true},
		{"delete refused: record moved on (later split at this key)", r6newer, del(r6), false},
		{"delete refused: different range", r5merged, del(r6), false},
		{"delete of nothing", nil, del(r6), false},
	}
	for _, c := range cases {
		if got := metaUpdateApplies(c.existing, c.req); got != c.want {
			t.Errorf("%s: got %v, want %v", c.name, got, c.want)
		}
	}
}
