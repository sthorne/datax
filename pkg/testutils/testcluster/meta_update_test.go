package testcluster

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/kvpb"
	"github.com/sthorne/datax/pkg/util/hlc"
)

// TestMetaUpdateOrdering: range-addressing repairs are ordered by
// descriptor generation. A split's and a merge's post-commit repairs of
// the same /meta key can land in either order; a stale repair (an older
// generation, or a delete aimed at a record that has since moved on) is
// refused, so routing keeps working. Blind puts and deletes used to let
// the stale one win and leave a record naming a range that no longer
// exists, which every lookup then followed into "no replica" until it
// gave up (issue #74).
func TestMetaUpdateOrdering(t *testing.T) {
	tc := Start(t, 3)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	db := tc.Nodes[0].DB()
	prefix := keys.TableDataPrefix(858)
	if err := db.Put(ctx, append(prefix.Clone(), "a"...), []byte("v")); err != nil {
		t.Fatal(err)
	}
	sr, err := db.AdminSplit(ctx, append(prefix.Clone(), "m"...))
	if err != nil {
		t.Fatal(err)
	}
	// The split's own (ordered) repair landed: both records present.
	readMeta := func(k keys.Key) *kvpb.RangeDescriptor {
		t.Helper()
		raw, err := db.Get(ctx, keys.RangeMetaKey(k))
		if err != nil {
			t.Fatal(err)
		}
		if raw == nil {
			return nil
		}
		var d kvpb.RangeDescriptor
		if err := json.Unmarshal(raw, &d); err != nil {
			t.Fatal(err)
		}
		return &d
	}
	if d := readMeta(sr.Left.EndKey); d == nil || d.RangeID != sr.Left.RangeID {
		t.Fatalf("left record after split: %v", d)
	}

	send := func(req *kvpb.UpdateMetaRequest) bool {
		t.Helper()
		ba := &kvpb.BatchRequest{Header: kvpb.BatchHeader{Timestamp: hlc.Timestamp{WallTime: time.Now().UnixNano()}}}
		ba.Add(req)
		br, kerr := db.Send(ctx, ba)
		if kerr != nil {
			t.Fatalf("update-meta: %v", kerr)
		}
		return br.Responses[0].UpdateMeta.Applied
	}
	// A stale repair: the pre-split parent (older generation) written at
	// the RHS's key. Refused; the RHS record stands.
	stale := sr.Right
	stale.RangeID = sr.Left.RangeID
	stale.Generation = sr.Left.Generation - 1
	stale.StartKey = sr.Left.StartKey.Clone()
	if send(&kvpb.UpdateMetaRequest{RequestHeader: kvpb.RequestHeader{Key: keys.RangeMetaKey(sr.Right.EndKey)}, Desc: &stale}) {
		t.Fatal("stale (older generation) addressing record was written")
	}
	if d := readMeta(sr.Right.EndKey); d == nil || d.RangeID != sr.Right.RangeID {
		t.Fatalf("right record after stale put: %v", d)
	}
	// A stale delete: aimed at the LHS record at an older generation than
	// the one there. Refused.
	if send(&kvpb.UpdateMetaRequest{RequestHeader: kvpb.RequestHeader{Key: keys.RangeMetaKey(sr.Left.EndKey)}, IfRangeID: sr.Left.RangeID, IfGeneration: sr.Left.Generation - 1}) {
		t.Fatal("stale delete removed a newer addressing record")
	}
	if d := readMeta(sr.Left.EndKey); d == nil {
		t.Fatal("left record deleted by a stale delete")
	}
	// An idempotent repeat of the current record applies; so does a newer
	// generation. Routing still resolves both halves throughout.
	if !send(&kvpb.UpdateMetaRequest{RequestHeader: kvpb.RequestHeader{Key: keys.RangeMetaKey(sr.Right.EndKey)}, Desc: &sr.Right}) {
		t.Fatal("idempotent repeat of the current record refused")
	}
	for _, k := range []keys.Key{append(prefix.Clone(), "a"...), append(prefix.Clone(), "z"...)} {
		if err := db.Put(ctx, k, []byte("w")); err != nil {
			t.Fatalf("write through addressing after stale repairs: %v", err)
		}
	}
	// And the merge's ordered repair: after merging, the LHS record is
	// gone and the merged record names the LHS range at a higher
	// generation; a re-send of the split's repair (now stale) is refused.
	mr, err := db.AdminMerge(ctx, sr.Left.StartKey)
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(20 * time.Second)
	for readMeta(sr.Left.EndKey) != nil {
		if time.Now().After(deadline) {
			t.Fatal("old LHS record not deleted by the merge's repair")
		}
		time.Sleep(50 * time.Millisecond)
	}
	if send(&kvpb.UpdateMetaRequest{RequestHeader: kvpb.RequestHeader{Key: keys.RangeMetaKey(sr.Right.EndKey)}, Desc: &sr.Right}) {
		t.Fatal("the split's stale repair overwrote the merged record")
	}
	if d := readMeta(mr.Desc.EndKey); d == nil || d.RangeID != mr.Desc.RangeID || d.Generation != mr.Desc.Generation {
		t.Fatalf("merged record: %v, want %v", d, mr.Desc)
	}
}
