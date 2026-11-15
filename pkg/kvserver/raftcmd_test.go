package kvserver

import (
	"encoding/json"
	"reflect"
	"testing"

	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/kvpb"
	"github.com/sthorne/datax/pkg/util/hlc"
)

func testRaftCmd() *raftCommand {
	ba := kvpb.BatchRequest{Header: kvpb.BatchHeader{Timestamp: hlc.Timestamp{WallTime: 42}, RangeID: 3}}
	ba.Add(&kvpb.PutRequest{RequestHeader: kvpb.RequestHeader{Key: keys.Key("k")}, Value: []byte("v")})
	desc := func(id int64) kvpb.RangeDescriptor {
		return kvpb.RangeDescriptor{
			RangeID:  3,
			StartKey: keys.Key("a"), EndKey: keys.Key("z"),
			Replicas:      []kvpb.ReplicaDescriptor{{NodeID: 1, StoreID: 1, ReplicaID: 1}},
			NextReplicaID: 2, Generation: id,
		}
	}
	return &raftCommand{
		ID:       "cmd-1",
		Batch:    ba,
		ClosedTS: hlc.Timestamp{WallTime: 40},
		Split:    &splitTrigger{Left: desc(4), Right: desc(5), ClosedTS: hlc.Timestamp{WallTime: 39}},
		Merge: &mergeTrigger{
			Left: desc(6), Right: desc(7), Merged: desc(8),
			RightAppliedIndex: 99, RightSizeBytes: 1234,
			RightGCThreshold: hlc.Timestamp{WallTime: 7},
		},
	}
}

// TestRaftCommandCarriesTheStickyBit: the sticky bit rides the log on
// both of its paths — inside a split trigger's right-hand descriptor and
// as a descriptor update of its own. A replica that decoded either
// without it would hold a descriptor its peers do not.
func TestRaftCommandCarriesTheStickyBit(t *testing.T) {
	cmd := testRaftCmd()
	cmd.Split.Right.StickyUntil = kvpb.StickyForever
	sticky := cmd.Split.Left
	sticky.Generation++
	sticky.StickyUntil = 1725000000000000000
	cmd.Descriptor = &descriptorTrigger{Desc: sticky}
	data, err := encodeRaftCommand(cmd)
	if err != nil {
		t.Fatal(err)
	}
	got, err := decodeRaftCommand(data)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(cmd, got) {
		t.Fatalf("round trip mismatch:\n got %+v\nwant %+v", got, cmd)
	}
	if got.Split.Left.StickyUntil != 0 || got.Split.Right.StickyUntil != kvpb.StickyForever || got.Descriptor.Desc.StickyUntil != sticky.StickyUntil {
		t.Fatalf("sticky bits: split %d / %d, update %d", got.Split.Left.StickyUntil, got.Split.Right.StickyUntil, got.Descriptor.Desc.StickyUntil)
	}
}

// TestStaleDescriptorChangeIsTyped: the refusal a split or a sticky-bit
// update gets when the range moved under it carries the typed detail its
// proposer retries on (issue #290) — the message is for people.
func TestStaleDescriptorChangeIsTyped(t *testing.T) {
	r := &Replica{rangeID: 4}
	e := r.descriptorChangedError("split", 0, 1)
	if e.DescriptorChanged == nil || *e.DescriptorChanged != (kvpb.DescriptorChangedError{RangeID: 4, BuiltFrom: 0, Current: 1}) {
		t.Fatalf("detail: %+v", e.DescriptorChanged)
	}
	if want := "r4: split against generation 0 but range is at 1; the range changed under it, retry against the current descriptor"; e.Error() != want {
		t.Fatalf("message %q, want %q", e.Error(), want)
	}
	// It survives the trip from the apply path to the proposer, where an
	// error is re-wrapped, and the trip to a remote caller.
	if w := kvpb.NewError(error(e)); w.DescriptorChanged == nil {
		t.Fatal("detail lost re-wrapping the error")
	}
	back, err := kvpb.ErrorFromProto(kvpb.ErrorToProto(e))
	if err != nil || back.DescriptorChanged == nil || back.DescriptorChanged.Current != 1 {
		t.Fatalf("detail lost on the wire: %+v (%v)", back, err)
	}
	f := r.frozenError(keys.Key("k"), "%s: cannot split: range is frozen for a merge", r.rangeID)
	if f.RangeKeyMismatch == nil || !f.RangeKeyMismatch.Frozen {
		t.Fatalf("frozen refusal is not typed: %+v", f)
	}
	if back, err = kvpb.ErrorFromProto(kvpb.ErrorToProto(f)); err != nil || back.RangeKeyMismatch == nil || !back.RangeKeyMismatch.Frozen {
		t.Fatalf("frozen mark lost on the wire: %+v (%v)", back, err)
	}
}

func TestRaftCommandRoundTrip(t *testing.T) {
	cmd := testRaftCmd()
	data, err := encodeRaftCommand(cmd)
	if err != nil {
		t.Fatal(err)
	}
	if data[0] != raftCommandVersionProto {
		t.Fatalf("missing version byte: 0x%02x", data[0])
	}
	got, err := decodeRaftCommand(data)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(cmd, got) {
		t.Fatalf("round trip mismatch:\n got %+v\nwant %+v", got, cmd)
	}
}

// TestRaftCommandDecodesLegacyJSON: the raft log is persistent, so entries
// written by the pre-proto format (bare JSON) must decode forever.
func TestRaftCommandDecodesLegacyJSON(t *testing.T) {
	cmd := testRaftCmd()
	data, err := json.Marshal(cmd)
	if err != nil {
		t.Fatal(err)
	}
	got, err := decodeRaftCommand(data)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(cmd, got) {
		t.Fatalf("legacy decode mismatch:\n got %+v\nwant %+v", got, cmd)
	}
}

func TestRaftCommandRejectsUnknownVersion(t *testing.T) {
	if _, err := decodeRaftCommand([]byte{0x7f, 1, 2}); err == nil {
		t.Fatal("unknown format byte accepted")
	}
	if _, err := decodeRaftCommand(nil); err == nil {
		t.Fatal("empty command accepted")
	}
}
