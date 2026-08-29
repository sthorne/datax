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
		ID:    "cmd-1",
		Batch: ba,
		Split: &splitTrigger{Left: desc(4), Right: desc(5)},
		Merge: &mergeTrigger{
			Left: desc(6), Right: desc(7), Merged: desc(8),
			RightAppliedIndex: 99, RightSizeBytes: 1234,
			RightGCThreshold: hlc.Timestamp{WallTime: 7},
		},
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
