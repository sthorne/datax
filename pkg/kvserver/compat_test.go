package kvserver

import (
	"encoding/hex"
	"encoding/json"
	"testing"

	"github.com/sthorne/datax/pkg/util/hlc"
)

// Golden compatibility tests: frozen encodings of kvserver's persisted
// state. Per the rules in pkg/version, old encodings must decode forever
// (the raft log and applied state survive binary upgrades) and the current
// shapes may only grow additively. A failure here means a change that
// breaks rolling upgrades or crash recovery across an upgrade.

const (
	goldenReplicaState = `{"applied_index":7,"truncated_index":3,"truncated_term":2,"gc_threshold":{"wall":50,"logical":0},"size_bytes":1234,"frozen":true,"merged_into":9,"closed_ts":{"wall":60,"logical":1}}`

	// A v1-era applied state: only the original field.
	goldenReplicaStateV1 = `{"applied_index":7}`

	goldenSnapshotHeader = `{"desc":{"range_id":5,"start_key":"YQ==","end_key":"cQ==","replicas":[{"node_id":1,"store_id":1,"replica_id":1},{"node_id":2,"store_id":2,"replica_id":2}],"next_replica_id":3,"generation":12},"replica_id":2,"applied_index":7,"term":3,"gc_threshold":{"wall":50,"logical":0},"size_bytes":88}`

	// goldenRaftCommandProto is an encodeRaftCommand output frozen at the
	// current proto schema: format byte 0x01, then a RaftCommand carrying a
	// one-Put batch, a closed timestamp, a load handoff, a checksum
	// trigger, and a split trigger. Old log entries must decode forever.
	goldenRaftCommandProto = "010a05636d642d3112160a080a04086410011805120a12080a030a01611201761a200a0c08051201611a016d2803300d120c080612016d1a0171280330011a0208282a020832320b09000000000000044010633a040a02636b"
)

func TestGoldenReplicaState(t *testing.T) {
	var st replicaState
	if err := json.Unmarshal([]byte(goldenReplicaState), &st); err != nil {
		t.Fatal(err)
	}
	if st.AppliedIndex != 7 || st.TruncatedIndex != 3 || st.TruncatedTerm != 2 ||
		st.GCThreshold.WallTime != 50 || st.SizeBytes != 1234 || !st.Frozen ||
		st.MergedInto != 9 || st.ClosedTS != (hlc.Timestamp{WallTime: 60, Logical: 1}) {
		t.Fatalf("decoded %+v", st)
	}
	raw, err := json.Marshal(st)
	if err != nil {
		t.Fatal(err)
	}
	if string(raw) != goldenReplicaState {
		t.Fatalf("encoding changed:\n got %s\nwant %s", raw, goldenReplicaState)
	}

	var old replicaState
	if err := json.Unmarshal([]byte(goldenReplicaStateV1), &old); err != nil {
		t.Fatal(err)
	}
	if old.AppliedIndex != 7 || old.Frozen || !old.ClosedTS.IsEmpty() {
		t.Fatalf("v1 state decoded %+v", old)
	}
}

func TestGoldenSnapshotHeader(t *testing.T) {
	var hdr snapshotHeader
	if err := json.Unmarshal([]byte(goldenSnapshotHeader), &hdr); err != nil {
		t.Fatal(err)
	}
	if hdr.Desc.RangeID != 5 || len(hdr.Desc.Replicas) != 2 || hdr.ReplicaID != 2 ||
		hdr.AppliedIndex != 7 || hdr.Term != 3 || hdr.GCThreshold.WallTime != 50 ||
		hdr.SizeBytes != 88 {
		t.Fatalf("decoded %+v", hdr)
	}
	raw, err := json.Marshal(hdr)
	if err != nil {
		t.Fatal(err)
	}
	if string(raw) != goldenSnapshotHeader {
		t.Fatalf("encoding changed:\n got %s\nwant %s", raw, goldenSnapshotHeader)
	}
}

func TestRaftCommandDecodesGoldenProto(t *testing.T) {
	data, err := hex.DecodeString(goldenRaftCommandProto)
	if err != nil {
		t.Fatal(err)
	}
	cmd, err := decodeRaftCommand(data)
	if err != nil {
		t.Fatalf("golden proto raft command no longer decodes: %v", err)
	}
	if cmd.ID != "cmd-1" {
		t.Fatalf("ID: %q", cmd.ID)
	}
	if cmd.Batch.Header.Timestamp != (hlc.Timestamp{WallTime: 100, Logical: 1}) ||
		cmd.Batch.Header.RangeID != 5 {
		t.Fatalf("batch header: %+v", cmd.Batch.Header)
	}
	if len(cmd.Batch.Requests) != 1 || cmd.Batch.Requests[0].Put == nil ||
		string(cmd.Batch.Requests[0].Put.Key) != "a" ||
		string(cmd.Batch.Requests[0].Put.Value) != "v" {
		t.Fatalf("batch requests: %+v", cmd.Batch.Requests)
	}
	if cmd.ClosedTS.WallTime != 50 {
		t.Fatalf("closed ts: %+v", cmd.ClosedTS)
	}
	if cmd.Load == nil || cmd.Load.QPS != 2.5 || cmd.Load.AtNanos != 99 {
		t.Fatalf("load: %+v", cmd.Load)
	}
	if cmd.Checksum == nil || cmd.Checksum.ID != "ck" {
		t.Fatalf("checksum: %+v", cmd.Checksum)
	}
	if cmd.Split == nil || cmd.Split.Left.RangeID != 5 || cmd.Split.Right.RangeID != 6 ||
		string(cmd.Split.Left.EndKey) != "m" || cmd.Split.ClosedTS.WallTime != 40 {
		t.Fatalf("split: %+v", cmd.Split)
	}
}
