package kvpb

import (
	"encoding/json"
	"testing"

	"github.com/google/uuid"
	"github.com/sthorne/datax/pkg/storage/enginepb"
)

// Golden compatibility tests: frozen JSON encodings of kvpb's persisted /
// wire types. Per the rules in pkg/version, old encodings must decode
// forever and the current shape must only ever grow additively. A failure
// here means a change that breaks rolling upgrades.

const (
	goldenNodeDescriptor = `{"node_id":4,"address":"10.0.0.4:26257","locality":{"tiers":[{"key":"region","value":"eu"},{"key":"rack","value":"2"}]},"liveness_time":1725000000000000000,"draining":true,"binary_version":2,"leader_qps":12.5,"leader_count":7,"replica_bytes":4096,"hot_ranges":[{"range_id":9,"qps":11.25}],"big_ranges":[{"range_id":8,"bytes":2048}]}`

	// A v1-era registry row: none of the post-v1 additive fields present.
	goldenNodeDescriptorV1 = `{"node_id":4,"address":"10.0.0.4:26257","locality":{},"liveness_time":1}`

	goldenRangeDescriptor = `{"range_id":5,"start_key":"YQ==","end_key":"cQ==","replicas":[{"node_id":1,"store_id":1,"replica_id":1},{"node_id":2,"store_id":2,"replica_id":2}],"next_replica_id":3,"generation":12}`

	goldenTransaction = `{"id":"aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee","key":"YW5jaG9y","epoch":1,"write_ts":{"wall":100,"logical":2},"min_ts":{"wall":90,"logical":0},"priority":3,"seq":4,"name":"t","status":3,"read_ts":{"wall":95,"logical":1},"last_heartbeat":{"wall":99,"logical":0},"intent_keys":["YW5jaG9y","b3RoZXI="],"waiting_for":"aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee","waiting_for_key":"dw==","in_flight_keys":["b3RoZXI="]}`
)

func TestGoldenNodeDescriptor(t *testing.T) {
	var nd NodeDescriptor
	if err := json.Unmarshal([]byte(goldenNodeDescriptor), &nd); err != nil {
		t.Fatal(err)
	}
	if nd.NodeID != 4 || nd.Address != "10.0.0.4:26257" || !nd.Draining ||
		nd.BinaryVersion != 2 || nd.LeaderQPS != 12.5 || nd.ReplicaBytes != 4096 {
		t.Fatalf("decoded %+v", nd)
	}
	raw, err := json.Marshal(nd)
	if err != nil {
		t.Fatal(err)
	}
	if string(raw) != goldenNodeDescriptor {
		t.Fatalf("encoding changed:\n got %s\nwant %s", raw, goldenNodeDescriptor)
	}

	var old NodeDescriptor
	if err := json.Unmarshal([]byte(goldenNodeDescriptorV1), &old); err != nil {
		t.Fatal(err)
	}
	if old.NodeID != 4 || old.BinaryVersion != 0 || old.Draining {
		t.Fatalf("v1 row decoded %+v", old)
	}
}

func TestGoldenRangeDescriptor(t *testing.T) {
	var d RangeDescriptor
	if err := json.Unmarshal([]byte(goldenRangeDescriptor), &d); err != nil {
		t.Fatal(err)
	}
	if d.RangeID != 5 || string(d.StartKey) != "a" || string(d.EndKey) != "q" ||
		len(d.Replicas) != 2 || d.NextReplicaID != 3 || d.Generation != 12 {
		t.Fatalf("decoded %+v", d)
	}
	raw, err := json.Marshal(d)
	if err != nil {
		t.Fatal(err)
	}
	if string(raw) != goldenRangeDescriptor {
		t.Fatalf("encoding changed:\n got %s\nwant %s", raw, goldenRangeDescriptor)
	}
}

func TestGoldenTransaction(t *testing.T) {
	var txn Transaction
	if err := json.Unmarshal([]byte(goldenTransaction), &txn); err != nil {
		t.Fatal(err)
	}
	if txn.ID != uuid.MustParse("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee") ||
		string(txn.Key) != "anchor" || txn.Epoch != 1 ||
		txn.Status != enginepb.STAGING ||
		txn.WriteTimestamp.WallTime != 100 || txn.WriteTimestamp.Logical != 2 ||
		txn.ReadTimestamp.WallTime != 95 ||
		len(txn.IntentKeys) != 2 || len(txn.InFlightKeys) != 1 ||
		txn.Sequence != 4 {
		t.Fatalf("decoded %+v", txn)
	}
	raw, err := json.Marshal(txn)
	if err != nil {
		t.Fatal(err)
	}
	if string(raw) != goldenTransaction {
		t.Fatalf("encoding changed:\n got %s\nwant %s", raw, goldenTransaction)
	}
}
