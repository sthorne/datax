package cluster

import (
	"encoding/json"
	"testing"

	"github.com/google/uuid"
	"github.com/sthorne/datax/pkg/base"
	"github.com/sthorne/datax/pkg/kvpb"
)

// Golden compatibility tests: these literals are the frozen JSON encodings
// of the cluster package's wire/persisted types. Per the rules in
// pkg/version, old encodings must decode forever and the current encoding
// must not change shape (new fields must be additive + omitempty). If one
// of these tests fails, the change breaks rolling upgrades — extend the
// type additively instead.

const (
	goldenStoreIdent = `{"cluster_id":"11111111-2222-3333-4444-555555555555","node_id":3,"store_id":3}`

	goldenJoinRequest = `{"address":"10.0.0.7:26257","locality":{"tiers":[{"key":"region","value":"eu"},{"key":"rack","value":"2"}]},"node_id":2,"cluster_id":"11111111-2222-3333-4444-555555555555","binary_version":2,"min_supported":1}`

	// A pre-versioning (v1-era) join request: absent version fields must
	// decode to zero and read as the [1, 1] window.
	goldenJoinRequestV1 = `{"address":"10.0.0.7:26257","locality":{}}`

	goldenJoinResponse = `{"cluster_id":"11111111-2222-3333-4444-555555555555","node_id":4,"nodes":[{"node_id":4,"address":"10.0.0.4:26257","locality":{"tiers":[{"key":"region","value":"eu"},{"key":"rack","value":"2"}]},"liveness_time":1725000000000000000,"draining":true,"binary_version":2,"leader_qps":12.5,"leader_count":7,"replica_bytes":4096,"hot_ranges":[{"range_id":9,"qps":11.25}],"big_ranges":[{"range_id":8,"bytes":2048}]}],"range1":{"range_id":5,"start_key":"YQ==","end_key":"cQ==","replicas":[{"node_id":1,"store_id":1,"replica_id":1},{"node_id":2,"store_id":2,"replica_id":2}],"next_replica_id":3,"generation":12}}`

	goldenRegistry = `[{"node_id":4,"address":"10.0.0.4:26257","locality":{"tiers":[{"key":"region","value":"eu"},{"key":"rack","value":"2"}]},"liveness_time":1725000000000000000,"draining":true,"binary_version":2,"leader_qps":12.5,"leader_count":7,"replica_bytes":4096,"hot_ranges":[{"range_id":9,"qps":11.25}],"big_ranges":[{"range_id":8,"bytes":2048}]}]`
)

func TestGoldenStoreIdent(t *testing.T) {
	var id StoreIdent
	if err := json.Unmarshal([]byte(goldenStoreIdent), &id); err != nil {
		t.Fatal(err)
	}
	want := StoreIdent{
		ClusterID: uuid.MustParse("11111111-2222-3333-4444-555555555555"),
		NodeID:    3, StoreID: 3,
	}
	if id != want {
		t.Fatalf("decoded %+v, want %+v", id, want)
	}
	raw, err := json.Marshal(want)
	if err != nil {
		t.Fatal(err)
	}
	if string(raw) != goldenStoreIdent {
		t.Fatalf("encoding changed:\n got %s\nwant %s", raw, goldenStoreIdent)
	}
}

func TestGoldenJoinRequest(t *testing.T) {
	var req JoinRequest
	if err := json.Unmarshal([]byte(goldenJoinRequest), &req); err != nil {
		t.Fatal(err)
	}
	if req.Address != "10.0.0.7:26257" || req.NodeID != 2 ||
		req.BinaryVersion != 2 || req.MinSupported != 1 {
		t.Fatalf("decoded %+v", req)
	}
	raw, err := json.Marshal(req)
	if err != nil {
		t.Fatal(err)
	}
	if string(raw) != goldenJoinRequest {
		t.Fatalf("encoding changed:\n got %s\nwant %s", raw, goldenJoinRequest)
	}

	// v1-era request: version fields absent, not an error.
	var old JoinRequest
	if err := json.Unmarshal([]byte(goldenJoinRequestV1), &old); err != nil {
		t.Fatal(err)
	}
	if old.BinaryVersion != 0 || old.MinSupported != 0 || old.NodeID != 0 {
		t.Fatalf("v1 request decoded %+v", old)
	}
}

func TestGoldenJoinResponse(t *testing.T) {
	var resp JoinResponse
	if err := json.Unmarshal([]byte(goldenJoinResponse), &resp); err != nil {
		t.Fatal(err)
	}
	if resp.NodeID != 4 || len(resp.Nodes) != 1 || resp.Nodes[0].BinaryVersion != 2 ||
		resp.Range1.RangeID != 5 || len(resp.Range1.Replicas) != 2 {
		t.Fatalf("decoded %+v", resp)
	}
	raw, err := json.Marshal(resp)
	if err != nil {
		t.Fatal(err)
	}
	if string(raw) != goldenJoinResponse {
		t.Fatalf("encoding changed:\n got %s\nwant %s", raw, goldenJoinResponse)
	}
}

func TestGoldenPersistedRegistry(t *testing.T) {
	var nodes []kvpb.NodeDescriptor
	if err := json.Unmarshal([]byte(goldenRegistry), &nodes); err != nil {
		t.Fatal(err)
	}
	if len(nodes) != 1 || nodes[0].NodeID != base.NodeID(4) || !nodes[0].Draining ||
		nodes[0].LeaderCount != 7 || len(nodes[0].HotRanges) != 1 {
		t.Fatalf("decoded %+v", nodes)
	}
	raw, err := json.Marshal(nodes)
	if err != nil {
		t.Fatal(err)
	}
	if string(raw) != goldenRegistry {
		t.Fatalf("encoding changed:\n got %s\nwant %s", raw, goldenRegistry)
	}
}
