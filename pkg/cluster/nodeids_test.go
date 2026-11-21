package cluster

import (
	"reflect"
	"testing"

	"github.com/sthorne/datax/pkg/base"
	"github.com/sthorne/datax/pkg/kvpb"
)

// TestRegistryNodeIDs: NodeIDs lists All's IDs in order, reuses its
// slice while the membership is unchanged, and sees a node added by
// either upsert.
func TestRegistryNodeIDs(t *testing.T) {
	r := NewRegistry()
	if got := r.NodeIDs(); len(got) != 0 {
		t.Fatalf("empty registry: %v", got)
	}
	r.Upsert(kvpb.NodeDescriptor{NodeID: 3, Address: "c", LivenessTime: 1})
	r.Upsert(kvpb.NodeDescriptor{NodeID: 1, Address: "a", LivenessTime: 1})
	ids := r.NodeIDs()
	if !reflect.DeepEqual(ids, []base.NodeID{1, 3}) {
		t.Fatalf("got %v", ids)
	}
	// A newer row for a known node, or a known address: the same set.
	r.Upsert(kvpb.NodeDescriptor{NodeID: 3, Address: "c2", LivenessTime: 2})
	r.UpsertAddress(1, "a2")
	if again := r.NodeIDs(); &again[0] != &ids[0] {
		t.Fatal("unchanged membership rebuilt the list")
	}
	if n := testing.AllocsPerRun(100, func() { _ = r.NodeIDs() }); n != 0 {
		t.Fatalf("unchanged membership allocated %v times", n)
	}
	r.UpsertAddress(2, "b")
	if got := r.NodeIDs(); !reflect.DeepEqual(got, []base.NodeID{1, 2, 3}) {
		t.Fatalf("after UpsertAddress of a new node: %v", got)
	}
	r.Upsert(kvpb.NodeDescriptor{NodeID: 0, Address: "z", LivenessTime: 1})
	r.Upsert(kvpb.NodeDescriptor{NodeID: 9, Address: "i", LivenessTime: 1})
	want := []base.NodeID{}
	for _, nd := range r.All() {
		want = append(want, nd.NodeID)
	}
	if got := r.NodeIDs(); !reflect.DeepEqual(got, want) {
		t.Fatalf("NodeIDs %v, All %v", got, want)
	}
}
