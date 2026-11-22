package server

import (
	"slices"
	"testing"
	"time"

	"github.com/sthorne/datax/pkg/base"
	"github.com/sthorne/datax/pkg/kvpb"
)

// TestMissingReplicasAfterGrace: a range whose descriptor names this node
// while no replica of it is loaded here is reported missing once that
// has lasted the grace period — not before (a follower yet to apply a
// split), and not once the replica is loaded or the descriptor stops
// naming the node.
func TestMissingReplicasAfterGrace(t *testing.T) {
	const self = base.NodeID(2)
	rep := func(ids ...base.NodeID) []kvpb.ReplicaDescriptor {
		var out []kvpb.ReplicaDescriptor
		for _, id := range ids {
			out = append(out, kvpb.ReplicaDescriptor{NodeID: id, StoreID: base.StoreID(id)})
		}
		return out
	}
	descs := []kvpb.RangeDescriptor{
		{RangeID: 1, Replicas: rep(1, 2, 3)}, // loaded here
		{RangeID: 5, Replicas: rep(1, 2, 3)}, // missing here
		{RangeID: 7, Replicas: rep(1, 3)},    // not ours
		{RangeID: 9, Replicas: rep(2, 3)},    // missing here, seen later
	}
	loaded := map[base.RangeID]bool{1: true}
	has := func(id base.RangeID) bool { return loaded[id] }
	grace := time.Minute
	t0 := time.Unix(1000, 0)

	since, missing := missingReplicas(descs[:3], self, has, nil, t0, grace)
	if len(missing) != 0 {
		t.Fatalf("reported missing before the grace: %v", missing)
	}
	since, missing = missingReplicas(descs, self, has, since, t0.Add(grace), grace)
	if want := []base.RangeID{5}; !slices.Equal(missing, want) {
		t.Fatalf("after the grace: missing %v, want %v", missing, want)
	}
	since, missing = missingReplicas(descs, self, has, since, t0.Add(2*grace), grace)
	if want := []base.RangeID{5, 9}; !slices.Equal(missing, want) {
		t.Fatalf("after r9's grace: missing %v, want %v", missing, want)
	}
	// r5's replica loads; r9 stops naming this node.
	loaded[5] = true
	descs[3].Replicas = rep(1, 3)
	if _, missing = missingReplicas(descs, self, has, since, t0.Add(3*grace), grace); len(missing) != 0 {
		t.Fatalf("still missing once loaded or no longer named: %v", missing)
	}
}
