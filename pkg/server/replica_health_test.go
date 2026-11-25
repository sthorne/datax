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

// TestReplicaReportCapped: a heartbeat lists at most maxReportedReplicas
// failed or missing replicas, the lowest range IDs, but its count stays
// exact; a row from a binary without counts reads its list length.
func TestReplicaReportCapped(t *testing.T) {
	var ids []base.RangeID
	for i := 1; i <= 1000; i++ {
		ids = append(ids, base.RangeID(i))
	}
	list, count := capReplicaReport(ids, maxReportedReplicas)
	if len(list) != maxReportedReplicas || list[0] != 1 || list[len(list)-1] != base.RangeID(maxReportedReplicas) {
		t.Fatalf("capped list: len %d, %v..%v", len(list), list[0], list[len(list)-1])
	}
	if count != 1000 {
		t.Fatalf("count = %d, want 1000", count)
	}
	if got := reportedCount(list, count); got != 1000 {
		t.Fatalf("reportedCount = %d, want 1000", got)
	}
	list, count = capReplicaReport(ids[:3], maxReportedReplicas)
	if len(list) != 3 || count != 3 {
		t.Fatalf("short list: %v, %d", list, count)
	}
	if got := reportedCount(ids[:3], 0); got != 3 {
		t.Fatalf("legacy row: reportedCount = %d, want 3", got)
	}
}

// TestReplicaFaultsUnknownPastCappedLists: a node whose capped lists
// could not be completed is known down for the ranges it lists, unknown
// above the last range its lists are exact to, and serving below it; a
// range whose state would differ with those unknown replicas serving or
// not is not classified.
func TestReplicaFaultsUnknownPastCappedLists(t *testing.T) {
	if to, capped := cappedReport([]base.RangeID{3, 7}, 2); capped || to != 0 {
		t.Fatalf("a complete list reads capped: %v %v", to, capped)
	}
	if to, capped := cappedReport([]base.RangeID{3, 7}, 0); capped || to != 0 {
		t.Fatalf("a legacy row (no count) reads capped: %v %v", to, capped)
	}
	if to, capped := cappedReport([]base.RangeID{3, 7}, 9); !capped || to != 7 {
		t.Fatalf("capped list: %v %v, want 7 true", to, capped)
	}
	f := &replicaFaults{
		down:         map[base.NodeID]map[base.RangeID]bool{},
		unknownAbove: map[base.NodeID]base.RangeID{3: 7},
	}
	markUnavailable(f.down, 3, []base.RangeID{3, 7})
	for _, c := range []struct {
		r             base.RangeID
		down, unknown bool
	}{{3, true, false}, {5, false, false}, {7, true, false}, {8, false, true}, {100, false, true}} {
		if down, unknown := f.state(3, c.r); down != c.down || unknown != c.unknown {
			t.Errorf("r%d on n3: down %v unknown %v, want %v %v", c.r, down, unknown, c.down, c.unknown)
		}
	}
	if down, unknown := f.state(2, 100); down || unknown {
		t.Errorf("a node with complete lists reads unknown")
	}
	for _, c := range []struct {
		live, unknown, total, want int
		state                      replState
		known                      bool
	}{
		{3, 0, 3, 3, replHealthy, true},
		{2, 0, 3, 3, replUnder, true},
		{1, 0, 3, 3, replNoQuorum, true},
		{3, 1, 3, 3, replHealthy, false}, // healthy or under
		{2, 1, 3, 3, replUnder, false},   // under or no quorum
		{2, 1, 5, 3, replNoQuorum, true}, // no quorum either way
		{3, 1, 4, 3, replOver, false},    // over or no quorum
		{5, 1, 5, 3, replOver, true},     // over either way
	} {
		state, known := classifyUncertain(c.live, c.unknown, c.total, c.want)
		if state != c.state || known != c.known {
			t.Errorf("classifyUncertain(%d, %d, %d, %d) = %v %v, want %v %v", c.live, c.unknown, c.total, c.want, state, known, c.state, c.known)
		}
	}
}
