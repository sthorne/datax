package placement

import (
	"testing"

	"github.com/sthorne/datax/pkg/base"
	"github.com/sthorne/datax/pkg/kvpb"
)

func nd(t *testing.T, id base.NodeID, loc string, ranges int) Candidate {
	t.Helper()
	l, err := base.ParseLocality(loc)
	if err != nil {
		t.Fatal(err)
	}
	return Candidate{Node: kvpb.NodeDescriptor{NodeID: id, Locality: l}, RangeCount: ranges}
}

func TestAllocateSpreadsAcrossRacks(t *testing.T) {
	// Replica on rack a; candidates: same rack vs other racks.
	existing := []kvpb.NodeDescriptor{nd(t, 1, "region=r1,rack=a", 0).Node}
	cands := []Candidate{
		nd(t, 2, "region=r1,rack=a", 0),
		nd(t, 3, "region=r1,rack=b", 0),
		nd(t, 4, "region=r1,rack=c", 5),
	}
	id, ok := AllocateTarget(existing, cands)
	if !ok || id != 3 {
		// rack=b and rack=c tie on diversity; b wins on range count.
		t.Fatalf("got n%d", id)
	}

	// With a and b taken, rack=c must win over another node in rack a.
	existing = append(existing, nd(t, 3, "region=r1,rack=b", 0).Node)
	cands = []Candidate{
		nd(t, 2, "region=r1,rack=a", 0),
		nd(t, 4, "region=r1,rack=c", 5),
	}
	id, ok = AllocateTarget(existing, cands)
	if !ok || id != 4 {
		t.Fatalf("got n%d", id)
	}
}

func TestAllocateCrossRegionBeatsCrossRack(t *testing.T) {
	existing := []kvpb.NodeDescriptor{nd(t, 1, "region=r1,rack=a", 0).Node}
	cands := []Candidate{
		nd(t, 2, "region=r1,rack=b", 0),
		nd(t, 3, "region=r2,rack=a", 0),
	}
	id, ok := AllocateTarget(existing, cands)
	if !ok || id != 3 {
		t.Fatalf("got n%d", id)
	}
}

func TestAllocateSkipsHolders(t *testing.T) {
	existing := []kvpb.NodeDescriptor{nd(t, 1, "rack=a", 0).Node, nd(t, 2, "rack=b", 0).Node}
	cands := []Candidate{nd(t, 1, "rack=a", 0), nd(t, 2, "rack=b", 0)}
	if _, ok := AllocateTarget(existing, cands); ok {
		t.Fatal("allocated onto an existing holder")
	}
}

func TestAllocateNoLocalities(t *testing.T) {
	// Without localities, allocation still works (spread by range count).
	existing := []kvpb.NodeDescriptor{nd(t, 1, "", 0).Node}
	cands := []Candidate{nd(t, 2, "", 3), nd(t, 3, "", 1)}
	id, ok := AllocateTarget(existing, cands)
	if !ok || id != 3 {
		t.Fatalf("got n%d", id)
	}
}

func TestRemoveTargetKeepsDiversity(t *testing.T) {
	// Two replicas on rack a, one on b: removing one of the a's keeps a+b.
	existing := []kvpb.NodeDescriptor{
		nd(t, 1, "region=r1,rack=a", 0).Node,
		nd(t, 2, "region=r1,rack=a", 0).Node,
		nd(t, 3, "region=r1,rack=b", 0).Node,
	}
	id, ok := RemoveTarget(existing)
	if !ok || (id != 1 && id != 2) {
		t.Fatalf("got n%d", id)
	}
}

func TestRebalanceKeepsDiversity(t *testing.T) {
	existing := []kvpb.NodeDescriptor{
		nd(t, 1, "region=r1,rack=a", 0).Node,
		nd(t, 2, "region=r1,rack=b", 0).Node,
		nd(t, 3, "region=r1,rack=c", 0).Node,
	}
	// Same-rack swap keeps diversity; cross-rack duplication loses it.
	if !RebalanceKeepsDiversity(existing, 2, nd(t, 4, "region=r1,rack=b", 0).Node) {
		t.Fatal("same-rack replacement rejected")
	}
	if RebalanceKeepsDiversity(existing, 1, nd(t, 4, "region=r1,rack=b", 0).Node) {
		t.Fatal("rack-duplicating move accepted")
	}
	// remove not in the set: never a valid move.
	if RebalanceKeepsDiversity(existing, 9, nd(t, 4, "region=r1,rack=d", 0).Node) {
		t.Fatal("move from a non-member accepted")
	}
	// No localities anywhere: every swap is diversity-neutral.
	plain := []kvpb.NodeDescriptor{nd(t, 1, "", 0).Node, nd(t, 2, "", 0).Node}
	if !RebalanceKeepsDiversity(plain, 1, nd(t, 3, "", 0).Node) {
		t.Fatal("locality-free swap rejected")
	}
}

// node builds a descriptor with a locality for the placement tests.
func nodeWithLocality(t *testing.T, id int, locality string) kvpb.NodeDescriptor {
	t.Helper()
	l, err := base.ParseLocality(locality)
	if err != nil {
		t.Fatal(err)
	}
	return kvpb.NodeDescriptor{NodeID: base.NodeID(id), Locality: l}
}

// TestAllocateTargetHonoursConstraints (issue #176): a constrained
// policy never places a replica outside it, and inside it still spreads
// across the failure domains it can reach.
func TestAllocateTargetHonoursConstraints(t *testing.T) {
	eu1a := nodeWithLocality(t, 1, "region=eu-west-1,rack=a")
	eu1b := nodeWithLocality(t, 2, "region=eu-west-1,rack=b")
	us1a := nodeWithLocality(t, 3, "region=us-east-1,rack=a")
	us1b := nodeWithLocality(t, 4, "region=us-east-1,rack=b")
	all := []Candidate{{Node: eu1a}, {Node: eu1b}, {Node: us1a}, {Node: us1b}}
	eu := base.PlacementPolicy{Constraints: []base.Constraint{{Key: "region", Value: "eu-west-1"}}}

	// With one replica already in eu-west-1 rack a, the only admissible
	// candidate is the other eu-west-1 node — the more diverse US nodes
	// are not eligible at all.
	got, ok := AllocateTargetFor(eu, []kvpb.NodeDescriptor{eu1a}, all)
	if !ok || got != eu1b.NodeID {
		t.Fatalf("constrained allocation chose n%d (ok=%v); want n%d", got, ok, eu1b.NodeID)
	}
	// Unconstrained, the same call prefers the most diverse node.
	got, ok = AllocateTargetFor(base.PlacementPolicy{}, []kvpb.NodeDescriptor{eu1a}, all)
	if !ok || (got != us1a.NodeID && got != us1b.NodeID) {
		t.Fatalf("unconstrained allocation chose n%d (ok=%v); want a us-east-1 node", got, ok)
	}
	// Within a region, the rack the range is not yet in wins.
	rackA := nodeWithLocality(t, 5, "region=eu-west-1,rack=a")
	got, ok = AllocateTargetFor(eu, []kvpb.NodeDescriptor{eu1a}, []Candidate{{Node: rackA}, {Node: eu1b}})
	if !ok || got != eu1b.NodeID {
		t.Fatalf("within a region the allocator chose n%d; want the other rack n%d", got, eu1b.NodeID)
	}
	// A policy no candidate satisfies places nothing rather than
	// widening itself.
	ap := base.PlacementPolicy{Constraints: []base.Constraint{{Key: "region", Value: "ap-south-1"}}}
	if got, ok := AllocateTargetFor(ap, nil, all); ok {
		t.Fatalf("an unsatisfiable policy placed a replica on n%d", got)
	}
}

func TestSatisfyingNodesAndMisplaced(t *testing.T) {
	eu := nodeWithLocality(t, 1, "region=eu-west-1")
	us := nodeWithLocality(t, 2, "region=us-east-1")
	policy := base.PlacementPolicy{Constraints: []base.Constraint{{Key: "region", Value: "eu-west-1"}}}
	if got := SatisfyingNodes(policy, []kvpb.NodeDescriptor{eu, us}); len(got) != 1 || got[0].NodeID != eu.NodeID {
		t.Fatalf("SatisfyingNodes gave %v", got)
	}
	if got := SatisfyingNodes(base.PlacementPolicy{}, []kvpb.NodeDescriptor{eu, us}); len(got) != 2 {
		t.Fatalf("an unconstrained policy filtered nodes: %v", got)
	}
	localities := map[base.NodeID]base.Locality{eu.NodeID: eu.Locality, us.NodeID: us.Locality}
	got := Misplaced(policy, []base.NodeID{eu.NodeID, us.NodeID, 99}, localities)
	if len(got) != 1 || got[0] != us.NodeID {
		// n99 is unknown to this node, so it is left alone rather than
		// assumed to be in violation.
		t.Fatalf("Misplaced gave %v; want just n%d", got, us.NodeID)
	}
	if got := Misplaced(base.PlacementPolicy{}, []base.NodeID{us.NodeID}, localities); got != nil {
		t.Fatalf("an unconstrained policy reported %v misplaced", got)
	}
}
