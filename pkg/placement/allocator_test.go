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
