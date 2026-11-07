package testcluster

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/sthorne/datax/pkg/server"
)

// TestLeftoverReplicaIsCollected (issue #245): a preseeded replica whose
// membership change never landed is a member of nothing, and nothing
// ever comes back to replace it on a range already at its replication
// factor. The housekeeping pass asks /meta about a replica that has
// led nothing, heard from no leader and applied nothing for the grace,
// and removes one the record does not list. The members, listed, stay
// however quiet; a replica added afterwards, listed and led, is never
// touched.
func TestLeftoverReplicaIsCollected(t *testing.T) {
	hold := newDescriptorHold()
	const grace = 3 * time.Second
	tc := StartWithOptions(t, 4, func(c *server.Config) {
		c.TestingKnobs.BeforeProposeDescriptorChange = hold.knob
		c.RebalanceThreshold = -1
		c.GCInterval = time.Second
		c.ReplicaGCGrace = grace
	})
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()
	f := newRaceFixture(t, ctx, tc, 863)
	spare := f.shedReplica(t, ctx)
	spareIdx := -1
	for i, n := range tc.Nodes {
		if n.NodeID() == spare {
			spareIdx = i
		}
	}

	// The refused change of TestReplicaChangeBuiltBeforeAMergeIsRefused:
	// the LHS is merged away under an addition built before, so the
	// addition is refused and the spare keeps the preseed.
	hold.arm(f.lhs.RangeID, "replica-change")
	lead := tc.LeaderIndex(f.lhs.RangeID)
	changeErr := make(chan error, 1)
	go func() {
		_, err := tc.Nodes[lead].DB().AdminChangeReplicas(ctx, f.lhs.StartKey, spare, 0)
		changeErr <- err
	}()
	hold.waitArrived(t, "the replica change")
	merged := mergeRanges(t, ctx, tc, f.lhs.RangeID)
	close(hold.release)
	select {
	case err := <-changeErr:
		if err == nil || !strings.Contains(err.Error(), "replica change against generation") {
			t.Fatalf("stale replica change: got %v, want refusal", err)
		}
	case <-time.After(60 * time.Second):
		t.Fatal("stale replica change never returned")
	}
	if _, ok := tc.Nodes[spareIdx].Store().LocalDescriptor(f.lhs.RangeID); !ok {
		t.Fatal("the spare does not hold the leftover the refused change left")
	}

	// Within the grace plus a tick or two, the leftover is gone from the
	// spare; the members of the merged range keep their replicas.
	deadline := time.Now().Add(grace + 30*time.Second)
	for {
		if _, ok := tc.Nodes[spareIdx].Store().LocalDescriptor(f.lhs.RangeID); !ok {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the leftover replica was never collected")
		}
		time.Sleep(200 * time.Millisecond)
	}
	for i, n := range tc.Nodes {
		if i == spareIdx {
			continue
		}
		if _, ok := n.Store().LocalDescriptor(merged.RangeID); !ok {
			t.Fatalf("node %d, a member of %s, lost its replica to replica GC", i, &merged)
		}
	}
	f.everyKeyReadable(t, ctx, "after the leftover was collected")

	// A real addition lands on the spare now that nothing is in the way,
	// is listed in /meta, and stays through a grace and more: a member
	// with a leader is not a candidate, and a listed one is never
	// removed.
	if _, err := tc.Nodes[tc.LeaderIndex(merged.RangeID)].DB().AdminChangeReplicas(ctx, merged.StartKey, spare, 0); err != nil {
		t.Fatalf("adding the spare after the leftover was collected: %v", err)
	}
	rec := f.metaRecord(t, ctx, merged.RangeID)
	if _, ok := rec.GetReplica(spare); !ok {
		t.Fatalf("/meta does not list the spare after the addition: %v", &rec)
	}
	time.Sleep(grace + 3*time.Second)
	if _, ok := tc.Nodes[spareIdx].Store().LocalDescriptor(merged.RangeID); !ok {
		t.Fatal("the spare's listed, led replica was collected")
	}
	f.everyKeyReadable(t, ctx, "after the spare rejoined")
}
