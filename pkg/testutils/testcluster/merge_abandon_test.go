package testcluster

import (
	"context"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sthorne/datax/pkg/base"
	"github.com/sthorne/datax/pkg/server"
)

// Issue #263: the nightly bench's 1000-way presplit left one node more
// than a minute behind on applying the split chain, so for a while it had
// no replica of most of the new ranges. The merge pass, undoing the tiny
// ranges, froze each RHS first and only then found no replica on that
// node to confirm the freeze with; the attempt failed, the RHS stayed
// frozen, and the next pass froze it again — every request to those
// ranges was refused ("range is frozen for a merge", which the store's
// re-routing then reported as "no replica covering key") until the node
// caught up. These tests take a replica away before and after the freeze
// and check that the RHS keeps serving either way.

// leadBothSides makes the LHS leader lead the RHS too, so the merge driver
// runs on it, and returns that node's index and the index of a node that
// leads neither (the one a test may take away).
func leadBothSides(t *testing.T, ctx context.Context, tc *TestCluster, f *raceFixture) (lead, victim int) {
	t.Helper()
	lead = tc.LeaderIndex(f.lhs.RangeID)
	deadline := time.Now().Add(30 * time.Second)
	for tc.LeaderIndex(f.rhs.RangeID) != lead {
		if time.Now().After(deadline) {
			t.Fatalf("n%d never came to lead %s", tc.Nodes[lead].NodeID(), f.rhs.RangeID)
		}
		_ = tc.Nodes[lead].DB().AdminTransferLease(ctx, f.rhs.StartKey, tc.Nodes[lead].NodeID())
		time.Sleep(100 * time.Millisecond)
	}
	for i := range tc.Nodes {
		if i != lead {
			return lead, i
		}
	}
	t.Fatal("no node other than the leader")
	return 0, 0
}

// rhsServes writes a fresh key in the RHS through the driver with a bound
// far below the gateway's frozen-range wait: a frozen RHS would not answer
// in time.
func rhsServes(t *testing.T, ctx context.Context, tc *TestCluster, f *raceFixture, lead int, when string) {
	t.Helper()
	wctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err := tc.Nodes[lead].DB().Put(wctx, append(f.prefix.Clone(), "z99"...), []byte("w")); err != nil {
		t.Fatalf("%s: the RHS does not serve: %v", when, err)
	}
}

// TestMergeIsNotAttemptedWhileAnRHSReplicaIsMissing: with an RHS replica's
// node down, the driver must find out before freezing anything, skip the
// merge, and leave the RHS serving. Once the node is back the merge goes
// through.
func TestMergeIsNotAttemptedWhileAnRHSReplicaIsMissing(t *testing.T) {
	var froze atomic.Int32
	tc, engines := StartWithEngines(t, 3, func(c *server.Config) {
		c.TestingKnobs.AfterMergeSubsume = func(_, _ base.RangeID) { froze.Add(1) }
	})
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()
	f := newRaceFixture(t, ctx, tc, 863)
	lead, victim := leadBothSides(t, ctx, tc, f)

	tc.StopNode(victim)
	_, err := tc.Nodes[lead].DB().AdminMerge(ctx, f.lhs.StartKey)
	if err == nil || !strings.Contains(err.Error(), "not merging") || !strings.Contains(err.Error(), "is not caught up") {
		t.Fatalf("merge with a replica's node down: got %v, want it declined before the freeze", err)
	}
	if n := froze.Load(); n != 0 {
		t.Fatalf("the RHS was frozen %d time(s) for a merge that could not be confirmed", n)
	}
	rhsServes(t, ctx, tc, f, lead, "with the node down")

	tc.RestartNode(victim, engines[victim])
	merged := mergeRanges(t, ctx, tc, f.lhs.RangeID)
	if !merged.EndKey.Equal(f.rhs.EndKey) {
		t.Fatalf("merge produced %v, want end %s", merged, f.rhs.EndKey)
	}
	f.everyKeyReadable(t, ctx, "after the merge")
}

// TestMergeAbandonedAfterTheFreezeUnfreezesTheRHS: the pre-freeze check
// passes, then an RHS replica's node goes away before the confirmation.
// The attempt fails, and the freeze it made is unwound at once rather than
// left for the laggard — the RHS serves again straight away, and the merge
// goes through once the node is back.
func TestMergeAbandonedAfterTheFreezeUnfreezesTheRHS(t *testing.T) {
	var (
		cluster atomic.Pointer[TestCluster]
		victim  atomic.Int64 // index+1, 0 = not chosen yet
		stopped atomic.Bool
	)
	tc, engines := StartWithEngines(t, 3, func(c *server.Config) {
		c.TestingKnobs.AfterMergeSubsume = func(_, _ base.RangeID) {
			if v := victim.Load(); v != 0 && stopped.CompareAndSwap(false, true) {
				cluster.Load().StopNode(int(v - 1))
			}
		}
	})
	cluster.Store(tc)
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()
	f := newRaceFixture(t, ctx, tc, 864)
	lead, v := leadBothSides(t, ctx, tc, f)
	victim.Store(int64(v + 1))

	_, err := tc.Nodes[lead].DB().AdminMerge(ctx, f.lhs.StartKey)
	if err == nil || !strings.Contains(err.Error(), "has not applied the subsume") {
		t.Fatalf("merge losing a replica after the freeze: got %v, want the confirmation failure", err)
	}
	if !stopped.Load() {
		t.Fatal("the knob never ran: the merge did not freeze the RHS")
	}
	rhsServes(t, ctx, tc, f, lead, "after the abandoned merge")

	tc.RestartNode(v, engines[v])
	merged := mergeRanges(t, ctx, tc, f.lhs.RangeID)
	if !merged.EndKey.Equal(f.rhs.EndKey) {
		t.Fatalf("merge produced %v, want end %s", merged, f.rhs.EndKey)
	}
	f.everyKeyReadable(t, ctx, "after the merge")
}
