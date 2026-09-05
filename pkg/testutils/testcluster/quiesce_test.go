package testcluster

import (
	"context"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/sthorne/datax/pkg/base"
	"github.com/sthorne/datax/pkg/cluster"
	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/kvserver"
	"github.com/sthorne/datax/pkg/metrics"
	"github.com/sthorne/datax/pkg/server"
	"github.com/sthorne/datax/pkg/sql"
	"github.com/sthorne/datax/pkg/sql/catalog"
	"github.com/sthorne/datax/pkg/util/hlc"
	"github.com/sthorne/datax/pkg/version"
)

// replicasOf returns the live replicas of a range, indexed by node.
func replicasOf(tc *TestCluster, rangeID base.RangeID) map[int]*kvserver.Replica {
	out := map[int]*kvserver.Replica{}
	for i, n := range tc.Nodes {
		if n == nil {
			continue
		}
		if r, ok := n.Store().GetReplica(rangeID); ok {
			out[i] = r
		}
	}
	return out
}

// carveRange splits a table's span off into its own range — range 1
// carries the liveness records, which never go idle — and returns it.
func carveRange(t *testing.T, ctx context.Context, tc *TestCluster, tableID uint64) base.RangeID {
	t.Helper()
	if _, err := tc.Nodes[0].DB().AdminSplit(ctx, keys.TableDataPrefix(tableID)); err != nil {
		t.Fatal(err)
	}
	resp, err := tc.Nodes[0].DB().AdminSplit(ctx, keys.TableDataPrefix(tableID).PrefixEnd())
	if err != nil {
		t.Fatal(err)
	}
	return resp.Left.RangeID
}

// waitAllQuiescent waits until every replica of the range is asleep.
func waitAllQuiescent(t *testing.T, tc *TestCluster, rangeID base.RangeID, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		all := true
		for _, r := range replicasOf(tc, rangeID) {
			all = all && r.Quiescent()
		}
		if all {
			return
		}
		if time.Now().After(deadline) {
			for i, r := range replicasOf(tc, rangeID) {
				t.Logf("n%d: leader=%v quiescent=%v", i+1, r.IsLeader(), r.Quiescent())
			}
			t.Fatalf("%s never quiesced on every node within %s", rangeID, timeout)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// assertAwake fails if the replica quiesces at any point during d.
func assertAwake(t *testing.T, r *kvserver.Replica, what string, d time.Duration) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if r.Quiescent() {
			t.Fatalf("%s quiesced", what)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// TestQuiescence (issue #102): an idle range goes quiescent on every
// replica; a write and a read wake it and succeed; with the leader
// partitioned away, a request landing on a follower wakes it, a new
// leader is elected and the request is served; after the partition heals
// the old leader rejoins and the range sleeps again.
func TestQuiescence(t *testing.T) {
	tc := Start(t, 3)
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()
	k := append(keys.TableDataPrefix(901), "k"...)
	rid := carveRange(t, ctx, tc, 901)
	if err := tc.Nodes[0].DB().Put(ctx, k, []byte("v1")); err != nil {
		t.Fatal(err)
	}
	waitAllQuiescent(t, tc, rid, 30*time.Second)

	// Asleep, every replica's closed timestamp keeps advancing through
	// the off-log group promise (follower reads stay fresh), and nobody
	// wakes for it.
	before := map[int]hlc.Timestamp{}
	for i, r := range replicasOf(tc, rid) {
		before[i] = r.ClosedTimestamp()
	}
	time.Sleep(3 * time.Second)
	for i, r := range replicasOf(tc, rid) {
		if !r.Quiescent() {
			t.Fatalf("n%d woke while idle", i+1)
		}
		if adv := r.ClosedTimestamp().WallTime - before[i].WallTime; adv < int64(2*time.Second) {
			t.Fatalf("n%d: closed timestamp advanced %s in 3 s while quiescent (before %s, now %s)", i+1, time.Duration(adv), before[i], r.ClosedTimestamp())
		}
	}

	// A write wakes the range and is applied everywhere; a read after the
	// range slept again is served (the leader re-establishes contact
	// first) and is fresh.
	start := time.Now()
	if err := tc.Nodes[1].DB().Put(ctx, k, []byte("v2")); err != nil {
		t.Fatalf("write to a quiescent range: %v", err)
	}
	t.Logf("write to a quiescent range took %s", time.Since(start))
	waitAllQuiescent(t, tc, rid, 30*time.Second)
	start = time.Now()
	v, err := tc.Nodes[2].DB().Get(ctx, k)
	if err != nil || string(v) != "v2" {
		t.Fatalf("read from a quiescent range: %q, %v", v, err)
	}
	if d := time.Since(start); d > 5*time.Second {
		t.Fatalf("read from a quiescent range took %s", d)
	}
	t.Logf("read from a quiescent range took %s", time.Since(start))
	waitAllQuiescent(t, tc, rid, 30*time.Second)

	// Leader change: the sleeping leader is partitioned away; a request
	// through another node wakes a follower, which times out, campaigns
	// and wins, and the request is served.
	leader := tc.LeaderIndex(rid)
	tc.Isolate(leader)
	other := (leader + 1) % 3
	deadline := time.Now().Add(60 * time.Second)
	for {
		rctx, rcancel := context.WithTimeout(ctx, 10*time.Second)
		v, err = tc.Nodes[other].DB().Get(rctx, k)
		rcancel()
		if err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("read with the quiescent leader partitioned never succeeded: %v", err)
		}
		time.Sleep(200 * time.Millisecond)
	}
	if string(v) != "v2" {
		t.Fatalf("read after the leader change: %q", v)
	}
	newLeader := -1
	for i, r := range replicasOf(tc, rid) {
		if i != leader && r.IsLeader() {
			newLeader = i
		}
	}
	if newLeader < 0 {
		t.Fatal("no new leader among the connected nodes")
	}
	if err := tc.Nodes[newLeader].DB().Put(ctx, k, []byte("v3")); err != nil {
		t.Fatal(err)
	}
	tc.Heal()
	// The old leader learns the new term when traffic reaches it again
	// and everyone sleeps once more; the write is visible through it.
	waitAllQuiescent(t, tc, rid, 60*time.Second)
	if v, err := tc.Nodes[leader].DB().Get(ctx, k); err != nil || string(v) != "v3" {
		t.Fatalf("read through the healed old leader: %q, %v", v, err)
	}
}

// TestQuiescencePartitionedFollower: a follower that cannot be reached
// keeps the range awake — the leader requires every follower to have
// answered recently before it sleeps, and the follower itself never sees
// the parting heartbeat — and everything sleeps once the partition heals.
func TestQuiescencePartitionedFollower(t *testing.T) {
	tc := Start(t, 3)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	k := append(keys.TableDataPrefix(902), "k"...)
	rid := carveRange(t, ctx, tc, 902)
	if err := tc.Nodes[0].DB().Put(ctx, k, []byte("v1")); err != nil {
		t.Fatal(err)
	}
	waitAllQuiescent(t, tc, rid, 30*time.Second)
	leader := tc.LeaderIndex(rid)
	follower := (leader + 1) % 3
	// Wake everyone with a write, then cut the follower off.
	if err := tc.Nodes[leader].DB().Put(ctx, k, []byte("v2")); err != nil {
		t.Fatal(err)
	}
	tc.Isolate(follower)
	reps := replicasOf(tc, rid)
	assertAwake(t, reps[leader], "the leader with an unreachable follower", 5*time.Second)
	if reps[follower].Quiescent() {
		// It may have slept before the cut; a partitioned replica that
		// is awake never sleeps, one already asleep just stays so.
		t.Log("the follower was already asleep when partitioned")
	}
	tc.Heal()
	waitAllQuiescent(t, tc, rid, 60*time.Second)
	if v, err := tc.Nodes[follower].DB().Get(ctx, k); err != nil || string(v) != "v2" {
		t.Fatalf("read after healing: %q, %v", v, err)
	}
}

// TestUpgradeV12Heartbeats: a v11 cluster upgraded to the v12 binary keeps
// sending per-range heartbeats and never quiesces until finalize (a v11
// node reads neither); after finalize heartbeats travel coalesced and
// idle ranges sleep, and traffic keeps flowing throughout.
func TestUpgradeV12Heartbeats(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 240*time.Second)
	defer cancel()
	asV11 := func(c *server.Config) { c.BinaryVersionOverride = version.V11 }
	tc, engines := StartWithEngines(t, 3, asV11)
	tc.LeaderIndex(1)
	k := append(keys.TableDataPrefix(903), "k"...)
	rid := carveRange(t, ctx, tc, 903)
	if err := tc.Nodes[0].DB().Put(ctx, k, []byte("v1")); err != nil {
		t.Fatal(err)
	}
	sess := func(i int) *sql.Session { return sql.NewSession(tc.Nodes[i].DB(), catalog.NewAccessor()) }

	// Rolling restart onto the v12 binary: still a v11 cluster.
	for i := 0; i < 3; i++ {
		tc.StopNode(i)
		tc.RestartNode(i, engines[i])
		tc.LeaderIndex(1)
	}
	waitForAdvertisedVersion(t, ctx, tc.Nodes[0].Addr(), []base.NodeID{1, 2, 3}, int(version.V12))
	envelopesBefore := testutil.ToFloat64(metrics.RaftHeartbeatEnvelopes)
	for i, r := range replicasOf(tc, rid) {
		assertAwake(t, r, "a replica before finalize", time.Duration(i+1)*time.Second)
	}
	if got := testutil.ToFloat64(metrics.RaftHeartbeatEnvelopes); got != envelopesBefore {
		t.Fatalf("coalesced heartbeat envelopes before finalize: %v → %v", envelopesBefore, got)
	}
	if v, err := tc.Nodes[1].DB().Get(ctx, k); err != nil || string(v) != "v1" {
		t.Fatalf("read before finalize: %q, %v", v, err)
	}

	resp := adminCall(t, ctx, tc.Nodes[0].Addr(), cluster.AdminRequest{Op: "upgrade-cluster", Version: int(version.V12)})
	if resp.Error != "" || resp.ClusterVersion != int(version.V12) {
		t.Fatalf("finalize v12: %+v", resp)
	}
	for i := range tc.Nodes {
		waitForClusterVersion(t, ctx, sess(i), version.V12)
	}
	if err := tc.Nodes[2].DB().Put(ctx, k, []byte("v2")); err != nil {
		t.Fatal(err)
	}
	waitAllQuiescent(t, tc, rid, 30*time.Second)
	if got := testutil.ToFloat64(metrics.RaftHeartbeatEnvelopes); got <= envelopesBefore {
		t.Fatalf("no coalesced heartbeat envelopes after finalize (%v)", got)
	}
	if v, err := tc.Nodes[0].DB().Get(ctx, k); err != nil || string(v) != "v2" {
		t.Fatalf("read after finalize: %q, %v", v, err)
	}
}
