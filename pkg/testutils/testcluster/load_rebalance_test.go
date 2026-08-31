package testcluster

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/sthorne/datax/pkg/base"
	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/server"
)

// leaderOf returns the index of the node whose store currently leads the
// range, or -1.
func leaderOf(tc *TestCluster, id base.RangeID) int {
	for i, n := range tc.Nodes {
		if r, ok := n.Store().GetReplica(id); ok && r.IsLeader() {
			return i
		}
	}
	return -1
}

// TestLeaseShedding: a node whose advertised leader QPS exceeds the mean
// by the shed factor gives up the lease whose departure best shrinks the
// imbalance; the per-range cooldown blocks an immediate repeat on stale
// aggregates; once balanced, further passes do nothing (zero churn).
func TestLeaseShedding(t *testing.T) {
	var hotMu sync.Mutex
	hot := map[base.RangeID]float64{}
	setHot := func(id base.RangeID, qps float64) {
		hotMu.Lock()
		defer hotMu.Unlock()
		hot[id] = qps
	}
	tc, _ := StartWithEngines(t, 3, func(c *server.Config) {
		c.SplitSizeThreshold = 1 << 30
		c.LoadSplitThreshold = -1           // hot ranges must not load-split away
		c.UpreplicationInterval = time.Hour // the test drives the allocator
		c.TestingKnobs.OverrideReplicaQPS = func(id base.RangeID) (float64, bool) {
			hotMu.Lock()
			defer hotMu.Unlock()
			q, ok := hot[id]
			return q, ok
		}
	})
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	db := tc.Nodes[0].DB()

	// Two table ranges, both led by node 1 (splits inherit leadership).
	prefix := keys.TableDataPrefix(830)
	for i := 0; i < 20; i++ {
		if err := db.Put(ctx, append(prefix.Clone(), fmt.Sprintf("row-%02d", i)...), []byte("v")); err != nil {
			t.Fatal(err)
		}
	}
	splitAt := append(prefix.Clone(), "row-10"...)
	if _, err := db.AdminSplit(ctx, splitAt); err != nil {
		t.Fatal(err)
	}
	descs, err := tc.ranges(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var rangeA, rangeB base.RangeID // A: [.., row-10), B: [row-10, ..)
	var startA, startB keys.Key
	for _, d := range descs {
		if d.StartKey.Compare(prefix) <= 0 && prefix.Compare(d.EndKey) < 0 {
			rangeA, startA = d.RangeID, d.StartKey.Clone()
		}
		if d.StartKey.Compare(splitAt) <= 0 && splitAt.Compare(d.EndKey) < 0 {
			rangeB, startB = d.RangeID, d.StartKey.Clone()
		}
	}
	if rangeA == 0 || rangeB == 0 || rangeA == rangeB {
		t.Fatalf("table ranges not found: %v %v", rangeA, rangeB)
	}
	// Put both leases on node 1 deterministically (elections may have
	// moved them).
	for _, rng := range []struct {
		id    base.RangeID
		start keys.Key
	}{{rangeA, startA}, {rangeB, startB}} {
		deadline := time.Now().Add(30 * time.Second)
		for leaderOf(tc, rng.id) != 0 {
			_ = db.AdminTransferLease(ctx, rng.start, 1)
			if time.Now().After(deadline) {
				t.Fatalf("could not settle leadership of %s on node 1", rng.id)
			}
			time.Sleep(100 * time.Millisecond)
		}
	}

	// Node 1 leads A at 2000 and B at 1500 QPS: 3500 total vs mean ~1167.
	// Shedding A would leave 1500 behind while handing 2000 to a zero-QPS
	// node (imbalance relocated, not shrunk — the projection guard blocks
	// it); shedding B (1500 moved, 2000 kept) genuinely improves, so B is
	// the one that must move.
	setHot(rangeA, 2000)
	setHot(rangeB, 1500)

	// The aggregates reach the allocator via heartbeats (~3s cadence);
	// poll until the pass acts.
	allocator := tc.Nodes[0]
	var action string
	deadline := time.Now().Add(30 * time.Second)
	for {
		action = allocator.RunRebalanceOnce(ctx)
		if action != "" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("lease shed never fired")
		}
		time.Sleep(200 * time.Millisecond)
	}
	if action != "lease-shed" {
		t.Fatalf("first action = %q, want lease-shed", action)
	}
	if got := leaderOf(tc, rangeB); got == 0 {
		t.Fatal("range B still led by node 1 after shed")
	}
	if got := leaderOf(tc, rangeA); got != 0 {
		t.Fatalf("range A moved (leader now node %d); the projection guard should have kept it", got+1)
	}

	// Immediately after, the registry still shows the pre-shed aggregates
	// — without the cooldown the same range would shed again.
	if a := allocator.RunRebalanceOnce(ctx); a != "" {
		t.Fatalf("op on stale aggregates right after a shed: %q", a)
	}

	// Converged: whatever the refresh timing, no further ops ever fire
	// (A's departure can only relocate the imbalance, B is settled).
	settleUntil := time.Now().Add(7 * time.Second) // > 2 heartbeats
	for time.Now().Before(settleUntil) {
		if a := allocator.RunRebalanceOnce(ctx); a != "" {
			t.Fatalf("churn after convergence: %q", a)
		}
		time.Sleep(300 * time.Millisecond)
	}
}

// TestByteRebalance: with range counts effectively level, a large byte
// spread moves the biggest range from the byte-fullest node to the
// emptiest; afterwards the pass goes quiet.
func TestByteRebalance(t *testing.T) {
	tc, _ := StartWithEngines(t, 4, func(c *server.Config) {
		c.SplitSizeThreshold = 1 << 30
		c.LoadSplitThreshold = -1
		c.UpreplicationInterval = time.Hour
		c.RebalanceThreshold = 100 // count moves off; surplus trim stays
		c.RebalanceBytesThreshold = 256 << 10
	})
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	db := tc.Nodes[0].DB()
	allocator := tc.Nodes[0]

	// Static bootstrap put every range on all 4 nodes; trim to RF3 first
	// (each call trims one range).
	deadline := time.Now().Add(30 * time.Second)
	for {
		a := allocator.RunRebalanceOnce(ctx)
		if a == "" {
			break
		}
		if a != "rebalance" {
			t.Fatalf("unexpected action while trimming: %q", a)
		}
		if time.Now().After(deadline) {
			t.Fatal("surplus trimming never settled")
		}
	}

	// ~1.5 MiB into one table range: the three holders get heavy, the
	// fourth node stays light.
	prefix := keys.TableDataPrefix(831)
	payload := make([]byte, 8<<10)
	for i := range payload {
		payload[i] = byte(i)
	}
	for i := 0; i < 192; i++ {
		if err := db.Put(ctx, append(prefix.Clone(), fmt.Sprintf("row-%03d", i)...), payload); err != nil {
			t.Fatal(err)
		}
	}
	var tableRange base.RangeID
	descs, err := tc.ranges(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, d := range descs {
		if d.StartKey.Compare(prefix) <= 0 && prefix.Compare(d.EndKey) < 0 {
			tableRange = d.RangeID
			break
		}
	}
	holders := func() map[base.NodeID]bool {
		out := map[base.NodeID]bool{}
		descs, err := tc.ranges(ctx)
		if err != nil {
			t.Fatal(err)
		}
		for _, d := range descs {
			if d.RangeID != tableRange {
				continue
			}
			for _, r := range d.Replicas {
				out[r.NodeID] = true
			}
		}
		return out
	}
	before := holders()
	if len(before) != 3 {
		t.Fatalf("table range on %d nodes, want 3", len(before))
	}
	var lightNode base.NodeID
	for i := 1; i <= 4; i++ {
		if !before[base.NodeID(i)] {
			lightNode = base.NodeID(i)
		}
	}

	// Wait for heartbeats to advertise the byte skew, then expect exactly
	// one byte move: the big range lands on the light node.
	var action string
	deadline = time.Now().Add(30 * time.Second)
	for {
		action = allocator.RunRebalanceOnce(ctx)
		if action != "" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("byte rebalance never fired")
		}
		time.Sleep(200 * time.Millisecond)
	}
	if action != "bytes" {
		t.Fatalf("action = %q, want bytes", action)
	}
	after := holders()
	if !after[lightNode] {
		t.Fatalf("big range did not land on the light node n%d (now on %v)", lightNode, after)
	}

	// Settled: the spread is now one big-range hop at minimum, and every
	// further pass stays quiet across heartbeat refreshes.
	settleUntil := time.Now().Add(7 * time.Second)
	for time.Now().Before(settleUntil) {
		if a := allocator.RunRebalanceOnce(ctx); a != "" {
			t.Fatalf("churn after byte rebalance: %q", a)
		}
		time.Sleep(300 * time.Millisecond)
	}
}

// TestLoadBalancingIdle: an idle cluster — QPS under the absolute floor,
// bytes level — never sheds or moves anything.
func TestLoadBalancingIdle(t *testing.T) {
	tc, _ := StartWithEngines(t, 3, func(c *server.Config) {
		c.UpreplicationInterval = time.Hour
	})
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	// Across a few heartbeat cycles: nothing to do, ever.
	until := time.Now().Add(7 * time.Second)
	for time.Now().Before(until) {
		if a := tc.Nodes[0].RunRebalanceOnce(ctx); a != "" {
			t.Fatalf("idle cluster acted: %q", a)
		}
		time.Sleep(250 * time.Millisecond)
	}
}
