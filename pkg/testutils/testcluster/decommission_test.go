package testcluster

import (
	"context"
	"testing"
	"time"

	"github.com/sthorne/datax/pkg/base"
	"github.com/sthorne/datax/pkg/cluster"
	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/rpc"
	"github.com/sthorne/datax/pkg/util/hlc"
)

// adminCall sends one admin RPC the way the datax debug CLI does.
func adminCall(t *testing.T, ctx context.Context, addr string, req cluster.AdminRequest) cluster.AdminResponse {
	t.Helper()
	trans := rpc.NewTransport(hlc.NewClock(nil, base.DefaultMaxClockOffset), nil, nil)
	cctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	var resp cluster.AdminResponse
	if err := trans.Call(cctx, addr, "admin", req, &resp); err != nil {
		t.Fatalf("admin %s: %v", req.Op, err)
	}
	return resp
}

// TestDecommissionDrainsReplicas: decommissioning the range-1 leader (the
// hardest case: the allocator must move its own leadership away) drains
// every replica to the spare while the node is alive; once empty and
// stopped, no repair churn follows. Regression test for issue #3.
func TestDecommissionDrainsReplicas(t *testing.T) {
	tc := startRepairCluster(t, 3, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()

	k := append(keys.TableDataPrefix(830), "k"...)
	if err := tc.Nodes[0].DB().Put(ctx, k, []byte("v")); err != nil {
		t.Fatal(err)
	}

	lead := tc.LeaderIndex(1)
	target := tc.Nodes[lead].NodeID()
	resp := adminCall(t, ctx, tc.Nodes[(lead+1)%3].Addr(), cluster.AdminRequest{Op: "decommission", NodeID: target})
	if resp.Error != "" || !resp.Draining {
		t.Fatalf("decommission response: %+v", resp)
	}

	// Drain completes: the node ends with zero replicas.
	deadline := time.Now().Add(60 * time.Second)
	for {
		counts, _, err := tc.rangeCounts(ctx)
		if err == nil && counts[target] == 0 {
			break
		}
		if time.Now().After(deadline) {
			c, _, _ := tc.rangeCounts(ctx)
			t.Fatalf("drain never completed: %v", c)
		}
		time.Sleep(250 * time.Millisecond)
	}
	if s := adminCall(t, ctx, tc.Nodes[(lead+1)%3].Addr(), cluster.AdminRequest{Op: "decommission", NodeID: target}); s.RemainingReplicas != 0 {
		t.Fatalf("status still reports %d replicas", s.RemainingReplicas)
	}

	// Stopping the drained node causes zero churn, even past the dead-node
	// threshold: there is nothing left to repair.
	_, before, err := tc.rangeCounts(ctx)
	if err != nil {
		t.Fatal(err)
	}
	tc.StopNode(lead)
	time.Sleep(repairTestThreshold + 3*time.Second)
	_, after, err := tc.rangeCounts(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(before) != len(after) {
		t.Fatalf("range set changed: %d -> %d", len(before), len(after))
	}
	for i := range before {
		if before[i].Generation != after[i].Generation {
			t.Fatalf("churn after stopping drained node: range %d generation %d -> %d",
				before[i].RangeID, before[i].Generation, after[i].Generation)
		}
	}
	if v, err := tc.Nodes[(lead+1)%3].DB().Get(ctx, k); err != nil || string(v) != "v" {
		t.Fatalf("read after decommission: %q, %v", v, err)
	}
}

// TestDecommissionRefusesBelowQuorum: with nowhere to move replicas, the
// drain stalls safely — the range never drops below its replication factor —
// and completes as soon as a spare joins.
func TestDecommissionRefusesBelowQuorum(t *testing.T) {
	tc := startRepairCluster(t, 3, 0)
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()

	resp := adminCall(t, ctx, tc.Nodes[0].Addr(), cluster.AdminRequest{Op: "decommission", NodeID: 3})
	if resp.Error != "" || !resp.Draining {
		t.Fatalf("decommission response: %+v", resp)
	}

	// Several allocator ticks later the replica is still there: stuck, safe.
	time.Sleep(3 * time.Second)
	counts, descs, err := tc.rangeCounts(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if counts[3] == 0 {
		t.Fatal("drain completed with nowhere to go")
	}
	for _, d := range descs {
		if len(d.Replicas) < 3 {
			t.Fatalf("range %d dropped below RF during a stuck drain: %+v", d.RangeID, d)
		}
	}

	// A fresh node joins; the drain finishes onto it.
	tc.AddNode("")
	deadline := time.Now().Add(90 * time.Second)
	for {
		counts, _, err := tc.rangeCounts(ctx)
		if err == nil && counts[3] == 0 {
			return
		}
		if time.Now().After(deadline) {
			c, _, _ := tc.rangeCounts(ctx)
			t.Fatalf("drain never completed after a spare joined: %v", c)
		}
		time.Sleep(250 * time.Millisecond)
	}
}

// TestDecommissionSurvivesRestartAndCancel: the draining flag is adopted
// from the node's own registry row across a restart, and --cancel clears it.
func TestDecommissionSurvivesRestartAndCancel(t *testing.T) {
	dirs := []string{t.TempDir(), t.TempDir(), t.TempDir()}
	tc := &TestCluster{T: t}
	n1 := startDiskNode(t, dirs[0], true, "")
	tc.Nodes = append(tc.Nodes, n1)
	tc.Nodes = append(tc.Nodes, startDiskNode(t, dirs[1], false, n1.Addr()))
	tc.Nodes = append(tc.Nodes, startDiskNode(t, dirs[2], false, n1.Addr()))
	defer tc.StopAll()
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()
	if err := tc.waitForReplication(ctx, 3, ""); err != nil {
		t.Fatal(err)
	}

	// Mark node 2 draining (no spare exists, so it just holds the flag).
	resp := adminCall(t, ctx, n1.Addr(), cluster.AdminRequest{Op: "decommission", NodeID: 2})
	if resp.Error != "" || !resp.Draining {
		t.Fatalf("decommission response: %+v", resp)
	}

	// Restart node 2 and wait for a post-restart heartbeat that still
	// carries the flag (adopted from its own row).
	restartAt := time.Now().UnixNano()
	tc.StopNode(1)
	tc.Nodes[1] = startDiskNode(t, dirs[1], false, "")
	deadline := time.Now().Add(30 * time.Second)
	for {
		raw, err := n1.DB().Get(ctx, keys.NodeRegistryKey(2))
		if err == nil && raw != nil {
			var nd struct {
				LivenessTime int64 `json:"liveness_time"`
				Draining     bool  `json:"draining"`
			}
			if jsonUnmarshal(raw, &nd) == nil && nd.LivenessTime > restartAt && nd.Draining {
				break
			}
		}
		if time.Now().After(deadline) {
			t.Fatal("draining flag did not survive the restart")
		}
		time.Sleep(300 * time.Millisecond)
	}

	// Cancel clears it (forwarded to the node, re-asserted off).
	resp = adminCall(t, ctx, n1.Addr(), cluster.AdminRequest{Op: "decommission", NodeID: 2, Cancel: true})
	if resp.Error != "" || resp.Draining {
		t.Fatalf("cancel response: %+v", resp)
	}
	deadline = time.Now().Add(30 * time.Second)
	for {
		raw, err := n1.DB().Get(ctx, keys.NodeRegistryKey(2))
		if err == nil && raw != nil {
			var nd struct {
				Draining bool `json:"draining"`
			}
			if jsonUnmarshal(raw, &nd) == nil && !nd.Draining {
				return
			}
		}
		if time.Now().After(deadline) {
			t.Fatal("cancel never cleared the draining flag")
		}
		time.Sleep(300 * time.Millisecond)
	}
}
