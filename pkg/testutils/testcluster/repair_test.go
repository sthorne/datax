package testcluster

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/sthorne/datax/pkg/base"
	"github.com/sthorne/datax/pkg/cluster"
	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/kvpb"
	"github.com/sthorne/datax/pkg/server"
)

const (
	repairTestGrace     = 5 * time.Second
	repairTestThreshold = 8 * time.Second
)

// startRepairCluster brings up numSeed static-membership nodes (each holding
// range 1) plus numSpare joined nodes (holding nothing), all with
// time-compressed repair knobs.
func startRepairCluster(t *testing.T, numSeed, numSpare int) *TestCluster {
	t.Helper()
	clusterID := uuid.New()
	listeners := make([]net.Listener, numSeed)
	nodeIDs := make([]base.NodeID, numSeed)
	var nodeDescs []kvpb.NodeDescriptor
	for i := 0; i < numSeed; i++ {
		lis, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		listeners[i] = lis
		nodeIDs[i] = base.NodeID(i + 1)
		nodeDescs = append(nodeDescs, kvpb.NodeDescriptor{
			NodeID: nodeIDs[i], Address: lis.Addr().String(), LivenessTime: time.Now().UnixNano(),
		})
	}
	range1 := cluster.Range1Descriptor(nodeIDs)
	tc := &TestCluster{T: t}
	for i := 0; i < numSeed; i++ {
		n, err := server.Start(server.Config{
			Listener:              listeners[i],
			UpreplicationInterval: 500 * time.Millisecond,
			LivenessGrace:         repairTestGrace,
			DeadNodeThreshold:     repairTestThreshold,
			StaticBootstrap: &server.StaticBootstrap{
				ClusterID: clusterID, NodeID: nodeIDs[i], Range1: range1, Nodes: nodeDescs,
			},
		})
		if err != nil {
			t.Fatalf("starting node %d: %v", i+1, err)
		}
		tc.Nodes = append(tc.Nodes, n)
	}
	t.Cleanup(tc.StopAll)
	for i := 0; i < numSpare; i++ {
		lis, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		n, err := server.Start(server.Config{
			Listener:              lis,
			Join:                  tc.Nodes[0].Addr(),
			UpreplicationInterval: 500 * time.Millisecond,
			LivenessGrace:         repairTestGrace,
			DeadNodeThreshold:     repairTestThreshold,
		})
		if err != nil {
			t.Fatalf("joining spare node: %v", err)
		}
		tc.Nodes = append(tc.Nodes, n)
	}
	return tc
}

// TestDeadNodeRepair: a 3-replica range on a 4-node cluster loses a node;
// after the dead-node threshold its replica is rebuilt on the spare node
// (add-then-remove), and no churn happens before the threshold.
func TestDeadNodeRepair(t *testing.T) {
	tc := startRepairCluster(t, 3, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	k := append(keys.TableDataPrefix(730), "k"...)
	if err := tc.Nodes[0].DB().Put(ctx, k, []byte("v")); err != nil {
		t.Fatal(err)
	}

	leader := tc.LeaderIndex(1)
	victim := (leader + 1) % 3
	victimID := base.NodeID(victim + 1)
	tc.StopNode(victim)

	// Within the grace window: no churn.
	time.Sleep(3 * time.Second)
	descs, err := tc.ranges(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, has := descs[0].GetReplica(victimID); !has || len(descs[0].Replicas) != 3 {
		t.Fatalf("premature repair before the dead-node threshold: %+v", descs[0].Replicas)
	}

	// After the threshold: the dead replica is replaced by the spare.
	deadline := time.Now().Add(60 * time.Second)
	for {
		descs, err := tc.ranges(ctx)
		if err == nil && len(descs) > 0 {
			d := descs[0]
			_, hasVictim := d.GetReplica(victimID)
			_, hasSpare := d.GetReplica(4)
			if !hasVictim && hasSpare && len(d.Replicas) == 3 {
				break
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("dead replica never repaired: %+v (err %v)", descs, err)
		}
		time.Sleep(200 * time.Millisecond)
	}

	// The spare's replica is real: it can serve once caught up, and data
	// survives.
	if _, ok := tc.Nodes[3].Store().GetReplica(1); !ok {
		t.Fatal("spare node has no replica of range 1 after repair")
	}
	if v, err := tc.Nodes[3].DB().Get(ctx, k); err != nil || string(v) != "v" {
		t.Fatalf("read after repair: %q, %v", v, err)
	}
	if err := tc.Nodes[3].DB().Put(ctx, k, []byte("v2")); err != nil {
		t.Fatalf("write after repair: %v", err)
	}
}

// TestDeadNodeRepairNoSpare: with no live node to repair onto, the loop
// idles safely — membership is untouched and the surviving quorum keeps
// serving.
func TestDeadNodeRepairNoSpare(t *testing.T) {
	tc := startRepairCluster(t, 3, 0)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	k := append(keys.TableDataPrefix(731), "k"...)
	if err := tc.Nodes[0].DB().Put(ctx, k, []byte("v")); err != nil {
		t.Fatal(err)
	}
	leader := tc.LeaderIndex(1)
	victim := (leader + 1) % 3
	victimID := base.NodeID(victim + 1)
	tc.StopNode(victim)

	// Well past the threshold plus several repair ticks.
	time.Sleep(repairTestThreshold + 4*time.Second)
	descs, err := tc.ranges(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, has := descs[0].GetReplica(victimID); !has || len(descs[0].Replicas) != 3 {
		t.Fatalf("membership changed with no spare available: %+v", descs[0].Replicas)
	}
	if v, err := tc.Nodes[leader].DB().Get(ctx, k); err != nil || string(v) != "v" {
		t.Fatalf("read with a dead minority: %q, %v", v, err)
	}
}
