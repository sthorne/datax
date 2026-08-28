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
	"github.com/sthorne/datax/pkg/kvclient"
	"github.com/sthorne/datax/pkg/kvpb"
	"github.com/sthorne/datax/pkg/server"
)

// startCluster3 starts a 3-node static cluster with the given lease-read
// setting.
func startCluster3(t *testing.T, disableLeaseReads bool) *TestCluster {
	t.Helper()
	clusterID := uuid.New()
	listeners := make([]net.Listener, 3)
	nodeIDs := make([]base.NodeID, 3)
	var nodeDescs []kvpb.NodeDescriptor
	for i := 0; i < 3; i++ {
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
	for i := 0; i < 3; i++ {
		n, err := server.Start(server.Config{
			Listener:          listeners[i],
			DisableLeaseReads: disableLeaseReads,
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
	return tc
}

func exerciseTxns(ctx context.Context, t *testing.T, tc *TestCluster, tableID uint64) {
	t.Helper()
	db := tc.Nodes[0].DB()
	k1 := append(keys.TableDataPrefix(tableID), "a"...)
	k2 := append(keys.TableDataPrefix(tableID), "b"...)
	if err := db.RunTxn(ctx, "setup", func(ctx context.Context, txn *kvclient.Txn) error {
		if err := txn.Put(ctx, k1, []byte("100")); err != nil {
			return err
		}
		return txn.Put(ctx, k2, []byte("0"))
	}); err != nil {
		t.Fatal(err)
	}
	// Transfer: read both, write both.
	if err := db.RunTxn(ctx, "transfer", func(ctx context.Context, txn *kvclient.Txn) error {
		v1, err := txn.Get(ctx, k1)
		if err != nil {
			return err
		}
		if string(v1) != "100" {
			t.Fatalf("read own committed write: got %q", v1)
		}
		if err := txn.Put(ctx, k1, []byte("40")); err != nil {
			return err
		}
		return txn.Put(ctx, k2, []byte("60"))
	}); err != nil {
		t.Fatal(err)
	}
	v1, err1 := db.Get(ctx, k1)
	v2, err2 := db.Get(ctx, k2)
	if err1 != nil || err2 != nil || string(v1) != "40" || string(v2) != "60" {
		t.Fatalf("post-transfer state: %q/%v %q/%v", v1, err1, v2, err2)
	}
}

// TestTxnsWithLeaseReadsOff proves the quorum-ReadIndex path (v1 behavior)
// still works when lease reads are disabled.
func TestTxnsWithLeaseReadsOff(t *testing.T) {
	tc := startCluster3(t, true /* disable lease reads */)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	exerciseTxns(ctx, t, tc, 740)
}

// TestTxnsWithLeaseReadsOn is the same workload on the default (lease-based)
// read path.
func TestTxnsWithLeaseReadsOn(t *testing.T) {
	tc := startCluster3(t, false)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	exerciseTxns(ctx, t, tc, 741)
}

// TestLeaseReadFailover: with lease reads on, killing the leader still
// yields linearizable reads through the new leader — a write accepted after
// failover is visible to a subsequent read.
func TestLeaseReadFailover(t *testing.T) {
	tc := startCluster3(t, false)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	k := append(keys.TableDataPrefix(742), "k"...)
	if err := tc.Nodes[0].DB().Put(ctx, k, []byte("before")); err != nil {
		t.Fatal(err)
	}
	leader := tc.LeaderIndex(1)
	tc.StopNode(leader)
	survivor := (leader + 1) % 3

	// The survivors elect a new leader; writes and reads resume.
	if err := tc.Nodes[survivor].DB().Put(ctx, k, []byte("after")); err != nil {
		t.Fatalf("write after leader death: %v", err)
	}
	v, err := tc.Nodes[survivor].DB().Get(ctx, k)
	if err != nil || string(v) != "after" {
		t.Fatalf("read after failover: %q, %v", v, err)
	}
}
