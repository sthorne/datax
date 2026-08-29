package testcluster

import (
	"context"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/sthorne/datax/pkg/base"
	"github.com/sthorne/datax/pkg/cluster"
	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/kvclient"
	"github.com/sthorne/datax/pkg/kvpb"
	"github.com/sthorne/datax/pkg/kvserver"
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

// TestLeaseBackstopRefusesStaleContact: once follower contact is
// invalidated (as a detected stall would), the leader refuses lease reads
// with NotLeader instead of serving on a possibly-expired lease — and
// recovers as soon as followers answer again. Regression test for issue
// #17.
func TestLeaseBackstopRefusesStaleContact(t *testing.T) {
	tc := startCluster3(t, false)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	k := append(keys.TableDataPrefix(743), "k"...)
	if err := tc.Nodes[0].DB().Put(ctx, k, []byte("v")); err != nil {
		t.Fatal(err)
	}
	leader := tc.LeaderIndex(1)
	rep, _ := tc.Nodes[leader].Store().GetReplica(1)

	// Expiring contact and reading immediately must yield NotLeader (a
	// heartbeat response can occasionally sneak into the microseconds
	// between the two calls, so accept any refusal across attempts).
	sawRefusal := false
	for i := 0; i < 20 && !sawRefusal; i++ {
		rep.TestingExpireLeaseContact()
		ba := &kvpb.BatchRequest{Header: kvpb.BatchHeader{RangeID: 1, Timestamp: tc.Nodes[leader].Clock().Now()}}
		ba.Add(&kvpb.GetRequest{RequestHeader: kvpb.RequestHeader{Key: k}})
		_, kerr := rep.Execute(ctx, ba)
		if kerr != nil && kerr.NotLeader != nil {
			sawRefusal = true
		}
	}
	if !sawRefusal {
		t.Fatal("stale-contact lease read was never refused")
	}

	// The refusal is transient: heartbeat responses restore contact and a
	// routed read succeeds.
	if v, err := tc.Nodes[leader].DB().Get(ctx, k); err != nil || string(v) != "v" {
		t.Fatalf("read after contact recovery: %q, %v", v, err)
	}
}

// TestLeaseBackstopPostEvalCheck: contact expiring DURING evaluation (a
// stall mid-read) is caught by the re-check before results are returned.
func TestLeaseBackstopPostEvalCheck(t *testing.T) {
	var expire atomic.Bool
	var target atomic.Pointer[kvserver.Replica]
	marker := append(keys.TableDataPrefix(744), "marker"...)
	knobs := kvserver.TestingKnobs{
		BeforeReadReturn: func(ba *kvpb.BatchRequest) {
			if !expire.Load() || len(ba.Requests) != 1 {
				return
			}
			if get := ba.Requests[0].Get; get != nil && get.Key.Equal(marker) {
				if r := target.Load(); r != nil {
					r.TestingExpireLeaseContact()
				}
			}
		},
	}
	tc, _ := StartWithEngines(t, 3, func(c *server.Config) { c.TestingKnobs = knobs })
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	if err := tc.Nodes[0].DB().Put(ctx, marker, []byte("v")); err != nil {
		t.Fatal(err)
	}
	leader := tc.LeaderIndex(1)
	rep, _ := tc.Nodes[leader].Store().GetReplica(1)
	target.Store(rep)
	expire.Store(true)

	// The pre-eval check passes (contact is fresh), the knob expires it
	// after evaluation, and the post-eval re-check must refuse.
	ba := &kvpb.BatchRequest{Header: kvpb.BatchHeader{RangeID: 1, Timestamp: tc.Nodes[leader].Clock().Now()}}
	ba.Add(&kvpb.GetRequest{RequestHeader: kvpb.RequestHeader{Key: marker}})
	_, kerr := rep.Execute(ctx, ba)
	if kerr == nil || kerr.NotLeader == nil {
		t.Fatalf("mid-read stall not caught by the post-eval check: %v", kerr)
	}
	expire.Store(false)
	if v, err := tc.Nodes[leader].DB().Get(ctx, marker); err != nil || string(v) != "v" {
		t.Fatalf("read after recovery: %q, %v", v, err)
	}
}

// TestLeaseBackstopSingleNodeExempt: a single-replica range is its own
// quorum; expired contact must not refuse its reads.
func TestLeaseBackstopSingleNodeExempt(t *testing.T) {
	n, _ := startGCNode(t) // single node, lease reads on by default
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	k := append(keys.TableDataPrefix(745), "k"...)
	if err := n.DB().Put(ctx, k, []byte("v")); err != nil {
		t.Fatal(err)
	}
	rep, _ := n.Store().GetReplica(1)
	rep.TestingExpireLeaseContact()
	ba := &kvpb.BatchRequest{Header: kvpb.BatchHeader{RangeID: 1, Timestamp: n.Clock().Now()}}
	ba.Add(&kvpb.GetRequest{RequestHeader: kvpb.RequestHeader{Key: k}})
	if _, kerr := rep.Execute(ctx, ba); kerr != nil {
		t.Fatalf("single-node read refused: %v", kerr)
	}
}
