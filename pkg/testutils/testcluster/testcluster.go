// Package testcluster starts multi-node in-process datax clusters for
// integration tests: real gRPC on loopback ports, in-memory Pebble engines.
package testcluster

import (
	"encoding/json"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/sthorne/datax/pkg/base"
	"github.com/sthorne/datax/pkg/cluster"
	"github.com/sthorne/datax/pkg/kvpb"
	"github.com/sthorne/datax/pkg/server"
	"github.com/sthorne/datax/pkg/storage"
)

// TestCluster is a set of in-process nodes sharing one cluster.
type TestCluster struct {
	T     *testing.T
	Nodes []*server.Node
}

// Start brings up numNodes nodes with static pre-agreed membership: every
// node holds a replica of range 1 from the start. localities[i] (optional)
// is parsed as a locality string for node i+1.
func Start(t *testing.T, numNodes int, localities ...string) *TestCluster {
	t.Helper()
	clusterID := uuid.New()

	listeners := make([]net.Listener, numNodes)
	nodeIDs := make([]base.NodeID, numNodes)
	var nodes []kvpb.NodeDescriptor
	for i := 0; i < numNodes; i++ {
		lis, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		listeners[i] = lis
		nodeIDs[i] = base.NodeID(i + 1)
		loc := locality(t, localities, i)
		nodes = append(nodes, kvpb.NodeDescriptor{
			NodeID:       nodeIDs[i],
			Address:      lis.Addr().String(),
			Locality:     loc,
			LivenessTime: time.Now().UnixNano(),
		})
	}
	range1 := cluster.Range1Descriptor(nodeIDs)

	tc := &TestCluster{T: t}
	for i := 0; i < numNodes; i++ {
		pglis, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		n, err := server.Start(server.Config{
			Listener:              listeners[i],
			PGListener:            pglis,
			Locality:              locality(t, localities, i),
			UpreplicationInterval: time.Second,
			StaticBootstrap: &server.StaticBootstrap{
				ClusterID: clusterID,
				NodeID:    nodeIDs[i],
				Range1:    range1,
				Nodes:     nodes,
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

// StartWithEngines brings up numNodes static-membership nodes over injected
// in-memory engines, so tests can inspect raw storage. The background
// housekeeping loop is disabled; tests drive GC/truncation explicitly.
func StartWithEngines(t *testing.T, numNodes int) (*TestCluster, []*storage.Engine) {
	t.Helper()
	clusterID := uuid.New()
	engines := make([]*storage.Engine, numNodes)
	listeners := make([]net.Listener, numNodes)
	nodeIDs := make([]base.NodeID, numNodes)
	var nodeDescs []kvpb.NodeDescriptor
	for i := 0; i < numNodes; i++ {
		eng, err := storage.Open("")
		if err != nil {
			t.Fatal(err)
		}
		engines[i] = eng
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
	t.Cleanup(func() {
		for _, eng := range engines {
			_ = eng.Close()
		}
	})
	for i := 0; i < numNodes; i++ {
		n, err := server.Start(server.Config{
			Listener:   listeners[i],
			Engine:     engines[i],
			GCInterval: -1, // no background housekeeping
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
	return tc, engines
}

func locality(t *testing.T, localities []string, i int) base.Locality {
	if i >= len(localities) || localities[i] == "" {
		return base.Locality{}
	}
	loc, err := base.ParseLocality(localities[i])
	if err != nil {
		t.Fatal(err)
	}
	return loc
}

// AddNode joins a fresh node to the cluster through node 1.
func (tc *TestCluster) AddNode(localityStr string) *server.Node {
	tc.T.Helper()
	var loc base.Locality
	if localityStr != "" {
		var err error
		loc, err = base.ParseLocality(localityStr)
		if err != nil {
			tc.T.Fatal(err)
		}
	}
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		tc.T.Fatal(err)
	}
	pglis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		tc.T.Fatal(err)
	}
	n, err := server.Start(server.Config{
		Listener:              lis,
		PGListener:            pglis,
		Join:                  tc.Nodes[0].Addr(),
		Locality:              loc,
		UpreplicationInterval: time.Second,
	})
	if err != nil {
		tc.T.Fatalf("joining node: %v", err)
	}
	tc.Nodes = append(tc.Nodes, n)
	return n
}

// LeaderIndex returns the index of the node whose replica of rangeID is the
// Raft leader, waiting up to 15s for one to emerge.
func (tc *TestCluster) LeaderIndex(rangeID base.RangeID) int {
	tc.T.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		for i, n := range tc.Nodes {
			if n == nil {
				continue
			}
			if r, ok := n.Store().GetReplica(rangeID); ok && r.IsLeader() {
				return i
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	tc.T.Fatalf("no leader emerged for %s", rangeID)
	return -1
}

// startDiskNode starts a node backed by an on-disk store (restart tests).
// A restarted node must come back on its previous address — peers find it
// through the persisted registry — so the listener address is reused via
// the store directory name mapping kept by the caller (here: we bind :0 the
// first time and the SAME port after, since datax has no address-change
// story in v1).
func startDiskNode(t *testing.T, dir string, bootstrap bool, join string) *server.Node {
	t.Helper()
	lis := listenerForDir(t, dir)
	n, err := server.Start(server.Config{
		Dir:           dir,
		Listener:      lis,
		BootstrapSelf: bootstrap,
		Join:          join,
	})
	if err != nil {
		t.Fatalf("starting disk node: %v", err)
	}
	return n
}

var diskAddrs sync.Map // dir -> address, so restarts reuse their port

func listenerForDir(t *testing.T, dir string) net.Listener {
	t.Helper()
	addr := "127.0.0.1:0"
	if prev, ok := diskAddrs.Load(dir); ok {
		addr = prev.(string)
	}
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	diskAddrs.Store(dir, lis.Addr().String())
	return lis
}

// StopNode stops one node (simulating a crash) and forgets it.
func (tc *TestCluster) StopNode(i int) {
	if tc.Nodes[i] != nil {
		tc.Nodes[i].Stop()
		tc.Nodes[i] = nil
	}
}

// StopAll stops every remaining node.
func (tc *TestCluster) StopAll() {
	for i := range tc.Nodes {
		tc.StopNode(i)
	}
}

// jsonUnmarshal avoids importing encoding/json in every test file.
func jsonUnmarshal(data []byte, v any) error { return json.Unmarshal(data, v) }
