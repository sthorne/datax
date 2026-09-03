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
	T     testing.TB
	Nodes []*server.Node
	// addrs records each node's RPC address so RestartNode can re-listen on
	// it (peers find a restarted node through their persisted registries,
	// which hold the old address). Filled by StartWithEngines.
	addrs []string
	// nodesMu orders Nodes slot writes (StopNode/Restart*) against
	// concurrent readers using Node(i) — workloads that keep running while
	// the test restarts nodes.
	nodesMu sync.RWMutex
}

// Node returns tc.Nodes[i] under the lock that StopNode and the Restart
// helpers hold while writing the slot — the race-free accessor for
// workload goroutines running across a restart. May return nil (node
// currently down).
func (tc *TestCluster) Node(i int) *server.Node {
	tc.nodesMu.RLock()
	defer tc.nodesMu.RUnlock()
	return tc.Nodes[i]
}

func (tc *TestCluster) setNode(i int, n *server.Node) {
	tc.nodesMu.Lock()
	tc.Nodes[i] = n
	tc.nodesMu.Unlock()
}

// Start brings up numNodes nodes with static pre-agreed membership: every
// node holds a replica of range 1 from the start. localities[i] (optional)
// is parsed as a locality string for node i+1.
func Start(t testing.TB, numNodes int, localities ...string) *TestCluster {
	t.Helper()
	return startCluster(t, numNodes, localities, nil)
}

// StartWithOptions is Start with a hook applied to every node's config
// before it starts (housekeeping cadence, thresholds, knobs).
func StartWithOptions(t testing.TB, numNodes int, opt func(*server.Config), localities ...string) *TestCluster {
	t.Helper()
	return startCluster(t, numNodes, localities, opt)
}

func startCluster(t testing.TB, numNodes int, localities []string, opt func(*server.Config)) *TestCluster {
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
		cfg := server.Config{
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
		}
		if opt != nil {
			opt(&cfg)
		}
		n, err := server.Start(cfg)
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
func StartWithEngines(t testing.TB, numNodes int, opts ...func(*server.Config)) (*TestCluster, []*storage.Engine) {
	t.Helper()
	return StartWithEngineOptions(t, numNodes, storage.Options{}, opts...)
}

// StartWithEngineOptions is StartWithEngines with explicit storage options
// for the injected in-memory engines (profile, encryption).
func StartWithEngineOptions(t testing.TB, numNodes int, storageOpts storage.Options, opts ...func(*server.Config)) (*TestCluster, []*storage.Engine) {
	t.Helper()
	clusterID := uuid.New()
	engines := make([]*storage.Engine, numNodes)
	listeners := make([]net.Listener, numNodes)
	nodeIDs := make([]base.NodeID, numNodes)
	var nodeDescs []kvpb.NodeDescriptor
	for i := 0; i < numNodes; i++ {
		eng, err := storage.Open("", storageOpts)
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
	for _, lis := range listeners {
		tc.addrs = append(tc.addrs, lis.Addr().String())
	}
	t.Cleanup(func() {
		for _, eng := range engines {
			_ = eng.Close()
		}
	})
	for i := 0; i < numNodes; i++ {
		cfg := server.Config{
			Listener:   listeners[i],
			Engine:     engines[i],
			GCInterval: -1, // no background housekeeping
			StaticBootstrap: &server.StaticBootstrap{
				ClusterID: clusterID, NodeID: nodeIDs[i], Range1: range1, Nodes: nodeDescs,
			},
		}
		for _, opt := range opts {
			opt(&cfg)
		}
		n, err := server.Start(cfg)
		if err != nil {
			t.Fatalf("starting node %d: %v", i+1, err)
		}
		tc.Nodes = append(tc.Nodes, n)
	}
	t.Cleanup(tc.StopAll)
	return tc, engines
}

func locality(t testing.TB, localities []string, i int) base.Locality {
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

// AddNodeErr joins a fresh node through node 1 like AddNode, but takes
// config options and returns the start error instead of failing the test —
// for tests that expect a join rejection (e.g. version gating).
func (tc *TestCluster) AddNodeErr(opts ...func(*server.Config)) (*server.Node, error) {
	tc.T.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		tc.T.Fatal(err)
	}
	cfg := server.Config{
		Listener:              lis,
		Join:                  tc.Nodes[0].Addr(),
		UpreplicationInterval: time.Second,
	}
	for _, opt := range opts {
		opt(&cfg)
	}
	n, err := server.Start(cfg)
	if err != nil {
		_ = lis.Close()
		return nil, err
	}
	tc.Nodes = append(tc.Nodes, n)
	return n, nil
}

// RestartNodeErr is RestartNode returning the start error instead of
// failing the test — for tests that expect a refused restart (e.g. a
// binary downgrade past a finalized upgrade).
func (tc *TestCluster) RestartNodeErr(i int, eng *storage.Engine, opts ...func(*server.Config)) (*server.Node, error) {
	tc.T.Helper()
	if tc.Nodes[i] != nil {
		tc.T.Fatalf("node %d is still running", i+1)
	}
	lis, err := net.Listen("tcp", tc.addrs[i])
	if err != nil {
		tc.T.Fatalf("re-listening on %s: %v", tc.addrs[i], err)
	}
	cfg := server.Config{
		Listener:   lis,
		Engine:     eng,
		GCInterval: -1,
	}
	for _, opt := range opts {
		opt(&cfg)
	}
	n, err := server.Start(cfg)
	if err != nil {
		_ = lis.Close()
		return nil, err
	}
	tc.setNode(i, n)
	return n, nil
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
// The listener address is pinned per store directory: these tests exercise
// the same-address restart path, where peers find a returning node purely
// through their persisted registries with no re-announce involved. The
// changed-address path is covered by RestartNodeNewPort and address_test.go.
func startDiskNode(t testing.TB, dir string, bootstrap bool, join string, opts ...func(*server.Config)) *server.Node {
	t.Helper()
	lis := listenerForDir(t, dir)
	cfg := server.Config{
		Dir:           dir,
		Listener:      lis,
		BootstrapSelf: bootstrap,
		Join:          join,
	}
	for _, opt := range opts {
		opt(&cfg)
	}
	n, err := server.Start(cfg)
	if err != nil {
		t.Fatalf("starting disk node: %v", err)
	}
	return n
}

var diskAddrs sync.Map // dir -> address, so restarts reuse their port

func listenerForDir(t testing.TB, dir string) net.Listener {
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

// RestartNode restarts a stopped StartWithEngines node on its retained
// engine and original address (a crash-and-return, not a wipe).
func (tc *TestCluster) RestartNode(i int, eng *storage.Engine, opts ...func(*server.Config)) *server.Node {
	tc.T.Helper()
	if tc.Nodes[i] != nil {
		tc.T.Fatalf("node %d is still running", i+1)
	}
	lis, err := net.Listen("tcp", tc.addrs[i])
	if err != nil {
		tc.T.Fatalf("re-listening on %s: %v", tc.addrs[i], err)
	}
	cfg := server.Config{
		Listener:   lis,
		Engine:     eng,
		GCInterval: -1,
	}
	for _, opt := range opts {
		opt(&cfg)
	}
	n, err := server.Start(cfg)
	if err != nil {
		tc.T.Fatalf("restarting node %d: %v", i+1, err)
	}
	tc.setNode(i, n)
	return n
}

// RestartNodeNewPort restarts a stopped StartWithEngines node on a FRESH
// port (an address change: rescheduling, port churn). join is the announce
// target for the restarted node ("" = rely on its persisted registry).
func (tc *TestCluster) RestartNodeNewPort(i int, eng *storage.Engine, join string, opts ...func(*server.Config)) *server.Node {
	tc.T.Helper()
	if tc.Nodes[i] != nil {
		tc.T.Fatalf("node %d is still running", i+1)
	}
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		tc.T.Fatal(err)
	}
	cfg := server.Config{
		Listener:   lis,
		Engine:     eng,
		Join:       join,
		GCInterval: -1,
	}
	for _, opt := range opts {
		opt(&cfg)
	}
	n, err := server.Start(cfg)
	if err != nil {
		tc.T.Fatalf("restarting node %d on new port: %v", i+1, err)
	}
	tc.addrs[i] = lis.Addr().String()
	tc.setNode(i, n)
	return n
}

// StopNode stops one node (simulating a crash) and forgets it.
func (tc *TestCluster) StopNode(i int) {
	if n := tc.Node(i); n != nil {
		n.Stop()
		tc.setNode(i, nil)
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

// Isolate partitions node i away from every other node, both directions —
// its outbound traffic is vetoed and every peer vetoes traffic to it.
func (tc *TestCluster) Isolate(i int) {
	target := base.NodeID(i + 1)
	for j, n := range tc.Nodes {
		if n == nil {
			continue
		}
		if j == i {
			n.InjectRPCDrop(func(base.NodeID) bool { return true })
		} else {
			n.InjectRPCDrop(func(to base.NodeID) bool { return to == target })
		}
	}
}

// Heal clears all injected partitions.
func (tc *TestCluster) Heal() {
	for _, n := range tc.Nodes {
		if n != nil {
			n.InjectRPCDrop(nil)
		}
	}
}
