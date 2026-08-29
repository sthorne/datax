package testcluster

import (
	"context"
	"fmt"
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

// startRebalanceCluster: numSeed static nodes (each holding range 1) plus
// spares joined empty, with per-node localities and time-compressed knobs.
// localities[i] applies to node i+1 across seeds then spares.
func startRebalanceCluster(t *testing.T, numSeed, numSpare int, localities []string) *TestCluster {
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
			NodeID: nodeIDs[i], Address: lis.Addr().String(),
			Locality: locality(t, localities, i), LivenessTime: time.Now().UnixNano(),
		})
	}
	range1 := cluster.Range1Descriptor(nodeIDs)
	tc := &TestCluster{T: t}
	for i := 0; i < numSeed; i++ {
		n, err := server.Start(server.Config{
			Listener:              listeners[i],
			Locality:              locality(t, localities, i),
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
			Locality:              locality(t, localities, numSeed+i),
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

// rangeCounts tallies replicas per node from the /meta records.
func (tc *TestCluster) rangeCounts(ctx context.Context) (map[base.NodeID]int, []kvpb.RangeDescriptor, error) {
	descs, err := tc.ranges(ctx)
	if err != nil {
		return nil, nil, err
	}
	counts := map[base.NodeID]int{}
	for _, n := range tc.Nodes {
		if n != nil {
			counts[n.NodeID()] = 0
		}
	}
	for _, d := range descs {
		for _, r := range d.Replicas {
			counts[r.NodeID]++
		}
	}
	return counts, descs, nil
}

func spread(counts map[base.NodeID]int) int {
	first := true
	var lo, hi int
	for _, c := range counts {
		if first {
			lo, hi, first = c, c, false
			continue
		}
		if c < lo {
			lo = c
		}
		if c > hi {
			hi = c
		}
	}
	return hi - lo
}

// TestAutoRebalance: two empty nodes join a loaded 3-node cluster; the
// allocator converges range counts to a spread of at most 1, then goes
// quiet — zero descriptor changes once balanced. Regression test for
// issue #2.
func TestAutoRebalance(t *testing.T) {
	tc := startRebalanceCluster(t, 3, 2, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()

	// 6 ranges total, all on the three seed nodes.
	for i := 0; i < 5; i++ {
		if _, err := tc.Nodes[0].DB().AdminSplit(ctx, keys.TableDataPrefix(uint64(810+i))); err != nil {
			t.Fatal(err)
		}
	}

	deadline := time.Now().Add(90 * time.Second)
	for {
		counts, descs, err := tc.rangeCounts(ctx)
		if err == nil && len(descs) == 6 && spread(counts) <= 1 {
			break
		}
		if time.Now().After(deadline) {
			c, _, _ := tc.rangeCounts(ctx)
			t.Fatalf("never balanced: %v", c)
		}
		time.Sleep(250 * time.Millisecond)
	}

	// Hysteresis: once balanced, nothing moves. Compare generations across
	// several allocator ticks.
	_, before, err := tc.rangeCounts(ctx)
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(3 * time.Second) // ~6 ticks
	_, after, err := tc.rangeCounts(ctx)
	if err != nil {
		t.Fatal(err)
	}
	gen := func(descs []kvpb.RangeDescriptor) map[base.RangeID]int64 {
		m := map[base.RangeID]int64{}
		for _, d := range descs {
			m[d.RangeID] = d.Generation
		}
		return m
	}
	gb, ga := gen(before), gen(after)
	for id, g := range gb {
		if ga[id] != g {
			t.Fatalf("churn after balance: range %d generation %d -> %d", id, g, ga[id])
		}
	}

	// The data plane still works everywhere.
	k := append(keys.TableDataPrefix(812), "probe"...)
	if err := tc.Nodes[4].DB().Put(ctx, k, []byte("v")); err != nil {
		t.Fatal(err)
	}
	if v, err := tc.Nodes[3].DB().Get(ctx, k); err != nil || string(v) != "v" {
		t.Fatalf("probe read: %q, %v", v, err)
	}
}

// TestRebalanceRespectsDiversity: a spare that duplicates an existing rack
// only ever receives replicas from its own rack's node — count pressure
// never trades away one-replica-per-rack placement.
func TestRebalanceRespectsDiversity(t *testing.T) {
	tc := startRebalanceCluster(t, 3, 1, []string{"rack=a", "rack=b", "rack=c", "rack=b"})
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()

	for i := 0; i < 2; i++ {
		if _, err := tc.Nodes[0].DB().AdminSplit(ctx, keys.TableDataPrefix(uint64(820+i))); err != nil {
			t.Fatal(err)
		}
	}
	// Counts start (3,3,3,0) with spread 3: the only diversity-preserving
	// donations are rack-b -> rack-b, so exactly one range moves n2 -> n4
	// and the allocator then holds at (3,2,3,1).
	deadline := time.Now().Add(60 * time.Second)
	for {
		counts, _, err := tc.rangeCounts(ctx)
		if err == nil && counts[4] == 1 {
			break
		}
		if time.Now().After(deadline) {
			c, _, _ := tc.rangeCounts(ctx)
			t.Fatalf("spare never received its one legal replica: %v", c)
		}
		time.Sleep(250 * time.Millisecond)
	}
	time.Sleep(3 * time.Second) // let any illegal move show up
	counts, descs, err := tc.rangeCounts(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if counts[1] != 3 || counts[3] != 3 || counts[2] != 2 || counts[4] != 1 {
		t.Fatalf("counts %v, want n1=3 n2=2 n3=3 n4=1", counts)
	}
	// Every range still has one replica per rack.
	rackOf := map[base.NodeID]string{1: "a", 2: "b", 3: "c", 4: "b"}
	for _, d := range descs {
		seen := map[string]int{}
		for _, r := range d.Replicas {
			seen[rackOf[r.NodeID]]++
		}
		if seen["a"] != 1 || seen["b"] != 1 || seen["c"] != 1 {
			t.Fatalf("range %d lost rack diversity: %v", d.RangeID, fmt.Sprint(seen))
		}
	}
}
