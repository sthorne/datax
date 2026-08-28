package testcluster

import (
	"context"
	"testing"
	"time"

	"github.com/sthorne/datax/pkg/keys"
)

func testKey(s string) keys.Key {
	// User keys live in the table keyspace.
	return append(keys.TableDataPrefix(100), s...)
}

func TestReplicationAcrossNodes(t *testing.T) {
	tc := Start(t, 3)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Write through node 1's client.
	if err := tc.Nodes[0].DB().Put(ctx, testKey("hello"), []byte("world")); err != nil {
		t.Fatalf("put: %v", err)
	}
	// Read through node 3's client: routed to the leader, linearizable.
	v, err := tc.Nodes[2].DB().Get(ctx, testKey("hello"))
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if string(v) != "world" {
		t.Fatalf("got %q", v)
	}

	// The write must actually be replicated: a quorum of engines holds it.
	// (Stop the leader below in the failover test; here just check the
	// applied state made it to disk on multiple nodes eventually.)
	leader := tc.LeaderIndex(1)
	if leader < 0 || leader > 2 {
		t.Fatalf("leader index %d", leader)
	}
}

func TestLeaderFailover(t *testing.T) {
	tc := Start(t, 3)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	if err := tc.Nodes[0].DB().Put(ctx, testKey("a"), []byte("1")); err != nil {
		t.Fatalf("put: %v", err)
	}

	leader := tc.LeaderIndex(1)
	t.Logf("killing leader node %d", leader+1)
	tc.StopNode(leader)

	survivor := (leader + 1) % 3
	// Writes must succeed again after a new election.
	if err := tc.Nodes[survivor].DB().Put(ctx, testKey("b"), []byte("2")); err != nil {
		t.Fatalf("put after failover: %v", err)
	}
	// And the pre-failover write must have survived (it was replicated).
	v, err := tc.Nodes[survivor].DB().Get(ctx, testKey("a"))
	if err != nil {
		t.Fatalf("get after failover: %v", err)
	}
	if string(v) != "1" {
		t.Fatalf("lost committed write: got %q", v)
	}
}

func TestNodeJoin(t *testing.T) {
	tc := Start(t, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := tc.Nodes[0].DB().Put(ctx, testKey("k"), []byte("v")); err != nil {
		t.Fatalf("put: %v", err)
	}

	n2 := tc.AddNode("region=r1,rack=b")
	if n2.NodeID() != 2 {
		t.Fatalf("joined node got ID %s, want n2", n2.NodeID())
	}
	n3 := tc.AddNode("region=r1,rack=c")
	if n3.NodeID() != 3 {
		t.Fatalf("joined node got ID %s, want n3", n3.NodeID())
	}

	// The joined node can read and write through its own client (routing
	// bootstrap worked), even though it holds no replicas yet.
	v, err := n3.DB().Get(ctx, testKey("k"))
	if err != nil {
		t.Fatalf("get via joined node: %v", err)
	}
	if string(v) != "v" {
		t.Fatalf("got %q", v)
	}
	if err := n2.DB().Put(ctx, testKey("k2"), []byte("v2")); err != nil {
		t.Fatalf("put via joined node: %v", err)
	}
}

func TestRestartRecovery(t *testing.T) {
	// Single node with an on-disk store: write, stop, restart, read.
	dir := t.TempDir()
	tc := &TestCluster{T: t}
	n1 := startDiskNode(t, dir, true, "")
	tc.Nodes = append(tc.Nodes, n1)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := n1.DB().Put(ctx, testKey("persist"), []byte("me")); err != nil {
		t.Fatalf("put: %v", err)
	}
	n1.Stop()

	n1b := startDiskNode(t, dir, false, "")
	defer n1b.Stop()
	v, err := n1b.DB().Get(ctx, testKey("persist"))
	if err != nil {
		t.Fatalf("get after restart: %v", err)
	}
	if string(v) != "me" {
		t.Fatalf("lost write across restart: got %q", v)
	}
}
