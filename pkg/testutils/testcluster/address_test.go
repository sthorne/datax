package testcluster

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/sthorne/datax/pkg/base"
	"github.com/sthorne/datax/pkg/keys"
)

// TestRestartOnNewAddress: a node restarts on a DIFFERENT port with no
// --join hint, re-announces itself to its persisted-registry peers, and the
// cluster converges: every registry holds the new address and replication
// to the moved node resumes. Regression test for issue #7 (previously a
// restarted node had to keep its address).
func TestRestartOnNewAddress(t *testing.T) {
	tc, engines := StartWithEngines(t, 3)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	prefix := keys.TableDataPrefix(870)
	put := func(n int, k, v string) {
		t.Helper()
		if err := tc.Nodes[n].DB().Put(ctx, append(prefix.Clone(), k...), []byte(v)); err != nil {
			t.Fatal(err)
		}
	}
	put(0, "k1", "v1")

	// Move a non-leader so writes keep flowing while it is down.
	leader := tc.LeaderIndex(1)
	moved := (leader + 1) % 3
	oldAddr := tc.Nodes[moved].Addr()
	tc.StopNode(moved)
	put(leader, "k2", "v2")

	n := tc.RestartNodeNewPort(moved, engines[moved], "" /* announce via persisted registry */)
	newAddr := n.Addr()
	if newAddr == oldAddr {
		t.Fatalf("restart reused the old address %s", oldAddr)
	}

	// Every live node's registry converges to the new address.
	deadline := time.Now().Add(30 * time.Second)
	for {
		converged := true
		for i, node := range tc.Nodes {
			if i == moved || node == nil {
				continue
			}
			if addr, err := node.Registry().Resolve(n.NodeID()); err != nil || addr != newAddr {
				converged = false
			}
		}
		if converged {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("registries did not adopt the new address %s", newAddr)
		}
		time.Sleep(100 * time.Millisecond)
	}

	// Replication to the moved node resumes: a fresh write reaches its
	// engine (byte-identical data under the prefix on all three).
	put(leader, "k3", "v3")
	deadline = time.Now().Add(30 * time.Second)
	for {
		sums, counts := dataChecksums(t, engines, prefix)
		if counts[0] > 0 && sums[0] == sums[1] && sums[1] == sums[2] {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("moved node did not converge: counts=%v", counts)
		}
		time.Sleep(200 * time.Millisecond)
	}

	// And the moved node itself still serves.
	if v, err := n.DB().Get(ctx, append(prefix.Clone(), "k3"...)); err != nil || !bytes.Equal(v, []byte("v3")) {
		t.Fatalf("read through moved node: %q, %v", v, err)
	}
}

// TestClusterRestartAllNewPorts: the whole cluster stops and every node
// comes back on a fresh port — the cold-start case where persisted
// registries are stale on every node at once. Nodes 2 and 3 are pointed at
// node 1's new address (the operator's static --join config); everything
// else converges via re-announce responses, raft-envelope address
// piggybacking, and registry rows once quorum re-forms.
func TestClusterRestartAllNewPorts(t *testing.T) {
	tc, engines := StartWithEngines(t, 3)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	prefix := keys.TableDataPrefix(871)
	if err := tc.Nodes[0].DB().Put(ctx, append(prefix.Clone(), "pre"...), []byte("survives")); err != nil {
		t.Fatal(err)
	}
	oldAddrs := make([]string, 3)
	for i, n := range tc.Nodes {
		oldAddrs[i] = n.Addr()
	}
	tc.StopAll()

	n1 := tc.RestartNodeNewPort(0, engines[0], "")
	tc.RestartNodeNewPort(1, engines[1], n1.Addr())
	tc.RestartNodeNewPort(2, engines[2], n1.Addr())
	for i, n := range tc.Nodes {
		if n.Addr() == oldAddrs[i] {
			t.Fatalf("node %d reused its old address %s", i+1, oldAddrs[i])
		}
	}

	// The cluster re-forms and serves: the old data is readable and new
	// writes commit.
	leader := tc.LeaderIndex(1)
	if v, err := tc.Nodes[leader].DB().Get(ctx, append(prefix.Clone(), "pre"...)); err != nil || !bytes.Equal(v, []byte("survives")) {
		t.Fatalf("pre-restart data: %q, %v", v, err)
	}
	if err := tc.Nodes[leader].DB().Put(ctx, append(prefix.Clone(), "post"...), []byte("works")); err != nil {
		t.Fatalf("write after full restart: %v", err)
	}

	// Registries converge to the new addresses on every node.
	deadline := time.Now().Add(45 * time.Second)
	for {
		converged := true
		for _, n := range tc.Nodes {
			for j, peer := range tc.Nodes {
				addr, err := n.Registry().Resolve(base.NodeID(j + 1))
				if err != nil || addr != peer.Addr() {
					converged = false
				}
			}
		}
		if converged {
			break
		}
		if time.Now().After(deadline) {
			for _, n := range tc.Nodes {
				for j := 1; j <= 3; j++ {
					addr, _ := n.Registry().Resolve(base.NodeID(j))
					t.Logf("n%s view of n%d: %s (want %s)", n.NodeID(), j, addr, tc.Nodes[j-1].Addr())
				}
			}
			t.Fatal("registries did not converge to the new addresses")
		}
		time.Sleep(200 * time.Millisecond)
	}

	// All three replicas converge byte-identically.
	deadline = time.Now().Add(30 * time.Second)
	for {
		sums, counts := dataChecksums(t, engines, prefix)
		if counts[0] == 2 && sums[0] == sums[1] && sums[1] == sums[2] {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("replicas did not converge after full restart: counts=%v", counts)
		}
		time.Sleep(200 * time.Millisecond)
	}
}
