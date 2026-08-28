package testcluster

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/sthorne/datax/pkg/base"
	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/kvpb"
)

// ranges lists all range descriptors via any live node's meta records.
func (tc *TestCluster) ranges(ctx context.Context) ([]kvpb.RangeDescriptor, error) {
	for _, n := range tc.Nodes {
		if n == nil {
			continue
		}
		start, end := keys.MetaSpan()
		rows, err := n.DB().Scan(ctx, start, end, 0)
		if err != nil {
			continue
		}
		var out []kvpb.RangeDescriptor
		for _, kv := range rows {
			var d kvpb.RangeDescriptor
			if err := jsonUnmarshal(kv.Value, &d); err == nil {
				out = append(out, d)
			}
		}
		return out, nil
	}
	return nil, fmt.Errorf("no live node answered")
}

// waitForReplication polls until every range has wantReplicas replicas, all
// on nodes with distinct values for the given locality tier.
func (tc *TestCluster) waitForReplication(ctx context.Context, wantReplicas int, distinctTier string) error {
	deadline := time.Now().Add(60 * time.Second)
	var last string
	for time.Now().Before(deadline) {
		descs, err := tc.ranges(ctx)
		ok := err == nil && len(descs) > 0
		if ok {
			for _, d := range descs {
				if len(d.Replicas) != wantReplicas {
					ok = false
					last = fmt.Sprintf("%s has %d replicas", d.RangeID, len(d.Replicas))
					break
				}
				if distinctTier != "" {
					seen := map[string]bool{}
					for _, r := range d.Replicas {
						nd, found := tc.nodeDesc(r.NodeID)
						if !found {
							ok = false
							last = fmt.Sprintf("no descriptor for n%d", r.NodeID)
							break
						}
						val := tierValue(nd.Locality, distinctTier)
						if seen[val] {
							ok = false
							last = fmt.Sprintf("%s has two replicas in %s=%s", d.RangeID, distinctTier, val)
							break
						}
						seen[val] = true
					}
				}
				if !ok {
					break
				}
			}
			if ok {
				return nil
			}
		}
		time.Sleep(200 * time.Millisecond)
	}
	return fmt.Errorf("replication did not converge: %s", last)
}

func (tc *TestCluster) nodeDesc(id base.NodeID) (kvpb.NodeDescriptor, bool) {
	for _, n := range tc.Nodes {
		if n == nil {
			continue
		}
		if nd, ok := n.Registry().Get(id); ok {
			return nd, true
		}
	}
	return kvpb.NodeDescriptor{}, false
}

func tierValue(l base.Locality, key string) string {
	for _, t := range l.Tiers {
		if t.Key == key {
			return t.Value
		}
	}
	return ""
}

// TestUpreplicationAcrossRacks is the Phase 7 checkpoint: a cluster grows
// from one node to three across racks; every range reaches three replicas
// with one per rack; and the ORIGINAL node can then die without losing data
// — proof the snapshots seeded real, consistent replicas.
func TestUpreplicationAcrossRacks(t *testing.T) {
	tc := Start(t, 1, "region=r1,rack=a")
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()
	db := tc.Nodes[0].DB()

	// Data before the other racks exist: two ranges' worth.
	key := func(s string) keys.Key { return append(keys.TableDataPrefix(500), s...) }
	for i := 0; i < 10; i++ {
		if err := db.Put(ctx, key(fmt.Sprintf("k%02d", i)), []byte(fmt.Sprintf("v%d", i))); err != nil {
			t.Fatalf("put: %v", err)
		}
	}
	if _, err := db.AdminSplit(ctx, key("k05")); err != nil {
		t.Fatalf("split: %v", err)
	}

	tc.AddNode("region=r1,rack=b")
	tc.AddNode("region=r1,rack=c")

	if err := tc.waitForReplication(ctx, 3, "rack"); err != nil {
		t.Fatal(err)
	}

	// The original node dies. Quorum (2 of 3) survives on racks b and c;
	// every key — including pre-upreplication writes that traveled by
	// snapshot — must still be readable and writable.
	tc.StopNode(0)
	db2 := tc.Nodes[1].DB()
	for i := 0; i < 10; i++ {
		v, err := db2.Get(ctx, key(fmt.Sprintf("k%02d", i)))
		if err != nil {
			t.Fatalf("get k%02d after killing n1: %v", i, err)
		}
		if string(v) != fmt.Sprintf("v%d", i) {
			t.Fatalf("k%02d = %q after killing n1", i, v)
		}
	}
	if err := db2.Put(ctx, key("post-failure"), []byte("alive")); err != nil {
		t.Fatalf("write after killing n1: %v", err)
	}
}

// TestManualRebalance moves a replica to a specific node.
func TestManualRebalance(t *testing.T) {
	tc := Start(t, 1, "region=r1,rack=a")
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()

	tc.AddNode("region=r1,rack=b")
	tc.AddNode("region=r1,rack=c")
	tc.AddNode("region=r1,rack=d")
	if err := tc.waitForReplication(ctx, 3, "rack"); err != nil {
		t.Fatal(err)
	}

	descs, err := tc.ranges(ctx)
	if err != nil || len(descs) == 0 {
		t.Fatalf("ranges: %v", err)
	}
	target := descs[0]
	var spare base.NodeID
	for _, n := range tc.Nodes {
		if _, holds := target.GetReplica(n.NodeID()); !holds {
			spare = n.NodeID()
			break
		}
	}
	if spare == 0 {
		t.Fatal("no spare node")
	}

	resp, err := tc.Nodes[0].DB().AdminChangeReplicas(ctx, target.StartKey, spare, 0)
	if err != nil {
		t.Fatalf("add replica: %v", err)
	}
	if len(resp.Desc.Replicas) != 4 {
		t.Fatalf("after add: %v", resp.Desc)
	}
	// Remove one of the originals (not the leader's own — pick any other).
	var victim base.NodeID
	leader := tc.LeaderIndex(target.RangeID)
	leaderID := tc.Nodes[leader].NodeID()
	for _, r := range resp.Desc.Replicas {
		if r.NodeID != spare && r.NodeID != leaderID {
			victim = r.NodeID
			break
		}
	}
	resp, err = tc.Nodes[0].DB().AdminChangeReplicas(ctx, target.StartKey, 0, victim)
	if err != nil {
		t.Fatalf("remove replica: %v", err)
	}
	if len(resp.Desc.Replicas) != 3 {
		t.Fatalf("after remove: %v", resp.Desc)
	}
	if _, holds := resp.Desc.GetReplica(spare); !holds {
		t.Fatal("moved-to node lost its replica")
	}
	if _, holds := resp.Desc.GetReplica(victim); holds {
		t.Fatal("removed node still has a replica")
	}
}
