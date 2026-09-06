package testcluster

import (
	"context"
	"encoding/json"
	"net"
	"testing"
	"time"

	"github.com/sthorne/datax/pkg/server"
)

// The replication section of /api/cluster (issue #152).

type replBucket struct {
	Count     int     `json:"count"`
	Ranges    []int64 `json:"ranges"`
	Truncated bool    `json:"truncated"`
}

type replDomain struct {
	Tier         string `json:"tier"`
	Value        string `json:"value"`
	Nodes        int    `json:"nodes"`
	Live         int    `json:"live_nodes"`
	Replicas     int    `json:"replicas"`
	Leases       int    `json:"leases"`
	LosesQuorum  int    `json:"loses_quorum"`
	BareMajority int    `json:"bare_majority"`
}

type replSection struct {
	Healthy       replBucket   `json:"healthy"`
	Under         replBucket   `json:"under_replicated"`
	Over          replBucket   `json:"over_replicated"`
	NoQuorum      replBucket   `json:"no_quorum"`
	Undiverse     replBucket   `json:"undiverse"`
	Domains       []replDomain `json:"domains"`
	Tiers         []string     `json:"tiers"`
	DefaultFactor int          `json:"default_replication_factor"`
}

func clusterReplication(t *testing.T, addr string) replSection {
	t.Helper()
	code, _, body := httpGet(t, "http://"+addr+"/api/cluster")
	if code != 200 {
		t.Fatalf("/api/cluster: %d", code)
	}
	var doc struct {
		Replication replSection `json:"replication"`
	}
	if err := json.Unmarshal([]byte(body), &doc); err != nil {
		t.Fatalf("decoding /api/cluster: %v", err)
	}
	return doc.Replication
}

// startWithHTTPLocalities is a cluster with an HTTP listener per node and
// a declared locality per node.
func startWithHTTPLocalities(t *testing.T, localities ...string) *TestCluster {
	t.Helper()
	listeners := make([]net.Listener, len(localities))
	for i := range listeners {
		lis, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		listeners[i] = lis
	}
	i := 0
	return StartWithOptions(t, len(localities), func(c *server.Config) {
		c.HTTPListener = listeners[i]
		i++
	}, localities...)
}

// TestReplicationViewAgreesAcrossNodes is the claim behind #152: the
// replication buckets and the failure-domain projection are cluster
// facts, so every node must report the same ones — the serving node is
// provenance, not the subject — and losing a rack must move them.
func TestReplicationViewAgreesAcrossNodes(t *testing.T) {
	tc := startWithHTTPLocalities(t, "region=r1,rack=a", "region=r1,rack=b", "region=r1,rack=c")
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()
	if err := tc.waitForReplication(ctx, 3, "rack"); err != nil {
		t.Fatal(err)
	}

	// Every node agrees, and with one replica per rack nothing is
	// undiverse and nothing lacks quorum.
	var first replSection
	for i, n := range tc.Nodes {
		got := clusterReplication(t, n.HTTPAddr())
		if i == 0 {
			first = got
			if got.Healthy.Count == 0 {
				t.Fatalf("no healthy ranges: %+v", got)
			}
			if got.NoQuorum.Count != 0 || got.Undiverse.Count != 0 {
				t.Fatalf("a settled 3-rack cluster should be healthy: %+v", got)
			}
			if got.DefaultFactor != 3 {
				t.Fatalf("default replication factor %d", got.DefaultFactor)
			}
			continue
		}
		if got.Healthy.Count != first.Healthy.Count ||
			got.Under.Count != first.Under.Count ||
			got.NoQuorum.Count != first.NoQuorum.Count {
			t.Fatalf("n%d buckets %+v differ from n1's %+v", i+1, got, first)
		}
	}

	// Three racks, one replica of every range each: losing any one rack
	// costs no quorum, but leaves every range with no margin.
	if len(first.Tiers) != 2 || first.Tiers[0] != "region" || first.Tiers[1] != "rack" {
		t.Fatalf("tiers %v, want region then rack", first.Tiers)
	}
	racks := 0
	for _, d := range first.Domains {
		if d.Tier != "rack" {
			continue
		}
		racks++
		if d.Nodes != 1 || d.Live != 1 {
			t.Fatalf("rack %s: %d nodes, %d live", d.Value, d.Nodes, d.Live)
		}
		if d.LosesQuorum != 0 {
			t.Fatalf("rack %s would cost quorum on %d range(s) in a 3-rack cluster", d.Value, d.LosesQuorum)
		}
		if d.BareMajority == 0 {
			t.Fatalf("rack %s: losing one of three racks should leave ranges at a bare majority", d.Value)
		}
		if d.Replicas == 0 {
			t.Fatalf("rack %s holds no replicas", d.Value)
		}
	}
	if racks != 3 {
		t.Fatalf("%d rack domains, want 3", racks)
	}
	// The whole region holds everything, so losing it loses quorum on
	// every range — the projection has to distinguish the two.
	for _, d := range first.Domains {
		if d.Tier == "region" && d.LosesQuorum == 0 {
			t.Fatalf("losing the only region should cost quorum: %+v", d)
		}
	}

	// A node dies. Its ranges become under-replicated on the nodes that
	// remain, and the projection for its rack changes with it.
	tc.StopNode(2)
	deadline := time.Now().Add(60 * time.Second)
	for {
		got := clusterReplication(t, tc.Nodes[0].HTTPAddr())
		if got.Under.Count > 0 || got.NoQuorum.Count > 0 {
			if len(got.Under.Ranges) == 0 && len(got.NoQuorum.Ranges) == 0 {
				t.Fatalf("a non-empty bucket carries no example ranges: %+v", got)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("no range became under-replicated after a node died: %+v", got)
		}
		time.Sleep(500 * time.Millisecond)
	}
}
