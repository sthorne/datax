package testcluster

import (
	"context"
	"errors"
	"fmt"
	"net"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sthorne/datax/pkg/base"
	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/server"
)

// TestHealthCountsFailedReplicas: a replica whose raft group a storage
// failure stopped (ENOSPC, EIO on the raft engine) serves nothing until
// its node restarts, but the node keeps heartbeating. The health check
// must count it as unavailable: one such replica of three leaves the
// range under-replicated, two lose its quorum. Before, health saw only
// liveness and reported nothing.
func TestHealthCountsFailedReplicas(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	first := true
	tc := StartWithOptions(t, 3, func(c *server.Config) {
		if first {
			c.HTTPListener = lis
			first = false
		}
	})
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	// A range of its own, away from the system ranges the heartbeat
	// writes to, replicated on all three nodes.
	split, err := tc.Nodes[0].DB().AdminSplit(ctx, append(keys.TableDataPrefix(761), "m"...))
	if err != nil {
		t.Fatalf("split: %v", err)
	}
	rid := split.Right.RangeID
	waitFor(t, 60*time.Second, "the range on all three nodes", func() bool {
		for _, n := range tc.Nodes {
			if _, ok := n.Store().GetReplica(rid); !ok {
				return false
			}
		}
		return true
	})
	problemFor := func(doc server.HealthStatus, check string) *server.Problem {
		for i := range doc.Problems {
			if p := &doc.Problems[i]; p.Check == check {
				return p
			}
		}
		return nil
	}
	waitFor(t, 60*time.Second, "a healthy cluster", func() bool {
		doc := healthDoc(t, tc, 0)
		return problemFor(doc, "under-replicated") == nil && problemFor(doc, "quorum-lost") == nil
	})

	fail := func(i int) {
		r, ok := tc.Nodes[i].Store().GetReplica(rid)
		if !ok {
			t.Fatalf("n%d holds no replica of %s", i+1, rid)
		}
		r.TestingFailReady(errors.New("raft engine commit: no space left on device"))
	}

	// One of three: the node is live, so only its report says the range
	// is short a replica.
	fail(2)
	waitFor(t, 30*time.Second, "replicas-failed and under-replicated", func() bool {
		doc := healthDoc(t, tc, 0)
		f, u := problemFor(doc, "replicas-failed"), problemFor(doc, "under-replicated")
		return f != nil && f.Node == 3 && base.RangeID(f.Range) == rid && f.Severity == server.SeverityCritical &&
			u != nil && base.RangeID(u.Range) == rid
	})

	// Two of three: the range has no quorum though every node is live.
	fail(1)
	waitFor(t, 30*time.Second, "quorum-lost", func() bool {
		p := problemFor(healthDoc(t, tc, 0), "quorum-lost")
		return p != nil && base.RangeID(p.Range) == rid
	})
}

// waitFor polls cond until it holds, failing after limit.
func waitFor(t *testing.T, limit time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(limit)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out after %s waiting for %s", limit, strings.TrimSpace(what))
		}
		time.Sleep(250 * time.Millisecond)
	}
}

// TestReplicationViewCountsFailedReplicas: the console's replication
// section counts a replica its live node reports failed as not live, as
// the health check does: one of three makes the range under-replicated,
// two take its quorum, and the range is listed under the dead-replicas
// overlay. Before, the section read liveness alone and showed the range
// healthy.
func TestReplicationViewCountsFailedReplicas(t *testing.T) {
	tc := startWithHTTP(t, 3)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	split, err := tc.Nodes[0].DB().AdminSplit(ctx, append(keys.TableDataPrefix(762), "m"...))
	if err != nil {
		t.Fatalf("split: %v", err)
	}
	rid := int64(split.Right.RangeID)
	waitFor(t, 60*time.Second, "the range on all three nodes", func() bool {
		for _, n := range tc.Nodes {
			if _, ok := n.Store().GetReplica(split.Right.RangeID); !ok {
				return false
			}
		}
		return true
	})
	in := func(b replBucket) bool { return slices.Contains(b.Ranges, rid) }
	addr := tc.Nodes[0].HTTPAddr()
	waitFor(t, 60*time.Second, "the range healthy", func() bool {
		return in(clusterReplication(t, addr).Healthy)
	})
	fail := func(i int) {
		r, ok := tc.Nodes[i].Store().GetReplica(split.Right.RangeID)
		if !ok {
			t.Fatalf("n%d holds no replica of r%d", i+1, rid)
		}
		r.TestingFailReady(errors.New("raft engine commit: input/output error"))
	}
	fail(2)
	waitFor(t, 30*time.Second, "the range under-replicated with a dead replica", func() bool {
		got := clusterReplication(t, addr)
		return in(got.Under) && in(got.Unavailable) && !in(got.Healthy)
	})
	fail(1)
	waitFor(t, 30*time.Second, "the range without quorum", func() bool {
		got := clusterReplication(t, addr)
		return in(got.NoQuorum) && in(got.Unavailable)
	})
}

// TestReplicaFaultsPastHeartbeatCap: a heartbeat lists at most a capped
// number of failed and missing replicas (the lowest range IDs), with
// exact counts. The health check and the replication view must still
// classify every range a node's unavailable replicas touch, not only
// those its capped lists name: they ask the node for the rest. A node
// that does not answer leaves the ranges past its lists unknown, and both
// say so rather than call them healthy. The cap is lowered here so a
// handful of failed replicas overflows it, as more than 64 would in
// production.
func TestReplicaFaultsPastHeartbeatCap(t *testing.T) {
	const reportCap, nRanges = 4, 10
	listeners := make([]net.Listener, 3)
	for i := range listeners {
		lis, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		listeners[i] = lis
	}
	var silent atomic.Bool // n3 does not answer for its full lists
	i := 0
	tc := StartWithOptions(t, 3, func(c *server.Config) {
		c.HTTPListener = listeners[i]
		c.TestingMaxReportedReplicas = reportCap
		if i == 2 {
			c.TestingAdminHook = func(ctx context.Context, op string) func() {
				if op == "unavailable-replicas" && silent.Load() {
					select {
					case <-ctx.Done():
					case <-time.After(10 * time.Second):
					}
				}
				return nil
			}
		}
		i++
	})
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()

	var rids []base.RangeID
	for k := range nRanges {
		split, err := tc.Nodes[0].DB().AdminSplit(ctx, append(keys.TableDataPrefix(763), byte('a'+k)))
		if err != nil {
			t.Fatalf("split %d: %v", k, err)
		}
		rids = append(rids, split.Right.RangeID)
	}
	slices.Sort(rids)
	waitFor(t, 60*time.Second, "the ranges on all three nodes", func() bool {
		for _, n := range tc.Nodes {
			for _, rid := range rids {
				if _, ok := n.Store().GetReplica(rid); !ok {
					return false
				}
			}
		}
		return true
	})
	addr := tc.Nodes[0].HTTPAddr()
	problem := func(doc server.HealthStatus, check string) *server.Problem {
		for i := range doc.Problems {
			if p := &doc.Problems[i]; p.Check == check {
				return p
			}
		}
		return nil
	}
	// count is the number of ranges a cluster-ranges problem reports.
	count := func(p *server.Problem) int {
		if p == nil {
			return 0
		}
		var n int
		if _, err := fmt.Sscanf(p.Summary, "%d range(s)", &n); err != nil {
			t.Fatalf("summary %q: %v", p.Summary, err)
		}
		return n
	}
	all := func(b replBucket, ids []base.RangeID) bool {
		if b.Count != len(ids) {
			return false
		}
		for _, id := range ids {
			if !slices.Contains(b.Ranges, int64(id)) {
				return false
			}
		}
		return true
	}
	waitFor(t, 60*time.Second, "a healthy cluster", func() bool {
		doc := healthDoc(t, tc, 0)
		healthy := clusterReplication(t, addr).Healthy
		for _, rid := range rids {
			if !slices.Contains(healthy.Ranges, int64(rid)) {
				return false
			}
		}
		return problem(doc, "under-replicated") == nil && problem(doc, "quorum-lost") == nil
	})
	fail := func(node int) {
		for _, rid := range rids {
			r, ok := tc.Nodes[node].Store().GetReplica(rid)
			if !ok {
				t.Fatalf("n%d holds no replica of %s", node+1, rid)
			}
			r.TestingFailReady(errors.New("raft engine commit: input/output error"))
		}
	}

	// n3 fails all ten; its heartbeat lists four. Every range is a
	// replica short, in both views.
	var last string
	defer func() {
		if t.Failed() {
			t.Logf("last seen: %s", last)
		}
	}()
	fail(2)
	waitFor(t, 60*time.Second, "all ten ranges under-replicated with a dead replica", func() bool {
		doc := healthDoc(t, tc, 0)
		got := clusterReplication(t, addr)
		last = fmt.Sprintf("health under=%d quorum=%d; view under=%v dead=%v healthy=%v",
			count(problem(doc, "under-replicated")), count(problem(doc, "quorum-lost")), got.Under, got.Unavailable, got.Healthy)
		f := problem(doc, "replicas-failed")
		return f != nil && f.Node == 3 && strings.Contains(f.Summary, fmt.Sprintf("has %d replica(s)", nRanges)) &&
			count(problem(doc, "under-replicated")) == nRanges && problem(doc, "replication-unknown") == nil &&
			all(got.Under, rids) && all(got.Unavailable, rids) && got.Unknown.Count == 0
	})
	t.Logf("exact: %s", last)

	// n3 stops answering for its full lists: the four it lists are
	// still known short; the other six could be either, and are said to
	// be unknown, not healthy.
	silent.Store(true)
	listed, rest := rids[:reportCap], rids[reportCap:]
	waitFor(t, 60*time.Second, "the ranges past n3's lists unknown", func() bool {
		doc := healthDoc(t, tc, 0)
		got := clusterReplication(t, addr)
		last = fmt.Sprintf("health under=%d unknown=%d; view under=%v unknown=%v dead=%v",
			count(problem(doc, "under-replicated")), count(problem(doc, "replication-unknown")), got.Under, got.Unknown, got.Unavailable)
		u := problem(doc, "replicas-unverified")
		return u != nil && u.Node == 3 &&
			count(problem(doc, "under-replicated")) == len(listed) && count(problem(doc, "replication-unknown")) == len(rest) &&
			all(got.Under, listed) && all(got.Unknown, rest) && all(got.Unavailable, listed)
	})
	t.Logf("unknown: %s", last)
	silent.Store(false)

	// n2 fails all ten too: every range has lost its quorum.
	fail(1)
	waitFor(t, 60*time.Second, "all ten ranges without quorum", func() bool {
		doc := healthDoc(t, tc, 0)
		got := clusterReplication(t, addr)
		last = fmt.Sprintf("health quorum=%d; view noquorum=%v", count(problem(doc, "quorum-lost")), got.NoQuorum)
		return count(problem(doc, "quorum-lost")) == nRanges && problem(doc, "under-replicated") == nil &&
			all(got.NoQuorum, rids) && all(got.Unavailable, rids)
	})
}
