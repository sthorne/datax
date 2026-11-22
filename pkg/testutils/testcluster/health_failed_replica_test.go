package testcluster

import (
	"context"
	"errors"
	"net"
	"strings"
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
