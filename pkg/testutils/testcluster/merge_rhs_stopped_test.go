package testcluster

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sthorne/datax/pkg/base"
	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/server"
)

// TestMergeApplyRHSStopped: when an LHS replica applies a merge and its
// local RHS replica's raft loop has died through an error — the replica
// stays in the store map with a frozen applied index short of the
// subsume — the apply must not spin until the next shutdown. It aborts
// the way a shutdown abort does (nothing applied, the applied index
// stays), so the raft loop exits with a clear log line instead of
// burning a core, Stop() cannot wedge on it, and the node restarts
// cleanly. The driver's pre-propose confirmation (#64) is disabled so
// the merge reaches apply at all: with it, every RHS replica has applied
// the subsume before the merge is proposed, and this wait passes at once
// — the abort is the fail-safe for the path a mixed-version cluster
// leaves open. Issue #70.
//
// What a restart can do for the replica is bounded by the same fact #64
// addressed: the RHS group is torn down on the nodes where the merge
// applied, so a re-started RHS that never persisted the subsume has no
// leader left to fetch it from, and the LHS's replayed merge waits again.
// The test therefore asserts the abort, the clean shutdown, and the
// restart — not a replayed merge.
func TestMergeApplyRHSStopped(t *testing.T) {
	var failRange atomic.Int64 // n3 fails to apply the next entry of this range
	tc, engines := StartWithEngines(t, 3, func(c *server.Config) {
		c.TestingKnobs.SkipMergeConfirmation = true
		if c.StaticBootstrap.NodeID == 3 {
			c.TestingKnobs.FailApply = func(id base.RangeID, _ uint64) error {
				if int64(id) == failRange.Load() {
					return errors.New("injected ready-handling failure")
				}
				return nil
			}
		}
	})
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()
	db := tc.Nodes[0].DB()
	prefix := keys.TableDataPrefix(857)
	if err := db.Put(ctx, append(prefix.Clone(), "a"...), []byte("v")); err != nil {
		t.Fatal(err)
	}
	sr, err := db.AdminSplit(ctx, append(prefix.Clone(), "m"...))
	if err != nil {
		t.Fatal(err)
	}
	lhsID, rhsID := sr.Left.RangeID, sr.Right.RangeID
	// Lead both sides from n1 so the merge driver runs there.
	for _, k := range []keys.Key{sr.Left.StartKey, sr.Right.StartKey} {
		if err := db.AdminTransferLease(ctx, k, tc.Nodes[0].NodeID()); err != nil {
			t.Fatal(err)
		}
	}
	waitReplica := func(n *server.Node, id base.RangeID) {
		t.Helper()
		deadline := time.Now().Add(30 * time.Second)
		for {
			if _, ok := n.Store().GetReplica(id); ok {
				return
			}
			if time.Now().After(deadline) {
				t.Fatalf("n%d never got a replica of %s", n.NodeID(), id)
			}
			time.Sleep(20 * time.Millisecond)
		}
	}
	waitReplica(tc.Nodes[2], rhsID)
	waitReplica(tc.Nodes[2], lhsID)

	// Kill n3's RHS raft loop: its next apply fails, the loop exits through
	// the generic error branch, and the replica stays behind for good.
	failRange.Store(int64(rhsID))
	if err := db.Put(ctx, append(prefix.Clone(), "z"...), []byte("v")); err != nil {
		t.Fatal(err)
	}
	rhs3, _ := tc.Nodes[2].Store().GetReplica(rhsID)
	select {
	case <-rhs3.TestingRaftStopped():
	case <-time.After(30 * time.Second):
		t.Fatal("n3's RHS raft loop did not stop on the injected apply failure")
	}
	failRange.Store(0)

	// Merge on the surviving quorum. n3's LHS applies the trigger, finds its
	// RHS dead short of the subsume, and must give up promptly rather than
	// spin: its raft loop exits.
	var merged keys.Key
	deadline := time.Now().Add(60 * time.Second)
	for {
		mr, err := db.AdminMerge(ctx, sr.Left.StartKey)
		if err == nil {
			merged = mr.Desc.EndKey
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("merge never committed: %v", err)
		}
		if strings.Contains(err.Error(), "does not lead the right neighbor") {
			_ = db.AdminTransferLease(ctx, sr.Right.StartKey, tc.Nodes[0].NodeID())
		}
		time.Sleep(50 * time.Millisecond)
	}
	lhs3, _ := tc.Nodes[2].Store().GetReplica(lhsID)
	select {
	case <-lhs3.TestingRaftStopped():
	case <-time.After(30 * time.Second):
		t.Fatal("n3's LHS raft loop is still waiting on a dead RHS (issue #70)")
	}
	if lhs3.Desc().EndKey.Equal(merged) {
		t.Fatal("n3's LHS adopted the merged descriptor without its RHS having applied the subsume")
	}

	// Shutdown must not wedge on the aborted apply, and the node restarts
	// and serves: both replicas come back from disk, the RHS short of the
	// subsume, and the cluster keeps working around them.
	stopped := make(chan struct{})
	go func() { tc.StopNode(2); close(stopped) }()
	select {
	case <-stopped:
	case <-time.After(45 * time.Second):
		t.Fatal("StopNode wedged after the aborted merge apply")
	}
	tc.RestartNode(2, engines[2])
	waitReplica(tc.Nodes[2], lhsID)
	waitReplica(tc.Nodes[2], rhsID)
	if v, err := tc.Nodes[2].DB().Get(ctx, append(prefix.Clone(), "a"...)); err != nil || string(v) != "v" {
		t.Fatalf("read through the restarted node: %q %v", v, err)
	}
	if v, err := db.Get(ctx, append(prefix.Clone(), "z"...)); err != nil || string(v) != "v" {
		t.Fatalf("read of the merged span: %q %v", v, err)
	}
}
