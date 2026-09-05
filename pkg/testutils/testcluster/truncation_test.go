package testcluster

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/server"
	"github.com/sthorne/datax/pkg/storage"
	"github.com/sthorne/datax/pkg/util/encoding"
)

// firstLogIndex returns the index of the lowest raft log entry physically
// present for range 1 in the engine (0 if the log is empty).
func firstLogIndex(t *testing.T, eng *storage.Engine) uint64 {
	t.Helper()
	lower := keys.RaftLogPrefix(1)
	it := eng.NewIter(lower, lower.PrefixEnd())
	defer func() { _ = it.Close() }()
	if !it.SeekGE(lower) {
		return 0
	}
	_, idx, err := encoding.DecodeUint64(it.Key()[len(lower):])
	if err != nil {
		t.Fatalf("malformed log key %x: %v", it.Key(), err)
	}
	return idx
}

func fillLog(ctx context.Context, t *testing.T, n *server.Node, tableID uint64, count int) {
	t.Helper()
	k := keys.TableDataPrefix(tableID)
	for i := 0; i < count; i++ {
		if err := n.DB().Put(ctx, k, []byte(fmt.Sprintf("v%d", i))); err != nil {
			t.Fatalf("put %d: %v", i, err)
		}
	}
}

// TestLogTruncation: after enough writes, the leader proposes a truncation
// and every replica physically drops its log prefix while staying live.
func TestLogTruncation(t *testing.T) {
	tc, _ := StartWithEngines(t, 3)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	fillLog(ctx, t, tc.Nodes[0], 720, 400)

	leader := tc.LeaderIndex(1)
	store := tc.Nodes[leader].Store()

	// Followers acknowledge asynchronously; retry until min(Match) has
	// caught up enough for a truncation to clear the min-pending bar.
	deadline := time.Now().Add(15 * time.Second)
	rep, _ := tc.Nodes[leader].Store().GetReplica(1)
	for rep.TruncatedIndex() == 0 {
		store.RunLogTruncationOnce(ctx)
		if time.Now().After(deadline) {
			t.Fatal("leader never truncated its log")
		}
		time.Sleep(100 * time.Millisecond)
	}
	truncIdx := rep.TruncatedIndex()
	if truncIdx < 256 {
		t.Fatalf("truncated only through %d; want >= 256", truncIdx)
	}

	// Every replica applies the truncation and physically drops the
	// prefix — on the split store once its own housekeeping has flushed
	// the state engine past the applies the prefix covered.
	for i, n := range tc.Nodes {
		r, ok := n.Store().GetReplica(1)
		if !ok {
			t.Fatalf("node %d has no replica of range 1", i+1)
		}
		for r.TruncatedIndex() < truncIdx {
			n.Store().RunLogTruncationOnce(ctx)
			if time.Now().After(deadline) {
				t.Fatalf("node %d never applied the truncation (at %d, want %d)", i+1, r.TruncatedIndex(), truncIdx)
			}
			time.Sleep(50 * time.Millisecond)
		}
		if first := firstLogIndex(t, tc.RaftEngine(i)); first <= truncIdx {
			t.Fatalf("node %d: log entry %d still on disk; truncated through %d", i+1, first, truncIdx)
		}
	}

	// The range still serves reads and writes.
	k := keys.TableDataPrefix(720)
	if err := tc.Nodes[0].DB().Put(ctx, k, []byte("after")); err != nil {
		t.Fatal(err)
	}
	if v, err := tc.Nodes[0].DB().Get(ctx, k); err != nil || string(v) != "after" {
		t.Fatalf("read after truncation: %q, %v", v, err)
	}
}

// TestLogTruncationAdvancesPastDeadVoter: a stopped voter no longer pins
// truncation — the log tracks the leader's applied index, and a returning
// voter is caught up by a raft snapshot instead (see the catch-up tests).
func TestLogTruncationAdvancesPastDeadVoter(t *testing.T) {
	tc, _ := StartWithEngines(t, 3)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	// Make sure the cluster works, then kill a follower while its Match is
	// still tiny.
	fillLog(ctx, t, tc.Nodes[0], 721, 3)
	leader := tc.LeaderIndex(1)
	tc.StopNode((leader + 1) % 3)

	fillLog(ctx, t, tc.Nodes[leader], 721, 400)
	store := tc.Nodes[leader].Store()
	rep, _ := store.GetReplica(1)
	deadline := time.Now().Add(15 * time.Second)
	for rep.TruncatedIndex() == 0 {
		store.RunLogTruncationOnce(ctx)
		if time.Now().After(deadline) {
			t.Fatal("truncation never advanced past the dead voter")
		}
		time.Sleep(100 * time.Millisecond)
	}
	if idx := rep.TruncatedIndex(); idx < 256 {
		t.Fatalf("truncated only through %d; want >= 256", idx)
	}
}

// TestLogTruncationRestart: a truncated log replays cleanly across a
// restart — the persisted TruncatedIndex/Term let raft resume from the
// truncation point.
func TestLogTruncationRestart(t *testing.T) {
	dir := t.TempDir()
	n := startDiskNode(t, dir, true, "")
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	fillLog(ctx, t, n, 722, 400)
	rep, _ := n.Store().GetReplica(1)
	// The truncation applies asynchronously and, on the split store,
	// lands once the state engine has flushed (the housekeeping call
	// forces that in tests).
	deadline := time.Now().Add(15 * time.Second)
	for rep.TruncatedIndex() < 256 {
		n.Store().RunLogTruncationOnce(ctx)
		if time.Now().After(deadline) {
			t.Fatalf("truncated only through %d; want >= 256", rep.TruncatedIndex())
		}
		time.Sleep(100 * time.Millisecond)
	}
	truncIdx := rep.TruncatedIndex()
	k := keys.TableDataPrefix(722)
	if err := n.DB().Put(ctx, k, []byte("persist")); err != nil {
		t.Fatal(err)
	}
	n.Stop()

	n2 := startDiskNode(t, dir, false, "")
	defer n2.Stop()
	rep2, ok := n2.Store().GetReplica(1)
	if !ok {
		t.Fatal("no replica of range 1 after restart")
	}
	if got := rep2.TruncatedIndex(); got < truncIdx {
		t.Fatalf("truncated index regressed across restart: %d < %d", got, truncIdx)
	}
	if v, err := n2.DB().Get(ctx, k); err != nil || string(v) != "persist" {
		t.Fatalf("read after restart: %q, %v", v, err)
	}
	if err := n2.DB().Put(ctx, k, []byte("again")); err != nil {
		t.Fatalf("write after restart: %v", err)
	}
}
