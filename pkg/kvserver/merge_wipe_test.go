package kvserver

import (
	"context"
	"testing"
	"time"

	"github.com/sthorne/datax/pkg/base"
	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/kvpb"
	"github.com/sthorne/datax/pkg/storage"
)

// startSplitRouterStore is startRouterStore on a split store: raft state
// on an engine of its own, the state engine without a write-ahead log.
func startSplitRouterStore(t *testing.T, flushAfter time.Duration) *routerEnv {
	t.Helper()
	return startRouterStoreCfg(t, func(cfg *StoreConfig) {
		state, err := storage.Open("", storage.Options{DisableWAL: true})
		if err != nil {
			t.Fatal(err)
		}
		raft, err := storage.Open("", storage.Options{Raft: true})
		if err != nil {
			t.Fatal(err)
		}
		cfg.Engine, cfg.RaftEngine = state, raft
		cfg.TruncationFlushAfter = flushAfter
	})
}

func raftStateHeld(t *testing.T, s *Store, id base.RangeID) bool {
	t.Helper()
	pre := keys.RangeUnreplicatedPrefix(id)
	it := s.raftEngine().NewIter(pre, pre.PrefixEnd())
	defer func() { _ = it.Close() }()
	return it.SeekGE(pre)
}

// TestMergeDoesNotFlushTheStateEngine is issue #395: a merge on a split
// store used to flush the state engine before deleting the absorbed
// range's raft state, and the merge queue folding hundreds of ranges
// back together drove the store into write stalls. The deletion now
// waits for a flush instead — the next one, or the one housekeeping
// forces once it has waited past the bound.
func TestMergeDoesNotFlushTheStateEngine(t *testing.T) {
	ctx := context.Background()
	env := startSplitRouterStore(t, -1) // no forced flush: the test flushes
	split := keys.Key("m")
	resp, kerr := env.split(ctx, split)
	if kerr != nil {
		t.Fatal(kerr)
	}
	rhs := resp.Right.RangeID
	env.put(t, keys.Key("n"), "v")
	if !raftStateHeld(t, env.store, rhs) {
		t.Fatal("the right-hand range has no raft state to begin with")
	}
	flushed := env.store.cfg.Engine.FlushedSeqNum()
	if kerr := env.merge(ctx, keys.Key("a")); kerr != nil {
		t.Fatal(kerr)
	}
	if _, ok := env.store.GetReplica(rhs); ok {
		t.Fatal("the right-hand replica outlived the merge")
	}
	if got := env.store.cfg.Engine.FlushedSeqNum(); got != flushed {
		t.Fatalf("the merge flushed the state engine (flushed seq %d → %d)", flushed, got)
	}
	if !raftStateHeld(t, env.store, rhs) {
		t.Fatal("the absorbed range's raft state went before the merge was durable on the state engine")
	}
	env.store.runDeferredTruncations()
	if !raftStateHeld(t, env.store, rhs) {
		t.Fatal("housekeeping removed the raft state with the merge not yet flushed")
	}
	if err := env.store.cfg.Engine.Flush(); err != nil {
		t.Fatal(err)
	}
	env.store.runDeferredTruncations()
	if raftStateHeld(t, env.store, rhs) {
		t.Fatal("the absorbed range's raft state outlived a flush past the merge")
	}
	br, kerr := env.send(ctx, &kvpb.GetRequest{RequestHeader: kvpb.RequestHeader{Key: keys.Key("n")}})
	if kerr != nil {
		t.Fatal(kerr)
	}
	if v := string(br.Responses[0].Get.Value); v != "v" {
		t.Fatalf("after the merge the absorbed key reads %q", v)
	}
}

// TestMergedRaftStateGoesWithTheForcedFlush: with nothing else flushing
// the state engine, housekeeping forces one once a merge has waited past
// the bound, and then removes the absorbed range's raft state.
func TestMergedRaftStateGoesWithTheForcedFlush(t *testing.T) {
	ctx := context.Background()
	env := startSplitRouterStore(t, time.Millisecond)
	resp, kerr := env.split(ctx, keys.Key("m"))
	if kerr != nil {
		t.Fatal(kerr)
	}
	rhs := resp.Right.RangeID
	// A write to the new right-hand range waits for it to elect a leader;
	// the merge refuses a right neighbor this node does not lead yet.
	env.put(t, keys.Key("n"), "v")
	if kerr := env.merge(ctx, keys.Key("a")); kerr != nil {
		t.Fatal(kerr)
	}
	time.Sleep(5 * time.Millisecond)
	env.store.runDeferredTruncations()
	if raftStateHeld(t, env.store, rhs) {
		t.Fatal("the absorbed range's raft state outlived housekeeping's forced flush")
	}
}
