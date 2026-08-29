package testcluster

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/kvserver"
	"github.com/sthorne/datax/pkg/storage"
	"github.com/sthorne/datax/pkg/util/hlc"
)

// dataChecksums hashes the encoded MVCC bytes under prefix on each engine.
func dataChecksums(t *testing.T, engines []*storage.Engine, prefix keys.Key) ([][32]byte, []int) {
	t.Helper()
	lo := storage.EncodeMVCCKey(prefix, hlc.Timestamp{})
	hi := storage.EncodeMVCCKey(prefix.PrefixEnd(), hlc.Timestamp{})
	sums := make([][32]byte, len(engines))
	counts := make([]int, len(engines))
	for i, eng := range engines {
		h := sha256.New()
		it := eng.NewIter(lo, hi)
		for ok := it.SeekGE(lo); ok; ok = it.Next() {
			h.Write(it.Key())
			h.Write(it.Value())
			counts[i]++
		}
		if err := it.Close(); err != nil {
			t.Fatal(err)
		}
		copy(sums[i][:], h.Sum(nil))
	}
	return sums, counts
}

// TestRaftCatchupSnapshot: a follower stops, the log is truncated far past
// its position, and it returns — previously unrecoverable without removal
// and re-add. The leader now streams a catch-up snapshot and the follower
// converges to byte-identical state. Regression test for issue #4.
func TestRaftCatchupSnapshot(t *testing.T) {
	tc, engines := StartWithEngines(t, 3)
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()

	prefix := keys.TableDataPrefix(840)
	put := func(n int, k, v string) {
		t.Helper()
		if err := tc.Nodes[n].DB().Put(ctx, append(prefix.Clone(), k...), []byte(v)); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < 20; i++ {
		put(0, fmt.Sprintf("k%03d", i), "before")
	}

	leader := tc.LeaderIndex(1)
	down := (leader + 1) % 3
	tc.StopNode(down)

	// Enough traffic that truncation moves far past the stopped follower.
	for i := 0; i < 400; i++ {
		put(leader, fmt.Sprintf("k%03d", i%40), fmt.Sprintf("v%d", i))
	}
	store := tc.Nodes[leader].Store()
	rep, _ := store.GetReplica(1)
	deadline := time.Now().Add(20 * time.Second)
	for rep.TruncatedIndex() < 300 {
		store.RunLogTruncationOnce(ctx)
		if time.Now().After(deadline) {
			t.Fatalf("truncation stuck at %d", rep.TruncatedIndex())
		}
		time.Sleep(100 * time.Millisecond)
	}

	// The follower returns behind the truncated log; raft requests a
	// snapshot and the leader streams it.
	n := tc.RestartNode(down, engines[down])
	restarted, ok := n.Store().GetReplica(1)
	if !ok {
		t.Fatal("restarted node lost its replica")
	}
	deadline = time.Now().Add(60 * time.Second)
	for {
		sums, _ := dataChecksums(t, engines, prefix)
		if restarted.AppliedIndex() >= rep.TruncatedIndex() && sums[0] == sums[1] && sums[1] == sums[2] {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("follower never caught up: applied %d vs truncated %d, checksums equal %v/%v",
				restarted.AppliedIndex(), rep.TruncatedIndex(), sums[0] == sums[1], sums[1] == sums[2])
		}
		time.Sleep(100 * time.Millisecond)
	}

	// Fresh writes replicate to all three again.
	put(leader, "after", "catchup")
	deadline = time.Now().Add(30 * time.Second)
	for {
		sums, _ := dataChecksums(t, engines, prefix)
		if sums[0] == sums[1] && sums[1] == sums[2] {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("replicas diverged after catch-up")
		}
		time.Sleep(100 * time.Millisecond)
	}
	if v, err := tc.Nodes[leader].DB().Get(ctx, append(prefix.Clone(), "after"...)); err != nil || string(v) != "catchup" {
		t.Fatalf("read after catch-up: %q, %v", v, err)
	}
}

// TestInstallSnapshotRejectsStale: an install below the replica's applied
// state is refused — the state machine never moves backward.
func TestInstallSnapshotRejectsStale(t *testing.T) {
	tc, _ := StartWithEngines(t, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	if err := tc.Nodes[0].DB().Put(ctx, append(keys.TableDataPrefix(841), "k"...), []byte("v")); err != nil {
		t.Fatal(err)
	}
	rep, _ := tc.Nodes[0].Store().GetReplica(1)
	desc := rep.Desc()
	header, err := json.Marshal(map[string]any{
		"desc": desc, "replica_id": 1, "applied_index": 1, "term": 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	next := func() ([]kvserver.SnapshotKV, error) { return nil, nil }
	if err := tc.Nodes[0].Store().ApplySnapshotStream(header, next); err == nil {
		t.Fatal("stale snapshot install accepted")
	}
}
