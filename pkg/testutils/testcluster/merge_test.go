package testcluster

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/sthorne/datax/pkg/base"
	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/kvpb"
	"github.com/sthorne/datax/pkg/server"
)

// mergeRanges drives lhs's merge with its right neighbor to completion,
// transferring the RHS's leadership to the LHS leader as needed.
func mergeRanges(t *testing.T, ctx context.Context, tc *TestCluster, lhsID base.RangeID) kvpb.RangeDescriptor {
	t.Helper()
	deadline := time.Now().Add(60 * time.Second)
	for {
		descs, err := tc.ranges(ctx)
		if err != nil {
			t.Fatal(err)
		}
		var lhs *kvpb.RangeDescriptor
		for i := range descs {
			if descs[i].RangeID == lhsID {
				lhs = &descs[i]
			}
		}
		if lhs == nil {
			t.Fatalf("range %d not found", lhsID)
		}
		lead := tc.LeaderIndex(lhsID)
		resp, err := tc.Nodes[lead].DB().AdminMerge(ctx, lhs.StartKey)
		if err == nil {
			return resp.Desc
		}
		if strings.Contains(err.Error(), "transfer its lease here first") {
			var rhs *kvpb.RangeDescriptor
			for i := range descs {
				if descs[i].StartKey.Equal(lhs.EndKey) {
					rhs = &descs[i]
				}
			}
			if rhs != nil {
				_ = tc.Nodes[lead].DB().AdminTransferLease(ctx, rhs.StartKey, tc.Nodes[lead].NodeID())
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("merge of %d never completed: %v", lhsID, err)
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// TestRangeMergeBasic: split, write both sides, merge back. The merged
// range serves everything, the RHS group is gone from every store, and the
// /meta addressing shrinks back to one record. Regression test for issue #1.
func TestRangeMergeBasic(t *testing.T) {
	tc, engines := StartWithEngines(t, 3)
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()

	prefix := keys.TableDataPrefix(850)
	splitKey := append(prefix.Clone(), "m"...)
	for i := 0; i < 10; i++ {
		side := "a"
		if i >= 5 {
			side = "z"
		}
		k := append(prefix.Clone(), fmt.Sprintf("%s%02d", side, i)...)
		if err := tc.Nodes[0].DB().Put(ctx, k, []byte(fmt.Sprintf("v%d", i))); err != nil {
			t.Fatal(err)
		}
	}
	sr, err := tc.Nodes[0].DB().AdminSplit(ctx, splitKey)
	if err != nil {
		t.Fatal(err)
	}
	rhsID := sr.Right.RangeID

	// A writer hammers both sides of the boundary throughout the merge;
	// acknowledged writes must survive (the freeze window surfaces only as
	// routing retries inside DB.Send).
	acked := make(map[string]bool)
	stopW := make(chan struct{})
	doneW := make(chan struct{})
	go func() {
		defer close(doneW)
		for i := 0; ; i++ {
			select {
			case <-stopW:
				return
			default:
			}
			side := "a"
			if i%2 == 1 {
				side = "z"
			}
			k := fmt.Sprintf("w-%s%04d", side, i)
			if err := tc.Nodes[1].DB().Put(ctx, append(prefix.Clone(), k...), []byte("w")); err == nil {
				acked[k] = true
			}
		}
	}()

	merged := mergeRanges(t, ctx, tc, sr.Left.RangeID)
	close(stopW)
	<-doneW
	if len(acked) == 0 {
		t.Fatal("no writes succeeded around the merge")
	}
	for k := range acked {
		if v, err := tc.Nodes[0].DB().Get(ctx, append(prefix.Clone(), k...)); err != nil || string(v) != "w" {
			t.Fatalf("acknowledged write %s lost across the merge: %q, %v", k, v, err)
		}
	}
	if !merged.StartKey.Equal(sr.Left.StartKey) || !merged.EndKey.Equal(sr.Right.EndKey) {
		t.Fatalf("merged bounds %s", &merged)
	}

	// The RHS group is gone from every store; its unreplicated state too.
	deadline := time.Now().Add(20 * time.Second)
	for {
		gone := true
		for _, n := range tc.Nodes {
			if _, ok := n.Store().GetReplica(rhsID); ok {
				gone = false
			}
		}
		if gone {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("RHS replica still present on some store")
		}
		time.Sleep(100 * time.Millisecond)
	}
	for i, eng := range engines {
		pre := keys.RangeUnreplicatedPrefix(rhsID)
		it := eng.NewIter(pre, pre.PrefixEnd())
		found := it.SeekGE(pre)
		var k []byte
		if found {
			k = append([]byte(nil), it.Key()...)
		}
		_ = it.Close()
		if found && !keys.Key(k).Equal(keys.RangeTombstoneKey(rhsID)) {
			t.Fatalf("engine %d retains RHS unreplicated key %x", i, k)
		}
	}

	// All data survives; writes on both old sides work; a transaction spans
	// the old boundary atomically.
	for i := 0; i < 10; i++ {
		side := "a"
		if i >= 5 {
			side = "z"
		}
		k := append(prefix.Clone(), fmt.Sprintf("%s%02d", side, i)...)
		if v, err := tc.Nodes[1].DB().Get(ctx, k); err != nil || string(v) != fmt.Sprintf("v%d", i) {
			t.Fatalf("read %s after merge: %q, %v", k, v, err)
		}
	}
	if err := tc.Nodes[2].DB().Put(ctx, append(prefix.Clone(), "post-z"...), []byte("w")); err != nil {
		t.Fatal(err)
	}

	// Routing self-heals to one record covering the whole span.
	deadline = time.Now().Add(20 * time.Second)
	for {
		descs, err := tc.ranges(ctx)
		if err == nil && len(descs) == 1 && descs[0].RangeID == merged.RangeID {
			break
		}
		if time.Now().After(deadline) {
			d, _ := tc.ranges(ctx)
			t.Fatalf("meta never shrank to the merged record: %v", d)
		}
		time.Sleep(100 * time.Millisecond)
	}

	// Replicas stay byte-identical after the merge and fresh writes.
	deadline = time.Now().Add(20 * time.Second)
	for {
		sums, _ := dataChecksums(t, engines, prefix)
		if sums[0] == sums[1] && sums[1] == sums[2] {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("replicas diverged after merge")
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// TestAutoMerge: the housekeeping pass merges adjacent underfull ranges on
// its own (transferring RHS leadership to itself across passes), and leaves
// ranges above the threshold alone.
func TestAutoMerge(t *testing.T) {
	tc, _ := StartWithEngines(t, 3, func(cfg *server.Config) {
		cfg.MergeSizeThreshold = 1 << 20
	})
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()

	prefix := keys.TableDataPrefix(851)
	if err := tc.Nodes[0].DB().Put(ctx, append(prefix.Clone(), "a"...), []byte("v")); err != nil {
		t.Fatal(err)
	}
	if _, err := tc.Nodes[0].DB().AdminSplit(ctx, append(prefix.Clone(), "m"...)); err != nil {
		t.Fatal(err)
	}

	deadline := time.Now().Add(60 * time.Second)
	for {
		for _, n := range tc.Nodes {
			n.Store().RunRangeMergeOnce(ctx)
		}
		descs, err := tc.ranges(ctx)
		if err == nil && len(descs) == 1 {
			break
		}
		if time.Now().After(deadline) {
			d, _ := tc.ranges(ctx)
			t.Fatalf("auto-merge never completed: %v", d)
		}
		time.Sleep(200 * time.Millisecond)
	}
	if v, err := tc.Nodes[1].DB().Get(ctx, append(prefix.Clone(), "a"...)); err != nil || string(v) != "v" {
		t.Fatalf("read after auto-merge: %q, %v", v, err)
	}
}

// TestAutoMergeRespectsThreshold: ranges above the merge threshold stay
// split.
func TestAutoMergeRespectsThreshold(t *testing.T) {
	tc, _ := StartWithEngines(t, 3, func(cfg *server.Config) {
		cfg.MergeSizeThreshold = 1 // effectively: never merge non-empty ranges
	})
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	prefix := keys.TableDataPrefix(852)
	for i := 0; i < 6; i++ {
		if err := tc.Nodes[0].DB().Put(ctx, append(prefix.Clone(), fmt.Sprintf("k%d", i)...), []byte("vvvvvvvv")); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := tc.Nodes[0].DB().AdminSplit(ctx, append(prefix.Clone(), "k3"...)); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 10; i++ {
		for _, n := range tc.Nodes {
			n.Store().RunRangeMergeOnce(ctx)
		}
		time.Sleep(50 * time.Millisecond)
	}
	descs, err := tc.ranges(ctx)
	if err != nil || len(descs) != 2 {
		t.Fatalf("threshold ignored: %v (%v)", descs, err)
	}
}

// TestMergeFrozenAndRecovery: a subsumed (frozen) range refuses traffic;
// an interrupted merge — freeze landed, driver "crashed" — is completed by
// the housekeeping pass; the frozen state survives a restart on disk.
func TestMergeFrozenAndRecovery(t *testing.T) {
	dir := t.TempDir()
	n := startDiskNode(t, dir, true, "")
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	prefix := keys.TableDataPrefix(853)
	for _, k := range []string{"a1", "z1"} {
		if err := n.DB().Put(ctx, append(prefix.Clone(), k...), []byte("v")); err != nil {
			t.Fatal(err)
		}
	}
	sr, err := n.DB().AdminSplit(ctx, append(prefix.Clone(), "m"...))
	if err != nil {
		t.Fatal(err)
	}

	// Freeze the RHS by hand — a merge whose driver dies right after
	// subsume.
	rhs, ok := n.Store().GetReplica(sr.Right.RangeID)
	if !ok {
		t.Fatal("no RHS replica")
	}
	sub := &kvpb.BatchRequest{Header: kvpb.BatchHeader{RangeID: sr.Right.RangeID}}
	sub.Add(&kvpb.SubsumeRequest{
		RequestHeader: kvpb.RequestHeader{Key: sr.Right.StartKey, EndKey: sr.Right.EndKey},
		MergeInto:     sr.Left.RangeID,
	})
	if _, kerr := rhs.Execute(ctx, sub); kerr != nil {
		t.Fatalf("subsume: %v", kerr)
	}

	// Frozen: direct writes to the RHS are refused.
	put := &kvpb.BatchRequest{Header: kvpb.BatchHeader{RangeID: sr.Right.RangeID, Timestamp: n.Clock().Now()}}
	put.Add(&kvpb.PutRequest{RequestHeader: kvpb.RequestHeader{Key: append(prefix.Clone(), "z2"...)}, Value: []byte("x")})
	if _, kerr := rhs.Execute(ctx, put); kerr == nil || kerr.RangeKeyMismatch == nil {
		t.Fatalf("frozen range accepted a write: %v", kerr)
	}

	// The freeze survives a restart.
	n.Stop()
	n = startDiskNode(t, dir, false, "")
	defer n.Stop()
	rhs2, ok := n.Store().GetReplica(sr.Right.RangeID)
	if !ok {
		t.Fatal("no RHS replica after restart")
	}
	deadlineElect := time.Now().Add(20 * time.Second)
	for {
		_, kerr := rhs2.Execute(ctx, put)
		if kerr != nil && kerr.RangeKeyMismatch != nil {
			break // frozen refusal — the flag survived
		}
		if kerr == nil {
			t.Fatal("freeze did not survive restart: write accepted")
		}
		if time.Now().After(deadlineElect) {
			t.Fatalf("freeze did not survive restart: %v", kerr)
		}
		time.Sleep(100 * time.Millisecond) // still electing; retry
	}

	// Housekeeping completes the interrupted merge (single node leads both
	// sides), and the data serves again through the merged range.
	deadline := time.Now().Add(60 * time.Second)
	for {
		n.Store().RunRangeMergeOnce(ctx)
		if _, ok := n.Store().GetReplica(sr.Right.RangeID); !ok {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("interrupted merge never completed")
		}
		time.Sleep(100 * time.Millisecond)
	}
	for _, k := range []string{"a1", "z1"} {
		if v, err := n.DB().Get(ctx, append(prefix.Clone(), k...)); err != nil || string(v) != "v" {
			t.Fatalf("read %s after recovery: %q, %v", k, v, err)
		}
	}
	if err := n.DB().Put(ctx, append(prefix.Clone(), "z2"...), []byte("post")); err != nil {
		t.Fatalf("write after recovery: %v", err)
	}
}

// TestMergeTimestampCache: a timestamp at or below reads the RHS served
// before the merge cannot write through the merged range — the merge bumps
// the surviving leader's cache over the absorbed span.
func TestMergeTimestampCache(t *testing.T) {
	tc, _ := StartWithEngines(t, 3)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	prefix := keys.TableDataPrefix(854)
	rhsKey := append(prefix.Clone(), "z"...)
	if err := tc.Nodes[0].DB().Put(ctx, rhsKey, []byte("v")); err != nil {
		t.Fatal(err)
	}
	sr, err := tc.Nodes[0].DB().AdminSplit(ctx, append(prefix.Clone(), "m"...))
	if err != nil {
		t.Fatal(err)
	}

	// A read on the RHS at time T.
	if _, err := tc.Nodes[0].DB().Get(ctx, rhsKey); err != nil {
		t.Fatal(err)
	}
	readTS := tc.Nodes[0].Clock().Now()

	merged := mergeRanges(t, ctx, tc, sr.Left.RangeID)

	// A non-transactional write at <= readTS through the merged leader must
	// be bounced above the timestamp cache, not silently applied beneath
	// the read.
	lead := tc.LeaderIndex(merged.RangeID)
	rep, _ := tc.Nodes[lead].Store().GetReplica(merged.RangeID)
	ba := &kvpb.BatchRequest{Header: kvpb.BatchHeader{RangeID: merged.RangeID, Timestamp: readTS}}
	ba.Add(&kvpb.PutRequest{RequestHeader: kvpb.RequestHeader{Key: rhsKey}, Value: []byte("beneath")})
	_, kerr := rep.Execute(ctx, ba)
	if kerr == nil || kerr.TxnRetry == nil {
		t.Fatalf("write beneath a pre-merge read was not pushed: %v", kerr)
	}
}

// TestMergeStopNodeNoDeadlock: stopping a node while it is applying a
// merge must not wedge Stop(). The merge apply used to spin waiting for
// the local RHS replica to reach the subsume index even after node
// shutdown had stopped the RHS's raft loop, deadlocking Stop() — the
// raftLoop stuck in stageMerge while Stop waited for it, the hang behind
// TestMergeTimestampCache's cleanup in issue #61. The apply now aborts on
// shutdown WITHOUT advancing the applied index, and the restart replays
// the merge. Exercised by racing merges against stop/restart cycles of a
// follower, each stop under a watchdog. Every round must also LAND: the
// driver defers a merge until every RHS replica confirms the subsume
// (a stopped follower blocks it, a restarted one unblocks it), and
// afterwards all three replicas — the restarted follower included — hold
// the merged descriptor, proving the follower neither skipped the merge
// nor wedged waiting for an RHS group that no longer exists.
func TestMergeStopNodeNoDeadlock(t *testing.T) {
	tc, engines := StartWithEngines(t, 3)
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()
	prefix := keys.TableDataPrefix(856)
	if err := tc.Nodes[0].DB().Put(ctx, append(prefix.Clone(), "a"...), []byte("v")); err != nil {
		t.Fatal(err)
	}

	for round := 0; round < 3; round++ {
		splitKey := append(prefix.Clone(), fmt.Sprintf("m%d", round)...)
		sr, err := tc.Nodes[0].DB().AdminSplit(ctx, splitKey)
		if err != nil {
			t.Fatal(err)
		}
		// Merge in the background while n3 stops: the merge commits on the
		// surviving quorum; n3 applies it just before the stop (the racy
		// window), aborts cleanly mid-apply, or replays it at restart.
		mergeDone := make(chan struct{})
		var mergeErr error
		go func(start keys.Key) {
			defer close(mergeDone)
			mctx, mcancel := context.WithTimeout(ctx, 60*time.Second)
			defer mcancel()
			db := tc.Nodes[0].DB()
			for {
				_, mergeErr = db.AdminMerge(mctx, start)
				if mergeErr == nil || mctx.Err() != nil {
					return
				}
				time.Sleep(50 * time.Millisecond)
			}
		}(sr.Left.StartKey.Clone())

		stopped := make(chan struct{})
		go func() { tc.StopNode(2); close(stopped) }()
		select {
		case <-stopped:
		case <-time.After(45 * time.Second):
			cancel() // release the merge goroutine before failing
			<-mergeDone
			t.Fatal("StopNode wedged: merge-apply vs shutdown deadlock (issue #61)")
		}
		tc.RestartNode(2, engines[2])
		<-mergeDone
		if mergeErr != nil {
			t.Fatalf("round %d: merge never landed: %v", round, mergeErr)
		}
		waitMergedEverywhere(t, tc, sr.Left.RangeID, sr.Right.EndKey, 30*time.Second)
	}

	if v, err := tc.Nodes[0].DB().Get(ctx, append(prefix.Clone(), "a"...)); err != nil || string(v) != "v" {
		t.Fatalf("read after merge/stop rounds: %q %v", v, err)
	}
}

// waitMergedEverywhere waits until every node's replica of rangeID
// reports the merged descriptor (end key extended to endKey), then checks
// the replica is live enough to report it again — a wedged raft loop
// (stuck at merge apply) never advances its descriptor.
func waitMergedEverywhere(t *testing.T, tc *TestCluster, rangeID base.RangeID, endKey keys.Key, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		var lagging []string
		for i, n := range tc.Nodes {
			r, ok := n.Store().GetReplica(rangeID)
			if !ok {
				lagging = append(lagging, fmt.Sprintf("n%d: no replica", i+1))
				continue
			}
			if d := r.Desc(); !d.EndKey.Equal(endKey) {
				lagging = append(lagging, fmt.Sprintf("n%d: end key %s at applied %d", i+1, d.EndKey, r.AppliedIndex()))
			}
		}
		if len(lagging) == 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s: merge not applied on every replica: %v", rangeID, lagging)
		}
		time.Sleep(50 * time.Millisecond)
	}
}
