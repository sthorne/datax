package kvserver

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/sthorne/datax/pkg/base"
	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/kvpb"
	"github.com/sthorne/datax/pkg/metrics"
	"github.com/sthorne/datax/pkg/storage"
	"github.com/sthorne/datax/pkg/util/hlc"
	"github.com/sthorne/datax/pkg/util/log"
)

// Range merges — the inverse of splits, in two replicated phases driven by
// the node that leads BOTH sides (the housekeeping pass transfers the
// right-hand side's leadership to itself first):
//
//  1. SUBSUME, a replicated command on the RHS: every replica persists a
//     frozen flag and the range refuses all traffic from then on — under a
//     whole-range exclusive latch, so nothing in flight straddles the
//     freeze. Serving is leader-only in datax and every future leader (and
//     restart) reloads the flag, so the freeze provably stops RHS traffic;
//     it also blocks membership changes, which is what makes the replica
//     sets stable for step 2. Because the driver leads the RHS, the
//     timestamps it ever served reads at are bounded by this node's clock.
//  2. MERGE, a replicated trigger on the LHS: at apply, every replica
//     waits for its local RHS replica to reach the subsume index (the
//     RHS's last command — engines are then identical), quiesces the RHS
//     raft group (keeping its data: bounds are logical, nothing moves),
//     and atomically widens the LHS descriptor, absorbs the RHS size and
//     GC threshold, and deletes the RHS's unreplicated state. Transaction
//     records are range-local ADDRESSED keys and follow their anchors
//     into the merged range with no rewriting. The trigger re-validates
//     the LHS generation at apply time — deterministically, so the merge
//     either lands or no-ops on every replica alike.
//
// After the merge the driver bumps its timestamp cache to now(), which
// (leading both sides on one clock) covers every read the RHS ever served.
// The /meta records are then repaired: one atomic batch overwrites the
// RHS's record (same key — the merged range shares its end key) and
// deletes the old LHS record; a failure there is benign, since a stale
// old-LHS record still routes its span to the same range ID.
//
// Crash recovery: subsume is idempotent; a frozen RHS whose merge never
// landed is re-driven (or unfrozen, if membership diverged before the
// freeze landed) by the housekeeping pass on whoever leads the LHS; the
// merge trigger replays like any entry, guarded by the applied index; the
// RHS descriptor key is deleted in the merge batch, so a restart never
// revives the absorbed group.

// mergeApplicationWait bounds how long the merge driver waits for every
// RHS replica to confirm the subsume before proposing the merge.
const mergeApplicationWait = 10 * time.Second

// mergeTrigger is carried by the replicated merge command on the LHS.
type mergeTrigger struct {
	Left   kvpb.RangeDescriptor `json:"left"`   // pre-merge LHS (generation check)
	Right  kvpb.RangeDescriptor `json:"right"`  // frozen RHS
	Merged kvpb.RangeDescriptor `json:"merged"` // LHS identity, RHS end key
	// RightAppliedIndex is the RHS's applied index after subsume — its
	// final log position; every replica waits for its local RHS to reach
	// it before absorbing.
	RightAppliedIndex uint64        `json:"right_applied"`
	RightSizeBytes    int64         `json:"right_size,omitempty"`
	RightGCThreshold  hlc.Timestamp `json:"right_gc,omitempty"`
}

// RunRangeMergeOnce merges adjacent underfull ranges: for every range this
// store leads, if its right neighbor is local, colocated (identical node
// sets), and both are below the merge threshold, drive the merge — first
// pulling the RHS's leadership here if needed. Also re-drives (or unwinds)
// merges interrupted by a crash. Called by the housekeeping loop.
func (s *Store) RunRangeMergeOnce(ctx context.Context) {
	threshold := s.cfg.MergeSizeThreshold
	if threshold < 0 {
		return
	}
	if threshold == 0 {
		threshold = s.cfg.SplitSizeThreshold / 4
	}
	s.VisitReplicas(func(lhs *Replica) bool {
		if ctx.Err() != nil {
			return false
		}
		if !lhs.IsLeader() || lhs.isFrozen() {
			return true
		}
		desc := lhs.Desc()
		rhs := s.replicaStartingAt(desc.EndKey)
		if rhs == nil {
			return true
		}
		rhsDesc := rhs.Desc()
		if !sameNodeSet(desc.Replicas, rhsDesc.Replicas) {
			if rhs.isFrozen() && rhs.mergedIntoRange() == lhs.rangeID {
				// Crashed mid-merge and membership diverged since (only
				// possible if the freeze landed after a conf change raced
				// it): unwind so the RHS serves again.
				s.tryUnfreeze(ctx, rhs)
			}
			return true
		}
		redrive := rhs.isFrozen() && rhs.mergedIntoRange() == lhs.rangeID
		if !redrive && (lhs.SizeBytes() >= threshold || rhs.SizeBytes() >= threshold) {
			return true
		}
		// Never undo a load split: skip merging while the pair is hot
		// (combined rate above a quarter of the split threshold) or while
		// either half was load-split within the settle window — fresh
		// trackers read ~0 QPS and would otherwise green-light an
		// immediate re-merge. Interrupted merges (redrive) always finish.
		if lt := s.loadSplitThreshold(); !redrive && lt > 0 {
			// Only mature (or test-injected) rates count: an immature
			// tracker's partial window would misread a bulk load as
			// sustained heat and wedge merging.
			var combined float64
			if lq, ok := s.effectiveQPS(lhs); ok {
				combined += lq
			}
			if rq, ok := s.effectiveQPS(rhs); ok {
				combined += rq
			}
			if combined > lt/4 {
				return true
			}
			settle := s.loadSettleWindow()
			if lhs.load.recentLoadSplit(settle) || rhs.load.recentLoadSplit(settle) {
				return true
			}
		}
		// Never merge ranges with differing retention policies: the merged
		// range GCs at one TTL and adopts the max of both GC thresholds, so
		// absorbing a short-retention neighbor could instantly put the
		// long-retention side's recent history below the threshold.
		if !redrive && s.cfg.RetentionOverride != nil {
			lt, lexp, lok := s.cfg.RetentionOverride(desc.StartKey, desc.EndKey)
			rt, rexp, rok := s.cfg.RetentionOverride(rhsDesc.StartKey, rhsDesc.EndKey)
			if lok != rok || lt != rt || lexp != rexp {
				return true
			}
		}
		if !rhs.isLeader() {
			// Pull the RHS's leadership to this node, then merge on a later
			// pass. TransferLeadership is forwarded to the current leader.
			r := rhs
			r.mu.Lock()
			lead := r.mu.leader
			r.mu.Unlock()
			if lead != 0 && lead != uint64(r.replicaID) {
				r.transferLeader(uint64(r.replicaID))
			}
			return true
		}
		if _, err := lhs.adminMerge(ctx); err != nil {
			log.Warnf("%s: merge with %s: %v", lhs.rangeID, rhsDesc.RangeID, err)
		}
		return true
	})
}

// replicaStartingAt returns this store's replica whose range starts exactly
// at key (the right neighbor for a merge), nil if absent.
func (s *Store) replicaStartingAt(key keys.Key) *Replica {
	var found *Replica
	s.VisitReplicas(func(r *Replica) bool {
		if r.Desc().StartKey.Equal(key) {
			found = r
			return false
		}
		return true
	})
	return found
}

func sameNodeSet(a, b []kvpb.ReplicaDescriptor) bool {
	if len(a) != len(b) {
		return false
	}
	nodes := map[int64]bool{}
	for _, r := range a {
		nodes[int64(r.NodeID)] = true
	}
	for _, r := range b {
		if !nodes[int64(r.NodeID)] {
			return false
		}
	}
	return true
}

// mergedIntoRange returns the range a frozen replica is being merged into.
func (r *Replica) mergedIntoRange() base.RangeID {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.mu.mergedInto
}

// adminMerge merges this (leading, unfrozen) range with its right
// neighbor, which must be local, led by this node, and colocated.
func (r *Replica) adminMerge(ctx context.Context) (*kvpb.AdminMergeResponse, *kvpb.Error) {
	desc := r.Desc()
	if r.isFrozen() {
		return nil, kvpb.NewErrorf("%s: cannot merge: range is itself frozen", r.rangeID)
	}
	rhs := r.store.replicaStartingAt(desc.EndKey)
	if rhs == nil {
		return nil, kvpb.NewErrorf("%s: no local right neighbor at %s", r.rangeID, desc.EndKey)
	}
	rhsDesc := rhs.Desc()
	if !sameNodeSet(desc.Replicas, rhsDesc.Replicas) {
		return nil, kvpb.NewErrorf("%s: cannot merge with %s: replica sets differ", r.rangeID, rhsDesc.RangeID)
	}
	if !rhs.isLeader() {
		return nil, kvpb.NewErrorf("%s: this node does not lead the right neighbor %s; transfer its lease here first", r.rangeID, rhsDesc.RangeID)
	}

	// Phase 1: freeze the RHS (idempotent for re-drives).
	sub := &kvpb.BatchRequest{Header: kvpb.BatchHeader{RangeID: rhsDesc.RangeID, Timestamp: r.store.cfg.Clock.Now()}}
	sub.Add(&kvpb.SubsumeRequest{
		RequestHeader: kvpb.RequestHeader{Key: rhsDesc.StartKey, EndKey: rhsDesc.EndKey},
		MergeInto:     r.rangeID,
	})
	if _, kerr := rhs.Execute(ctx, sub); kerr != nil {
		return nil, kerr
	}

	// Re-read both descriptors now that the subsume is the RHS's last
	// command. Anything captured before it can be stale: a split of the
	// RHS proposed just ahead of the subsume applies first, and the merge
	// must absorb the post-split span and generation — building the
	// merged descriptor from the earlier copy produced a range claiming
	// the split-off half's keys as well (issue #111). The LHS may have
	// moved too (a split of its own); its generation is re-checked at
	// apply, but the spans must still meet here.
	desc = r.Desc()
	rhsDesc = rhs.Desc()
	if !rhsDesc.StartKey.Equal(desc.EndKey) {
		return nil, kvpb.NewErrorf("%s: cannot merge: right neighbor %s now starts at %s, not at this range's end %s; retried by housekeeping",
			r.rangeID, rhsDesc.RangeID, rhsDesc.StartKey, desc.EndKey)
	}

	// Capture the RHS's final position: subsume was its last command.
	trig := &mergeTrigger{
		Left:              desc,
		Right:             rhsDesc,
		RightAppliedIndex: rhs.AppliedIndex(),
		RightSizeBytes:    rhs.SizeBytes(),
		RightGCThreshold:  rhs.GCThreshold(),
	}

	// Every RHS replica must hold the subsume before the merge is
	// proposed. The merge trigger waits at apply for the local RHS to
	// reach RightAppliedIndex, and once the merge applies on a quorum the
	// RHS group is detached there — a replica that had not yet learned
	// the subsume's commit (partitioned, restarting, a dropped MsgApp)
	// would then have no leader left to learn it from and would wait at
	// merge apply forever, wedging its raft loop. Confirming application
	// here keeps the RHS group alive until every member has caught up;
	// a straggler defers the merge, which housekeeping re-drives.
	if wait := r.store.cfg.WaitForApplication; wait != nil && !r.store.cfg.TestingKnobs.SkipMergeConfirmation {
		wctx, cancel := context.WithTimeout(ctx, mergeApplicationWait)
		defer cancel()
		for _, rep := range rhsDesc.Replicas {
			if rep.NodeID == r.store.cfg.NodeID {
				continue
			}
			if err := wait(wctx, rep.NodeID, rhsDesc.RangeID, trig.RightAppliedIndex); err != nil {
				return nil, kvpb.NewErrorf("%s: cannot merge %s yet: its replica on n%d has not applied the subsume: %v (re-driven by housekeeping)",
					r.rangeID, rhsDesc.RangeID, rep.NodeID, err)
			}
		}
	}
	gen := desc.Generation
	if rhsDesc.Generation > gen {
		gen = rhsDesc.Generation
	}
	trig.Merged = kvpb.RangeDescriptor{
		RangeID:       desc.RangeID,
		StartKey:      desc.StartKey.Clone(),
		EndKey:        rhsDesc.EndKey.Clone(),
		Replicas:      append([]kvpb.ReplicaDescriptor(nil), desc.Replicas...),
		NextReplicaID: desc.NextReplicaID,
		Generation:    gen + 1,
	}

	// Phase 2: the replicated merge on the LHS, serialized against all LHS
	// traffic like a split.
	guard, gerr := r.latches.Acquire(ctx, []latchSpan{wholeRangeSpan}, latchExclusive)
	if gerr != nil {
		return nil, kvpb.NewError(gerr)
	}
	if _, kerr := r.proposeCmd(ctx, &kvpb.BatchRequest{Header: kvpb.BatchHeader{RangeID: r.rangeID}}, cmdTriggers{merge: trig}); kerr != nil {
		guard.Release()
		return nil, kerr
	}
	guard.Release()

	// Repair the addressing records (outside the latch, like splits): the
	// merged record overwrites the RHS's (same end key); the old LHS record
	// is deleted. A failure is benign — a surviving old-LHS record routes
	// its span to the same range ID, and the pass re-cleans.
	sender := r.store.getSender()
	if sender != nil {
		if err := updateMetaRecords(ctx, sender, r.store.cfg.Clock.Now(), r.store.orderedMetaUpdates(),
			[]kvpb.RangeDescriptor{trig.Merged}, []kvpb.RangeDescriptor{trig.Left}); err != nil {
			log.Warnf("%s: merge committed but meta update failed: %v", r.rangeID, err)
		}
	}
	metrics.RangeMerges.Inc()
	log.Infof("merged %s into %s: now [%s, %s)", trig.Right.RangeID, r.rangeID, trig.Merged.StartKey, trig.Merged.EndKey)
	r.store.cfg.Events.Record("merge", "%s absorbed %s; now [%s, %s)", r.rangeID, trig.Right.RangeID, trig.Merged.StartKey, trig.Merged.EndKey)
	return &kvpb.AdminMergeResponse{Desc: trig.Merged}, nil
}

// tryUnfreeze abandons a stuck merge by clearing the RHS freeze (needs RHS
// leadership on this node; otherwise retried on a later pass).
func (s *Store) tryUnfreeze(ctx context.Context, rhs *Replica) {
	if !rhs.isLeader() {
		return
	}
	d := rhs.Desc()
	ba := &kvpb.BatchRequest{Header: kvpb.BatchHeader{RangeID: d.RangeID, Timestamp: s.cfg.Clock.Now()}}
	ba.Add(&kvpb.UnfreezeRequest{RequestHeader: kvpb.RequestHeader{Key: d.StartKey, EndKey: d.EndKey}})
	if _, kerr := rhs.Execute(ctx, ba); kerr != nil {
		log.Warnf("%s: unfreezing abandoned merge: %v", d.RangeID, kerr)
	} else {
		log.Infof("%s: abandoned merge unwound; range serves again", d.RangeID)
	}
}

// updateMetaRecords writes and deletes /meta addressing records in one
// atomic batch (all meta keys live on the meta range). deletes name the
// descriptors whose records go (the pre-merge LHS): with ordered set the
// delete applies only while the record still names that range at that
// generation or older, and the puts only if they advance the record
// (kvpb.UpdateMetaRequest); otherwise blind writes.
func updateMetaRecords(ctx context.Context, sender Sender, now hlc.Timestamp, ordered bool, puts, deletes []kvpb.RangeDescriptor) error {
	ba := &kvpb.BatchRequest{Header: kvpb.BatchHeader{Timestamp: now}}
	for i := range puts {
		d := puts[i]
		if ordered {
			ba.Add(&kvpb.UpdateMetaRequest{RequestHeader: kvpb.RequestHeader{Key: keys.RangeMetaKey(d.EndKey)}, Desc: &d})
			continue
		}
		raw, err := json.Marshal(d)
		if err != nil {
			return err
		}
		ba.Add(&kvpb.PutRequest{RequestHeader: kvpb.RequestHeader{Key: keys.RangeMetaKey(d.EndKey)}, Value: raw})
	}
	for _, d := range deletes {
		k := keys.RangeMetaKey(d.EndKey)
		if ordered {
			ba.Add(&kvpb.UpdateMetaRequest{RequestHeader: kvpb.RequestHeader{Key: k}, IfRangeID: d.RangeID, IfGeneration: d.Generation})
			continue
		}
		ba.Add(&kvpb.DeleteRequest{RequestHeader: kvpb.RequestHeader{Key: k}})
	}
	if _, kerr := sender.Send(ctx, ba); kerr != nil {
		return kerr
	}
	return nil
}

// stageMerge applies the merge on an LHS replica: wait out the local RHS,
// quiesce its group, and absorb its span, size, and GC threshold into the
// staged batch. Returns a deterministic error (the merge no-ops everywhere)
// when the LHS generation moved between propose and apply, and the
// NON-deterministic errApplyAborted when the node shuts down mid-wait or
// the local RHS's raft loop is dead short of the subsume — the caller then
// abandons the apply without advancing the applied index, so the restart
// replays the merge instead of skipping it.
func (r *Replica) stageMerge(ctx context.Context, b *storage.Batch, trig *mergeTrigger) error {
	if r.Desc().Generation != trig.Left.Generation {
		return kvpb.NewErrorf("%s: merge against generation %d but range is at %d; retried by housekeeping",
			r.rangeID, trig.Left.Generation, r.Desc().Generation)
	}
	rhs, ok := r.store.GetReplica(trig.Right.RangeID)
	if !ok {
		// The subsume committed and membership was frozen, so every LHS
		// replica's node holds an RHS replica until the merge deletes it —
		// and this log position is the first time it can be deleted.
		log.Fatalf("%s: merge apply: local RHS replica %s missing", r.rangeID, trig.Right.RangeID)
	}
	// The RHS's engine content is identical on every node once it reaches
	// the subsume index (its final command). The driver confirmed every
	// RHS replica had applied it before proposing the merge (adminMerge),
	// so on a live node this loop normally passes at once; it still
	// covers a replica whose RHS is re-applying after a restart. It must
	// NOT spin when nothing can advance the RHS any more: during node
	// shutdown (the RHS's raft loop may have exited before applying the
	// subsume; spinning wedges this replica's raft loop and, with it,
	// Stop() itself — issue #61), and when the RHS's raft loop died on
	// its own through an error, which leaves the replica in the store map
	// with its applied index frozen until a restart (issue #70). Both
	// abort: the applied index stays put and the restart replays this
	// command, waiting again with a live RHS group.
wait:
	for rhs.AppliedIndex() < trig.RightAppliedIndex {
		select {
		case <-ctx.Done():
			return errApplyAborted
		case <-rhs.stoppedCh:
			if ctx.Err() != nil {
				return errApplyAborted
			}
			// A merge is the only thing that detaches a live RHS, and this
			// is that merge — so the loop exited through an error. It may
			// have applied the subsume first; only a stop short of it is
			// fatal to this apply.
			if rhs.AppliedIndex() >= trig.RightAppliedIndex {
				break wait
			}
			return &applyAbortedError{msg: fmt.Sprintf(
				"%s: merge apply aborted: local RHS %s raft loop stopped at applied index %d, short of the subsume index %d; this replica is out of service until a restart replays the merge",
				r.rangeID, trig.Right.RangeID, rhs.AppliedIndex(), trig.RightAppliedIndex)}
		default:
		}
		time.Sleep(2 * time.Millisecond)
	}
	// The RHS is frozen at the subsume, so its descriptor here is final
	// and identical on every replica: the trigger must have been built
	// from it, or the merged span would not be the RHS's (deterministic
	// refusal; the frozen RHS is re-driven by housekeeping).
	if rd := rhs.Desc(); rd.Generation != trig.Right.Generation || !rd.EndKey.Equal(trig.Merged.EndKey) {
		return kvpb.NewErrorf("%s: merge trigger carries %s at generation %d ending at %s, but the frozen replica is at generation %d ending at %s; retried by housekeeping",
			r.rangeID, trig.Right.RangeID, trig.Right.Generation, trig.Merged.EndKey, rd.Generation, rd.EndKey)
	}
	// Stop the RHS group before its unreplicated keys go: a live Ready
	// handler could otherwise resurrect its HardState after our batch.
	// (This apply normally runs on an apply worker, so the RHS's pass —
	// even one grouped with this replica's — is waited out; only an
	// inline apply is the pass itself.)
	var from *Replica
	if r.inlineApply.Load() {
		from = r
	}
	r.store.detachReplica(trig.Right.RangeID, from)

	if err := PutRangeDescriptor(b, trig.Merged); err != nil {
		return err
	}
	pre := keys.RangeUnreplicatedPrefix(trig.Right.RangeID)
	if err := b.DeleteRange(pre, pre.PrefixEnd()); err != nil {
		return err
	}
	if err := b.Put(keys.RangeTombstoneKey(trig.Right.RangeID), []byte("merged")); err != nil {
		return err
	}
	r.mu.Lock()
	r.mu.sizeBytes += trig.RightSizeBytes
	if r.mu.gcThreshold.Less(trig.RightGCThreshold) {
		r.mu.gcThreshold = trig.RightGCThreshold
	}
	r.mu.Unlock()
	return nil
}

// finishMerge runs post-commit on every LHS replica: adopt the widened
// descriptor and close the timestamp-cache window over the absorbed span.
func (r *Replica) finishMerge(trig *mergeTrigger) {
	r.mu.Lock()
	r.mu.desc = trig.Merged
	r.mu.Unlock()
	r.rs.setConfState(trig.Merged)
	// The RHS leader's timestamp cache died with its group. The driver led
	// both sides on this clock, so now() bounds every read the RHS ever
	// served; the bump covers only the absorbed span — the LHS's own reads
	// are already tracked. On other replicas it is harmless, and a later
	// LHS leader gets the standard new-leader bump anyway.
	r.tsCache.Bump([]latchSpan{{Start: trig.Right.StartKey, End: trig.Right.EndKey}},
		r.store.cfg.Clock.Now(), uuid.Nil)
	log.Infof("merge applied: %s absorbed %s → [%s, %s)", r.rangeID, trig.Right.RangeID, trig.Merged.StartKey, trig.Merged.EndKey)
}

// detachReplica stops a replica's raft group and removes it from the store
// WITHOUT touching its data — the merge path, where the range's content now
// belongs to its neighbor. Compare removeReplica, which wipes. from is the
// replica whose apply is detaching (the LHS), so a pass that holds both
// sides does not wait for itself.
func (s *Store) detachReplica(rangeID base.RangeID, from *Replica) {
	s.mu.Lock()
	r, ok := s.mu.replicas[rangeID]
	if ok {
		delete(s.mu.replicas, rangeID)
	}
	s.mu.Unlock()
	if !ok {
		return
	}
	s.sched.stopReplica(r, from)
	r.mu.Lock()
	r.mu.destroyed = true
	r.mu.Unlock()
}
