package kvserver

import (
	"context"
	"encoding/json"
	"github.com/google/uuid"
	"time"

	"github.com/sthorne/datax/pkg/base"
	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/kvpb"
	"github.com/sthorne/datax/pkg/metrics"
	"github.com/sthorne/datax/pkg/storage"
	"github.com/sthorne/datax/pkg/util/hlc"
	"github.com/sthorne/datax/pkg/util/log"
)

// Splits. The split itself is a replicated command on the left-hand range:
// at apply time every replica atomically writes both descriptors, and after
// commit starts the right-hand side's Raft group locally (all RHS replicas
// live on the same stores as the LHS's, and no data moves — range bounds
// are logical). The leader then repairs the /meta addressing records; until
// that lands, stale routing is corrected by RangeKeyMismatch errors that
// carry fresh descriptors.

// adminSplit executes an AdminSplitRequest on the leader.
func (r *Replica) adminSplit(ctx context.Context, splitKey keys.Key) (*kvpb.AdminSplitResponse, *kvpb.Error) {
	if r.isFrozen() {
		return nil, kvpb.NewErrorf("%s: cannot split: range is frozen for a merge", r.rangeID)
	}
	desc := r.Desc()
	if splitKey.Equal(desc.StartKey) {
		return nil, kvpb.NewErrorf("%s: split key %s is already a range boundary", r.rangeID, splitKey)
	}
	if !desc.ContainsKey(splitKey) {
		e := kvpb.NewErrorf("%s: split key %s outside range", r.rangeID, splitKey)
		e.RangeKeyMismatch = &kvpb.RangeKeyMismatchError{RequestKey: splitKey, ActualDescriptors: []kvpb.RangeDescriptor{desc}}
		return nil, e
	}
	sender := r.store.getSender()
	if sender == nil {
		return nil, kvpb.NewErrorf("store has no client; cannot split")
	}

	// Allocate the new range ID through the replicated counter.
	inc := &kvpb.BatchRequest{Header: kvpb.BatchHeader{Timestamp: r.store.cfg.Clock.Now()}}
	inc.Add(&kvpb.IncrementRequest{RequestHeader: kvpb.RequestHeader{Key: keys.RangeIDGenKey()}, By: 1})
	ibr, kerr := sender.Send(ctx, inc)
	if kerr != nil {
		return nil, kerr
	}
	newID := base.RangeID(ibr.Responses[0].Increment.NewValue)

	left := desc
	left.StartKey = desc.StartKey.Clone()
	left.EndKey = splitKey.Clone()
	left.Generation = desc.Generation + 1
	right := kvpb.RangeDescriptor{
		RangeID:       newID,
		StartKey:      splitKey.Clone(),
		EndKey:        desc.EndKey.Clone(),
		Replicas:      append([]kvpb.ReplicaDescriptor(nil), desc.Replicas...),
		NextReplicaID: desc.NextReplicaID,
		Generation:    desc.Generation + 1,
	}
	// The RHS inherits the parent's closed timestamp: reads served at or
	// below it covered the RHS span, so no post-split write may land there.
	trig := &splitTrigger{Left: left, Right: right, ClosedTS: r.ClosedTimestamp()}
	if k := r.store.cfg.TestingKnobs.BeforeProposeDescriptorChange; k != nil {
		k(r.rangeID, "split")
	}

	// Serialize against everything: nothing may be in flight through the
	// old bounds while the split lands.
	guard, gerr := r.latches.Acquire(ctx, []latchSpan{wholeRangeSpan}, latchExclusive)
	if gerr != nil {
		return nil, kvpb.NewError(gerr)
	}
	if _, kerr := r.proposeCmd(ctx, &kvpb.BatchRequest{Header: kvpb.BatchHeader{RangeID: r.rangeID}}, cmdTriggers{split: trig}); kerr != nil {
		guard.Release()
		return nil, kerr
	}
	guard.Release()

	// Repair the addressing records (one batch, both on the meta range, so
	// the update is atomic). Done outside the latch: if the split was on
	// the meta-carrying range itself this would deadlock inside it.
	if err := putMetaRecords(ctx, sender, r.store.cfg.Clock.Now(), r.store.orderedMetaUpdates(), left, right); err != nil {
		// Routing still works via mismatch corrections; log and continue.
		log.Warnf("%s: split committed but meta update failed: %v", r.rangeID, err)
	}
	return &kvpb.AdminSplitResponse{Left: left, Right: right}, nil
}

// putMetaRecords writes the /meta addressing records for the given
// descriptors in one atomic batch (all meta keys live on the meta range).
// With ordered set, each write lands only if it advances the record's
// generation (kvpb.UpdateMetaRequest); otherwise it is a blind put.
func putMetaRecords(ctx context.Context, sender Sender, now hlc.Timestamp, ordered bool, descs ...kvpb.RangeDescriptor) error {
	ba := &kvpb.BatchRequest{Header: kvpb.BatchHeader{Timestamp: now}}
	for i := range descs {
		d := descs[i]
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
	if _, kerr := sender.Send(ctx, ba); kerr != nil {
		return kerr
	}
	return nil
}

// stageSplit applies the split on a replica. The trigger was built from
// the descriptor the leader saw when it was proposed; if the range has
// moved on since — a merge widened it, a replica change rewrote its
// membership — both sides of the trigger are wrong (the merged span's
// right part would belong to no range at all, or the new member would
// vanish from the descriptor), so the split is refused deterministically
// and the caller retries against the current descriptor (issue #200).
// stageMerge guards the same way with a different comparison: a split
// trigger's Left carries the generation the split PRODUCES, a merge
// trigger's Left the generation the merge was BUILT FROM (the pre-merge
// LHS), so the two sites cannot share one expression — see the comment
// there before "tidying" either into the other (that is mutant G1).
func (r *Replica) stageSplit(b *storage.Batch, trig *splitTrigger) error {
	if cur := r.Desc().Generation; staleDescriptorChange(cur, trig.Left.Generation) {
		metrics.DescriptorChangeRefusals.WithLabelValues("split").Inc()
		return kvpb.NewErrorf("%s: split against generation %d but range is at %d; the range changed under it, retry against the current descriptor",
			r.rangeID, trig.Left.Generation-1, cur)
	}
	if err := PutRangeDescriptor(b, trig.Left); err != nil {
		return err
	}
	if err := PutRangeDescriptor(b, trig.Right); err != nil {
		return err
	}
	// Recompute each side's exact data size from the staged state (the
	// batch view is identical on every replica, so the result is
	// deterministic), and seed the RHS's replicated state: it inherits the
	// GC threshold — its keyspace was subject to the LHS's GC.
	leftSize, err := spanSizeBytes(b, trig.Left.StartKey, trig.Left.EndKey)
	if err != nil {
		return err
	}
	rightSize, err := spanSizeBytes(b, trig.Right.StartKey, trig.Right.EndKey)
	if err != nil {
		return err
	}
	r.mu.Lock()
	thr := r.mu.gcThreshold
	r.mu.sizeBytes = leftSize
	r.mu.Unlock()
	return putReplicaState(b, trig.Right.RangeID, replicaState{
		GCThreshold: thr,
		SizeBytes:   rightSize,
		// The parent's closed timestamp covered the RHS span; carrying it
		// over keeps every future RHS leader (newReplica seeds its
		// timestamp cache floor from it) from admitting writes beneath
		// reads the parent already served there.
		ClosedTS: trig.ClosedTS,
	})
}

// staleDescriptorChange reports whether a descriptor change (split, merge
// or replica change) that produced generation produced was built from a
// descriptor other than the one the replica holds now, at current. Every
// change bumps the generation by exactly one, so a trigger built from the
// current descriptor produces current+1; anything else was built from an
// earlier descriptor and the range has changed under it. Pure, for tests.
func staleDescriptorChange(current, produced int64) bool {
	return produced != current+1
}

// spanSizeBytes sums the stored MVCC bytes of [start, end) as seen by r.
func spanSizeBytes(r storage.Reader, start, end keys.Key) (int64, error) {
	lower := storage.EncodeMVCCKey(start, hlc.Timestamp{})
	upper := storage.EncodeMVCCKey(end, hlc.Timestamp{})
	it := r.NewIter(lower, upper)
	var size int64
	for ok := it.SeekGE(lower); ok; ok = it.Next() {
		size += int64(len(it.Key()) + len(it.Value()))
	}
	return size, it.Close()
}

func (r *Replica) finishSplit(trig *splitTrigger) {
	r.mu.Lock()
	r.mu.desc = trig.Left
	r.mu.Unlock()
	r.rs.setConfState(trig.Left)

	rep, ok := trig.Right.GetReplica(r.store.cfg.NodeID)
	if !ok {
		return
	}
	// bootstrap=false: every replica of the RHS starts it the same way,
	// deriving the voter set from the descriptor (like a restart) instead
	// of replaying StartNode's conf-change entries — which would also block
	// the immediate campaign below.
	rhs, err := r.store.startReplica(trig.Right, rep.ReplicaID, false)
	if err != nil {
		log.Errorf("%s: starting RHS replica after split: %v", trig.Right.RangeID, err)
		return
	}
	// The parent served reads on the RHS span up to this moment, and only
	// those at or below its closed timestamp — which trails now() by the
	// closed-timestamp lag — reached the RHS through the trigger; the rest
	// live in the parent's in-memory timestamp cache and would be lost.
	// So the RHS's cache floor is bumped to now(), on every replica (the
	// cache is per-replica state, and whichever replica leads must refuse
	// a write beneath a read the parent served), as finishMerge does for
	// an absorbed span (issue #134). The RHS's first leader is at term 1
	// and takes no new-leader bump of its own.
	rhs.tsCache.Bump([]latchSpan{wholeRangeSpan}, r.store.cfg.Clock.Now(), uuid.Nil)
	// The LHS leader campaigns for the RHS immediately: without this the
	// RHS sits leaderless for an election timeout. (The floor bump above
	// pushes a transaction that began before the split and writes to the
	// RHS after it — once, the same as a leadership change would.) The
	// first attempts can fail while peers have not applied the split yet
	// (their replicas don't exist to vote), so retry briefly.
	if r.isLeader() {
		_ = r.store.cfg.Stopper.RunWorker(func(ctx context.Context) {
			for i := 0; i < 20; i++ {
				if rhs.hasLeader() {
					return
				}
				if err := rhs.campaign(); err != nil {
					log.Debugf("%s: campaign after split: %v", trig.Right.RangeID, err)
				}
				select {
				case <-ctx.Done():
					return
				case <-time.After(100 * time.Millisecond):
				}
			}
		})
	}
	log.Infof("split %s at %s → %s [%s, %s)", r.rangeID, trig.Left.EndKey, trig.Right.RangeID, trig.Right.StartKey, trig.Right.EndKey)
	recordKeyed(r.store.cfg.Events, "split", "%s split at %s; %s takes [%s, %s)", r.rangeID, trig.Left.EndKey, trig.Right.RangeID, trig.Right.StartKey, trig.Right.EndKey)
}
