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
	"github.com/sthorne/datax/pkg/version"
)

// Splits. The split itself is a replicated command on the left-hand range:
// at apply time every replica atomically writes both descriptors, and after
// commit starts the right-hand side's Raft group locally (all RHS replicas
// live on the same stores as the LHS's, and no data moves — range bounds
// are logical). The leader then repairs the /meta addressing records; until
// that lands, stale routing is corrected by RangeKeyMismatch errors that
// carry fresh descriptors.

// maxDescriptorChangeAttempts bounds how often an admin split (or a
// sticky-bit update) rebuilds its change after the range moved under it.
// Each retry means some other descriptor change landed in between, so the
// loop only spins while the range is being reshaped by others; the bound
// is there so a caller is told rather than held.
const maxDescriptorChangeAttempts = 10

// frozenError is the refusal of a request on a range frozen for a merge:
// a RangeKeyMismatch marked Frozen, which the KV client waits out on the
// same descriptor and then follows to the merged range (or back here, if
// the merge is abandoned) — see checkFrozen.
func (r *Replica) frozenError(key keys.Key, format string, args ...any) *kvpb.Error {
	e := kvpb.NewErrorf(format, args...)
	e.RangeKeyMismatch = &kvpb.RangeKeyMismatchError{RequestKey: key, Frozen: true}
	return e
}

// descriptorChangedError is the refusal of a descriptor change that was
// built from generation builtFrom when the range is at current.
func (r *Replica) descriptorChangedError(op string, builtFrom, current int64) *kvpb.Error {
	e := kvpb.NewErrorf("%s: %s against generation %d but range is at %d; the range changed under it, retry against the current descriptor",
		r.rangeID, op, builtFrom, current)
	e.DescriptorChanged = &kvpb.DescriptorChangedError{RangeID: r.rangeID, BuiltFrom: builtFrom, Current: current}
	return e
}

// adminSplit executes an AdminSplitRequest on the leader.
//
// What the caller asks for is a state — a range boundary at the key, and
// for a manual split a sticky one — so the request stays meaningful when
// the range changes between reading its descriptor and applying the
// split. A split refused at apply for that reason is rebuilt here from
// the current descriptor (issue #290: a statement's split used to fail
// with "split against generation 0 but range is at 1" when another split
// or a merge landed first): the key may still be inside the range (split
// again), may have become its start (the boundary exists; done), or may
// have left it (a RangeKeyMismatch sends the client to the range that
// holds it now). A range frozen for a merge answers the way it answers
// any request, so the client waits the merge out and asks again.
func (r *Replica) adminSplit(ctx context.Context, req *kvpb.AdminSplitRequest) (*kvpb.AdminSplitResponse, *kvpb.Error) {
	splitKey := req.Key
	stickyUntil := req.StickyUntil
	if !r.store.stickyBitsOK() {
		stickyUntil = 0
	}
	sender := r.store.getSender()
	if sender == nil {
		return nil, kvpb.NewErrorf("store has no client; cannot split")
	}
	var newID base.RangeID
	var lastErr *kvpb.Error
	for attempt := 0; attempt < maxDescriptorChangeAttempts; attempt++ {
		if err := ctx.Err(); err != nil {
			return nil, kvpb.NewError(err)
		}
		if r.isFrozen() {
			return nil, r.frozenError(splitKey, "%s: cannot split: range is frozen for a merge", r.rangeID)
		}
		desc := r.Desc()
		if splitKey.Equal(desc.StartKey) {
			// The boundary is there already: the state asked for. A
			// manual split still has its bit to record.
			if stickyUntil != 0 && desc.StickyUntil != stickyUntil {
				updated, kerr := r.setStickyBit(ctx, stickyUntil)
				if kerr != nil {
					return nil, kerr
				}
				desc = updated
			}
			return &kvpb.AdminSplitResponse{Right: desc}, nil
		}
		if !desc.ContainsKey(splitKey) {
			e := kvpb.NewErrorf("%s: split key %s outside range", r.rangeID, splitKey)
			e.RangeKeyMismatch = &kvpb.RangeKeyMismatchError{RequestKey: splitKey, ActualDescriptors: []kvpb.RangeDescriptor{desc}}
			return nil, e
		}

		// Allocate the new range ID through the replicated counter, once:
		// a retry reuses it (the refused split created nothing).
		if newID == 0 {
			inc := &kvpb.BatchRequest{Header: kvpb.BatchHeader{Timestamp: r.store.cfg.Clock.Now()}}
			inc.Add(&kvpb.IncrementRequest{RequestHeader: kvpb.RequestHeader{Key: keys.RangeIDGenKey()}, By: 1})
			ibr, kerr := sender.Send(ctx, inc)
			if kerr != nil {
				return nil, kerr
			}
			newID = base.RangeID(ibr.Responses[0].Increment.NewValue)
		}

		// The left half keeps the parent's sticky bit — it describes the
		// start boundary, which does not move. The right half's start
		// boundary is the one being made, sticky only if asked for.
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
			StickyUntil:   stickyUntil,
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
		_, kerr := r.proposeCmd(ctx, &kvpb.BatchRequest{Header: kvpb.BatchHeader{RangeID: r.rangeID}}, cmdTriggers{split: trig})
		guard.Release()
		if kerr != nil {
			if kerr.DescriptorChanged != nil {
				// Refused at apply on every replica: nothing was split.
				// Look at the range again.
				lastErr = kerr
				continue
			}
			return nil, kerr
		}

		// Repair the addressing records (one batch, both on the meta range, so
		// the update is atomic). Done outside the latch: if the split was on
		// the meta-carrying range itself this would deadlock inside it.
		if err := putMetaRecords(ctx, sender, r.store.cfg.Clock.Now(), r.store.orderedMetaUpdates(), left, right); err != nil {
			// Routing still works via mismatch corrections; log and continue.
			log.Warnf("%s: split committed but meta update failed: %v", r.rangeID, err)
		}
		return &kvpb.AdminSplitResponse{Left: left, Right: right}, nil
	}
	return nil, lastErr
}

// stickyBitsOK reports whether a descriptor may carry a sticky bit and a
// descriptor-update trigger may be proposed (cluster version v20): an
// older replica would drop the one and ignore the other.
func (s *Store) stickyBitsOK() bool {
	if s.cfg.ClusterVersion == nil {
		return true
	}
	return s.cfg.ClusterVersion() >= version.V20
}

// descriptorTrigger is carried by the replicated command that replaces a
// range's descriptor without changing its bounds or membership — today,
// setting or clearing the sticky bit. Desc is the whole new descriptor,
// one generation past the one it was built from.
type descriptorTrigger struct {
	Desc kvpb.RangeDescriptor `json:"desc"`
}

// setStickyBit replicates a new value of the range's sticky bit (0 clears
// it) and repairs the addressing record, returning the descriptor as it
// stands afterwards. The update is a descriptor change like any other: it
// takes a generation, so a split or merge built from the descriptor
// before it is refused instead of silently writing the old bit back, and
// it is itself refused — and rebuilt here — if one of those lands first.
// It stops, returning the current descriptor untouched, if the range's
// start key moved in the meantime (only a merge of this range away does
// that, and then the replica is gone).
func (r *Replica) setStickyBit(ctx context.Context, stickyUntil int64) (kvpb.RangeDescriptor, *kvpb.Error) {
	if !r.store.stickyBitsOK() {
		return kvpb.RangeDescriptor{}, kvpb.NewErrorf("%s: sticky range boundaries need cluster version %s (finalize the upgrade first)", r.rangeID, version.V20)
	}
	var lastErr *kvpb.Error
	for attempt := 0; attempt < maxDescriptorChangeAttempts; attempt++ {
		if err := ctx.Err(); err != nil {
			return kvpb.RangeDescriptor{}, kvpb.NewError(err)
		}
		desc := r.Desc()
		if r.isFrozen() {
			return desc, r.frozenError(desc.StartKey, "%s: cannot change the sticky bit: range is frozen for a merge", r.rangeID)
		}
		if desc.StickyUntil == stickyUntil {
			return desc, nil
		}
		next := desc
		next.Generation = desc.Generation + 1
		next.StickyUntil = stickyUntil
		if k := r.store.cfg.TestingKnobs.BeforeProposeDescriptorChange; k != nil {
			k(r.rangeID, "sticky")
		}
		_, kerr := r.proposeCmd(ctx, &kvpb.BatchRequest{Header: kvpb.BatchHeader{RangeID: r.rangeID}}, cmdTriggers{descriptor: &descriptorTrigger{Desc: next}})
		if kerr != nil {
			if kerr.DescriptorChanged != nil {
				lastErr = kerr
				continue
			}
			return desc, kerr
		}
		if sender := r.store.getSender(); sender != nil {
			if err := putMetaRecords(ctx, sender, r.store.cfg.Clock.Now(), r.store.orderedMetaUpdates(), next); err != nil {
				log.Warnf("%s: sticky bit committed but meta update failed: %v", r.rangeID, err)
			}
		}
		return next, nil
	}
	return r.Desc(), lastErr
}

// adminUnsplit executes an AdminUnsplitRequest on the leader: the range
// starting at the key gives up its sticky bit, and the merge queue is
// free to fold it into its left neighbor when both are small. Nothing
// merges here — the boundary simply stops being held.
func (r *Replica) adminUnsplit(ctx context.Context, req *kvpb.AdminUnsplitRequest) (*kvpb.AdminUnsplitResponse, *kvpb.Error) {
	desc := r.Desc()
	if !req.Key.Equal(desc.StartKey) {
		if !desc.ContainsKey(req.Key) {
			e := kvpb.NewErrorf("%s: key %s outside range", r.rangeID, req.Key)
			e.RangeKeyMismatch = &kvpb.RangeKeyMismatchError{RequestKey: req.Key, ActualDescriptors: []kvpb.RangeDescriptor{desc}}
			return nil, e
		}
		return nil, kvpb.NewErrorf("%s: cannot unsplit at %s: not a range boundary (the range containing it starts at %s)", r.rangeID, req.Key, desc.StartKey)
	}
	updated, kerr := r.setStickyBit(ctx, 0)
	if kerr != nil {
		return nil, kerr
	}
	return &kvpb.AdminUnsplitResponse{Desc: updated}, nil
}

// stageDescriptorUpdate applies a descriptorTrigger on a replica, refused
// deterministically — like a split — if the range is frozen for a merge
// or is no longer at the generation the update was built from.
func (r *Replica) stageDescriptorUpdate(b *storage.Batch, trig *descriptorTrigger) error {
	if r.isFrozen() {
		metrics.DescriptorChangeRefusals.WithLabelValues("sticky").Inc()
		return r.frozenError(trig.Desc.StartKey, "%s: cannot change the sticky bit: range is frozen for a merge (the freeze applied after the change was proposed)", r.rangeID)
	}
	if cur := r.Desc().Generation; staleDescriptorChange(cur, trig.Desc.Generation) {
		metrics.DescriptorChangeRefusals.WithLabelValues("sticky").Inc()
		return r.descriptorChangedError("sticky-bit update", trig.Desc.Generation-1, cur)
	}
	return PutRangeDescriptor(b, trig.Desc)
}

func (r *Replica) finishDescriptorUpdate(trig *descriptorTrigger) {
	r.mu.Lock()
	r.mu.desc = trig.Desc
	r.mu.Unlock()
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
// and the caller retries against the current descriptor (issue #200;
// adminSplit does, issue #290).
// stageMerge guards the same way with a different comparison: a split
// trigger's Left carries the generation the split PRODUCES, a merge
// trigger's Left the generation the merge was BUILT FROM (the pre-merge
// LHS), so the two sites cannot share one expression — see the comment
// there before "tidying" either into the other (that is mutant G1).
func (r *Replica) stageSplit(b *storage.Batch, trig *splitTrigger) error {
	// A split proposed before a Subsume applied lands on a frozen range.
	// The freeze is replicated state (set by the Subsume's apply, in log
	// order, on every replica), so this refusal is deterministic — and
	// necessary: the merge driver read the frozen range's descriptor as
	// its final one, and a split applied after that would make the merge
	// absorb a span the split had already given to a new range (issue
	// #263's CI run: r1 absorbed a pre-split r5 while r6 held half of
	// it, and /meta kept a record no replica answered for). adminSplit's
	// own frozen check runs before proposing and cannot see a freeze
	// that applies in between.
	if r.isFrozen() {
		metrics.DescriptorChangeRefusals.WithLabelValues("split").Inc()
		return r.frozenError(trig.Right.StartKey, "%s: cannot split: range is frozen for a merge (the freeze applied after the split was proposed); retry once the merge lands", r.rangeID)
	}
	if cur := r.Desc().Generation; staleDescriptorChange(cur, trig.Left.Generation) {
		metrics.DescriptorChangeRefusals.WithLabelValues("split").Inc()
		return r.descriptorChangedError("split", trig.Left.Generation-1, cur)
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
