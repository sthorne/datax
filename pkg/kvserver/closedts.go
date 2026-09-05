package kvserver

import (
	"context"
	"time"

	"github.com/google/uuid"
	"go.etcd.io/raft/v3"

	"github.com/sthorne/datax/pkg/base"
	"github.com/sthorne/datax/pkg/kvpb"
	"github.com/sthorne/datax/pkg/metrics"
	"github.com/sthorne/datax/pkg/util/hlc"
	"github.com/sthorne/datax/pkg/util/log"
)

// Closed timestamps enable follower reads (issue #5): the leader
// periodically promises "no write at or below T will ever commit on this
// range", and any replica may then serve reads at or below T locally.
//
// The promise rides the raft log itself as a tiny replicated command
// rather than a side channel, which buys two properties for free:
//
//   - The "applied index has caught up to the publication" condition is
//     log order: by the time a replica applies the closed-ts command,
//     every write below T has applied too. No index bookkeeping.
//   - The closed timestamp is replicated state (persisted in
//     replicaState), so it survives restarts and leader failure — a
//     follower keeps serving reads below T with the leader gone, which is
//     the acceptance bar for the issue.
//
// Publication correctness on the leader (nothing may sneak beneath T):
//
//  1. Acquire a whole-range SHARED latch. Writes hold exclusive latches
//     from their timestamp-cache check until they apply (invariant L1),
//     so acquisition drains every in-flight write — their log entries
//     precede the closed-ts command — while readers are undisturbed.
//  2. Bump the timestamp-cache floor to T under the latch. Every write
//     checked afterwards is forwarded above T (transactional writes get
//     pushed, non-transactional ones bounced — the standard machinery).
//  3. Release and propose. Post-bump writes are above T, so their log
//     position relative to the command is irrelevant.
//
// EndTxn is exempt from the timestamp-cache check (it writes no MVCC
// versions), so a transaction that wrote intents BEFORE the bump could
// still commit at ≤ T and resolution would move its versions to a commit
// timestamp ≤ T. That is safe for followers because those intents applied
// before the publication (step 1 drained them): a follower read at ≤ T
// either sees the intent (and bails to the leader) or sees the resolved
// committed value — never a miss that later materializes.
const (
	defaultClosedTSLag      = 3 * time.Second
	defaultClosedTSInterval = time.Second
)

// StartClosedTimestamps starts the store's closed-timestamp publisher. A
// negative lag disables publication (and with it follower reads).
func (s *Store) StartClosedTimestamps() error {
	lag := s.cfg.ClosedTimestampLag
	if lag < 0 {
		return nil
	}
	if lag == 0 {
		lag = defaultClosedTSLag
	}
	interval := s.cfg.ClosedTimestampInterval
	if interval <= 0 {
		interval = defaultClosedTSInterval
	}
	return s.cfg.Stopper.RunWorker(func(ctx context.Context) {
		t := time.NewTicker(interval)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				s.publishClosedTimestamps(ctx, lag)
			}
		}
	})
}

// publishClosedTimestamps is one publication round over the ranges this
// store leads. An awake range whose log grew since its last logged
// promise proposes the promise (publishClosedTimestamp); an awake one
// whose log did not sends it off the log per range
// (publishClosedTimestampSide); the QUIESCENT ranges share ONE group
// promise per follower node (the off-log transport, below), so an idle
// store's publication cost is a few envelopes a second however many
// ranges it holds.
//
// The off-log transport for quiescent ranges. A quiescent leader has no
// write in flight (any would have woken it first) and its log and term
// cannot change while it sleeps, so its promise is the same tuple every
// round. It REGISTERS once per follower node — a per-range entry with the
// term, the last index and an explicit promise made under the usual
// latch drain and cache bump — and thereafter the store's round sends
// each follower node one GROUP entry, "every range you hold registered
// from me is closed at T", which the follower applies to its registry,
// re-validating each range (same leader and term, applied index) and
// dropping one that fails. A range that wakes is dropped: its wake goes
// out as a per-range entry in the same envelope, ahead of the group
// promise, and until the followers see it the leader honors every group
// promise it may still be applying by forwarding its own timestamp-cache
// floor to the store's latest promise on wake (unquiesce) — the promise
// is advanced before the woken set is collected, so a wake either lands
// in the set or bumps to the promise being sent.
func (s *Store) publishClosedTimestamps(ctx context.Context, lag time.Duration) {
	target := s.cfg.Clock.Now().AddNanos(-lag.Nanoseconds())
	coalesced := s.coalescedHeartbeats()
	var woken map[base.RangeID]struct{}
	if coalesced {
		// Promise first, then collect the wakes (see above).
		s.side.Lock()
		if s.side.groupTS.Load().Less(target) {
			t := target
			s.side.groupTS.Store(&t)
		}
		woken, s.side.woken = s.side.woken, make(map[base.RangeID]struct{})
		s.side.Unlock()
	}
	for id := range woken {
		r, ok := s.GetReplica(id)
		if !ok {
			continue
		}
		self := uint64(r.replicaID)
		for _, rep := range r.Desc().Replicas {
			if uint64(rep.ReplicaID) != self {
				s.queueClosedTimestamp(rep.NodeID, RaftHeartbeat{RangeID: id, To: uint64(rep.ReplicaID), From: self})
			}
		}
	}
	dests := map[base.NodeID]struct{}{}
	s.VisitReplicas(func(r *Replica) bool {
		if err := ctx.Err(); err != nil {
			return false
		}
		if !r.isLeader() || r.isFrozen() {
			return true
		}
		if coalesced && r.isQuiescent() {
			r.registerClosedTimestamp(ctx, target, dests)
			return true
		}
		if !r.ClosedTimestamp().Less(target) {
			return true
		}
		pctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		var err error
		if r.closedTimestampNeedsLog() || !coalesced {
			err = r.publishClosedTimestamp(pctx, target)
		} else {
			err = r.publishClosedTimestampSide(pctx, target)
		}
		if err != nil {
			log.Debugf("%s: closed timestamp publication failed: %v", r.rangeID, err)
		}
		cancel()
		return true
	})
	for to := range dests {
		s.queueClosedTimestamp(to, RaftHeartbeat{ClosedTS: target})
	}
	if len(dests) > 0 {
		metrics.ClosedTimestampGroupUpdates.Add(float64(len(dests)))
	}
	s.sendQueuedHeartbeats(ctx)
}

// registerClosedTimestamp enrolls a quiescent leader with any follower
// node that does not hold its registration yet (a per-range promise made
// like the awake side path), and names every follower node in dests so
// the round's group promise reaches it.
func (r *Replica) registerClosedTimestamp(ctx context.Context, target hlc.Timestamp, dests map[base.NodeID]struct{}) {
	r.mu.Lock()
	desc := r.mu.desc
	var missing []kvpb.ReplicaDescriptor
	for _, rep := range desc.Replicas {
		if uint64(rep.ReplicaID) == uint64(r.replicaID) {
			continue
		}
		dests[rep.NodeID] = struct{}{}
		if !r.mu.sideRegistered[rep.NodeID] {
			missing = append(missing, rep)
		}
	}
	r.mu.Unlock()
	if len(missing) == 0 {
		return
	}
	var st raft.BasicStatus
	if err := r.withRaftGroup(func(rn *raft.RawNode) error {
		st = rn.BasicStatus()
		return nil
	}); err != nil || st.RaftState != raft.StateLeader {
		return
	}
	guard, gerr := r.latches.Acquire(ctx, []latchSpan{wholeRangeSpan}, latchShared)
	if gerr != nil {
		return
	}
	r.tsCache.Bump([]latchSpan{wholeRangeSpan}, target, uuid.Nil)
	guard.Release()
	index := r.rs.lastIndex()
	r.mu.Lock()
	if !r.mu.quiescent {
		// Woken meanwhile: the wake is queued for the next round; a
		// registration now would outlive it.
		r.mu.Unlock()
		return
	}
	if r.mu.sideClosedTS.Less(target) {
		r.mu.sideClosedTS = target
	}
	if r.mu.sideRegistered == nil {
		r.mu.sideRegistered = make(map[base.NodeID]bool)
	}
	for _, rep := range missing {
		r.mu.sideRegistered[rep.NodeID] = true
	}
	r.mu.Unlock()
	self := uint64(r.replicaID)
	for _, rep := range missing {
		r.store.queueClosedTimestamp(rep.NodeID, RaftHeartbeat{
			RangeID: r.rangeID, To: uint64(rep.ReplicaID), From: self, Term: st.Term, Index: index, ClosedTS: target,
		})
	}
	metrics.ClosedTimestampSideUpdates.Add(float64(len(missing)))
}

// handleClosedTimestamps applies a peer node's off-log entries, in order:
// a per-range entry with a promise registers (or refreshes) the range
// and applies it; one without a promise is a wake and drops the
// registration; one with no range is the group promise for every range
// still registered from that node.
func (s *Store) handleClosedTimestamps(from base.NodeID, closed []RaftHeartbeat) {
	s.side.Lock()
	reg := s.side.reg[from]
	if reg == nil {
		reg = make(map[base.RangeID]RaftHeartbeat)
		s.side.reg[from] = reg
	}
	s.side.Unlock()
	for _, hb := range closed {
		switch {
		case hb.RangeID == 0:
			s.side.Lock()
			ids := make([]base.RangeID, 0, len(reg))
			for id := range reg {
				ids = append(ids, id)
			}
			s.side.Unlock()
			for _, id := range ids {
				s.side.Lock()
				entry, ok := reg[id]
				s.side.Unlock()
				if !ok {
					continue
				}
				entry.ClosedTS = hb.ClosedTS
				r, live := s.GetReplica(id)
				if !live || !r.acceptClosedTimestamp(entry) {
					s.side.Lock()
					delete(reg, id)
					s.side.Unlock()
				}
			}
		case hb.ClosedTS.IsEmpty():
			s.side.Lock()
			delete(reg, hb.RangeID)
			s.side.Unlock()
		default:
			if r, ok := s.GetReplica(hb.RangeID); ok && r.acceptClosedTimestamp(hb) && hb.Index > 0 {
				s.side.Lock()
				reg[hb.RangeID] = hb
				s.side.Unlock()
			}
		}
	}
}

// publishClosedTimestampSide closes a range whose log has not grown since
// the last logged promise, without appending or waking it: the same
// latch drain and cache bump as the log path make the promise, and it
// travels to the followers in the next coalesced envelope instead of a
// raft command (quiesce.go). A follower honors it only while it still
// follows this leader at this term and has applied the index the promise
// was made at — raft's vote lease means no other leader can have
// committed anything it has not heard of while it still follows this
// one, which is what log order gave the replicated path. The value lives
// in memory only (sideClosedTS), so the replicated state stays
// byte-identical across replicas and a restart falls back to the last
// logged promise (re-learned within a publication interval). The first
// promise after new entries rides the log again.
func (r *Replica) publishClosedTimestampSide(ctx context.Context, target hlc.Timestamp) error {
	st, ok := r.raftStatus()
	if !ok || st.RaftState != raft.StateLeader {
		return nil
	}
	guard, gerr := r.latches.Acquire(ctx, []latchSpan{wholeRangeSpan}, latchShared)
	if gerr != nil {
		return gerr
	}
	r.tsCache.Bump([]latchSpan{wholeRangeSpan}, target, uuid.Nil)
	guard.Release()
	index := r.rs.lastIndex()
	r.mu.Lock()
	if r.mu.sideClosedTS.Less(target) {
		r.mu.sideClosedTS = target
	}
	desc := r.mu.desc
	r.mu.Unlock()
	self := uint64(r.replicaID)
	for _, rep := range desc.Replicas {
		if uint64(rep.ReplicaID) == self {
			continue
		}
		r.store.queueClosedTimestamp(rep.NodeID, RaftHeartbeat{
			RangeID: r.rangeID, To: uint64(rep.ReplicaID), From: self, Term: st.Term, Index: index, ClosedTS: target,
		})
	}
	metrics.ClosedTimestampSideUpdates.Inc()
	return nil
}

// acceptClosedTimestamp adopts a leader's off-log promise when it still
// applies here: same leader and term (raft's vote lease means no other
// leader can have committed anything this replica has not heard of while
// it still follows this one), and this replica has applied everything
// up to the index the promise was made at.
func (r *Replica) acceptClosedTimestamp(hb RaftHeartbeat) bool {
	valid := false
	_ = r.withRaftGroup(func(rn *raft.RawNode) error {
		st := rn.BasicStatus()
		valid = st.RaftState == raft.StateFollower && st.Term == hb.Term && st.Lead == hb.From
		return nil
	})
	if !valid {
		return false
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.mu.appliedIndex < hb.Index || r.mu.frozen {
		return false
	}
	if r.mu.sideClosedTS.Less(hb.ClosedTS) {
		r.mu.sideClosedTS = hb.ClosedTS
	}
	return true
}

// publishClosedTimestamp closes the range at target: after it returns
// successfully, no write at or below target will ever commit here.
func (r *Replica) publishClosedTimestamp(ctx context.Context, target hlc.Timestamp) error {
	// Step 1: drain in-flight writes (they hold exclusive latches until
	// applied); readers share fine.
	guard, gerr := r.latches.Acquire(ctx, []latchSpan{wholeRangeSpan}, latchShared)
	if gerr != nil {
		return gerr
	}
	// Step 2: from here on no write may pass the cache at or below target.
	r.tsCache.Bump([]latchSpan{wholeRangeSpan}, target, uuid.Nil)
	guard.Release()
	// Step 3: replicate. Log order does the rest.
	_, kerr := r.proposeCmd(ctx, &kvpb.BatchRequest{Header: kvpb.BatchHeader{RangeID: r.rangeID}}, cmdTriggers{closedTS: target})
	if kerr != nil {
		return kerr
	}
	idx := r.rs.lastIndex()
	r.mu.Lock()
	r.mu.closedTSLogged = idx
	r.mu.Unlock()
	return nil
}

// closedTimestampNeedsLog reports whether the next promise must ride the
// log: only when entries were appended since the last logged promise. A
// range with nothing new in its log publishes off the log instead
// (publishClosedTimestampSide) — no entry, no fsync, no wake — which is
// what lets an idle range stay quiescent while its followers keep
// serving fresh stale reads.
func (r *Replica) closedTimestampNeedsLog() bool {
	idx := r.rs.lastIndex()
	r.mu.Lock()
	defer r.mu.Unlock()
	return idx != r.mu.closedTSLogged
}

// executeStaleRead serves a read-only batch pinned at a fixed timestamp on
// a NON-leader replica, legal exactly when the timestamp is at or below
// the range's closed timestamp. No latches: entries apply here without
// latching anyway, and everything at or below the closed timestamp is
// immutable (intent resolution only moves versions to commit timestamps
// above it). No timestamp-cache bump either — the closed timestamp already
// keeps every future write above this read.
func (r *Replica) executeStaleRead(ba *kvpb.BatchRequest) (*kvpb.BatchResponse, *kvpb.Error) {
	if err := r.checkKeyBounds(ba); err != nil {
		return nil, err
	}
	if err := r.checkFrozen(ba); err != nil {
		return nil, err
	}
	ts := readTimestamp(ba)
	if thr := r.GCThreshold(); !thr.IsEmpty() && ts.LessEq(thr) {
		return nil, kvpb.NewErrorf("%s: batch timestamp %s is below the GC threshold %s", r.rangeID, ts, thr)
	}
	if closed := r.ClosedTimestamp(); closed.Less(ts) {
		// Not provably closed here (yet): the leader serves it.
		return nil, r.notLeaderError()
	}
	br, rerr := r.evalReadOnly(ba)
	if rerr != nil {
		if rerr.WriteIntent != nil {
			// Conflict machinery (pushes, resolution) is the leader's;
			// redirect rather than looping on an intent only the leader's
			// path can clear.
			return nil, r.notLeaderError()
		}
		return nil, rerr
	}
	metrics.FollowerReads.Inc()
	return br, nil
}
