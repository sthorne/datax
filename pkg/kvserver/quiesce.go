package kvserver

import (
	"context"
	"sync"
	"time"

	"github.com/google/uuid"

	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
	"go.etcd.io/raft/v3/tracker"

	"github.com/sthorne/datax/pkg/base"
	"github.com/sthorne/datax/pkg/metrics"
	"github.com/sthorne/datax/pkg/util/hlc"
	"github.com/sthorne/datax/pkg/util/log"
	"github.com/sthorne/datax/pkg/version"
)

// Range quiescence and coalesced heartbeats (issue #102, part c; cluster
// version v12).
//
// Heartbeats are the one cost that scales with the number of ranges on a
// node whatever the workload: each leader beats each follower every three
// ticks and each follower answers each beat. Two changes make that cost a
// constant per peer node:
//
//   - Coalescing: a heartbeat or a response is not sent as its own
//     envelope; it is queued per destination node and every scheduler pass
//     flushes the queue as ONE RaftEnvelope carrying every range's
//     heartbeats (RaftHeartbeat, the five fields raft reads). The receiver
//     fans them out to its replicas. Heartbeats carrying a read-index
//     context (quorum reads, DisableLeaseReads) keep their own envelope.
//
//   - Quiescence: a leader that has seen no proposal, read-index request,
//     snapshot or lagging follower for quiesceAfterTicks ticks, with every
//     follower holding its whole log and answering recently, tells its
//     followers it is going idle (a heartbeat with Quiesce set) and stops
//     ticking; a follower that holds the leader's commit index stops
//     ticking too. Nobody sends anything for an idle range, and no
//     election timer runs — a quiesced follower cannot campaign, so the
//     leader's lease reads stay safe once contact is re-established. A
//     replica wakes on any raft message other than a heartbeat response,
//     on a proposal, read-index or leadership request, and on a client
//     request landing on it (a follower woken by a request meant for a
//     dead leader ticks, times out and campaigns, so a partitioned-away
//     leader is still replaced on demand). A woken leader heartbeats at
//     once and its lease backstop forgets pre-quiescence contact, so the
//     first read after a long idle waits one round trip instead of
//     trusting stale answers.
//
// Both stay off until the cluster finalizes v12: a v11 receiver drops a
// coalesced envelope and keeps expecting per-range heartbeats, and a v11
// follower never quiesces, so a quiesced v12 leader would let it time out
// and campaign.

// RaftHeartbeat is one range's MsgHeartbeat or MsgHeartbeatResp reduced
// to the fields raft reads, carried coalesced per peer node.
type RaftHeartbeat struct {
	RangeID base.RangeID
	To      uint64
	From    uint64
	Term    uint64
	Commit  uint64
	Quiesce bool
	// Closed-timestamp updates only (closedts.go): the leader's last log
	// index when the promise was made, and the promise.
	Index    uint64
	ClosedTS hlc.Timestamp
}

// quiesceAfterTicks is how many consecutive idle ticks a leader waits
// before quiescing (2 s at the default 100 ms tick).
const quiesceAfterTicks = 20

// heartbeatQueue collects heartbeats per destination between flushes.
type heartbeatQueue struct {
	sync.Mutex
	beats  map[base.NodeID][]RaftHeartbeat
	resps  map[base.NodeID][]RaftHeartbeat
	closed map[base.NodeID][]RaftHeartbeat
}

// coalescedHeartbeats reports whether the cluster is at v12: heartbeats
// travel coalesced and ranges may quiesce.
func (s *Store) coalescedHeartbeats() bool {
	if s.cfg.ClusterVersion == nil {
		return true
	}
	return s.cfg.ClusterVersion() >= version.V12
}

// queueHeartbeat parks a heartbeat (or, resp, a response) for the next
// flush.
func (s *Store) queueHeartbeat(to base.NodeID, hb RaftHeartbeat, resp bool) {
	s.hbq.Lock()
	if resp {
		s.hbq.resps[to] = append(s.hbq.resps[to], hb)
	} else {
		s.hbq.beats[to] = append(s.hbq.beats[to], hb)
	}
	s.hbq.Unlock()
}

// queueClosedTimestamp parks a quiescent leader's closed-timestamp
// update for a follower (closedts.go).
func (s *Store) queueClosedTimestamp(to base.NodeID, hb RaftHeartbeat) {
	s.hbq.Lock()
	s.hbq.closed[to] = append(s.hbq.closed[to], hb)
	s.hbq.Unlock()
}

// sendQueuedHeartbeats flushes the queue: one envelope per destination
// node. Called at the end of every scheduler pass and after a
// closed-timestamp publication round.
func (s *Store) sendQueuedHeartbeats(ctx context.Context) {
	s.hbq.Lock()
	if len(s.hbq.beats) == 0 && len(s.hbq.resps) == 0 && len(s.hbq.closed) == 0 {
		s.hbq.Unlock()
		return
	}
	beats, resps, closed := s.hbq.beats, s.hbq.resps, s.hbq.closed
	s.hbq.beats = make(map[base.NodeID][]RaftHeartbeat)
	s.hbq.resps = make(map[base.NodeID][]RaftHeartbeat)
	s.hbq.closed = make(map[base.NodeID][]RaftHeartbeat)
	s.hbq.Unlock()
	dests := make(map[base.NodeID]struct{}, len(beats)+len(resps)+len(closed))
	for _, m := range []map[base.NodeID][]RaftHeartbeat{beats, resps, closed} {
		for to := range m {
			dests[to] = struct{}{}
		}
	}
	for to := range dests {
		b, r, c := beats[to], resps[to], closed[to]
		metrics.RaftHeartbeatsCoalesced.Add(float64(len(b) + len(r) + len(c)))
		metrics.RaftHeartbeatEnvelopes.Inc()
		if err := s.cfg.Transport.SendRaftHeartbeats(ctx, to, b, r, c); err != nil {
			log.Debugf("coalesced heartbeats to n%d: %v", to, err)
		}
	}
}

// HandleRaftHeartbeats fans a peer's coalesced heartbeats and responses
// out to their replicas.
func (s *Store) HandleRaftHeartbeats(ctx context.Context, from base.NodeID, beats, resps, closed []RaftHeartbeat) {
	if len(closed) > 0 {
		s.handleClosedTimestamps(from, closed)
	}
	for _, hb := range beats {
		r, ok := s.GetReplica(hb.RangeID)
		if !ok {
			continue
		}
		m := raftpb.Message{Type: raftpb.MsgHeartbeat, To: hb.To, From: hb.From, Term: hb.Term, Commit: hb.Commit}
		if err := r.stepHeartbeat(ctx, m, hb.Quiesce); err != nil {
			log.Debugf("%s: coalesced heartbeat from n%d: %v", hb.RangeID, from, err)
		}
	}
	for _, hb := range resps {
		r, ok := s.GetReplica(hb.RangeID)
		if !ok {
			continue
		}
		m := raftpb.Message{Type: raftpb.MsgHeartbeatResp, To: hb.To, From: hb.From, Term: hb.Term}
		if err := r.stepRaftMessage(ctx, m); err != nil {
			log.Debugf("%s: coalesced heartbeat response from n%d: %v", hb.RangeID, from, err)
		}
	}
}

// stepHeartbeat steps a leader's heartbeat; one carrying the quiesce
// flag puts this follower to sleep if it holds everything the leader
// committed (the flag is only sent when it should).
func (r *Replica) stepHeartbeat(ctx context.Context, m raftpb.Message, quiesce bool) error {
	if !quiesce {
		return r.stepRaftMessage(ctx, m)
	}
	r.mu.Lock()
	r.mu.lastFollowerResp[m.From] = time.Now()
	r.mu.Unlock()
	err := r.withRaftGroup(func(rn *raft.RawNode) error { return rn.Step(m) })
	if err != nil {
		return err
	}
	r.store.sched.enqueue(r.rangeID, schedReady)
	r.mu.Lock()
	canSleep := !r.mu.quiescent && r.mu.leader != uint64(r.replicaID) && r.mu.pendingInstall == nil && !r.mu.destroyed &&
		m.Commit == r.rs.lastIndex()
	if canSleep {
		r.mu.quiescent = true
	}
	r.mu.Unlock()
	if canSleep {
		metrics.RaftQuiesces.Inc()
	}
	return nil
}

// isQuiescent reports whether the replica is asleep (the store ticker
// skips it).
func (r *Replica) isQuiescent() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.mu.quiescent
}

// Quiescent reports whether the replica is asleep (status and tests).
func (r *Replica) Quiescent() bool { return r.isQuiescent() }

// unquiesce wakes a sleeping replica: ticks resume, pre-sleep follower
// contact stops vouching for lease reads, and a leader heartbeats at
// once so contact is back within a round trip. A replica that was awake
// only resets its idle count.
func (r *Replica) unquiesce() (was bool) {
	r.mu.Lock()
	was = r.mu.quiescent
	r.mu.quiescent = false
	r.mu.idleTicks = 0
	if was {
		now := time.Now()
		r.mu.contactFloor = now
		r.mu.lastTickAt = now
	}
	registered := len(r.mu.sideRegistered) > 0
	r.mu.sideRegistered = nil
	leader := r.mu.leader == uint64(r.replicaID)
	r.mu.Unlock()
	if !was {
		return false
	}
	metrics.RaftUnquiesces.Inc()
	if leader {
		// The off-log closed-timestamp transport (closedts.go): the
		// followers drop this range from the group promise on the next
		// round, and until then every group promise they may apply is
		// honored here by forwarding the timestamp-cache floor to it —
		// noted as woken BEFORE reading the promise, so a round that
		// advances the promise either sees the wake or is covered by
		// the bump.
		if registered {
			r.store.side.Lock()
			r.store.side.woken[r.rangeID] = struct{}{}
			r.store.side.Unlock()
		}
		if gt := *r.store.side.groupTS.Load(); !gt.IsEmpty() {
			r.tsCache.Bump([]latchSpan{wholeRangeSpan}, gt, uuid.Nil)
		}
		// A local MsgBeat makes raft broadcast heartbeats now instead of
		// at its next heartbeat tick; the local-thread From is what
		// RawNode.Step accepts for a local message.
		_ = r.withRaftGroup(func(rn *raft.RawNode) error {
			return rn.Step(raftpb.Message{Type: raftpb.MsgBeat, From: raft.LocalAppendThread})
		})
	}
	r.store.sched.enqueue(r.rangeID, schedTick|schedReady)
	return true
}

// maybeQuiesce runs on a leader's tick, under raftMu with raft's status
// in hand, before the tick is applied: after quiesceAfterTicks idle
// ticks with nothing pending and every follower caught up and having
// answered within that period, it tells the followers to sleep and
// reports true so the tick is skipped. The follower contact requirement
// means an unreachable follower keeps the range awake: it is not idle,
// and its return would wake everyone anyway.
func (r *Replica) maybeQuiesce(rn *raft.RawNode) bool {
	if r.store.cfg.DisableQuiescence || !r.store.coalescedHeartbeats() || rn.HasReady() {
		r.resetIdle()
		return false
	}
	st := rn.BasicStatus()
	self := uint64(r.replicaID)
	if st.RaftState != raft.StateLeader || st.Lead != self {
		r.resetIdle()
		return false
	}
	last := r.rs.lastIndex()
	if st.Commit != last || st.Applied != last {
		r.resetIdle()
		return false
	}
	full := rn.Status()
	// A follower counts as answering if it did within the idle period
	// itself: the point is not to sleep on an unreachable follower, and
	// the lease-read window would be too strict on a loaded store, where
	// heartbeat responses lag and every range that stays awake for it
	// adds to the load (the strict window still governs lease reads).
	window := quiesceAfterTicks * r.store.cfg.RaftTickInterval
	now := time.Now()
	r.mu.Lock()
	defer r.mu.Unlock()
	idle := len(r.mu.proposals) == 0 && len(r.mu.readWaits) == 0 && len(r.mu.riPending) == 0 && len(r.mu.appliedWaits) == 0 &&
		len(r.mu.snapInFlight) == 0 && r.mu.pendingInstall == nil && !r.mu.destroyed
	var followers []uint64
	for id, pr := range full.Progress {
		if id == self {
			continue
		}
		if pr.Match != last || pr.State != tracker.StateReplicate || pr.IsPaused() {
			idle = false
			break
		}
		if ts, ok := r.mu.lastFollowerResp[id]; !ok || now.Sub(ts) > window || !ts.After(r.mu.contactFloor) {
			idle = false
			break
		}
		followers = append(followers, id)
	}
	if !idle {
		r.mu.idleTicks = 0
		return false
	}
	r.mu.idleTicks++
	if r.mu.idleTicks < quiesceAfterTicks {
		return false
	}
	r.mu.quiescent = true
	r.mu.idleTicks = 0
	desc := r.mu.desc
	for _, id := range followers {
		var target base.NodeID
		for _, rep := range desc.Replicas {
			if uint64(rep.ReplicaID) == id {
				target = rep.NodeID
			}
		}
		if target == 0 {
			continue
		}
		r.store.queueHeartbeat(target, RaftHeartbeat{RangeID: r.rangeID, To: id, From: self, Term: st.Term, Commit: st.Commit, Quiesce: true}, false)
	}
	metrics.RaftQuiesces.Inc()
	return true
}

func (r *Replica) resetIdle() {
	r.mu.Lock()
	r.mu.idleTicks = 0
	r.mu.Unlock()
}

// awaitContact waits (briefly) for a woken leader's followers to answer,
// so the first lease read after an idle period succeeds instead of
// bouncing NotLeader while the heartbeat round trip is in flight.
func (r *Replica) awaitContact(ctx context.Context, timeout time.Duration) {
	deadline := time.Now().Add(timeout)
	for !r.leaseContactFresh() && time.Now().Before(deadline) && ctx.Err() == nil {
		time.Sleep(2 * time.Millisecond)
	}
}
