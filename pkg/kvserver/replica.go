package kvserver

import (
	"context"
	"errors"
	"fmt"
	"github.com/sthorne/datax/pkg/util/faultpoint"
	"sync"
	"time"

	"github.com/google/uuid"
	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"

	"github.com/sthorne/datax/pkg/base"
	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/kvpb"
	"github.com/sthorne/datax/pkg/metrics"
	"github.com/sthorne/datax/pkg/storage"
	"github.com/sthorne/datax/pkg/util/hlc"
	"github.com/sthorne/datax/pkg/util/log"
)

// raftCommand is what travels through the Raft log: a uniquely identified
// write batch. The proposing replica matches the ID back to a waiting
// caller at application time.
type raftCommand struct {
	ID    string            `json:"id"`
	Batch kvpb.BatchRequest `json:"batch"`
	Split *splitTrigger     `json:"split,omitempty"`
	Merge *mergeTrigger     `json:"merge,omitempty"`
	// ClosedTS publishes "no write at or below this timestamp will ever
	// commit on this range". Riding the log makes the applied-index
	// condition automatic: applying this command implies every earlier
	// write applied.
	ClosedTS hlc.Timestamp `json:"closed_ts,omitempty"`
	// Load hands the outgoing leaseholder's measured request rate to the
	// incoming one ahead of a lease transfer (see adminTransferLease).
	Load *loadHandoff `json:"load,omitempty"`
	// Checksum asks every replica to checksum the range's replicated
	// state at this command's applied index — identical across replicas
	// by construction, so any divergence is corruption.
	Checksum *checksumTrigger `json:"checksum,omitempty"`
}

// loadHandoff carries a leaseholder's measured QPS through the log.
// Applying it stores the value replica-locally; the replica that next
// becomes leader seeds its load tracker from it if it is fresh, so
// rebalancing decisions keep seeing the range's real load across the
// transfer instead of a zeroed, immature tracker.
type loadHandoff struct {
	QPS     float64 `json:"qps"`
	AtNanos int64   `json:"at_nanos"`
}

// checksumTrigger identifies one consistency-check computation.
type checksumTrigger struct {
	ID string `json:"id"`
}

// cmdTriggers bundles the optional replicated side effects a proposal may
// carry alongside (or instead of) its write batch.
type cmdTriggers struct {
	split    *splitTrigger
	merge    *mergeTrigger
	closedTS hlc.Timestamp
	load     *loadHandoff
	checksum *checksumTrigger
}

// splitTrigger is carried by the replicated split command (Phase 3).
type splitTrigger struct {
	Left  kvpb.RangeDescriptor `json:"left"`
	Right kvpb.RangeDescriptor `json:"right"`
	// ClosedTS hands the parent's closed timestamp to the new RHS: reads
	// the parent served at or below it covered the RHS span too, so no
	// post-split RHS write may land beneath it.
	ClosedTS hlc.Timestamp `json:"closed_ts,omitempty"`
}

type proposalResult struct {
	resp *kvpb.BatchResponse
	err  *kvpb.Error
}

// errReplicaRemoved signals that this replica was removed from its range by
// an applied ConfChange and must shut down.
var errReplicaRemoved = errors.New("replica removed from range")

// Replica is one replica of a range: a member of the range's Raft group,
// applying its log to the shared store engine.
type Replica struct {
	store     *Store
	rangeID   base.RangeID
	replicaID base.ReplicaID

	rs *raftStorage

	// raftMu guards the raft group. Held only for RawNode calls (a step,
	// a proposal, taking or advancing a Ready) — never across Ready
	// handling, which the store's scheduler serializes per replica
	// (scheduler.go). stopped means the group is gone (a merge absorbed
	// the range, the replica was removed, or its apply failed): every
	// call refuses with errRaftStopped.
	raftMu struct {
		sync.Mutex
		rn      *raft.RawNode
		stopped bool
	}

	// tsCache is the read timestamp cache (leader-authoritative; see
	// docs/transactions.md).
	tsCache tsCache

	// latches serialize overlapping requests: a write holds exclusive
	// latches on its spans from its timestamp-cache check until it is
	// applied, so an overlapping read (shared holder) can never evaluate
	// concurrently with a write that already passed the check but is not
	// yet visible. Disjoint requests run in parallel. See latch.go for the
	// L1/L2 invariants.
	latches *latchManager

	// leaseReads is whether ReadIndex runs lease-based (no quorum round
	// trip per read); fixed at replica creation.
	leaseReads bool

	// load is the leader-local, unreplicated request-rate tracker feeding
	// load-based splitting (see loadsplit.go). It has its own mutex.
	load replicaLoad

	// applyMu serializes state-machine replacement (an incoming catch-up
	// snapshot install) against ordinary entry application. Held by
	// applyEntry and by Store.installSnapshot's commit-and-swap.
	applyMu sync.Mutex

	mu struct {
		sync.Mutex
		desc         kvpb.RangeDescriptor
		appliedIndex uint64
		gcThreshold  hlc.Timestamp // replicated; raised by applied GC commands
		sizeBytes    int64         // replicated approximate MVCC data size
		term         uint64        // highest raft term observed
		leader       uint64        // last known raft leader (replica ID); 0 unknown
		proposals    map[string]chan proposalResult
		readWaits    map[string]chan uint64
		appliedWaits []appliedWait
		destroyed    bool
		// lastFollowerResp records when each peer last answered this
		// replica (heartbeat/append responses) — the lease backstop's input.
		lastFollowerResp map[uint64]time.Time
		// lastTickAt / contactFloor implement stall detection: when the raft
		// ticker observes a gap far beyond its interval, the process (or at
		// least this replica's loop) was stalled — GC pause, VM freeze — and
		// follower contact from before the stall can no longer prove a live
		// lease. contactFloor is raised to the wake time; only responses
		// AFTER it count toward leaseContactFresh.
		lastTickAt   time.Time
		contactFloor time.Time
		// ReadIndex coalescing: waiters register here; one goroutine drains
		// rounds while riInFlight.
		riPending  []chan readIndexResult
		riInFlight bool
		// snapInFlight tracks outgoing snapshot streams by target replica
		// ID → the applied index registered at start. Log truncation never
		// advances past the minimum, so a follower being caught up can
		// still receive the entries after its snapshot.
		snapInFlight map[uint64]uint64
		// pendingInstall is a staged incoming snapshot awaiting raft's
		// restore (committed by applySnapshot). See catchup.go.
		pendingInstall *pendingSnapshot
		// Split stores (raftengine.go): applySeqs pairs recent applied
		// indexes with their batches' sequence numbers, durableApplied
		// is the highest one the state engine has flushed, and
		// pendingTrunc is an applied TruncateLog whose raft-side deletion
		// waits for that flush.
		applySeqs      []appliedSeq
		durableApplied uint64
		pendingTrunc   truncatedState
		pendingSince   time.Time
		// frozen: a Subsume applied — the range refuses traffic pending a
		// merge into mergedInto (see merge.go). Mirrors replicaState.
		frozen     bool
		mergedInto base.RangeID
		// closedTS mirrors replicaState.ClosedTS: the range's replicated
		// closed timestamp, below which this replica may serve reads
		// locally without being the leader.
		closedTS hlc.Timestamp
		// loadHandoff is the newest applied load handoff (in-memory only —
		// warmth is best-effort); consumed when this replica becomes leader.
		loadHandoff *loadHandoff
		// checksums parks completed consistency-check results by check ID.
		checksums map[string]checksumResult
		// quiescent: the replica is asleep (quiesce.go) — the store ticker
		// skips it. idleTicks counts a leader's consecutive idle ticks
		// toward quiescing.
		quiescent bool
		idleTicks int
		// sideClosedTS is a closed timestamp learned off the log
		// (closedts.go): in memory only, never persisted or checksummed,
		// and forgotten by a restart. closedTSLogged is the leader's last
		// index after its last logged promise: a log that has not grown
		// since publishes off the log.
		sideClosedTS   hlc.Timestamp
		closedTSLogged uint64
		// sideRegistered names the follower nodes that hold this
		// quiescent leader's registration in the off-log transport, so
		// the store's group promise covers the range there; cleared on
		// wake (closedts.go).
		sideRegistered map[base.NodeID]bool
		// appliedTerm is the term of the newest applied entry: a leader
		// whose term it equals has committed an entry in its term, the
		// condition under which raft answers a lease-based read index
		// with its commit index (leaseReadIndex).
		appliedTerm uint64
	}

	// stoppedCh closes when the raft group is stopped for good: the
	// scheduler will never run another pass for this replica.
	stopOnce  sync.Once
	stoppedCh chan struct{}
}

// errRaftStopped is returned by every raft-group call on a stopped
// replica.
var errRaftStopped = errors.New("raft group stopped")

type readIndexResult struct {
	idx uint64
	err *kvpb.Error
}

type appliedWait struct {
	idx uint64
	ch  chan struct{}
}

// raftElectionTicks is the raft election timeout in ticks; with the tick
// interval it bounds how long a partitioned leader can outlive its lease
// (see leaseContactFresh).
const raftElectionTicks = 10

func raftConfig(id uint64, applied uint64, st raft.Storage, leaseReads bool) *raft.Config {
	cfg := &raft.Config{
		ID:                        id,
		ElectionTick:              raftElectionTicks,
		HeartbeatTick:             3,
		Storage:                   st,
		Applied:                   applied,
		MaxSizePerMsg:             1 << 20,
		MaxInflightMsgs:           256,
		PreVote:                   true,
		CheckQuorum:               true,
		DisableProposalForwarding: true, // the leader owns the timestamp cache
		Logger:                    &raftLogger{},
	}
	if leaseReads {
		// Lease-based reads answer ReadIndex from the leader's CheckQuorum
		// lease instead of a quorum round trip. Safe with CheckQuorum +
		// PreVote (both set above) plus the wall-clock backstop in
		// leaseContactFresh; see docs/replication-and-placement.md.
		cfg.ReadOnlyOption = raft.ReadOnlyLeaseBased
	}
	return cfg
}

// newReplica loads or creates the replica and its Raft group; the group
// is scheduled by startRaft, once the store holds the replica (see
// Store.startReplica). bootstrap must be true exactly once per replica
// lifetime — when the range is first created with its initial membership.
func newReplica(s *Store, desc kvpb.RangeDescriptor, replicaID base.ReplicaID, bootstrap bool) (*Replica, error) {
	rs, err := newRaftStorage(s.raftEngine(), s.cfg.Engine, desc.RangeID, desc)
	if err != nil {
		return nil, err
	}
	st, err := loadReplicaState(s.cfg.Engine, desc.RangeID)
	if err != nil {
		return nil, err
	}
	r := &Replica{store: s, rangeID: desc.RangeID, replicaID: replicaID, rs: rs, latches: newLatchManager(), leaseReads: !s.cfg.DisableLeaseReads}
	r.load.init(s.loadNow)
	rs.setApplied(st.AppliedIndex)
	r.mu.desc = desc
	r.mu.appliedIndex = st.AppliedIndex
	r.mu.gcThreshold = st.GCThreshold
	r.mu.sizeBytes = st.SizeBytes
	if hs, _, err := rs.InitialState(); err == nil {
		r.mu.term = hs.Term
	}
	r.mu.proposals = make(map[string]chan proposalResult)
	r.mu.readWaits = make(map[string]chan uint64)
	r.mu.lastFollowerResp = make(map[uint64]time.Time)
	r.mu.snapInFlight = make(map[uint64]uint64)
	r.mu.frozen = st.Frozen
	r.mu.mergedInto = st.MergedInto
	r.mu.closedTS = st.ClosedTS
	if !st.ClosedTS.IsEmpty() {
		// The closed timestamp is a promise about the whole range; the
		// timestamp cache floor must enforce it on whichever replica leads
		// — including a fresh post-split RHS leader (term 1, which skips
		// the new-leader bump) and a restarted one.
		r.tsCache.Bump([]latchSpan{wholeRangeSpan}, st.ClosedTS, uuid.Nil)
	}
	r.stoppedCh = make(chan struct{})

	cfg := raftConfig(uint64(replicaID), st.AppliedIndex, rs, r.leaseReads)
	rn, err := raft.NewRawNode(cfg)
	if err != nil {
		return nil, err
	}
	if bootstrap {
		peers := make([]raft.Peer, 0, len(desc.Replicas))
		for _, rep := range desc.Replicas {
			peers = append(peers, raft.Peer{ID: uint64(rep.ReplicaID)})
		}
		if err := rn.Bootstrap(peers); err != nil {
			return nil, err
		}
	}
	r.raftMu.rn = rn

	return r, nil
}

// startRaft hands the replica to the store's scheduler. Separate from
// construction so a restart can load every replica before any of them
// applies: a replayed merge looks its RHS up in the store map, and an LHS
// that started before its sibling was loaded would find it missing and
// fatal the node (issue #70).
func (r *Replica) startRaft() error {
	if err := r.store.sched.start(); err != nil {
		return err
	}
	r.store.sched.enqueue(r.rangeID, schedReady)
	return nil
}

// withRaftGroup runs f against the raft group under raftMu, refusing once
// the group is stopped. Callers that may have produced work (a step, a
// proposal, a tick) enqueue the replica afterwards, outside the lock.
func (r *Replica) withRaftGroup(f func(rn *raft.RawNode) error) error {
	r.raftMu.Lock()
	defer r.raftMu.Unlock()
	if r.raftMu.stopped {
		return errRaftStopped
	}
	return f(r.raftMu.rn)
}

// stopRaftGroup marks the group stopped from within its own pass.
func (r *Replica) stopRaftGroup() {
	r.raftMu.Lock()
	r.raftMu.stopped = true
	r.raftMu.Unlock()
}

// markStopped closes stoppedCh (once).
func (r *Replica) markStopped() {
	r.stopOnce.Do(func() { close(r.stoppedCh) })
}

// raftStatus is the group's raft status (ok=false once stopped).
func (r *Replica) raftStatus() (st raft.Status, ok bool) {
	err := r.withRaftGroup(func(rn *raft.RawNode) error {
		st = rn.Status()
		return nil
	})
	return st, err == nil
}

// reportUnreachable tells raft a peer could not be reached (it backs off
// its replication to that peer).
func (r *Replica) reportUnreachable(id uint64) {
	_ = r.withRaftGroup(func(rn *raft.RawNode) error {
		rn.ReportUnreachable(id)
		return nil
	})
	r.store.sched.enqueue(r.rangeID, schedReady)
}

// reportSnapshot tells raft how an out-of-band snapshot to a peer ended.
func (r *Replica) reportSnapshot(id uint64, status raft.SnapshotStatus) {
	_ = r.withRaftGroup(func(rn *raft.RawNode) error {
		rn.ReportSnapshot(id, status)
		return nil
	})
	r.store.sched.enqueue(r.rangeID, schedReady)
}

// campaign starts an election for this replica.
func (r *Replica) campaign() error {
	r.unquiesce()
	err := r.withRaftGroup(func(rn *raft.RawNode) error { return rn.Campaign() })
	r.store.sched.enqueue(r.rangeID, schedReady)
	return err
}

// transferLeader asks the group's leader to hand leadership to
// transferee; on a follower raft forwards the request to the leader.
func (r *Replica) transferLeader(transferee uint64) {
	r.unquiesce()
	_ = r.withRaftGroup(func(rn *raft.RawNode) error {
		rn.TransferLeader(transferee)
		return nil
	})
	r.store.sched.enqueue(r.rangeID, schedReady)
}

// Desc returns the replica's current view of the range descriptor.
func (r *Replica) Desc() kvpb.RangeDescriptor {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.mu.desc
}

// GCThreshold returns the range's replicated GC threshold.
func (r *Replica) GCThreshold() hlc.Timestamp {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.mu.gcThreshold
}

// ClosedTimestamp returns the range's replicated closed timestamp: no
// write at or below it will ever commit, so reads there are servable by
// any replica.
func (r *Replica) ClosedTimestamp() hlc.Timestamp {
	r.mu.Lock()
	defer r.mu.Unlock()
	ct := r.mu.closedTS.Forward(r.mu.sideClosedTS)
	if r.mu.quiescent && len(r.mu.sideRegistered) > 0 {
		// A registered quiescent leader is covered by the store's group
		// promise (closedts.go).
		ct = ct.Forward(*r.store.side.groupTS.Load())
	}
	return ct
}

// SizeBytes returns the range's replicated approximate data size.
func (r *Replica) SizeBytes() int64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.mu.sizeBytes
}

// AppliedIndex returns the highest applied raft log index.
// LastIndex is the raft log's last index (0 when unreadable): with
// AppliedIndex, the gap a restart has left to replay.
func (r *Replica) LastIndex() uint64 {
	idx, err := r.rs.LastIndex()
	if err != nil {
		return 0
	}
	return idx
}

func (r *Replica) AppliedIndex() uint64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.mu.appliedIndex
}

// TestingRaftStopped is closed once the replica's raft loop has exited.
// Test hook.
func (r *Replica) TestingRaftStopped() <-chan struct{} { return r.stoppedCh }

// IsLeader reports whether this replica believes it is the Raft leader.
func (r *Replica) IsLeader() bool { return r.isLeader() }

// hasLeader reports whether the replica knows any current leader.
func (r *Replica) hasLeader() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.mu.leader != 0
}

func (r *Replica) isLeader() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.mu.leader == uint64(r.replicaID)
}

// leaderHint maps the last known raft leader to a node ID (0 if unknown).
func (r *Replica) leaderHint() base.NodeID {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, rep := range r.mu.desc.Replicas {
		if uint64(rep.ReplicaID) == r.mu.leader {
			return rep.NodeID
		}
	}
	return 0
}

// isFrozen reports whether a Subsume has frozen this range for a merge.
func (r *Replica) isFrozen() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.mu.frozen
}

// checkFrozen refuses traffic on a frozen range. Subsume (idempotent
// re-drive) and Unfreeze (abandonment) are the only requests allowed
// through. The error carries a RangeKeyMismatch so clients re-resolve
// routing — once the merge lands, the merged range's descriptor answers.
func (r *Replica) checkFrozen(ba *kvpb.BatchRequest) *kvpb.Error {
	if !r.isFrozen() {
		return nil
	}
	for _, u := range ba.Requests {
		switch u.GetInner().(type) {
		case *kvpb.SubsumeRequest, *kvpb.UnfreezeRequest:
			return nil
		}
	}
	addr, _ := addrOf(ba.Requests[0].GetInner().Header().Key)
	e := kvpb.NewErrorf("%s: range is frozen for a merge", r.rangeID)
	e.RangeKeyMismatch = &kvpb.RangeKeyMismatchError{RequestKey: addr}
	return e
}

func (r *Replica) notLeaderError() *kvpb.Error {
	e := kvpb.NewErrorf("%s: replica %d is not the leader", r.rangeID, r.replicaID)
	e.NotLeader = &kvpb.NotLeaderError{RangeID: r.rangeID, LeaderHint: r.leaderHint()}
	return e
}

// takeReady runs a tick if the pass was flagged for one and takes raft's
// Ready if it has one. ok=false: nothing to do, or the group is stopped.
func (r *Replica) takeReady(flags raftSchedFlags) (rd raft.Ready, ok bool) {
	if flags&schedTick != 0 {
		r.noteTick()
	}
	err := r.withRaftGroup(func(rn *raft.RawNode) error {
		if flags&schedTick != 0 && !r.maybeQuiesce(rn) {
			rn.Tick()
		}
		if rn.HasReady() {
			rd, ok = rn.Ready(), true
		}
		return nil
	})
	if err != nil {
		return raft.Ready{}, false
	}
	return rd, ok
}

// stageReady is the first half of handling a Ready: leadership
// bookkeeping, the snapshot (installed out of band, acknowledged here),
// and the HardState and new entries staged into b — the scheduler's
// group batch, committed with one sync for every replica in the pass
// before any of them sends a message (the raft durability contract).
func (r *Replica) stageReady(b *storage.Batch, rd raft.Ready) error {
	// 1. Track term and leadership changes. Raft keeps stepping messages
	// while this loop works through a Ready, so a leadership interruption
	// can complete entirely between two Readies — lost and regained, or
	// transferred away and back — and the next Ready then carries NO
	// SoftState (the soft state equals the last one this loop saw), only a
	// higher HardState.Term. A raft leader never changes term without
	// stepping down first, so a term advance on a replica that was and
	// still is the leader means it was not the leader in between: entries
	// proposed in the old term may have been truncated by the interim
	// leader, so their proposers are answered (ambiguously) now instead of
	// waiting out the client's deadline, and the interim leader may have
	// served reads this replica knows nothing about, so the timestamp
	// cache is bumped exactly as on any new leadership (issue #74).
	var (
		becameLeader, lostLeadership bool
		term                         uint64
		handoff                      *loadHandoff
		pending                      []chan proposalResult
	)
	r.mu.Lock()
	prevTerm := r.mu.term
	if !raft.IsEmptyHardState(rd.HardState) && rd.HardState.Term > r.mu.term {
		r.mu.term = rd.HardState.Term
	}
	term = r.mu.term
	termAdvanced := term > prevTerm
	self := uint64(r.replicaID)
	wasLeader := r.mu.leader == self
	isLeader := wasLeader
	if rd.SoftState != nil {
		r.mu.leader = rd.SoftState.Lead
		isLeader = rd.SoftState.RaftState == raft.StateLeader
	}
	becameLeader = isLeader && (!wasLeader || termAdvanced)
	lostLeadership = wasLeader && (!isLeader || termAdvanced)
	if becameLeader {
		handoff, r.mu.loadHandoff = r.mu.loadHandoff, nil
	}
	if lostLeadership {
		for id, ch := range r.mu.proposals {
			pending = append(pending, ch)
			delete(r.mu.proposals, id)
		}
	}
	r.mu.Unlock()
	if becameLeader && term > 1 {
		// A new leader cannot know what reads the old leader served:
		// conservatively push all future writes above now. A term-1
		// leader is the range's first ever — no prior reads exist, so
		// fresh ranges (splits, bootstrap) skip the bump.
		r.tsCache.Bump([]latchSpan{wholeRangeSpan}, r.store.cfg.Clock.Now(), uuid.Nil)
	}
	if becameLeader && handoff != nil {
		// A lease transfer handed us the outgoing leader's measured
		// rate: start warm instead of amnesiac (see loadHandoff).
		r.load.seed(handoff.QPS, handoff.AtNanos)
	}
	for _, ch := range pending {
		ch <- proposalResult{err: &kvpb.Error{
			Message:   "leadership lost with proposal in flight",
			Ambiguous: &kvpb.AmbiguousResultError{},
		}}
	}

	// 2. Acknowledge the snapshot (state already installed out of band),
	// then stage HardState and new entries. Snapshot first: entries in
	// the same Ready follow the snapshot's index, and Advance() expects the
	// storage to already report the post-snapshot positions.
	if !raft.IsEmptySnap(rd.Snapshot) {
		if err := r.applySnapshot(rd.Snapshot); err != nil {
			return err
		}
	}
	if !raft.IsEmptyHardState(rd.HardState) {
		if err := r.rs.setHardState(b, rd.HardState); err != nil {
			return err
		}
	}
	return r.rs.append(b, rd.Entries)
}

// finishReady is the second half, after the group batch is durable: send
// the messages, satisfy read-index waiters, apply the committed entries.
func (r *Replica) finishReady(ctx context.Context, rd raft.Ready) error {
	// 3. Send messages.
	r.sendRaftMessages(ctx, rd.Messages)

	// 4. Satisfy ReadIndex waiters.
	for _, rs := range rd.ReadStates {
		r.mu.Lock()
		ch, ok := r.mu.readWaits[string(rs.RequestCtx)]
		if ok {
			delete(r.mu.readWaits, string(rs.RequestCtx))
		}
		r.mu.Unlock()
		if ok {
			ch <- rs.Index
		}
	}

	// 5. Apply committed entries.
	for _, ent := range rd.CommittedEntries {
		if err := r.applyEntry(ctx, ent); err != nil {
			return err
		}
		faultpoint.Hit("raft-apply")
	}
	return nil
}

// advanceReady tells raft the Ready is handled and re-enqueues the
// replica when raft has more to say, so every range gets a turn.
func (r *Replica) advanceReady(rd raft.Ready) {
	more := false
	_ = r.withRaftGroup(func(rn *raft.RawNode) error {
		rn.Advance(rd)
		more = rn.HasReady()
		return nil
	})
	if more {
		r.store.sched.enqueue(r.rangeID, schedReady)
	}
}

// failReady stops the group after a Ready could not be handled: a
// removed replica wipes its state; any other failure leaves the replica
// in the store map with a frozen applied index until a restart.
func (r *Replica) failReady(err error) {
	r.stopRaftGroup()
	if err == errReplicaRemoved {
		log.Infof("%s/%d: removed from range; shutting replica down", r.rangeID, r.replicaID)
		r.store.removeReplica(r.rangeID, r.Desc())
	} else if err == errApplyAborted {
		// Clean shutdown interleaved with a merge apply: nothing was
		// applied; the restart replays the entry (issue #61).
		log.Infof("%s/%d: apply aborted by shutdown; entry replays after restart", r.rangeID, r.replicaID)
	} else if errors.Is(err, errApplyAborted) {
		// Aborted without a shutdown (a dead local RHS at merge apply,
		// issue #70): nothing was applied, and this replica is out of
		// service until the node restarts.
		log.Warnf("%s/%d: %v", r.rangeID, r.replicaID, err)
	} else {
		log.Errorf("%s/%d: ready handling failed: %v", r.rangeID, r.replicaID, err)
	}
	r.markStopped()
}

// raftStopped reports whether the group has been stopped.
func (r *Replica) raftStopped() bool {
	r.raftMu.Lock()
	defer r.raftMu.Unlock()
	return r.raftMu.stopped
}

func (r *Replica) sendRaftMessages(ctx context.Context, msgs []raftpb.Message) {
	desc := r.Desc()
	coalesce := r.store.coalescedHeartbeats()
	for i := range msgs {
		m := msgs[i]
		var target base.NodeID
		for _, rep := range desc.Replicas {
			if uint64(rep.ReplicaID) == m.To {
				target = rep.NodeID
				break
			}
		}
		if target == 0 {
			log.Debugf("%s: dropping %s to unknown replica %d (desc replicas %v)", r.rangeID, m.Type, m.To, desc.Replicas)
			continue
		}
		if m.Type == raftpb.MsgSnap {
			// Snapshot data never rides inside raft messages (the transport
			// caps message size and drops under pressure): stream it out of
			// band, then forward a metadata-only MsgSnap. See catchup.go.
			r.startCatchupSnapshot(target, m)
			continue
		}
		if (m.Type == raftpb.MsgHeartbeat || m.Type == raftpb.MsgHeartbeatResp) && len(m.Context) == 0 && coalesce {
			// One envelope per peer node per scheduler pass carries every
			// range's heartbeats (quiesce.go).
			r.store.queueHeartbeat(target, RaftHeartbeat{RangeID: r.rangeID, To: m.To, From: m.From, Term: m.Term, Commit: m.Commit},
				m.Type == raftpb.MsgHeartbeatResp)
			continue
		}
		if err := r.store.cfg.Transport.SendRaftMessage(ctx, target, r.rangeID, m); err != nil {
			// Raft tolerates message loss; report unreachability so it backs off.
			r.reportUnreachable(m.To)
		}
	}
}

// stepRaftMessage feeds an incoming message into the Raft state machine.
// Any message but a heartbeat response wakes a quiescent replica (the
// response to a leader's parting heartbeat must not wake it back up).
func (r *Replica) stepRaftMessage(ctx context.Context, m raftpb.Message) error {
	switch m.Type {
	case raftpb.MsgHeartbeatResp, raftpb.MsgAppResp:
		// Follower contact, timestamped for the lease-read backstop.
		r.mu.Lock()
		r.mu.lastFollowerResp[m.From] = time.Now()
		r.mu.Unlock()
	}
	if m.Type != raftpb.MsgHeartbeatResp {
		r.unquiesce()
	}
	err := r.withRaftGroup(func(rn *raft.RawNode) error { return rn.Step(m) })
	if err == nil {
		r.store.sched.enqueue(r.rangeID, schedReady)
	}
	return err
}

// noteTick runs on every raft ticker fire and detects stalls: a gap far
// beyond the tick interval means this process slept (GC pause, cgroup
// throttling, VM freeze). Pre-sleep follower contact then proves nothing —
// an election may have completed while we were out — so the contact floor
// is raised to the wake time and lease reads are refused until a majority
// answers again.
func (r *Replica) noteTick() {
	now := time.Now()
	stallBound := time.Duration(raftElectionTicks) * r.store.cfg.RaftTickInterval / 2
	r.mu.Lock()
	if !r.mu.lastTickAt.IsZero() && now.Sub(r.mu.lastTickAt) > stallBound {
		r.mu.contactFloor = now
	}
	r.mu.lastTickAt = now
	r.mu.Unlock()
}

// TestingExpireLeaseContact invalidates all follower contact, as a detected
// stall would — the next lease read is refused until a majority answers
// again. Test hook.
func (r *Replica) TestingExpireLeaseContact() {
	r.mu.Lock()
	r.mu.contactFloor = time.Now()
	r.mu.Unlock()
}

// leaseContactFresh is the wall-clock backstop for lease-based reads: serve
// only while a majority (self included) has answered within
// electionTimeout − MaxOffset, and after any detected stall. A follower
// that answered at time T cannot vote for a new leader before
// T + electionTimeout (measured on its own tick clock), so within the
// window no new leader can exist and CheckQuorum guarantees this one still
// holds its lease. MaxOffset absorbs modest tick-rate skew between nodes;
// the stall detector (noteTick) covers this process sleeping outright; the
// cost of any false negative is only a NotLeader retry.
func (r *Replica) leaseContactFresh() bool {
	desc := r.Desc()
	n := len(desc.Replicas)
	if n <= 1 {
		return true
	}
	window := time.Duration(raftElectionTicks)*r.store.cfg.RaftTickInterval - r.store.cfg.Clock.MaxOffset()
	if window <= 0 {
		return false // MaxOffset swamps the election timeout: never lease-read
	}
	needed := (n/2 + 1) - 1 // majority minus this replica itself
	now := time.Now()
	fresh := 0
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, ts := range r.mu.lastFollowerResp {
		if now.Sub(ts) <= window && ts.After(r.mu.contactFloor) {
			fresh++
		}
	}
	return fresh >= needed
}

// propose submits a write batch through Raft and waits for its application.
func (r *Replica) propose(ctx context.Context, ba *kvpb.BatchRequest) (*kvpb.BatchResponse, *kvpb.Error) {
	return r.proposeCmd(ctx, ba, cmdTriggers{})
}

// proposeCmd submits a write batch, optionally carrying replicated
// triggers (split, merge, closed-timestamp publication, load handoff,
// checksum probe).
func (r *Replica) proposeCmd(ctx context.Context, ba *kvpb.BatchRequest, trig cmdTriggers) (*kvpb.BatchResponse, *kvpb.Error) {
	cmd := raftCommand{
		ID: uuid.NewString(), Batch: *ba,
		Split: trig.split, Merge: trig.merge, ClosedTS: trig.closedTS,
		Load: trig.load, Checksum: trig.checksum,
	}
	data, err := encodeRaftCommand(&cmd)
	if err != nil {
		return nil, kvpb.NewError(err)
	}
	ch := make(chan proposalResult, 1)
	r.mu.Lock()
	if r.mu.destroyed {
		r.mu.Unlock()
		return nil, kvpb.NewErrorf("%s: replica destroyed", r.rangeID)
	}
	r.mu.proposals[cmd.ID] = ch
	r.mu.Unlock()
	defer func() {
		r.mu.Lock()
		delete(r.mu.proposals, cmd.ID)
		r.mu.Unlock()
	}()

	r.unquiesce()
	if err := r.withRaftGroup(func(rn *raft.RawNode) error { return rn.Propose(data) }); err != nil {
		if err == raft.ErrProposalDropped {
			return nil, r.notLeaderError()
		}
		return nil, kvpb.NewError(err)
	}
	r.store.sched.enqueue(r.rangeID, schedReady)
	select {
	case <-ctx.Done():
		e := kvpb.NewErrorf("%s: proposal abandoned: %v", r.rangeID, ctx.Err())
		e.Ambiguous = &kvpb.AmbiguousResultError{}
		return nil, e
	case res := <-ch:
		return res.resp, res.err
	}
}

// linearizableReadIndex runs the ReadIndex protocol: confirm leadership
// (quorum round trip, or the leader's lease when lease reads are on), then
// wait until the applied state includes everything committed as of that
// confirmation. Concurrent readers coalesce: waiters that arrive while a
// confirmation is in flight share the NEXT one — registered before it is
// issued, so its index still covers everything committed before they
// arrived.
func (r *Replica) linearizableReadIndex(ctx context.Context) *kvpb.Error {
	mine := make(chan readIndexResult, 1)
	r.mu.Lock()
	r.mu.riPending = append(r.mu.riPending, mine)
	spawn := !r.mu.riInFlight
	if spawn {
		r.mu.riInFlight = true
	}
	r.mu.Unlock()
	if spawn {
		go r.readIndexLoop()
	}
	select {
	case <-ctx.Done():
		return kvpb.NewErrorf("%s: read index abandoned: %v", r.rangeID, ctx.Err())
	case res := <-mine:
		if res.err != nil {
			return res.err
		}
		return r.waitApplied(ctx, res.idx)
	}
}

// readIndexLoop drains rounds of coalesced ReadIndex confirmations until no
// waiters remain.
func (r *Replica) readIndexLoop() {
	for {
		r.mu.Lock()
		cohort := r.mu.riPending
		r.mu.riPending = nil
		if len(cohort) == 0 {
			r.mu.riInFlight = false
			r.mu.Unlock()
			return
		}
		r.mu.Unlock()
		idx, kerr := r.issueReadIndex()
		for _, ch := range cohort {
			ch <- readIndexResult{idx: idx, err: kerr}
		}
	}
}

// issueReadIndex performs one ReadIndex round trip on behalf of a cohort.
// It runs under the store's lifetime (not any single caller's context) with
// a bounded timeout; a timeout surfaces to the cohort as a retryable error.
func (r *Replica) issueReadIndex() (uint64, *kvpb.Error) {
	if r.leaseReads {
		if idx, ok := r.leaseReadIndex(); ok {
			return idx, nil
		}
	}
	ctx, cancel := context.WithTimeout(r.store.cfg.Stopper.Ctx(), 3*time.Second)
	defer cancel()
	rctx := uuid.NewString()
	ch := make(chan uint64, 1)
	r.mu.Lock()
	r.mu.readWaits[rctx] = ch
	r.mu.Unlock()
	defer func() {
		r.mu.Lock()
		delete(r.mu.readWaits, rctx)
		r.mu.Unlock()
	}()

	r.unquiesce()
	if err := r.withRaftGroup(func(rn *raft.RawNode) error {
		rn.ReadIndex([]byte(rctx))
		return nil
	}); err != nil {
		return 0, kvpb.NewError(err)
	}
	r.store.sched.enqueue(r.rangeID, schedReady)
	select {
	case <-ctx.Done():
		return 0, kvpb.NewErrorf("%s: read index abandoned: %v", r.rangeID, ctx.Err())
	case idx := <-ch:
		return idx, nil
	}
}

func (r *Replica) waitApplied(ctx context.Context, idx uint64) *kvpb.Error {
	r.mu.Lock()
	if r.mu.appliedIndex >= idx {
		r.mu.Unlock()
		return nil
	}
	ch := make(chan struct{})
	r.mu.appliedWaits = append(r.mu.appliedWaits, appliedWait{idx: idx, ch: ch})
	r.mu.Unlock()
	select {
	case <-ctx.Done():
		return kvpb.NewErrorf("%s: wait for applied index %d abandoned: %v", r.rangeID, idx, ctx.Err())
	case <-ch:
		return nil
	}
}

// noteAppliedTerm records the term of the entry just applied.
func (r *Replica) noteAppliedTerm(term uint64) {
	r.mu.Lock()
	if term > r.mu.appliedTerm {
		r.mu.appliedTerm = term
	}
	r.mu.Unlock()
}

// leaseReadIndex is the fast path of a lease-based read index: raft's
// ReadOnlyLeaseBased answer is the leader's commit index, given at once
// when the leader has committed an entry in its own term — exactly what
// raft would put in the next Ready's ReadStates, minus a scheduler pass
// and a Ready per read. ok=false (not the leader, or no entry of this
// term applied yet) falls back to the full round through raft.
func (r *Replica) leaseReadIndex() (idx uint64, ok bool) {
	r.mu.Lock()
	appliedTerm := r.mu.appliedTerm
	r.mu.Unlock()
	_ = r.withRaftGroup(func(rn *raft.RawNode) error {
		st := rn.BasicStatus()
		if st.RaftState == raft.StateLeader && st.Lead == uint64(r.replicaID) && st.Term == appliedTerm {
			idx, ok = st.Commit, true
		}
		return nil
	})
	return idx, ok
}

// setApplied advances the applied index and wakes satisfied waiters.
// Called from the apply path only.
func (r *Replica) setApplied(idx uint64) {
	r.rs.setApplied(idx)
	r.mu.Lock()
	r.mu.appliedIndex = idx
	var remaining []appliedWait
	for _, w := range r.mu.appliedWaits {
		if idx >= w.idx {
			close(w.ch)
		} else {
			remaining = append(remaining, w)
		}
	}
	r.mu.appliedWaits = remaining
	r.mu.Unlock()
}

// Execute serves a batch against this replica. Reads and writes go to the
// leader (which owns the timestamp cache); the one exception is a stale
// read at or below the range's closed timestamp, which any replica serves
// locally (see executeStaleRead).
func (r *Replica) Execute(ctx context.Context, ba *kvpb.BatchRequest) (*kvpb.BatchResponse, *kvpb.Error) {
	// A stale read at or below the closed timestamp needs no leader and
	// no wake: any replica serves it locally, a sleeping one included.
	if ba.Header.StaleRead && ba.IsReadOnly() && readTimestamp(ba).LessEq(r.ClosedTimestamp()) {
		return r.executeStaleRead(ba)
	}
	// Any other request wakes a sleeping replica: a leader resumes
	// heartbeating (and, for a lease read, waits for its followers'
	// answers, which pre-sleep contact no longer stands in for); a
	// follower resumes ticking, so if the leader the client is looking
	// for is gone it times out and campaigns (quiesce.go).
	if r.unquiesce() && r.isLeader() && ba.IsReadOnly() && r.leaseReads {
		r.awaitContact(ctx, 2*time.Duration(raftElectionTicks)*r.store.cfg.RaftTickInterval)
	}
	if !r.isLeader() {
		if ba.Header.StaleRead && ba.IsReadOnly() {
			return r.executeStaleRead(ba)
		}
		return nil, r.notLeaderError()
	}
	if len(ba.Requests) == 1 && ba.Requests[0].AdminSplit != nil {
		resp, err := r.adminSplit(ctx, ba.Requests[0].AdminSplit.Key)
		if err != nil {
			return nil, err
		}
		return &kvpb.BatchResponse{Responses: []kvpb.ResponseUnion{{AdminSplit: resp}}}, nil
	}
	if len(ba.Requests) == 1 && ba.Requests[0].AdminChangeReplicas != nil {
		resp, err := r.adminChangeReplicas(ctx, ba.Requests[0].AdminChangeReplicas)
		if err != nil {
			return nil, err
		}
		return &kvpb.BatchResponse{Responses: []kvpb.ResponseUnion{{AdminChangeReplicas: resp}}}, nil
	}
	if len(ba.Requests) == 1 && ba.Requests[0].AdminTransferLease != nil {
		resp, err := r.adminTransferLease(ctx, ba.Requests[0].AdminTransferLease)
		if err != nil {
			return nil, err
		}
		return &kvpb.BatchResponse{Responses: []kvpb.ResponseUnion{{AdminTransferLease: resp}}}, nil
	}
	if len(ba.Requests) == 1 && ba.Requests[0].AdminMerge != nil {
		resp, err := r.adminMerge(ctx)
		if err != nil {
			return nil, err
		}
		return &kvpb.BatchResponse{Responses: []kvpb.ResponseUnion{{AdminMerge: resp}}}, nil
	}
	if err := r.checkKeyBounds(ba); err != nil {
		return nil, err
	}
	spans, mode, serr := latchSpans(ba)
	if serr != nil {
		return nil, kvpb.NewError(serr)
	}
	// Load tracking: one count per batch, keyed by its first span. Admin
	// ops never reach here and non-leaders bailed above, so this measures
	// exactly the leader's served traffic.
	if len(spans) > 0 {
		r.load.record(spans[0].Start)
	}
	guard, gerr := r.latches.Acquire(ctx, spans, mode)
	if gerr != nil {
		return nil, kvpb.NewError(gerr)
	}
	defer guard.Release()
	// The frozen check sits AFTER latch acquisition: the Subsume command
	// holds a whole-range exclusive latch until it applies, so a request
	// that was in flight when the freeze landed waits it out here and then
	// observes the flag — nothing slips between flag-set and latch-release.
	if err := r.checkFrozen(ba); err != nil {
		return nil, err
	}
	if k := r.store.cfg.TestingKnobs.AfterLatch; k != nil {
		k(ba)
	}
	// Reads at or below the GC threshold could miss reclaimed versions:
	// reject them outright (non-retryable — the data is gone). GC commands
	// themselves are exempt.
	if !isGCBatch(ba) {
		if thr := r.GCThreshold(); !thr.IsEmpty() && readTimestamp(ba).LessEq(thr) {
			return nil, kvpb.NewErrorf("%s: batch timestamp %s is below the GC threshold %s", r.rangeID, readTimestamp(ba), thr)
		}
	}
	// An incremental export whose window opens below the GC threshold could
	// miss deletions whose tombstones GC has already reclaimed: refuse it
	// (non-retryable — the history is gone, the base backup is too old).
	for _, u := range ba.Requests {
		if exp := u.Export; exp != nil && !exp.StartTS.IsEmpty() {
			if thr := r.GCThreshold(); !thr.IsEmpty() && exp.StartTS.Less(thr) {
				return nil, kvpb.NewErrorf("%s: export start timestamp %s is below the GC threshold %s (incremental base too old)", r.rangeID, exp.StartTS, thr)
			}
		}
	}

	if ba.IsReadOnly() {
		// Bump the cache BEFORE evaluating (invariant L2): an overlapping
		// write checked after this point can no longer slip beneath us. The
		// bump covers exactly the batch's spans (the same ones latched), so
		// disjoint writers stay unaffected.
		r.tsCache.Bump(spans, readTimestamp(ba), batchTxnID(ba))
		if r.leaseReads && !r.leaseContactFresh() {
			// Lease reads skip the quorum round trip, so refuse to serve on
			// a possibly-expired lease; the client retries and either we
			// re-establish contact or a real new leader answers.
			return nil, r.notLeaderError()
		}
		if err := r.linearizableReadIndex(ctx); err != nil {
			if ctx.Err() == nil {
				// The confirmation round itself timed out (a fresh range
				// still electing, a partitioned quorum), not the caller:
				// leadership is in doubt, so answer NotLeader and let the
				// client re-route and retry under its own deadline instead
				// of surfacing a one-off failure.
				return nil, r.notLeaderError()
			}
			return nil, err
		}
		br, rerr := r.evalReadOnly(ba)
		if rerr != nil {
			return nil, rerr
		}
		if k := r.store.cfg.TestingKnobs.BeforeReadReturn; k != nil {
			k(ba)
		}
		// Re-check the lease immediately before returning: a stall during
		// evaluation must not let pre-stall contact vouch for this result.
		if r.leaseReads && !r.leaseContactFresh() {
			return nil, r.notLeaderError()
		}
		return br, nil
	}

	// Backpressure: shed table-data writes with a retryable error while the
	// engine is overloaded, well before Pebble's own hard write stall would
	// freeze raft appends and heartbeats for every range on this store.
	// Only user table data (0x04-prefixed) is gated — /system and /meta
	// writes (liveness heartbeats, descriptor updates, range metadata) must
	// keep flowing exactly when the store is struggling, and GC batches are
	// what dig it out. EndTxn/pushes/resolves write no MVCC versions and
	// are exempt via HasMVCCWrites, so intent cleanup is never blocked.
	if ba.HasMVCCWrites() && !isGCBatch(ba) && batchIsTableData(ba) {
		over, cause, why := false, "leader", ""
		if k := r.store.cfg.TestingKnobs.OverrideOverloaded; k != nil {
			over, why = k()
		} else if eng := r.store.cfg.Engine; eng != nil {
			var c storage.OverloadCause
			over, c, why = eng.OverloadedCause()
			if c == storage.CauseDebt {
				cause = "debt"
			}
		}
		if !over {
			// Quorum health: an overloaded member ANYWHERE in the replica
			// set sheds too — a sick follower otherwise lags raft silently
			// until it needs a catch-up snapshot (or the range quietly
			// rides one node from quorum loss). Verdicts are piggybacked
			// on raft traffic; an absent one reads as healthy, an
			// overloaded one holds until the peer reports healthy again
			// (a stalled follower sends nothing — see node_health.go).
			desc := r.Desc()
			for _, rep := range desc.Replicas {
				if rep.NodeID == r.store.cfg.NodeID {
					continue
				}
				if o, fwhy := r.store.NodeOverloaded(rep.NodeID); o {
					over, cause = true, "follower"
					why = fmt.Sprintf("follower n%d overloaded: %s", rep.NodeID, fwhy)
					break
				}
			}
		}
		if over {
			metrics.StorageBackpressure.Inc()
			metrics.StorageBackpressureCause.WithLabelValues(cause).Inc()
			e := kvpb.NewErrorf("%s: storage overloaded, write shed: %s", r.rangeID, why)
			e.StorageOverloaded = &kvpb.StorageOverloadedError{}
			e.TxnRetry = &kvpb.TxnRetryError{}
			return nil, e
		}
	}

	// No MVCC write may slip beneath a timestamp already served to readers
	// of another transaction. Transaction-record-only batches (EndTxn,
	// pushes, resolves) write no MVCC versions and are exempt. A
	// transactional write is not rejected but PUSHED: its provisional
	// intents simply land above the cache, the response echoes the
	// forwarded transaction, and the coordinator settles up at commit
	// (refresh, then the EndTxn — itself exempt here — flips the record at
	// the pushed timestamp). Rejecting instead would let a steady reader
	// starve every writer on the range: by the time the coordinator's
	// refresh round trip lands, the cache has moved again.
	if ba.HasMVCCWrites() {
		if is1PC(ba) {
			// A one-phase commit is never pushed in place: its EndTxn
			// evaluates in the same proposal, and the commit-equality
			// invariant (read ts == write ts) must hold at apply. Reject
			// pre-propose with a retryable error instead; the client's
			// refresh loop re-stamps and resends. This is the only extra
			// cost of 1PC, and only when the batch would actually be
			// pushed.
			txn := ba.Header.Txn
			if !txn.ReadTimestamp.Equal(txn.WriteTimestamp) {
				e := kvpb.NewErrorf("%s: one-phase commit of %s arrived with diverged timestamps (read %s, write %s)",
					r.rangeID, txn.ID, txn.ReadTimestamp, txn.WriteTimestamp)
				e.TxnRetry = &kvpb.TxnRetryError{RetryTimestamp: txn.WriteTimestamp}
				return nil, e
			}
			if ok, low := r.tsCache.AllowsWrite(mvccWriteSpans(ba), writeTimestamp(ba), batchTxnID(ba)); !ok {
				e := kvpb.NewErrorf("%s: one-phase commit of %s at %s below timestamp cache %s",
					r.rangeID, txn.ID, writeTimestamp(ba), low)
				e.TxnRetry = &kvpb.TxnRetryError{RetryTimestamp: low.Next()}
				return nil, e
			}
		} else if ok, low := r.tsCache.AllowsWrite(mvccWriteSpans(ba), writeTimestamp(ba), batchTxnID(ba)); !ok {
			if ba.Header.Txn == nil {
				e := kvpb.NewErrorf("%s: write timestamp %s below timestamp cache %s", r.rangeID, writeTimestamp(ba), low)
				e.TxnRetry = &kvpb.TxnRetryError{RetryTimestamp: low.Next()}
				return nil, e
			}
			pushed := *ba.Header.Txn
			pushed.WriteTimestamp = pushed.WriteTimestamp.Forward(low.Next())
			ba.Header.Txn = &pushed
		}
	}
	br, kerr := r.propose(ctx, ba)
	if kerr == nil && ba.Header.Txn != nil {
		br.Txn = ba.Header.Txn
	}
	return br, kerr
}

// checkKeyBounds verifies every request key is addressed to this range.
func (r *Replica) checkKeyBounds(ba *kvpb.BatchRequest) *kvpb.Error {
	desc := r.Desc()
	for _, u := range ba.Requests {
		req := u.GetInner()
		if req == nil {
			return kvpb.NewErrorf("empty request union")
		}
		h := req.Header()
		addr, err := addrOf(h.Key)
		if err != nil {
			return kvpb.NewError(err)
		}
		if !desc.ContainsKey(addr) {
			e := kvpb.NewErrorf("%s: key %s not in range [%s, %s)", r.rangeID, addr, desc.StartKey, desc.EndKey)
			e.RangeKeyMismatch = &kvpb.RangeKeyMismatchError{RequestKey: addr, ActualDescriptors: []kvpb.RangeDescriptor{desc}}
			return e
		}
		if len(h.EndKey) > 0 {
			endAddr, err := addrOf(h.EndKey)
			if err != nil {
				return kvpb.NewError(err)
			}
			if desc.EndKey.Compare(endAddr) < 0 {
				e := kvpb.NewErrorf("%s: span end %s beyond range end %s", r.rangeID, endAddr, desc.EndKey)
				e.RangeKeyMismatch = &kvpb.RangeKeyMismatchError{RequestKey: endAddr, ActualDescriptors: []kvpb.RangeDescriptor{desc}}
				return e
			}
		}
	}
	return nil
}

// batchIsTableData reports whether the batch's first MVCC-writing request
// targets user table data (the 0x04 table prefix) — the only writes the
// backpressure gate may shed.
func batchIsTableData(ba *kvpb.BatchRequest) bool {
	for _, u := range ba.Requests {
		req := u.GetInner()
		if req == nil || req.IsReadOnly() {
			continue
		}
		k := req.Header().Key
		return len(k) > 0 && k[0] == keys.TablePrefix[0]
	}
	return false
}

func isGCBatch(ba *kvpb.BatchRequest) bool {
	for _, u := range ba.Requests {
		if u.GC != nil {
			return true
		}
	}
	return false
}

// mvccWriteSpans returns the spans of the batch's MVCC-writing requests
// (Put/Delete/Increment, plus locking reads whose intents commit as
// versions) — the ones the timestamp cache gates. A mixed batch's plain
// reads are deliberately excluded: only what the batch WRITES can violate
// a served read.
func mvccWriteSpans(ba *kvpb.BatchRequest) []latchSpan {
	spans := make([]latchSpan, 0, len(ba.Requests))
	for _, u := range ba.Requests {
		switch r := u.GetInner().(type) {
		case *kvpb.PutRequest, *kvpb.DeleteRequest, *kvpb.IncrementRequest, *kvpb.UpdateMetaRequest:
		case *kvpb.GetRequest:
			if !r.ForUpdate {
				continue
			}
		case *kvpb.ScanRequest:
			if !r.ForUpdate {
				continue
			}
		default:
			continue
		}
		h := u.GetInner().Header()
		start, err := addrOf(h.Key)
		if err != nil {
			continue // unaddressable keys are caught by checkKeyBounds
		}
		sp := latchSpan{Start: start}
		if len(h.EndKey) > 0 {
			if end, err := addrOf(h.EndKey); err == nil {
				sp.End = end
			}
		}
		spans = append(spans, sp)
	}
	return spans
}

// is1PC reports whether the batch is one-phase-commit shaped: bound to a
// transaction, its last request a committing EndTxn with All set (the
// client's "this is the entire transaction" declaration), and every other
// request a plain Put or Delete. Deterministic from batch content alone,
// so the leader's pre-propose gate and every replica's apply agree.
func is1PC(ba *kvpb.BatchRequest) bool {
	if ba.Header.Txn == nil || len(ba.Requests) < 2 {
		return false
	}
	last := ba.Requests[len(ba.Requests)-1].EndTxn
	if last == nil || !last.Commit || !last.All {
		return false
	}
	for i := 0; i < len(ba.Requests)-1; i++ {
		switch ba.Requests[i].GetInner().(type) {
		case *kvpb.PutRequest, *kvpb.DeleteRequest:
		default:
			return false
		}
	}
	return true
}

func batchTxnID(ba *kvpb.BatchRequest) uuid.UUID {
	if ba.Header.Txn != nil {
		return ba.Header.Txn.ID
	}
	return uuid.Nil
}

func readTimestamp(ba *kvpb.BatchRequest) hlc.Timestamp {
	if ba.Header.Txn != nil {
		return ba.Header.Txn.ReadTimestamp
	}
	return ba.Header.Timestamp
}

func writeTimestamp(ba *kvpb.BatchRequest) hlc.Timestamp {
	if ba.Header.Txn != nil {
		return ba.Header.Txn.WriteTimestamp
	}
	return ba.Header.Timestamp
}

// raftLogger adapts etcd-raft logging to ours, keeping raft chatter at
// debug level.
type raftLogger struct{}

func (raftLogger) Debug(v ...any)                   { log.Debugf("raft: %s", fmt.Sprint(v...)) }
func (raftLogger) Debugf(format string, v ...any)   { log.Debugf("raft: "+format, v...) }
func (raftLogger) Error(v ...any)                   { log.Errorf("raft: %s", fmt.Sprint(v...)) }
func (raftLogger) Errorf(format string, v ...any)   { log.Errorf("raft: "+format, v...) }
func (raftLogger) Info(v ...any)                    { log.Debugf("raft: %s", fmt.Sprint(v...)) }
func (raftLogger) Infof(format string, v ...any)    { log.Debugf("raft: "+format, v...) }
func (raftLogger) Warning(v ...any)                 { log.Warnf("raft: %s", fmt.Sprint(v...)) }
func (raftLogger) Warningf(format string, v ...any) { log.Warnf("raft: "+format, v...) }
func (raftLogger) Fatal(v ...any)                   { log.Fatalf("raft: %s", fmt.Sprint(v...)) }
func (raftLogger) Fatalf(format string, v ...any)   { log.Fatalf("raft: "+format, v...) }
func (raftLogger) Panic(v ...any)                   { log.Fatalf("raft panic: %s", fmt.Sprint(v...)) }
func (raftLogger) Panicf(format string, v ...any)   { log.Fatalf("raft panic: "+format, v...) }
