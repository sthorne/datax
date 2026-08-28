package kvserver

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"

	"github.com/sthorne/datax/pkg/base"
	"github.com/sthorne/datax/pkg/kvpb"
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
}

// splitTrigger is carried by the replicated split command (Phase 3).
type splitTrigger struct {
	Left  kvpb.RangeDescriptor `json:"left"`
	Right kvpb.RangeDescriptor `json:"right"`
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

	node raft.Node
	rs   *raftStorage

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
		// ReadIndex coalescing: waiters register here; one goroutine drains
		// rounds while riInFlight.
		riPending  []chan readIndexResult
		riInFlight bool
	}
}

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

// newReplica loads or creates the replica and starts its Raft group.
// bootstrap must be true exactly once per replica lifetime — when the range
// is first created with its initial membership.
func newReplica(s *Store, desc kvpb.RangeDescriptor, replicaID base.ReplicaID, bootstrap bool) (*Replica, error) {
	rs, err := newRaftStorage(s.cfg.Engine, desc.RangeID, desc)
	if err != nil {
		return nil, err
	}
	st, err := loadReplicaState(s.cfg.Engine, desc.RangeID)
	if err != nil {
		return nil, err
	}
	r := &Replica{store: s, rangeID: desc.RangeID, replicaID: replicaID, rs: rs, latches: newLatchManager(), leaseReads: !s.cfg.DisableLeaseReads}
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

	cfg := raftConfig(uint64(replicaID), st.AppliedIndex, rs, r.leaseReads)
	if bootstrap {
		peers := make([]raft.Peer, 0, len(desc.Replicas))
		for _, rep := range desc.Replicas {
			peers = append(peers, raft.Peer{ID: uint64(rep.ReplicaID)})
		}
		r.node = raft.StartNode(cfg, peers)
	} else {
		r.node = raft.RestartNode(cfg)
	}

	if err := s.cfg.Stopper.RunWorker(r.raftLoop); err != nil {
		r.node.Stop()
		return nil, err
	}
	return r, nil
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

// SizeBytes returns the range's replicated approximate data size.
func (r *Replica) SizeBytes() int64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.mu.sizeBytes
}

// AppliedIndex returns the highest applied raft log index.
func (r *Replica) AppliedIndex() uint64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.mu.appliedIndex
}

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

func (r *Replica) notLeaderError() *kvpb.Error {
	e := kvpb.NewErrorf("%s: replica %d is not the leader", r.rangeID, r.replicaID)
	e.NotLeader = &kvpb.NotLeaderError{RangeID: r.rangeID, LeaderHint: r.leaderHint()}
	return e
}

// raftLoop is the replica's Ready-processing goroutine.
func (r *Replica) raftLoop(ctx context.Context) {
	ticker := time.NewTicker(r.store.cfg.RaftTickInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			r.node.Stop()
			return
		case <-ticker.C:
			r.node.Tick()
		case rd := <-r.node.Ready():
			err := r.handleReady(ctx, rd)
			if err == errReplicaRemoved {
				log.Infof("%s/%d: removed from range; shutting replica down", r.rangeID, r.replicaID)
				r.node.Stop()
				r.store.removeReplica(r.rangeID, r.Desc())
				return
			}
			if err != nil {
				log.Errorf("%s/%d: ready handling failed: %v", r.rangeID, r.replicaID, err)
				r.node.Stop()
				return
			}
			r.node.Advance()
		}
	}
}

func (r *Replica) handleReady(ctx context.Context, rd raft.Ready) error {
	// 1. Track leadership changes.
	if rd.SoftState != nil {
		r.mu.Lock()
		if rd.HardState.Term > r.mu.term {
			r.mu.term = rd.HardState.Term
		}
		term := r.mu.term
		prevLeader := r.mu.leader
		r.mu.leader = rd.SoftState.Lead
		becameLeader := rd.SoftState.RaftState == raft.StateLeader && prevLeader != uint64(r.replicaID)
		lostLeadership := prevLeader == uint64(r.replicaID) && rd.SoftState.RaftState != raft.StateLeader
		var pending []chan proposalResult
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
			r.tsCache.Bump(r.store.cfg.Clock.Now(), uuid.Nil)
		}
		for _, ch := range pending {
			ch <- proposalResult{err: &kvpb.Error{
				Message:   "leadership lost with proposal in flight",
				Ambiguous: &kvpb.AmbiguousResultError{},
			}}
		}
	}

	// 2. Persist HardState and new entries — synced, BEFORE sending any
	// messages (the Raft durability contract).
	if !raft.IsEmptyHardState(rd.HardState) || len(rd.Entries) > 0 {
		b := r.store.cfg.Engine.NewBatch()
		if !raft.IsEmptyHardState(rd.HardState) {
			if err := r.rs.setHardState(b, rd.HardState); err != nil {
				_ = b.Close()
				return err
			}
		}
		if err := r.rs.append(b, rd.Entries); err != nil {
			_ = b.Close()
			return err
		}
		if err := b.Commit(true); err != nil {
			return err
		}
	}

	if !raft.IsEmptySnap(rd.Snapshot) {
		if err := r.applySnapshot(rd.Snapshot); err != nil {
			return err
		}
	}

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
		if err := r.applyEntry(ent); err != nil {
			return err
		}
	}
	return nil
}

func (r *Replica) sendRaftMessages(ctx context.Context, msgs []raftpb.Message) {
	desc := r.Desc()
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
		if err := r.store.cfg.Transport.SendRaftMessage(ctx, target, r.rangeID, m); err != nil {
			// Raft tolerates message loss; report unreachability so it backs off.
			r.node.ReportUnreachable(m.To)
		}
	}
}

// stepRaftMessage feeds an incoming message into the Raft state machine.
func (r *Replica) stepRaftMessage(ctx context.Context, m raftpb.Message) error {
	switch m.Type {
	case raftpb.MsgHeartbeatResp, raftpb.MsgAppResp:
		// Follower contact, timestamped for the lease-read backstop.
		r.mu.Lock()
		r.mu.lastFollowerResp[m.From] = time.Now()
		r.mu.Unlock()
	}
	return r.node.Step(ctx, m)
}

// leaseContactFresh is the wall-clock backstop for lease-based reads: serve
// only while a majority (self included) has answered within
// electionTimeout − MaxOffset. A follower that answered at time T cannot
// vote for a new leader before T + electionTimeout (measured on its own
// tick clock), so within the window no new leader can exist and CheckQuorum
// guarantees this one still holds its lease. MaxOffset absorbs modest
// tick-rate skew between nodes; pathological scheduling skew beyond that is
// the residual (documented) risk, and the cost of a false negative is only
// a NotLeader retry.
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
		if now.Sub(ts) <= window {
			fresh++
		}
	}
	return fresh >= needed
}

// propose submits a write batch through Raft and waits for its application.
func (r *Replica) propose(ctx context.Context, ba *kvpb.BatchRequest) (*kvpb.BatchResponse, *kvpb.Error) {
	return r.proposeCmd(ctx, ba, nil)
}

// proposeCmd submits a write batch, optionally carrying a split trigger.
func (r *Replica) proposeCmd(ctx context.Context, ba *kvpb.BatchRequest, split *splitTrigger) (*kvpb.BatchResponse, *kvpb.Error) {
	cmd := raftCommand{ID: uuid.NewString(), Batch: *ba, Split: split}
	data, err := json.Marshal(&cmd)
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

	if err := r.node.Propose(ctx, data); err != nil {
		if err == raft.ErrProposalDropped {
			return nil, r.notLeaderError()
		}
		return nil, kvpb.NewError(err)
	}
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

	if err := r.node.ReadIndex(ctx, []byte(rctx)); err != nil {
		return 0, kvpb.NewError(err)
	}
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

// setApplied advances the applied index and wakes satisfied waiters.
// Called from the apply path only.
func (r *Replica) setApplied(idx uint64) {
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

// Execute serves a batch against this replica. Reads and writes are both
// leader-only in v1 (the leader owns the timestamp cache).
func (r *Replica) Execute(ctx context.Context, ba *kvpb.BatchRequest) (*kvpb.BatchResponse, *kvpb.Error) {
	if !r.isLeader() {
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
	if err := r.checkKeyBounds(ba); err != nil {
		return nil, err
	}
	spans, mode, serr := latchSpans(ba)
	if serr != nil {
		return nil, kvpb.NewError(serr)
	}
	guard, gerr := r.latches.Acquire(ctx, spans, mode)
	if gerr != nil {
		return nil, kvpb.NewError(gerr)
	}
	defer guard.Release()
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

	if ba.IsReadOnly() {
		// Bump the cache BEFORE evaluating (invariant L2): an overlapping
		// write checked after this point can no longer slip beneath us.
		r.tsCache.Bump(readTimestamp(ba), batchTxnID(ba))
		if r.leaseReads && !r.leaseContactFresh() {
			// Lease reads skip the quorum round trip, so refuse to serve on
			// a possibly-expired lease; the client retries and either we
			// re-establish contact or a real new leader answers.
			return nil, r.notLeaderError()
		}
		if err := r.linearizableReadIndex(ctx); err != nil {
			return nil, err
		}
		return r.evalReadOnly(ba)
	}

	// No MVCC write may slip beneath a timestamp already served to readers
	// of another transaction. Transaction-record-only batches (EndTxn,
	// pushes, resolves) write no MVCC versions and are exempt.
	if ba.HasMVCCWrites() {
		if ok, low := r.tsCache.AllowsWrite(writeTimestamp(ba), batchTxnID(ba)); !ok {
			e := kvpb.NewErrorf("%s: write timestamp %s below timestamp cache %s", r.rangeID, writeTimestamp(ba), low)
			e.TxnRetry = &kvpb.TxnRetryError{RetryTimestamp: low.Next()}
			return nil, e
		}
	}
	return r.propose(ctx, ba)
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

func isGCBatch(ba *kvpb.BatchRequest) bool {
	for _, u := range ba.Requests {
		if u.GC != nil {
			return true
		}
	}
	return false
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
