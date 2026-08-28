package kvserver

import (
	"context"
	"encoding/json"
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

	// latch serializes MVCC-write batches against reads: a write holds it
	// exclusively from its timestamp-cache check until it is applied, so a
	// read (shared holder) can never evaluate concurrently with a write
	// that already passed the check but is not yet visible. v1 keeps this
	// range-coarse; per-key latches are future work.
	latch sync.RWMutex

	mu struct {
		sync.Mutex
		desc         kvpb.RangeDescriptor
		appliedIndex uint64
		term         uint64 // highest raft term observed
		leader       uint64 // last known raft leader (replica ID); 0 unknown
		proposals    map[string]chan proposalResult
		readWaits    map[string]chan uint64
		appliedWaits []appliedWait
		destroyed    bool
	}
}

type appliedWait struct {
	idx uint64
	ch  chan struct{}
}

func raftConfig(id uint64, applied uint64, st raft.Storage) *raft.Config {
	return &raft.Config{
		ID:                        id,
		ElectionTick:              10,
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
	r := &Replica{store: s, rangeID: desc.RangeID, replicaID: replicaID, rs: rs}
	r.mu.desc = desc
	r.mu.appliedIndex = st.AppliedIndex
	if hs, _, err := rs.InitialState(); err == nil {
		r.mu.term = hs.Term
	}
	r.mu.proposals = make(map[string]chan proposalResult)
	r.mu.readWaits = make(map[string]chan uint64)

	cfg := raftConfig(uint64(replicaID), st.AppliedIndex, rs)
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
			if err := r.handleReady(ctx, rd); err != nil {
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
			continue // unknown recipient (stale message); drop
		}
		if err := r.store.cfg.Transport.SendRaftMessage(ctx, target, r.rangeID, m); err != nil {
			// Raft tolerates message loss; report unreachability so it backs off.
			r.node.ReportUnreachable(m.To)
		}
	}
}

// stepRaftMessage feeds an incoming message into the Raft state machine.
func (r *Replica) stepRaftMessage(ctx context.Context, m raftpb.Message) error {
	return r.node.Step(ctx, m)
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

// linearizableReadIndex runs the ReadIndex protocol: confirm leadership with
// a quorum, then wait until the applied state includes everything committed
// as of that confirmation.
func (r *Replica) linearizableReadIndex(ctx context.Context) *kvpb.Error {
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
		return kvpb.NewError(err)
	}
	select {
	case <-ctx.Done():
		return kvpb.NewErrorf("%s: read index abandoned: %v", r.rangeID, ctx.Err())
	case idx := <-ch:
		return r.waitApplied(ctx, idx)
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
	if err := r.checkKeyBounds(ba); err != nil {
		return nil, err
	}
	if ba.IsReadOnly() {
		r.latch.RLock()
		defer r.latch.RUnlock()
		// Bump the cache BEFORE evaluating: a write checked after this
		// point can no longer slip beneath us.
		r.tsCache.Bump(readTimestamp(ba), batchTxnID(ba))
		if err := r.linearizableReadIndex(ctx); err != nil {
			return nil, err
		}
		return r.evalReadOnly(ba)
	}

	r.latch.Lock()
	defer r.latch.Unlock()
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
