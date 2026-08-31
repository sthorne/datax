package kvserver

import (
	"context"
	"fmt"

	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"

	"github.com/sthorne/datax/pkg/base"
	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/metrics"
	"github.com/sthorne/datax/pkg/storage"
	"github.com/sthorne/datax/pkg/util/hlc"
	"github.com/sthorne/datax/pkg/util/log"
)

// Raft catch-up snapshots. When a follower needs entries the leader has
// truncated, raft requests a snapshot (raftStorage.Snapshot — metadata
// only) and emits a MsgSnap. Snapshot bytes never ride inside raft
// messages: sendRaftMessages intercepts the MsgSnap and this file streams
// the state machine out of band over the same path that preseeds new
// replicas, then forwards a metadata-only MsgSnap matching what was
// streamed and reports the outcome to raft. The receiver installs the
// stream into its existing replica BEFORE the raft message arrives, so
// when raft asks the follower to restore, the storage already agrees and
// the restore is an acknowledgement.
//
// A replica whose range BOUNDS changed while it was away (it missed a
// split) cannot be caught up by snapshot — its old span may overlap
// sibling replicas on the same store, and wiping it would destroy their
// data. Such a replica is refused (raft keeps retrying at its own slow
// pace) and is repaired by removal and re-add — the same remedy as a dead
// node. Documented in docs/replication-and-placement.md.

// startCatchupSnapshot begins one out-of-band snapshot stream to the
// follower a MsgSnap addresses, deduplicating per follower.
func (r *Replica) startCatchupSnapshot(target base.NodeID, m raftpb.Message) {
	r.mu.Lock()
	if _, busy := r.mu.snapInFlight[m.To]; busy {
		r.mu.Unlock()
		return // already streaming; raft re-requests if this one fails
	}
	// Reserve the slot (streamSnapshot re-registers with the exact index it
	// captures); the reservation also pins log truncation.
	idx := r.mu.appliedIndex
	if m.Snapshot != nil && m.Snapshot.Metadata.Index < idx {
		idx = m.Snapshot.Metadata.Index
	}
	r.mu.snapInFlight[m.To] = idx
	r.mu.Unlock()

	if err := r.store.cfg.Stopper.RunWorker(func(ctx context.Context) {
		defer func() {
			r.mu.Lock()
			delete(r.mu.snapInFlight, m.To)
			r.mu.Unlock()
		}()
		r.runCatchupSnapshot(ctx, target, m)
	}); err != nil {
		r.mu.Lock()
		delete(r.mu.snapInFlight, m.To)
		r.mu.Unlock()
	}
}

func (r *Replica) runCatchupSnapshot(ctx context.Context, target base.NodeID, m raftpb.Message) {
	desc := r.Desc()
	hdr, err := r.streamSnapshot(ctx, target, desc, base.ReplicaID(m.To))
	if err != nil {
		log.Warnf("%s: catch-up snapshot to n%d (replica %d): %v", r.rangeID, target, m.To, err)
		r.node.ReportSnapshot(m.To, raft.SnapshotFailure)
		return
	}
	// Forward a metadata-only MsgSnap at the position actually streamed
	// (the engine may have applied past raft's requested index). ConfState
	// must name the receiver or raft ignores the restore.
	snap := raftpb.Snapshot{Metadata: raftpb.SnapshotMetadata{Index: hdr.AppliedIndex, Term: hdr.Term}}
	for _, rep := range desc.Replicas {
		snap.Metadata.ConfState.Voters = append(snap.Metadata.ConfState.Voters, uint64(rep.ReplicaID))
	}
	fm := m
	fm.Snapshot = &snap
	if err := r.store.cfg.Transport.SendRaftMessage(ctx, target, r.rangeID, fm); err != nil {
		r.node.ReportSnapshot(m.To, raft.SnapshotFailure)
		r.node.ReportUnreachable(m.To)
		return
	}
	r.node.ReportSnapshot(m.To, raft.SnapshotFinish)
	metrics.CatchupSnapshots.Inc()
	log.Infof("%s: caught up replica %d on n%d by snapshot at index %d", r.rangeID, m.To, target, hdr.AppliedIndex)
}

// minSnapshotInFlight returns the lowest applied index of any outgoing
// snapshot stream (0 = none): log truncation must not advance past it, or
// the receiver could not be caught up after its install.
func (r *Replica) minSnapshotInFlight() uint64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	var min uint64
	for _, idx := range r.mu.snapInFlight {
		if min == 0 || idx < min {
			min = idx
		}
	}
	return min
}

// pendingSnapshot is a fully staged (uncommitted) snapshot install, waiting
// for raft to restore: the receiver may not touch raft's storage until the
// metadata MsgSnap has gone through raft and comes back as Ready.Snapshot —
// mutating the log bookkeeping underneath a live raft node panics it.
type pendingSnapshot struct {
	b     *storage.Batch
	h     snapshotHeader
	count int
}

// installSnapshot stages a streamed snapshot for an existing replica: the
// engine batch wipes the range's replicated content and stale raft log
// (the HardState survives — term and vote belong to raft, not the state
// machine) and installs the streamed keys, but nothing is committed here.
// The commit happens in applySnapshot, when raft delivers the matching
// metadata snapshot through its own Ready flow.
func (s *Store) installSnapshot(r *Replica, h snapshotHeader, next func() ([]SnapshotKV, error)) error {
	if h.AppliedIndex <= r.AppliedIndex() {
		return fmt.Errorf("%s: stale snapshot at index %d (applied %d)", r.rangeID, h.AppliedIndex, r.AppliedIndex())
	}
	old := r.Desc()
	if !old.StartKey.Equal(h.Desc.StartKey) || !old.EndKey.Equal(h.Desc.EndKey) {
		return fmt.Errorf("%s: bounds changed ([%s,%s) -> [%s,%s)); replica must be removed and re-added",
			r.rangeID, old.StartKey, old.EndKey, h.Desc.StartKey, h.Desc.EndKey)
	}

	b := s.cfg.Engine.NewBatch()
	fail := func(err error) error {
		_ = b.Close()
		return err
	}
	if err := b.DeleteRange(storage.EncodeMVCCKey(h.Desc.StartKey, hlc.Timestamp{}), storage.EncodeMVCCKey(h.Desc.EndKey, hlc.Timestamp{})); err != nil {
		return fail(err)
	}
	loL, hiL := keys.RangeLocalAddressedSpan(h.Desc.StartKey, h.Desc.EndKey)
	if err := b.DeleteRange(loL, hiL); err != nil {
		return fail(err)
	}
	logPre := keys.RaftLogPrefix(r.rangeID)
	if err := b.DeleteRange(logPre, logPre.PrefixEnd()); err != nil {
		return fail(err)
	}
	count := 0
	for {
		kvs, err := next()
		if err != nil {
			return fail(err)
		}
		if len(kvs) == 0 {
			break
		}
		for _, kv := range kvs {
			if err := b.Put(kv.Key, kv.Value); err != nil {
				return fail(err)
			}
			count++
		}
	}
	if err := putReplicaState(b, r.rangeID, replicaState{
		AppliedIndex:   h.AppliedIndex,
		TruncatedIndex: h.AppliedIndex,
		TruncatedTerm:  h.Term,
		GCThreshold:    h.GCThreshold,
		SizeBytes:      h.SizeBytes,
	}); err != nil {
		return fail(err)
	}
	if err := PutRangeDescriptor(b, h.Desc); err != nil {
		return fail(err)
	}

	r.mu.Lock()
	prev := r.mu.pendingInstall
	r.mu.pendingInstall = &pendingSnapshot{b: b, h: h, count: count}
	r.mu.Unlock()
	if prev != nil {
		_ = prev.b.Close() // superseded by a fresh stream
	}
	log.Infof("%s: replica %d staged catch-up snapshot (%d keys, applied index %d); awaiting raft restore",
		r.rangeID, r.replicaID, count, h.AppliedIndex)
	return nil
}

// applySnapshot commits the staged install when raft restores to the
// snapshot the leader forwarded. Runs on the raft loop (between Ready and
// Advance), so raft is not concurrently reading its storage; applyMu
// additionally excludes ordinary entry application.
func (r *Replica) applySnapshot(snap raftpb.Snapshot) error {
	r.mu.Lock()
	p := r.mu.pendingInstall
	r.mu.pendingInstall = nil
	r.mu.Unlock()

	if p == nil {
		if snap.Metadata.Index <= r.AppliedIndex() {
			return nil // duplicate restore of already-installed state
		}
		return fmt.Errorf("%s: raft snapshot at index %d with no staged data stream",
			r.rangeID, snap.Metadata.Index)
	}
	if p.h.AppliedIndex != snap.Metadata.Index {
		_ = p.b.Close()
		return fmt.Errorf("%s: staged snapshot at index %d does not match raft restore at %d",
			r.rangeID, p.h.AppliedIndex, snap.Metadata.Index)
	}

	r.applyMu.Lock()
	defer r.applyMu.Unlock()
	if p.h.AppliedIndex <= r.AppliedIndex() {
		_ = p.b.Close()
		return nil // overtaken; the state machine never moves backward
	}
	if err := p.b.Commit(true); err != nil {
		return err
	}
	r.rs.applyIncomingSnapshot(p.h.AppliedIndex, p.h.Term)
	r.rs.setConfState(p.h.Desc)
	r.mu.Lock()
	r.mu.desc = p.h.Desc
	r.mu.gcThreshold = p.h.GCThreshold
	r.mu.sizeBytes = p.h.SizeBytes
	r.mu.Unlock()
	r.setApplied(p.h.AppliedIndex)
	log.Infof("%s: replica %d state replaced by catch-up snapshot (%d keys, applied index %d)",
		r.rangeID, r.replicaID, p.count, p.h.AppliedIndex)
	return nil
}
