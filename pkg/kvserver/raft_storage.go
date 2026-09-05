// Package kvserver implements the replicated range layer: one Raft
// consensus group per range (multi-raft), command application into MVCC
// storage, linearizable reads via ReadIndex, and the server-side
// transaction record operations. See docs/replication-and-placement.md.
package kvserver

import (
	"fmt"
	"sync"

	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"

	"github.com/sthorne/datax/pkg/base"
	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/kvpb"
	"github.com/sthorne/datax/pkg/storage"
)

// raftStorage implements raft.Storage on top of the store's Pebble engine,
// reading the per-replica unreplicated keys (HardState, log entries).
//
// The log is truncated by replicated TruncateLog commands (see truncate.go);
// FirstIndex is the truncation point plus one.
type raftStorage struct {
	eng     *storage.Engine // the raft engine (the state engine on a single-engine store)
	split   bool            // raft state lives on its own engine
	rangeID base.RangeID

	mu struct {
		sync.Mutex
		lastIndex uint64
		// confState is derived from the current range descriptor.
		confState raftpb.ConfState
		// applied mirrors the replica's applied index; Snapshot() reports
		// the state machine at this position.
		applied uint64
		// truncated tracks the (index, term) the log logically starts after.
		// Non-zero only for replicas seeded from a snapshot.
		truncated struct{ index, term uint64 }
	}
}

func newRaftStorage(raftEng, stateEng *storage.Engine, rangeID base.RangeID, desc kvpb.RangeDescriptor) (*raftStorage, error) {
	rs := &raftStorage{eng: raftEng, split: raftEng != stateEng, rangeID: rangeID}
	rs.setConfState(desc)

	// Recover truncated state and last index from disk.
	ts, err := loadTruncatedState(raftEng, stateEng, rangeID)
	if err != nil {
		return nil, err
	}
	rs.mu.truncated.index, rs.mu.truncated.term = ts.Index, ts.Term
	last := ts.Index
	lower := keys.RaftLogPrefix(rangeID)
	it := rs.eng.NewIter(lower, lower.PrefixEnd())
	for ok := it.SeekGE(lower); ok; ok = it.Next() {
		ent, err := decodeEntry(it.Value())
		if err != nil {
			_ = it.Close()
			return nil, err
		}
		if ent.Index > last {
			last = ent.Index
		}
	}
	if err := it.Close(); err != nil {
		return nil, err
	}
	rs.mu.lastIndex = last
	return rs, nil
}

func (rs *raftStorage) setConfState(desc kvpb.RangeDescriptor) {
	var cs raftpb.ConfState
	for _, r := range desc.Replicas {
		cs.Voters = append(cs.Voters, uint64(r.ReplicaID))
	}
	rs.setConfStateRaw(cs)
}

func (rs *raftStorage) setConfStateRaw(cs raftpb.ConfState) {
	rs.mu.Lock()
	rs.mu.confState = cs
	rs.mu.Unlock()
}

func (rs *raftStorage) truncatedState() truncatedState {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	return truncatedState{Index: rs.mu.truncated.index, Term: rs.mu.truncated.term}
}

func decodeEntry(raw []byte) (raftpb.Entry, error) {
	var ent raftpb.Entry
	if err := ent.Unmarshal(raw); err != nil {
		return ent, fmt.Errorf("corrupt raft log entry: %w", err)
	}
	return ent, nil
}

// InitialState implements raft.Storage.
func (rs *raftStorage) InitialState() (raftpb.HardState, raftpb.ConfState, error) {
	var hs raftpb.HardState
	raw, err := rs.eng.Get(keys.RaftHardStateKey(rs.rangeID))
	if err != nil {
		return hs, raftpb.ConfState{}, err
	}
	if raw != nil {
		if err := hs.Unmarshal(raw); err != nil {
			return hs, raftpb.ConfState{}, fmt.Errorf("corrupt HardState: %w", err)
		}
	}
	rs.mu.Lock()
	cs := rs.mu.confState
	rs.mu.Unlock()
	return hs, cs, nil
}

// Entries implements raft.Storage.
func (rs *raftStorage) Entries(lo, hi, maxSize uint64) ([]raftpb.Entry, error) {
	rs.mu.Lock()
	truncIdx := rs.mu.truncated.index
	last := rs.mu.lastIndex
	rs.mu.Unlock()
	if lo <= truncIdx {
		return nil, raft.ErrCompacted
	}
	if hi > last+1 {
		return nil, fmt.Errorf("entries(%d, %d): beyond last index %d", lo, hi, last)
	}

	var ents []raftpb.Entry
	var size uint64
	lower := keys.RaftLogKey(rs.rangeID, lo)
	upper := keys.RaftLogKey(rs.rangeID, hi)
	it := rs.eng.NewIter(lower, upper)
	defer func() { _ = it.Close() }()
	for ok := it.SeekGE(lower); ok; ok = it.Next() {
		ent, err := decodeEntry(it.Value())
		if err != nil {
			return nil, err
		}
		if len(ents) > 0 && size+uint64(ent.Size()) > maxSize {
			break
		}
		size += uint64(ent.Size())
		ents = append(ents, ent)
	}
	if len(ents) == 0 || ents[0].Index != lo {
		return nil, raft.ErrUnavailable
	}
	return ents, nil
}

// Term implements raft.Storage.
func (rs *raftStorage) Term(i uint64) (uint64, error) {
	rs.mu.Lock()
	truncIdx, truncTerm := rs.mu.truncated.index, rs.mu.truncated.term
	last := rs.mu.lastIndex
	rs.mu.Unlock()
	if i == truncIdx {
		return truncTerm, nil
	}
	if i < truncIdx {
		return 0, raft.ErrCompacted
	}
	if i > last {
		return 0, raft.ErrUnavailable
	}
	raw, err := rs.eng.Get(keys.RaftLogKey(rs.rangeID, i))
	if err != nil {
		return 0, err
	}
	if raw == nil {
		return 0, raft.ErrUnavailable
	}
	ent, err := decodeEntry(raw)
	if err != nil {
		return 0, err
	}
	return ent.Term, nil
}

// lastIndex is the newest entry's index (LastIndex without the error).
func (rs *raftStorage) lastIndex() uint64 {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	return rs.mu.lastIndex
}

// LastIndex implements raft.Storage.
func (rs *raftStorage) LastIndex() (uint64, error) {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	return rs.mu.lastIndex, nil
}

// FirstIndex implements raft.Storage.
func (rs *raftStorage) FirstIndex() (uint64, error) {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	return rs.mu.truncated.index + 1, nil
}

// setApplied mirrors the replica's applied index for Snapshot().
func (rs *raftStorage) setApplied(idx uint64) {
	rs.mu.Lock()
	if idx > rs.mu.applied {
		rs.mu.applied = idx
	}
	rs.mu.Unlock()
}

// Snapshot implements raft.Storage. The returned snapshot is METADATA ONLY
// — index, term, and voter set at the applied position. The actual state
// machine bytes never ride inside raft messages: when raft asks the leader
// to snapshot a follower, the replica intercepts the resulting MsgSnap and
// streams the data out of band (the same path that preseeds new replicas),
// then forwards a matching metadata-only MsgSnap. See sendRaftMessages.
//
// Only raft-error returns are legal here: etcd-raft treats any other error
// from a snapshot request as fatal (panic), and ErrSnapshotTemporarilyUnavailable
// simply makes it retry later.
func (rs *raftStorage) Snapshot() (raftpb.Snapshot, error) {
	rs.mu.Lock()
	applied := rs.mu.applied
	cs := rs.mu.confState
	rs.mu.Unlock()
	if applied == 0 || len(cs.Voters) == 0 {
		return raftpb.Snapshot{}, raft.ErrSnapshotTemporarilyUnavailable
	}
	term, err := rs.Term(applied)
	if err != nil {
		return raftpb.Snapshot{}, raft.ErrSnapshotTemporarilyUnavailable
	}
	return raftpb.Snapshot{Metadata: raftpb.SnapshotMetadata{
		Index: applied, Term: term, ConfState: cs,
	}}, nil
}

// append persists new log entries (and prunes any conflicting suffix) into
// the given batch. Caller commits the batch with sync=true before any
// messages are sent — the Raft durability contract.
func (rs *raftStorage) append(b *storage.Batch, ents []raftpb.Entry) error {
	if len(ents) == 0 {
		return nil
	}
	for i := range ents {
		raw, err := ents[i].Marshal()
		if err != nil {
			return err
		}
		if err := b.Put(keys.RaftLogKey(rs.rangeID, ents[i].Index), raw); err != nil {
			return err
		}
	}
	newLast := ents[len(ents)-1].Index

	rs.mu.Lock()
	oldLast := rs.mu.lastIndex
	rs.mu.Unlock()
	// Entries after newLast are from a divergent term and must not survive.
	for i := newLast + 1; i <= oldLast; i++ {
		if err := b.Delete(keys.RaftLogKey(rs.rangeID, i)); err != nil {
			return err
		}
	}
	rs.mu.Lock()
	rs.mu.lastIndex = newLast
	rs.mu.Unlock()
	return nil
}

// stageTruncate stages deletion of all log entries at or below index into b
// and adopts the new truncated state (index, term). Idempotent: replayed or
// stale truncations are no-ops. Called from the apply path only, so raft is
// not concurrently reading the storage (Ready handling is single-threaded).
func (rs *raftStorage) stageTruncate(b *storage.Batch, index, term uint64) error {
	rs.mu.Lock()
	old := rs.mu.truncated.index
	last := rs.mu.lastIndex
	rs.mu.Unlock()
	if index <= old {
		return nil
	}
	if index > last {
		return fmt.Errorf("truncating to %d beyond last index %d", index, last)
	}
	for i := old + 1; i <= index; i++ {
		if err := b.Delete(keys.RaftLogKey(rs.rangeID, i)); err != nil {
			return err
		}
	}
	rs.mu.Lock()
	rs.mu.truncated.index, rs.mu.truncated.term = index, term
	rs.mu.Unlock()
	return nil
}

// stageTruncatedState writes the current truncation point to the raft
// engine's own key (split stores: the applied state's copy is only a
// hint there, since the deletion it describes happens later).
func (rs *raftStorage) stageTruncatedState(b *storage.Batch) error {
	if !rs.split {
		return nil
	}
	return putTruncatedState(b, rs.rangeID, rs.truncatedState())
}

func (rs *raftStorage) setHardState(b *storage.Batch, hs raftpb.HardState) error {
	raw, err := hs.Marshal()
	if err != nil {
		return err
	}
	return b.Put(keys.RaftHardStateKey(rs.rangeID), raw)
}

// applyIncomingSnapshot resets log bookkeeping after the state machine was
// replaced by a snapshot at (index, term). Callers wipe the on-disk log, so
// the log becomes logically empty starting after index.
func (rs *raftStorage) applyIncomingSnapshot(index, term uint64) {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	rs.mu.truncated.index, rs.mu.truncated.term = index, term
	rs.mu.lastIndex = index
	if rs.mu.applied < index {
		rs.mu.applied = index
	}
}

var _ raft.Storage = (*raftStorage)(nil)
