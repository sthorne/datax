package kvserver

import (
	"time"

	"github.com/sthorne/datax/pkg/base"
	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/metrics"
	"github.com/sthorne/datax/pkg/storage"
	"github.com/sthorne/datax/pkg/util/encoding"
	"github.com/sthorne/datax/pkg/util/log"
)

// The split store (issue #105). A store may keep its raft state — every
// replica's HardState, log entries and truncated state — on an engine of
// its own (StoreConfig.RaftEngine) while the state machine's engine runs
// without a write-ahead log. The raft engine's group-committed, synced
// appends are the durability record; the state engine's writes become
// durable when its memtable flushes, and whatever a crash loses is
// replayed from the log: a replica restarts at its last flushed applied
// index and raft hands it the committed entries above it again (apply is
// idempotent by construction). So every applied command is written to
// disk once — into the log — instead of twice, once into the log and
// once into the state engine's WAL.
//
// Two rules keep the engines consistent:
//
//   - Log truncation is deferred. A TruncateLog command applies on the
//     state engine like any other, but the entries at or below its index
//     are deleted from the raft engine only once the state engine has
//     flushed past the batch that applied that index (Engine.FlushedSeqNum
//     against the apply batch's SeqNum). Until then a crash could still
//     need them.
//   - Structural changes flush first. Before a merge or a replica removal
//     deletes a range's raft state, and before a catch-up snapshot resets
//     a log, the state engine flushes, so the two engines never disagree
//     about which replicas exist in a way replay cannot repair. A split
//     needs no flush: an RHS whose split was lost is re-created by the
//     LHS's replay, and finds its own raft state where it left it.
//
// A raft-engine range prefix without a descriptor on the state engine
// is swept at startup when the state engine holds a tombstone for it (a
// removal or merge whose raft-side deletion did not happen), and kept
// otherwise (an RHS whose split is being replayed).

// raftEngine is the engine that holds raft state: the store's own when
// split, the state engine otherwise.
func (s *Store) raftEngine() *storage.Engine {
	if s.cfg.RaftEngine != nil {
		return s.cfg.RaftEngine
	}
	return s.cfg.Engine
}

// splitEngines reports whether raft state lives on its own engine.
func (s *Store) splitEngines() bool { return s.cfg.RaftEngine != nil }

// flushState makes every state-engine write so far durable. Cheap on a
// store with a WAL (nothing to do), a memtable flush on a split store;
// called before the rare raft-side deletions that must not outrun the
// state they follow.
func (s *Store) flushState() error {
	if !s.splitEngines() || !s.cfg.Engine.WALDisabled() {
		return nil
	}
	return s.cfg.Engine.Flush()
}

// wipeRaftState deletes a range's raft state from the raft engine after
// its state-machine removal has been flushed (a merged RHS, a removed
// replica). On a single-engine store the caller's own batch already
// covered the range-local prefix, so this is a no-op.
func (s *Store) wipeRaftState(rangeID base.RangeID) error {
	if !s.splitEngines() {
		return nil
	}
	if err := s.flushState(); err != nil {
		return err
	}
	b := s.raftEngine().NewBatch()
	pre := keys.RangeUnreplicatedPrefix(rangeID)
	if err := b.DeleteRange(pre, pre.PrefixEnd()); err != nil {
		_ = b.Close()
		return err
	}
	return b.Commit(true)
}

// appliedSeq pairs an applied index with the engine sequence number of
// the batch that applied it; durableApplied compares the latter with the
// state engine's flushed watermark.
type appliedSeq struct {
	index uint64
	seq   uint64
}

// appliedSeqSpacing bounds the per-replica record: one entry per this
// many applied indexes (the newest is always kept exact).
const appliedSeqSpacing = 32

// noteAppliedSeq records that idx applied in the batch committed at seq
// (split stores only; a WAL makes every applied index durable at once).
func (r *Replica) noteAppliedSeq(idx, seq uint64) {
	if !r.store.splitEngines() {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	n := len(r.mu.applySeqs)
	if n > 0 && idx-r.mu.applySeqs[n-1].index < appliedSeqSpacing && n > 1 && idx-r.mu.applySeqs[n-2].index < appliedSeqSpacing {
		// Too close to the previous samples: move the newest forward.
		r.mu.applySeqs[n-1] = appliedSeq{index: idx, seq: seq}
		return
	}
	r.mu.applySeqs = append(r.mu.applySeqs, appliedSeq{index: idx, seq: seq})
}

// durableApplied is the highest applied index the state engine has
// flushed to an sstable — the point below which the raft log is no
// longer needed for crash recovery. On a single-engine store that is the
// applied index itself (its WAL made it durable).
func (r *Replica) durableApplied() uint64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.durableAppliedLocked()
}

// durableAppliedLocked is durableApplied under r.mu.
func (r *Replica) durableAppliedLocked() uint64 {
	if !r.store.splitEngines() {
		return r.mu.appliedIndex
	}
	flushed := r.store.cfg.Engine.FlushedSeqNum()
	durable := r.mu.durableApplied
	keep := 0
	for i, as := range r.mu.applySeqs {
		if as.seq <= flushed {
			if as.index > durable {
				durable = as.index
			}
			keep = i + 1
		} else {
			break
		}
	}
	if keep > 0 {
		r.mu.applySeqs = append(r.mu.applySeqs[:0], r.mu.applySeqs[keep:]...)
	}
	r.mu.durableApplied = durable
	return durable
}

// notePendingTruncate records an applied TruncateLog whose raft-side
// deletion waits for the state engine to flush past it.
func (r *Replica) notePendingTruncate(index, term uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if index > r.mu.pendingTrunc.Index {
		if r.mu.pendingTrunc.Index == 0 {
			r.mu.pendingSince = time.Now()
		}
		r.mu.pendingTrunc = truncatedState{Index: index, Term: term}
	}
}

// defaultTruncationFlushAfter is how long a pending truncation waits for
// a natural state-engine flush before the housekeeping tick forces one.
const defaultTruncationFlushAfter = 30 * time.Second

// truncationFlushAfter is the store's bound (StoreConfig).
func (s *Store) truncationFlushAfter() time.Duration {
	if s.cfg.TruncationFlushAfter == 0 {
		return defaultTruncationFlushAfter
	}
	return s.cfg.TruncationFlushAfter
}

// maybeTruncateDeferred performs the pending log truncation once the
// entries it removes are no longer needed: the state engine has flushed
// the apply of every index at or below it. Runs after each apply and
// from the housekeeping loop (a quiet range still flushes eventually).
func (r *Replica) maybeTruncateDeferred() error {
	r.mu.Lock()
	pending := r.mu.pendingTrunc
	r.mu.Unlock()
	if pending.Index == 0 || r.durableApplied() < pending.Index {
		return nil
	}
	r.applyMu.Lock()
	defer r.applyMu.Unlock()
	return r.maybeTruncateDeferredLocked()
}

// maybeTruncateDeferredLocked is maybeTruncateDeferred under applyMu
// (raft is not reading the log concurrently: Ready handling is
// single-threaded per replica and truncation runs on its apply path).
func (r *Replica) maybeTruncateDeferredLocked() error {
	r.mu.Lock()
	pending := r.mu.pendingTrunc
	r.mu.Unlock()
	if pending.Index == 0 || r.durableApplied() < pending.Index {
		return nil
	}
	r.mu.Lock()
	if r.mu.pendingTrunc.Index == pending.Index {
		r.mu.pendingTrunc = truncatedState{}
	}
	r.mu.Unlock()
	if r.raftStopped() {
		return nil
	}
	b := r.store.raftEngine().NewBatch()
	if err := r.rs.stageTruncate(b, pending.Index, pending.Term); err != nil {
		_ = b.Close()
		return err
	}
	if err := r.rs.stageTruncatedState(b); err != nil {
		_ = b.Close()
		return err
	}
	if b.Empty() {
		return b.Close()
	}
	// Unsynced: losing the deletion to a crash only leaves entries the
	// next truncation removes again.
	if err := b.Commit(false); err != nil {
		return err
	}
	metrics.RaftDeferredTruncations.Inc()
	return nil
}

// runDeferredTruncations gives every replica its chance to truncate
// (the housekeeping tick; flushes happen on their own schedule).
func (s *Store) runDeferredTruncations() {
	if !s.splitEngines() {
		return
	}
	// A quiet store may not flush for a long time (a memtable fills at
	// the write rate), and until it does every pending truncation holds
	// its log: past the bound, flush so the logs can go.
	if after := s.truncationFlushAfter(); after >= 0 {
		var oldest time.Time
		s.VisitReplicas(func(r *Replica) bool {
			r.mu.Lock()
			if r.mu.pendingTrunc.Index > 0 && r.durableAppliedLocked() < r.mu.pendingTrunc.Index && (oldest.IsZero() || r.mu.pendingSince.Before(oldest)) {
				oldest = r.mu.pendingSince
			}
			r.mu.Unlock()
			return true
		})
		if !oldest.IsZero() && time.Since(oldest) >= after {
			if err := s.cfg.Engine.Flush(); err != nil {
				log.Warnf("flushing the state engine for log truncation: %v", err)
			} else {
				metrics.RaftTruncationFlushes.Inc()
			}
		}
	}
	s.VisitReplicas(func(r *Replica) bool {
		if err := r.maybeTruncateDeferred(); err != nil {
			log.Warnf("%s: deferred log truncation: %v", r.rangeID, err)
		}
		return true
	})
}

// sweepOrphanRaftState removes raft state left behind on the raft engine
// by a merge or removal whose state-machine side was flushed (the state
// engine holds the tombstone) but whose raft-side deletion did not
// happen before a crash. Raft state without a tombstone is kept: it may
// be an RHS whose split the LHS is about to replay.
func (s *Store) sweepOrphanRaftState() error {
	if !s.splitEngines() {
		return nil
	}
	lower := keys.Key{0x01, 'u', 'r'}
	upper := lower.PrefixEnd()
	it := s.raftEngine().NewIter(lower, upper)
	seen := map[base.RangeID]struct{}{}
	for ok := it.SeekGE(lower); ok; ok = it.Next() {
		k := it.Key()
		if len(k) < len(lower)+8 {
			continue
		}
		_, rid, err := encoding.DecodeUint64(k[len(lower):])
		if err != nil {
			continue
		}
		seen[base.RangeID(rid)] = struct{}{}
	}
	if err := it.Close(); err != nil {
		return err
	}
	var swept int
	for rid := range seen {
		if _, ok, err := loadRangeDescriptor(s.cfg.Engine, rid); err != nil {
			return err
		} else if ok {
			continue
		}
		tomb, err := s.cfg.Engine.Get(keys.RangeTombstoneKey(rid))
		if err != nil {
			return err
		}
		if tomb == nil {
			continue
		}
		b := s.raftEngine().NewBatch()
		pre := keys.RangeUnreplicatedPrefix(rid)
		if err := b.DeleteRange(pre, pre.PrefixEnd()); err != nil {
			_ = b.Close()
			return err
		}
		if err := b.Commit(true); err != nil {
			return err
		}
		swept++
	}
	if swept > 0 {
		log.Infof("swept raft state of %d removed ranges from the raft engine", swept)
	}
	return nil
}

// MigrateRaftState moves a single-engine store's raft state — every
// replica's HardState, log entries and truncated state — onto a raft
// engine, and marks the state engine as split (keys.StoreRaftEngineKey).
// Idempotent: a store already migrated (or partly migrated by a crash)
// moves whatever remains. The caller opens both engines with their WALs
// on and flushes the state engine afterwards.
func MigrateRaftState(state, raft *storage.Engine) (moved int, err error) {
	lower := keys.Key{0x01, 'u', 'r'}
	upper := lower.PrefixEnd()
	rb := raft.NewBatch()
	sb := state.NewBatch()
	fail := func(err error) (int, error) {
		_ = rb.Close()
		_ = sb.Close()
		return 0, err
	}
	it := state.NewIter(lower, upper)
	ranges := map[base.RangeID]struct{}{}
	for ok := it.SeekGE(lower); ok; ok = it.Next() {
		k := keys.Key(it.Key())
		if len(k) >= len(lower)+8 {
			if _, rid, derr := encoding.DecodeUint64(k[len(lower):]); derr == nil {
				ranges[base.RangeID(rid)] = struct{}{}
			}
		}
		if !keys.IsRaftEngineKey(k) {
			continue
		}
		if err := rb.Put(k.Clone(), append([]byte(nil), it.Value()...)); err != nil {
			_ = it.Close()
			return fail(err)
		}
		if err := sb.Delete(k.Clone()); err != nil {
			_ = it.Close()
			return fail(err)
		}
		moved++
	}
	if err := it.Close(); err != nil {
		return fail(err)
	}
	// The truncated state, kept in the applied state until now, gets its
	// own key on the raft engine.
	for rid := range ranges {
		st, err := loadReplicaStateFrom(state, rid)
		if err != nil {
			return fail(err)
		}
		if st.TruncatedIndex == 0 && st.TruncatedTerm == 0 {
			continue
		}
		if existing, err := raft.Get(keys.RaftTruncatedStateKey(rid)); err != nil {
			return fail(err)
		} else if existing != nil {
			continue
		}
		if err := putTruncatedState(rb, rid, truncatedState{Index: st.TruncatedIndex, Term: st.TruncatedTerm}); err != nil {
			return fail(err)
		}
	}
	if err := sb.Put(keys.StoreRaftEngineKey(), []byte("1")); err != nil {
		return fail(err)
	}
	if err := rb.Commit(true); err != nil {
		_ = sb.Close()
		return 0, err
	}
	if err := sb.Commit(true); err != nil {
		return 0, err
	}
	return moved, nil
}

// IsSplitStore reports whether the state engine carries the split-store
// marker.
func IsSplitStore(state *storage.Engine) (bool, error) {
	raw, err := state.Get(keys.StoreRaftEngineKey())
	return raw != nil, err
}

// replayPending is how many committed entries a restarted replica will
// re-apply: the log's committed point past its applied state.
func (r *Replica) replayPending() uint64 {
	hs, _, err := r.rs.InitialState()
	if err != nil {
		return 0
	}
	r.mu.Lock()
	applied := r.mu.appliedIndex
	r.mu.Unlock()
	if hs.Commit > applied {
		return hs.Commit - applied
	}
	return 0
}
