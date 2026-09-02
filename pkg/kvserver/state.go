package kvserver

import (
	"encoding/json"
	"fmt"

	"github.com/sthorne/datax/pkg/util/hlc"

	"github.com/sthorne/datax/pkg/base"
	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/kvpb"
	"github.com/sthorne/datax/pkg/storage"
)

// replicaState is the per-replica applied state, persisted at
// RaftAppliedStateKey atomically with every application batch. This is what
// makes crash-recovery replay idempotent: entries at or below AppliedIndex
// are skipped.
type replicaState struct {
	AppliedIndex uint64 `json:"applied_index"`
	// TruncatedIndex/Term describe the log position the state machine
	// already includes (non-zero only for snapshot-seeded replicas).
	TruncatedIndex uint64 `json:"truncated_index,omitempty"`
	TruncatedTerm  uint64 `json:"truncated_term,omitempty"`
	// GCThreshold: reads at or below this timestamp are rejected; versions
	// beneath it may have been reclaimed. Replicated state, raised only by
	// applied GC commands.
	GCThreshold hlc.Timestamp `json:"gc_threshold,omitempty"`
	// SizeBytes approximates the range's replicated MVCC data size. Staged
	// deterministically at apply (every replica computes the same value
	// from the same commands), recomputed exactly at splits, and reduced by
	// GC. Drives size-based auto-splits.
	SizeBytes int64 `json:"size_bytes,omitempty"`
	// Frozen: a Subsume command applied — the range refuses all traffic
	// until it is absorbed by MergedInto (or unfrozen). Replicated so every
	// future leader and every restart honors the freeze.
	Frozen     bool         `json:"frozen,omitempty"`
	MergedInto base.RangeID `json:"merged_into,omitempty"`
	// ClosedTS: no write at or below this timestamp will ever commit on
	// this range. Replicated (raised by applied closed-timestamp commands;
	// log order guarantees every earlier write applied first), so any
	// replica may serve reads at or below it locally — follower reads.
	ClosedTS hlc.Timestamp `json:"closed_ts,omitempty"`
}

// getter is the read capability shared by Engine, Batch, and Snapshot.
type getter interface {
	Get(key []byte) ([]byte, error)
}

func loadReplicaState(eng *storage.Engine, rangeID base.RangeID) (replicaState, error) {
	return loadReplicaStateFrom(eng, rangeID)
}

func loadReplicaStateFrom(r getter, rangeID base.RangeID) (replicaState, error) {
	var st replicaState
	raw, err := r.Get(keys.RaftAppliedStateKey(rangeID))
	if err != nil {
		return st, err
	}
	if raw == nil {
		return st, nil
	}
	if err := json.Unmarshal(raw, &st); err != nil {
		return st, fmt.Errorf("corrupt replica state for %s: %w", rangeID, err)
	}
	return st, nil
}

func putReplicaState(w storage.Writer, rangeID base.RangeID, st replicaState) error {
	raw, err := json.Marshal(st)
	if err != nil {
		return err
	}
	return w.Put(keys.RaftAppliedStateKey(rangeID), raw)
}

type truncatedState struct{ Index, Term uint64 }

func loadTruncatedState(eng *storage.Engine, rangeID base.RangeID) (truncatedState, error) {
	st, err := loadReplicaState(eng, rangeID)
	if err != nil {
		return truncatedState{}, err
	}
	return truncatedState{Index: st.TruncatedIndex, Term: st.TruncatedTerm}, nil
}

// loadRangeDescriptor reads a replica's local descriptor copy.
func loadRangeDescriptor(eng *storage.Engine, rangeID base.RangeID) (kvpb.RangeDescriptor, bool, error) {
	var desc kvpb.RangeDescriptor
	raw, err := eng.Get(keys.RangeDescriptorKey(rangeID))
	if err != nil || raw == nil {
		return desc, false, err
	}
	if err := json.Unmarshal(raw, &desc); err != nil {
		return desc, false, fmt.Errorf("corrupt range descriptor for %s: %w", rangeID, err)
	}
	return desc, true, nil
}

// PutRangeDescriptor writes a replica's local descriptor copy. Exported for
// bootstrap (pkg/cluster) and snapshot application.
func PutRangeDescriptor(w storage.Writer, desc kvpb.RangeDescriptor) error {
	raw, err := json.Marshal(desc)
	if err != nil {
		return err
	}
	return w.Put(keys.RangeDescriptorKey(desc.RangeID), raw)
}
