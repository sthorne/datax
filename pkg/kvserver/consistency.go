package kvserver

import (
	"context"
	"crypto/sha256"
	"time"

	"github.com/google/uuid"
	"github.com/sthorne/datax/pkg/base"
	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/kvpb"
	"github.com/sthorne/datax/pkg/metrics"
	"github.com/sthorne/datax/pkg/storage"
	"github.com/sthorne/datax/pkg/util/hlc"
	"github.com/sthorne/datax/pkg/util/log"
)

// Replica consistency checking (issue #54). The leader proposes a
// checksum trigger through the raft log; every replica that applies it
// checksums the range's replicated state — the MVCC data span plus the
// range-local addressed keys (transaction records) — over an engine
// snapshot taken atomically at that applied index. Because replicas apply
// the same log prefix to the same state machine (GC included: it
// replicates the exact versions to delete), the state at a given applied
// index is byte-identical by construction; any divergence is corruption
// (disk fault, bug), which is exactly what this sweep exists to catch.
//
// The computation runs asynchronously on the captured snapshot so entry
// application never stalls behind hashing a large range; results park in
// a small per-replica map that the collection RPC (an admin op, see
// pkg/server) polls.

// checksumKeep bounds how many completed checksums a replica retains.
const checksumKeep = 4

type checksumResult struct {
	sum   []byte
	index uint64 // applied index the checksum was computed at
	at    time.Time
}

// startChecksum captures an engine snapshot at the current applied state
// (called post-commit under applyMu, so the snapshot is exactly this
// entry's state) and computes the range checksum in the background.
func (r *Replica) startChecksum(id string, idx uint64) {
	snap := r.store.cfg.Engine.NewSnapshot()
	desc := r.Desc()
	go func() {
		defer func() { _ = snap.Close() }()
		sum, err := checksumRangeState(snap, desc)
		if err != nil {
			log.Errorf("%s: consistency checksum %s: %v", r.rangeID, id, err)
			return
		}
		r.mu.Lock()
		if r.mu.checksums == nil {
			r.mu.checksums = map[string]checksumResult{}
		}
		r.mu.checksums[id] = checksumResult{sum: sum, index: idx, at: time.Now()}
		if len(r.mu.checksums) > checksumKeep {
			oldestID, oldest := "", time.Time{}
			for cid, res := range r.mu.checksums {
				if oldest.IsZero() || res.at.Before(oldest) {
					oldestID, oldest = cid, res.at
				}
			}
			delete(r.mu.checksums, oldestID)
		}
		r.mu.Unlock()
	}()
}

// getChecksum returns a completed checksum computation, if present.
func (r *Replica) getChecksum(id string) (checksumResult, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	res, ok := r.mu.checksums[id]
	return res, ok
}

// checksumRangeState hashes the range's replicated content in fixed span
// order: range-local addressed keys, then the MVCC data span — raw engine
// key/value bytes, so every version, tombstone, intent, and transaction
// record participates.
func checksumRangeState(snap *storage.Snapshot, desc kvpb.RangeDescriptor) ([]byte, error) {
	h := sha256.New()
	loLocal, hiLocal := keys.RangeLocalAddressedSpan(desc.StartKey, desc.EndKey)
	spans := [][2][]byte{
		{[]byte(loLocal), []byte(hiLocal)},
		{storage.EncodeMVCCKey(desc.StartKey, hlc.Timestamp{}), storage.EncodeMVCCKey(desc.EndKey, hlc.Timestamp{})},
	}
	var lenBuf [8]byte
	writeChunk := func(b []byte) {
		n := len(b)
		for i := 0; i < 8; i++ {
			lenBuf[i] = byte(n >> (8 * i))
		}
		_, _ = h.Write(lenBuf[:])
		_, _ = h.Write(b)
	}
	for _, sp := range spans {
		it := snap.NewIter(sp[0], sp[1])
		for ok := it.SeekGE(sp[0]); ok; ok = it.Next() {
			writeChunk(it.Key())
			writeChunk(it.Value())
		}
		if err := it.Close(); err != nil {
			return nil, err
		}
	}
	return h.Sum(nil), nil
}

// ConsistencyProbe is one proposed consistency check: the leader's own
// result plus what a collector needs to query the other replicas.
type ConsistencyProbe struct {
	RangeID  base.RangeID
	CheckID  string
	Desc     kvpb.RangeDescriptor
	LocalSum []byte
	Index    uint64
}

// checksumWait bounds how long proposers and collectors wait for an
// asynchronous checksum computation to finish.
const checksumWait = 10 * time.Second

// waitChecksum polls for a computation to complete.
func (r *Replica) waitChecksum(ctx context.Context, id string) (checksumResult, bool) {
	deadline := time.Now().Add(checksumWait)
	for {
		if res, ok := r.getChecksum(id); ok {
			return res, true
		}
		if time.Now().After(deadline) || ctx.Err() != nil {
			return checksumResult{}, false
		}
		select {
		case <-ctx.Done():
			return checksumResult{}, false
		case <-time.After(20 * time.Millisecond):
		}
	}
}

// ProposeChecksum runs one consistency probe on the next range this store
// leads (round-robin), returning the leader's own checksum and the
// descriptor so the caller can collect and compare the followers'. Returns
// nil when the store leads no eligible range this pass.
func (s *Store) ProposeChecksum(ctx context.Context) (*ConsistencyProbe, error) {
	var replicas []*Replica
	s.VisitReplicas(func(r *Replica) bool {
		if r.isLeader() && !r.isFrozen() {
			replicas = append(replicas, r)
		}
		return true
	})
	if len(replicas) == 0 {
		return nil, nil
	}
	s.consistencyMu.Lock()
	cursor := s.consistencyMu.cursor
	s.consistencyMu.cursor++
	s.consistencyMu.Unlock()
	r := replicas[int(cursor%uint64(len(replicas)))]
	return s.ProposeChecksumOn(ctx, r.rangeID)
}

// ProposeChecksumOn runs one consistency probe on a specific range this
// store leads.
func (s *Store) ProposeChecksumOn(ctx context.Context, rangeID base.RangeID) (*ConsistencyProbe, error) {
	r, ok := s.GetReplica(rangeID)
	if !ok {
		return nil, kvpb.NewErrorf("%s: no replica on this store", rangeID)
	}
	if !r.isLeader() {
		return nil, kvpb.NewErrorf("%s: not the leader", rangeID)
	}
	id := uuid.NewString()
	if _, kerr := r.proposeCmd(ctx, &kvpb.BatchRequest{Header: kvpb.BatchHeader{RangeID: r.rangeID}},
		cmdTriggers{checksum: &checksumTrigger{ID: id}}); kerr != nil {
		return nil, kerr
	}
	res, ok := r.waitChecksum(ctx, id)
	if !ok {
		return nil, kvpb.NewErrorf("%s: local checksum %s did not complete", r.rangeID, id)
	}
	metrics.ConsistencyChecks.Inc()
	return &ConsistencyProbe{RangeID: r.rangeID, CheckID: id, Desc: r.Desc(), LocalSum: res.sum, Index: res.index}, nil
}

// LookupChecksum serves a collector's query for this store's result of a
// probe, waiting for the trigger to apply and compute if necessary.
func (s *Store) LookupChecksum(ctx context.Context, rangeID base.RangeID, checkID string) ([]byte, uint64, bool) {
	r, ok := s.GetReplica(rangeID)
	if !ok {
		return nil, 0, false
	}
	res, ok := r.waitChecksum(ctx, checkID)
	if !ok {
		return nil, 0, false
	}
	return res.sum, res.index, true
}
