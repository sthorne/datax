package kvserver

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/kvpb"
	"github.com/sthorne/datax/pkg/metrics"
	"github.com/sthorne/datax/pkg/storage"
	"github.com/sthorne/datax/pkg/storage/enginepb"
	"github.com/sthorne/datax/pkg/util/hlc"
	"github.com/sthorne/datax/pkg/util/log"
)

// Garbage collection reclaims MVCC versions and transaction records that no
// possible read can observe anymore.
//
// The leader of each range periodically enumerates garbage from ONE
// consistent engine snapshot and proposes a replicated GCRequest naming the
// exact versions to delete plus the new threshold. Replication is what keeps
// this safe and simple:
//
//   - every replica deletes the same keys, so replicas stay byte-identical
//     (cross-replica checksums are a divergence tripwire in tests);
//   - the threshold is replicated state (replicaState.GCThreshold), so it
//     survives crashes, leadership changes, and preseed snapshots;
//   - the command takes the ordinary propose/apply path, so its exclusive
//     whole-range latch (Key = StartKey, EndKey = EndKey) serializes it
//     against every read on the range (invariant L1).
//
// What is garbage at threshold T: for each user key, the newest version at
// or below T is the "survivor" — it is exactly what a read just above T
// observes. Every older version is unreachable once reads at or below T are
// rejected, and the survivor itself is unreachable too if it is a deletion
// tombstone (reads above T see nothing). Keys with an unresolved intent are
// skipped entirely: their version history is still in flux.
//
// Enumerate-then-propose does not race with concurrent writes: committed
// versions are immutable, new writes land above the threshold (live
// transactions are TTL-younger than it), and intent resolution only touches
// keys the enumeration skipped.
//
// Transaction records: a finalized (committed or aborted) record whose
// timestamps are TTL-old is reclaimed — but a COMMITTED record is proof of
// its intents' fate, so it may only be deleted once every intent it wrote
// is resolved. Committed records carry the transaction's write set
// (Transaction.IntentKeys, recorded at commit); before collecting one, the
// leader resolves those keys through the routed sender — wherever their
// ranges live — and collects the record only when every resolve succeeded
// (resolution is idempotent, so racing the coordinator's own cleanup is
// harmless). ABORTED records are collectible outright: a pusher that later
// finds a record-less TTL-old intent aborts it, which is the correct
// outcome. createTxnRecord's resurrection guard rejects transactions born
// at or below the threshold, so a zombie coordinator cannot recreate a
// reclaimed record.

// gcChunkSize bounds how many items (versions + record keys) one replicated
// GC command carries, keeping raft entries reasonably sized.
const gcChunkSize = 1000

// StartHousekeeping starts the store's background maintenance loop. Each
// tick, for every range this store leads, garbage older than gcTTL is
// collected (gcTTL <= 0 disables GC) and the Raft log is truncated when
// enough of it is reclaimable. A non-positive interval disables the loop.
func (s *Store) StartHousekeeping(gcTTL, gcInterval time.Duration) error {
	if gcInterval <= 0 {
		return nil
	}
	return s.cfg.Stopper.RunWorker(func(ctx context.Context) {
		t := time.NewTicker(gcInterval)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				// With a RetentionOverride, GC must run even when the
				// store-wide TTL disables it — retention tables still age
				// out (RunGCOnce skips replicas whose effective TTL is 0).
				if gcTTL > 0 || s.cfg.RetentionOverride != nil {
					s.RunGCOnce(ctx, gcTTL)
				}
				s.RunLogTruncationOnce(ctx)
				s.RunAutoSplitOnce(ctx)
				s.RunRangeMergeOnce(ctx)
			}
		}
	})
}

// RunGCOnce runs one GC pass over every range this store currently leads,
// collecting garbage older than ttl — or than the range's
// RetentionOverride TTL when one applies. Exported for tests
// (time-compressed TTLs) and future debug tooling.
func (s *Store) RunGCOnce(ctx context.Context, ttl time.Duration) {
	now := s.cfg.Clock.Now()
	s.VisitReplicas(func(r *Replica) bool {
		if ctx.Err() != nil {
			return false
		}
		if !r.IsLeader() || r.isFrozen() {
			return true
		}
		effective, expire := ttl, false
		if s.cfg.RetentionOverride != nil {
			desc := r.Desc()
			if t, exp, ok := s.cfg.RetentionOverride(desc.StartKey, desc.EndKey); ok {
				effective, expire = t, exp
			}
		}
		// Row-level retention: a range that only partially overlaps
		// retention tables keeps the conservative TTL, but individual
		// rows past their table's retention still expire.
		var rowExpired func(keys.Key, hlc.Timestamp) bool
		if !expire && s.cfg.RowExpiry != nil {
			desc := r.Desc()
			if pred, ok := s.cfg.RowExpiry(desc.StartKey, desc.EndKey); ok {
				rowExpired = pred
			}
		}
		if effective <= 0 {
			return true // GC disabled for this range
		}
		threshold := now.AddNanos(-effective.Nanoseconds())
		if threshold.WallTime <= 0 {
			return true
		}
		if err := r.runGC(ctx, threshold, expire, rowExpired); err != nil {
			log.Warnf("%s: gc at threshold %s: %v", r.rangeID, threshold, err)
		}
		return true
	})
}

// runGC enumerates and collects this range's garbage below threshold.
// expire (retention ranges) collects EVERY version at or below the
// threshold, survivors included — the rows are past their retention.
// rowExpired, when non-nil (mixed ranges), additionally collects every
// version of a row the predicate ages out — above the threshold included
// — without changing how the threshold ratchets.
func (r *Replica) runGC(ctx context.Context, threshold hlc.Timestamp, expire bool, rowExpired func(keys.Key, hlc.Timestamp) bool) error {
	if threshold.LessEq(r.GCThreshold()) {
		return nil
	}
	desc := r.Desc()

	snap := r.store.cfg.Engine.NewSnapshot()
	versions, rowExpiredCount, liveIntentTxns, err := enumerateGarbageVersions(snap, desc, threshold, expire, rowExpired)
	var recordWork []gcTxnRecord
	if err == nil {
		recordWork, err = enumerateGarbageTxnRecords(snap, desc, threshold, liveIntentTxns)
	}
	if cerr := snap.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		return err
	}

	// A committed record is only reclaimable once every intent it wrote is
	// resolved; resolve first (idempotent), and keep the record for a later
	// pass if resolution cannot be completed now.
	var records []keys.Key
	for _, tr := range recordWork {
		if len(tr.resolve) > 0 && !r.resolveRecordIntents(ctx, tr) {
			continue
		}
		records = append(records, tr.key)
	}
	if len(versions) == 0 && len(records) == 0 {
		// Nothing to reclaim; leave the threshold where it is (raising it
		// would reject old reads for no benefit).
		return nil
	}

	for len(versions) > 0 || len(records) > 0 {
		var chunkV []kvpb.GCVersion
		var chunkR []keys.Key
		n := 0
		for len(versions) > 0 && n < gcChunkSize {
			chunkV = append(chunkV, versions[0])
			versions = versions[1:]
			n++
		}
		for len(records) > 0 && n < gcChunkSize {
			chunkR = append(chunkR, records[0])
			records = records[1:]
			n++
		}
		ba := &kvpb.BatchRequest{
			Header: kvpb.BatchHeader{RangeID: r.rangeID, Timestamp: r.store.cfg.Clock.Now()},
			Requests: []kvpb.RequestUnion{{GC: &kvpb.GCRequest{
				RequestHeader: kvpb.RequestHeader{Key: desc.StartKey, EndKey: desc.EndKey},
				Threshold:     threshold,
				Versions:      chunkV,
				TxnRecordKeys: chunkR,
			}}},
		}
		if _, kerr := r.Execute(ctx, ba); kerr != nil {
			return kerr
		}
		metrics.GCRuns.Inc()
		log.Debugf("%s: gc reclaimed %d versions, %d txn records (threshold %s)",
			r.rangeID, len(chunkV), len(chunkR), threshold)
	}
	if rowExpiredCount > 0 {
		metrics.RetentionRowsExpired.Add(float64(rowExpiredCount))
	}
	return nil
}

// enumerateGarbageVersions scans the range's MVCC span in one consistent
// snapshot and returns the versions that are garbage at threshold (plus
// how many of them rowExpired aged out beyond the ordinary rules), plus
// the set of transactions that still hold an intent somewhere in the
// range.
func enumerateGarbageVersions(snap *storage.Snapshot, desc kvpb.RangeDescriptor, threshold hlc.Timestamp, expire bool, rowExpired func(keys.Key, hlc.Timestamp) bool) ([]kvpb.GCVersion, int, map[uuid.UUID]struct{}, error) {
	lower := storage.EncodeMVCCKey(desc.StartKey, hlc.Timestamp{})
	upper := storage.EncodeMVCCKey(desc.EndKey, hlc.Timestamp{})
	it := snap.NewIter(lower, upper)
	defer func() { _ = it.Close() }()

	var out []kvpb.GCVersion
	rowExpiredCount := 0
	liveIntentTxns := make(map[uuid.UUID]struct{})
	ok := it.SeekGE(lower)
	for ok {
		userKey, vts, err := storage.DecodeMVCCKey(it.Key())
		if err != nil {
			return nil, 0, nil, err
		}
		cur := keys.Key(userKey).Clone()
		if vts.IsEmpty() {
			// Intent metadata: the key's history is in flux; skip it whole.
			if id, err := intentTxnID(it.Value()); err == nil {
				liveIntentTxns[id] = struct{}{}
			}
			ok = it.SeekGE(storage.EncodeMVCCKey(cur.Next(), hlc.Timestamp{}))
			continue
		}
		// Versions of cur, newest first. The newest at or below the
		// threshold is the survivor (what a read just above the threshold
		// sees) — garbage only if it is a tombstone; everything older is
		// always garbage.
		survivorSeen := false
		for ok {
			k, cvts, err := storage.DecodeMVCCKey(it.Key())
			if err != nil {
				return nil, 0, nil, err
			}
			if !cur.Equal(k) {
				break // next user key; outer loop re-examines this position
			}
			stored := int64(len(it.Key()) + len(it.Value()))
			if rowExpired != nil && rowExpired(cur, cvts) {
				// Row-level retention (mixed ranges): the row's own
				// timestamp column and this version's write age are both
				// past the table's retention — every such version goes,
				// survivors and tombstones included, independent of the
				// range's conservative threshold.
				out = append(out, kvpb.GCVersion{Key: cur, TS: cvts, Bytes: stored})
				rowExpiredCount++
				ok = it.Next()
				continue
			}
			if cvts.LessEq(threshold) {
				if survivorSeen || expire {
					// expire (retention ranges): the survivor goes too —
					// rows at or below the threshold are past retention,
					// and reads at or below it are rejected outright.
					out = append(out, kvpb.GCVersion{Key: cur, TS: cvts, Bytes: stored})
				} else {
					survivorSeen = true
					tomb, err := storage.IsTombstoneValue(it.Value())
					if err != nil {
						return nil, 0, nil, err
					}
					if tomb {
						out = append(out, kvpb.GCVersion{Key: cur, TS: cvts, Bytes: stored})
					}
				}
			}
			ok = it.Next()
		}
	}
	return out, rowExpiredCount, liveIntentTxns, nil
}

// intentTxnID extracts the owning transaction's ID from raw intent metadata.
func intentTxnID(raw []byte) (uuid.UUID, error) {
	meta, err := storage.DecodeMVCCMetadata(raw)
	if err != nil {
		return uuid.Nil, err
	}
	return meta.Txn.ID, nil
}

// gcTxnRecord is one reclaimable transaction record; resolve lists the
// committed write set that must be resolved before the record may go.
type gcTxnRecord struct {
	key      keys.Key
	resolve  []keys.Key
	txnID    uuid.UUID
	commitTS hlc.Timestamp
}

// enumerateGarbageTxnRecords scans the range's transaction records and
// returns the finalized, TTL-old ones. Committed records carry their write
// set for pre-collection resolution; records without one (aborted, or from
// transactions still holding an in-range intent) follow the conservative
// rules: aborted records are collectible outright, and records of
// transactions with a live in-range intent wait for lazy resolution.
func enumerateGarbageTxnRecords(snap *storage.Snapshot, desc kvpb.RangeDescriptor, threshold hlc.Timestamp, liveIntentTxns map[uuid.UUID]struct{}) ([]gcTxnRecord, error) {
	lo, hi := keys.RangeLocalAddressedSpan(desc.StartKey, desc.EndKey)
	it := snap.NewIter(lo, hi)
	defer func() { _ = it.Close() }()

	var out []gcTxnRecord
	for ok := it.SeekGE(lo); ok; ok = it.Next() {
		if !keys.IsTransactionKey(keys.Key(it.Key())) {
			// Leave unknown range-local keys alone.
			continue
		}
		txn, err := kvpb.UnmarshalTxnRecord(it.Value())
		if err != nil {
			return nil, fmt.Errorf("corrupt transaction record at %s: %w", keys.Pretty(keys.Key(it.Key())), err)
		}
		if txn.Status == enginepb.PENDING || txn.Status == enginepb.STAGING {
			// Live, expired-but-unpushed, or awaiting status recovery
			// (a STAGING record may be implicitly committed): not ours.
			continue
		}
		if !txn.WriteTimestamp.LessEq(threshold) || !txn.LastHeartbeat.LessEq(threshold) {
			continue
		}
		tr := gcTxnRecord{key: keys.Key(it.Key()).Clone(), txnID: txn.ID, commitTS: txn.WriteTimestamp}
		if txn.Status == enginepb.COMMITTED && len(txn.IntentKeys) > 0 {
			tr.resolve = txn.IntentKeys
		} else if _, held := liveIntentTxns[txn.ID]; held {
			// No recorded write set but an intent survives in this range:
			// leave the record for lazy resolution to consume first.
			continue
		}
		out = append(out, tr)
	}
	return out, nil
}

// resolveRecordIntents resolves a committed transaction's recorded write
// set through the routed sender (the keys may live on other ranges).
// Returns false when resolution could not be completed — the record then
// simply waits for a later GC pass.
func (r *Replica) resolveRecordIntents(ctx context.Context, tr gcTxnRecord) bool {
	sender := r.store.getSender()
	if sender == nil {
		return false
	}
	const chunk = 100
	for i := 0; i < len(tr.resolve); i += chunk {
		ba := &kvpb.BatchRequest{Header: kvpb.BatchHeader{Timestamp: r.store.cfg.Clock.Now()}}
		for _, k := range tr.resolve[i:min(i+chunk, len(tr.resolve))] {
			ba.Add(&kvpb.ResolveIntentRequest{
				RequestHeader: kvpb.RequestHeader{Key: k},
				TxnID:         tr.txnID,
				Status:        enginepb.COMMITTED,
				CommitTS:      tr.commitTS,
			})
		}
		if _, kerr := sender.Send(ctx, ba); kerr != nil {
			log.Warnf("%s: resolving intents of committed txn %s before record GC: %v", r.rangeID, tr.txnID, kerr)
			return false
		}
	}
	return true
}
