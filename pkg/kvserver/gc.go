package kvserver

import (
	"context"
	"encoding/json"
	"time"

	"github.com/google/uuid"

	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/kvpb"
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
// timestamps are TTL-old is deleted, unless this range still holds an
// intent of that transaction (the enumeration pass sees every intent, so
// this check is free). A pusher that later finds a record-less intent from
// a TTL-old transaction judges it expired and aborts it — correct for
// aborted transactions, and for committed ones the coordinator resolves
// intents promptly after commit; an intent orphaned for a whole TTL on a
// range OTHER than the record's is the residual (documented) hazard.
// createTxnRecord's resurrection guard rejects transactions born at or
// below the threshold, so a zombie coordinator cannot recreate a reclaimed
// record.

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
				if gcTTL > 0 {
					s.RunGCOnce(ctx, gcTTL)
				}
				s.RunLogTruncationOnce(ctx)
			}
		}
	})
}

// RunGCOnce runs one GC pass over every range this store currently leads,
// collecting garbage older than ttl. Exported for tests (time-compressed
// TTLs) and future debug tooling.
func (s *Store) RunGCOnce(ctx context.Context, ttl time.Duration) {
	threshold := s.cfg.Clock.Now().AddNanos(-ttl.Nanoseconds())
	if threshold.WallTime <= 0 {
		return
	}
	s.VisitReplicas(func(r *Replica) bool {
		if ctx.Err() != nil {
			return false
		}
		if !r.IsLeader() {
			return true
		}
		if err := r.runGC(ctx, threshold); err != nil {
			log.Warnf("%s: gc at threshold %s: %v", r.rangeID, threshold, err)
		}
		return true
	})
}

// runGC enumerates and collects this range's garbage below threshold.
func (r *Replica) runGC(ctx context.Context, threshold hlc.Timestamp) error {
	if threshold.LessEq(r.GCThreshold()) {
		return nil
	}
	desc := r.Desc()

	snap := r.store.cfg.Engine.NewSnapshot()
	versions, liveIntentTxns, err := enumerateGarbageVersions(snap, desc, threshold)
	var records []keys.Key
	if err == nil {
		records, err = enumerateGarbageTxnRecords(snap, desc, threshold, liveIntentTxns)
	}
	if cerr := snap.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		return err
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
		log.Debugf("%s: gc reclaimed %d versions, %d txn records (threshold %s)",
			r.rangeID, len(chunkV), len(chunkR), threshold)
	}
	return nil
}

// enumerateGarbageVersions scans the range's MVCC span in one consistent
// snapshot and returns the versions that are garbage at threshold, plus the
// set of transactions that still hold an intent somewhere in the range.
func enumerateGarbageVersions(snap *storage.Snapshot, desc kvpb.RangeDescriptor, threshold hlc.Timestamp) ([]kvpb.GCVersion, map[uuid.UUID]struct{}, error) {
	lower := storage.EncodeMVCCKey(desc.StartKey, hlc.Timestamp{})
	upper := storage.EncodeMVCCKey(desc.EndKey, hlc.Timestamp{})
	it := snap.NewIter(lower, upper)
	defer func() { _ = it.Close() }()

	var out []kvpb.GCVersion
	liveIntentTxns := make(map[uuid.UUID]struct{})
	ok := it.SeekGE(lower)
	for ok {
		userKey, vts, err := storage.DecodeMVCCKey(it.Key())
		if err != nil {
			return nil, nil, err
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
				return nil, nil, err
			}
			if !cur.Equal(k) {
				break // next user key; outer loop re-examines this position
			}
			if cvts.LessEq(threshold) {
				if survivorSeen {
					out = append(out, kvpb.GCVersion{Key: cur, TS: cvts})
				} else {
					survivorSeen = true
					tomb, err := storage.IsTombstoneValue(it.Value())
					if err != nil {
						return nil, nil, err
					}
					if tomb {
						out = append(out, kvpb.GCVersion{Key: cur, TS: cvts})
					}
				}
			}
			ok = it.Next()
		}
	}
	return out, liveIntentTxns, nil
}

// intentTxnID extracts the owning transaction's ID from raw intent metadata.
func intentTxnID(raw []byte) (uuid.UUID, error) {
	var meta enginepb.MVCCMetadata
	if err := json.Unmarshal(raw, &meta); err != nil {
		return uuid.Nil, err
	}
	return meta.Txn.ID, nil
}

// enumerateGarbageTxnRecords scans the range's transaction records and
// returns the storage keys of finalized, TTL-old records — except those of
// transactions that still hold an intent in this range (resolution of those
// intents may yet consult the record via a push).
func enumerateGarbageTxnRecords(snap *storage.Snapshot, desc kvpb.RangeDescriptor, threshold hlc.Timestamp, liveIntentTxns map[uuid.UUID]struct{}) ([]keys.Key, error) {
	lo, hi := keys.RangeLocalAddressedSpan(desc.StartKey, desc.EndKey)
	it := snap.NewIter(lo, hi)
	defer func() { _ = it.Close() }()

	var out []keys.Key
	for ok := it.SeekGE(lo); ok; ok = it.Next() {
		var txn kvpb.Transaction
		if err := json.Unmarshal(it.Value(), &txn); err != nil {
			// Not a transaction record; leave unknown range-local keys alone.
			continue
		}
		if txn.Status == enginepb.PENDING {
			continue // live (or expired-but-unpushed): not ours to reclaim
		}
		if _, held := liveIntentTxns[txn.ID]; held {
			continue
		}
		if txn.WriteTimestamp.LessEq(threshold) && txn.LastHeartbeat.LessEq(threshold) {
			out = append(out, keys.Key(it.Key()).Clone())
		}
	}
	return out, nil
}
