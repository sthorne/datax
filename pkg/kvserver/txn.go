package kvserver

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/kvpb"
	"github.com/sthorne/datax/pkg/storage"
	"github.com/sthorne/datax/pkg/storage/enginepb"
	"github.com/sthorne/datax/pkg/util/hlc"
)

// TxnExpiration is how stale a transaction record's heartbeat may be before
// any pusher may abort it. The coordinator heartbeats every ~1s.
const TxnExpiration = 5 * time.Second

// Transaction records are replicated, non-MVCC keys in the range-local
// addressed keyspace (see pkg/keys): created with the transaction's first
// write, flipped exactly once to COMMITTED or ABORTED, and reclaimed by GC
// once finalized and TTL-old (see gc.go).

func txnRecordKey(txn *enginepb.TxnMeta) keys.Key {
	return keys.TransactionKey(txn.Key, txn.ID)
}

func loadTxnRecord(r storage.Reader, key keys.Key) (*kvpb.Transaction, error) {
	raw, err := r.Get(key)
	if err != nil || raw == nil {
		return nil, err
	}
	var txn kvpb.Transaction
	if err := json.Unmarshal(raw, &txn); err != nil {
		return nil, fmt.Errorf("corrupt transaction record at %s: %w", key, err)
	}
	return &txn, nil
}

func putTxnRecord(w storage.Writer, key keys.Key, txn *kvpb.Transaction) error {
	raw, err := json.Marshal(txn)
	if err != nil {
		return err
	}
	return w.Put(key, raw)
}

// createTxnRecord runs when a batch carries the CreateTxnRecord flag: the
// transaction's first write, on its anchor range, creates the record
// atomically with the write itself.
func (r *Replica) createTxnRecord(b *storage.Batch, txn *kvpb.Transaction) *kvpb.Error {
	if txn == nil {
		return kvpb.NewErrorf("CreateTxnRecord without a transaction")
	}
	// Resurrection guard: once records below the GC threshold are
	// reclaimed, a zombie coordinator's first write must not recreate its
	// record as PENDING (it may have been aborted before the GC). Any
	// transaction born at or below the threshold is TTL-old — vastly beyond
	// TxnExpiration — so no live transaction trips this. Deterministic:
	// the threshold is replicated state.
	r.mu.Lock()
	thr := r.mu.gcThreshold
	r.mu.Unlock()
	if !thr.IsEmpty() && txn.MinTimestamp.LessEq(thr) {
		e := kvpb.NewErrorf("transaction %s predates the GC threshold %s", txn.ID, thr)
		e.TxnAborted = &kvpb.TxnAbortedError{}
		return e
	}
	key := txnRecordKey(&txn.TxnMeta)
	existing, err := loadTxnRecord(b, key)
	if err != nil {
		return kvpb.NewError(err)
	}
	if existing != nil {
		switch existing.Status {
		case enginepb.ABORTED:
			// A pusher poisoned the record before our first write landed.
			e := kvpb.NewErrorf("transaction %s aborted before its record was created", txn.ID)
			e.TxnAborted = &kvpb.TxnAbortedError{}
			return e
		case enginepb.COMMITTED:
			return kvpb.NewErrorf("transaction %s: record already committed at record creation", txn.ID)
		}
		// PENDING from an earlier epoch: fall through and overwrite.
	}
	rec := txn.Clone()
	rec.Status = enginepb.PENDING
	return kvpb.NewError(putTxnRecord(b, key, rec))
}

// evalEndTxn is the atomic commit/abort: one compare-and-set of the record,
// replicated through Raft. The moment a COMMITTED record applies, every
// intent the transaction wrote anywhere is logically committed.
func (r *Replica) evalEndTxn(b *storage.Batch, txn *kvpb.Transaction, req *kvpb.EndTxnRequest) (*kvpb.EndTxnResponse, *kvpb.Error) {
	if txn == nil {
		return nil, kvpb.NewErrorf("EndTxn without a transaction")
	}
	key := txnRecordKey(&txn.TxnMeta)
	rec, err := loadTxnRecord(b, key)
	if err != nil {
		return nil, kvpb.NewError(err)
	}
	if rec != nil {
		switch rec.Status {
		case enginepb.ABORTED:
			e := kvpb.NewErrorf("transaction %s aborted by a conflicting transaction", txn.ID)
			e.TxnAborted = &kvpb.TxnAbortedError{}
			return nil, e
		case enginepb.COMMITTED:
			if req.Commit && rec.Epoch == txn.Epoch {
				// Idempotent commit retry (or the explicit finalize of a
				// parallel commit that status recovery already finalized).
				return &kvpb.EndTxnResponse{CommitTimestamp: rec.WriteTimestamp}, nil
			}
			return nil, kvpb.NewErrorf("transaction %s already committed", txn.ID)
		}
		// PENDING and STAGING both accept the flip: a STAGING record is
		// re-staged (the coordinator retried at a higher timestamp),
		// finalized to COMMITTED (in-flight writes proven applied), or
		// aborted (the parallel commit failed).
		if rec.Epoch > txn.Epoch {
			return nil, kvpb.NewErrorf("transaction %s: record at newer epoch %d than request %d", txn.ID, rec.Epoch, txn.Epoch)
		}
	}
	if req.Commit {
		// The heart of retry-only serializability: a transaction may only
		// commit if nothing pushed its write timestamp beyond its read
		// timestamp. (See docs/transactions.md.)
		if !txn.ReadTimestamp.Equal(txn.WriteTimestamp) {
			e := kvpb.NewErrorf("transaction %s cannot commit: write timestamp %s diverged from read timestamp %s",
				txn.ID, txn.WriteTimestamp, txn.ReadTimestamp)
			e.TxnRetry = &kvpb.TxnRetryError{RetryTimestamp: txn.WriteTimestamp}
			return nil, e
		}
	}
	final := txn.Clone()
	if req.Commit {
		if len(req.InFlight) > 0 {
			// Parallel commit: the record STAGES with the writes pipelined
			// alongside it. The transaction is implicitly committed once
			// all of them are present at or below the staged timestamp;
			// recovery (or the coordinator's explicit finalize) settles it.
			final.Status = enginepb.STAGING
			final.InFlightKeys = req.InFlight
		} else {
			final.Status = enginepb.COMMITTED
			final.InFlightKeys = nil
		}
		// The write set travels on the record so GC can resolve any intents
		// a crashed coordinator left behind before reclaiming the record.
		final.IntentKeys = req.IntentKeys
	} else {
		final.Status = enginepb.ABORTED
		final.InFlightKeys = nil
	}
	if err := putTxnRecord(b, key, final); err != nil {
		return nil, kvpb.NewError(err)
	}
	return &kvpb.EndTxnResponse{CommitTimestamp: final.WriteTimestamp}, nil
}

// evalHeartbeatTxn refreshes the record's liveness, telling the coordinator
// if it has been aborted.
func (r *Replica) evalHeartbeatTxn(b *storage.Batch, txn *kvpb.Transaction, req *kvpb.HeartbeatTxnRequest) (*kvpb.HeartbeatTxnResponse, *kvpb.Error) {
	if txn == nil {
		return nil, kvpb.NewErrorf("HeartbeatTxn without a transaction")
	}
	key := txnRecordKey(&txn.TxnMeta)
	rec, err := loadTxnRecord(b, key)
	if err != nil {
		return nil, kvpb.NewError(err)
	}
	if rec == nil {
		e := kvpb.NewErrorf("transaction %s: no record to heartbeat", txn.ID)
		e.TxnNotFound = &kvpb.TxnNotFoundError{}
		return nil, e
	}
	if rec.Status == enginepb.PENDING {
		rec.LastHeartbeat = rec.LastHeartbeat.Forward(req.Now)
		// Publish the coordinator's current wait edge (uuid.Nil clears it)
		// for the deadlock detector's chain walk.
		rec.WaitingFor = req.WaitingFor
		rec.WaitingForKey = req.WaitingForKey
		if err := putTxnRecord(b, key, rec); err != nil {
			return nil, kvpb.NewError(err)
		}
	}
	return &kvpb.HeartbeatTxnResponse{Status: rec.Status}, nil
}

// evalPushTxn resolves a conflict between a pusher (who found an intent)
// and the pushee (who wrote it). Outcomes:
//   - pushee finalized → report it, pusher resolves the intent;
//   - pushee expired (crashed coordinator) → abort it;
//   - pushee alive, pusher outranks it and wants an abort → abort it;
//   - otherwise → PENDING: the pusher waits and retries.
func (r *Replica) evalPushTxn(b *storage.Batch, req *kvpb.PushTxnRequest) (*kvpb.PushTxnResponse, *kvpb.Error) {
	key := keys.TransactionKey(req.PusheeTxn.Key, req.PusheeTxn.ID)
	rec, err := loadTxnRecord(b, key)
	if err != nil {
		return nil, kvpb.NewError(err)
	}
	if rec == nil {
		// No record. Either the pushee never managed to create one, or its
		// record creation is still in flight — a parallel or one-phase
		// commit races the write batch against the record-creating EndTxn,
		// so an intent CAN be observable before the record exists. Judge
		// expiry from the transaction's birth; the coordinator's poison
		// guard (kvclient's txnPoisonGuardAge) keeps transactions old
		// enough to be judged expired here from entering that race.
		if expired(req.Now, req.PusheeTxn.MinTimestamp) {
			poisoned := &kvpb.Transaction{TxnMeta: req.PusheeTxn, Status: enginepb.ABORTED}
			if err := putTxnRecord(b, key, poisoned); err != nil {
				return nil, kvpb.NewError(err)
			}
			return &kvpb.PushTxnResponse{Status: enginepb.ABORTED}, nil
		}
		return &kvpb.PushTxnResponse{Status: enginepb.PENDING}, nil
	}

	switch rec.Status {
	case enginepb.COMMITTED:
		return &kvpb.PushTxnResponse{Status: enginepb.COMMITTED, CommitTS: rec.WriteTimestamp}, nil
	case enginepb.ABORTED:
		return &kvpb.PushTxnResponse{Status: enginepb.ABORTED}, nil
	case enginepb.STAGING:
		// A parallel commit is in flight: past the point where pushes,
		// priority aborts, or expiry may decide its fate — the pusher must
		// run status recovery against the staged write set (even for an
		// expired record: an implicitly committed transaction must never
		// be aborted).
		return &kvpb.PushTxnResponse{
			Status:       enginepb.STAGING,
			CommitTS:     rec.WriteTimestamp,
			InFlightKeys: rec.InFlightKeys,
		}, nil
	}

	if expired(req.Now, rec.LastHeartbeat) {
		rec.Status = enginepb.ABORTED
		if err := putTxnRecord(b, key, rec); err != nil {
			return nil, kvpb.NewError(err)
		}
		return &kvpb.PushTxnResponse{Status: enginepb.ABORTED}, nil
	}

	// A detected deadlock cycle's chosen victim is aborted regardless of
	// priority — the detector already picked deterministically.
	if req.ForceAbort {
		rec.Status = enginepb.ABORTED
		if err := putTxnRecord(b, key, rec); err != nil {
			return nil, kvpb.NewError(err)
		}
		return &kvpb.PushTxnResponse{Status: enginepb.ABORTED}, nil
	}
	var pusherPriority int32 = 1 << 30 // non-transactional pushers win
	if req.PusherTxn != nil {
		pusherPriority = req.PusherTxn.Priority
	}
	if req.PushAbort && pusherPriority > rec.Priority {
		rec.Status = enginepb.ABORTED
		if err := putTxnRecord(b, key, rec); err != nil {
			return nil, kvpb.NewError(err)
		}
		return &kvpb.PushTxnResponse{Status: enginepb.ABORTED}, nil
	}
	return &kvpb.PushTxnResponse{Status: enginepb.PENDING}, nil
}

// evalRecoverTxn finalizes a STAGING record after status recovery: the
// recoverer verified every staged in-flight write (present at or below
// the staged timestamp -> commit; any missing AND prevented from ever
// landing -> abort). Idempotent: a record no longer STAGING is reported
// as is; a missing record is reported ABORTED (already recovered and
// GC'd, or never staged — either way not committed).
func (r *Replica) evalRecoverTxn(b *storage.Batch, req *kvpb.RecoverTxnRequest) (*kvpb.RecoverTxnResponse, *kvpb.Error) {
	key := keys.TransactionKey(req.Key, req.TxnID)
	rec, err := loadTxnRecord(b, key)
	if err != nil {
		return nil, kvpb.NewError(err)
	}
	if rec == nil {
		return &kvpb.RecoverTxnResponse{Status: enginepb.ABORTED}, nil
	}
	if rec.Status != enginepb.STAGING {
		return &kvpb.RecoverTxnResponse{Status: rec.Status}, nil
	}
	if req.Commit {
		rec.Status = enginepb.COMMITTED
	} else {
		rec.Status = enginepb.ABORTED
	}
	rec.InFlightKeys = nil
	if err := putTxnRecord(b, key, rec); err != nil {
		return nil, kvpb.NewError(err)
	}
	return &kvpb.RecoverTxnResponse{Status: rec.Status}, nil
}

func expired(now, last hlc.Timestamp) bool {
	return now.WallTime-last.WallTime > int64(TxnExpiration)
}
