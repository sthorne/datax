package storage

import (
	"bytes"
	"encoding/json"
	"fmt"

	"github.com/google/uuid"
	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/storage/enginepb"
	"github.com/sthorne/datax/pkg/util/hlc"
)

// MVCCGetOptions configure MVCCGet and MVCCScan.
type MVCCGetOptions struct {
	// Txn, if set, makes the read transactional: the transaction sees its
	// own intents (read-your-writes) and ignores intents from its own older
	// epochs.
	Txn *enginepb.TxnMeta
	// UncertaintyLimit, when non-empty and greater than the read timestamp,
	// makes reads fail with UncertaintyError if a version exists in
	// (readTS, UncertaintyLimit] — the caller cannot know whether such a
	// write causally preceded it, given bounded clock skew.
	UncertaintyLimit hlc.Timestamp
	// Inconsistent reads never block on (or report) intents: they read the
	// newest committed version beneath any intent. Used for meta/gossip
	// scans where staleness is fine but stalls are not.
	Inconsistent bool
}

// intentAboveRead reports whether a foreign intent at intentTS is strictly
// above everything a read at ts can observe — above the read timestamp AND
// above the uncertainty limit. Such an intent cannot change the read's
// answer however it resolves: resolution only moves a write's timestamp
// FORWARD (a commit timestamp is never below the intent's provisional
// timestamp), so the eventual committed version stays invisible too, and
// the read may simply look beneath the intent. An intent inside the
// uncertainty window (ts, limit] is NOT skippable — like a committed
// version there, it may causally precede the read.
func intentAboveRead(intentTS, ts, uncertaintyLimit hlc.Timestamp) bool {
	limit := ts
	if limit.Less(uncertaintyLimit) {
		limit = uncertaintyLimit
	}
	return limit.Less(intentTS)
}

// mvccKeyBounds returns engine-key bounds covering exactly the metadata and
// versions of user key k.
func mvccKeyBounds(k keys.Key) (lower, upper []byte) {
	return EncodeMVCCKey(k, hlc.Timestamp{}), EncodeMVCCKey(k.Next(), hlc.Timestamp{})
}

func decodeMeta(raw []byte) (enginepb.MVCCMetadata, error) {
	var meta enginepb.MVCCMetadata
	if err := json.Unmarshal(raw, &meta); err != nil {
		return meta, fmt.Errorf("malformed MVCC metadata: %w", err)
	}
	return meta, nil
}

func encodeMeta(meta enginepb.MVCCMetadata) []byte {
	b, err := json.Marshal(meta)
	if err != nil {
		panic(err) // MVCCMetadata is always marshalable
	}
	return b
}

// MVCCGet returns the value of key visible at timestamp ts, or nil if the
// key has no visible value (never written, or deleted). See MVCCGetOptions
// for transactional semantics.
func MVCCGet(r Reader, key keys.Key, ts hlc.Timestamp, opts MVCCGetOptions) ([]byte, error) {
	metaKey, upper := mvccKeyBounds(key)
	it := r.NewIter(metaKey, upper)
	defer func() { _ = it.Close() }()

	var readAt hlc.Timestamp // exact version to read (own intent); empty = normal read
	var skipAt hlc.Timestamp // provisional version to skip (own older epoch); empty = none

	if it.SeekGE(metaKey) && bytes.Equal(it.Key(), metaKey) {
		meta, err := decodeMeta(it.Value())
		if err != nil {
			return nil, err
		}
		switch {
		case opts.Txn != nil && meta.Txn.ID == opts.Txn.ID:
			if meta.Txn.Epoch == opts.Txn.Epoch {
				readAt = meta.Timestamp
			} else {
				skipAt = meta.Timestamp
			}
		case opts.Inconsistent:
			skipAt = meta.Timestamp // read beneath the intent
		case intentAboveRead(meta.Timestamp, ts, opts.UncertaintyLimit):
			// A foreign intent strictly above everything this read can
			// observe cannot affect its answer no matter how it resolves
			// (resolution only moves timestamps forward): read beneath it
			// instead of pushing its transaction.
			skipAt = meta.Timestamp
		default:
			return nil, &WriteIntentError{Intents: []Intent{{Key: key.Clone(), Txn: meta.Txn}}}
		}
	}

	if !readAt.IsEmpty() {
		// Read-your-writes: return the provisional value, regardless of ts.
		if !it.SeekGE(EncodeMVCCKey(key, readAt)) {
			return nil, fmt.Errorf("intent on %s has no provisional value at %s", key, readAt)
		}
		_, vts, err := DecodeMVCCKey(it.Key())
		if err != nil {
			return nil, err
		}
		if !vts.Equal(readAt) {
			return nil, fmt.Errorf("intent on %s has no provisional value at %s (found %s)", key, readAt, vts)
		}
		data, tombstone, err := decodeMVCCValue(it.Value())
		if err != nil || tombstone {
			return nil, err
		}
		return append([]byte(nil), data...), nil
	}

	// Uncertainty: any version in (ts, limit] — from any writer — means we
	// cannot order ourselves relative to it. Seek to the first version at or
	// below the limit and check whether it is above our read timestamp.
	if !opts.UncertaintyLimit.IsEmpty() && ts.Less(opts.UncertaintyLimit) {
		if it.SeekGE(EncodeMVCCKey(key, opts.UncertaintyLimit)) {
			for it.Valid() {
				_, vts, err := DecodeMVCCKey(it.Key())
				if err != nil {
					return nil, err
				}
				if !skipAt.IsEmpty() && vts.Equal(skipAt) {
					it.Next() // own old-epoch provisional value: not a real write
					continue
				}
				if ts.Less(vts) {
					return nil, &UncertaintyError{ReadTimestamp: ts, ExistingTimestamp: vts}
				}
				break
			}
		}
	}

	// Normal read: newest version at or below ts.
	if !it.SeekGE(EncodeMVCCKey(key, ts)) {
		return nil, nil
	}
	for it.Valid() {
		_, vts, err := DecodeMVCCKey(it.Key())
		if err != nil {
			return nil, err
		}
		if !skipAt.IsEmpty() && vts.Equal(skipAt) {
			it.Next()
			continue
		}
		data, tombstone, err := decodeMVCCValue(it.Value())
		if err != nil || tombstone {
			return nil, err
		}
		return append([]byte(nil), data...), nil
	}
	return nil, nil
}

// MVCCPut writes value to key at ts. With a transaction it lays down a write
// intent at txn.WriteTimestamp (replacing any previous intent of the same
// transaction); without one it writes a committed version directly, bumping
// the timestamp above any existing newer version (blind non-transactional
// writes cannot conflict, they just serialize after).
func MVCCPut(b *Batch, key keys.Key, ts hlc.Timestamp, value []byte, txn *enginepb.TxnMeta) error {
	return mvccWrite(b, key, ts, value, false, txn)
}

// MVCCDelete writes a deletion tombstone. Same rules as MVCCPut.
func MVCCDelete(b *Batch, key keys.Key, ts hlc.Timestamp, txn *enginepb.TxnMeta) error {
	return mvccWrite(b, key, ts, nil, true, txn)
}

// MVCCPutCommitted writes a bare committed version at exactly ts, with the
// TRANSACTIONAL conflict rules — any intent is a WriteIntentError, a
// version at or above ts is a WriteTooOldError — but no intent metadata.
// The one-phase-commit primitive: never the nil-txn timestamp ratchet
// (which would commit at a timestamp the client never agreed to).
func MVCCPutCommitted(b *Batch, key keys.Key, ts hlc.Timestamp, value []byte) error {
	return mvccWriteCommitted(b, key, ts, value, false)
}

// MVCCDeleteCommitted is MVCCPutCommitted's tombstone counterpart.
func MVCCDeleteCommitted(b *Batch, key keys.Key, ts hlc.Timestamp) error {
	return mvccWriteCommitted(b, key, ts, nil, true)
}

func mvccWriteCommitted(b *Batch, key keys.Key, ts hlc.Timestamp, value []byte, tombstone bool) error {
	metaKey, upper := mvccKeyBounds(key)
	rawMeta, err := b.Get(metaKey)
	if err != nil {
		return err
	}
	if rawMeta != nil {
		meta, err := decodeMeta(rawMeta)
		if err != nil {
			return err
		}
		// Any intent conflicts — a one-phase transaction has no prior
		// writes of its own, so ownership is irrelevant here.
		return &WriteIntentError{Intents: []Intent{{Key: key.Clone(), Txn: meta.Txn}}}
	}
	it := b.NewIter(metaKey, upper)
	firstVersion := append(append([]byte(nil), metaKey...), 0x00)
	if it.SeekGE(firstVersion) {
		_, vts, err := DecodeMVCCKey(it.Key())
		if cerr := it.Close(); err == nil {
			err = cerr
		}
		if err != nil {
			return err
		}
		if ts.LessEq(vts) {
			return &WriteTooOldError{Timestamp: ts, ActualTimestamp: vts.Next()}
		}
	} else if err := it.Close(); err != nil {
		return err
	}
	return b.Put(EncodeMVCCKey(key, ts), encodeMVCCValue(value, tombstone))
}

func mvccWrite(b *Batch, key keys.Key, ts hlc.Timestamp, value []byte, tombstone bool, txn *enginepb.TxnMeta) error {
	metaKey, upper := mvccKeyBounds(key)

	rawMeta, err := b.Get(metaKey)
	if err != nil {
		return err
	}
	var history []enginepb.IntentValue
	if rawMeta != nil {
		meta, err := decodeMeta(rawMeta)
		if err != nil {
			return err
		}
		if txn == nil || meta.Txn.ID != txn.ID {
			return &WriteIntentError{Intents: []Intent{{Key: key.Clone(), Txn: meta.Txn}}}
		}
		// Rewrite our own intent (same epoch: later statement overwrote the
		// key; older epoch: retry rewriting its footprint). Same-epoch
		// rewrites preserve the superseded provisional value in the intent
		// history so a savepoint rollback can restore it; a new epoch
		// starts fresh.
		if meta.Txn.Epoch == txn.Epoch {
			raw, err := b.Get(EncodeMVCCKey(key, meta.Timestamp))
			if err != nil {
				return err
			}
			if raw != nil {
				val, tomb, err := decodeMVCCValue(raw)
				if err != nil {
					return err
				}
				history = append(append([]enginepb.IntentValue(nil), meta.History...),
					enginepb.IntentValue{Sequence: meta.Txn.Sequence, Value: val, Tombstone: tomb})
			}
		}
		if err := b.Delete(EncodeMVCCKey(key, meta.Timestamp)); err != nil {
			return err
		}
	} else {
		// No intent. Find the newest committed version and refuse to write
		// beneath it. (The first engine key after the metadata key is the
		// newest version, since versions sort newest-first.)
		it := b.NewIter(metaKey, upper)
		firstVersion := append(append([]byte(nil), metaKey...), 0x00)
		if it.SeekGE(firstVersion) {
			_, vts, err := DecodeMVCCKey(it.Key())
			if cerr := it.Close(); err == nil {
				err = cerr
			}
			if err != nil {
				return err
			}
			if ts.LessEq(vts) {
				if txn != nil {
					return &WriteTooOldError{Timestamp: ts, ActualTimestamp: vts.Next()}
				}
				ts = vts.Next()
			}
		} else if err := it.Close(); err != nil {
			return err
		}
	}

	if txn != nil {
		ts = txn.WriteTimestamp
		meta := enginepb.MVCCMetadata{Txn: *txn, Timestamp: ts, History: history}
		if err := b.Put(metaKey, encodeMeta(meta)); err != nil {
			return err
		}
	}
	return b.Put(EncodeMVCCKey(key, ts), encodeMVCCValue(value, tombstone))
}

// MVCCRollbackIntent rolls the transaction's intent on key back to its
// newest state at or below seq — the savepoint-rollback primitive. The
// intent history holds the superseded provisional values; the newest
// entry at or below seq is physically restored, or the intent is removed
// entirely when the key was first written after the savepoint. Idempotent,
// and a no-op on another transaction's intent or one already at or below
// seq — so a rollback may be sent for every key the transaction ever
// wrote.
func MVCCRollbackIntent(b *Batch, key keys.Key, txnID uuid.UUID, seq int32) error {
	metaKey, _ := mvccKeyBounds(key)
	rawMeta, err := b.Get(metaKey)
	if err != nil {
		return err
	}
	if rawMeta == nil {
		return nil
	}
	meta, err := decodeMeta(rawMeta)
	if err != nil {
		return err
	}
	if meta.Txn.ID != txnID || meta.Txn.Sequence <= seq {
		return nil
	}
	if err := b.Delete(EncodeMVCCKey(key, meta.Timestamp)); err != nil {
		return err
	}
	// History is append-ordered, so sequences ascend: the newest entry at
	// or below seq is the state to restore.
	idx := -1
	for i := len(meta.History) - 1; i >= 0; i-- {
		if meta.History[i].Sequence <= seq {
			idx = i
			break
		}
	}
	if idx < 0 {
		// First write to this key came after the savepoint: no intent left.
		return b.Delete(metaKey)
	}
	ent := meta.History[idx]
	meta.History = meta.History[:idx]
	meta.Txn.Sequence = ent.Sequence
	if err := b.Put(metaKey, encodeMeta(meta)); err != nil {
		return err
	}
	return b.Put(EncodeMVCCKey(key, meta.Timestamp), encodeMVCCValue(ent.Value, ent.Tombstone))
}

// MVCCLock lays a LOCKING intent on key for txn, pinning the state the
// transaction observed at readTS: value is the visible value at readTS
// (nil = absent or deleted, pinned as a tombstone). The intent serializes
// all other writers behind the transaction exactly like a real write, and
// commits as a version carrying the same bytes — invisible to readers'
// results. Fails with WriteTooOldError if any committed version exists
// ABOVE readTS (the caller's snapshot is stale; a version exactly at
// readTS was observed and is lockable), and with WriteIntentError on a
// foreign intent.
func MVCCLock(b *Batch, key keys.Key, readTS hlc.Timestamp, value []byte, txn *enginepb.TxnMeta) error {
	if txn == nil {
		return fmt.Errorf("MVCCLock requires a transaction")
	}
	// readTS.Next() makes mvccWrite's conflict check reject versions
	// strictly above readTS while admitting one exactly at it.
	return mvccWrite(b, key, readTS.Next(), value, value == nil, txn)
}

// KeyValue is one row of a scan result.
type KeyValue struct {
	Key   keys.Key
	Value []byte
}

// ScanResult is the outcome of an MVCCScan.
type ScanResult struct {
	KVs []KeyValue
	// Resume, if non-nil, is where a continuation scan should start (the
	// scan stopped early because it reached max rows).
	Resume keys.Key
}

// maxIntentsPerError bounds how many conflicting intents a single scan
// reports before giving up collecting more.
const maxIntentsPerError = 16

// MVCCScan returns up to max (0 = unlimited) visible key/value pairs in
// [start, end) at timestamp ts. If it encounters intents from other
// transactions it aggregates them (up to a limit) and returns a
// WriteIntentError — the caller pushes those transactions and retries.
func MVCCScan(r Reader, start, end keys.Key, ts hlc.Timestamp, max int64, opts MVCCGetOptions) (ScanResult, error) {
	lower := EncodeMVCCKey(start, hlc.Timestamp{})
	upper := EncodeMVCCKey(end, hlc.Timestamp{})
	it := r.NewIter(lower, upper)
	defer func() { _ = it.Close() }()

	var res ScanResult
	var intents []Intent

	ok := it.SeekGE(lower)
	for ok {
		userKey, _, err := DecodeMVCCKey(it.Key())
		if err != nil {
			return ScanResult{}, err
		}
		cur := keys.Key(userKey).Clone()
		value, found, err := mvccScanKey(it, cur, ts, opts, &intents)
		if err != nil {
			return ScanResult{}, err
		}
		if found && len(intents) == 0 {
			res.KVs = append(res.KVs, KeyValue{Key: cur, Value: value})
			if max > 0 && int64(len(res.KVs)) == max {
				res.Resume = cur.Next()
				return res, nil
			}
		}
		ok = it.SeekGE(EncodeMVCCKey(cur.Next(), hlc.Timestamp{}))
	}

	if len(intents) > 0 {
		return ScanResult{}, &WriteIntentError{Intents: intents}
	}
	return res, nil
}

// MVCCReverseScan is MVCCScan iterating [start, end) from the end
// BACKWARDS: rows come back largest-key-first, and Resume, when max rows
// stop the scan early, is the exclusive END of the continuation page
// ([start, Resume)). Each user key costs one SeekLT to find it plus the
// shared forward per-key walk to read it — metadata sorts before the
// key's versions, so landing anywhere inside a key and re-seeking to its
// first engine key reuses MVCCScan's visibility logic verbatim.
func MVCCReverseScan(r Reader, start, end keys.Key, ts hlc.Timestamp, max int64, opts MVCCGetOptions) (ScanResult, error) {
	lower := EncodeMVCCKey(start, hlc.Timestamp{})
	upper := EncodeMVCCKey(end, hlc.Timestamp{})
	it := r.NewIter(lower, upper)
	defer func() { _ = it.Close() }()

	var res ScanResult
	var intents []Intent

	ok := it.SeekLT(upper)
	for ok {
		userKey, _, err := DecodeMVCCKey(it.Key())
		if err != nil {
			return ScanResult{}, err
		}
		cur := keys.Key(userKey).Clone()
		curStart := EncodeMVCCKey(cur, hlc.Timestamp{})
		if !it.SeekGE(curStart) {
			break // unreachable: an engine key of cur was just observed
		}
		value, found, err := mvccScanKey(it, cur, ts, opts, &intents)
		if err != nil {
			return ScanResult{}, err
		}
		if found && len(intents) == 0 {
			res.KVs = append(res.KVs, KeyValue{Key: cur, Value: value})
			if max > 0 && int64(len(res.KVs)) == max {
				res.Resume = cur
				return res, nil
			}
		}
		ok = it.SeekLT(curStart)
	}

	if len(intents) > 0 {
		return ScanResult{}, &WriteIntentError{Intents: intents}
	}
	return res, nil
}

// mvccScanKey reads user key cur's visible value during a scan. The
// iterator must be positioned at cur's FIRST engine key (intent metadata
// or newest version); it is left at an unspecified position, so callers
// re-seek to continue. A blocking foreign intent is appended to *intents
// and reported as not-found (the scan stops returning rows and
// ultimately surfaces a WriteIntentError).
func mvccScanKey(it Iterator, cur keys.Key, ts hlc.Timestamp, opts MVCCGetOptions, intents *[]Intent) ([]byte, bool, error) {
	_, vts, err := DecodeMVCCKey(it.Key())
	if err != nil {
		return nil, false, err
	}

	var readAt, skipAt hlc.Timestamp
	if vts.IsEmpty() {
		// Intent metadata for cur.
		meta, err := decodeMeta(it.Value())
		if err != nil {
			return nil, false, err
		}
		switch {
		case opts.Txn != nil && meta.Txn.ID == opts.Txn.ID:
			if meta.Txn.Epoch == opts.Txn.Epoch {
				readAt = meta.Timestamp
			} else {
				skipAt = meta.Timestamp
			}
		case opts.Inconsistent:
			skipAt = meta.Timestamp // read beneath the intent
		case intentAboveRead(meta.Timestamp, ts, opts.UncertaintyLimit):
			// Foreign intent strictly above everything observable: read
			// beneath it (see MVCCGet).
			skipAt = meta.Timestamp
		default:
			*intents = append(*intents, Intent{Key: cur, Txn: meta.Txn})
			if len(*intents) >= maxIntentsPerError {
				return nil, false, &WriteIntentError{Intents: *intents}
			}
			return nil, false, nil
		}
		if !it.Next() {
			return nil, false, nil
		}
	}

	// Positioned at the newest version of cur (or beyond cur entirely if
	// it has no versions).
	var value []byte
	var found bool
	if !readAt.IsEmpty() {
		// Own intent: read the provisional version.
		if it.SeekGE(EncodeMVCCKey(cur, readAt)) {
			k, pvts, err := DecodeMVCCKey(it.Key())
			if err != nil {
				return nil, false, err
			}
			if bytes.Equal(k, cur) && pvts.Equal(readAt) {
				data, tombstone, err := decodeMVCCValue(it.Value())
				if err != nil {
					return nil, false, err
				}
				if !tombstone {
					value, found = append([]byte(nil), data...), true
				}
			}
		}
	} else {
		// Uncertainty check, then newest version <= ts.
		if len(*intents) == 0 && !opts.UncertaintyLimit.IsEmpty() && ts.Less(opts.UncertaintyLimit) {
			uok := it.Valid()
			k := it.Key()
			// Current position is the newest version; walk down to the
			// first version <= limit, skipping own old-epoch writes.
			for uok {
				ku, uvts, err := DecodeMVCCKey(k)
				if err != nil {
					return nil, false, err
				}
				if !bytes.Equal(ku, cur) {
					break
				}
				if uvts.LessEq(opts.UncertaintyLimit) && !(!skipAt.IsEmpty() && uvts.Equal(skipAt)) {
					if ts.Less(uvts) {
						return nil, false, &UncertaintyError{ReadTimestamp: ts, ExistingTimestamp: uvts}
					}
					break
				}
				if !it.Next() {
					break
				}
				k = it.Key()
			}
		}
		ok := it.SeekGE(EncodeMVCCKey(cur, ts))
		for ok {
			k, cvts, err := DecodeMVCCKey(it.Key())
			if err != nil {
				return nil, false, err
			}
			if !bytes.Equal(k, cur) {
				break
			}
			if !skipAt.IsEmpty() && cvts.Equal(skipAt) {
				ok = it.Next()
				continue
			}
			data, tombstone, err := decodeMVCCValue(it.Value())
			if err != nil {
				return nil, false, err
			}
			if !tombstone {
				value, found = append([]byte(nil), data...), true
			}
			break
		}
	}
	return value, found, nil
}

// MVCCCheckForWrites reports whether [start, end) contains any write that a
// transaction refreshing its read timestamp from fromTS to toTS would have
// missed: a committed version with timestamp in (fromTS, toTS], or any
// intent from another transaction (which could commit inside the window).
// The transaction's own intents and provisional versions are ignored.
// Returns nil when the refresh is safe.
func MVCCCheckForWrites(r Reader, start, end keys.Key, fromTS, toTS hlc.Timestamp, ownTxn uuid.UUID) error {
	lower := EncodeMVCCKey(start, hlc.Timestamp{})
	upper := EncodeMVCCKey(end, hlc.Timestamp{})
	it := r.NewIter(lower, upper)
	defer func() { _ = it.Close() }()

	ok := it.SeekGE(lower)
	for ok {
		userKey, vts, err := DecodeMVCCKey(it.Key())
		if err != nil {
			return err
		}
		cur := keys.Key(userKey).Clone()
		var skipAt hlc.Timestamp
		if vts.IsEmpty() {
			meta, err := decodeMeta(it.Value())
			if err != nil {
				return err
			}
			if meta.Txn.ID != ownTxn {
				return &WriteIntentError{Intents: []Intent{{Key: cur, Txn: meta.Txn}}}
			}
			skipAt = meta.Timestamp
			ok = it.Next()
			if !ok {
				break
			}
		}
		// Newest version at or below toTS; skipping our own provisional one.
		ok = it.SeekGE(EncodeMVCCKey(cur, toTS))
		for ok {
			k, cvts, err := DecodeMVCCKey(it.Key())
			if err != nil {
				return err
			}
			if !bytes.Equal(k, cur) {
				break
			}
			if !skipAt.IsEmpty() && cvts.Equal(skipAt) {
				ok = it.Next()
				continue
			}
			if fromTS.Less(cvts) {
				return fmt.Errorf("refresh of [%s, %s) failed: write on %s at %s within (%s, %s]",
					start, end, cur, cvts, fromTS, toTS)
			}
			break
		}
		ok = it.SeekGE(EncodeMVCCKey(cur.Next(), hlc.Timestamp{}))
	}
	return nil
}

// MVCCResolveIntent resolves the intent on key owned by transaction txnID
// according to the transaction's final status. Idempotent: resolving an
// already-resolved (or never-written) intent is a no-op.
//
// Invariant: this is the ONLY way intents disappear, and callers may only
// invoke it with the status recorded in the transaction's record (or ABORTED
// for an expired record they pushed). See docs/transactions.md.
func MVCCResolveIntent(b *Batch, key keys.Key, txnID uuid.UUID, status enginepb.TxnStatus, commitTS hlc.Timestamp) error {
	if status == enginepb.PENDING {
		return fmt.Errorf("cannot resolve intent with PENDING status")
	}
	metaKey, _ := mvccKeyBounds(key)
	rawMeta, err := b.Get(metaKey)
	if err != nil {
		return err
	}
	if rawMeta == nil {
		return nil
	}
	meta, err := decodeMeta(rawMeta)
	if err != nil {
		return err
	}
	if meta.Txn.ID != txnID {
		return nil // someone else's intent now
	}
	if err := b.Delete(metaKey); err != nil {
		return err
	}
	provKey := EncodeMVCCKey(key, meta.Timestamp)
	if status == enginepb.ABORTED {
		return b.Delete(provKey)
	}
	// COMMITTED: the provisional value becomes a committed version at the
	// commit timestamp (rewrite it if the transaction's timestamp moved).
	if commitTS.IsEmpty() || commitTS.Equal(meta.Timestamp) {
		return nil
	}
	raw, err := b.Get(provKey)
	if err != nil {
		return err
	}
	if raw == nil {
		return fmt.Errorf("intent on %s has no provisional value at %s", key, meta.Timestamp)
	}
	if err := b.Delete(provKey); err != nil {
		return err
	}
	return b.Put(EncodeMVCCKey(key, commitTS), raw)
}
