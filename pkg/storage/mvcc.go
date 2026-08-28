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

func mvccWrite(b *Batch, key keys.Key, ts hlc.Timestamp, value []byte, tombstone bool, txn *enginepb.TxnMeta) error {
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
		if txn == nil || meta.Txn.ID != txn.ID {
			return &WriteIntentError{Intents: []Intent{{Key: key.Clone(), Txn: meta.Txn}}}
		}
		// Rewrite our own intent (same epoch: later statement overwrote the
		// key; older epoch: retry rewriting its footprint). Drop the old
		// provisional version, then fall through to write the new one.
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
		meta := enginepb.MVCCMetadata{Txn: *txn, Timestamp: ts}
		if err := b.Put(metaKey, encodeMeta(meta)); err != nil {
			return err
		}
	}
	return b.Put(EncodeMVCCKey(key, ts), encodeMVCCValue(value, tombstone))
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
		userKey, vts, err := DecodeMVCCKey(it.Key())
		if err != nil {
			return ScanResult{}, err
		}
		cur := keys.Key(userKey).Clone()

		var readAt, skipAt hlc.Timestamp
		if vts.IsEmpty() {
			// Intent metadata for cur.
			meta, err := decodeMeta(it.Value())
			if err != nil {
				return ScanResult{}, err
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
			default:
				intents = append(intents, Intent{Key: cur, Txn: meta.Txn})
				if len(intents) >= maxIntentsPerError {
					return ScanResult{}, &WriteIntentError{Intents: intents}
				}
				ok = it.SeekGE(EncodeMVCCKey(cur.Next(), hlc.Timestamp{}))
				continue
			}
			ok = it.Next()
			if !ok {
				break
			}
		}

		// Positioned at the newest version of cur (or beyond cur entirely if
		// it has no versions).
		var value []byte
		var found bool
		if !readAt.IsEmpty() {
			// Own intent: read the provisional version.
			ok = it.SeekGE(EncodeMVCCKey(cur, readAt))
			if ok {
				k, pvts, err := DecodeMVCCKey(it.Key())
				if err != nil {
					return ScanResult{}, err
				}
				if bytes.Equal(k, cur) && pvts.Equal(readAt) {
					data, tombstone, err := decodeMVCCValue(it.Value())
					if err != nil {
						return ScanResult{}, err
					}
					if !tombstone {
						value, found = append([]byte(nil), data...), true
					}
				}
			}
		} else {
			// Uncertainty check, then newest version <= ts.
			if len(intents) == 0 && !opts.UncertaintyLimit.IsEmpty() && ts.Less(opts.UncertaintyLimit) {
				uok := it.Valid()
				k := it.Key()
				// Current position is the newest version; walk down to the
				// first version <= limit, skipping own old-epoch writes.
				for uok {
					ku, uvts, err := DecodeMVCCKey(k)
					if err != nil {
						return ScanResult{}, err
					}
					if !bytes.Equal(ku, cur) {
						uok = false
						break
					}
					if uvts.LessEq(opts.UncertaintyLimit) && !(!skipAt.IsEmpty() && uvts.Equal(skipAt)) {
						if ts.Less(uvts) {
							return ScanResult{}, &UncertaintyError{ReadTimestamp: ts, ExistingTimestamp: uvts}
						}
						break
					}
					if !it.Next() {
						uok = false
						break
					}
					k = it.Key()
				}
			}
			ok = it.SeekGE(EncodeMVCCKey(cur, ts))
			for ok {
				k, cvts, err := DecodeMVCCKey(it.Key())
				if err != nil {
					return ScanResult{}, err
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
					return ScanResult{}, err
				}
				if !tombstone {
					value, found = append([]byte(nil), data...), true
				}
				break
			}
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
