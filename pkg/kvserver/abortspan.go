package kvserver

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/sthorne/datax/pkg/base"
	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/kvpb"
	"github.com/sthorne/datax/pkg/storage"
	"github.com/sthorne/datax/pkg/storage/enginepb"
	"github.com/sthorne/datax/pkg/util/hlc"
	"github.com/sthorne/datax/pkg/version"
)

// The abort span (issue #320, cluster version v22).
//
// A pusher that aborts a transaction resolves the intents it ran into, and
// resolving an intent as aborted removes it. The aborted transaction does
// not know yet — it learns from its next heartbeat or its commit — and in
// the meantime a read of a key it wrote finds its own write gone: an
// INSERT reported a row and the SELECT after it saw none. So a resolution
// that removes an intent of a transaction someone else aborted leaves an
// entry for the transaction in the range's abort span, in the same
// replicated command, and every data request of the transaction on the
// range checks for the entry and fails with a TxnAbortedError (SQLSTATE
// 40001) if it is there.
//
// The check and the removal cannot miss each other. A write is checked at
// apply, in log order, against the same replicated state the removal
// changed. A read is checked after it has evaluated, against the engine
// as it stands then: the removal and the entry land in one engine batch,
// so a read that did not see the intent ran after the batch committed,
// and so does the check that follows it.
//
// Entries are keyed by range and transaction (keys.AbortSpanKey): the
// check is one point lookup whatever the request reads. They are
// replicated state: streamed with the range's snapshots, copied to both
// halves of a split (either half may hold the keys the transaction
// wrote), carried into the left-hand range by a merge, compared by the
// consistency checker, and wiped with the range's other local keys when
// a replica is removed. An entry goes when the transaction's own
// coordinator resolves its intents on the range as aborted (it has given
// up and sends no more requests), or by GC once it is older than
// AbortSpanTTL.

// AbortSpanTTL is how long an abort-span entry is kept when the
// transaction's coordinator does not remove it. The entry is only needed
// until the coordinator learns that its transaction was aborted, which its
// heartbeat (every second) tells it, and from then on it fails every
// request itself; so an hour is a margin of thousands of heartbeats. It
// is not tied to the record's lifetime: an aborted record stays until it
// is GC-TTL old (25h by default), and on a contended range that would keep
// an entry for every transaction aborted there in the last day. An entry
// is only ever written for a transaction whose record says ABORTED, and
// a record never leaves ABORTED except by being reclaimed, so no record
// lookup could keep an entry alive that this age would not.
const AbortSpanTTL = time.Hour

// abortSpanEntry is the value of an abort-span entry: when it was written
// (the resolving batch's timestamp, carried in the command, so every
// replica stores the same bytes). JSON, additive-only (pkg/version).
type abortSpanEntry struct {
	Timestamp hlc.Timestamp `json:"ts"`
}

func putAbortSpanEntry(w storage.Writer, rangeID base.RangeID, txnID uuid.UUID, e abortSpanEntry) error {
	raw, err := json.Marshal(e)
	if err != nil {
		return err
	}
	return w.Put(keys.AbortSpanKey(rangeID, txnID), raw)
}

func loadAbortSpanEntry(r getter, rangeID base.RangeID, txnID uuid.UUID) (abortSpanEntry, bool, error) {
	raw, err := r.Get(keys.AbortSpanKey(rangeID, txnID))
	if err != nil || raw == nil {
		return abortSpanEntry{}, false, err
	}
	var e abortSpanEntry
	if err := json.Unmarshal(raw, &e); err != nil {
		return abortSpanEntry{}, false, fmt.Errorf("corrupt abort-span entry for %s on %s: %w", txnID, rangeID, err)
	}
	return e, true, nil
}

// visitAbortSpan calls f for each of the range's entries, in key order.
func visitAbortSpan(r storage.Reader, rangeID base.RangeID, f func(txnID uuid.UUID, e abortSpanEntry) error) error {
	pre := keys.AbortSpanPrefix(rangeID)
	end := pre.PrefixEnd()
	it := r.NewIter(pre, end)
	for ok := it.SeekGE(pre); ok; ok = it.Next() {
		k := it.Key()
		if len(k) != len(pre)+16 {
			_ = it.Close()
			return fmt.Errorf("malformed abort-span key %q", k)
		}
		var id uuid.UUID
		copy(id[:], k[len(pre):])
		var e abortSpanEntry
		if err := json.Unmarshal(it.Value(), &e); err != nil {
			_ = it.Close()
			return fmt.Errorf("corrupt abort-span entry for %s on %s: %w", id, rangeID, err)
		}
		if err := f(id, e); err != nil {
			_ = it.Close()
			return err
		}
	}
	return it.Close()
}

// copyAbortSpan writes every entry of range from into range to, keeping
// the later timestamp where both have one: a split seeds its right-hand
// side with the parent's entries, a merge carries the right-hand side's
// into the left. Reads and writes the same batch, so every replica copies
// the same entries.
func copyAbortSpan(b *storage.Batch, from, to base.RangeID) error {
	type kv struct {
		id uuid.UUID
		e  abortSpanEntry
	}
	var all []kv
	if err := visitAbortSpan(b, from, func(id uuid.UUID, e abortSpanEntry) error {
		all = append(all, kv{id, e})
		return nil
	}); err != nil {
		return err
	}
	for _, c := range all {
		if cur, ok, err := loadAbortSpanEntry(b, to, c.id); err != nil {
			return err
		} else if ok && c.e.Timestamp.Less(cur.Timestamp) {
			continue
		}
		if err := putAbortSpanEntry(b, to, c.id, c.e); err != nil {
			return err
		}
	}
	return nil
}

// collectAbortSpan removes the range's entries written at or below cutoff
// (an applied GC command), returning how many went.
func collectAbortSpan(b *storage.Batch, rangeID base.RangeID, cutoff hlc.Timestamp) (int, error) {
	var old []uuid.UUID
	if err := visitAbortSpan(b, rangeID, func(id uuid.UUID, e abortSpanEntry) error {
		if e.Timestamp.LessEq(cutoff) {
			old = append(old, id)
		}
		return nil
	}); err != nil {
		return 0, err
	}
	for _, id := range old {
		if err := b.Delete(keys.AbortSpanKey(rangeID, id)); err != nil {
			return 0, err
		}
	}
	return len(old), nil
}

// countAbortSpan reports how many of the range's entries there are, and
// how many were written at or below cutoff.
func countAbortSpan(r storage.Reader, rangeID base.RangeID, cutoff hlc.Timestamp) (total, old int, _ error) {
	err := visitAbortSpan(r, rangeID, func(_ uuid.UUID, e abortSpanEntry) error {
		total++
		if e.Timestamp.LessEq(cutoff) {
			old++
		}
		return nil
	})
	return total, old, err
}

// updateAbortSpan is an applied ABORTED resolution's effect on the abort
// span: a poisoning one that removed an intent records the abort; the
// coordinator's own removes any entry for the transaction.
func (r *Replica) updateAbortSpan(b *storage.Batch, req *kvpb.ResolveIntentRequest, removed bool, ts hlc.Timestamp) error {
	if req.Status != enginepb.ABORTED {
		return nil
	}
	if req.Poison {
		if !removed {
			// Nothing of the transaction was taken away here: its reads
			// on this range still see whatever it wrote.
			return nil
		}
		return putAbortSpanEntry(b, r.rangeID, req.TxnID, abortSpanEntry{Timestamp: ts})
	}
	key := keys.AbortSpanKey(r.rangeID, req.TxnID)
	raw, err := b.Get(key)
	if err != nil || raw == nil {
		return err
	}
	return b.Delete(key)
}

// abortSpanChecked reports whether the batch's requests are ones its
// transaction must not run after an abort removed its writes on the range:
// those that read or write the range's data. Record operations
// (heartbeat, EndTxn, pushes), resolutions and savepoint rollbacks are
// not checked: they never read past a removed write, and the coordinator
// needs them to learn of the abort and clean up.
func abortSpanChecked(ba *kvpb.BatchRequest) bool {
	if ba.Header.Txn == nil || ba.Header.StaleRead {
		return false
	}
	for _, u := range ba.Requests {
		switch {
		case u.Get != nil, u.Scan != nil, u.Put != nil, u.Delete != nil, u.Increment != nil, u.Refresh != nil:
			return true
		}
	}
	return false
}

// checkAbortSpan fails the batch with a TxnAbortedError when its
// transaction has an entry in the range's abort span (see the top of this
// file for when it must run relative to the batch's evaluation).
func (r *Replica) checkAbortSpan(rd getter, ba *kvpb.BatchRequest) *kvpb.Error {
	if !abortSpanChecked(ba) {
		return nil
	}
	_, ok, err := loadAbortSpanEntry(rd, r.rangeID, ba.Header.Txn.ID)
	if err != nil {
		return kvpb.NewError(err)
	}
	if !ok {
		return nil
	}
	e := kvpb.NewErrorf("transaction %s was aborted by a conflicting transaction, which removed its writes on %s", ba.Header.Txn.ID, r.rangeID)
	e.TxnAborted = &kvpb.TxnAbortedError{}
	return e
}

// abortSpanOK reports whether abort-span entries may be written or
// collected (cluster version v22): a v21 replica does not know them.
func (s *Store) abortSpanOK() bool {
	if s.cfg.ClusterVersion == nil {
		return true
	}
	return s.cfg.ClusterVersion() >= version.V22
}
