package kvclient

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/google/uuid"
	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/kvpb"
	"github.com/sthorne/datax/pkg/storage"
)

// LockWait says what a transaction's reads do about a row another
// transaction holds locked (a foreign write intent) — the wait policies
// of SELECT ... FOR UPDATE.
type LockWait int

const (
	// LockWaitBlock queues behind the lock holder: the default.
	LockWaitBlock LockWait = iota
	// LockWaitSkip passes the locked row over as if it were not there
	// (SKIP LOCKED).
	LockWaitSkip
	// LockWaitError fails with a LockNotAvailableError (NOWAIT).
	LockWaitError
)

type lockWaitKey struct{}

// WithLockWait makes the plain reads sent under ctx (Get, GetBatch, Scan,
// ReverseScan) follow the given policy when they run into a row another
// transaction has locked. The fetch of a SELECT ... FOR UPDATE SKIP
// LOCKED or NOWAIT runs under it: an ordinary read waits for an intent at
// or below its timestamp, which is exactly the wait those statements
// exist to avoid.
//
// Under LockWaitSkip a locked key is not read at all — it reads as absent
// and stays out of the transaction's read set — so a caller must not take
// absence under this policy to mean the key does not exist.
func WithLockWait(ctx context.Context, w LockWait) context.Context {
	if w == LockWaitBlock {
		return ctx
	}
	return context.WithValue(ctx, lockWaitKey{}, w)
}

// LockWaitFrom returns the policy ctx carries (LockWaitBlock when none).
func LockWaitFrom(ctx context.Context) LockWait {
	w, _ := ctx.Value(lockWaitKey{}).(LockWait)
	return w
}

// LockNotAvailableError is a read or locking read that would have had to
// wait for a live transaction's lock and was told not to (NOWAIT). SQL
// maps it to SQLSTATE 55P03.
type LockNotAvailableError struct {
	Intents []storage.Intent
}

func (e *LockNotAvailableError) Error() string {
	return fmt.Sprintf("could not obtain lock: key %s is locked by transaction %s", e.Intents[0].Key, e.Intents[0].Txn.ID)
}

// IsLockNotAvailable reports whether err is (or wraps) a
// LockNotAvailableError.
func IsLockNotAvailable(err error) bool {
	var ln *LockNotAvailableError
	return errors.As(err, &ln)
}

// staleLock reports whether err is a locking read refused, under
// sendOpts.noRefresh, because the key has a version committed above the
// transaction's read timestamp.
func staleLock(err error) bool {
	var re *RetryableError
	return errors.As(err, &re) && (re.Cause.WriteTooOld != nil || re.Cause.Uncertainty != nil)
}

// GetBatchForUpdateNoWait locks ks as GetBatchForUpdate does, except that
// it never waits for another transaction's lock. With skip false (NOWAIT)
// the first key a live transaction holds fails the call with a
// LockNotAvailableError. With skip true (SKIP LOCKED) such a key is left
// unlocked and the rest are locked; so is a key with a version committed
// above the read timestamp — a row another transaction changed and
// committed since this one's snapshot, which under plain FOR UPDATE would
// be a refresh or a restart. The result says, positionally, which keys
// this transaction now holds.
//
// A lock holder is never aborted on this path, whatever the priorities:
// its transaction is pushed only to learn whether it is still alive (an
// intent left by a finished or expired transaction is resolved and then
// locked like any free key).
func (t *Txn) GetBatchForUpdateNoWait(ctx context.Context, ks []keys.Key, skip bool) ([]bool, error) {
	if len(ks) == 0 {
		return nil, nil
	}
	if t.historical {
		return nil, fmt.Errorf("cannot lock in a read-only historical transaction")
	}
	if err := t.flushDeferred(ctx); err != nil {
		return nil, err
	}
	locked := make([]bool, len(ks))
	rest := make([]int, len(ks))
	for i := range ks {
		rest[i] = i
	}
	// Until the transaction has its record, lock one key at a time. A
	// single-key batch either lays its intent (and creates the record
	// with it) or changes nothing, so a refused key can simply stop being
	// the anchor candidate; a multi-range batch that fails on one range
	// may have laid intents on another, and those would name an anchor
	// where no record was ever written.
	for len(rest) > 0 && !t.anchored() {
		i := rest[0]
		rest = rest[1:]
		ok, err := t.lockOneNoWait(ctx, ks[i], skip)
		if err != nil {
			return nil, err
		}
		locked[i] = ok
	}
	opts := sendOpts{noWait: true, noRefresh: skip}
	for len(rest) > 0 {
		t.mu.Lock()
		t.mu.txn.Sequence++
		txn := t.mu.txn
		t.mu.Unlock()
		ba := &kvpb.BatchRequest{Header: kvpb.BatchHeader{Txn: &txn}}
		for _, i := range rest {
			ba.Add(&kvpb.GetRequest{RequestHeader: kvpb.RequestHeader{Key: ks[i].Clone()}, ForUpdate: true})
		}
		_, err := t.sendWith(ctx, ba, true, opts)
		var ln *LockNotAvailableError
		switch {
		case err == nil:
			for _, i := range rest {
				t.recordRead(ks[i], nil)
				t.recordWrite(ks[i])
				locked[i] = true
			}
			rest = nil
		case errors.As(err, &ln) && skip:
			// The sub-batch of each refused key laid nothing; drop those
			// keys and lock the rest again (re-locking a key an earlier
			// attempt reached on another range rewrites this
			// transaction's own intent).
			held := make(map[string]bool, len(ln.Intents))
			for _, in := range ln.Intents {
				held[string(in.Key)] = true
			}
			kept := rest[:0]
			for _, i := range rest {
				if !held[string(ks[i])] {
					kept = append(kept, i)
				}
			}
			if len(kept) == len(rest) {
				return nil, err // a conflict on no key of ours: not one to skip
			}
			rest = kept
		case errors.As(err, &ln):
			// NOWAIT fails here, but the batch may have locked keys on
			// the ranges that had no conflict. Track every key so the
			// transaction's end — or a savepoint rollback — releases
			// them (resolving a key that holds no intent of this
			// transaction is a no-op).
			for _, i := range rest {
				t.recordWrite(ks[i])
			}
			return nil, err
		case skip && staleLock(err):
			// The error does not say which key is stale: find out one
			// key at a time.
			for _, i := range rest {
				ok, oerr := t.lockOneNoWait(ctx, ks[i], true)
				if oerr != nil {
					return nil, oerr
				}
				locked[i] = ok
			}
			rest = nil
		default:
			return nil, err
		}
	}
	return locked, nil
}

// lockOneNoWait is GetBatchForUpdateNoWait for a single key.
func (t *Txn) lockOneNoWait(ctx context.Context, key keys.Key, skip bool) (bool, error) {
	t.mu.Lock()
	candidate := !t.mu.anchored && len(t.mu.txn.Key) == 0
	t.mu.Unlock()
	ba, k := t.prepareWrite(&kvpb.GetRequest{RequestHeader: kvpb.RequestHeader{Key: key.Clone()}, ForUpdate: true})
	_, err := t.sendWith(ctx, ba, true, sendOpts{noWait: true, noRefresh: skip})
	if err == nil {
		t.recordRead(key, nil)
		t.recordWrite(k)
		return true, nil
	}
	refused := IsLockNotAvailable(err) || skip && staleLock(err)
	if refused && candidate {
		// The refused batch wrote nothing, the record included: the next
		// write picks the anchor afresh.
		t.mu.Lock()
		if !t.mu.anchored && keys.Key(t.mu.txn.Key).Equal(k) {
			t.mu.txn.Key = nil
		}
		t.mu.Unlock()
	}
	if refused && skip {
		return false, nil
	}
	return false, err
}

func (t *Txn) anchored() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.mu.anchored
}

// ReadMark returns a position in the transaction's read set, for
// ForgetReads.
func (t *Txn) ReadMark() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return len(t.mu.readSpans)
}

// ForgetReads removes ks from the reads recorded since mark: a point read
// of one of them is dropped and a scan over one is split around it. SKIP
// LOCKED uses it for the rows it fetched and then passed over. Left in
// the read set they would undo the point of skipping: the transaction
// that holds such a row usually changes it and commits, and the next
// refresh of this transaction (any push of its write timestamp triggers
// one) would find that write inside a span it "read" and fail with a
// serialization error over a row it never returned.
//
// Reads recorded before mark are kept whole — an earlier statement that
// really read the row is still entitled to its protection. The gaps
// between the forgotten keys stay covered, so a row inserted into the
// scanned span is still noticed.
func (t *Txn) ForgetReads(mark int, ks []keys.Key) {
	if len(ks) == 0 {
		return
	}
	sorted := append([]keys.Key(nil), ks...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].Less(sorted[j]) })
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.mu.refreshUnusable || mark >= len(t.mu.readSpans) {
		return
	}
	tail := t.mu.readSpans[mark:]
	out := make([]readSpan, 0, len(tail))
	for _, sp := range tail {
		if len(sp.end) == 0 {
			i := sort.Search(len(sorted), func(i int) bool { return !sorted[i].Less(sp.start) })
			if i == len(sorted) || !sorted[i].Equal(sp.start) {
				out = append(out, sp)
			}
			continue
		}
		cur := sp.start
		for i := sort.Search(len(sorted), func(i int) bool { return !sorted[i].Less(sp.start) }); i < len(sorted) && sorted[i].Less(sp.end); i++ {
			if cur.Less(sorted[i]) {
				out = append(out, readSpan{start: cur, end: sorted[i].Clone()})
			}
			cur = sorted[i].Next()
		}
		if cur.Less(sp.end) {
			out = append(out, readSpan{start: cur, end: sp.end})
		}
	}
	if mark+len(out) > maxReadSpans {
		t.mu.refreshUnusable = true
		t.mu.readSpans = nil
		return
	}
	t.mu.readSpans = append(t.mu.readSpans[:mark:mark], out...)
}

// ForgetSpan removes [start, end) from the reads recorded since mark: a
// point read inside it is dropped and a scan is cut back to the parts
// outside it. A statement that read further than its result depended
// on uses it for the excess (issue #320): an ordered scan with a
// residual filter and a LIMIT reads its span to the end, page by page,
// though once the limit is met the rows after the last one it kept
// cannot change its result. Left in the read set they turn every change
// another transaction commits there into a failed refresh. As with
// ForgetReads, reads recorded before mark are kept whole.
func (t *Txn) ForgetSpan(mark int, start, end keys.Key) {
	if !start.Less(end) {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.mu.refreshUnusable || mark >= len(t.mu.readSpans) {
		return
	}
	tail := t.mu.readSpans[mark:]
	out := make([]readSpan, 0, len(tail))
	for _, sp := range tail {
		if len(sp.end) == 0 {
			if sp.start.Less(start) || !sp.start.Less(end) {
				out = append(out, sp)
			}
			continue
		}
		if sp.start.Less(start) {
			cut := sp.end
			if start.Less(cut) {
				cut = start
			}
			out = append(out, readSpan{start: sp.start, end: cut})
		}
		if end.Less(sp.end) {
			from := sp.start
			if from.Less(end) {
				from = end
			}
			out = append(out, readSpan{start: from, end: sp.end})
		}
	}
	if mark+len(out) > maxReadSpans {
		t.mu.refreshUnusable = true
		t.mu.readSpans = nil
		return
	}
	t.mu.readSpans = append(t.mu.readSpans[:mark:mark], out...)
}

// recordScan records [start, end) as read, less the keys the scan passed
// over under LockWaitSkip.
func (t *Txn) recordScan(ctx context.Context, start, end keys.Key, skipped []storage.Intent) {
	mark := t.ReadMark()
	t.recordRead(start, end)
	if len(skipped) == 0 {
		return
	}
	ks := make([]keys.Key, len(skipped))
	for i, in := range skipped {
		ks[i] = in.Key
	}
	t.ForgetReads(mark, ks)
	t.db.resolveAbandonedLocks(ctx, skipped)
}

// abandonedLockAge is how old a transaction must be before a SKIP LOCKED
// read that stepped around one of its intents pushes it. A younger
// transaction cannot have expired (kvserver.TxnExpiration is 5s — keep in
// sync), and one that finished resolves its own intents promptly unless
// its coordinator died, so pushing it would buy a round trip per skipped
// row for nothing.
const abandonedLockAge = 5 * time.Second

// abandonedLockPushEvery spaces the pushes of one transaction: a lock
// held for minutes by a live transaction is asked after once a second,
// not once per statement that skips it.
const abandonedLockPushEvery = time.Second

// resolveAbandonedLocks pushes the transactions behind intents a SKIP
// LOCKED read passed over, resolving the intents of any that turn out to
// be finished or expired. Nothing else would: a skipping reader does not
// conflict with the intent, so the ordinary push-on-conflict never runs,
// and a row whose locker died would stay invisible to every SKIP LOCKED
// query for as long as those were the table's only readers. Best effort;
// the read that skipped the row is not redone.
func (db *DB) resolveAbandonedLocks(ctx context.Context, skipped []storage.Intent) {
	if len(skipped) == 0 {
		return
	}
	now := db.clock.Now()
	wall := time.Now()
	var push []storage.Intent
	db.skipPushes.Lock()
	if len(db.skipPushes.last) > 4096 {
		for id, at := range db.skipPushes.last {
			if wall.Sub(at) > abandonedLockPushEvery {
				delete(db.skipPushes.last, id)
			}
		}
	}
	due := map[uuid.UUID]bool{}
	for _, in := range skipped {
		id := in.Txn.ID
		if id == uuid.Nil {
			continue // passed over for a newer committed version, not a lock
		}
		ok, seen := due[id]
		if !seen {
			old := in.Txn.MinTimestamp.AddNanos(int64(abandonedLockAge)).Less(now)
			last, pushed := db.skipPushes.last[id]
			ok = old && (!pushed || wall.Sub(last) >= abandonedLockPushEvery)
			due[id] = ok
			if ok {
				if db.skipPushes.last == nil {
					db.skipPushes.last = map[uuid.UUID]time.Time{}
				}
				db.skipPushes.last[id] = wall
			}
		}
		if ok {
			push = append(push, in)
		}
	}
	db.skipPushes.Unlock()
	if len(push) == 0 {
		return
	}
	// One push per transaction is enough to learn its fate, and
	// PushAndResolveIntents pushes per intent: order them by transaction
	// so a live one costs a single round trip.
	sort.SliceStable(push, func(i, j int) bool { return bytes.Compare(push[i].Txn.ID[:], push[j].Txn.ID[:]) < 0 })
	for i := 0; i < len(push); {
		j := i + 1
		for j < len(push) && push[j].Txn.ID == push[i].Txn.ID {
			j++
		}
		if all, err := db.PushAndResolveIntents(ctx, push[i:i+1]); err == nil && all && j > i+1 {
			_, _ = db.PushAndResolveIntents(ctx, push[i+1:j])
		}
		i = j
	}
}
