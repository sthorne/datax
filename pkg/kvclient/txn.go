package kvclient

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math/rand"
	"sync"
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

// RetryableError wraps a conflict that requires restarting the transaction
// at a higher timestamp. SQL maps it to SQLSTATE 40001; implicit
// (auto-commit) statements are retried transparently by RunTxn.
type RetryableError struct {
	Cause *kvpb.Error
}

func (e *RetryableError) Error() string {
	return fmt.Sprintf("restart transaction: %s", e.Cause.Message)
}

// IsRetryable reports whether err calls for a transaction restart.
func IsRetryable(err error) bool {
	var re *RetryableError
	return errors.As(err, &re)
}

// Txn is a client transaction coordinator: it stamps requests with the
// transaction's state, lays down intents through writes, tracks them for
// resolution, keeps the transaction record alive with heartbeats, and
// drives conflict pushes. See docs/transactions.md.
type Txn struct {
	db *DB
	// historical: a read-only transaction pinned at a fixed past
	// timestamp. Its reads carry the StaleRead flag, so followers whose
	// closed timestamp covers it serve them locally; writes are refused.
	historical bool
	// pipelining defers RunBatch flushes so Commit can run them IN
	// PARALLEL with a staged EndTxn — the parallel-commit fast path that
	// saves one consensus round. Any operation needing read-your-writes
	// flushes the deferred batch first, so semantics never change.
	pipelining bool

	mu struct {
		sync.Mutex
		txn      kvpb.Transaction
		writes   map[string]struct{} // keys with intents (across epochs)
		anchored bool                // transaction record created
		finished bool
		// readSpans are the spans this transaction has observed; refresh
		// verifies them when moving the read timestamp forward.
		readSpans []readSpan
		// refreshUnusable: too many spans (or tracking disabled) — fall
		// back to v1's restart behavior.
		refreshUnusable bool
		// deferred is the pipelined write batch awaiting Commit (see
		// pipelining above).
		deferred *WriteBatch
		// savepoints by name; spOrder hands out creation order so RELEASE
		// and ROLLBACK TO can discard later savepoints (PG semantics).
		savepoints map[string]*savepoint
		spOrder    int
		// waitingFor/waitingForKey: the transaction this one is currently
		// blocked on in a push loop (Nil = none). Published on the record
		// (immediately on change, and with every heartbeat) so pushers can
		// walk wait chains and detect deadlock cycles.
		waitingFor    uuid.UUID
		waitingForKey keys.Key
	}
	heartbeatCancel context.CancelFunc
}

type readSpan struct {
	start, end keys.Key // end nil = point read
}

// maxReadSpans bounds refresh bookkeeping; beyond it, conflicts surface as
// restarts (v1 behavior).
const maxReadSpans = 512

func (t *Txn) recordRead(start, end keys.Key) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.mu.refreshUnusable {
		return
	}
	if len(t.mu.readSpans) >= maxReadSpans {
		t.mu.refreshUnusable = true
		t.mu.readSpans = nil
		return
	}
	t.mu.readSpans = append(t.mu.readSpans, readSpan{start: start.Clone(), end: end.Clone()})
}

// NewTxn begins a transaction at the current clock reading.
func (db *DB) NewTxn(name string) *Txn {
	t := &Txn{db: db}
	t.mu.txn = *kvpb.NewTransaction(name, rand.Int31n(1<<20), db.clock.Now())
	t.mu.writes = make(map[string]struct{})
	return t
}

// NewHistoricalTxn begins a READ-ONLY transaction pinned at ts: every read
// observes exactly the data committed at or below ts, is servable by
// follower replicas whose closed timestamp covers ts, and needs no
// uncertainty restarts. Writes are refused. The timestamp must be above
// the GC threshold (within the GC TTL) and, for follower serving, at or
// below the closed timestamp — more recent reads fall back to leaders.
func (db *DB) NewHistoricalTxn(name string, ts hlc.Timestamp) *Txn {
	t := &Txn{db: db, historical: true}
	t.mu.txn = *kvpb.NewTransaction(name, 0, ts)
	t.mu.writes = make(map[string]struct{})
	return t
}

// TestingSetPriority overrides the transaction's conflict priority.
// Deadlock tests use equal priorities so priority-based aborts (which
// require a strictly greater pusher) cannot fire and cycle detection is
// provably the mechanism that resolves the deadlock.
func (t *Txn) TestingSetPriority(p int32) {
	t.mu.Lock()
	t.mu.txn.Priority = p
	t.mu.Unlock()
}

// EnablePipelining defers write batches to Commit, which then stages the
// transaction record in parallel with them (parallel commit). Reads and
// point writes transparently flush first, preserving read-your-writes.
func (t *Txn) EnablePipelining() { t.pipelining = true }

// flushDeferred sends any pipelined write batch now — called by every
// operation that must observe (or order after) those writes.
func (t *Txn) flushDeferred(ctx context.Context) error {
	t.mu.Lock()
	wb := t.mu.deferred
	t.mu.deferred = nil
	t.mu.Unlock()
	if wb == nil || wb.Len() == 0 {
		return nil
	}
	return t.runBatchNow(ctx, wb)
}

// savepoint captures a sequence point of the transaction: everything
// needed to roll its state back to this moment.
type savepoint struct {
	order           int
	seq             int32
	writes          map[string]struct{}
	readSpans       int
	refreshUnusable bool
}

// Savepoint establishes (or moves) a named savepoint at the transaction's
// current state.
func (t *Txn) Savepoint(name string) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.mu.finished {
		return fmt.Errorf("transaction already finished")
	}
	if t.mu.savepoints == nil {
		t.mu.savepoints = make(map[string]*savepoint)
	}
	writes := make(map[string]struct{}, len(t.mu.writes))
	for k := range t.mu.writes {
		writes[k] = struct{}{}
	}
	t.mu.spOrder++
	t.mu.savepoints[name] = &savepoint{
		order:           t.mu.spOrder,
		seq:             t.mu.txn.Sequence,
		writes:          writes,
		readSpans:       len(t.mu.readSpans),
		refreshUnusable: t.mu.refreshUnusable,
	}
	return nil
}

// ReleaseSavepoint destroys the named savepoint and every savepoint
// established after it (PG semantics). The transaction's effects are kept.
func (t *Txn) ReleaseSavepoint(name string) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	sp, ok := t.mu.savepoints[name]
	if !ok {
		return fmt.Errorf("savepoint %q does not exist", name)
	}
	for n, other := range t.mu.savepoints {
		if other.order >= sp.order {
			delete(t.mu.savepoints, n)
		}
	}
	return nil
}

// RollbackToSavepoint rolls the transaction back to the named savepoint:
// every intent laid after it is physically restored to its state at the
// savepoint (or removed), read-span tracking is truncated, and savepoints
// established after it are destroyed. The savepoint itself survives and
// can be rolled back to again.
func (t *Txn) RollbackToSavepoint(ctx context.Context, name string) error {
	t.mu.Lock()
	if t.mu.finished {
		t.mu.Unlock()
		return fmt.Errorf("transaction already finished")
	}
	sp, ok := t.mu.savepoints[name]
	if !ok {
		t.mu.Unlock()
		return fmt.Errorf("savepoint %q does not exist", name)
	}
	txn := t.mu.txn
	if t.mu.deferred != nil {
		// Deferred writes were never sent: rolling back before the flush
		// simply discards the portion after the savepoint... simplest and
		// correct: flush first, then roll back through the normal path.
		t.mu.Unlock()
		if err := t.flushDeferred(ctx); err != nil {
			return err
		}
		return t.RollbackToSavepoint(ctx, name)
	}
	writeKeys := make([]keys.Key, 0, len(t.mu.writes))
	for k := range t.mu.writes {
		writeKeys = append(writeKeys, keys.Key(k).Clone())
	}
	t.mu.Unlock()

	// Physically restore every intent to its newest state at or below the
	// savepoint's sequence. Idempotent per key (no-op for keys untouched
	// since the savepoint), so the whole write set is sent; one batch, the
	// DistSender groups per range.
	if len(writeKeys) > 0 {
		ba := &kvpb.BatchRequest{Header: kvpb.BatchHeader{Timestamp: t.db.clock.Now()}}
		for _, k := range writeKeys {
			ba.Add(&kvpb.RollbackIntentRequest{
				RequestHeader: kvpb.RequestHeader{Key: k},
				TxnID:         txn.ID,
				Sequence:      sp.seq,
			})
		}
		if _, kerr := t.db.Send(ctx, ba); kerr != nil {
			return fmt.Errorf("rolling back to savepoint %q: %w", name, kerr)
		}
	}

	t.mu.Lock()
	defer t.mu.Unlock()
	restored := make(map[string]struct{}, len(sp.writes))
	for k := range sp.writes {
		restored[k] = struct{}{}
	}
	t.mu.writes = restored
	if len(t.mu.readSpans) > sp.readSpans {
		t.mu.readSpans = t.mu.readSpans[:sp.readSpans]
	}
	t.mu.refreshUnusable = sp.refreshUnusable
	for n, other := range t.mu.savepoints {
		if other.order > sp.order {
			delete(t.mu.savepoints, n)
		}
	}
	return nil
}

// proto returns a snapshot of the transaction state.
func (t *Txn) proto() *kvpb.Transaction {
	t.mu.Lock()
	defer t.mu.Unlock()
	txn := t.mu.txn
	return &txn
}

// conflictBudget bounds how long an operation waits on a live conflicting
// transaction before surfacing a retryable error to the client. With
// deadlock detection (cycles are found and broken in a few poll rounds),
// this is a generous backstop rather than the deadlock breaker it was in
// v1, so waiters queueing behind a slow-but-live lock holder are no longer
// aborted after 2s.
const conflictBudget = 10 * time.Second

// Get reads a key at the transaction's read timestamp (seeing the
// transaction's own writes).
func (t *Txn) Get(ctx context.Context, key keys.Key) ([]byte, error) {
	if err := t.flushDeferred(ctx); err != nil {
		return nil, err
	}
	ba := &kvpb.BatchRequest{Header: kvpb.BatchHeader{Txn: t.proto()}}
	ba.Add(&kvpb.GetRequest{RequestHeader: kvpb.RequestHeader{Key: key}})
	br, err := t.send(ctx, ba, false)
	if err != nil {
		return nil, err
	}
	t.recordRead(key, nil)
	return br.Responses[0].Get.Value, nil
}

// Scan reads [start, end) at the transaction's read timestamp.
func (t *Txn) Scan(ctx context.Context, start, end keys.Key, max int64) ([]kvpb.KeyValue, error) {
	if err := t.flushDeferred(ctx); err != nil {
		return nil, err
	}
	ba := &kvpb.BatchRequest{Header: kvpb.BatchHeader{Txn: t.proto()}}
	ba.Add(&kvpb.ScanRequest{RequestHeader: kvpb.RequestHeader{Key: start, EndKey: end}, MaxRows: max})
	br, err := t.send(ctx, ba, false)
	if err != nil {
		return nil, err
	}
	resp := br.Responses[0].Scan
	observedEnd := end
	if len(resp.Resume) > 0 {
		observedEnd = resp.Resume // only [start, resume) was observed
	}
	t.recordRead(start, observedEnd)
	return resp.Rows, nil
}

// GetForUpdate reads key at the transaction's read timestamp AND locks it:
// the server atomically lays a write intent pinning the observed state
// (value or absence), so no other transaction can change the key until
// this one finishes. Serializes read-modify-write upfront — the SELECT FOR
// UPDATE primitive.
func (t *Txn) GetForUpdate(ctx context.Context, key keys.Key) ([]byte, error) {
	if t.historical {
		return nil, fmt.Errorf("cannot lock in a read-only historical transaction")
	}
	if err := t.flushDeferred(ctx); err != nil {
		return nil, err
	}
	ba, k := t.prepareWrite(&kvpb.GetRequest{RequestHeader: kvpb.RequestHeader{Key: key.Clone()}, ForUpdate: true})
	br, err := t.send(ctx, ba, true)
	if err != nil {
		return nil, err
	}
	t.recordRead(key, nil)
	// The lock is an intent on the key (absent rows are pinned with a
	// tombstone intent), so it must be resolved at commit/abort.
	t.recordWrite(k)
	return br.Responses[0].Get.Value, nil
}

// ScanForUpdate reads [start, end) at the transaction's read timestamp and
// locks every returned row (absent keys in the span are not locked; the
// recorded read span still protects the gap via refresh).
func (t *Txn) ScanForUpdate(ctx context.Context, start, end keys.Key, max int64) ([]kvpb.KeyValue, error) {
	if t.historical {
		return nil, fmt.Errorf("cannot lock in a read-only historical transaction")
	}
	if err := t.flushDeferred(ctx); err != nil {
		return nil, err
	}
	ba, k := t.prepareWrite(&kvpb.ScanRequest{RequestHeader: kvpb.RequestHeader{Key: start.Clone(), EndKey: end.Clone()}, ForUpdate: true, MaxRows: max})
	br, err := t.send(ctx, ba, true)
	if err != nil {
		return nil, err
	}
	resp := br.Responses[0].Scan
	observedEnd := end
	if len(resp.Resume) > 0 {
		observedEnd = resp.Resume
	}
	t.recordRead(start, observedEnd)
	// Anchor bookkeeping: the scan's start key was the prepareWrite anchor
	// candidate; record it (resolution of a key with no intent is a no-op)
	// so the record is heartbeat-kept even when the scan locked nothing.
	t.recordWrite(k)
	for _, kv := range resp.Rows {
		t.recordWrite(kv.Key)
	}
	return resp.Rows, nil
}

// Put writes key = value as a write intent.
func (t *Txn) Put(ctx context.Context, key keys.Key, value []byte) error {
	return t.write(ctx, &kvpb.PutRequest{RequestHeader: kvpb.RequestHeader{Key: key.Clone()}, Value: value})
}

// Delete deletes key (an intent carrying a tombstone).
func (t *Txn) Delete(ctx context.Context, key keys.Key) error {
	return t.write(ctx, &kvpb.DeleteRequest{RequestHeader: kvpb.RequestHeader{Key: key.Clone()}})
}

// Increment atomically increments a counter within the transaction.
func (t *Txn) Increment(ctx context.Context, key keys.Key, by int64) (int64, error) {
	ba, key0 := t.prepareWrite(&kvpb.IncrementRequest{RequestHeader: kvpb.RequestHeader{Key: key.Clone()}, By: by})
	br, err := t.send(ctx, ba, true)
	if err != nil {
		return 0, err
	}
	t.recordWrite(key0)
	return br.Responses[0].Increment.NewValue, nil
}

func (t *Txn) write(ctx context.Context, req kvpb.Request) error {
	if t.historical {
		return fmt.Errorf("cannot write in a read-only historical transaction")
	}
	if err := t.flushDeferred(ctx); err != nil {
		return err
	}
	ba, key := t.prepareWrite(req)
	if _, err := t.send(ctx, ba, true); err != nil {
		return err
	}
	t.recordWrite(key)
	return nil
}

// prepareWrite anchors the transaction on its first-ever write: the record
// is created on the range of that key, atomically with the write itself.
func (t *Txn) prepareWrite(req kvpb.Request) (*kvpb.BatchRequest, keys.Key) {
	t.mu.Lock()
	t.mu.txn.Sequence++ // orders own writes; savepoint rollback keys off it
	key := req.Header().Key
	createRecord := false
	if !t.mu.anchored {
		if len(t.mu.txn.Key) == 0 {
			t.mu.txn.Key = key.Clone()
		}
		// Record creation must be co-located with a write on the anchor
		// range, so only the batch writing the anchor key itself creates it.
		if keys.Key(t.mu.txn.Key).Equal(key) {
			createRecord = true
		}
	}
	txn := t.mu.txn
	t.mu.Unlock()

	ba := &kvpb.BatchRequest{Header: kvpb.BatchHeader{Txn: &txn, CreateTxnRecord: createRecord}}
	ba.Add(req)
	return ba, key
}

func (t *Txn) recordWrite(key keys.Key) {
	t.mu.Lock()
	t.mu.writes[string(key)] = struct{}{}
	anchoredNow := !t.mu.anchored && keys.Key(t.mu.txn.Key).Equal(key)
	if anchoredNow {
		t.mu.anchored = true
	}
	t.mu.Unlock()
	if anchoredNow {
		t.startHeartbeat()
	}
}

// send executes a batch, handling conflicts: intents are pushed (and
// resolved when their transaction is finalized or expired); live conflicts
// are waited out briefly; timestamp conflicts trigger a read refresh and,
// only if that fails, surface as RetryableError.
func (t *Txn) send(ctx context.Context, ba *kvpb.BatchRequest, isWrite bool) (*kvpb.BatchResponse, error) {
	if t.historical {
		// Pinned-timestamp reads are follower-servable and need no
		// uncertainty interval.
		ba.Header.StaleRead = true
	}
	waited := time.Duration(0)
	refreshes := 0
	for {
		br, kerr := t.db.Send(ctx, ba)
		if kerr == nil {
			t.publishWait(ctx, nil) // no longer blocked, if we were
			if br.Txn != nil {
				// The server may push a write's timestamp above its
				// timestamp cache instead of rejecting it; adopt the pushed
				// timestamp so commit knows to refresh.
				t.mu.Lock()
				t.mu.txn.WriteTimestamp = t.mu.txn.WriteTimestamp.Forward(br.Txn.WriteTimestamp)
				t.mu.Unlock()
			}
			return br, nil
		}
		switch {
		case kerr.WriteIntent != nil:
			resolvedAll, pending, err := t.pushIntents(ctx, kerr.WriteIntent.Intents, isWrite)
			if err != nil {
				t.publishWait(ctx, nil)
				return nil, err
			}
			if resolvedAll {
				t.publishWait(ctx, nil)
				continue
			}
			// We are blocked on a live transaction: publish the wait edge
			// and walk the chain for a cycle before backing off.
			t.publishWait(ctx, &pending.Txn)
			if derr := t.detectDeadlock(ctx, pending); derr != nil {
				return nil, derr // self chosen as deadlock victim
			}
			if waited >= conflictBudget {
				t.publishWait(ctx, nil)
				return nil, &RetryableError{Cause: kerr}
			}
			select {
			case <-ctx.Done():
				t.publishWait(ctx, nil)
				return nil, ctx.Err()
			case <-time.After(100 * time.Millisecond):
				waited += 100 * time.Millisecond
			}
		case kerr.TxnAborted != nil:
			t.markFinished()
			return nil, &RetryableError{Cause: kerr}
		case kerr.IsRetryableTxnError():
			// Timestamp conflict (WriteTooOld / tsCache push / uncertainty
			// / commit equality): try to move the read timestamp forward by
			// proving our read spans saw no writes in the window, instead
			// of restarting.
			newTS := kerr.RetryTimestamp(t.db.clock.Now())
			if refreshes < maxRefreshesPerOp && t.maybeRefresh(ctx, newTS) {
				refreshes++
				metrics.TxnRefreshes.Inc()
				ba.Header.Txn = t.proto() // re-stamp with refreshed timestamps
				continue
			}
			metrics.TxnRetries.Inc()
			return nil, &RetryableError{Cause: kerr}
		default:
			return nil, kerr
		}
	}
}

// maxRefreshesPerOp bounds refresh loops under pathological contention.
const maxRefreshesPerOp = 10

// txnPoisonGuardAge is how old a transaction may be before parallelCommit
// stops racing its write batch against the record-creating staged EndTxn.
// Half of kvserver.TxnExpiration (5s — keep in sync): beyond it, a pusher
// finding an intent before the record exists could judge the transaction
// expired from MinTimestamp and poison the record ABORTED mid-commit.
const txnPoisonGuardAge = 2500 * time.Millisecond

// maybeRefresh verifies every tracked read span saw no foreign write in
// (readTS, newTS] and, on success, advances the transaction's read and
// write timestamps to newTS. See docs/transactions.md.
func (t *Txn) maybeRefresh(ctx context.Context, newTS hlc.Timestamp) bool {
	t.mu.Lock()
	if t.mu.refreshUnusable {
		t.mu.Unlock()
		return false
	}
	oldRead := t.mu.txn.ReadTimestamp
	spans := append([]readSpan(nil), t.mu.readSpans...)
	provisional := t.mu.txn
	t.mu.Unlock()

	if newTS.LessEq(oldRead) {
		return true // nothing to move
	}
	provisional.ReadTimestamp = newTS
	provisional.WriteTimestamp = newTS
	for _, sp := range spans {
		ba := &kvpb.BatchRequest{Header: kvpb.BatchHeader{Txn: provisional.Clone()}}
		ba.Add(&kvpb.RefreshRequest{
			RequestHeader: kvpb.RequestHeader{Key: sp.start, EndKey: sp.end},
			FromTS:        oldRead,
		})
		if _, kerr := t.db.Send(ctx, ba); kerr != nil {
			log.Debugf("txn %s refresh to %s failed: %v", provisional.ID, newTS, kerr)
			return false
		}
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if !t.mu.txn.ReadTimestamp.Equal(oldRead) {
		return false // concurrent change; be conservative
	}
	t.mu.txn.ReadTimestamp = newTS
	t.mu.txn.WriteTimestamp = t.mu.txn.WriteTimestamp.Forward(newTS)
	return true
}

// publishWait records the transaction this one is blocked on (nil = no
// longer blocked) and publishes the edge on the transaction record with an
// immediate heartbeat, so pushers walking wait chains see it without
// waiting for the periodic beat. Best-effort: the periodic heartbeat
// re-publishes every second regardless.
func (t *Txn) publishWait(ctx context.Context, pushee *enginepb.TxnMeta) {
	var id uuid.UUID
	var key keys.Key
	if pushee != nil {
		id = pushee.ID
		key = keys.Key(pushee.Key).Clone()
	}
	t.mu.Lock()
	changed := t.mu.waitingFor != id
	t.mu.waitingFor, t.mu.waitingForKey = id, key
	anchored := t.mu.anchored
	txn := t.mu.txn
	t.mu.Unlock()
	if !changed || !anchored {
		// Unanchored transactions hold no intents, so nothing can wait on
		// them and they can never be part of a cycle — no edge to publish.
		return
	}
	ba := &kvpb.BatchRequest{Header: kvpb.BatchHeader{Txn: &txn}}
	ba.Add(&kvpb.HeartbeatTxnRequest{
		RequestHeader: kvpb.RequestHeader{Key: keys.Key(txn.Key).Clone()},
		Now:           t.db.clock.Now(),
		WaitingFor:    id,
		WaitingForKey: key,
	})
	hctx, cancel := context.WithTimeout(ctx, time.Second)
	if _, kerr := t.db.Send(hctx, ba); kerr != nil {
		log.Debugf("txn %s wait-edge publish: %v", txn.ID, kerr)
	}
	cancel()
}

// maxDeadlockChain bounds the wait-chain walk.
const maxDeadlockChain = 8

// detectDeadlock walks the advertised wait chain starting at the pushee
// this transaction is blocked on. If the chain leads back to this
// transaction, a deadlock cycle exists: the member with the lowest
// priority (transaction ID as tie-break) is chosen deterministically by
// every walker and force-aborted. Returns a RetryableError when this
// transaction itself is the victim; nil otherwise (including when a
// victim elsewhere in the cycle was aborted — the caller keeps polling
// and observes the chain unblock).
//
// Wait edges are advisory and may be stale, so a detected "cycle" can be
// a phantom; the cost of acting on one is a single spurious retryable
// abort, never an anomaly.
func (t *Txn) detectDeadlock(ctx context.Context, pending *storage.Intent) error {
	type member struct {
		id       uuid.UUID
		key      keys.Key
		priority int32
	}
	self := t.proto()
	cur := member{id: pending.Txn.ID, key: keys.Key(pending.Txn.Key).Clone()}
	var chain []member
	cycle := false
	for hop := 0; hop < maxDeadlockChain; hop++ {
		ba := &kvpb.BatchRequest{Header: kvpb.BatchHeader{Timestamp: t.db.clock.Now()}}
		ba.Add(&kvpb.PushTxnRequest{
			RequestHeader: kvpb.RequestHeader{Key: cur.key.Clone()},
			PusheeTxn:     enginepb.TxnMeta{ID: cur.id, Key: cur.key},
			QueryOnly:     true,
			Now:           t.db.clock.Now(),
		})
		br, kerr := t.db.Send(ctx, ba)
		if kerr != nil {
			return nil // walk is best-effort
		}
		resp := br.Responses[0].PushTxn
		if resp.Status != enginepb.PENDING {
			return nil // chain broken: someone finished
		}
		cur.priority = resp.Priority
		chain = append(chain, cur)
		if resp.WaitingFor == uuid.Nil {
			return nil // head of the chain is running, not waiting
		}
		if resp.WaitingFor == self.ID {
			cycle = true
			break
		}
		cur = member{id: resp.WaitingFor, key: keys.Key(resp.WaitingForKey).Clone()}
	}
	if !cycle {
		return nil
	}

	victim := member{id: self.ID, key: keys.Key(self.Key), priority: self.Priority}
	for _, m := range chain {
		if m.priority < victim.priority ||
			(m.priority == victim.priority && bytes.Compare(m.id[:], victim.id[:]) < 0) {
			victim = m
		}
	}
	metrics.DeadlockAborts.Inc()
	if victim.id == self.ID {
		// We are the victim. Abort our own record NOW — the cycle is only
		// broken once our partners' next poll sees ABORTED and resolves our
		// intents; leaving the record to expire would stall them for the
		// whole expiration window.
		log.Debugf("txn %s: deadlock cycle of %d, self is victim", self.ID, len(chain)+1)
		aba := &kvpb.BatchRequest{Header: kvpb.BatchHeader{Timestamp: t.db.clock.Now()}}
		aba.Add(&kvpb.PushTxnRequest{
			RequestHeader: kvpb.RequestHeader{Key: keys.Key(self.Key).Clone()},
			PusheeTxn:     self.TxnMeta,
			ForceAbort:    true,
			Now:           t.db.clock.Now(),
		})
		if _, kerr := t.db.Send(ctx, aba); kerr != nil {
			log.Debugf("txn %s: self-abort as deadlock victim: %v", self.ID, kerr)
		}
		t.markFinished()
		e := kvpb.NewErrorf("deadlock detected: transaction %s chosen as victim", self.ID)
		e.TxnAborted = &kvpb.TxnAbortedError{}
		return &RetryableError{Cause: e}
	}
	log.Debugf("txn %s: deadlock cycle of %d, aborting victim %s", self.ID, len(chain)+1, victim.id)
	ba := &kvpb.BatchRequest{Header: kvpb.BatchHeader{Timestamp: t.db.clock.Now()}}
	ba.Add(&kvpb.PushTxnRequest{
		RequestHeader: kvpb.RequestHeader{Key: victim.key.Clone()},
		PusherTxn:     self,
		PusheeTxn:     enginepb.TxnMeta{ID: victim.id, Key: victim.key},
		ForceAbort:    true,
		Now:           t.db.clock.Now(),
	})
	if _, kerr := t.db.Send(ctx, ba); kerr != nil {
		log.Debugf("txn %s: aborting deadlock victim %s: %v", self.ID, victim.id, kerr)
	}
	return nil
}

// pushIntents pushes the owners of conflicting intents. Returns true if
// every intent was resolved (the pushees were finalized, expired, or
// aborted by priority) and the operation can be retried immediately;
// otherwise pending names one still-live blocker.
func (t *Txn) pushIntents(ctx context.Context, intents []storage.Intent, pushAbort bool) (bool, *storage.Intent, error) {
	all := true
	var pending *storage.Intent
	for _, intent := range intents {
		ba := &kvpb.BatchRequest{Header: kvpb.BatchHeader{Timestamp: t.db.clock.Now()}}
		ba.Add(&kvpb.PushTxnRequest{
			RequestHeader: kvpb.RequestHeader{Key: keys.Key(intent.Txn.Key).Clone()},
			PusherTxn:     t.proto(),
			PusheeTxn:     intent.Txn,
			PushAbort:     pushAbort,
			Now:           t.db.clock.Now(),
		})
		br, kerr := t.db.Send(ctx, ba)
		if kerr != nil {
			return false, nil, kerr
		}
		push := br.Responses[0].PushTxn
		if push.Status == enginepb.STAGING {
			// A parallel commit in flight: run status recovery, then let
			// the next push observe the finalized record.
			t.db.recoverStagedTxn(ctx, intent.Txn, push)
			br, kerr = t.db.Send(ctx, ba)
			if kerr != nil {
				return false, nil, kerr
			}
			push = br.Responses[0].PushTxn
		}
		switch push.Status {
		case enginepb.COMMITTED, enginepb.ABORTED:
			rba := &kvpb.BatchRequest{Header: kvpb.BatchHeader{Timestamp: t.db.clock.Now()}}
			rba.Add(&kvpb.ResolveIntentRequest{
				RequestHeader: kvpb.RequestHeader{Key: intent.Key.Clone()},
				TxnID:         intent.Txn.ID,
				Status:        push.Status,
				CommitTS:      push.CommitTS,
			})
			if _, kerr := t.db.Send(ctx, rba); kerr != nil {
				return false, nil, kerr
			}
		default:
			all = false // pushee alive; caller waits
			if pending == nil {
				p := intent
				pending = &p
			}
		}
	}
	return all, pending, nil
}

// Commit atomically commits the transaction: one replicated flip of its
// record, then best-effort intent resolution (lazy resolution by future
// readers covers any failure here).
func (t *Txn) Commit(ctx context.Context) error {
	t.mu.Lock()
	if t.mu.finished {
		t.mu.Unlock()
		return fmt.Errorf("transaction already finished")
	}
	wb := t.mu.deferred
	t.mu.deferred = nil
	t.mu.Unlock()
	if wb != nil && wb.Len() > 0 {
		return t.parallelCommit(ctx, wb)
	}
	t.mu.Lock()
	anchored := t.mu.anchored
	txn := t.mu.txn
	t.mu.Unlock()

	if !anchored {
		// Read-only transaction: nothing to flip, nothing to resolve.
		t.markFinished()
		return nil
	}
	t.mu.Lock()
	intentKeys := make([]keys.Key, 0, len(t.mu.writes))
	for k := range t.mu.writes {
		intentKeys = append(intentKeys, keys.Key(k).Clone())
	}
	t.mu.Unlock()
	ba := &kvpb.BatchRequest{Header: kvpb.BatchHeader{Txn: &txn}}
	ba.Add(&kvpb.EndTxnRequest{
		RequestHeader: kvpb.RequestHeader{Key: keys.Key(txn.Key).Clone()},
		Commit:        true,
		IntentKeys:    intentKeys,
	})
	br, err := t.send(ctx, ba, true)
	if err != nil {
		t.markFinished()
		return err
	}
	t.markFinished()
	metrics.TxnCommits.Inc()
	t.resolveAll(enginepb.COMMITTED, br.Responses[0].EndTxn.CommitTimestamp)
	return nil
}

// parallelCommit is the pipelined commit fast path: the deferred write
// batch and a STAGING EndTxn are sent IN PARALLEL — one consensus round of
// client-visible latency instead of two. The transaction is implicitly
// committed the moment both succeed at the staged timestamp; an explicit
// finalize (and intent resolution) then runs asynchronously. If anything
// forwarded the writes above the staged timestamp, the commit condition is
// settled the classic way: refresh, then a finalizing EndTxn. On failure
// the staged record is explicitly aborted so status recovery agrees with
// the error we return.
func (t *Txn) parallelCommit(ctx context.Context, wb *WriteBatch) error {
	t.mu.Lock()
	t.mu.txn.Sequence++
	if len(t.mu.txn.Key) == 0 {
		t.mu.txn.Key = wb.kys[0].Clone()
	}
	createRecord := !t.mu.anchored
	txn := t.mu.txn
	intentKeys := make([]keys.Key, 0, len(t.mu.writes)+len(wb.kys))
	for k := range t.mu.writes {
		intentKeys = append(intentKeys, keys.Key(k).Clone())
	}
	t.mu.Unlock()
	inFlight := make([]keys.Key, 0, len(wb.kys))
	for _, k := range wb.kys {
		inFlight = append(inFlight, k.Clone())
		intentKeys = append(intentKeys, k.Clone())
	}
	// Track the batch's keys now so any failure path's cleanup resolves
	// them (recordWrite also anchors and starts the heartbeat).
	for _, k := range wb.kys {
		t.recordWrite(k)
	}

	wbBA := &kvpb.BatchRequest{Header: kvpb.BatchHeader{Txn: txn.Clone()}, Requests: wb.reqs}
	etBA := &kvpb.BatchRequest{Header: kvpb.BatchHeader{Txn: txn.Clone(), CreateTxnRecord: createRecord}}
	etBA.Add(&kvpb.EndTxnRequest{
		RequestHeader: kvpb.RequestHeader{Key: keys.Key(txn.Key).Clone()},
		Commit:        true,
		IntentKeys:    intentKeys,
		InFlight:      inFlight,
	})

	var (
		wbBR, etBR   *kvpb.BatchResponse
		wbErr, etErr error
	)
	if t.db.clock.Now().WallTime-txn.MinTimestamp.WallTime > int64(txnPoisonGuardAge) {
		// Poison guard: a pusher that finds one of the write batch's
		// intents BEFORE the staged record exists judges expiry from
		// MinTimestamp (kvserver's rec==nil push path) and would abort the
		// record out from under a transaction older than TxnExpiration.
		// For a transaction old enough to be at risk, forfeit the
		// parallelism: create the staged record first, then send the
		// writes.
		etBR, etErr = t.send(ctx, etBA, true)
		if etErr == nil {
			wbBR, wbErr = t.send(ctx, wbBA, true)
		}
	} else {
		var wg sync.WaitGroup
		wg.Add(2)
		go func() { defer wg.Done(); wbBR, wbErr = t.send(ctx, wbBA, true) }()
		go func() { defer wg.Done(); etBR, etErr = t.send(ctx, etBA, true) }()
		wg.Wait()
	}

	if wbErr != nil || etErr != nil {
		// Make the record's fate explicit before surfacing the error: a
		// dangling STAGING record with all writes present would otherwise
		// be RECOVERED AS COMMITTED, contradicting the error we return.
		aba := &kvpb.BatchRequest{Header: kvpb.BatchHeader{Txn: t.proto()}}
		aba.Add(&kvpb.EndTxnRequest{RequestHeader: kvpb.RequestHeader{Key: keys.Key(txn.Key).Clone()}, Commit: false})
		if _, kerr := t.db.Send(ctx, aba); kerr != nil && kerr.TxnAborted == nil {
			log.Debugf("aborting failed parallel commit of %s: %v", txn.ID, kerr)
		}
		t.markFinished()
		t.resolveAll(enginepb.ABORTED, txn.WriteTimestamp)
		if wbErr != nil {
			return wbErr
		}
		return etErr
	}

	stagedTS := etBR.Responses[0].EndTxn.CommitTimestamp
	writesTS := stagedTS
	if wbBR.Txn != nil {
		writesTS = writesTS.Forward(wbBR.Txn.WriteTimestamp)
	}
	if stagedTS.Less(writesTS) {
		// The writes were forwarded above the staged timestamp: the staged
		// commit condition does not hold. Settle classically: refresh the
		// reads to the writes' timestamp, then finalize at it.
		if !t.maybeRefresh(ctx, writesTS) {
			aba := &kvpb.BatchRequest{Header: kvpb.BatchHeader{Txn: t.proto()}}
			aba.Add(&kvpb.EndTxnRequest{RequestHeader: kvpb.RequestHeader{Key: keys.Key(txn.Key).Clone()}, Commit: false})
			if _, kerr := t.db.Send(ctx, aba); kerr != nil && kerr.TxnAborted == nil {
				log.Debugf("aborting unrefreshable parallel commit of %s: %v", txn.ID, kerr)
			}
			t.markFinished()
			t.resolveAll(enginepb.ABORTED, txn.WriteTimestamp)
			e := kvpb.NewErrorf("parallel commit of %s forwarded to %s and refresh failed", txn.ID, writesTS)
			e.TxnRetry = &kvpb.TxnRetryError{RetryTimestamp: writesTS}
			metrics.TxnRetries.Inc()
			return &RetryableError{Cause: e}
		}
		fba := &kvpb.BatchRequest{Header: kvpb.BatchHeader{Txn: t.proto()}}
		fba.Add(&kvpb.EndTxnRequest{
			RequestHeader: kvpb.RequestHeader{Key: keys.Key(txn.Key).Clone()},
			Commit:        true,
			IntentKeys:    intentKeys,
		})
		fbr, err := t.send(ctx, fba, true)
		if err != nil {
			t.markFinished()
			return err
		}
		t.markFinished()
		metrics.TxnCommits.Inc()
		t.resolveAll(enginepb.COMMITTED, fbr.Responses[0].EndTxn.CommitTimestamp)
		return nil
	}

	// Implicitly committed: every pipelined write applied at or below the
	// staged timestamp. Return to the client now; finalize asynchronously
	// (a reader tripping over an intent first runs status recovery, which
	// reaches the same verdict).
	t.markFinished()
	metrics.TxnCommits.Inc()
	metrics.ParallelCommits.Inc()
	go func() {
		fctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		fba := &kvpb.BatchRequest{Header: kvpb.BatchHeader{Txn: txn.Clone()}}
		fba.Add(&kvpb.EndTxnRequest{
			RequestHeader: kvpb.RequestHeader{Key: keys.Key(txn.Key).Clone()},
			Commit:        true,
			IntentKeys:    intentKeys,
		})
		if _, kerr := t.db.Send(fctx, fba); kerr != nil {
			log.Debugf("finalizing parallel commit of %s: %v (recovery will cover)", txn.ID, kerr)
			return
		}
		t.resolveAll(enginepb.COMMITTED, stagedTS)
	}()
	return nil
}

// recoverStagedTxn runs status recovery against a STAGING record: verify
// each staged in-flight write with a PREVENTION READ at the staged
// timestamp — the ordinary read path bumps the timestamp cache first
// (invariant L2), so a write found missing can never land at or below the
// staged timestamp afterwards — then finalize the record accordingly.
func (db *DB) recoverStagedTxn(ctx context.Context, pushee enginepb.TxnMeta, push *kvpb.PushTxnResponse) {
	stagedTS := push.CommitTS
	committed := true
	for _, k := range push.InFlightKeys {
		ba := &kvpb.BatchRequest{Header: kvpb.BatchHeader{Timestamp: stagedTS}}
		ba.Add(&kvpb.GetRequest{RequestHeader: kvpb.RequestHeader{Key: k.Clone()}})
		_, kerr := db.Send(ctx, ba)
		if kerr == nil {
			// No intent of the staged transaction at or below stagedTS —
			// and the read's timestamp-cache bump now PREVENTS one forever.
			committed = false
			break
		}
		if kerr.WriteIntent != nil {
			present := false
			for _, in := range kerr.WriteIntent.Intents {
				if in.Txn.ID == pushee.ID {
					present = true
				}
			}
			if present {
				continue
			}
			// A different transaction's intent: ours is not there, and the
			// bump (taken before evaluation) prevents it.
			committed = false
			break
		}
		// Transport or routing trouble: recovery is best-effort; the next
		// push retries it.
		log.Debugf("recovering staged txn %s: probing %s: %v", pushee.ID, k, kerr)
		return
	}
	metrics.TxnRecoveries.Inc()
	rba := &kvpb.BatchRequest{Header: kvpb.BatchHeader{Timestamp: db.clock.Now()}}
	rba.Add(&kvpb.RecoverTxnRequest{
		RequestHeader: kvpb.RequestHeader{Key: keys.Key(pushee.Key).Clone()},
		TxnID:         pushee.ID,
		Commit:        committed,
	})
	if _, kerr := db.Send(ctx, rba); kerr != nil {
		log.Debugf("recovering staged txn %s: %v", pushee.ID, kerr)
	}
}

// Rollback aborts the transaction and cleans its intents (best effort).
func (t *Txn) Rollback(ctx context.Context) error {
	t.mu.Lock()
	if t.mu.finished {
		t.mu.Unlock()
		return nil
	}
	anchored := t.mu.anchored
	txn := t.mu.txn
	t.mu.Unlock()
	t.markFinished()
	if !anchored {
		return nil
	}
	ba := &kvpb.BatchRequest{Header: kvpb.BatchHeader{Txn: &txn}}
	ba.Add(&kvpb.EndTxnRequest{RequestHeader: kvpb.RequestHeader{Key: keys.Key(txn.Key).Clone()}, Commit: false})
	metrics.TxnAborts.Inc()
	if _, kerr := t.db.Send(ctx, ba); kerr != nil && kerr.TxnAborted == nil {
		log.Debugf("rollback of %s: %v", txn.ID, kerr)
	}
	t.resolveAll(enginepb.ABORTED, txn.WriteTimestamp)
	return nil
}

// resolveAll resolves every tracked intent (best effort — lazy resolution
// by future readers covers failures).
func (t *Txn) resolveAll(status enginepb.TxnStatus, commitTS hlc.Timestamp) {
	t.mu.Lock()
	txn := t.mu.txn
	writeKeys := make([]keys.Key, 0, len(t.mu.writes))
	for k := range t.mu.writes {
		writeKeys = append(writeKeys, keys.Key(k))
	}
	t.mu.Unlock()
	if len(writeKeys) == 0 {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	ts := commitTS
	if ts.IsEmpty() {
		ts = txn.WriteTimestamp
	}
	ba := &kvpb.BatchRequest{Header: kvpb.BatchHeader{Timestamp: t.db.clock.Now()}}
	for _, k := range writeKeys {
		ba.Add(&kvpb.ResolveIntentRequest{
			RequestHeader: kvpb.RequestHeader{Key: k},
			TxnID:         txn.ID,
			Status:        status,
			CommitTS:      ts,
		})
	}
	if _, kerr := t.db.Send(ctx, ba); kerr != nil {
		log.Debugf("intent resolution for %s: %v (lazy resolution will cover)", txn.ID, kerr)
	}
}

// Restart begins a new epoch at a fresh timestamp; accumulated intents are
// rewritten by the new epoch's writes and resolved at the end either way.
func (t *Txn) Restart(cause error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	now := t.db.clock.Now()
	var re *RetryableError
	if errors.As(cause, &re) {
		now = now.Forward(re.Cause.RetryTimestamp(now))
	}
	t.mu.txn.Restart(now)
	t.mu.finished = false
}

func (t *Txn) markFinished() {
	t.mu.Lock()
	t.mu.finished = true
	cancel := t.heartbeatCancel
	t.heartbeatCancel = nil
	t.mu.Unlock()
	if cancel != nil {
		cancel()
	}
}

// AbandonForTesting simulates a coordinator crash: heartbeats stop but the
// transaction is neither committed nor rolled back, leaving its intents and
// record behind for lazy cleanup by whoever trips over them.
func (t *Txn) AbandonForTesting() {
	t.mu.Lock()
	cancel := t.heartbeatCancel
	t.heartbeatCancel = nil
	t.mu.Unlock()
	if cancel != nil {
		cancel()
	}
}

// SetPriorityForTesting forces a conflict-priority, making push outcomes
// deterministic in tests.
func (t *Txn) SetPriorityForTesting(p int32) {
	t.mu.Lock()
	t.mu.txn.Priority = p
	t.mu.Unlock()
}

// startHeartbeat keeps the transaction record alive. If it stops (crash),
// the record expires and anyone who trips over our intents aborts us —
// that is the crashed-coordinator cleanup story.
func (t *Txn) startHeartbeat() {
	ctx, cancel := context.WithCancel(context.Background())
	t.mu.Lock()
	if t.mu.finished || t.heartbeatCancel != nil {
		t.mu.Unlock()
		cancel()
		return
	}
	t.heartbeatCancel = cancel
	t.mu.Unlock()

	go func() {
		ticker := time.NewTicker(1 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
			txn := t.proto()
			t.mu.Lock()
			waitingFor, waitingForKey := t.mu.waitingFor, t.mu.waitingForKey
			t.mu.Unlock()
			ba := &kvpb.BatchRequest{Header: kvpb.BatchHeader{Txn: txn}}
			ba.Add(&kvpb.HeartbeatTxnRequest{
				RequestHeader: kvpb.RequestHeader{Key: keys.Key(txn.Key).Clone()},
				Now:           t.db.clock.Now(),
				WaitingFor:    waitingFor,
				WaitingForKey: waitingForKey,
			})
			hctx, hcancel := context.WithTimeout(ctx, 3*time.Second)
			br, kerr := t.db.Send(hctx, ba)
			hcancel()
			if kerr != nil {
				log.Debugf("txn %s heartbeat: %v", txn.ID, kerr)
				continue
			}
			if br.Responses[0].HeartbeatTxn.Status == enginepb.ABORTED {
				return // a pusher won; the next operation will discover it
			}
		}
	}()
}

// RunTxn runs fn in a transaction, committing on success and transparently
// retrying (with a fresh timestamp and epoch) on serialization conflicts.
// This is the auto-retry loop implicit (single-statement) SQL uses; explicit
// SQL transactions instead surface RetryableError to the client as 40001.
func (db *DB) RunTxn(ctx context.Context, name string, fn func(ctx context.Context, txn *Txn) error) error {
	txn := db.NewTxn(name)
	txn.EnablePipelining()
	for attempt := 0; ; attempt++ {
		err := fn(ctx, txn)
		if err == nil {
			err = txn.Commit(ctx)
		}
		if err == nil {
			return nil
		}
		if !IsRetryable(err) || ctx.Err() != nil || attempt >= 20 {
			_ = txn.Rollback(ctx)
			return err
		}
		_ = txn.Rollback(ctx)
		next := db.NewTxn(name)
		next.EnablePipelining()
		next.mu.Lock()
		pri := txn.proto().Priority + 1 // push harder each retry
		next.mu.txn.Priority = pri
		next.mu.Unlock()
		txn = next
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Duration(attempt+1) * 20 * time.Millisecond):
		}
	}
}
