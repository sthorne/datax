package kvclient

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/kvpb"
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

// proto returns a snapshot of the transaction state.
func (t *Txn) proto() *kvpb.Transaction {
	t.mu.Lock()
	defer t.mu.Unlock()
	txn := t.mu.txn
	return &txn
}

// conflictBudget bounds how long an operation waits on a live conflicting
// transaction before surfacing a retryable error to the client.
const conflictBudget = 2 * time.Second

// Get reads a key at the transaction's read timestamp (seeing the
// transaction's own writes).
func (t *Txn) Get(ctx context.Context, key keys.Key) ([]byte, error) {
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
	waited := time.Duration(0)
	refreshes := 0
	for {
		br, kerr := t.db.Send(ctx, ba)
		if kerr == nil {
			return br, nil
		}
		switch {
		case kerr.WriteIntent != nil:
			resolvedAll, err := t.pushIntents(ctx, kerr.WriteIntent.Intents, isWrite)
			if err != nil {
				return nil, err
			}
			if resolvedAll {
				continue
			}
			if waited >= conflictBudget {
				return nil, &RetryableError{Cause: kerr}
			}
			select {
			case <-ctx.Done():
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
				ba.Header.Txn = t.proto() // re-stamp with refreshed timestamps
				continue
			}
			return nil, &RetryableError{Cause: kerr}
		default:
			return nil, kerr
		}
	}
}

// maxRefreshesPerOp bounds refresh loops under pathological contention.
const maxRefreshesPerOp = 10

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

// pushIntents pushes the owners of conflicting intents. Returns true if
// every intent was resolved (the pushees were finalized, expired, or
// aborted by priority) and the operation can be retried immediately.
func (t *Txn) pushIntents(ctx context.Context, intents []storage.Intent, pushAbort bool) (bool, error) {
	all := true
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
			return false, kerr
		}
		push := br.Responses[0].PushTxn
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
				return false, kerr
			}
		default:
			all = false // pushee alive; caller waits
		}
	}
	return all, nil
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
	anchored := t.mu.anchored
	txn := t.mu.txn
	t.mu.Unlock()

	if !anchored {
		// Read-only transaction: nothing to flip, nothing to resolve.
		t.markFinished()
		return nil
	}
	ba := &kvpb.BatchRequest{Header: kvpb.BatchHeader{Txn: &txn}}
	ba.Add(&kvpb.EndTxnRequest{RequestHeader: kvpb.RequestHeader{Key: keys.Key(txn.Key).Clone()}, Commit: true})
	br, err := t.send(ctx, ba, true)
	if err != nil {
		t.markFinished()
		return err
	}
	t.markFinished()
	t.resolveAll(enginepb.COMMITTED, br.Responses[0].EndTxn.CommitTimestamp)
	return nil
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
			ba := &kvpb.BatchRequest{Header: kvpb.BatchHeader{Txn: txn}}
			ba.Add(&kvpb.HeartbeatTxnRequest{
				RequestHeader: kvpb.RequestHeader{Key: keys.Key(txn.Key).Clone()},
				Now:           t.db.clock.Now(),
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
