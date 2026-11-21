package kvclient

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math/rand"
	"sort"
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
	"github.com/sthorne/datax/pkg/version"
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

// UpdateDeadline lowers the transaction's commit deadline to ts (a zero
// ts is ignored; a later ts than the current deadline leaves it alone).
// Historical (pinned-timestamp) transactions never commit writes and
// take no deadline.
func (t *Txn) UpdateDeadline(ts hlc.Timestamp) {
	if ts.IsEmpty() || t.historical {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.mu.deadline.IsEmpty() || ts.Less(t.mu.deadline) {
		t.mu.deadline = ts
	}
}

// Deadline returns the commit deadline, zero when none is set.
func (t *Txn) Deadline() hlc.Timestamp {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.mu.deadline
}

// checkDeadline refuses to send a committing batch whose transaction
// would commit at or past its deadline. The server commits at exactly
// the write timestamp the batch carries (EndTxn and the one-phase path
// both require it to equal the read timestamp, refreshing otherwise),
// so this client-side check is the authoritative one.
func (t *Txn) checkDeadline(ba *kvpb.BatchRequest) error {
	if !batchCommits(ba) {
		return nil
	}
	t.mu.Lock()
	deadline := t.mu.deadline
	ts := t.mu.txn.WriteTimestamp
	t.mu.Unlock()
	if ba.Header.Txn != nil {
		ts = ts.Forward(ba.Header.Txn.WriteTimestamp)
	}
	if deadline.IsEmpty() || ts.Less(deadline) {
		return nil
	}
	e := kvpb.NewErrorf("transaction cannot commit at %s: past its deadline %s (a schema lease it planned under has expired)", ts, deadline)
	e.TxnRetry = &kvpb.TxnRetryError{RetryTimestamp: ts}
	return &RetryableError{Cause: e}
}

// batchCommits reports whether ba carries a committing EndTxn.
func batchCommits(ba *kvpb.BatchRequest) bool {
	for _, u := range ba.Requests {
		if u.EndTxn != nil && u.EndTxn.Commit {
			return true
		}
	}
	return false
}

// onlyCPutConflicts reports whether every intent ba ran into lies on a
// key one of its conditional puts writes. Such a conflict is the read the
// conditional put replaces (an INSERT's uniqueness check), and a read
// waits for a live owner rather than aborting it: pushing with the power
// to abort would let an INSERT kill, by priority alone, a transaction
// that merely holds an uncommitted row with the same key, where the read
// it stands for would have waited for that transaction to finish.
func onlyCPutConflicts(ba *kvpb.BatchRequest, intents []storage.Intent) bool {
	if len(intents) == 0 {
		return false
	}
	for _, in := range intents {
		found := false
		for i := range ba.Requests {
			if c := ba.Requests[i].CPut; c != nil && c.Key.Equal(in.Key) {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	return true
}

// SnapshotTooOldError is a historical transaction's read refused by a
// range whose GC threshold has passed the transaction's timestamp. The
// timestamp is the client's (AS OF SYSTEM TIME, an imported snapshot),
// and the history at it is gone: no restart can help. It is the only
// GC-threshold refusal that is not a restart (issue #374).
type SnapshotTooOldError struct {
	Cause *kvpb.Error
}

func (e *SnapshotTooOldError) Error() string { return e.Cause.Message }

func (e *SnapshotTooOldError) Unwrap() error { return e.Cause }

// IsRetryable reports whether err calls for a transaction restart: a
// RetryableError, a write shed under storage backpressure, or a
// GC-threshold refusal that is not a historical
// read's SnapshotTooOldError. The transaction coordinator turns the
// refusals of its own reads and writes into RetryableErrors, but a
// refusal can reach a statement by other ways — a push or an intent
// resolution of a peer older than the exemption in kvserver, a
// non-transactional read at a timestamp the cluster chose (a backfill's
// boundary) — and wherever the timestamp was the cluster's, running
// again at a new one succeeds (issue #374). Counting them here makes
// every retry loop that consults IsRetryable (implicit statements,
// streamed results, COPY) retry them, and SQL map them to 40001.
func IsRetryable(err error) bool {
	var re *RetryableError
	if errors.As(err, &re) {
		return true
	}
	var stoo *SnapshotTooOldError
	if errors.As(err, &stoo) {
		return false
	}
	var kerr *kvpb.Error
	// A write shed under storage backpressure is retryable however it
	// arrives: a non-transactional write's shed is not wrapped in a
	// RetryableError, and it too must reach the client as 40001 rather
	// than XX000 (issue #391).
	return errors.As(err, &kerr) && (kerr.IsBelowGCThreshold() || kerr.StorageOverloaded != nil)
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
		// condensed holds the reads that overflowed readSpans, merged
		// into at most maxCondensedSpans spans covering them (and the
		// keys between them): refresh verifies these too. Covering more
		// than was read can only fail a refresh that would have passed,
		// never pass one that should fail. Marks and savepoints index
		// readSpans alone; condensed is never forgotten.
		condensed []readSpan
		// refreshUnusable: too many spans (or tracking disabled) — fall
		// back to v1's restart behavior.
		refreshUnusable bool
		// deferred is the pipelined write batch awaiting Commit (see
		// pipelining above).
		deferred *WriteBatch
		// savepoints by name; spOrder hands out creation order so RELEASE
		// and ROLLBACK TO can discard later savepoints (PG semantics).
		savepoints map[string]*savepoint
		// deadline, when set, bounds the commit timestamp: the transaction
		// may not commit at or above it. The SQL layer pins it to the
		// expiration of every descriptor lease the transaction planned
		// against, so a statement that planned under a lease the DDL
		// drain has since written off cannot commit past the drain's
		// boundary (issue #110). Cleared by Restart: a new epoch
		// re-plans and re-pins.
		deadline hlc.Timestamp
		spOrder  int
		// waitingFor/waitingForKey: the transaction this one is currently
		// blocked on in a push loop (Nil = none). Published on the record
		// (immediately on change, and with every heartbeat) so pushers can
		// walk wait chains and detect deadlock cycles.
		waitingFor    uuid.UUID
		waitingForKey keys.Key
		// aborted: the coordinator has learned that its transaction was
		// aborted by another (a heartbeat found the record ABORTED, or a
		// request failed with a TxnAbortedError). Every later request
		// fails at once (issue #320): the record can never commit, and a
		// pusher may already have removed some of the transaction's
		// writes, so whatever it read now could miss them.
		aborted bool
		// abortCleaned: the transaction's own intents have been (or are
		// being) resolved as aborted after it gave up, which also removes
		// the abort-span entries pushers left for it.
		abortCleaned bool
	}
	heartbeatCancel context.CancelFunc
}

type readSpan struct {
	start, end keys.Key // end nil = point read
}

// maxReadSpans bounds the read spans kept as they were read. Beyond it
// they are condensed (maxCondensedSpans), so that a transaction that
// reads many keys — DROP DATABASE of a database of hundreds of tables
// reads each one's name and descriptor — can still refresh when its
// timestamp is pushed rather than restart (issue #322); before, it gave
// up its read set and every push ended in a restart, which an implicit
// transaction retried until it gave up with 40001.
//
// It is sized for a bulk INSERT's uniqueness probes: a statement reads
// one key per row (more with unique indexes and foreign keys) and writes
// rows next to other writers' rows. Condensed, its reads of random keys
// become spans covering most of the table, which fail to refresh once
// any other worker has committed a row there — every push became a
// restart — and which latch, and push through the timestamp cache,
// every concurrent writer of the table. At 512, INSERTs of 1,000 rows
// under 16 workers spent 1.5 to 3 s per second waiting on latches and
// ran at less than half the rows/s of 100-row INSERTs.
const maxReadSpans = 4096

// maxCondensedSpans bounds the condensed read spans, and so the refresh
// requests a transaction's commit can need.
const maxCondensedSpans = 64

func (t *Txn) recordRead(start, end keys.Key) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.mu.refreshUnusable {
		return
	}
	if len(t.mu.readSpans) >= maxReadSpans {
		t.mu.condensed = condenseSpans(append(t.mu.condensed, t.mu.readSpans...), maxCondensedSpans)
		t.mu.readSpans = nil
	}
	t.mu.readSpans = append(t.mu.readSpans, readSpan{start: start.Clone(), end: end.Clone()})
}

// condenseSpans merges spans into at most max spans that cover them:
// overlapping and touching spans first, then the neighbours in key order
// that are closest — whose keys share the longest prefix — so a merged
// span stays inside the keyspace its reads were in (one table's rows,
// the descriptors) rather than reaching across another that others
// write all the time (the descriptor leases).
func condenseSpans(spans []readSpan, max int) []readSpan {
	norm := make([]readSpan, 0, len(spans))
	for _, sp := range spans {
		end := sp.end
		if len(end) == 0 {
			end = sp.start.Next()
		}
		norm = append(norm, readSpan{start: sp.start, end: end})
	}
	sort.Slice(norm, func(i, j int) bool { return norm[i].start.Less(norm[j].start) })
	var out []readSpan
	for _, sp := range norm {
		if n := len(out); n > 0 && !out[n-1].end.Less(sp.start) {
			if out[n-1].end.Less(sp.end) {
				out[n-1].end = sp.end
			}
			continue
		}
		out = append(out, sp)
	}
	if len(out) <= max {
		return out
	}
	// Close the len(out)-max gaps whose sides share the longest prefix.
	gaps := make([]int, len(out)-1)
	shared := make([]int, len(out)-1)
	for i := range gaps {
		gaps[i] = i
		a, b := out[i].end, out[i+1].start
		for shared[i] < len(a) && shared[i] < len(b) && a[shared[i]] == b[shared[i]] {
			shared[i]++
		}
	}
	sort.SliceStable(gaps, func(x, y int) bool { return shared[gaps[x]] > shared[gaps[y]] })
	closed := make([]bool, len(out)-1)
	for _, g := range gaps[:len(out)-max] {
		closed[g] = true
	}
	merged := out[:0:0]
	for i, sp := range out {
		if i > 0 && closed[i-1] {
			merged[len(merged)-1].end = sp.end
			continue
		}
		merged = append(merged, sp)
	}
	return merged
}

// NewTxn begins a transaction at the current clock reading.
func (db *DB) NewTxn(name string) *Txn {
	t := &Txn{db: db}
	t.mu.txn = kvpb.MakeTransaction(name, rand.Int31n(1<<20), db.clock.Now())
	t.mu.txn.HistoryFloor = -1 // no savepoint yet: rewrites keep no history (issue #162)
	// From cluster version v14 the intents and the record are stored in
	// the binary encoding (issue #141); the flag rides in every command.
	t.mu.txn.BinaryMeta = db.ClusterVersion() >= version.V14
	// t.mu.writes is made by the first write (recordWrite): a read-only
	// transaction never needs it.
	return t
}

// updateHistoryFloor recomputes TxnMeta.HistoryFloor from the live
// savepoints (issue #162): the servers keep, on a rewrite of one of the
// transaction's intents, only the superseded values a rollback to a live
// savepoint could restore. Called with mu held whenever the savepoint
// set changes.
func (t *Txn) updateHistoryFloor() {
	floor := int32(-1)
	for _, sp := range t.mu.savepoints {
		if floor < 0 || sp.seq+1 < floor {
			floor = sp.seq + 1
		}
	}
	t.mu.txn.HistoryFloor = floor
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

// ReadTimestamp is the timestamp the transaction currently reads at. A
// transaction commits at or above it.
func (t *Txn) ReadTimestamp() hlc.Timestamp {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.mu.txn.ReadTimestamp
}

// ClusterVersion is the finalized cluster version as the transaction's
// DB observes it (DB.ClusterVersion), for the version-gated decisions
// of code that holds a transaction and nothing else.
func (t *Txn) ClusterVersion() version.Version { return t.db.ClusterVersion() }

// Historical reports whether this is a read-only transaction pinned at a
// fixed past timestamp (NewHistoricalTxn). Catalog lookups key off it:
// a historical transaction reads the descriptor version current AT its
// timestamp, never a cached or leased current one.
func (t *Txn) Historical() bool { return t.historical }

// BumpPriority makes the transaction push harder than prev did: the
// retry loops give each attempt one more point of conflict priority.
func (t *Txn) BumpPriority(prev *Txn) {
	pri := prev.proto().Priority + 1
	t.mu.Lock()
	t.mu.txn.Priority = pri
	t.mu.Unlock()
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
		return t.finishedErrLocked()
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
	t.updateHistoryFloor()
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
	t.updateHistoryFloor()
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
		err := t.finishedErrLocked()
		t.mu.Unlock()
		return err
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
	t.updateHistoryFloor()
	return nil
}

// proto returns a snapshot of the transaction state.
func (t *Txn) proto() *kvpb.Transaction {
	t.mu.Lock()
	defer t.mu.Unlock()
	txn := t.mu.txn
	return &txn
}

// LockTimeoutError is the wait on a live conflicting transaction
// exceeding the lock timeout the context carries (WithLockTimeout).
type LockTimeoutError struct {
	Waited time.Duration
}

func (e *LockTimeoutError) Error() string {
	return fmt.Sprintf("lock timeout after %s waiting on a conflicting transaction", e.Waited)
}

// IsLockTimeout reports whether err is (or wraps) a LockTimeoutError.
func IsLockTimeout(err error) bool {
	var lt *LockTimeoutError
	return errors.As(err, &lt)
}

type lockTimeoutKey struct{}

// WithLockTimeout bounds how long operations under ctx wait on a live
// conflicting intent before failing with a LockTimeoutError (0 = the
// default budget only).
func WithLockTimeout(ctx context.Context, d time.Duration) context.Context {
	if d <= 0 {
		return ctx
	}
	return context.WithValue(ctx, lockTimeoutKey{}, d)
}

// LockTimeoutFrom returns the lock timeout ctx carries (0 = none): for
// a wait outside the KV layer (an advisory lock) to honour lock_timeout
// as a wait on an intent does.
func LockTimeoutFrom(ctx context.Context) time.Duration { return lockTimeoutFrom(ctx) }

func lockTimeoutFrom(ctx context.Context) time.Duration {
	d, _ := ctx.Value(lockTimeoutKey{}).(time.Duration)
	return d
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
	wait := LockWaitFrom(ctx)
	ba := &kvpb.BatchRequest{Header: kvpb.BatchHeader{Txn: t.proto()}}
	ba.Add(&kvpb.GetRequest{RequestHeader: kvpb.RequestHeader{Key: key}, SkipLocked: wait == LockWaitSkip})
	br, err := t.sendWith(ctx, ba, false, sendOpts{noWait: wait == LockWaitError})
	if err != nil {
		return nil, err
	}
	resp := br.Responses[0].Get
	if resp.Skipped != nil {
		// Passed over, not read: the key stays out of the read set.
		t.db.resolveAbandonedLocks(ctx, []storage.Intent{*resp.Skipped})
		return nil, nil
	}
	t.recordRead(key, nil)
	return resp.Value, nil
}

// GetBatch reads many keys in ONE routed batch — the per-range sub-batches
// fan out in parallel — returning values positionally (nil = absent).
// Vastly cheaper than N sequential Gets when the keys are known up front
// (COPY's per-chunk uniqueness probes).
func (t *Txn) GetBatch(ctx context.Context, ks []keys.Key) ([][]byte, error) {
	if len(ks) == 0 {
		return nil, nil
	}
	if err := t.flushDeferred(ctx); err != nil {
		return nil, err
	}
	wait := LockWaitFrom(ctx)
	ba := &kvpb.BatchRequest{Header: kvpb.BatchHeader{Txn: t.proto()}}
	for _, k := range ks {
		ba.Add(&kvpb.GetRequest{RequestHeader: kvpb.RequestHeader{Key: k.Clone()}, SkipLocked: wait == LockWaitSkip})
	}
	br, err := t.sendWith(ctx, ba, false, sendOpts{noWait: wait == LockWaitError})
	if err != nil {
		return nil, err
	}
	vals := make([][]byte, len(ks))
	var skipped []storage.Intent
	for i, k := range ks {
		resp := br.Responses[i].Get
		if resp.Skipped != nil {
			skipped = append(skipped, *resp.Skipped)
			continue
		}
		t.recordRead(k, nil)
		vals[i] = resp.Value
	}
	t.db.resolveAbandonedLocks(ctx, skipped)
	return vals, nil
}

// Scan reads [start, end) at the transaction's read timestamp.
func (t *Txn) Scan(ctx context.Context, start, end keys.Key, max int64) ([]kvpb.KeyValue, error) {
	if err := t.flushDeferred(ctx); err != nil {
		return nil, err
	}
	wait := LockWaitFrom(ctx)
	ba := &kvpb.BatchRequest{Header: kvpb.BatchHeader{Txn: t.proto()}}
	ba.Add(&kvpb.ScanRequest{RequestHeader: kvpb.RequestHeader{Key: start, EndKey: end}, MaxRows: max, SkipLocked: wait == LockWaitSkip})
	br, err := t.sendWith(ctx, ba, false, sendOpts{noWait: wait == LockWaitError})
	if err != nil {
		return nil, err
	}
	resp := br.Responses[0].Scan
	observedEnd := end
	if len(resp.Resume) > 0 {
		observedEnd = resp.Resume // only [start, resume) was observed
	}
	t.recordScan(ctx, start, observedEnd, resp.Skipped)
	return resp.Rows, nil
}

// ReverseScan reads [start, end) BACKWARDS (largest key first) at the
// transaction's read timestamp. When max stops the scan early, the next
// page is [start, resume) — resume is the exclusive end. Refresh-wise the
// observed span is [resume-or-start, end). Callers must consult the DB's
// version gate before using reverse scans (pkg/version rule 4: a v2 node
// would silently run a forward scan).
func (t *Txn) ReverseScan(ctx context.Context, start, end keys.Key, max int64) ([]kvpb.KeyValue, error) {
	if err := t.flushDeferred(ctx); err != nil {
		return nil, err
	}
	wait := LockWaitFrom(ctx)
	ba := &kvpb.BatchRequest{Header: kvpb.BatchHeader{Txn: t.proto()}}
	ba.Add(&kvpb.ScanRequest{RequestHeader: kvpb.RequestHeader{Key: start, EndKey: end}, MaxRows: max, Reverse: true, SkipLocked: wait == LockWaitSkip})
	br, err := t.sendWith(ctx, ba, false, sendOpts{noWait: wait == LockWaitError})
	if err != nil {
		return nil, err
	}
	resp := br.Responses[0].Scan
	observedStart := start
	if len(resp.Resume) > 0 {
		observedStart = resp.Resume // only [resume, end) was observed
	}
	t.recordScan(ctx, observedStart, end, resp.Skipped)
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

// GetBatchForUpdate is GetForUpdate over many keys in ONE routed batch
// (the per-range sub-batches fan out in parallel): every key is read at
// the transaction's read timestamp and locked with an intent, values
// come back positionally (nil = absent, pinned by a tombstone intent).
// SELECT ... FOR UPDATE over a scanned result locks its rows this way.
func (t *Txn) GetBatchForUpdate(ctx context.Context, ks []keys.Key) ([][]byte, error) {
	if len(ks) == 0 {
		return nil, nil
	}
	if t.historical {
		return nil, fmt.Errorf("cannot lock in a read-only historical transaction")
	}
	if err := t.flushDeferred(ctx); err != nil {
		return nil, err
	}
	t.mu.Lock()
	t.mu.txn.Sequence++
	createRecord := false
	if !t.mu.anchored {
		if len(t.mu.txn.Key) == 0 {
			t.mu.txn.Key = ks[0].Clone()
		}
		for _, k := range ks {
			if keys.Key(t.mu.txn.Key).Equal(k) {
				createRecord = true
				break
			}
		}
	}
	txn := t.mu.txn
	t.mu.Unlock()
	ba := &kvpb.BatchRequest{Header: kvpb.BatchHeader{Txn: &txn, CreateTxnRecord: createRecord}}
	for _, k := range ks {
		ba.Add(&kvpb.GetRequest{RequestHeader: kvpb.RequestHeader{Key: k.Clone()}, ForUpdate: true})
	}
	br, err := t.send(ctx, ba, true)
	if err != nil {
		return nil, err
	}
	vals := make([][]byte, len(ks))
	for i, k := range ks {
		t.recordRead(k, nil)
		t.recordWrite(k)
		vals[i] = br.Responses[i].Get.Value
	}
	return vals, nil
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

// HasWrites reports whether the transaction has written anything (in
// any epoch): a cheap test before HasWritten.
func (t *Txn) HasWrites() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return len(t.mu.writes) > 0
}

// HasWritten reports whether the transaction has written key, in any
// epoch and not rolled back to a savepoint since: a key whose readers
// must read through the transaction rather than from a cache.
func (t *Txn) HasWritten(key keys.Key) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	_, ok := t.mu.writes[string(key)]
	return ok
}

// WrittenIn returns the keys in [start, end) the transaction has written
// (as HasWritten counts them), in no particular order.
func (t *Txn) WrittenIn(start, end keys.Key) []keys.Key {
	t.mu.Lock()
	defer t.mu.Unlock()
	var out []keys.Key
	for k := range t.mu.writes {
		if key := keys.Key(k); key.Compare(start) >= 0 && key.Compare(end) < 0 {
			out = append(out, key.Clone())
		}
	}
	return out
}

func (t *Txn) recordWrite(key keys.Key) {
	t.mu.Lock()
	if t.mu.writes == nil {
		t.mu.writes = make(map[string]struct{})
	}
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
	return t.sendWith(ctx, ba, isWrite, sendOpts{})
}

// sendOpts changes how send treats the two conflicts a locking read under
// NOWAIT or SKIP LOCKED must not handle the ordinary way.
type sendOpts struct {
	// noWait fails the batch with a LockNotAvailableError naming the
	// intents of live transactions instead of waiting for them. Their
	// owners are still pushed — that is how an abandoned intent is told
	// from a held lock, and resolved — but never with the power to abort:
	// declining to wait for a lock is no reason to take it by force.
	noWait bool
	// noRefresh returns a stale-snapshot conflict (a version committed
	// above the read timestamp) to the caller as it is, without the
	// refresh that would move the whole transaction forward or fail it:
	// SKIP LOCKED passes such a row over.
	noRefresh bool
}

func (t *Txn) sendWith(ctx context.Context, ba *kvpb.BatchRequest, isWrite bool, opts sendOpts) (*kvpb.BatchResponse, error) {
	if t.historical {
		// Pinned-timestamp reads are follower-servable and need no
		// uncertainty interval.
		ba.Header.StaleRead = true
	}
	var cw conflictWait
	refreshes := 0
	for attempt := 1; ; attempt++ {
		if err := t.failIfAborted(); err != nil {
			t.publishWait(ctx, nil)
			return nil, err
		}
		if err := t.checkDeadline(ba); err != nil {
			t.publishWait(ctx, nil)
			return nil, err
		}
		br, kerr := t.db.Send(ctx, ba)
		if kerr != nil && attempt%20 == 0 {
			log.Debugf("txn %s: batch of %d still failing after %d attempts (%s waited on live conflicts): %v", t.mu.txn.ID, len(ba.Requests), attempt, cw.waited(), kerr)
		}
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
			// Watch for the owners finishing before pushing them, so a
			// finish between the push and the wait still wakes us.
			watch := t.db.waits.watch(kerr.WriteIntent.Intents)
			live, err := t.pushIntents(ctx, kerr.WriteIntent.Intents, isWrite && !opts.noWait && !onlyCPutConflicts(ba, kerr.WriteIntent.Intents))
			if err != nil {
				watch.release()
				t.publishWait(ctx, nil)
				return nil, err
			}
			if len(live) == 0 {
				watch.release()
				t.publishWait(ctx, nil)
				continue
			}
			if opts.noWait {
				watch.release()
				return nil, &LockNotAvailableError{Intents: live}
			}
			if rerr := t.cannotOutwait(live); rerr != nil {
				watch.release()
				t.publishWait(ctx, nil)
				return nil, rerr
			}
			pending := &live[0]
			// We are blocked on a live transaction: publish the wait edge
			// and walk the chain for a cycle before backing off.
			if cw.dueLog() {
				log.Debugf("txn %s: blocked %s on live transaction %s at %s", t.mu.txn.ID, cw.waited(), pending.Txn.ID, pending.Key)
			}
			t.publishWait(ctx, &pending.Txn)
			if derr := t.detectDeadlock(ctx, pending); derr != nil {
				watch.release()
				return nil, derr // self chosen as deadlock victim
			}
			waited := cw.waited()
			lt := lockTimeoutFrom(ctx)
			if lt > 0 && waited >= lt {
				watch.release()
				t.publishWait(ctx, nil)
				return nil, &LockTimeoutError{Waited: waited}
			}
			if waited >= conflictBudget {
				watch.release()
				t.publishWait(ctx, nil)
				return nil, &RetryableError{Cause: kerr}
			}
			if err := watch.wait(ctx, pending.Txn.ID, cw.next(pending.Txn.ID, lt)); err != nil {
				t.publishWait(ctx, nil)
				return nil, err
			}
		case kerr.TxnAborted != nil:
			t.publishWait(ctx, nil)
			if !batchCommits(ba) {
				// A failed commit settles itself (abortFailedCommit).
				t.giveUpAborted()
			}
			return nil, &RetryableError{Cause: kerr}
		case kerr.Ambiguous != nil && !batchHasEndTxn(ba):
			// The proposal's fate is unknown: leadership was interrupted
			// with it in flight. Re-sending a transactional batch that
			// carries no EndTxn is safe — a write lays down this
			// transaction's intent again (a rewrite by the same transaction
			// supersedes the earlier one; the value is the same), and reads
			// are idempotent — so retry in place rather than surface an
			// error the statement cannot act on. An EndTxn is not: its
			// fate must be read from the record, which parallelCommit's
			// recovery does. Issue #74.
			metrics.TxnAmbiguousResends.Inc()
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(ambiguousResendDelay):
			}
			continue
		case kerr.StorageOverloaded != nil:
			// Shed under storage backpressure: sendToRange already backed
			// off for the overload budget. A refresh would only resend the
			// batch and wait the budget again (issue #391).
			metrics.TxnRetries.Inc()
			return nil, &RetryableError{Cause: kerr}
		case kerr.IsRetryableTxnError():
			// Timestamp conflict (WriteTooOld / tsCache push / uncertainty
			// / commit equality): try to move the read timestamp forward by
			// proving our read spans saw no writes in the window, instead
			// of restarting.
			if opts.noRefresh && (kerr.WriteTooOld != nil || kerr.Uncertainty != nil) {
				return nil, &RetryableError{Cause: kerr}
			}
			newTS := kerr.RetryTimestamp(t.db.clock.Now())
			if refreshes < maxRefreshesPerOp && t.maybeRefresh(ctx, newTS) {
				refreshes++
				metrics.TxnRefreshes.Inc()
				ba.Header.Txn = t.proto() // re-stamp with refreshed timestamps
				continue
			}
			metrics.TxnRetries.Inc()
			return nil, &RetryableError{Cause: kerr}
		case t.restartsBelowGCThreshold(kerr):
			metrics.TxnRetries.Inc()
			return nil, &RetryableError{Cause: kerr}
		case kerr.IsBelowGCThreshold():
			return nil, &SnapshotTooOldError{Cause: kerr}
		default:
			return nil, kerr
		}
	}
}

// restartsBelowGCThreshold reports whether kerr is a range refusing the
// transaction's timestamp as at or below its GC threshold, and the
// transaction is one a restart can help (issue #313). A transaction the
// cluster timestamped — one that has run longer than a short gc_ttl, or
// began before a purge of what it reads — succeeds when run again at a
// new timestamp, so the refusal is a restart like any other conflict. A
// historical transaction's timestamp is the client's, and the history at
// it is gone: no restart can help, and the refusal stands.
func (t *Txn) restartsBelowGCThreshold(kerr *kvpb.Error) bool {
	return !t.historical && kerr.IsBelowGCThreshold()
}

// maxRefreshesPerOp bounds refresh loops under pathological contention.
const maxRefreshesPerOp = 10

// ambiguousResendDelay is the pause before a batch whose proposal was
// interrupted by a leadership change is re-sent — long enough for the
// range to settle on a leader.
const ambiguousResendDelay = 50 * time.Millisecond

// batchHasEndTxn reports whether the batch finalizes the transaction.
func batchHasEndTxn(ba *kvpb.BatchRequest) bool {
	for _, u := range ba.Requests {
		if u.EndTxn != nil {
			return true
		}
	}
	return false
}

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
	spans := append(append([]readSpan(nil), t.mu.condensed...), t.mu.readSpans...)
	provisional := t.mu.txn
	t.mu.Unlock()

	if newTS.LessEq(oldRead) {
		return true // nothing to move
	}
	provisional.ReadTimestamp = newTS
	provisional.WriteTimestamp = newTS
	for _, ba := range refreshBatches(spans, &provisional, oldRead) {
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

// refreshBatches builds the requests that refresh spans from oldRead to
// txn's (provisional) read timestamp: every point span in ONE batch, which
// the DB fans out per range like any batch of point requests, and the
// ranged spans in another, each stitched across ranges on its own. A
// refresh used to send one batch per span, one after another: a pushed
// 1,000-row INSERT, whose uniqueness probes are 1,000 point reads, paid
// 1,000 sequential round trips and latch acquisitions before it could
// commit, and kept its probes' keys latched (shared) the whole time.
func refreshBatches(spans []readSpan, txn *kvpb.Transaction, oldRead hlc.Timestamp) []*kvpb.BatchRequest {
	var points, ranged *kvpb.BatchRequest
	for _, sp := range spans {
		b := &points
		if len(sp.end) > 0 {
			b = &ranged
		}
		if *b == nil {
			*b = &kvpb.BatchRequest{Header: kvpb.BatchHeader{Txn: txn.Clone()}}
		}
		(*b).Add(&kvpb.RefreshRequest{
			RequestHeader: kvpb.RequestHeader{Key: sp.start, EndKey: sp.end},
			FromTS:        oldRead,
		})
	}
	var out []*kvpb.BatchRequest
	for _, b := range []*kvpb.BatchRequest{points, ranged} {
		if b != nil {
			out = append(out, b)
		}
	}
	return out
}

// AdvanceTo moves the transaction forward so that it commits at or above
// ts, by the same proof a timestamp conflict takes: every span it has
// read saw no foreign write up to ts. A transaction whose commit must
// not precede writes made outside it uses this — a large object's pages
// are written outside the transaction that creates the object, and the
// row that makes the object visible must not carry a timestamp below
// theirs (pkg/sql/largeobject.go). When the proof fails the transaction
// cannot commit and the error is retryable.
func (t *Txn) AdvanceTo(ctx context.Context, ts hlc.Timestamp) error {
	if t.historical {
		return fmt.Errorf("cannot advance a read-only historical transaction")
	}
	if err := t.flushDeferred(ctx); err != nil {
		return err
	}
	if !t.maybeRefresh(ctx, ts) {
		metrics.TxnRetries.Inc()
		return &RetryableError{Cause: &kvpb.Error{Message: fmt.Sprintf("transaction could not be moved forward to %s: something it read has changed since (or it has read too much to tell)", ts)}}
	}
	return nil
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
	hb := txn // the snapshot sent; txn stays on the stack on the early return
	ba := &kvpb.BatchRequest{Header: kvpb.BatchHeader{Txn: &hb}}
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
		t.giveUpAborted()
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
	} else {
		t.db.waits.finished(victim.id)
	}
	return nil
}

// cannotOutwait returns a retryable error when waiting for the live
// intents a batch ran into cannot end in this transaction committing,
// and nil when it can (the caller then waits as usual).
//
// An intent above the read timestamp on a key the transaction has read
// is such a wait (issue #315). If the intent's transaction commits, its
// version lands above our read timestamp inside a span we read. We were
// blocked on it either to write the key — the write then goes above that
// version (WriteTooOld) — or to read it, the intent being inside our
// uncertainty window — the read then finds the version uncertain. Either
// way our timestamp must move past the version, and the refresh that
// moves it re-reads the span, finds the version and fails. What the wait
// could buy is the holder aborting, or, for a read, the holder being
// pushed past our uncertainty limit before it commits (the read then
// passes beneath its version); a pusher able to abort the holder has
// already done so, since these intents survived the push. Both are rare,
// and a restart is always safe: returning now gives them up for the wait
// we would otherwise spend learning that the holder committed. On the
// bank workload a transfer that met a newer transfer of an account it
// had read waited a poll (100 ms) or more and then failed its read of
// the account as uncertain, nearly every time.
//
// With the read set no longer tracked the same holds for any intent
// above the read timestamp: every timestamp push then ends in a restart.
func (t *Txn) cannotOutwait(live []storage.Intent) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	readTS := t.mu.txn.ReadTimestamp
	for i := range live {
		in := &live[i]
		if !readTS.Less(in.Txn.WriteTimestamp) {
			continue
		}
		if !t.mu.refreshUnusable && !readSpansCover(t.mu.readSpans, in.Key) && !readSpansCover(t.mu.condensed, in.Key) {
			continue
		}
		e := kvpb.NewErrorf("transaction %s must restart: transaction %s is writing key %s at %s, above the read timestamp %s at which it was read",
			t.mu.txn.ID, in.Txn.ID, in.Key, in.Txn.WriteTimestamp, readTS)
		e.TxnRetry = &kvpb.TxnRetryError{RetryTimestamp: in.Txn.WriteTimestamp}
		metrics.TxnRetries.Inc()
		return &RetryableError{Cause: e}
	}
	return nil
}

// readSpansCover reports whether key lies in one of spans.
func readSpansCover(spans []readSpan, key keys.Key) bool {
	for _, sp := range spans {
		if len(sp.end) == 0 {
			if sp.start.Equal(key) {
				return true
			}
		} else if !key.Less(sp.start) && key.Less(sp.end) {
			return true
		}
	}
	return false
}

// pushIntents pushes the owners of conflicting intents and returns the
// intents whose transactions are still live. None means every intent was
// resolved (the pushees were finalized, expired, or aborted by priority)
// and the operation can be retried immediately.
//
// Each conflicting transaction is pushed once, however many of its
// intents the batch ran into, and the intents of every finalized pushee
// are resolved in one batch (the sender splits it by range). A batch
// that overlaps hundreds of intents left by one dead transaction — a
// re-shard backfill chunk retried after its first attempt was aborted —
// then costs one push and one resolve round, not one of each per key
// (issue #74).
func (t *Txn) pushIntents(ctx context.Context, intents []storage.Intent, pushAbort bool) ([]storage.Intent, error) {
	var live []storage.Intent
	type verdict struct {
		status   enginepb.TxnStatus
		commitTS hlc.Timestamp
		live     bool
	}
	verdicts := map[uuid.UUID]verdict{}
	rba := &kvpb.BatchRequest{Header: kvpb.BatchHeader{Timestamp: t.db.clock.Now()}}
	for _, intent := range intents {
		v, pushed := verdicts[intent.Txn.ID]
		if !pushed {
			ba := &kvpb.BatchRequest{Header: kvpb.BatchHeader{Timestamp: t.db.clock.Now()}}
			ba.Add(&kvpb.PushTxnRequest{
				RequestHeader: kvpb.RequestHeader{Key: keys.Key(intent.Txn.Key).Clone()},
				PusherTxn:     t.proto(),
				PusheeTxn:     intent.Txn,
				PushAbort:     pushAbort,
				Now:           t.db.clock.Now(),
			})
			metrics.TxnPushes.Inc()
			br, kerr := t.db.Send(ctx, ba)
			if kerr != nil {
				return nil, kerr
			}
			push := br.Responses[0].PushTxn
			if push.Status == enginepb.STAGING {
				// A parallel commit in flight: run status recovery, then let
				// the next push observe the finalized record.
				t.db.recoverStagedTxn(ctx, intent.Txn, push)
				metrics.TxnPushes.Inc()
				br, kerr = t.db.Send(ctx, ba)
				if kerr != nil {
					return nil, kerr
				}
				push = br.Responses[0].PushTxn
			}
			v = verdict{status: push.Status, commitTS: push.CommitTS}
			v.live = push.Status != enginepb.COMMITTED && push.Status != enginepb.ABORTED
			verdicts[intent.Txn.ID] = v
		}
		if v.live {
			live = append(live, intent) // pushee alive; caller waits
			continue
		}
		rba.Add(&kvpb.ResolveIntentRequest{
			RequestHeader: kvpb.RequestHeader{Key: intent.Key.Clone()},
			TxnID:         intent.Txn.ID,
			Status:        v.status,
			CommitTS:      v.commitTS,
			Poison:        t.db.poisonsAborted(v.status),
		})
	}
	if len(rba.Requests) > 0 {
		if _, kerr := t.db.Send(ctx, rba); kerr != nil {
			return nil, kerr
		}
		// Others on this node blocked on a pushee found final here need
		// not wait out their backoff to learn it.
		for id, v := range verdicts {
			if !v.live {
				t.db.waits.finished(id)
			}
		}
	}
	return live, nil
}

// Commit atomically commits the transaction: one replicated flip of its
// record, then best-effort intent resolution (lazy resolution by future
// readers covers any failure here).
func (t *Txn) Commit(ctx context.Context) error {
	t.mu.Lock()
	if t.mu.finished {
		err := t.finishedErrLocked()
		t.mu.Unlock()
		return err
	}
	wb := t.mu.deferred
	t.mu.deferred = nil
	t.mu.Unlock()
	if wb != nil && wb.Len() > 0 {
		if handled, err := t.commit1PC(ctx, wb); handled {
			return err
		}
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
	end := txn // the snapshot sent; txn stays on the stack for a read-only commit
	ba := &kvpb.BatchRequest{Header: kvpb.BatchHeader{Txn: &end}}
	ba.Add(&kvpb.EndTxnRequest{
		RequestHeader: kvpb.RequestHeader{Key: keys.Key(txn.Key).Clone()},
		Commit:        true,
		IntentKeys:    intentKeys,
	})
	br, err := t.send(ctx, ba, true)
	if err != nil {
		t.abortFailedCommit(ctx)
		return err
	}
	t.markFinished()
	metrics.TxnCommits.Inc()
	t.resolveAll(enginepb.COMMITTED, br.Responses[0].EndTxn.CommitTimestamp)
	t.wakeWaiters()
	return nil
}

// abortFailedCommit settles a commit that returned an error. Unless the
// error was ambiguous the record is still PENDING (or STAGING), and the
// transaction's intents still block every reader and writer of their
// keys. The SQL session forgets the transaction on a failed COMMIT, and
// a Rollback after it finds the transaction finished, so left alone the
// intents would stay until the record expired — the heartbeat stops
// with the transaction — and everyone queued behind them would wait out
// kvserver.TxnExpiration (5s) for a transaction that had already given
// up (issue #315). So the record is aborted here and the intents
// resolved at once.
//
// The abort is the record's compare-and-set: it fails on a COMMITTED
// record (an ambiguous commit that did apply), and a commit still in
// flight cannot apply over the ABORTED record it leaves. The intents are
// resolved as aborted only when the record says so; otherwise they are
// left to the pushers that find them, which read the record's verdict.
func (t *Txn) abortFailedCommit(ctx context.Context) {
	t.mu.Lock()
	anchored := t.mu.anchored
	txn := t.mu.txn
	cleaned := t.mu.abortCleaned
	t.mu.abortCleaned = true
	t.mu.Unlock()
	t.markFinished()
	defer t.wakeWaiters()
	if !anchored {
		return
	}
	// The commit may have failed because ctx did; the cleanup must not.
	actx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	ba := &kvpb.BatchRequest{Header: kvpb.BatchHeader{Txn: &txn}}
	ba.Add(&kvpb.EndTxnRequest{RequestHeader: kvpb.RequestHeader{Key: keys.Key(txn.Key).Clone()}, Commit: false})
	if _, kerr := t.db.Send(actx, ba); kerr != nil && kerr.TxnAborted == nil {
		log.Debugf("aborting %s after its commit failed: %v (lazy resolution will cover)", txn.ID, kerr)
		return
	}
	metrics.TxnAborts.Inc()
	if !cleaned {
		// (A part of the commit that found the transaction aborted by
		// another has resolved its intents already: giveUpAborted.)
		t.resolveAll(enginepb.ABORTED, txn.WriteTimestamp)
	}
}

// commit1PC attempts the one-phase fast path: when the deferred write
// batch is provably the transaction's ENTIRE write set (unanchored, no
// tracked writes) and every key — anchor included — maps to one range by
// WARM-CACHE lookup, the writes and a committing EndTxn{All} go out as a
// single BatchRequest evaluated in one raft proposal, writing committed
// values directly: no record, no intents, nothing to resolve. Returns
// handled=false (nothing applied) when ineligible or when routing shifted
// under the pinned descriptor — the caller falls back to parallelCommit.
// The batch keeps CreateTxnRecord so an old server that drops the unknown
// All field evaluates record + intents + commit classically in the same
// proposal — correct, just unoptimized — revealed by the missing OnePhase
// in its response.
func (t *Txn) commit1PC(ctx context.Context, wb *WriteBatch) (bool, error) {
	t.mu.Lock()
	if t.mu.anchored || len(t.mu.writes) != 0 {
		t.mu.Unlock()
		return false, nil
	}
	t.mu.Unlock()
	anchor := wb.kys[0]
	desc, ok := t.db.cache.Lookup(anchor)
	if !ok {
		return false, nil // cold cache: no lookup RPCs on this path
	}
	for _, k := range wb.kys {
		if !desc.ContainsKey(k) {
			return false, nil
		}
	}

	t.mu.Lock()
	t.mu.txn.Sequence++
	if len(t.mu.txn.Key) == 0 {
		t.mu.txn.Key = anchor.Clone()
	}
	txn := t.mu.txn
	t.mu.Unlock()

	intentKeys := make([]keys.Key, 0, len(wb.kys))
	for _, k := range wb.kys {
		intentKeys = append(intentKeys, k.Clone())
	}
	reqs := make([]kvpb.RequestUnion, 0, len(wb.reqs)+1)
	reqs = append(reqs, wb.reqs...)
	ba := &kvpb.BatchRequest{
		Header:   kvpb.BatchHeader{Txn: txn.Clone(), CreateTxnRecord: true},
		Requests: reqs,
	}
	ba.Add(&kvpb.EndTxnRequest{
		RequestHeader: kvpb.RequestHeader{Key: keys.Key(txn.Key).Clone()},
		Commit:        true,
		All:           true,
		IntentKeys:    intentKeys,
	})

	br, sent, err := t.sendSingleRange(ctx, ba, desc)
	if !sent {
		return false, nil // routing changed; nothing applied — classic path
	}
	if err != nil {
		t.markFinished()
		t.wakeWaiters()
		var kerr *kvpb.Error
		if errors.As(err, &kerr) {
			err = t.conditionFailure(wb, kerr)
		}
		return true, err
	}
	et := br.Responses[len(br.Responses)-1].EndTxn
	if !et.OnePhase {
		// An old server evaluated the batch classically (record + intents
		// + committed EndTxn, one atomic proposal): track the writes so
		// they can be resolved at the commit timestamp.
		for _, k := range wb.kys {
			t.recordWrite(k)
		}
	}
	t.markFinished()
	metrics.TxnCommits.Inc()
	if et.OnePhase {
		metrics.OnePhaseCommits.Inc()
		return true, nil
	}
	t.resolveAll(enginepb.COMMITTED, et.CommitTimestamp)
	t.wakeWaiters()
	return true, nil
}

// sendSingleRange executes ba against one pinned range with send's
// conflict handling (pushes, refreshes) but never through DB.Send — a
// one-phase batch must not be split by routing, or an EndTxn{All} could
// travel without its writes. sent=false (nothing applied) reports that
// the range's bounds changed under us.
func (t *Txn) sendSingleRange(ctx context.Context, ba *kvpb.BatchRequest, desc kvpb.RangeDescriptor) (_ *kvpb.BatchResponse, sent bool, _ error) {
	var cw conflictWait
	refreshes := 0
	for {
		// The commit deadline holds on this path as on send's (issue
		// #133): inside the loop, since a refresh moves the write
		// timestamp and the moved timestamp is what would commit.
		if err := t.checkDeadline(ba); err != nil {
			return nil, true, err
		}
		gh := ba.Header
		gh.Txn = t.proto() // re-stamp: refreshes move the timestamps
		start := time.Now()
		br, regroup, kerr := t.db.sendPartial(ctx, &gh, ba.Requests, desc)
		// This path bypasses DB.Send, so it counts itself: the one-phase
		// commits of autocommit writes would otherwise be missing from
		// datax_kv_batch_latency_seconds and from a bench record's
		// kv_batches_per_op.
		metrics.KVBatchLatency.Observe(time.Since(start).Seconds())
		if obs := t.db.sendObserver.Load(); obs != nil {
			(*obs)(ctx, ba, br) // this path bypasses DB.Send
		}
		if regroup {
			return nil, false, nil
		}
		if kerr == nil {
			if br.Txn != nil {
				t.mu.Lock()
				t.mu.txn.WriteTimestamp = t.mu.txn.WriteTimestamp.Forward(br.Txn.WriteTimestamp)
				t.mu.Unlock()
			}
			return br, true, nil
		}
		switch {
		case kerr.WriteIntent != nil:
			watch := t.db.waits.watch(kerr.WriteIntent.Intents)
			live, err := t.pushIntents(ctx, kerr.WriteIntent.Intents, !onlyCPutConflicts(ba, kerr.WriteIntent.Intents))
			if err != nil {
				watch.release()
				return nil, true, err
			}
			if len(live) == 0 {
				watch.release()
				continue
			}
			if rerr := t.cannotOutwait(live); rerr != nil {
				watch.release()
				return nil, true, rerr
			}
			if derr := t.detectDeadlock(ctx, &live[0]); derr != nil {
				watch.release()
				return nil, true, derr
			}
			if cw.waited() >= conflictBudget {
				watch.release()
				return nil, true, &RetryableError{Cause: kerr}
			}
			if err := watch.wait(ctx, live[0].Txn.ID, cw.next(live[0].Txn.ID, 0)); err != nil {
				return nil, true, err
			}
		case kerr.TxnAborted != nil:
			return nil, true, &RetryableError{Cause: kerr}
		case kerr.StorageOverloaded != nil:
			// As in send: the overload budget is spent (issue #391).
			metrics.TxnRetries.Inc()
			return nil, true, &RetryableError{Cause: kerr}
		case kerr.IsRetryableTxnError():
			newTS := kerr.RetryTimestamp(t.db.clock.Now())
			if refreshes < maxRefreshesPerOp && t.maybeRefresh(ctx, newTS) {
				refreshes++
				metrics.TxnRefreshes.Inc()
				continue
			}
			metrics.TxnRetries.Inc()
			return nil, true, &RetryableError{Cause: kerr}
		case t.restartsBelowGCThreshold(kerr):
			metrics.TxnRetries.Inc()
			return nil, true, &RetryableError{Cause: kerr}
		case kerr.IsBelowGCThreshold():
			return nil, true, &SnapshotTooOldError{Cause: kerr}
		default:
			return nil, true, kerr
		}
	}
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
		t.abortFailedCommit(ctx)
		if wbErr != nil {
			var kerr *kvpb.Error
			if errors.As(wbErr, &kerr) {
				return t.conditionFailure(wb, kerr)
			}
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
			t.abortFailedCommit(ctx)
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
			t.abortFailedCommit(ctx)
			return err
		}
		t.markFinished()
		metrics.TxnCommits.Inc()
		t.resolveAll(enginepb.COMMITTED, fbr.Responses[0].EndTxn.CommitTimestamp)
		t.wakeWaiters()
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
		// Waiters wake once the record is final and the intents resolved,
		// or, if finalizing fails, to run status recovery themselves.
		defer t.db.waits.finished(txn.ID)
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
	t.db.waits.finished(txn.ID)
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
	t.mu.deadline = hlc.Timestamp{} // the new epoch re-plans and re-pins
}

// failIfAborted refuses any further request of a transaction the
// coordinator knows was aborted by another (issue #320), cleaning up its
// intents the first time.
func (t *Txn) failIfAborted() error {
	t.mu.Lock()
	aborted, id := t.mu.aborted, t.mu.txn.ID
	t.mu.Unlock()
	if !aborted {
		return nil
	}
	t.giveUpAborted()
	e := kvpb.NewErrorf("transaction %s was aborted by a conflicting transaction", id)
	e.TxnAborted = &kvpb.TxnAbortedError{}
	return &RetryableError{Cause: e}
}

// finishedErrLocked is the error for an operation on a finished
// transaction; t.mu must be held. A transaction that finished because
// another aborted it (issue #320) answers with the same retryable abort
// every other request of it gets, so the client sees 40001 and retries,
// rather than a generic "already finished" that surfaces as XX000.
func (t *Txn) finishedErrLocked() error {
	if t.mu.aborted {
		e := kvpb.NewErrorf("transaction %s was aborted by a conflicting transaction", t.mu.txn.ID)
		e.TxnAborted = &kvpb.TxnAbortedError{}
		return &RetryableError{Cause: e}
	}
	return fmt.Errorf("transaction already finished")
}

// giveUpAborted settles a transaction that was aborted by another (or by
// itself, as a deadlock victim): it is finished, every later request
// fails at once, and its intents are resolved as aborted now, once. The
// SQL session forgets a transaction once a statement of it has failed,
// and its ROLLBACK then finds it finished, so without this the intents
// would wait for pushers, and the abort-span entries pushers left for it
// (which its own resolution removes) for GC.
func (t *Txn) giveUpAborted() {
	t.mu.Lock()
	t.mu.aborted = true
	cleaned := t.mu.abortCleaned
	t.mu.abortCleaned = true
	txn := t.mu.txn
	t.mu.Unlock()
	t.markFinished()
	if !cleaned {
		t.resolveAll(enginepb.ABORTED, txn.WriteTimestamp)
	}
	t.wakeWaiters()
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
				// A pusher won. The next operation fails at once rather
				// than reading past writes the pusher may have removed
				// (issue #320).
				t.mu.Lock()
				t.mu.aborted = true
				t.mu.Unlock()
				return
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
		if attempt >= 3 {
			log.Debugf("txn %s: attempt %d failed, retrying: %v", name, attempt, err)
		}
		// A write shed under storage backpressure is not restarted: the
		// shed batch already waited out the overload budget, and twenty
		// more budgets is the hang issue #391 reported.
		if !IsRetryable(err) || IsStorageOverloaded(err) || ctx.Err() != nil || attempt >= 20 {
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
