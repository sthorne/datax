package kvclient

import (
	"context"
	"errors"

	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/kvpb"
)

// WriteBatch buffers Put/Delete operations so a statement's writes travel
// in ONE routed batch (one Raft proposal per touched range) instead of one
// round trip per row.
type WriteBatch struct {
	reqs []kvpb.RequestUnion
	kys  []keys.Key
	// fails holds, for each conditional put, the error the batch fails
	// with when the put finds its key holding a value (nil for the other
	// requests). nil while the batch holds no conditional put; otherwise
	// as long as reqs.
	fails []error
}

// Put buffers key = value.
func (b *WriteBatch) Put(key keys.Key, value []byte) {
	b.reqs = append(b.reqs, kvpb.RequestUnion{Put: &kvpb.PutRequest{
		RequestHeader: kvpb.RequestHeader{Key: key.Clone()}, Value: value,
	}})
	b.kys = append(b.kys, key.Clone())
	if b.fails != nil {
		b.fails = append(b.fails, nil)
	}
}

// CPut buffers key = value on the condition that key has no live value
// at the transaction's read timestamp (kvpb.CPutRequest). If it has one,
// the batch fails with onFail, nothing of it is written, and the key's
// read counts as the transaction's (as a Get of it would). When several
// conditional puts fail, onFail is the one buffered first's. Send one
// only once the cluster has finalized version.V24 (CPutsOK).
func (b *WriteBatch) CPut(key keys.Key, value []byte, onFail error) {
	if b.fails == nil {
		b.fails = make([]error, len(b.reqs), cap(b.reqs)+1)
	}
	b.reqs = append(b.reqs, kvpb.RequestUnion{CPut: &kvpb.CPutRequest{
		RequestHeader: kvpb.RequestHeader{Key: key.Clone()}, Value: value,
	}})
	b.kys = append(b.kys, key.Clone())
	b.fails = append(b.fails, onFail)
}

// HasCPuts reports whether the batch holds a conditional put.
func (b *WriteBatch) HasCPuts() bool { return b.fails != nil }

// Delete buffers a deletion of key.
func (b *WriteBatch) Delete(key keys.Key) {
	b.reqs = append(b.reqs, kvpb.RequestUnion{Delete: &kvpb.DeleteRequest{
		RequestHeader: kvpb.RequestHeader{Key: key.Clone()},
	}})
	b.kys = append(b.kys, key.Clone())
	if b.fails != nil {
		b.fails = append(b.fails, nil)
	}
}

// appendBatch appends o's requests to b.
func (b *WriteBatch) appendBatch(o *WriteBatch) {
	if o.fails != nil && b.fails == nil {
		b.fails = make([]error, len(b.reqs), len(b.reqs)+len(o.reqs))
	}
	if b.fails != nil {
		if o.fails != nil {
			b.fails = append(b.fails, o.fails...)
		} else {
			b.fails = append(b.fails, make([]error, len(o.reqs))...)
		}
	}
	b.reqs = append(b.reqs, o.reqs...)
	b.kys = append(b.kys, o.kys...)
}

// conditionFailure is the error a batch that failed with kerr reports:
// for a failed conditional put, the error it was buffered with (the
// first such put of the key), and kerr itself otherwise. ok reports a
// failed conditional put of this batch, of key.
func (b *WriteBatch) conditionFailure(kerr *kvpb.Error) (key keys.Key, ok bool, err error) {
	if kerr == nil {
		return nil, false, nil
	}
	if kerr.ConditionFailed == nil || b.fails == nil {
		return nil, false, kerr
	}
	k := kerr.ConditionFailed.Key
	for i := range b.reqs {
		if c := b.reqs[i].CPut; c != nil && c.Key.Equal(k) {
			if b.fails[i] != nil {
				return k, true, b.fails[i]
			}
			return k, true, kerr
		}
	}
	return nil, false, kerr
}

// FirstConditionFailure reads, at the transaction's timestamp and in
// buffered order, the keys of the batch's conditional puts, and returns
// the failure error of the first one that holds a value — or the read's
// own error, or otherwise orig. Nothing is written. A statement whose batch is not yet sent
// uses it when it fails for another reason: the uniqueness checks the
// batch carries for the rows before the failure would have failed it
// first, had they been made as reads.
func (t *Txn) FirstConditionFailure(ctx context.Context, b *WriteBatch, orig error) error {
	if b.fails == nil {
		return orig
	}
	var ks []keys.Key
	var fails []error
	for i := range b.reqs {
		if c := b.reqs[i].CPut; c != nil {
			ks = append(ks, c.Key)
			fails = append(fails, b.fails[i])
		}
	}
	vals, err := t.GetBatch(ctx, ks)
	if err != nil {
		return err
	}
	for i, v := range vals {
		if v != nil && fails[i] != nil {
			return fails[i]
		}
	}
	return orig
}

// Len returns the number of buffered operations.
func (b *WriteBatch) Len() int { return len(b.reqs) }

// RunBatch executes every buffered write as intents of the transaction.
// The first key anchors the transaction if it is not yet anchored (the
// record is created atomically with the writes on the anchor's range; the
// DistSender scopes the creation flag to that range's sub-batch). Conflict
// handling matches single writes: pushes, refreshes, then RetryableError.
func (t *Txn) RunBatch(ctx context.Context, b *WriteBatch) error {
	if b.Len() == 0 {
		return nil
	}
	if t.pipelining {
		// Defer the flush: Commit will send it in parallel with a staged
		// EndTxn (parallel commit). Reads and point writes flush first, so
		// read-your-writes is preserved.
		t.mu.Lock()
		if t.mu.deferred == nil {
			t.mu.deferred = &WriteBatch{}
		}
		t.mu.deferred.appendBatch(b)
		t.mu.Unlock()
		return nil
	}
	return t.runBatchNow(ctx, b)
}

// runBatchNow is the classic synchronous flush.
func (t *Txn) runBatchNow(ctx context.Context, b *WriteBatch) error {
	t.mu.Lock()
	// One sequence for the whole batch: a savepoint cannot be established
	// mid-statement, so a statement's writes roll back as a unit.
	t.mu.txn.Sequence++
	createRecord, anchorSet := false, false
	if !t.mu.anchored {
		if len(t.mu.txn.Key) == 0 {
			t.mu.txn.Key = b.kys[0].Clone()
			anchorSet = true
		}
		// Only a batch that writes the anchor key itself may create the
		// record (co-location with a write on the anchor range).
		for _, k := range b.kys {
			if keys.Key(t.mu.txn.Key).Equal(k) {
				createRecord = true
				break
			}
		}
	}
	txn := t.mu.txn
	t.mu.Unlock()

	ba := &kvpb.BatchRequest{
		Header:   kvpb.BatchHeader{Txn: &txn, CreateTxnRecord: createRecord},
		Requests: b.reqs,
	}
	if _, err := t.send(ctx, ba, true); err != nil {
		return t.failedBatch(b, err, createRecord, anchorSet)
	}
	for _, k := range b.kys {
		t.recordWrite(k)
	}
	return nil
}

// failedBatch settles a write batch that failed with err and returns the
// error to report. A batch with conditional puts fails as a matter of
// course (a duplicate key), and when it spans ranges the other ranges'
// parts may have applied: unless the gateway knows none did, every key
// is then tracked as written, so that a rollback — to a savepoint or of
// the whole transaction — removes whatever did apply (removing an intent
// that is not there is a no-op), and a batch that was to create the
// record counts it as created (the record's part is sent first, alone,
// and succeeded if any other part was sent).
//
// When none of it applied and the batch chose the transaction's anchor
// (anchorSet: the first key of its first write), the choice is undone: the
// record was never created, and a later write that does not touch the
// failed batch's first key would otherwise never create it — its intents
// would belong to a transaction with no record, which COMMIT, seeing no
// anchor, would take for read-only and leave unresolved.
func (t *Txn) failedBatch(b *WriteBatch, err error, createRecord, anchorSet bool) error {
	var kerr *kvpb.Error
	if !b.HasCPuts() || !errors.As(err, &kerr) || kerr.ConditionFailed == nil {
		return err
	}
	if kerr.ConditionFailed.NoneApplied {
		if anchorSet {
			t.mu.Lock()
			if !t.mu.anchored {
				t.mu.txn.Key = nil
			}
			t.mu.Unlock()
		}
		return t.conditionFailure(b, kerr)
	}
	t.mu.Lock()
	if t.mu.writes == nil {
		t.mu.writes = make(map[string]struct{})
	}
	for _, k := range b.kys {
		t.mu.writes[string(k)] = struct{}{}
	}
	anchoredNow := createRecord && !t.mu.anchored
	if anchoredNow {
		t.mu.anchored = true
	}
	t.mu.Unlock()
	if anchoredNow {
		t.startHeartbeat()
	}
	return t.conditionFailure(b, kerr)
}

// conditionFailure maps a batch's failed conditional put to the error it
// was buffered with, recording the failed key's read: the transaction has
// observed its value (see kvpb.CPutRequest).
func (t *Txn) conditionFailure(b *WriteBatch, kerr *kvpb.Error) error {
	key, ok, err := b.conditionFailure(kerr)
	if ok {
		t.recordRead(key, nil)
	}
	return err
}
