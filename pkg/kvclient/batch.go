package kvclient

import (
	"context"

	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/kvpb"
)

// WriteBatch buffers Put/Delete operations so a statement's writes travel
// in ONE routed batch (one Raft proposal per touched range) instead of one
// round trip per row.
type WriteBatch struct {
	reqs []kvpb.RequestUnion
	kys  []keys.Key
}

// Put buffers key = value.
func (b *WriteBatch) Put(key keys.Key, value []byte) {
	b.reqs = append(b.reqs, kvpb.RequestUnion{Put: &kvpb.PutRequest{
		RequestHeader: kvpb.RequestHeader{Key: key.Clone()}, Value: value,
	}})
	b.kys = append(b.kys, key.Clone())
}

// Delete buffers a deletion of key.
func (b *WriteBatch) Delete(key keys.Key) {
	b.reqs = append(b.reqs, kvpb.RequestUnion{Delete: &kvpb.DeleteRequest{
		RequestHeader: kvpb.RequestHeader{Key: key.Clone()},
	}})
	b.kys = append(b.kys, key.Clone())
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
		t.mu.deferred.reqs = append(t.mu.deferred.reqs, b.reqs...)
		t.mu.deferred.kys = append(t.mu.deferred.kys, b.kys...)
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
	createRecord := false
	if !t.mu.anchored {
		if len(t.mu.txn.Key) == 0 {
			t.mu.txn.Key = b.kys[0].Clone()
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
		return err
	}
	for _, k := range b.kys {
		t.recordWrite(k)
	}
	return nil
}
