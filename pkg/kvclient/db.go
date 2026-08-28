// Package kvclient is the KV client layer: DB routes batches to the right
// range replicas (the "DistSender" role) and — Phase 4 — Txn coordinates
// distributed transactions.
package kvclient

import (
	"context"
	"strconv"
	"time"

	"github.com/sthorne/datax/pkg/base"
	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/kvpb"
	"github.com/sthorne/datax/pkg/rpc"
	"github.com/sthorne/datax/pkg/util/hlc"
	"github.com/sthorne/datax/pkg/util/log"
)

// LocalStore is the interface DB uses to short-circuit to the local store.
type LocalStore interface {
	NodeID() base.NodeID
	ExecuteBatch(ctx context.Context, ba *kvpb.BatchRequest) (*kvpb.BatchResponse, *kvpb.Error)
}

// DB routes KV batches to range replicas: local fast path when this node
// holds a replica, otherwise RPC. It retries around leadership changes and
// stale routing.
type DB struct {
	local     LocalStore // may be nil
	transport *rpc.Transport
	clock     *hlc.Clock
	cache     *rangeCache
	// lookupRange, when set (Phase 3), refreshes descriptors from /meta.
	lookupRange func(ctx context.Context, key keys.Key) (kvpb.RangeDescriptor, error)
}

func NewDB(local LocalStore, transport *rpc.Transport, clock *hlc.Clock) *DB {
	return &DB{local: local, transport: transport, clock: clock, cache: newRangeCache()}
}

// SeedDescriptor primes the range cache (bootstrap: range 1 from init/join).
func (db *DB) SeedDescriptor(desc kvpb.RangeDescriptor) { db.cache.Insert(desc) }

// CachedDescriptor returns the cached descriptor covering key, if any.
func (db *DB) CachedDescriptor(key keys.Key) (kvpb.RangeDescriptor, bool) {
	return db.cache.Lookup(key)
}

// Clock exposes the node clock (transaction timestamps come from it).
func (db *DB) Clock() *hlc.Clock { return db.clock }

const (
	perAttemptTimeout = 3 * time.Second
	retryBackoff      = 50 * time.Millisecond
)

// Send routes and executes a batch. All requests in the batch must address
// a single range (multi-range batches are split by the caller — Phase 3
// adds automatic splitting for scans/transactions).
func (db *DB) Send(ctx context.Context, ba *kvpb.BatchRequest) (*kvpb.BatchResponse, *kvpb.Error) {
	if len(ba.Requests) == 0 {
		return nil, kvpb.NewErrorf("empty batch")
	}
	addr, err := keys.Addr(ba.Requests[0].GetInner().Header().Key)
	if err != nil {
		return nil, kvpb.NewError(err)
	}
	for {
		desc, ok := db.cache.Lookup(addr)
		if !ok {
			if db.lookupRange == nil {
				return nil, kvpb.NewErrorf("no descriptor for key %s and no range lookup configured", addr)
			}
			d, err := db.lookupRange(ctx, addr)
			if err != nil {
				return nil, kvpb.NewError(err)
			}
			db.cache.Insert(d)
			desc = d
		}
		br, kerr := db.sendToRange(ctx, ba, desc)
		if kerr != nil && ba.Header.Txn == nil && kerr.IsRetryableTxnError() {
			// Non-transactional batches have no reads to protect: refresh
			// the timestamp (above the server's floor) and try again.
			ts := db.clock.Now().Forward(kerr.RetryTimestamp(ba.Header.Timestamp))
			ba.Header.Timestamp = ts
			select {
			case <-ctx.Done():
				return nil, kerr
			default:
				continue
			}
		}
		if kerr != nil && (kerr.RangeKeyMismatch != nil || kerr.RangeNotFound != nil) {
			db.cache.Evict(desc.RangeID)
			if kerr.RangeKeyMismatch != nil {
				db.cache.Insert(kerr.RangeKeyMismatch.ActualDescriptors...)
			}
			if db.lookupRange != nil {
				select {
				case <-ctx.Done():
					return nil, kvpb.NewError(ctx.Err())
				default:
					continue // re-route with fresh descriptor
				}
			}
		}
		return br, kerr
	}
}

// sendToRange tries the range's replicas — leader hint first, local replica
// next — retrying around NotLeader and transport errors until ctx expires.
func (db *DB) sendToRange(ctx context.Context, ba *kvpb.BatchRequest, desc kvpb.RangeDescriptor) (*kvpb.BatchResponse, *kvpb.Error) {
	ba.Header.RangeID = desc.RangeID
	var lastErr *kvpb.Error
	for attempt := 0; ; attempt++ {
		if err := ctx.Err(); err != nil {
			if lastErr != nil {
				return nil, lastErr
			}
			return nil, kvpb.NewError(err)
		}
		for _, target := range db.replicaOrder(desc) {
			br, kerr, err := db.sendToReplica(ctx, ba, target)
			if err != nil {
				lastErr = kvpb.NewErrorf("n%d unreachable: %v", target, err)
				continue
			}
			if kerr == nil {
				db.cache.SetHint(desc.RangeID, target)
				return br, nil
			}
			if kerr.NotLeader != nil {
				db.cache.SetHint(desc.RangeID, kerr.NotLeader.LeaderHint)
				lastErr = kerr
				continue
			}
			if kerr.RangeNotFound != nil {
				lastErr = kerr
				continue
			}
			// Definitive KV-level answer (including txn conflicts): return.
			return br, kerr
		}
		select {
		case <-ctx.Done():
			return nil, lastErr
		case <-time.After(retryBackoff):
		}
		if attempt > 0 && attempt%20 == 0 {
			log.Debugf("still retrying batch on %s after %d attempts: %v", desc.RangeID, attempt, lastErr)
		}
	}
}

// replicaOrder yields target nodes: leader hint, then local, then the rest.
func (db *DB) replicaOrder(desc kvpb.RangeDescriptor) []base.NodeID {
	var order []base.NodeID
	seen := map[base.NodeID]bool{}
	add := func(n base.NodeID) {
		if n != 0 && !seen[n] {
			seen[n] = true
			order = append(order, n)
		}
	}
	add(db.cache.Hint(desc.RangeID))
	if db.local != nil {
		if _, ok := desc.GetReplica(db.local.NodeID()); ok {
			add(db.local.NodeID())
		}
	}
	for _, r := range desc.Replicas {
		add(r.NodeID)
	}
	return order
}

func (db *DB) sendToReplica(ctx context.Context, ba *kvpb.BatchRequest, target base.NodeID) (*kvpb.BatchResponse, *kvpb.Error, error) {
	actx, cancel := context.WithTimeout(ctx, perAttemptTimeout)
	defer cancel()
	if db.local != nil && target == db.local.NodeID() {
		br, kerr := db.local.ExecuteBatch(actx, ba)
		return br, kerr, nil
	}
	return db.transport.SendBatch(actx, target, ba)
}

// ---------------------------------------------------------------------------
// Non-transactional convenience helpers (server-assigned timestamps).

func (db *DB) Get(ctx context.Context, key keys.Key) ([]byte, error) {
	ba := &kvpb.BatchRequest{Header: kvpb.BatchHeader{Timestamp: db.clock.Now()}}
	ba.Add(&kvpb.GetRequest{RequestHeader: kvpb.RequestHeader{Key: key}})
	br, kerr := db.Send(ctx, ba)
	if kerr != nil {
		return nil, kerr
	}
	return br.Responses[0].Get.Value, nil
}

func (db *DB) Put(ctx context.Context, key keys.Key, value []byte) error {
	ba := &kvpb.BatchRequest{Header: kvpb.BatchHeader{Timestamp: db.clock.Now()}}
	ba.Add(&kvpb.PutRequest{RequestHeader: kvpb.RequestHeader{Key: key}, Value: value})
	_, kerr := db.Send(ctx, ba)
	if kerr != nil {
		return kerr
	}
	return nil
}

func (db *DB) Scan(ctx context.Context, start, end keys.Key, max int64) ([]kvpb.KeyValue, error) {
	ba := &kvpb.BatchRequest{Header: kvpb.BatchHeader{Timestamp: db.clock.Now()}}
	ba.Add(&kvpb.ScanRequest{RequestHeader: kvpb.RequestHeader{Key: start, EndKey: end}, MaxRows: max})
	br, kerr := db.Send(ctx, ba)
	if kerr != nil {
		return nil, kerr
	}
	return br.Responses[0].Scan.Rows, nil
}

// Increment atomically adds by to the counter at key, returning the new
// value.
func (db *DB) Increment(ctx context.Context, key keys.Key, by int64) (int64, error) {
	ba := &kvpb.BatchRequest{Header: kvpb.BatchHeader{Timestamp: db.clock.Now()}}
	ba.Add(&kvpb.IncrementRequest{RequestHeader: kvpb.RequestHeader{Key: key}, By: by})
	br, kerr := db.Send(ctx, ba)
	if kerr != nil {
		return 0, kerr
	}
	return br.Responses[0].Increment.NewValue, nil
}

// ParseCounter decodes a counter value written by Increment.
func ParseCounter(raw []byte) (int64, error) {
	if raw == nil {
		return 0, nil
	}
	return strconv.ParseInt(string(raw), 10, 64)
}
