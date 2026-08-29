// Package kvclient is the KV client layer: DB routes batches to the right
// range replicas (the "DistSender" role) and Txn coordinates distributed
// transactions.
package kvclient

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/sthorne/datax/pkg/base"
	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/kvpb"
	"github.com/sthorne/datax/pkg/metrics"
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
// holds a replica, otherwise RPC. It splits batches across ranges, stitches
// multi-range scans, refreshes routing from /meta records, and retries
// around leadership changes.
type DB struct {
	local      *localSender
	transport  *rpc.Transport
	clock      *hlc.Clock
	cache      *rangeCache
	metaLookup bool
	// nodeLister enumerates known cluster nodes (the registry): the routing
	// fallback when a cached descriptor's replicas are unreachable (e.g. a
	// stale pre-upreplication descriptor whose only replica died).
	nodeLister func() []base.NodeID
}

// SetNodeLister wires the cluster-node enumeration used as routing fallback.
func (db *DB) SetNodeLister(f func() []base.NodeID) { db.nodeLister = f }

type localSender struct {
	store LocalStore
}

func NewDB(local LocalStore, transport *rpc.Transport, clock *hlc.Clock) *DB {
	db := &DB{transport: transport, clock: clock, cache: newRangeCache()}
	if local != nil {
		db.local = &localSender{store: local}
	}
	return db
}

// SeedDescriptor primes the range cache (bootstrap: range 1 from init/join).
func (db *DB) SeedDescriptor(desc kvpb.RangeDescriptor) { db.cache.Insert(desc) }

// CachedDescriptor returns the cached descriptor covering key, if any.
func (db *DB) CachedDescriptor(key keys.Key) (kvpb.RangeDescriptor, bool) {
	return db.cache.Lookup(key)
}

// EnableMetaLookup turns on routing refresh from /meta records (requires a
// seeded descriptor covering the meta span).
func (db *DB) EnableMetaLookup() { db.metaLookup = true }

// Clock exposes the node clock (transaction timestamps come from it).
func (db *DB) Clock() *hlc.Clock { return db.clock }

const (
	perAttemptTimeout = 3 * time.Second
	retryBackoff      = 50 * time.Millisecond
	maxRoutingRetries = 30
)

// Send routes and executes a batch. Requests are grouped by range and the
// groups execute in order; scans spanning ranges are stitched together.
// Note that a non-transactional batch that crosses ranges is NOT atomic —
// atomicity across ranges is the transaction layer's job.
func (db *DB) Send(ctx context.Context, ba *kvpb.BatchRequest) (*kvpb.BatchResponse, *kvpb.Error) {
	if len(ba.Requests) == 0 {
		return nil, kvpb.NewErrorf("empty batch")
	}
	defer func(start time.Time) { metrics.KVBatchLatency.Observe(time.Since(start).Seconds()) }(time.Now())
	out := &kvpb.BatchResponse{Txn: ba.Header.Txn, Timestamp: ba.Header.Timestamp}
	header := ba.Header

	i := 0
	regroups := 0
	for i < len(ba.Requests) {
		if scan := ba.Requests[i].Scan; scan != nil {
			resp, kerr := db.sendScan(ctx, header, scan)
			if kerr != nil {
				return nil, kerr
			}
			out.Responses = append(out.Responses, kvpb.ResponseUnion{Scan: resp})
			i++
			continue
		}
		if ref := ba.Requests[i].Refresh; ref != nil && len(ref.EndKey) > 0 {
			if kerr := db.sendRefresh(ctx, header, ref); kerr != nil {
				return nil, kerr
			}
			out.Responses = append(out.Responses, kvpb.ResponseUnion{Refresh: &kvpb.RefreshResponse{}})
			i++
			continue
		}

		addr, err := keys.Addr(ba.Requests[i].GetInner().Header().Key)
		if err != nil {
			return nil, kvpb.NewError(err)
		}
		desc, kerr := db.descForKey(ctx, addr)
		if kerr != nil {
			return nil, kerr
		}
		// Extend the group with consecutive point requests on the same range.
		j := i + 1
		for j < len(ba.Requests) {
			if ba.Requests[j].Scan != nil {
				break
			}
			a, err := keys.Addr(ba.Requests[j].GetInner().Header().Key)
			if err != nil {
				return nil, kvpb.NewError(err)
			}
			if !desc.ContainsKey(a) {
				break
			}
			j++
		}
		// The transaction record must be created on the anchor key's range,
		// atomically with a write there: scope the creation flag to the one
		// group that actually writes the anchor key.
		gh := header
		if gh.CreateTxnRecord && gh.Txn != nil {
			gh.CreateTxnRecord = false
			for _, u := range ba.Requests[i:j] {
				if keys.Key(gh.Txn.Key).Equal(u.GetInner().Header().Key) {
					gh.CreateTxnRecord = true
					break
				}
			}
		}
		br, regroup, kerr := db.sendPartial(ctx, &gh, ba.Requests[i:j], desc)
		header.Timestamp = gh.Timestamp
		if regroup {
			if regroups++; regroups > maxRoutingRetries {
				return nil, kvpb.NewErrorf("routing did not converge after %d retries: %v", regroups, kerr)
			}
			continue // descriptors refreshed; re-group from request i
		}
		if kerr != nil {
			return nil, kerr
		}
		out.Responses = append(out.Responses, br.Responses...)
		if br.Txn != nil {
			out.Txn = br.Txn
		}
		i = j
	}
	out.Timestamp = header.Timestamp
	return out, nil
}

// descForKey resolves the descriptor covering an addressed key, consulting
// /meta on cache miss.
func (db *DB) descForKey(ctx context.Context, addr keys.Key) (kvpb.RangeDescriptor, *kvpb.Error) {
	if d, ok := db.cache.Lookup(addr); ok {
		return d, nil
	}
	if !db.metaLookup {
		return kvpb.RangeDescriptor{}, kvpb.NewErrorf("no descriptor for key %s and meta lookup disabled", addr)
	}
	d, err := db.lookupMeta(ctx, addr)
	if err != nil {
		return kvpb.RangeDescriptor{}, kvpb.NewError(err)
	}
	db.cache.Insert(d)
	return d, nil
}

// lookupMeta scans /meta for the first record whose range ends beyond key.
// The scan is inconsistent (never blocks on intents) and itself routed via
// the cache — the bootstrap invariant is that a descriptor covering the
// meta span is always seeded.
func (db *DB) lookupMeta(ctx context.Context, key keys.Key) (kvpb.RangeDescriptor, error) {
	start := keys.Key(keys.RangeMetaKey(key)).Next()
	_, metaEnd := keys.MetaSpan()
	header := kvpb.BatchHeader{Timestamp: db.clock.Now(), ReadInconsistent: true}
	scan := &kvpb.ScanRequest{RequestHeader: kvpb.RequestHeader{Key: start, EndKey: metaEnd}, MaxRows: 1}

	for attempt := 0; attempt < 5; attempt++ {
		desc, ok := db.cache.Lookup(start)
		if !ok {
			return kvpb.RangeDescriptor{}, fmt.Errorf("no routing descriptor for the meta span; cluster bootstrap incomplete")
		}
		br, regroup, kerr := db.sendPartial(ctx, &header, []kvpb.RequestUnion{{Scan: scan}}, desc)
		if regroup {
			continue
		}
		if kerr != nil {
			return kvpb.RangeDescriptor{}, kerr
		}
		rows := br.Responses[0].Scan.Rows
		if len(rows) == 0 {
			return kvpb.RangeDescriptor{}, fmt.Errorf("no meta record covering key %s", key)
		}
		var d kvpb.RangeDescriptor
		if err := json.Unmarshal(rows[0].Value, &d); err != nil {
			return kvpb.RangeDescriptor{}, fmt.Errorf("corrupt meta record: %w", err)
		}
		if !d.ContainsKey(key) {
			return kvpb.RangeDescriptor{}, fmt.Errorf("meta record %s does not cover key %s (stale addressing)", &d, key)
		}
		return d, nil
	}
	return kvpb.RangeDescriptor{}, fmt.Errorf("meta lookup for %s did not converge", key)
}

// sendScan executes a scan, stitching across range boundaries.
func (db *DB) sendScan(ctx context.Context, header kvpb.BatchHeader, req *kvpb.ScanRequest) (*kvpb.ScanResponse, *kvpb.Error) {
	out := &kvpb.ScanResponse{}
	cur := req.Key.Clone()
	remaining := req.MaxRows
	regroups := 0
	for cur.Less(req.EndKey) {
		desc, kerr := db.descForKey(ctx, cur)
		if kerr != nil {
			return nil, kerr
		}
		end := req.EndKey
		if desc.EndKey.Less(end) {
			end = desc.EndKey
		}
		sub := &kvpb.ScanRequest{RequestHeader: kvpb.RequestHeader{Key: cur, EndKey: end}, MaxRows: remaining}
		br, regroup, kerr := db.sendPartial(ctx, &header, []kvpb.RequestUnion{{Scan: sub}}, desc)
		if regroup {
			if regroups++; regroups > maxRoutingRetries {
				return nil, kvpb.NewErrorf("scan routing did not converge: %v", kerr)
			}
			continue
		}
		if kerr != nil {
			return nil, kerr
		}
		sr := br.Responses[0].Scan
		out.Rows = append(out.Rows, sr.Rows...)
		if len(sr.Resume) > 0 {
			out.Resume = sr.Resume
			return out, nil
		}
		if req.MaxRows > 0 {
			remaining -= int64(len(sr.Rows))
			if remaining <= 0 {
				if end.Less(req.EndKey) {
					out.Resume = end
				}
				return out, nil
			}
		}
		cur = end
	}
	return out, nil
}

// sendRefresh executes a ranged refresh, split across range boundaries
// (each subspan check is independent; all must pass).
func (db *DB) sendRefresh(ctx context.Context, header kvpb.BatchHeader, req *kvpb.RefreshRequest) *kvpb.Error {
	cur := req.Key.Clone()
	regroups := 0
	for cur.Less(req.EndKey) {
		desc, kerr := db.descForKey(ctx, cur)
		if kerr != nil {
			return kerr
		}
		end := req.EndKey
		if desc.EndKey.Less(end) {
			end = desc.EndKey
		}
		sub := &kvpb.RefreshRequest{RequestHeader: kvpb.RequestHeader{Key: cur, EndKey: end}, FromTS: req.FromTS}
		_, regroup, kerr := db.sendPartial(ctx, &header, []kvpb.RequestUnion{{Refresh: sub}}, desc)
		if regroup {
			if regroups++; regroups > maxRoutingRetries {
				return kvpb.NewErrorf("refresh routing did not converge: %v", kerr)
			}
			continue
		}
		if kerr != nil {
			return kerr
		}
		cur = end
	}
	return nil
}

// sendPartial sends one single-range sub-batch, handling timestamp-refresh
// retries (non-transactional writes) inline and reporting stale routing to
// the caller (regroup=true after refreshing the cache).
func (db *DB) sendPartial(ctx context.Context, header *kvpb.BatchHeader, reqs []kvpb.RequestUnion, desc kvpb.RangeDescriptor) (br *kvpb.BatchResponse, regroup bool, kerr *kvpb.Error) {
	for {
		ba := &kvpb.BatchRequest{Header: *header, Requests: reqs}
		br, kerr = db.sendToRange(ctx, ba, desc)
		if kerr == nil {
			return br, false, nil
		}
		if header.Txn == nil && kerr.IsRetryableTxnError() {
			// Non-transactional batches have no reads to protect: refresh
			// the timestamp above the server's floor and try again.
			header.Timestamp = db.clock.Now().Forward(kerr.RetryTimestamp(header.Timestamp))
			select {
			case <-ctx.Done():
				return nil, false, kerr
			default:
				continue
			}
		}
		if kerr.RangeKeyMismatch != nil || kerr.RangeNotFound != nil {
			db.cache.Evict(desc.RangeID)
			if kerr.RangeKeyMismatch != nil {
				db.cache.Insert(kerr.RangeKeyMismatch.ActualDescriptors...)
			}
			return nil, true, kerr
		}
		return nil, false, kerr
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
			// Definitive KV-level answer (including txn conflicts and
			// routing corrections): return to caller.
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
		if _, ok := desc.GetReplica(db.local.store.NodeID()); ok {
			add(db.local.store.NodeID())
		}
	}
	for _, r := range desc.Replicas {
		add(r.NodeID)
	}
	// Fallback: the descriptor may be stale (membership changed since it
	// was cached). Any node hosting a replica will serve or redirect;
	// nodes without one answer RangeNotFound and we move on.
	if db.local != nil {
		add(db.local.store.NodeID())
	}
	if db.nodeLister != nil {
		for _, id := range db.nodeLister() {
			add(id)
		}
	}
	return order
}

func (db *DB) sendToReplica(ctx context.Context, ba *kvpb.BatchRequest, target base.NodeID) (*kvpb.BatchResponse, *kvpb.Error, error) {
	actx, cancel := context.WithTimeout(ctx, perAttemptTimeout)
	defer cancel()
	if db.local != nil && target == db.local.store.NodeID() {
		br, kerr := db.local.store.ExecuteBatch(actx, ba)
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

func (db *DB) Delete(ctx context.Context, key keys.Key) error {
	ba := &kvpb.BatchRequest{Header: kvpb.BatchHeader{Timestamp: db.clock.Now()}}
	ba.Add(&kvpb.DeleteRequest{RequestHeader: kvpb.RequestHeader{Key: key}})
	_, kerr := db.Send(ctx, ba)
	if kerr != nil {
		return kerr
	}
	return nil
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

// AdminChangeReplicas adds and/or removes a replica of the range covering
// key.
func (db *DB) AdminChangeReplicas(ctx context.Context, key keys.Key, add, remove base.NodeID) (*kvpb.AdminChangeReplicasResponse, error) {
	ba := &kvpb.BatchRequest{Header: kvpb.BatchHeader{Timestamp: db.clock.Now()}}
	ba.Add(&kvpb.AdminChangeReplicasRequest{RequestHeader: kvpb.RequestHeader{Key: key}, AddNode: add, RemoveNode: remove})
	br, kerr := db.Send(ctx, ba)
	if kerr != nil {
		return nil, kerr
	}
	resp := br.Responses[0].AdminChangeReplicas
	db.cache.Evict(resp.Desc.RangeID)
	db.cache.Insert(resp.Desc)
	return resp, nil
}

// AdminSplit splits the range containing key at key.
func (db *DB) AdminSplit(ctx context.Context, key keys.Key) (*kvpb.AdminSplitResponse, error) {
	ba := &kvpb.BatchRequest{Header: kvpb.BatchHeader{Timestamp: db.clock.Now()}}
	ba.Add(&kvpb.AdminSplitRequest{RequestHeader: kvpb.RequestHeader{Key: key}})
	br, kerr := db.Send(ctx, ba)
	if kerr != nil {
		return nil, kerr
	}
	resp := br.Responses[0].AdminSplit
	db.cache.Evict(resp.Left.RangeID)
	db.cache.Insert(resp.Left, resp.Right)
	return resp, nil
}

// ParseCounter decodes a counter value written by Increment.
func ParseCounter(raw []byte) (int64, error) {
	if raw == nil {
		return 0, nil
	}
	return strconv.ParseInt(string(raw), 10, 64)
}
