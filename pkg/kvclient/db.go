// Package kvclient is the KV client layer: DB routes batches to the right
// range replicas (the "DistSender" role) and Txn coordinates distributed
// transactions.
package kvclient

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sthorne/datax/pkg/base"
	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/kvpb"
	"github.com/sthorne/datax/pkg/metrics"
	"github.com/sthorne/datax/pkg/rpc"
	"github.com/sthorne/datax/pkg/storage"
	"github.com/sthorne/datax/pkg/storage/enginepb"
	"github.com/sthorne/datax/pkg/util/hlc"
	"github.com/sthorne/datax/pkg/util/log"
	"github.com/sthorne/datax/pkg/version"
)

// LocalStore is the interface DB uses to short-circuit to the local store.
type LocalStore interface {
	NodeID() base.NodeID
	ExecuteBatch(ctx context.Context, ba *kvpb.BatchRequest) (*kvpb.BatchResponse, *kvpb.Error)
}

// ClosedTimestampSource is optionally implemented by a LocalStore
// (kvserver.Store does): the oldest closed timestamp across its local
// replicas — what bounded-staleness reads use to pick the freshest
// locally-servable statement timestamp.
type ClosedTimestampSource interface {
	MinClosedTimestamp() hlc.Timestamp
}

// LocalRanges is optionally implemented by a LocalStore (kvserver.Store
// does): descriptor access for replicas this node actually holds. The
// store is the authority on local membership — a cached descriptor can
// predate this node's upreplication into the range (it was seeded at
// join and nothing refreshes a descriptor that still routes to the
// leader successfully), silently hiding the local replica.
type LocalRanges interface {
	LocalDescriptor(base.RangeID) (kvpb.RangeDescriptor, bool)
}

// holdsReplica reports whether this node holds a replica of the range,
// consulting the local store when the (possibly stale) descriptor says
// no. A store descriptor newer than the routing cache's repairs the
// cache as a side effect, so later statements route correctly upfront.
func (db *DB) holdsReplica(desc kvpb.RangeDescriptor) bool {
	if db.local == nil {
		return false
	}
	self := db.local.store.NodeID()
	if _, ok := desc.GetReplica(self); ok {
		return true
	}
	lr, ok := db.local.store.(LocalRanges)
	if !ok {
		return false
	}
	ldesc, ok := lr.LocalDescriptor(desc.RangeID)
	if !ok {
		return false
	}
	if _, ok := ldesc.GetReplica(self); !ok {
		return false
	}
	if ldesc.Generation > desc.Generation {
		db.cache.Insert(ldesc)
	}
	return true
}

// LocalClosedTimestamp reports the local store's MinClosedTimestamp, or
// zero when there is no local store (a pure gateway) or it does not
// publish closed timestamps.
func (db *DB) LocalClosedTimestamp() hlc.Timestamp {
	if db.local == nil {
		return hlc.Timestamp{}
	}
	if src, ok := db.local.store.(ClosedTimestampSource); ok {
		return src.MinClosedTimestamp()
	}
	return hlc.Timestamp{}
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
	// versionGate reports the finalized cluster version; version-gated
	// request shapes (pkg/version rule 4) are sent only when it allows
	// them. Nil (tests, tools) reads as the binary's own version: a
	// process without version plumbing never talks to older binaries.
	versionGate atomic.Pointer[func() version.Version]
}

// SetNodeLister wires the cluster-node enumeration used as routing fallback.
func (db *DB) SetNodeLister(f func() []base.NodeID) { db.nodeLister = f }

// SetVersionGate wires the finalized-cluster-version source (the node's
// mirror) consulted before sending version-gated request shapes.
func (db *DB) SetVersionGate(f func() version.Version) { db.versionGate.Store(&f) }

// ReverseScansOK reports whether reverse scans may be SENT: they were
// introduced under cluster version v3, and a v2 node silently runs a
// forward scan when it sees one (pkg/version rule 4).
func (db *DB) ReverseScansOK() bool {
	return db.ClusterVersion() >= version.V3
}

// ClusterVersion reports the finalized cluster version the gate observes
// (the binary's own version when no gate is installed: a single-binary
// process).
func (db *DB) ClusterVersion() version.Version {
	f := db.versionGate.Load()
	if f == nil {
		return version.Current
	}
	return (*f)()
}

// LocalNodeID is the node this DB runs on (0 for a store-less client).
func (db *DB) LocalNodeID() base.NodeID {
	if db.local == nil {
		return 0
	}
	return db.local.store.NodeID()
}

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

// EvictDescriptor drops the cached routing entry for a range, forcing the
// next request for its keys through a /meta lookup (tests use it to
// exercise the lookup path).
func (db *DB) EvictDescriptor(rangeID base.RangeID) { db.cache.Evict(rangeID) }

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

	// Transactional batches of point requests spanning several ranges fan
	// out concurrently — one range's consensus round no longer waits for
	// another's. Everything else (non-transactional batches, whose
	// per-group timestamp ratchet is observable ordering, and ranged
	// requests) keeps the sequential path below.
	if ba.Header.Txn != nil && len(ba.Requests) > 1 && batchIsPointRequests(ba) {
		groups, kerr := db.partitionByRange(ctx, ba.Requests, nil)
		if kerr != nil {
			return nil, kerr
		}
		if len(groups) > 1 {
			return db.sendParallel(ctx, ba, groups)
		}
	}

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
		if exp := ba.Requests[i].Export; exp != nil {
			resp, kerr := db.sendExport(ctx, header, exp)
			if kerr != nil {
				return nil, kerr
			}
			out.Responses = append(out.Responses, kvpb.ResponseUnion{Export: resp})
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
			if berr := routingBackoff(ctx, regroups); berr != nil {
				return nil, berr
			}
			continue // descriptors refreshed; re-group from request i
		}
		if kerr != nil {
			return nil, kerr
		}
		out.Responses = append(out.Responses, br.Responses...)
		if br.Txn != nil {
			// Merge, never overwrite: a multi-range batch executes as
			// per-range sub-batches, and any ONE of them may have had its
			// write timestamp pushed by that range's timestamp cache. The
			// batch response must report the MAXIMUM write timestamp across
			// all sub-batches — taking the last group's verbatim would let
			// an earlier group's push vanish, and a parallel commit would
			// then stage, commit, and resolve intents BELOW a timestamp
			// already served to a reader (a serializability violation: the
			// pushed intent's version is moved back down beneath the read).
			if out.Txn == nil {
				out.Txn = br.Txn
			} else if out.Txn.WriteTimestamp.Less(br.Txn.WriteTimestamp) {
				merged := *out.Txn
				merged.WriteTimestamp = br.Txn.WriteTimestamp
				out.Txn = &merged
			}
		}
		i = j
	}
	out.Timestamp = header.Timestamp
	return out, nil
}

// batchIsPointRequests reports whether every request in the batch is a
// point request — the shape the parallel fan-out handles. Ranged requests
// (Scan, Export, span Refresh) have their own multi-range stitching.
func batchIsPointRequests(ba *kvpb.BatchRequest) bool {
	for i := range ba.Requests {
		if ba.Requests[i].Scan != nil || ba.Requests[i].Export != nil {
			return false
		}
		if ref := ba.Requests[i].Refresh; ref != nil && len(ref.EndKey) > 0 {
			return false
		}
		if ba.Requests[i].GetInner() == nil {
			return false
		}
	}
	return true
}

// rangeGroup is one range's share of a partitioned batch, with the
// original request positions for response reassembly.
type rangeGroup struct {
	desc    kvpb.RangeDescriptor
	indices []int
	reqs    []kvpb.RequestUnion
}

// partitionByRange groups point requests by the range covering them — a
// true partition, unlike the sequential path's contiguous runs, so a batch
// whose keys interleave across ranges (a SQL INSERT's rows and index
// entries) still yields one sub-batch per range. indices maps reqs back to
// original batch positions (nil = identity).
func (db *DB) partitionByRange(ctx context.Context, reqs []kvpb.RequestUnion, indices []int) ([]rangeGroup, *kvpb.Error) {
	var groups []rangeGroup
	byRange := map[base.RangeID]int{}
	for n := range reqs {
		addr, err := keys.Addr(reqs[n].GetInner().Header().Key)
		if err != nil {
			return nil, kvpb.NewError(err)
		}
		desc, kerr := db.descForKey(ctx, addr)
		if kerr != nil {
			return nil, kerr
		}
		idx := n
		if indices != nil {
			idx = indices[n]
		}
		if gi, ok := byRange[desc.RangeID]; ok {
			groups[gi].reqs = append(groups[gi].reqs, reqs[n])
			groups[gi].indices = append(groups[gi].indices, idx)
		} else {
			byRange[desc.RangeID] = len(groups)
			groups = append(groups, rangeGroup{
				desc:    desc,
				indices: []int{idx},
				reqs:    []kvpb.RequestUnion{reqs[n]},
			})
		}
	}
	return groups, nil
}

// maxParallelSubBatches caps how many per-range sub-batches of one batch
// are in flight at once.
const maxParallelSubBatches = 8

// sendParallel executes a transactional point-request batch as per-range
// sub-batches fanned out concurrently. Ordering constraints preserved:
//
//   - Anchor-first: when the batch creates the transaction record, the
//     sub-batch writing the anchor key completes BEFORE any sibling is
//     sent, so no intent is ever observable before the record exists (a
//     pusher finding such an intent judges expiry from MinTimestamp and
//     can poison the record ABORTED).
//   - The response's transaction reports the MAXIMUM write timestamp
//     across sub-batches (same merge as the sequential path — see the
//     comment there); the merge is mutex-guarded here.
//   - Responses are placed positionally via the partition's index map.
//
// Sub-batches that need re-routing (splits, moved replicas) are
// re-partitioned into the next wave under the batch-wide retry budget.
func (db *DB) sendParallel(ctx context.Context, ba *kvpb.BatchRequest, groups []rangeGroup) (*kvpb.BatchResponse, *kvpb.Error) {
	header := ba.Header
	out := &kvpb.BatchResponse{Txn: ba.Header.Txn, Timestamp: ba.Header.Timestamp}
	out.Responses = make([]kvpb.ResponseUnion, len(ba.Requests))

	var txnMu sync.Mutex
	mergeTxn := func(br *kvpb.BatchResponse) {
		if br.Txn == nil {
			return
		}
		txnMu.Lock()
		defer txnMu.Unlock()
		if out.Txn == nil {
			out.Txn = br.Txn
		} else if out.Txn.WriteTimestamp.Less(br.Txn.WriteTimestamp) {
			merged := *out.Txn
			merged.WriteTimestamp = br.Txn.WriteTimestamp
			out.Txn = &merged
		}
	}

	// sendGroup executes one sub-batch and, on success, places its
	// responses. The CreateTxnRecord flag is scoped to the group writing
	// the anchor key, exactly as on the sequential path.
	sendGroup := func(g rangeGroup) (regroup bool, kerr *kvpb.Error) {
		gh := header
		if gh.CreateTxnRecord && gh.Txn != nil {
			gh.CreateTxnRecord = false
			for i := range g.reqs {
				if keys.Key(gh.Txn.Key).Equal(g.reqs[i].GetInner().Header().Key) {
					gh.CreateTxnRecord = true
					break
				}
			}
		}
		br, regroup, kerr := db.sendPartial(ctx, &gh, g.reqs, g.desc)
		if regroup || kerr != nil {
			return regroup, kerr
		}
		for k := range g.reqs {
			out.Responses[g.indices[k]] = br.Responses[k]
		}
		mergeTxn(br)
		return false, nil
	}

	anchorGroup := func(groups []rangeGroup) int {
		if !header.CreateTxnRecord || header.Txn == nil {
			return -1
		}
		for i := range groups {
			for j := range groups[i].reqs {
				if keys.Key(header.Txn.Key).Equal(groups[i].reqs[j].GetInner().Header().Key) {
					return i
				}
			}
		}
		return -1
	}

	regroups := 0
	regroupWait := func(kerr *kvpb.Error) *kvpb.Error {
		if regroups++; regroups > maxRoutingRetries {
			return kvpb.NewErrorf("routing did not converge after %d retries: %v", regroups, kerr)
		}
		return routingBackoff(ctx, regroups)
	}
	repartition := func(gs []rangeGroup) ([]rangeGroup, *kvpb.Error) {
		var reqs []kvpb.RequestUnion
		var idxs []int
		for _, g := range gs {
			reqs = append(reqs, g.reqs...)
			idxs = append(idxs, g.indices...)
		}
		return db.partitionByRange(ctx, reqs, idxs)
	}

	for len(groups) > 0 {
		// Anchor-first when >1 group remains and the record is not yet
		// created.
		if ai := anchorGroup(groups); ai >= 0 && len(groups) > 1 {
			regroup, kerr := sendGroup(groups[ai])
			if regroup {
				if werr := regroupWait(kerr); werr != nil {
					return nil, werr
				}
				var perr *kvpb.Error
				if groups, perr = repartition(groups); perr != nil {
					return nil, perr
				}
				continue
			}
			if kerr != nil {
				return nil, kerr
			}
			groups = append(groups[:ai:ai], groups[ai+1:]...)
			header.CreateTxnRecord = false // record exists; siblings never create
		}

		var (
			wg       sync.WaitGroup
			resMu    sync.Mutex
			retry    []rangeGroup
			firstErr *kvpb.Error
			lastRe   *kvpb.Error
		)
		sem := make(chan struct{}, maxParallelSubBatches)
		for _, g := range groups {
			wg.Add(1)
			sem <- struct{}{}
			go func(g rangeGroup) {
				defer wg.Done()
				defer func() { <-sem }()
				regroup, kerr := sendGroup(g)
				resMu.Lock()
				defer resMu.Unlock()
				switch {
				case regroup:
					retry = append(retry, g)
					lastRe = kerr
				case kerr != nil && firstErr == nil:
					firstErr = kerr
				}
			}(g)
		}
		wg.Wait()
		if firstErr != nil {
			return nil, firstErr
		}
		if len(retry) == 0 {
			break
		}
		if werr := regroupWait(lastRe); werr != nil {
			return nil, werr
		}
		var perr *kvpb.Error
		if groups, perr = repartition(retry); perr != nil {
			return nil, perr
		}
	}
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

// metaStaleWait bounds how long lookupMeta keeps re-scanning /meta while
// the records it finds do not cover the key. Splits and merges repair
// addressing with separate writes after they commit, so a scan can land
// between them (the old record gone, the new one not yet there, or the
// old one still naming a span that has since shrunk); the repair lands
// within milliseconds, and a batch should wait for it rather than fail.
const metaStaleWait = 3 * time.Second

// lookupMeta scans /meta for the first record whose range ends beyond key.
// The scan is inconsistent (never blocks on intents) and itself routed via
// the cache — the bootstrap invariant is that a descriptor covering the
// meta span is always seeded. A record that does not cover the key, or no
// record at all, is treated as an addressing repair in flight and
// re-scanned with backoff for up to metaStaleWait.
func (db *DB) lookupMeta(ctx context.Context, key keys.Key) (kvpb.RangeDescriptor, error) {
	start := keys.Key(keys.RangeMetaKey(key)).Next()
	_, metaEnd := keys.MetaSpan()
	header := kvpb.BatchHeader{Timestamp: db.clock.Now(), ReadInconsistent: true}
	scan := &kvpb.ScanRequest{RequestHeader: kvpb.RequestHeader{Key: start, EndKey: metaEnd}, MaxRows: 1}

	deadline := time.Now().Add(metaStaleWait)
	backoff := 5 * time.Millisecond
	var stale error
	for regroups := 0; ; {
		desc, ok := db.cache.Lookup(start)
		if !ok {
			return kvpb.RangeDescriptor{}, fmt.Errorf("no routing descriptor for the meta span; cluster bootstrap incomplete")
		}
		br, regroup, kerr := db.sendPartial(ctx, &header, []kvpb.RequestUnion{{Scan: scan}}, desc)
		if regroup {
			if regroups++; regroups >= 5 {
				return kvpb.RangeDescriptor{}, fmt.Errorf("meta lookup for %s did not converge", key)
			}
			continue
		}
		if kerr != nil {
			return kvpb.RangeDescriptor{}, kerr
		}
		rows := br.Responses[0].Scan.Rows
		if len(rows) == 0 {
			stale = fmt.Errorf("no meta record covering key %s", key)
		} else {
			var d kvpb.RangeDescriptor
			if err := json.Unmarshal(rows[0].Value, &d); err != nil {
				return kvpb.RangeDescriptor{}, fmt.Errorf("corrupt meta record: %w", err)
			}
			if d.ContainsKey(key) {
				return d, nil
			}
			stale = fmt.Errorf("meta record %s does not cover key %s (stale addressing)", &d, key)
			// Whatever this gateway cached for that range is no fresher.
			db.cache.Evict(d.RangeID)
		}
		if time.Now().After(deadline) {
			return kvpb.RangeDescriptor{}, fmt.Errorf("addressing did not converge within %s: %w", metaStaleWait, stale)
		}
		select {
		case <-ctx.Done():
			return kvpb.RangeDescriptor{}, ctx.Err()
		case <-time.After(backoff):
		}
		if backoff *= 2; backoff > 100*time.Millisecond {
			backoff = 100 * time.Millisecond
		}
		header.Timestamp = db.clock.Now() // read at a fresh timestamp: the repair may have just landed
	}
}

// routingBackoff paces a routing retry: a few immediate retries absorb
// simple staleness; after that, back off briefly — mid-flight replica
// moves and meta repairs need real time to land, and spinning burns the
// retry budget in microseconds. Returns the ctx error once it expires.
func routingBackoff(ctx context.Context, regroups int) *kvpb.Error {
	if regroups <= 3 {
		return nil
	}
	delay := time.Duration(regroups) * 10 * time.Millisecond
	if delay > 200*time.Millisecond {
		delay = 200 * time.Millisecond
	}
	select {
	case <-ctx.Done():
		return kvpb.NewError(ctx.Err())
	case <-time.After(delay):
		return nil
	}
}

// sendScan executes a scan, stitching across range boundaries.
func (db *DB) sendScan(ctx context.Context, header kvpb.BatchHeader, req *kvpb.ScanRequest) (*kvpb.ScanResponse, *kvpb.Error) {
	if req.Reverse {
		return db.sendReverseScan(ctx, header, req)
	}
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
		sub := &kvpb.ScanRequest{RequestHeader: kvpb.RequestHeader{Key: cur, EndKey: end}, MaxRows: remaining, ForUpdate: req.ForUpdate}
		// The transaction record is created on the anchor key's range only:
		// scope the flag to the one segment covering it (locking scans may
		// be a transaction's first, anchoring operation).
		gh := header
		if gh.CreateTxnRecord && gh.Txn != nil {
			a := keys.Key(gh.Txn.Key)
			gh.CreateTxnRecord = cur.Compare(a) <= 0 && a.Less(end)
		}
		br, regroup, kerr := db.sendPartial(ctx, &gh, []kvpb.RequestUnion{{Scan: sub}}, desc)
		header.Timestamp = gh.Timestamp
		if regroup {
			if regroups++; regroups > maxRoutingRetries {
				return nil, kvpb.NewErrorf("scan routing did not converge: %v", kerr)
			}
			if berr := routingBackoff(ctx, regroups); berr != nil {
				return nil, berr
			}
			continue
		}
		if kerr != nil {
			return nil, kerr
		}
		sr := br.Responses[0].Scan
		out.Rows = append(out.Rows, sr.Rows...)
		if req.MaxRows > 0 {
			remaining -= int64(len(sr.Rows))
			if remaining <= 0 {
				switch {
				case len(sr.Resume) > 0:
					out.Resume = sr.Resume
				case end.Less(req.EndKey):
					out.Resume = end
				}
				return out, nil
			}
		}
		if len(sr.Resume) > 0 {
			// The range paged its answer by bytes: the rest of it next.
			cur = sr.Resume
			continue
		}
		cur = end
	}
	return out, nil
}

// sendReverseScan executes a reverse scan, stitching across range
// boundaries from the END of the span backwards. The meta index answers
// "which range contains key K", not "which range precedes K", so each
// iteration walks the (cached, in-memory) descriptors forward to locate
// the LAST not-yet-scanned segment and reverse-scans it — O(ranges²)
// cache lookups, always against fresh descriptors, so splits and merges
// racing the scan resolve through the ordinary regroup path.
func (db *DB) sendReverseScan(ctx context.Context, header kvpb.BatchHeader, req *kvpb.ScanRequest) (*kvpb.ScanResponse, *kvpb.Error) {
	out := &kvpb.ScanResponse{}
	curEnd := req.EndKey.Clone()
	remaining := req.MaxRows
	regroups := 0
	for req.Key.Less(curEnd) {
		// Locate the last segment of [req.Key, curEnd).
		var segStart, segEnd keys.Key
		var desc kvpb.RangeDescriptor
		s := req.Key.Clone()
		for s.Less(curEnd) {
			d, kerr := db.descForKey(ctx, s)
			if kerr != nil {
				return nil, kerr
			}
			e := curEnd
			if d.EndKey.Less(e) {
				e = d.EndKey
			}
			segStart, segEnd, desc = s, e, d
			s = e
		}
		sub := &kvpb.ScanRequest{RequestHeader: kvpb.RequestHeader{Key: segStart, EndKey: segEnd}, MaxRows: remaining, Reverse: true}
		// Anchor scoping, as in the forward path.
		gh := header
		if gh.CreateTxnRecord && gh.Txn != nil {
			a := keys.Key(gh.Txn.Key)
			gh.CreateTxnRecord = segStart.Compare(a) <= 0 && a.Less(segEnd)
		}
		br, regroup, kerr := db.sendPartial(ctx, &gh, []kvpb.RequestUnion{{Scan: sub}}, desc)
		header.Timestamp = gh.Timestamp
		if regroup {
			if regroups++; regroups > maxRoutingRetries {
				return nil, kvpb.NewErrorf("reverse scan routing did not converge: %v", kerr)
			}
			if berr := routingBackoff(ctx, regroups); berr != nil {
				return nil, berr
			}
			continue
		}
		if kerr != nil {
			return nil, kerr
		}
		sr := br.Responses[0].Scan
		out.Rows = append(out.Rows, sr.Rows...)
		if req.MaxRows > 0 {
			remaining -= int64(len(sr.Rows))
			if remaining <= 0 {
				switch {
				case len(sr.Resume) > 0:
					out.Resume = sr.Resume
				case req.Key.Less(segStart):
					out.Resume = segStart
				}
				return out, nil
			}
		}
		if len(sr.Resume) > 0 {
			// The range paged its answer by bytes: the rest of it (below
			// the resume key, exclusive) next.
			curEnd = sr.Resume
			continue
		}
		curEnd = segStart
	}
	return out, nil
}

// sendExport executes an export, split across range boundaries and
// stitched back together like sendScan. Exports are never transactional.
func (db *DB) sendExport(ctx context.Context, header kvpb.BatchHeader, req *kvpb.ExportRequest) (*kvpb.ExportResponse, *kvpb.Error) {
	out := &kvpb.ExportResponse{}
	cur := req.Key.Clone()
	remaining := req.MaxRecords
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
		sub := &kvpb.ExportRequest{RequestHeader: kvpb.RequestHeader{Key: cur, EndKey: end}, StartTS: req.StartTS, MaxRecords: remaining}
		br, regroup, kerr := db.sendPartial(ctx, &header, []kvpb.RequestUnion{{Export: sub}}, desc)
		if regroup {
			if regroups++; regroups > maxRoutingRetries {
				return nil, kvpb.NewErrorf("export routing did not converge: %v", kerr)
			}
			continue
		}
		if kerr != nil {
			return nil, kerr
		}
		er := br.Responses[0].Export
		out.Records = append(out.Records, er.Records...)
		if len(er.Resume) > 0 {
			out.Resume = er.Resume
			return out, nil
		}
		if req.MaxRecords > 0 {
			remaining -= int64(len(er.Records))
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

// overloadBackoff is the jittered exponential delay before retrying a
// write the leader shed under storage backpressure: 10ms doubling to a 1s
// cap, ±50% jitter.
func overloadBackoff(n int) time.Duration {
	d := 10 * time.Millisecond << uint(min(n, 7)) // 10ms .. 1.28s
	if d > time.Second {
		d = time.Second
	}
	return d/2 + time.Duration(rand.Int64N(int64(d)))
}

// sendToRange tries the range's replicas — leader hint first, local replica
// next — retrying around NotLeader and transport errors until ctx expires.
func (db *DB) sendToRange(ctx context.Context, ba *kvpb.BatchRequest, desc kvpb.RangeDescriptor) (*kvpb.BatchResponse, *kvpb.Error) {
	ba.Header.RangeID = desc.RangeID
	// Follower-read fallback accounting: a stale read the gateway cannot
	// serve from its own replica goes to the leader instead. Counted at
	// most once per sub-batch — either the gateway holds no replica of
	// this range at all, or its replica answers NotLeader (closed
	// timestamp too old, or an intent).
	fallbackCounted := false
	if ba.Header.StaleRead && db.local != nil && !db.holdsReplica(desc) {
		metrics.FollowerReadFallbacks.Inc()
		fallbackCounted = true
	}
	var lastErr *kvpb.Error
	overloads := 0
	for attempt := 0; ; attempt++ {
		if err := ctx.Err(); err != nil {
			if lastErr != nil {
				return nil, lastErr
			}
			return nil, kvpb.NewError(err)
		}
		for _, target := range db.replicaOrder(desc, ba.Header.StaleRead) {
			br, kerr, err := db.sendToReplica(ctx, ba, target)
			if err != nil {
				lastErr = kvpb.NewErrorf("n%d unreachable: %v", target, err)
				continue
			}
			if kerr == nil {
				if !ba.Header.StaleRead {
					// A follower serving a stale read is not the leader;
					// pinning the hint to it would misroute writes.
					db.cache.SetHint(desc.RangeID, target)
				}
				return br, nil
			}
			if kerr.StorageOverloaded != nil {
				// The leader shed the write under engine backpressure before
				// proposing anything, so resending the identical batch is
				// safe — but retry with jittered exponential backoff: a hot
				// retry loop is exactly the load an overloaded store cannot
				// absorb.
				lastErr = kerr
				select {
				case <-ctx.Done():
					return nil, kerr
				case <-time.After(overloadBackoff(overloads)):
				}
				overloads++
				break // restart from the leader hint
			}
			if kerr.NotLeader != nil {
				if ba.Header.StaleRead && !fallbackCounted && db.local != nil && target == db.local.store.NodeID() {
					// The local replica could not serve the stale read.
					metrics.FollowerReadFallbacks.Inc()
					fallbackCounted = true
				}
				db.cache.SetHint(desc.RangeID, kerr.NotLeader.LeaderHint)
				lastErr = kerr
				continue
			}
			if kerr.RangeNotFound != nil {
				// This node shed its replica (a stale descriptor or hint
				// pointed at it); another target may still hold one — the
				// fallback order ends with every known node.
				db.cache.SetHint(desc.RangeID, 0)
				lastErr = kerr
				continue
			}
			// Definitive KV-level answer (including txn conflicts and
			// routing corrections): return to caller.
			return br, kerr
		}
		// A full pass finding no replica anywhere means the descriptor
		// itself is stale (the range moved or was reshaped): surface the
		// RangeNotFound so the caller evicts and re-resolves via meta.
		if lastErr != nil && lastErr.RangeNotFound != nil {
			return nil, lastErr
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

// replicaOrder yields target nodes: leader hint, then local, then the
// rest. A stale read prefers the LOCAL replica over the leader hint — any
// replica whose closed timestamp covers it can serve, and local is the
// whole point of follower reads.
func (db *DB) replicaOrder(desc kvpb.RangeDescriptor, staleOK bool) []base.NodeID {
	var order []base.NodeID
	seen := map[base.NodeID]bool{}
	add := func(n base.NodeID) {
		if n != 0 && !seen[n] {
			seen[n] = true
			order = append(order, n)
		}
	}
	if staleOK && db.holdsReplica(desc) {
		add(db.local.store.NodeID())
	}
	add(db.cache.Hint(desc.RangeID))
	if db.holdsReplica(desc) {
		add(db.local.store.NodeID())
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

// ScanAt reads [start, end) inconsistently at a FIXED timestamp: intents
// are ignored and the result set is exactly the rows committed at or below
// ts, however long the scan takes. Used where a stable snapshot matters
// more than recency (the index backfill's planning sweep).
func (db *DB) ScanAt(ctx context.Context, start, end keys.Key, max int64, ts hlc.Timestamp) ([]kvpb.KeyValue, error) {
	ba := &kvpb.BatchRequest{Header: kvpb.BatchHeader{Timestamp: ts, ReadInconsistent: true}}
	ba.Add(&kvpb.ScanRequest{RequestHeader: kvpb.RequestHeader{Key: start, EndKey: end}, MaxRows: max})
	br, kerr := db.Send(ctx, ba)
	if kerr != nil {
		return nil, kerr
	}
	return br.Responses[0].Scan.Rows, nil
}

// PushAndResolveIntents timidly pushes the owners of conflicting intents
// on behalf of a non-transactional reader (an export): finalized, staged,
// or expired-and-abandoned transactions get their intents resolved;
// healthy live ones are left alone. Returns true when every intent was
// resolved and the read can retry immediately; false means a live
// transaction still blocks it — wait and retry.
func (db *DB) PushAndResolveIntents(ctx context.Context, intents []storage.Intent) (bool, error) {
	all := true
	for _, intent := range intents {
		ba := &kvpb.BatchRequest{Header: kvpb.BatchHeader{Timestamp: db.clock.Now()}}
		ba.Add(&kvpb.PushTxnRequest{
			RequestHeader: kvpb.RequestHeader{Key: keys.Key(intent.Txn.Key).Clone()},
			PusherTxn:     nil, // non-transactional pusher: no priority contest
			PusheeTxn:     intent.Txn,
			PushAbort:     false, // succeed only on finalized or expired pushees
			Now:           db.clock.Now(),
		})
		br, kerr := db.Send(ctx, ba)
		if kerr != nil {
			return false, kerr
		}
		push := br.Responses[0].PushTxn
		if push.Status == enginepb.STAGING {
			db.recoverStagedTxn(ctx, intent.Txn, push)
			br, kerr = db.Send(ctx, ba)
			if kerr != nil {
				return false, kerr
			}
			push = br.Responses[0].PushTxn
		}
		switch push.Status {
		case enginepb.COMMITTED, enginepb.ABORTED:
			rba := &kvpb.BatchRequest{Header: kvpb.BatchHeader{Timestamp: db.clock.Now()}}
			rba.Add(&kvpb.ResolveIntentRequest{
				RequestHeader: kvpb.RequestHeader{Key: intent.Key.Clone()},
				TxnID:         intent.Txn.ID,
				Status:        push.Status,
				CommitTS:      push.CommitTS,
			})
			if _, kerr := db.Send(ctx, rba); kerr != nil {
				return false, kerr
			}
		default:
			all = false // pushee alive; caller waits
		}
	}
	return all, nil
}

// ExportSpan exports [start, end) as of ts: for every key that changed in
// (startTS, ts], its newest version at or below ts — deletions included as
// tombstone records. startTS zero exports everything visible at ts (a full
// backup); non-zero exports the delta since a prior export (an
// incremental). Consistent: conflicts with (pushes) intents at or below
// ts, so the result is exactly the committed state at ts. max bounds the
// records returned; a non-nil Resume says where to continue.
func (db *DB) ExportSpan(ctx context.Context, start, end keys.Key, startTS, ts hlc.Timestamp, max int64) (*kvpb.ExportResponse, error) {
	ba := &kvpb.BatchRequest{Header: kvpb.BatchHeader{Timestamp: ts}}
	ba.Add(&kvpb.ExportRequest{RequestHeader: kvpb.RequestHeader{Key: start, EndKey: end}, StartTS: startTS, MaxRecords: max})
	br, kerr := db.Send(ctx, ba)
	if kerr != nil {
		return nil, kerr
	}
	return br.Responses[0].Export, nil
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

// AdminTransferLease moves leadership (and with it the lease) of the range
// covering key to target, which must already hold a replica.
func (db *DB) AdminTransferLease(ctx context.Context, key keys.Key, target base.NodeID) error {
	ba := &kvpb.BatchRequest{Header: kvpb.BatchHeader{Timestamp: db.clock.Now()}}
	ba.Add(&kvpb.AdminTransferLeaseRequest{RequestHeader: kvpb.RequestHeader{Key: key}, Target: target})
	br, kerr := db.Send(ctx, ba)
	if kerr != nil {
		return kerr
	}
	// Route follow-up traffic straight at the new leader.
	db.cache.SetHint(br.Responses[0].AdminTransferLease.Desc.RangeID, target)
	return nil
}

// AdminMerge merges the range containing key with its right neighbor.
func (db *DB) AdminMerge(ctx context.Context, key keys.Key) (*kvpb.AdminMergeResponse, error) {
	ba := &kvpb.BatchRequest{Header: kvpb.BatchHeader{Timestamp: db.clock.Now()}}
	ba.Add(&kvpb.AdminMergeRequest{RequestHeader: kvpb.RequestHeader{Key: key}})
	br, kerr := db.Send(ctx, ba)
	if kerr != nil {
		return nil, kerr
	}
	resp := br.Responses[0].AdminMerge
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
