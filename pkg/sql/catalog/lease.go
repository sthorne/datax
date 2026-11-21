package catalog

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"sync/atomic"
	"time"

	"github.com/google/uuid"

	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/kvclient"
	"github.com/sthorne/datax/pkg/util/hlc"
	"github.com/sthorne/datax/pkg/util/log"
	"github.com/sthorne/datax/pkg/util/stop"
)

// Descriptor leases give schema changes a two-step safety story across
// gateways. Every gateway that caches a descriptor holds a lease record
// (/system/lease/<descID>/<gateway>) naming the version it may be using
// and an expiration; a background loop renews the lease at TTL/3 by
// re-reading the descriptor — which is also how new versions are adopted.
// DDL, after committing a new version, DRAINS: it waits until every live
// lease on the descriptor is at (or past) the new version, so once the
// statement returns, no gateway is still planning against the old schema.
// Expired leases cannot be used (the cache entry expires with them), so a
// crashed gateway delays the drain by at most one TTL.
//
// Three rules keep the drain sound. The first two are against a gateway
// whose renewals stall (issue #110): the cache entry carries the very
// expiration written into the lease record, so the cache never outlives
// what the drain waits for; and a transaction that plans against a leased
// descriptor takes the lease's expiration as its commit deadline
// (kvclient.Txn.UpdateDeadline), so it cannot commit after the drain has
// written the lease off.
//
// The third is against a gateway whose renewals are perfectly healthy
// (issue #185). Adopting a new version is not the same as having stopped
// using the old one: a statement is pinned to the descriptor it planned
// against, and one served from the cache never read that descriptor
// inside its own transaction, so nothing but its deadline stops it
// committing after the renewal that superseded it. Publishing the new
// version therefore also publishes PriorExpiration — the expiration of
// the entry that was handed out — and the drain waits for that too. A
// statement's deadline is exactly that expiration, so once it passes,
// no statement anywhere can still commit against the old schema.
//
// The cost is that a schema change waits out the superseded entry, up to
// one TTL, whenever a gateway served that descriptor from cache within
// the last renewal interval. Draining is what makes an online schema
// change safe; this is the part of it that was missing.
//
// A statement that took its descriptor from the UNCACHED path holds no
// deadline and needs none: it read the descriptor inside its own
// transaction, so a schema change committing after that read fails the
// transaction's refresh.

// Drops (issue #377). A drop has no next version to drain to: the
// descriptor and its name go in the dropping transaction. Draining it
// still means the same two things — once DROP returns, no gateway plans
// against the table, and no statement planned against it can still
// commit — so a gateway that sees its cached descriptor gone answers
// the drain with an acknowledgement rather than letting its lease
// lapse: it uncaches the table and rewrites its lease record at
// droppedLeaseVersion, a version every drain counts as adopted, with
// PriorExpiration set to the latest deadline it handed out under the
// entry (ackDropped). After the dropping transaction commits, the
// session drains each dropped descriptor by id (FinishDrop) as it
// drains any other schema change: until every live lease on it is an
// acknowledgement whose handed-out deadlines have passed. Lookups after
// that resolve nothing and fail with 42P01, and a write planned before
// the drop fails its deadline rather than commit into the dropped span.
//
// The acknowledgement keeps the lease's own expiration, so a record
// stays live until every deadline taken under the lease has passed;
// the dropped-table reclaim does not clear a table's span while any
// lease on it is live (DescriptorLeased).
//
// A lease transaction that read the descriptor before the drop
// committed can still be installing its entry after this gateway has
// acknowledged the drop — a renewal, or a first use. Every lease
// transaction is registered as in flight (beginFlight) before it reads,
// and a drop seen on the gateway marks the descriptor in every flight
// (markDroppedLocked); an install whose flight was marked after it began
// is refused, and the lease it took is acknowledged as dropped too.

// Eviction (issue #311). A gateway holds a lease, and renews it every
// TTL/3, for each descriptor in its cache; left alone, the cache and the
// renewal traffic grow with every table the gateway has ever resolved.
// Two rules bound both.
//
// An entry no statement has used for idleLeasePeriods lease periods is
// dropped by the renewal loop, and its lease record deleted rather than
// renewed. That is safe because of what "unused" means here: lastUsed
// moves whenever the entry is handed to a statement, a hand-out carries a
// deadline of at most one TTL, and so every deadline taken under the
// entry or its predecessors passed long ago (the loop checks outstanding
// rather than trust the arithmetic). With no statement left that could
// commit under the lease, deleting the record tells a drain the truth —
// this gateway is not using the descriptor — and a schema change on an
// idle table waits for nothing, where a record merely left to lapse would
// have held it up for the rest of a TTL. The next use is an ordinary
// first use: the descriptor is read and the lease taken in one
// transaction, so it is the current version.
//
// The delete is conditional on the record still being the evicted
// entry's (releaseLease). A lookup may take a fresh lease on the same
// descriptor between the entry leaving the cache and the delete running;
// deleting that record would leave a cached descriptor no drain waits
// for, which is the failure leases exist to prevent.
//
// And the cache holds at most maxEntries descriptors. A lookup that finds
// it full neither caches nor leases: the statement plans against the
// descriptor its own transaction read, the path a failed acquisition has
// always taken, at the price of two point reads per statement until idle
// entries make room. Nothing in use is ever pushed out to admit a
// newcomer, because an entry in use is the record of deadlines a drain
// must still wait for.

// DefaultDescCacheSize is how many descriptors a leasing gateway caches,
// and so the most leases it renews: at the default TTL, about three
// hundred lease writes a second when full.
const DefaultDescCacheSize = 1024

// idleLeasePeriods is how many lease TTLs an entry may go unused before
// the renewal loop evicts it.
const idleLeasePeriods = 3

// SetCacheLimits overrides the cache bound and the idle time after which
// an unused entry is evicted (zero keeps the default for either). Call
// before serving sessions.
func (a *Accessor) SetCacheLimits(maxEntries int, idleAfter time.Duration) {
	a.mu.Lock()
	a.maxEntries, a.idleAfter = maxEntries, idleAfter
	a.mu.Unlock()
}

func (a *Accessor) maxEntriesLocked() int {
	if a.maxEntries > 0 {
		return a.maxEntries
	}
	return DefaultDescCacheSize
}

// reserveSlot claims room in the cache for key before its lease is
// taken. An entry already there (expired, about to be replaced) needs
// none; otherwise the slot is counted until install or releaseSlot, so
// that lookups racing for the last slots cannot together exceed the
// bound. ok is false when the cache is full.
func (a *Accessor) reserveSlot(key string) (reserved, ok bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if _, present := a.cache[key]; present {
		return false, true
	}
	if len(a.cache)+a.acquiring >= a.maxEntriesLocked() {
		return false, false
	}
	a.acquiring++
	return true, true
}

func (a *Accessor) releaseSlot(reserved bool) {
	if !reserved {
		return
	}
	a.mu.Lock()
	a.acquiring--
	a.mu.Unlock()
}

// install puts a leased entry in the cache, consuming the reservation if
// there was one. It reports false, caching nothing, when the lookup had
// counted on replacing an entry that has since gone and the cache has no
// room left — or, with dropped true, when the descriptor was seen dropped
// on this gateway after the lease transaction began (lease.go, "Drops"),
// with the deadline still outstanding from the drop.
func (a *Accessor) install(key string, entry *cachedDesc, reserved bool, seq uint64) (ok, dropped bool, outstanding int64) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if reserved {
		a.acquiring--
	}
	if dropped, out := a.droppedSinceLocked(key, entry.desc.ID, seq); dropped {
		return false, true, out
	}
	if !reserved {
		if _, present := a.cache[key]; !present && len(a.cache)+a.acquiring >= a.maxEntriesLocked() {
			return false, false, 0
		}
	}
	a.cache[key] = entry
	return true, false, 0
}

// evictIdle removes the entries no statement has used for the idle time
// and returns them, for their leases to be released.
func (a *Accessor) evictIdle() []*cachedDesc {
	a.mu.Lock()
	defer a.mu.Unlock()
	idle := a.idleAfter
	if idle <= 0 {
		idle = idleLeasePeriods * a.ttl
	}
	now := a.nowWallLocked()
	var out []*cachedDesc
	for key, c := range a.cache {
		if now-c.lastUsed < idle.Nanoseconds() || c.outstanding(now) {
			continue
		}
		delete(a.cache, key)
		out = append(out, c)
	}
	return out
}

// testingBeforeLeaseRelease, when set, runs after an entry has left the
// cache and before its lease record is released: where a test parks an
// eviction while the table is used again or its schema changed.
var testingBeforeLeaseRelease atomic.Pointer[func(a *Accessor, descID uint64)]

// SetTestingBeforeLeaseRelease installs (or, with nil, removes) the hook.
func SetTestingBeforeLeaseRelease(f func(a *Accessor, descID uint64)) {
	if f == nil {
		testingBeforeLeaseRelease.Store(nil)
		return
	}
	testingBeforeLeaseRelease.Store(&f)
}

// releaseLease deletes this gateway's lease record on a descriptor,
// provided the record is still the one carrying expiration — the lease of
// the entry being given up, and not one taken since. Read and delete are
// one transaction, so an acquisition that commits in between fails the
// read's validation and the retry finds a record that is no longer ours
// to delete. (Two leases of one gateway on one descriptor cannot share an
// expiration: each is the HLC's reading inside its own transaction plus
// the TTL.) A failure leaves the record to lapse at its expiration, which
// is all a drain ever waits for.
func (a *Accessor) releaseLease(ctx context.Context, descID uint64, expiration int64) {
	if hook := testingBeforeLeaseRelease.Load(); hook != nil {
		(*hook)(a, descID)
	}
	key := keys.DescLeaseKey(descID, a.gateway)
	err := a.db.RunTxn(ctx, "lease-release", func(ctx context.Context, txn *kvclient.Txn) error {
		raw, err := txn.Get(ctx, key)
		if err != nil || raw == nil {
			return err
		}
		var l descLease
		if json.Unmarshal(raw, &l) != nil || l.Expiration != expiration {
			return nil
		}
		return txn.Delete(ctx, key)
	})
	if err != nil {
		log.Debugf("releasing the lease on descriptor %d: %v", descID, err)
	}
}

// DefaultDescLeaseTTL is how long a gateway's descriptor lease (and cache
// entry) lives without renewal.
const DefaultDescLeaseTTL = 10 * time.Second

// descLease is the stored lease record.
type descLease struct {
	Version    uint64 `json:"version"`
	Expiration int64  `json:"expiration"` // HLC wall nanos
	// PriorExpiration is set when this gateway adopted Version while a
	// statement it had already handed the PREVIOUS version to could still
	// commit: that statement is bounded by the replaced entry's
	// expiration, which is this value. A drain waiting for Version is not
	// finished with this gateway until it passes. Zero when nothing is
	// outstanding (issue #185).
	PriorExpiration int64 `json:"prior_expiration,omitempty"`
}

// leaseInfo is what one acquisition established.
type leaseInfo struct {
	expiration, priorExpiration, handedOutExpiration int64
	// flightSeq is where the drop marks stood when the lease
	// transaction began (beginFlight): a mark above it is a drop the
	// transaction's read may predate.
	flightSeq uint64
}

// droppedLeaseVersion is the version a lease record claims once its
// gateway has seen the descriptor dropped and stopped using it (issue
// #377): at or past any version a drain waits for, so only the record's
// PriorExpiration — the deadlines still outstanding — holds a drain up.
const droppedLeaseVersion = math.MaxUint64

// TestingPauseRenewal stops (or resumes) the background lease renewal:
// tests use it to let this gateway's leases expire while its sessions
// keep running, the shape of a stalled gateway.
func (a *Accessor) TestingPauseRenewal(paused bool) { a.renewalPaused.Store(paused) }

// TestingRenewNow runs one renewal pass — what the renewal loop does on
// a tick — so a test can put a renewal exactly where it wants one
// (issue #234). Errors are returned rather than logged.
func (a *Accessor) TestingRenewNow(ctx context.Context) error { return a.renewAll(ctx) }

// TestingGatewayID is the identity this accessor's lease records are
// keyed by (keys.DescLeaseKey).
func (a *Accessor) TestingGatewayID() uuid.UUID {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.gateway
}

// TestingCacheSize is the number of descriptors this accessor caches.
func (a *Accessor) TestingCacheSize() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return len(a.cache)
}

// StartLeasing enables leasing on this accessor and starts the renewal
// loop. Call once, before serving sessions. ttl <= 0 uses the default.
func (a *Accessor) StartLeasing(db *kvclient.DB, clock *hlc.Clock, stopper *stop.Stopper, ttl time.Duration) error {
	if ttl <= 0 {
		ttl = DefaultDescLeaseTTL
	}
	a.mu.Lock()
	a.leasing = true
	a.db = db
	a.clock = clock
	a.gateway = uuid.New()
	a.ttl = ttl
	a.mu.Unlock()
	return stopper.RunWorker(a.renewalLoop)
}

func (a *Accessor) renewalLoop(ctx context.Context) {
	t := time.NewTicker(a.ttl / 3)
	defer t.Stop()
	// The schema generations are watched more often than leases are
	// renewed, so a schema change's drain is not held up for a whole
	// renewal period (schemacache.go, pollSchemaGens).
	poll := time.NewTicker(max(a.ttl/20, 10*time.Millisecond))
	defer poll.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if a.renewalPaused.Load() {
				continue
			}
			if err := a.renewAll(ctx); err != nil {
				log.Debugf("lease renewal: %v", err)
			}
		case <-poll.C:
			if a.renewalPaused.Load() {
				continue
			}
			if err := a.pollSchemaGens(ctx); err != nil {
				log.Debugf("schema generation poll: %v", err)
			}
		}
	}
}

// renewAll gives up the leases of the tables no statement has used
// lately (see "Eviction" above) and renews this gateway's lease on every
// other cached table, adopting any new version; the first error is
// returned after every table has been tried.
func (a *Accessor) renewAll(ctx context.Context) error {
	for _, c := range a.evictIdle() {
		a.releaseLease(ctx, c.desc.ID, c.expiration)
	}
	a.pruneStats()
	var first error
	if err := a.renewSchemaSets(ctx); err != nil {
		first = fmt.Errorf("schema lease renewal: %w", err)
	}
	a.mu.Lock()
	names := make([]string, 0, len(a.cache))
	for name := range a.cache {
		names = append(names, name)
	}
	a.mu.Unlock()
	for _, name := range names {
		if _, err := a.refreshOne(ctx, name); err != nil && first == nil {
			first = fmt.Errorf("lease renewal for %q: %w", name, err)
		}
	}
	return first
}

// refreshOne re-reads a table's descriptor (adopting any new version),
// renews this gateway's lease on it, and updates the cache. A dropped
// table is uncached and the drop acknowledged in its lease record
// (ackDropped). Returns the fresh descriptor (nil when dropped).
func (a *Accessor) refreshOne(ctx context.Context, name string) (*TableDescriptor, error) {
	dbID, bare := splitCacheKey(name)
	desc, info, done, err := a.acquireLease(ctx, dbID, bare, name)
	defer done() // after the cache below holds the new entry
	if err != nil {
		return nil, err // keep the old (still-live) cache entry
	}
	if hook := testingAfterLeaseCommit.Load(); hook != nil {
		(*hook)(a, name)
	}
	if desc == nil {
		a.mu.Lock()
		old, had := a.cache[name]
		delete(a.cache, name)
		var out int64
		if had && old.desc != nil {
			out = old.outstandingExpiration()
			a.markDroppedLocked(old.desc.ID, out, name)
		}
		a.mu.Unlock()
		if had && old.desc != nil {
			// Only if the record is still this entry's: after a rename
			// the same descriptor may already be cached, and leased,
			// under its new name.
			a.ackDropped(ctx, old.desc.ID, old.expiration, out)
		}
		return nil, nil
	}
	entry := &cachedDesc{desc: desc, expiration: info.expiration, priorExpiration: info.priorExpiration, handedOutExpiration: info.handedOutExpiration}
	a.mu.Lock()
	if dropped, out := a.droppedSinceLocked(name, desc.ID, info.flightSeq); dropped {
		// The descriptor this transaction read has been dropped since
		// (see "Drops" above): the entry must not be installed, and the
		// lease just taken is given up the same way.
		if cur, ok := a.cache[name]; ok && cur.desc != nil && cur.desc.ID == desc.ID {
			delete(a.cache, name)
			out = max(out, cur.outstandingExpiration())
		}
		a.mu.Unlock()
		a.ackDropped(ctx, desc.ID, info.expiration, max(out, info.priorExpiration, info.handedOutExpiration))
		return nil, nil
	}
	old, had := a.cache[name]
	if had {
		// A renewal is not a use.
		entry.lastUsed = old.lastUsed
	} else {
		entry.lastUsed = a.nowWallLocked()
	}
	fits := had || len(a.cache)+a.acquiring < a.maxEntriesLocked()
	if fits {
		a.cache[name] = entry
	}
	a.mu.Unlock()
	if !fits {
		// A schema change on a table this gateway does not cache, with
		// the cache full: the lease just taken covers nothing.
		a.releaseLease(ctx, desc.ID, info.expiration)
	}
	if had && old.desc != nil && old.desc.ID != desc.ID {
		// The name now belongs to another table: the one cached under it
		// was dropped (or renamed away), and this gateway is done with it.
		a.ackDropped(ctx, old.desc.ID, old.expiration, old.outstandingExpiration())
	}
	return desc, nil
}

// testingBeforeLeaseWrite, when set, runs inside a lease transaction
// between the descriptor read and the lease write — a test's way to hold
// a gateway there while a schema change commits and drains. Atomic: the
// renewal loops of a running cluster read it while a test sets it.
var testingBeforeLeaseWrite atomic.Pointer[func(a *Accessor, name string)]

// testingAfterLeaseCommit, when set, runs in a renewal after the lease
// transaction has committed and before the cache holds the new entry: a
// test's way to hold a gateway in that window while a drain returns.
var testingAfterLeaseCommit atomic.Pointer[func(a *Accessor, name string)]

// SetTestingAfterLeaseCommit installs (or, with nil, removes) the hook.
func SetTestingAfterLeaseCommit(f func(a *Accessor, name string)) {
	if f == nil {
		testingAfterLeaseCommit.Store(nil)
		return
	}
	testingAfterLeaseCommit.Store(&f)
}

// SetTestingBeforeLeaseWrite installs (or, with nil, removes) the hook.
func SetTestingBeforeLeaseWrite(f func(a *Accessor, name string)) {
	if f == nil {
		testingBeforeLeaseWrite.Store(nil)
		return
	}
	testingBeforeLeaseWrite.Store(&f)
}

// acquireLease reads a table's descriptor and records this gateway's lease
// on it — at that version, until the returned expiration (HLC wall nanos;
// the one value the lease record and the cache entry carry) — in ONE
// serializable transaction. Read in one transaction and written in
// another, the record could claim a version that had already been
// superseded: a gateway that read version 1, then wrote its lease after
// a schema change committed version 2 and drained (finding this gateway's
// previous lease lapsed, nothing to wait for), served version 1 from its
// cache for a whole TTL. In one transaction the write cannot commit over
// a descriptor that changed since the read: the transaction restarts and
// reads the new version. A dropped table returns a nil descriptor.
//
// The lease record is visible to a drain the moment the transaction
// commits, before the caller installs the entry in the cache; a statement
// handed the old cached entry in between would plan against a version
// the drain already counts as given up. So just before committing,
// acquireLease marks name as adopting d.Version, and until done is
// called — by the caller, after installing — the cache does not hand out
// an older entry for name (supersededLocked). done is never nil.
func (a *Accessor) acquireLease(ctx context.Context, dbID uint64, bare, name string) (desc *TableDescriptor, info leaseInfo, done func(), err error) {
	marked := false
	seq := a.beginFlight(name)
	done = func() {
		if marked {
			a.unmarkAdopting(name)
		}
		a.endFlight(name)
	}
	err = a.db.RunTxn(ctx, "lease-acquire", func(ctx context.Context, txn *kvclient.Txn) error {
		desc, info = nil, leaseInfo{flightSeq: seq}
		d, err := lookupUncached(ctx, txn, dbID, bare, a.isDefaultID(dbID))
		var nf *ErrTableNotFound
		if errors.As(err, &nf) {
			return nil
		}
		if err != nil {
			return err
		}
		if hook := testingBeforeLeaseWrite.Load(); hook != nil {
			(*hook)(a, name)
		}
		now := a.clock.Now().WallTime
		e := now + a.ttl.Nanoseconds()
		prior, handedOut := a.carryOver(name, d.ID, d.Version, now)
		raw, err := json.Marshal(descLease{Version: d.Version, Expiration: e, PriorExpiration: prior})
		if err != nil {
			return err
		}
		// The write rides in the deferred batch and commits in one phase
		// with EndTxn — one raft append, what the plain Put cost — rather
		// than as an intent, a commit and a resolution. The read before it
		// is still validated at commit: a commit pushed past a scan of the
		// lease span (the drain's) refreshes the descriptor read, and a
		// changed descriptor fails the refresh.
		txn.EnablePipelining()
		var wb kvclient.WriteBatch
		wb.Put(keys.DescLeaseKey(d.ID, a.gateway), raw)
		if err := txn.RunBatch(ctx, &wb); err != nil {
			return err
		}
		desc, info = d, leaseInfo{expiration: e, priorExpiration: prior, handedOutExpiration: handedOut, flightSeq: seq}
		a.markAdopting(name, d.Version, !marked)
		marked = true
		return nil
	})
	return desc, info, done, err
}

// adoptMark counts the lease transactions on a key that are publishing
// a version, and the newest such version.
type adoptMark struct {
	n       int
	version uint64
}

// markAdopting records that a lease transaction on name is about to
// commit version; first is true on the transaction's first attempt (a
// retry raises the version but is the same transaction).
func (a *Accessor) markAdopting(name string, version uint64, first bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.adopting == nil {
		a.adopting = make(map[string]*adoptMark)
	}
	m := a.adopting[name]
	if m == nil {
		m = &adoptMark{}
		a.adopting[name] = m
	}
	if first {
		m.n++
	}
	if version > m.version {
		m.version = version
	}
}

func (a *Accessor) unmarkAdopting(name string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if m := a.adopting[name]; m != nil {
		if m.n--; m.n <= 0 {
			delete(a.adopting, name)
		}
	}
}

// supersededLocked reports whether a lease on a newer version of c's
// table is being or has been published on this gateway and its entry is
// not yet installed: c must not be handed out. Callers hold a.mu.
func (a *Accessor) supersededLocked(key string, c *cachedDesc) bool {
	m := a.adopting[key]
	return m != nil && c.desc != nil && m.version > c.desc.Version
}

// carryOver is what the entry acquired at version inherits from the one
// it replaces: prior, the expiration a drain must still wait out on this
// gateway (published in the lease record as PriorExpiration), and
// handedOut, the latest expiration handed to a statement at the version
// being kept (carried in the cache, published only once the version
// changes).
//
// A statement is pinned to the descriptor it planned against and bounded
// only by that entry's expiration (pinDeadline). Publishing the new
// version tells a drain this gateway has stopped using the old one —
// true of new statements, not of one already in flight. Without this the
// drain could return, the CREATE INDEX backfill take its boundary, and
// that statement then commit above the boundary having maintained no
// index: a row in the table with no entry in the new index (issue #185).
//
// The entry handed out need not be the one current when the new version
// is adopted: a renewal in between replaces it with a fresh entry at the
// same version, which no statement has taken, and the drain would then
// have nothing to wait for while the statement could still commit. So
// the handed-out expiration is carried from entry to entry for as long
// as the version stays the same, and published when it changes. It
// cannot be published sooner: a drain must never wait for statements
// holding the version it is draining to, or a busy gateway would keep
// it waiting for ever (issue #234).
//
// Either value is zero when there is nothing to wait for: nothing was
// handed out, or its expiration has passed.
func (a *Accessor) carryOver(name string, id, version uint64, nowWall int64) (prior, handedOut int64) {
	a.mu.Lock()
	defer a.mu.Unlock()
	old, ok := a.cache[name]
	if !ok || old.desc == nil || old.desc.ID != id {
		// Nothing cached, or another table that had the name: what was
		// handed out under that one is acknowledged with its drop.
		return 0, 0
	}
	handedOut = old.handedOutExpiration
	if old.handedOut && old.expiration > handedOut {
		handedOut = old.expiration
	}
	prior = old.priorExpiration // an older drain may still be outstanding
	if old.desc.Version < version {
		if handedOut > prior {
			prior = handedOut
		}
		handedOut = 0 // nothing at the new version has been handed out yet
	}
	if prior <= nowWall {
		prior = 0
	}
	if handedOut <= nowWall {
		handedOut = 0
	}
	return prior, handedOut
}

// FinishDDL runs after a schema change on name commits: it adopts the new
// version locally (renewing this gateway's own lease) and drains — blocks
// until every live gateway lease on the descriptor has adopted it. Nil for
// unleased accessors and for dropped tables, which the session drains by
// id with FinishDrop.
func (a *Accessor) FinishDDL(ctx context.Context, name string) error {
	return a.FinishDDLIn(ctx, DefaultDatabase, name)
}

// FinishDDLIn is FinishDDL for a table in database db (or name's own
// qualifier).
func (a *Accessor) FinishDDLIn(ctx context.Context, db, name string) error {
	a.mu.Lock()
	leasing := a.leasing
	a.mu.Unlock()
	if !leasing {
		return nil
	}
	if IsReservedTableName(name) && !IsRelationName(name) {
		// A session's temporary table is never leased (temp.go).
		return nil
	}
	if parts := SplitName(name); len(parts) == 2 && parts[0] == TempSchema {
		return nil
	}
	var cands []nameCandidate
	if err := a.db.RunTxn(ctx, "ddl-database", func(ctx context.Context, txn *kvclient.Txn) error {
		var err error
		cands, err = a.resolveWith(ctx, txn, db, name, false)
		return err
	}); err != nil {
		return err
	}
	// The first place of the name that holds a table is the table the
	// statement changed (the others hold none of that name, or come
	// after it on the search path).
	for _, c := range cands {
		desc, err := a.refreshOne(ctx, cacheKey(c.dbID, c.entry))
		if err != nil {
			return err
		}
		if desc != nil {
			return a.waitForAdoption(ctx, desc.ID, desc.Version)
		}
	}
	return nil
}

// FinishDDLByID is FinishDDL for a table named by its database's id and
// its bare name, as its descriptor has them: for a change made outside a
// statement (a restore recording its bands, issue #316).
func (a *Accessor) FinishDDLByID(ctx context.Context, dbID uint64, name string) error {
	a.mu.Lock()
	leasing := a.leasing
	a.mu.Unlock()
	if !leasing {
		return nil
	}
	return a.finishDDL(ctx, dbID, name)
}

func (a *Accessor) finishDDL(ctx context.Context, dbID uint64, name string) error {
	desc, err := a.refreshOne(ctx, cacheKey(dbID, name))
	if err != nil || desc == nil {
		return err
	}
	return a.waitForAdoption(ctx, desc.ID, desc.Version)
}

// LeasesAdopted reports whether no gateway can still use a version of
// descriptor descID below version: every live lease is at version or
// later, and none still has a statement planned against an earlier one.
// One read of the leases, without waiting (FinishDDL waits); for a
// background job that must not act before a schema change has reached
// every gateway (the seal job, after ALTER TABLE ... SET
// (encryption_key)), and that checks again later rather than give up on
// a lease that does not move — such a lease expires. True on an accessor
// without leasing.
func (a *Accessor) LeasesAdopted(ctx context.Context, descID, version uint64) (bool, error) {
	a.mu.Lock()
	leasing := a.leasing
	a.mu.Unlock()
	if !leasing {
		return true, nil
	}
	return a.adopted(ctx, descID, version)
}

func (a *Accessor) adopted(ctx context.Context, descID, version uint64) (bool, error) {
	lo, hi := keys.DescLeaseSpan(descID)
	return a.adoptedIn(ctx, lo, hi, version)
}

// adoptedIn is adopted for the lease records in [lo, hi): a table
// descriptor's, or a database's schema set's (schemacache.go).
func (a *Accessor) adoptedIn(ctx context.Context, lo, hi keys.Key, version uint64) (bool, error) {
	rows, err := a.db.Scan(ctx, lo, hi, 0)
	if err != nil {
		return false, err
	}
	nowWall := a.clock.Now().WallTime
	for _, kv := range rows {
		var l descLease
		if json.Unmarshal(kv.Value, &l) != nil {
			continue
		}
		if l.Expiration <= nowWall {
			continue // expired: unusable, nothing to wait for
		}
		if l.Version < version {
			return false, nil
		}
		// The gateway is on the new version, but it handed the previous
		// one to a statement that can still commit, up to the expiration
		// that entry carried. Until then this gateway is not done with
		// the old schema, whatever its published version says (issue
		// #185).
		if l.PriorExpiration > nowWall {
			return false, nil
		}
	}
	return true, nil
}

// waitForAdoption polls the descriptor's leases until every live one is at
// version or later. A live lease stuck below the version past 2×TTL is
// anomalous (renewal adopts within TTL/3); it is logged and no longer
// waited for — by then it has expired or its holder cannot renew.
func (a *Accessor) waitForAdoption(ctx context.Context, descID, version uint64) error {
	lo, hi := keys.DescLeaseSpan(descID)
	return a.waitForAdoptionIn(ctx, lo, hi, version, fmt.Sprintf("descriptor %d", descID))
}

// waitForAdoptionIn is waitForAdoption for the lease records in [lo,
// hi), which belong to what (for the log).
func (a *Accessor) waitForAdoptionIn(ctx context.Context, lo, hi keys.Key, version uint64, what string) error {
	deadline := time.Now().Add(2 * a.ttl)
	poll := a.ttl / 20
	if poll < 10*time.Millisecond {
		poll = 10 * time.Millisecond
	}
	for {
		adopted, err := a.adoptedIn(ctx, lo, hi, version)
		adopted = adopted && err == nil
		if adopted {
			return nil
		}
		if time.Now().After(deadline) {
			if version == droppedLeaseVersion {
				log.Warnf("%s: a live lease never acknowledged the drop; proceeding after drain timeout", what)
			} else {
				log.Warnf("%s: a live lease never adopted version %d; proceeding after drain timeout", what, version)
			}
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(poll):
		}
	}
}

// leaseFlight counts the lease transactions on a cache key that have
// begun and not yet finished installing (or giving up) their entry, and
// carries the drops marked while they were in flight.
type leaseFlight struct {
	n       int
	dropped map[uint64]dropMark // descriptor id → the latest mark
}

// dropMark records that a descriptor was seen dropped on this gateway:
// seq orders it against the flights' starts, and outstanding is the
// latest deadline the gateway had handed out under its entries for it.
type dropMark struct {
	seq         uint64
	outstanding int64
}

// beginFlight registers a lease transaction on name before it reads
// the descriptor, and returns the mark sequence it starts at.
func (a *Accessor) beginFlight(name string) uint64 {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.flights == nil {
		a.flights = make(map[string]*leaseFlight)
	}
	f := a.flights[name]
	if f == nil {
		f = &leaseFlight{}
		a.flights[name] = f
	}
	f.n++
	return a.flightSeq
}

func (a *Accessor) endFlight(name string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if f := a.flights[name]; f != nil {
		if f.n--; f.n <= 0 {
			delete(a.flights, name)
		}
	}
}

// markDroppedLocked records in the lease transactions in flight —
// those on name, or every one when name is empty — that descriptor id
// has been seen dropped: none of them may install an entry for it.
// Callers hold a.mu.
func (a *Accessor) markDroppedLocked(id uint64, outstanding int64, name string) {
	if len(a.flights) == 0 {
		return
	}
	a.flightSeq++
	for key, f := range a.flights {
		if name != "" && key != name {
			continue
		}
		if f.dropped == nil {
			f.dropped = make(map[uint64]dropMark)
		}
		m := f.dropped[id]
		m.seq = a.flightSeq
		m.outstanding = max(m.outstanding, outstanding)
		f.dropped[id] = m
	}
}

// droppedSinceLocked reports whether descriptor id was marked dropped
// in name's flight after seq, and the deadline outstanding with it.
// Callers hold a.mu.
func (a *Accessor) droppedSinceLocked(name string, id, seq uint64) (bool, int64) {
	f := a.flights[name]
	if f == nil {
		return false, 0
	}
	m, ok := f.dropped[id]
	if !ok || m.seq <= seq {
		return false, 0
	}
	return true, m.outstanding
}

// forgetIDLocked removes every cache entry for descriptor id and
// returns the latest deadline handed out under them. Callers hold a.mu.
func (a *Accessor) forgetIDLocked(id uint64) int64 {
	var out int64
	for k, c := range a.cache {
		if c.desc != nil && c.desc.ID == id {
			out = max(out, c.outstandingExpiration())
			delete(a.cache, k)
		}
	}
	return out
}

// ackDropped rewrites this gateway's lease record on a dropped
// descriptor as an acknowledgement: version droppedLeaseVersion, its
// expiration kept (extended to outstanding if that is later), and
// PriorExpiration the deadline still outstanding under it, which is
// what a drain of the drop waits out. With expectExp nonzero the record
// is rewritten only if it is still the lease of that expiration — the
// entry being given up, not one taken since (as releaseLease). A record
// with nothing left live is deleted. Errors are logged: an unacknowledged
// lease holds the drain up until it expires, no longer.
func (a *Accessor) ackDropped(ctx context.Context, id uint64, expectExp, outstanding int64) {
	key := keys.DescLeaseKey(id, a.gateway)
	err := a.db.RunTxn(ctx, "lease-ack-drop", func(ctx context.Context, txn *kvclient.Txn) error {
		raw, err := txn.Get(ctx, key)
		if err != nil || raw == nil {
			return err
		}
		var l descLease
		if json.Unmarshal(raw, &l) != nil {
			return txn.Delete(ctx, key)
		}
		if expectExp != 0 && l.Expiration != expectExp {
			return nil
		}
		now := a.clock.Now().WallTime
		prior := outstanding
		if l.Version == droppedLeaseVersion {
			prior = max(prior, l.PriorExpiration)
		}
		if prior <= now {
			prior = 0
		}
		exp := max(l.Expiration, prior)
		if exp <= now {
			return txn.Delete(ctx, key)
		}
		raw, err = json.Marshal(descLease{Version: droppedLeaseVersion, Expiration: exp, PriorExpiration: prior})
		if err != nil {
			return err
		}
		return txn.Put(ctx, key, raw)
	})
	if err != nil {
		log.Debugf("acknowledging the drop of descriptor %d: %v", id, err)
	}
}

// FinishDrop runs after a transaction that dropped table (or view) id
// commits: it uncaches the descriptor here, acknowledges the drop in
// this gateway's lease, and drains — blocks until every live lease on
// the descriptor has acknowledged it and no statement planned against
// it on any gateway can still commit (see "Drops" above). A descriptor
// that is there after all (a drop rolled back to a savepoint, a table
// restored under its id) is not drained. Nil for unleased accessors.
func (a *Accessor) FinishDrop(ctx context.Context, id uint64) error {
	a.mu.Lock()
	leasing := a.leasing
	a.mu.Unlock()
	if !leasing {
		return nil
	}
	// Read in a transaction: the dropping transaction's intent on the
	// key may not be resolved yet, and a transactional read resolves it
	// where a plain one would fail on it.
	var present bool
	if err := a.db.RunTxn(ctx, "drop-check", func(ctx context.Context, txn *kvclient.Txn) error {
		raw, err := txn.Get(ctx, keys.TableDescKey(id))
		present = raw != nil
		return err
	}); err != nil {
		return err
	}
	if present {
		return nil
	}
	a.mu.Lock()
	// The dropping transaction uncached the descriptor under its name;
	// whatever a lease transaction installed since goes too, and any
	// still installing is refused.
	out := a.forgetIDLocked(id)
	a.markDroppedLocked(id, out, "")
	a.mu.Unlock()
	a.ackDropped(ctx, id, 0, out)
	if err := a.waitForAdoption(ctx, id, droppedLeaseVersion); err != nil {
		return err
	}
	a.sweepExpiredLeases(ctx, id)
	return nil
}

// sweepExpiredLeases deletes the lapsed lease records on a dropped
// descriptor; acknowledgements still live are left to lapse, and the
// dropped-table reclaim takes what is left.
func (a *Accessor) sweepExpiredLeases(ctx context.Context, id uint64) {
	err := a.db.RunTxn(ctx, "lease-sweep-drop", func(ctx context.Context, txn *kvclient.Txn) error {
		return DeleteExpiredLeases(ctx, txn, id, a.clock.Now().WallTime)
	})
	if err != nil {
		log.Debugf("sweeping the leases of dropped descriptor %d: %v", id, err)
	}
}

// DeleteExpiredLeases deletes, in txn, every lease record on descriptor
// id that expired by nowWall: after a drop's drain, and for the
// dropped-table reclaim once the table is gone for good.
func DeleteExpiredLeases(ctx context.Context, txn *kvclient.Txn, id uint64, nowWall int64) error {
	lo, hi := keys.DescLeaseSpan(id)
	rows, err := txn.Scan(ctx, lo, hi, 0)
	if err != nil {
		return err
	}
	for _, kv := range rows {
		var l descLease
		if json.Unmarshal(kv.Value, &l) == nil && l.Expiration > nowWall {
			continue
		}
		if err := txn.Delete(ctx, kv.Key); err != nil {
			return err
		}
	}
	return nil
}

// DescriptorLeased reports whether any gateway holds a live lease on
// descriptor id at nowWall (HLC wall nanos). A statement planned under a
// cached descriptor commits before its lease's expiration, and a
// gateway's acknowledgement of a drop keeps that expiration, so once
// this is false no statement can still write under the descriptor: the
// dropped-table reclaim waits for it before clearing the span.
func DescriptorLeased(ctx context.Context, txn *kvclient.Txn, id uint64, nowWall int64) (bool, error) {
	lo, hi := keys.DescLeaseSpan(id)
	rows, err := txn.Scan(ctx, lo, hi, 0)
	if err != nil {
		return false, err
	}
	for _, kv := range rows {
		var l descLease
		if json.Unmarshal(kv.Value, &l) == nil && l.Expiration > nowWall {
			return true, nil
		}
	}
	return false, nil
}
