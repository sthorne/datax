package catalog

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
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
type leaseInfo struct{ expiration, priorExpiration, handedOutExpiration int64 }

// TestingPauseRenewal stops (or resumes) the background lease renewal:
// tests use it to let this gateway's leases expire while its sessions
// keep running, the shape of a stalled gateway.
func (a *Accessor) TestingPauseRenewal(paused bool) { a.renewalPaused.Store(paused) }

// TestingRenewNow runs one renewal pass — what the renewal loop does on
// a tick — so a test can put a renewal exactly where it wants one
// (issue #234). Errors are returned rather than logged.
func (a *Accessor) TestingRenewNow(ctx context.Context) error { return a.renewAll(ctx) }

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
		}
	}
}

// renewAll renews this gateway's lease on every cached table, adopting
// any new version; the first error is returned after every table has
// been tried.
func (a *Accessor) renewAll(ctx context.Context) error {
	a.mu.Lock()
	names := make([]string, 0, len(a.cache))
	for name := range a.cache {
		names = append(names, name)
	}
	a.mu.Unlock()
	var first error
	for _, name := range names {
		if _, err := a.refreshOne(ctx, name); err != nil && first == nil {
			first = fmt.Errorf("lease renewal for %q: %w", name, err)
		}
	}
	return first
}

// refreshOne re-reads a table's descriptor (adopting any new version),
// renews this gateway's lease on it, and updates the cache. A dropped
// table is uncached and its lease released. Returns the fresh descriptor
// (nil when dropped).
func (a *Accessor) refreshOne(ctx context.Context, name string) (*TableDescriptor, error) {
	dbID, bare := splitCacheKey(name)
	desc, info, err := a.acquireLease(ctx, dbID, bare, name)
	if err != nil {
		return nil, err // keep the old (still-live) cache entry
	}
	if desc == nil {
		a.mu.Lock()
		old, had := a.cache[name]
		delete(a.cache, name)
		a.mu.Unlock()
		if had {
			_ = a.db.Delete(ctx, keys.DescLeaseKey(old.desc.ID, a.gateway))
		}
		return nil, nil
	}
	a.mu.Lock()
	a.cache[name] = &cachedDesc{desc: desc, expiration: info.expiration, priorExpiration: info.priorExpiration, handedOutExpiration: info.handedOutExpiration}
	a.mu.Unlock()
	return desc, nil
}

// testingBeforeLeaseWrite, when set, runs inside a lease transaction
// between the descriptor read and the lease write — a test's way to hold
// a gateway there while a schema change commits and drains. Atomic: the
// renewal loops of a running cluster read it while a test sets it.
var testingBeforeLeaseWrite atomic.Pointer[func(a *Accessor, name string)]

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
func (a *Accessor) acquireLease(ctx context.Context, dbID uint64, bare, name string) (desc *TableDescriptor, info leaseInfo, err error) {
	err = a.db.RunTxn(ctx, "lease-acquire", func(ctx context.Context, txn *kvclient.Txn) error {
		desc, info = nil, leaseInfo{}
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
		prior, handedOut := a.carryOver(name, d.Version, now)
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
		desc, info = d, leaseInfo{expiration: e, priorExpiration: prior, handedOutExpiration: handedOut}
		return nil
	})
	return desc, info, err
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
func (a *Accessor) carryOver(name string, version uint64, nowWall int64) (prior, handedOut int64) {
	a.mu.Lock()
	defer a.mu.Unlock()
	old, ok := a.cache[name]
	if !ok || old.desc == nil {
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
// unleased accessors and for dropped tables.
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
	if q, bare := SplitTableName(name); q != "" {
		db, name = q, bare
	}
	var dbID uint64
	if err := a.db.RunTxn(ctx, "ddl-database", func(ctx context.Context, txn *kvclient.Txn) error {
		id, err := a.databaseID(ctx, txn, db)
		dbID = id
		return err
	}); err != nil {
		return err
	}
	desc, err := a.refreshOne(ctx, cacheKey(dbID, name))
	if err != nil || desc == nil {
		return err
	}
	return a.waitForAdoption(ctx, desc.ID, desc.Version)
}

// waitForAdoption polls the descriptor's leases until every live one is at
// version or later. A live lease stuck below the version past 2×TTL is
// anomalous (renewal adopts within TTL/3); it is logged and no longer
// waited for — by then it has expired or its holder cannot renew.
func (a *Accessor) waitForAdoption(ctx context.Context, descID, version uint64) error {
	deadline := time.Now().Add(2 * a.ttl)
	poll := a.ttl / 20
	if poll < 10*time.Millisecond {
		poll = 10 * time.Millisecond
	}
	lo, hi := keys.DescLeaseSpan(descID)
	for {
		rows, err := a.db.Scan(ctx, lo, hi, 0)
		adopted := err == nil
		if adopted {
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
					adopted = false
					break
				}
				// The gateway is on the new version, but it handed the
				// previous one to a statement that can still commit, up
				// to the expiration that entry carried. Until then this
				// gateway is not done with the old schema, whatever its
				// published version says (issue #185).
				if l.PriorExpiration > nowWall {
					adopted = false
					break
				}
			}
		}
		if adopted {
			return nil
		}
		if time.Now().After(deadline) {
			log.Warnf("descriptor %d: a live lease never adopted version %d; proceeding after drain timeout", descID, version)
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(poll):
		}
	}
}
