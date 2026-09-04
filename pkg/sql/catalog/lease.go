package catalog

import (
	"context"
	"encoding/json"
	"errors"
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
// Two rules keep the drain sound against a gateway whose renewals stall
// (issue #110): the cache entry carries the very expiration written into
// the lease record, so the cache never outlives what the drain waits for;
// and a transaction that plans against a leased descriptor takes the
// lease's expiration as its commit deadline (kvclient.Txn.UpdateDeadline),
// so it cannot commit after the drain has written the lease off.
//
// Remaining gap (tracked in issue #22): a transaction that BEGAN before the
// drain, on another gateway, may keep using the descriptor version it
// started with until it commits while its gateway's renewals keep the
// lease live at the NEW version — statements are pinned to the descriptor
// they planned against. The deadline covers the lapsed-lease case only.

// DefaultDescLeaseTTL is how long a gateway's descriptor lease (and cache
// entry) lives without renewal.
const DefaultDescLeaseTTL = 10 * time.Second

// descLease is the stored lease record.
type descLease struct {
	Version    uint64 `json:"version"`
	Expiration int64  `json:"expiration"` // HLC wall nanos
}

// TestingPauseRenewal stops (or resumes) the background lease renewal:
// tests use it to let this gateway's leases expire while its sessions
// keep running, the shape of a stalled gateway.
func (a *Accessor) TestingPauseRenewal(paused bool) { a.renewalPaused.Store(paused) }

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
			a.mu.Lock()
			names := make([]string, 0, len(a.cache))
			for name := range a.cache {
				names = append(names, name)
			}
			a.mu.Unlock()
			for _, name := range names {
				if _, err := a.refreshOne(ctx, name); err != nil {
					log.Debugf("lease renewal for %q: %v", name, err)
				}
			}
		}
	}
}

// refreshOne re-reads a table's descriptor (adopting any new version),
// renews this gateway's lease on it, and updates the cache. A dropped
// table is uncached and its lease released. Returns the fresh descriptor
// (nil when dropped).
func (a *Accessor) refreshOne(ctx context.Context, name string) (*TableDescriptor, error) {
	dbID, bare := splitCacheKey(name)
	var desc *TableDescriptor
	err := a.db.RunTxn(ctx, "lease-refresh", func(ctx context.Context, txn *kvclient.Txn) error {
		d, err := lookupUncached(ctx, txn, dbID, bare, a.isDefaultID(dbID))
		var nf *ErrTableNotFound
		if errors.As(err, &nf) {
			desc = nil
			return nil
		}
		desc = d
		return err
	})
	if err != nil {
		return nil, err
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
	exp, err := a.writeLease(ctx, desc)
	if err != nil {
		return desc, err // keep the old (still-live) cache entry
	}
	a.mu.Lock()
	a.cache[name] = &cachedDesc{desc: desc, expiration: exp}
	a.mu.Unlock()
	return desc, nil
}

// writeLease records that this gateway may use desc at its version until
// the returned expiration (HLC wall nanos) — the one value both the
// lease record and the cache entry carry.
func (a *Accessor) writeLease(ctx context.Context, desc *TableDescriptor) (int64, error) {
	exp := a.clock.Now().WallTime + a.ttl.Nanoseconds()
	raw, err := json.Marshal(descLease{Version: desc.Version, Expiration: exp})
	if err != nil {
		return 0, err
	}
	if err := a.db.Put(ctx, keys.DescLeaseKey(desc.ID, a.gateway), raw); err != nil {
		return 0, err
	}
	return exp, nil
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
