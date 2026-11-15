package keystore

import (
	"context"
	"errors"
	"sync"
	"time"
)

// Cache keeps named keys' material on a gateway, in held memory, under a
// lease (design, "Key cache and revocation"). Every state change of a key
// bumps its generation (Store); the cache checks the generation it holds
// against the row at most a lease after its last check, and drops what it
// holds for the key when it has moved. So a key disabled, destroyed,
// rotated or re-granted is seen by every gateway within one lease, and a
// statement that must not return before that — DROP and DISABLE — waits
// out Lease after its change commits.
//
// The lease runs from the moment a check starts, not when it returns: a
// check that began before a change committed confirms the old state for
// at most a lease after the change, which is the bound waited out.
//
// It is meant to be shared by everything on a gateway that uses named
// keys: the key service's functions (service.go) and the sealing of
// tables (later steps of issue #310). Use it as
//
//	err := cache.With(ctx, id, version, func(m *keystore.Material) error {
//		... use m.Key; do not keep it past the call ...
//	})
//
// and run Run in the background to drop material nobody has used for a
// lease, so a key's material does not linger in memory after the key is
// destroyed only because nothing asked for it again.
type Cache struct {
	src   Source
	lease time.Duration
	now   func() time.Time

	mu   sync.Mutex
	keys map[int64]*cacheEntry
}

// Source is what a Cache reads keys from; *Store is one.
type Source interface {
	GetByID(ctx context.Context, id int64) (Key, error)
	Unwrap(ctx context.Context, id int64, version int) (*Material, error)
}

// DefaultCacheLease is the lease of a cache made with no lease: the
// descriptor lease (catalog.DefaultDescLeaseTTL), as the design bounds it.
const DefaultCacheLease = 10 * time.Second

// CacheConfig configures a Cache.
type CacheConfig struct {
	// Lease bounds how long the cache trusts a check of a key's
	// generation (0: DefaultCacheLease).
	Lease time.Duration
	// Now is the clock (nil: time.Now).
	Now func() time.Time
}

// NewCache returns a cache over src.
func NewCache(src Source, cfg CacheConfig) *Cache {
	if cfg.Lease <= 0 {
		cfg.Lease = DefaultCacheLease
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	return &Cache{src: src, lease: cfg.Lease, now: cfg.Now, keys: map[int64]*cacheEntry{}}
}

// Lease is how long a change to a key can go unseen by this cache: what
// a statement that changes a key waits out before it returns.
func (c *Cache) Lease() time.Duration { return c.lease }

type cacheEntry struct {
	// refresh serializes checks of the key, so a burst of callers after
	// the lease ran out makes one read, not one each.
	refresh sync.Mutex

	mu      sync.Mutex
	key     Key
	checked time.Time // when the check that confirmed key began; zero: never
	held    map[int]*heldMaterial
	// gone marks an entry Forget removed from the map: a caller that
	// still has it starts over rather than put material where no sweep
	// will find it.
	gone bool
}

// heldMaterial is one version's material and how many callers are using
// it; dropped material is released when the last of them is done.
type heldMaterial struct {
	m       *Material
	refs    int
	dropped bool
}

func (e *cacheEntry) dropLocked() {
	for v, hm := range e.held {
		hm.dropped = true
		if hm.refs == 0 {
			hm.m.Release()
		}
		delete(e.held, v)
	}
}

func (c *Cache) entry(id int64) *cacheEntry {
	c.mu.Lock()
	defer c.mu.Unlock()
	e := c.keys[id]
	if e == nil {
		e = &cacheEntry{held: map[int]*heldMaterial{}}
		c.keys[id] = e
	}
	return e
}

// check returns key id's metadata as confirmed within the lease, reading
// the row when the last check is older (or force).
func (c *Cache) check(ctx context.Context, id int64, force bool) (*cacheEntry, Key, error) {
	e := c.entry(id)
	fresh := func() (Key, bool) {
		e.mu.Lock()
		defer e.mu.Unlock()
		if e.checked.IsZero() || c.now().Sub(e.checked) >= c.lease {
			return Key{}, false
		}
		return e.key, true
	}
	if !force {
		if k, ok := fresh(); ok {
			return e, k, nil
		}
	}
	e.refresh.Lock()
	defer e.refresh.Unlock()
	if !force {
		if k, ok := fresh(); ok { // another caller checked while this one waited
			return e, k, nil
		}
	}
	start := c.now()
	k, err := c.src.GetByID(ctx, id)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			c.Forget(id)
		}
		return nil, Key{}, err
	}
	e.mu.Lock()
	if e.key.Generation != k.Generation || e.key.ID != k.ID {
		e.dropLocked()
	}
	e.key, e.checked = k, start
	e.mu.Unlock()
	return e, k, nil
}

// Key returns key id's metadata, as checked against the row within the
// lease. It never carries material.
func (c *Cache) Key(ctx context.Context, id int64) (Key, error) {
	_, k, err := c.check(ctx, id, false)
	return k, err
}

// Refresh checks key id against the row now, whatever the lease says:
// for a caller about to refuse because of what the cache holds (a
// disabled key, a missing grant), so that a change in the caller's favour
// is not held back by the lease.
func (c *Cache) Refresh(ctx context.Context, id int64) (Key, error) {
	_, k, err := c.check(ctx, id, true)
	return k, err
}

// With runs fn with version (0: the current one) of key id's material,
// held for the duration of the call; fn must not keep m.Key past it. A
// disabled or destroyed key is refused (ErrDisabled, ErrDestroyed) as of
// the last check, which is within the lease.
func (c *Cache) With(ctx context.Context, id int64, version int, fn func(m *Material) error) error {
	for attempt := 0; ; attempt++ {
		e, k, err := c.check(ctx, id, attempt > 0)
		if err != nil {
			return err
		}
		switch k.State {
		case StateDestroyed:
			return ErrDestroyed
		case StateDisabled:
			return ErrDisabled
		}
		v := version
		if v == 0 {
			v = k.Version
		}
		hm, retry, err := c.acquire(ctx, e, k, v)
		if err != nil {
			return err
		}
		if retry {
			if attempt >= 2 {
				return errors.New("the key kept changing while it was being read; retry")
			}
			continue
		}
		defer c.release(e, hm)
		return fn(hm.m)
	}
}

// acquire takes version v of the entry's material, unwrapping it when it
// is not held. retry: the key moved on since k was checked.
func (c *Cache) acquire(ctx context.Context, e *cacheEntry, k Key, v int) (*heldMaterial, bool, error) {
	e.mu.Lock()
	if e.gone || e.key.Generation != k.Generation {
		e.mu.Unlock()
		return nil, true, nil
	}
	if hm := e.held[v]; hm != nil {
		hm.refs++
		e.mu.Unlock()
		return hm, false, nil
	}
	e.mu.Unlock()
	// The unwrap reads the row and opens the version under the cluster
	// key: done outside the entry's lock, and its result kept only if the
	// key has not moved on meanwhile.
	m, err := c.src.Unwrap(ctx, k.ID, v)
	if err != nil {
		return nil, false, err
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.gone || m.Generation != k.Generation || e.key.Generation != k.Generation {
		m.Release()
		return nil, true, nil
	}
	if hm := e.held[v]; hm != nil { // another caller unwrapped it first
		m.Release()
		hm.refs++
		return hm, false, nil
	}
	hm := &heldMaterial{m: m, refs: 1}
	e.held[v] = hm
	return hm, false, nil
}

func (c *Cache) release(e *cacheEntry, hm *heldMaterial) {
	e.mu.Lock()
	defer e.mu.Unlock()
	hm.refs--
	if hm.dropped && hm.refs == 0 {
		hm.m.Release()
	}
}

// Forget drops what the cache holds for key id: after this gateway
// changed the key itself, so its next use reads the row.
func (c *Cache) Forget(id int64) {
	c.mu.Lock()
	e := c.keys[id]
	delete(c.keys, id)
	c.mu.Unlock()
	if e != nil {
		e.mu.Lock()
		e.gone = true
		e.dropLocked()
		e.mu.Unlock()
	}
}

// Sweep drops every key whose last check is older than the lease: their
// material is released rather than kept until something asks again.
func (c *Cache) Sweep() {
	c.mu.Lock()
	var stale []int64
	now := c.now()
	for id, e := range c.keys {
		e.mu.Lock()
		if e.checked.IsZero() || now.Sub(e.checked) >= c.lease {
			stale = append(stale, id)
		}
		e.mu.Unlock()
	}
	c.mu.Unlock()
	for _, id := range stale {
		c.Forget(id)
	}
}

// Run sweeps the cache every lease until ctx is done, then releases
// everything it holds.
func (c *Cache) Run(ctx context.Context) {
	t := time.NewTicker(c.lease)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			c.Close()
			return
		case <-t.C:
			c.Sweep()
		}
	}
}

// Close releases everything the cache holds.
func (c *Cache) Close() {
	c.mu.Lock()
	ids := make([]int64, 0, len(c.keys))
	for id := range c.keys {
		ids = append(ids, id)
	}
	c.mu.Unlock()
	for _, id := range ids {
		c.Forget(id)
	}
}
