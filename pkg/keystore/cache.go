package keystore

import (
	"container/list"
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/hmac"
	"crypto/sha256"
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
//
// A cache made with MaxKeys holds at most that many keys: the key used
// least recently is dropped to make room for another (a gateway serving
// many tenants of a per-tenant sealed table, issue #310 step 4). A
// dropped key is read again on its next use, as after its lease.
type Cache struct {
	src     Source
	lease   time.Duration
	now     func() time.Time
	maxKeys int

	mu   sync.Mutex
	keys map[int64]*cacheEntry
	// lru orders the entries by last use, most recent at the front; its
	// elements' values are the key ids. Kept only with maxKeys.
	lru *list.List
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
	// MaxKeys bounds how many keys the cache holds at once, dropping the
	// least recently used beyond it (0: no bound).
	MaxKeys int
}

// NewCache returns a cache over src.
func NewCache(src Source, cfg CacheConfig) *Cache {
	if cfg.Lease <= 0 {
		cfg.Lease = DefaultCacheLease
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	c := &Cache{src: src, lease: cfg.Lease, now: cfg.Now, maxKeys: cfg.MaxKeys, keys: map[int64]*cacheEntry{}}
	if c.maxKeys > 0 {
		c.lru = list.New()
	}
	return c
}

// Len is how many keys the cache holds.
func (c *Cache) Len() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.keys)
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
	// elem is the entry's place in the cache's lru (nil without a bound);
	// guarded by the cache's mu.
	elem *list.Element
}

// heldMaterial is one version's material and how many callers are using
// it; dropped material is released when the last of them is done.
type heldMaterial struct {
	m       *Material
	refs    int
	dropped bool
	// aead is AES-256-GCM under m.Key, made on first use (AEAD).
	aeadOnce sync.Once
	aead     cipher.AEAD
	aeadErr  error
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
	e := c.keys[id]
	if e != nil {
		if e.elem != nil {
			c.lru.MoveToFront(e.elem)
		}
		c.mu.Unlock()
		return e
	}
	e = &cacheEntry{held: map[int]*heldMaterial{}}
	c.keys[id] = e
	var evict []*cacheEntry
	if c.lru != nil {
		e.elem = c.lru.PushFront(id)
		for len(c.keys) > c.maxKeys {
			last := c.lru.Back()
			if last == nil || last == e.elem {
				break
			}
			old := last.Value.(int64)
			evict = append(evict, c.removeLocked(old))
		}
	}
	c.mu.Unlock()
	// Material still in use by a caller is released when that caller is
	// done (heldMaterial.dropped), as after Forget.
	for _, old := range evict {
		old.drop()
	}
	return e
}

// removeLocked takes key id's entry out of the map and the lru; the
// caller drops it (drop) once c.mu is released.
func (c *Cache) removeLocked(id int64) *cacheEntry {
	e := c.keys[id]
	if e == nil {
		return nil
	}
	delete(c.keys, id)
	if e.elem != nil {
		c.lru.Remove(e.elem)
		e.elem = nil
	}
	return e
}

// drop marks an entry removed and releases its unused material.
func (e *cacheEntry) drop() {
	if e == nil {
		return
	}
	e.mu.Lock()
	e.gone = true
	e.dropLocked()
	e.mu.Unlock()
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
	return c.withHeld(ctx, id, version, func(hm *heldMaterial) error { return fn(hm.m) })
}

// AEAD returns AES-256-GCM under version (0: the current one) of key id,
// and the version it is: for a caller that encrypts with the key itself
// rather than handling its material (the sealing of tables). The AEAD is
// made once per cached version and holds its own key schedule, so it
// stays usable for the operation in flight after the material is
// released; a caller must not keep it beyond that operation, and asks
// again for the next one, so that a disabled or destroyed key stops
// being used within the lease. Refusals are With's.
func (c *Cache) AEAD(ctx context.Context, id int64, version int) (cipher.AEAD, int, error) {
	var out cipher.AEAD
	var ver int
	err := c.withHeld(ctx, id, version, func(hm *heldMaterial) error {
		hm.aeadOnce.Do(func() {
			block, err := aes.NewCipher(hm.m.Key)
			if err != nil {
				hm.aeadErr = err
				return
			}
			hm.aead, hm.aeadErr = cipher.NewGCM(block)
		})
		out, ver = hm.aead, hm.m.Version
		return hm.aeadErr
	})
	return out, ver, err
}

// MAC returns HMAC-SHA256 under version (0: the current one) of key id
// of label, a zero byte, and data: a keyed, deterministic digest for a
// caller that names things by a key without handling its material (the
// names of a per-tenant table's tenant keys, which its table key keys).
// The label separates uses of one key. Refusals are With's.
func (c *Cache) MAC(ctx context.Context, id int64, version int, label string, data []byte) ([]byte, error) {
	var out []byte
	err := c.With(ctx, id, version, func(m *Material) error {
		h := hmac.New(sha256.New, m.Key)
		h.Write([]byte(label))
		h.Write([]byte{0})
		h.Write(data)
		out = h.Sum(nil)
		return nil
	})
	return out, err
}

func (c *Cache) withHeld(ctx context.Context, id int64, version int, fn func(hm *heldMaterial) error) error {
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
		return fn(hm)
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
	e := c.removeLocked(id)
	c.mu.Unlock()
	e.drop()
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
