package keystore

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/hmac"
	"crypto/sha256"
	"errors"
	"sort"
	"sync"
	"sync/atomic"
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
// A cache made with MaxKeys holds at most that many keys: the keys used
// least recently are dropped to make room for others (a gateway serving
// many tenants of a per-tenant sealed table, issue #310 step 4). A
// dropped key is read again on its next use, as after its lease.
//
// The hit — a key checked within the lease whose version is held, which
// is every row a busy gateway seals or opens — takes no mutex: the map of
// entries is a sync.Map, each entry publishes what a hit needs as an
// immutable snapshot behind an atomic pointer (the key's state, version
// and generation, when its check began, the versions held), a version's
// material is reference-counted atomically, and recency is an atomic
// timestamp. Everything else — a check, an unwrap, a drop, an eviction —
// runs under the entry's or the cache's mutex, as before, and publishes
// a new snapshot.
type Cache struct {
	src     Source
	lease   time.Duration
	now     func() time.Time
	maxKeys int

	// keys maps a key id to its *cacheEntry; n counts them. Inserting
	// and evicting hold mu, so the count and the eviction stay exact; a
	// lookup holds nothing.
	keys sync.Map
	n    atomic.Int64
	mu   sync.Mutex
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
	return &Cache{src: src, lease: cfg.Lease, now: cfg.Now, maxKeys: cfg.MaxKeys}
}

// Len is how many keys the cache holds.
func (c *Cache) Len() int { return int(c.n.Load()) }

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
	// gone marks an entry removed from the map (Forget, an eviction): a
	// caller that still has it starts over rather than put material where
	// no sweep will find it.
	gone bool

	// snap is what a hit reads without a lock: published under mu by
	// every change of the fields above, nil once the entry is gone.
	snap atomic.Pointer[entrySnap]
	// used is when the entry was last used (UnixNano), for eviction.
	used atomic.Int64
}

// entrySnap is an immutable view of an entry for the hit path.
type entrySnap struct {
	state      State
	version    int
	generation int64
	checked    int64 // UnixNano; 0: never
	held       map[int]*heldMaterial
}

// publishLocked publishes the entry's current state for hits.
func (e *cacheEntry) publishLocked() {
	if e.gone {
		e.snap.Store(nil)
		return
	}
	s := &entrySnap{state: e.key.State, version: e.key.Version, generation: e.key.Generation, held: make(map[int]*heldMaterial, len(e.held))}
	if !e.checked.IsZero() {
		s.checked = e.checked.UnixNano()
	}
	for v, hm := range e.held {
		s.held[v] = hm
	}
	e.snap.Store(s)
}

// heldMaterial is one version's material. refs counts the cache's own
// hold (1, until the version is dropped) and each caller using it; the
// material is released when it reaches zero, so a version dropped while
// in use is released when its last caller is done.
type heldMaterial struct {
	m    *Material
	refs atomic.Int64
	// aead is AES-256-GCM under m.Key, made on first use (AEAD); ready is
	// set once it is, after which a hit hands it out without a reference.
	aeadOnce sync.Once
	aead     cipher.AEAD
	aeadErr  error
	ready    atomic.Bool
}

func newHeld(m *Material) *heldMaterial {
	hm := &heldMaterial{m: m}
	hm.refs.Store(2) // the cache's hold and the caller's
	return hm
}

// tryAcquire takes a reference unless the material is already released.
func (hm *heldMaterial) tryAcquire() bool {
	for {
		r := hm.refs.Load()
		if r <= 0 {
			return false
		}
		if hm.refs.CompareAndSwap(r, r+1) {
			return true
		}
	}
}

// unref gives back a reference, releasing the material with the last.
func (hm *heldMaterial) unref() {
	if hm.refs.Add(-1) == 0 {
		hm.m.Release()
	}
}

// makeAEAD makes the version's AEAD once.
func (hm *heldMaterial) makeAEAD() {
	hm.aeadOnce.Do(func() {
		block, err := aes.NewCipher(hm.m.Key)
		if err != nil {
			hm.aeadErr = err
			return
		}
		if hm.aead, hm.aeadErr = cipher.NewGCM(block); hm.aeadErr == nil {
			hm.ready.Store(true)
		}
	})
}

func (e *cacheEntry) dropLocked() {
	for v, hm := range e.held {
		delete(e.held, v)
		hm.unref() // the cache's hold
	}
	e.publishLocked()
}

// touch records a use of the entry, for eviction, the sweep and the
// background refresh; a timestamp already this recent is not written
// again.
func (c *Cache) touch(e *cacheEntry, now int64) {
	if now-e.used.Load() > int64(time.Millisecond) {
		e.used.Store(now)
	}
}

func (c *Cache) lookup(id int64) *cacheEntry {
	if v, ok := c.keys.Load(id); ok {
		return v.(*cacheEntry)
	}
	return nil
}

func (c *Cache) entry(id int64) *cacheEntry {
	if e := c.lookup(id); e != nil {
		c.touch(e, c.now().UnixNano())
		return e
	}
	c.mu.Lock()
	if v, ok := c.keys.Load(id); ok {
		c.mu.Unlock()
		return v.(*cacheEntry)
	}
	e := &cacheEntry{held: map[int]*heldMaterial{}}
	e.used.Store(c.now().UnixNano())
	c.keys.Store(id, e)
	var evict []*cacheEntry
	if c.n.Add(1) > int64(c.maxKeys) && c.maxKeys > 0 {
		evict = c.evictLocked(id)
	}
	c.mu.Unlock()
	// Material still in use by a caller is released when that caller is
	// done, as after Forget.
	for _, old := range evict {
		old.drop()
	}
	return e
}

// evictLocked takes the least recently used entries out of the map, down
// to nine tenths of the bound (so the scan it costs is paid once per
// tenth of the bound's insertions), sparing keep; the caller drops them.
func (c *Cache) evictLocked(keep int64) []*cacheEntry {
	type aged struct {
		id   int64
		used int64
	}
	var all []aged
	c.keys.Range(func(k, v any) bool {
		if id := k.(int64); id != keep {
			all = append(all, aged{id, v.(*cacheEntry).used.Load()})
		}
		return true
	})
	target := c.maxKeys - c.maxKeys/10
	if target < 1 {
		target = 1
	}
	over := int(c.n.Load()) - target
	if over <= 0 {
		return nil
	}
	sort.Slice(all, func(i, j int) bool { return all[i].used < all[j].used })
	var out []*cacheEntry
	for _, a := range all[:min(over, len(all))] {
		if e := c.removeLocked(a.id); e != nil {
			out = append(out, e)
		}
	}
	return out
}

// removeLocked takes key id's entry out of the map; the caller drops it
// (drop) once c.mu is released.
func (c *Cache) removeLocked(id int64) *cacheEntry {
	v, ok := c.keys.LoadAndDelete(id)
	if !ok {
		return nil
	}
	c.n.Add(-1)
	return v.(*cacheEntry)
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
		for v, hm := range e.held {
			delete(e.held, v)
			hm.unref()
		}
	}
	e.key, e.checked = k, start
	e.publishLocked()
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

// hit is the lock-free path: version (0: the current one) of key id, if
// the entry's last check is within the lease, the key is enabled and the
// version is held. With ref, it takes a reference on the material, which
// the caller gives back (unref); without, the version's AEAD must be
// made already, and nothing is held.
func (c *Cache) hit(id int64, version int, ref bool) (*heldMaterial, bool) {
	e := c.lookup(id)
	if e == nil {
		return nil, false
	}
	s := e.snap.Load()
	if s == nil || s.checked == 0 || s.state != StateEnabled {
		return nil, false
	}
	now := c.now().UnixNano()
	if now-s.checked >= int64(c.lease) {
		return nil, false
	}
	if version == 0 {
		version = s.version
	}
	hm := s.held[version]
	if hm == nil {
		return nil, false
	}
	if ref {
		if !hm.tryAcquire() {
			return nil, false
		}
	} else if !hm.ready.Load() {
		return nil, false
	}
	c.touch(e, now)
	return hm, true
}

// With runs fn with version (0: the current one) of key id's material,
// held for the duration of the call; fn must not keep m.Key past it. A
// disabled or destroyed key is refused (ErrDisabled, ErrDestroyed) as of
// the last check, which is within the lease.
func (c *Cache) With(ctx context.Context, id int64, version int, fn func(m *Material) error) error {
	if hm, ok := c.hit(id, version, true); ok {
		defer hm.unref()
		return fn(hm.m)
	}
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
	if hm, ok := c.hit(id, version, false); ok {
		return hm.aead, hm.m.Version, nil
	}
	var out cipher.AEAD
	var ver int
	err := c.withHeld(ctx, id, version, func(hm *heldMaterial) error {
		hm.makeAEAD()
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
	// changed counts retries because the key moved on; an entry dropped
	// from the map under the caller (an eviction, a sweep, Forget) is
	// retried as well, more times, without reading the key again by
	// force, since the key itself may not have changed at all.
	changed, dropped := 0, 0
	for {
		e, k, err := c.check(ctx, id, changed > 0)
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
			e.mu.Lock()
			gone := e.gone
			e.mu.Unlock()
			if gone && dropped < 16 {
				dropped++
				continue
			}
			if changed++; changed > 2 {
				return errors.New("the key kept changing while it was being read; retry")
			}
			continue
		}
		defer hm.unref()
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
	if hm := e.held[v]; hm != nil && hm.tryAcquire() {
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
	if hm := e.held[v]; hm != nil && hm.tryAcquire() { // another caller unwrapped it first
		m.Release()
		return hm, false, nil
	}
	hm := newHeld(m)
	e.held[v] = hm
	e.publishLocked()
	return hm, false, nil
}

// Forget drops what the cache holds for key id: after this gateway
// changed the key itself, so its next use reads the row.
func (c *Cache) Forget(id int64) {
	c.mu.Lock()
	e := c.removeLocked(id)
	c.mu.Unlock()
	e.drop()
}

// Sweep drops every key nobody has used for a lease and whose last check
// is older than the lease: their material is released rather than kept
// until something asks again.
func (c *Cache) Sweep() {
	var stale []int64
	now := c.now()
	c.keys.Range(func(k, v any) bool {
		e := v.(*cacheEntry)
		if now.UnixNano()-e.used.Load() < int64(c.lease) {
			return true
		}
		e.mu.Lock()
		if e.checked.IsZero() || now.Sub(e.checked) >= c.lease {
			stale = append(stale, k.(int64))
		}
		e.mu.Unlock()
		return true
	})
	for _, id := range stale {
		c.Forget(id)
	}
}

// BatchSource is a Source that also reads many keys' metadata in one go
// (*Store is one): with it, the cache's background refresh checks the
// keys in use in batches, so that a gateway using many keys — a
// per-tenant table's — does not check each one synchronously, on a
// statement's path, every lease.
type BatchSource interface {
	// GetByIDs returns the metadata of the keys of ids that exist.
	GetByIDs(ctx context.Context, ids []int64) (map[int64]Key, error)
}

// refreshBatch is how many keys one read of the background refresh
// checks.
const refreshBatch = 500

// RefreshInUse checks, in batches, every key used within the last lease
// whose last check is older than half the lease, as a statement's check
// would: a key whose generation moved is dropped, a key that is gone is
// forgotten, and the rest are confirmed from the moment the batch's read
// began. Keys in use then stay on the hit path. It returns how many keys
// it checked; it needs a BatchSource (0 otherwise).
func (c *Cache) RefreshInUse(ctx context.Context) int {
	bs, ok := c.src.(BatchSource)
	if !ok {
		return 0
	}
	now := c.now().UnixNano()
	var ids []int64
	c.keys.Range(func(k, v any) bool {
		e := v.(*cacheEntry)
		s := e.snap.Load()
		if s == nil || s.checked == 0 || now-s.checked < int64(c.lease)/2 || now-e.used.Load() >= int64(c.lease) {
			return true
		}
		ids = append(ids, k.(int64))
		return true
	})
	checked := 0
	for at := 0; at < len(ids); at += refreshBatch {
		chunk := ids[at:min(at+refreshBatch, len(ids))]
		start := c.now()
		got, err := bs.GetByIDs(ctx, chunk)
		if err != nil {
			return checked // the statements' own checks carry on
		}
		for _, id := range chunk {
			k, ok := got[id]
			if !ok {
				c.Forget(id)
				continue
			}
			e := c.lookup(id)
			if e == nil {
				continue
			}
			e.mu.Lock()
			if !e.gone && start.After(e.checked) {
				if e.key.Generation != k.Generation || e.key.ID != k.ID {
					for v, hm := range e.held {
						delete(e.held, v)
						hm.unref()
					}
				}
				e.key, e.checked = k, start
				e.publishLocked()
			}
			e.mu.Unlock()
			checked++
		}
	}
	return checked
}

// Run refreshes the keys in use every half lease (RefreshInUse) and
// sweeps the cache every lease, until ctx is done; then it releases
// everything the cache holds.
func (c *Cache) Run(ctx context.Context) {
	t := time.NewTicker(c.lease / 2)
	defer t.Stop()
	for tick := 0; ; tick++ {
		select {
		case <-ctx.Done():
			c.Close()
			return
		case <-t.C:
			c.RefreshInUse(ctx)
			if tick%2 == 1 {
				c.Sweep()
			}
		}
	}
}

// Close releases everything the cache holds.
func (c *Cache) Close() {
	var ids []int64
	c.keys.Range(func(k, _ any) bool {
		ids = append(ids, k.(int64))
		return true
	})
	for _, id := range ids {
		c.Forget(id)
	}
}
