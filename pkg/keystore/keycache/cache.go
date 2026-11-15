// Package keycache is a gateway's cache of unwrapped named keys (issue
// #310, docs/design-key-service.md, "Key cache and revocation"): the
// material of the keys a gateway seals rows under (sealed tables) or
// wraps an application's data keys under (the key service), held in
// memory beside the descriptor cache so that the hot path does not read
// datax_keys.
//
// It is a package of its own, beneath pkg/keystore, because the SQL layer
// uses it and pkg/keystore imports the SQL layer; pkg/keystore gives a
// Store's Source (Store.CacheSource).
//
// The rules:
//
//   - An entry is one version of one key: its material in held memory
//     (enc.HoldKey) and an AES-256-GCM AEAD made from it.
//   - Each key is under a lease of at most MaxLease (10 seconds, the
//     descriptor lease). Past the lease, the next use reads the key's
//     generation from the Source; when it has moved — the key was
//     disabled, destroyed or rotated — every cached version of the key
//     is dropped and released, and the use unwraps afresh, which a
//     disabled or destroyed key refuses. So a change to a key reaches
//     every gateway within one lease of the change.
//   - Current is the key's newest version as of the lease; a rotation is
//     therefore picked up for new writes within one lease too, while
//     Version still opens every version the key has.
//   - Released material is zeroed. An AEAD already handed out keeps
//     working for the use in flight (its key schedule is its own); a
//     caller must not keep one beyond the operation it was fetched for.
package keycache

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/sthorne/datax/pkg/storage/enc"
)

// MaxLease is the longest lease a key is cached under before its
// generation is checked again: the descriptor lease.
const MaxLease = 10 * time.Second

// Errors a Source returns, which pkg/keystore's errors are: they say why
// a key gives no material, and the callers tell a destroyed key from an
// unreachable one by them.
var (
	ErrNotFound  = errors.New("no such key")
	ErrDisabled  = errors.New("the key is disabled")
	ErrDestroyed = errors.New("the key has been destroyed")
	ErrNoVersion = errors.New("the key has no such version")
)

// Material is one version of a key's material, as a Source returns it.
// Key is in held memory, and the cache owns it from then on.
type Material struct {
	ID         int64
	Version    int
	Generation int64
	Key        []byte
}

// Source is where the cache gets material and generations from:
// pkg/keystore's Store, through Store.CacheSource.
type Source interface {
	// Unwrap returns version (0: the current one) of key id. A disabled
	// or destroyed key is refused with ErrDisabled or ErrDestroyed.
	Unwrap(ctx context.Context, id int64, version int) (*Material, error)
	// Generation returns the key's generation, which every change of its
	// state moves.
	Generation(ctx context.Context, id int64) (int64, error)
}

// Key is one version of a key, ready to use.
type Key struct {
	ID      int64
	Version int
	// AEAD is AES-256-GCM under the version's material.
	AEAD cipher.AEAD
}

// Options tune a cache.
type Options struct {
	// Lease is how long a key is used before its generation is checked
	// again: at most MaxLease, which is also the default.
	Lease time.Duration
	// Now is the clock; nil is time.Now.
	Now func() time.Time
}

// Cache is a gateway's cache of unwrapped keys. It is safe for
// concurrent use.
type Cache struct {
	src   Source
	lease time.Duration
	now   func() time.Time

	mu   sync.RWMutex
	keys map[int64]*keyState
}

// keyState is what the cache holds of one key. A use within the lease
// takes its read lock only; a refresh takes the write lock, so a lease
// that runs out under load costs one read of the key.
type keyState struct {
	mu       sync.RWMutex
	gen      int64
	checked  time.Time
	current  int // newest version as of the lease; 0 until known
	versions map[int]*entry
}

type entry struct {
	material []byte
	aead     cipher.AEAD
}

// New returns a cache over src.
func New(src Source, opts Options) *Cache {
	if opts.Lease <= 0 || opts.Lease > MaxLease {
		opts.Lease = MaxLease
	}
	if opts.Now == nil {
		opts.Now = time.Now
	}
	return &Cache{src: src, lease: opts.Lease, now: opts.Now, keys: map[int64]*keyState{}}
}

func (c *Cache) state(id int64) *keyState {
	c.mu.RLock()
	ks := c.keys[id]
	c.mu.RUnlock()
	if ks != nil {
		return ks
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	ks = c.keys[id]
	if ks == nil {
		ks = &keyState{versions: map[int]*entry{}}
		c.keys[id] = ks
	}
	return ks
}

// Current returns key id's current version.
func (c *Cache) Current(ctx context.Context, id int64) (Key, error) {
	return c.get(ctx, id, 0)
}

// Version returns version v of key id.
func (c *Cache) Version(ctx context.Context, id int64, v int) (Key, error) {
	if v <= 0 {
		return Key{}, fmt.Errorf("key %d: version %d: %w", id, v, ErrNoVersion)
	}
	return c.get(ctx, id, v)
}

// WithMaterial runs fn with version v (0: current) of key id's raw
// material, for a caller that needs the key itself rather than an AEAD
// over it. fn must not keep the slice: it is zeroed when the entry goes.
func (c *Cache) WithMaterial(ctx context.Context, id int64, v int, fn func(version int, material []byte) error) error {
	ks := c.state(id)
	for {
		ks.mu.RLock()
		if e, ver, ok := c.fresh(ks, v); ok {
			defer ks.mu.RUnlock()
			return fn(ver, e.material)
		}
		ks.mu.RUnlock()
		ks.mu.Lock()
		_, _, err := c.locked(ctx, id, ks, v)
		ks.mu.Unlock()
		if err != nil {
			return err
		}
		// Loop to use the entry under the read lock: a refresh by another
		// caller may have replaced it in between, which only means one
		// more turn.
	}
}

func (c *Cache) get(ctx context.Context, id int64, v int) (Key, error) {
	ks := c.state(id)
	ks.mu.RLock()
	e, ver, ok := c.fresh(ks, v)
	ks.mu.RUnlock()
	if ok {
		return Key{ID: id, Version: ver, AEAD: e.aead}, nil
	}
	ks.mu.Lock()
	defer ks.mu.Unlock()
	e, ver, err := c.locked(ctx, id, ks, v)
	if err != nil {
		return Key{}, err
	}
	return Key{ID: id, Version: ver, AEAD: e.aead}, nil
}

// fresh returns the entry for version v (0: current) when it is cached
// and the key's lease has not run out; ks.mu is held, read or write.
func (c *Cache) fresh(ks *keyState, v int) (*entry, int, bool) {
	if ks.checked.IsZero() || c.now().Sub(ks.checked) >= c.lease {
		return nil, 0, false
	}
	if v == 0 {
		v = ks.current
	}
	e := ks.versions[v]
	return e, v, v != 0 && e != nil
}

// locked returns the entry for version v (0: current) of key id, with
// ks.mu held: the lease is checked first, then a missing version is
// unwrapped.
func (c *Cache) locked(ctx context.Context, id int64, ks *keyState, v int) (*entry, int, error) {
	now := c.now()
	if !ks.checked.IsZero() && now.Sub(ks.checked) >= c.lease {
		gen, err := c.src.Generation(ctx, id)
		if err != nil {
			// The key cannot be checked, so it cannot be used: a key
			// whose row is unreadable is not known to be enabled.
			ks.drop()
			return nil, 0, err
		}
		if gen != ks.gen {
			ks.drop()
		} else {
			ks.checked = now
		}
	}
	if v == 0 {
		v = ks.current
	}
	if e := ks.versions[v]; v != 0 && e != nil {
		return e, v, nil
	}
	m, err := c.src.Unwrap(ctx, id, v)
	if err != nil {
		return nil, 0, err
	}
	e, err := newEntry(m.Key)
	if err != nil {
		enc.ReleaseKey(m.Key)
		return nil, 0, fmt.Errorf("key %d version %d: %w", id, m.Version, err)
	}
	if len(ks.versions) == 0 || m.Generation != ks.gen {
		// The first version cached under this generation starts the
		// lease; one from another generation replaces what was cached.
		ks.drop()
		ks.gen, ks.checked = m.Generation, now
	}
	if v == 0 {
		ks.current = m.Version
	}
	ks.versions[m.Version] = e
	return e, m.Version, nil
}

func newEntry(material []byte) (*entry, error) {
	if len(material) != enc.KeyLen {
		return nil, fmt.Errorf("key material is %d bytes, not %d", len(material), enc.KeyLen)
	}
	block, err := aes.NewCipher(material)
	if err != nil {
		return nil, err
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	return &entry{material: material, aead: aead}, nil
}

// drop releases every cached version of the key; ks.mu is held.
func (ks *keyState) drop() {
	for v, e := range ks.versions {
		enc.ReleaseKey(e.material)
		delete(ks.versions, v)
	}
	ks.current, ks.checked, ks.gen = 0, time.Time{}, 0
}

// Invalidate drops key id at once, on this gateway: a statement that
// changed the key (destroyed it, say) need not wait out its own lease.
func (c *Cache) Invalidate(id int64) {
	c.mu.Lock()
	ks := c.keys[id]
	delete(c.keys, id)
	c.mu.Unlock()
	if ks != nil {
		ks.mu.Lock()
		ks.drop()
		ks.mu.Unlock()
	}
}

// Close releases every cached key.
func (c *Cache) Close() {
	c.mu.Lock()
	all := c.keys
	c.keys = map[int64]*keyState{}
	c.mu.Unlock()
	for _, ks := range all {
		ks.mu.Lock()
		ks.drop()
		ks.mu.Unlock()
	}
}

// Len reports how many key versions are cached (for tests and metrics).
func (c *Cache) Len() int {
	c.mu.Lock()
	all := make([]*keyState, 0, len(c.keys))
	for _, ks := range c.keys {
		all = append(all, ks)
	}
	c.mu.Unlock()
	n := 0
	for _, ks := range all {
		ks.mu.Lock()
		n += len(ks.versions)
		ks.mu.Unlock()
	}
	return n
}
