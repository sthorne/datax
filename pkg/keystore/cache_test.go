package keystore

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// fakeSource is a Source over one key held in memory, counting reads.
type fakeSource struct {
	mu       sync.Mutex
	key      Key
	material map[int][]byte
	gets     int
	unwraps  int
	missing  bool
}

func (f *fakeSource) GetByID(_ context.Context, id int64) (Key, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.gets++
	if f.missing || id != f.key.ID {
		return Key{}, ErrNotFound
	}
	return f.key, nil
}

func (f *fakeSource) Unwrap(_ context.Context, id int64, version int) (*Material, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.unwraps++
	switch f.key.State {
	case StateDisabled:
		return nil, ErrDisabled
	case StateDestroyed:
		return nil, ErrDestroyed
	}
	if version == 0 {
		version = f.key.Version
	}
	m, ok := f.material[version]
	if !ok {
		return nil, ErrNoVersion
	}
	return &Material{ID: id, Version: version, Generation: f.key.Generation, Key: append([]byte(nil), m...)}, nil
}

func (f *fakeSource) change(fn func(k *Key)) {
	f.mu.Lock()
	defer f.mu.Unlock()
	fn(&f.key)
	f.key.Generation++
}

type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *fakeClock) Now() time.Time { c.mu.Lock(); defer c.mu.Unlock(); return c.now }
func (c *fakeClock) Add(d time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(d)
	c.mu.Unlock()
}

// TestCacheLease (issue #310): the cache reads a key once per lease, not
// once per use; a change to the key (disable, rotate) is not seen until
// the lease runs out, and is seen at the first use after; Refresh sees it
// at once; a destroyed or vanished key's material is dropped.
func TestCacheLease(t *testing.T) {
	ctx := context.Background()
	src := &fakeSource{key: Key{ID: 9, State: StateEnabled, Version: 1, Generation: 1}, material: map[int][]byte{1: testMaterial(1), 2: testMaterial(2)}}
	clk := &fakeClock{now: time.Unix(1000, 0)}
	c := NewCache(src, CacheConfig{Lease: 10 * time.Second, Now: clk.Now})
	use := func(version int) ([]byte, error) {
		var got []byte
		err := c.With(ctx, 9, version, func(m *Material) error {
			got = append([]byte(nil), m.Key...)
			return nil
		})
		return got, err
	}
	for i := 0; i < 5; i++ {
		got, err := use(0)
		if err != nil || !bytes.Equal(got, testMaterial(1)) {
			t.Fatalf("use %d: %x %v", i, got, err)
		}
	}
	if src.gets != 1 || src.unwraps != 1 {
		t.Fatalf("five uses in a lease made %d reads and %d unwraps, want 1 and 1", src.gets, src.unwraps)
	}

	// Disabled: still served until the lease runs out, refused after.
	src.change(func(k *Key) { k.State = StateDisabled })
	clk.Add(9 * time.Second)
	if _, err := use(0); err != nil {
		t.Fatalf("within the lease after a disable: %v", err)
	}
	clk.Add(time.Second)
	if _, err := use(0); !errors.Is(err, ErrDisabled) {
		t.Fatalf("after the lease: %v, want ErrDisabled", err)
	}
	// Enabled again: Refresh sees it without waiting.
	src.change(func(k *Key) { k.State = StateEnabled })
	if _, err := use(0); !errors.Is(err, ErrDisabled) {
		t.Fatalf("a re-enable seen before the lease ran out: %v", err)
	}
	if k, err := c.Refresh(ctx, 9); err != nil || k.State != StateEnabled {
		t.Fatalf("refresh: %+v %v", k, err)
	}
	if _, err := use(0); err != nil {
		t.Fatalf("after refresh: %v", err)
	}

	// Rotated: the current version moves at the next check; the old one
	// is still served on request.
	src.change(func(k *Key) { k.Version = 2 })
	clk.Add(10 * time.Second)
	if got, err := use(0); err != nil || !bytes.Equal(got, testMaterial(2)) {
		t.Fatalf("current after rotation: %x %v", got, err)
	}
	if got, err := use(1); err != nil || !bytes.Equal(got, testMaterial(1)) {
		t.Fatalf("version 1 after rotation: %x %v", got, err)
	}

	// Destroyed: refused, and the material the cache held is released.
	var held *Material
	_ = c.With(ctx, 9, 2, func(m *Material) error { held = m; return nil })
	src.change(func(k *Key) { k.State = StateDestroyed })
	clk.Add(10 * time.Second)
	if _, err := use(0); !errors.Is(err, ErrDestroyed) {
		t.Fatalf("destroyed: %v", err)
	}
	if held.Key != nil {
		t.Fatal("a destroyed key's material is still held")
	}
	src.missing = true
	clk.Add(10 * time.Second)
	if _, err := use(0); !errors.Is(err, ErrNotFound) {
		t.Fatalf("vanished: %v", err)
	}
}

// TestCacheReleasesAfterUse: material dropped while a caller is using it
// is released when that caller is done, not under it; Sweep releases
// material nobody has used for a lease.
func TestCacheReleasesAfterUse(t *testing.T) {
	ctx := context.Background()
	src := &fakeSource{key: Key{ID: 3, State: StateEnabled, Version: 1, Generation: 1}, material: map[int][]byte{1: testMaterial(7)}}
	clk := &fakeClock{now: time.Unix(1000, 0)}
	c := NewCache(src, CacheConfig{Lease: time.Second, Now: clk.Now})
	err := c.With(ctx, 3, 0, func(m *Material) error {
		c.Forget(3)
		if !bytes.Equal(m.Key, testMaterial(7)) {
			t.Fatal("material released while in use")
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	var held *Material
	_ = c.With(ctx, 3, 0, func(m *Material) error { held = m; return nil })
	c.Sweep()
	if held.Key == nil {
		t.Fatal("swept within the lease")
	}
	clk.Add(time.Second)
	c.Sweep()
	if held.Key != nil {
		t.Fatal("not swept after the lease")
	}
}

// TestCacheConcurrent exercises the cache from many goroutines while the
// key changes underneath (run under -race).
func TestCacheConcurrent(t *testing.T) {
	ctx := context.Background()
	src := &fakeSource{key: Key{ID: 1, State: StateEnabled, Version: 1, Generation: 1}, material: map[int][]byte{1: testMaterial(1)}}
	c := NewCache(src, CacheConfig{Lease: time.Millisecond})
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 200; i++ {
				_ = c.With(ctx, 1, 0, func(m *Material) error {
					if !bytes.Equal(m.Key, testMaterial(1)) {
						t.Error("wrong or released material")
					}
					return nil
				})
				if i%50 == 0 {
					c.Sweep()
				}
			}
		}()
	}
	for i := 0; i < 50; i++ {
		src.change(func(k *Key) {})
		time.Sleep(100 * time.Microsecond)
	}
	wg.Wait()
	c.Close()
}

// TestCacheAEAD (issue #310, step 3): AEAD gives AES-GCM under a version
// of a key — the current one, then a rotated one for new seals while the
// old version still opens — made once per cached version, and refuses a
// destroyed key after the lease as With does.
func TestCacheAEAD(t *testing.T) {
	ctx := context.Background()
	src := &fakeSource{key: Key{ID: 9, State: StateEnabled, Version: 1, Generation: 1}, material: map[int][]byte{1: testMaterial(1), 2: testMaterial(2)}}
	clk := &fakeClock{now: time.Unix(1000, 0)}
	c := NewCache(src, CacheConfig{Lease: 10 * time.Second, Now: clk.Now})
	defer c.Close()
	a1, v, err := c.AEAD(ctx, 9, 0)
	if err != nil || v != 1 {
		t.Fatalf("current: v%d %v", v, err)
	}
	if again, _, _ := c.AEAD(ctx, 9, 1); again != a1 {
		t.Fatal("the AEAD is made again for a cached version")
	}
	nonce := make([]byte, a1.NonceSize())
	sealed := a1.Seal(nil, nonce, []byte("row"), nil)
	src.change(func(k *Key) { k.Version = 2 })
	clk.Add(10 * time.Second)
	if _, v, err := c.AEAD(ctx, 9, 0); err != nil || v != 2 {
		t.Fatalf("after rotation: v%d %v", v, err)
	}
	old, _, err := c.AEAD(ctx, 9, 1)
	if err != nil {
		t.Fatal(err)
	}
	if p, err := old.Open(nil, nonce, sealed, nil); err != nil || string(p) != "row" {
		t.Fatalf("v1 after rotation: %q %v", p, err)
	}
	src.change(func(k *Key) { k.State = StateDestroyed })
	clk.Add(10 * time.Second)
	if _, _, err := c.AEAD(ctx, 9, 1); !errors.Is(err, ErrDestroyed) {
		t.Fatalf("destroyed: %v", err)
	}
}

// manySource is a Source over many keys, id i's material being i's byte
// repeated, counting reads.
type manySource struct {
	mu    sync.Mutex
	gets  int
	state map[int64]State
}

func (m *manySource) GetByID(_ context.Context, id int64) (Key, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.gets++
	st := StateEnabled
	if s, ok := m.state[id]; ok {
		st = s
	}
	return Key{ID: id, State: st, Version: 1, Generation: 1}, nil
}

func (m *manySource) Unwrap(_ context.Context, id int64, version int) (*Material, error) {
	return &Material{ID: id, Version: 1, Generation: 1, Key: testMaterial(byte(id))}, nil
}

// TestCacheBound (issue #310, step 4): a cache made with MaxKeys holds no
// more keys than that, dropping the least recently used; a key in use is
// not read again while it stays among the most recent; material a caller
// holds while its key is dropped stays usable for that call.
func TestCacheBound(t *testing.T) {
	ctx := context.Background()
	src := &manySource{}
	clk := &fakeClock{now: time.Unix(1000, 0)}
	c := NewCache(src, CacheConfig{Lease: time.Hour, Now: clk.Now, MaxKeys: 4})
	defer c.Close()
	use := func(id int64) {
		t.Helper()
		clk.Add(time.Second) // recency is by the clock
		if err := c.With(ctx, id, 0, func(m *Material) error {
			if !bytes.Equal(m.Key, testMaterial(byte(id))) {
				t.Fatalf("key %d: wrong material", id)
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	}
	for id := int64(1); id <= 100; id++ {
		use(id)
		use(1) // kept hot: never the least recently used
		if n := c.Len(); n > 4 {
			t.Fatalf("after key %d the cache holds %d keys", id, n)
		}
	}
	if src.gets != 100 {
		t.Fatalf("%d reads for 100 keys, one of them used throughout (want 100)", src.gets)
	}
	// Material held across an eviction.
	err := c.With(ctx, 500, 0, func(m *Material) error {
		for id := int64(600); id < 610; id++ {
			use(id)
		}
		if !bytes.Equal(m.Key, testMaterial(byte(500%256))) {
			t.Fatal("held material changed under its caller")
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	// Unbounded by default.
	u := NewCache(src, CacheConfig{Lease: time.Hour, Now: clk.Now})
	defer u.Close()
	for id := int64(1); id <= 50; id++ {
		if _, err := u.Key(ctx, id); err != nil {
			t.Fatal(err)
		}
	}
	if u.Len() != 50 {
		t.Fatalf("an unbounded cache holds %d of 50 keys", u.Len())
	}
}

// TestCacheMAC (issue #310, step 4): MAC is deterministic for one key,
// label and input, differs across keys, labels and inputs, and refuses a
// destroyed key.
func TestCacheMAC(t *testing.T) {
	ctx := context.Background()
	src := &manySource{state: map[int64]State{3: StateDestroyed}}
	c := NewCache(src, CacheConfig{})
	defer c.Close()
	mac := func(id int64, label, data string) string {
		t.Helper()
		m, err := c.MAC(ctx, id, 1, label, []byte(data))
		if err != nil || len(m) != 32 {
			t.Fatalf("MAC: %x %v", m, err)
		}
		return string(m)
	}
	a := mac(1, "l", "x")
	if a != mac(1, "l", "x") {
		t.Fatal("not deterministic")
	}
	for _, other := range []string{mac(2, "l", "x"), mac(1, "m", "x"), mac(1, "l", "y"), mac(1, "lx", "")} {
		if other == a {
			t.Fatal("two inputs share a MAC")
		}
	}
	if _, err := c.MAC(ctx, 3, 1, "l", nil); !errors.Is(err, ErrDestroyed) {
		t.Fatalf("destroyed: %v", err)
	}
}

// TestTenantKeyNames (issue #310, step 4): a destroyed tenant key's name
// leaves the namespace of live tenant keys, and PrefixEnd bounds a scan
// of one prefix.
func TestTenantKeyNames(t *testing.T) {
	n := TenantKeyPrefix + "12/ab"
	d := DestroyedName(n, 77)
	if d != "destroyed/tenant/12/ab/77" {
		t.Fatalf("destroyed name %q", d)
	}
	p := TenantKeyPrefix + "12/"
	end := PrefixEnd(p)
	if end != TenantKeyPrefix+"120" {
		t.Fatalf("end of %q: %q", p, end)
	}
	for _, in := range []string{n, p, p + "\xff"} {
		if !(in >= p && in < end) {
			t.Fatalf("%q is outside [%q, %q)", in, p, end)
		}
	}
	for _, out := range []string{TenantKeyPrefix + "120/x", TenantKeyPrefix + "1/x", d} {
		if out >= p && out < end {
			t.Fatalf("%q is inside [%q, %q)", out, p, end)
		}
	}
	if PrefixEnd("") != "" || PrefixEnd("\xff\xff") != "" || PrefixEnd("a\xff") != "b" {
		t.Fatal("PrefixEnd edge cases")
	}
}

// BenchmarkCacheHitParallel is a cached hit — a key whose lease is valid
// and whose version is held — from every core at once, the way a busy
// gateway's sealed-table reads and writes ask: AEAD (what the sealing of
// rows uses) and With (what the key service uses), on one hot key and
// spread over many keys of a bounded cache.
func BenchmarkCacheHitParallel(b *testing.B) {
	ctx := context.Background()
	for _, bc := range []struct {
		name string
		keys int64
		max  int
	}{{"one-key", 1, 0}, {"1000-keys-bounded", 1000, 10000}} {
		c := NewCache(&manySource{}, CacheConfig{Lease: time.Hour, MaxKeys: bc.max})
		for id := int64(1); id <= bc.keys; id++ {
			if _, _, err := c.AEAD(ctx, id, 0); err != nil {
				b.Fatal(err)
			}
		}
		b.Run("AEAD/"+bc.name, func(b *testing.B) {
			var n atomic.Int64
			b.RunParallel(func(pb *testing.PB) {
				id := n.Add(1)
				for pb.Next() {
					id = id%bc.keys + 1
					if _, _, err := c.AEAD(ctx, id, 0); err != nil {
						b.Error(err)
						return
					}
				}
			})
		})
		b.Run("With/"+bc.name, func(b *testing.B) {
			var n atomic.Int64
			b.RunParallel(func(pb *testing.PB) {
				id := n.Add(1)
				for pb.Next() {
					id = id%bc.keys + 1
					if err := c.With(ctx, id, 0, func(*Material) error { return nil }); err != nil {
						b.Error(err)
						return
					}
				}
			})
		})
		c.Close()
	}
}

// TestCacheHitRace (issue #310, step 4): hits on the lock-free path race
// checks, rotations, Forget, sweeps and evictions without a caller ever
// seeing released material, and a destroyed key is refused once the
// lease after the change has run out.
func TestCacheHitRace(t *testing.T) {
	ctx := context.Background()
	src := &fakeSource{key: Key{ID: 9, State: StateEnabled, Version: 1, Generation: 1}, material: map[int][]byte{1: testMaterial(1), 2: testMaterial(2)}}
	clk := &fakeClock{now: time.Unix(1000, 0)}
	c := NewCache(src, CacheConfig{Lease: 10 * time.Second, Now: clk.Now, MaxKeys: 2})
	defer c.Close()
	stop := make(chan struct{})
	var wg sync.WaitGroup
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				err := c.With(ctx, 9, 0, func(m *Material) error {
					if len(m.Key) != 32 || (m.Key[0] != 1 && m.Key[0] != 2) || m.Key[31] != m.Key[0] {
						t.Errorf("material in use was released: %x", m.Key)
					}
					return nil
				})
				if err != nil && !errors.Is(err, ErrNotFound) {
					t.Errorf("With: %v", err)
				}
				if a, _, err := c.AEAD(ctx, 9, 0); err == nil && a == nil {
					t.Error("nil AEAD")
				}
				_, _ = c.Key(ctx, int64(100+w)) // churn the bound
			}
		}()
	}
	for i := 0; i < 300; i++ {
		switch i % 5 {
		case 0:
			src.change(func(k *Key) { k.Version = 1 + i%2 })
			clk.Add(10 * time.Second)
		case 1:
			c.Forget(9)
		case 2:
			c.Sweep()
		case 3:
			clk.Add(time.Second)
		}
	}
	close(stop)
	wg.Wait()
	src.change(func(k *Key) { k.State = StateDestroyed })
	clk.Add(10 * time.Second)
	if _, _, err := c.AEAD(ctx, 9, 0); !errors.Is(err, ErrDestroyed) {
		t.Fatalf("AEAD of a destroyed key after the lease: %v", err)
	}
	if err := c.With(ctx, 9, 0, func(*Material) error { return nil }); !errors.Is(err, ErrDestroyed) {
		t.Fatalf("With of a destroyed key after the lease: %v", err)
	}
}
