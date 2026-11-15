package keystore

import (
	"bytes"
	"context"
	"errors"
	"sync"
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
