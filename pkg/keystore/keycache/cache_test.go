package keycache

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/sthorne/datax/pkg/storage/enc"
)

// fakeSource is a key store of one key, whose state a test changes.
type fakeSource struct {
	mu        sync.Mutex
	versions  map[int][]byte
	current   int
	gen       int64
	state     error // nil, ErrDisabled or ErrDestroyed
	unwraps   int
	genChecks int
}

func newFake(t *testing.T) *fakeSource {
	f := &fakeSource{versions: map[int][]byte{}, gen: 1}
	f.rotate(t)
	f.gen = 1
	return f
}

func (f *fakeSource) rotate(t *testing.T) {
	f.mu.Lock()
	defer f.mu.Unlock()
	k := make([]byte, enc.KeyLen)
	if _, err := rand.Read(k); err != nil {
		t.Fatal(err)
	}
	f.current++
	f.versions[f.current] = k
	f.gen++
}

func (f *fakeSource) set(state error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.state = state
	f.gen++
}

func (f *fakeSource) Unwrap(_ context.Context, id int64, v int) (*Material, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.unwraps++
	if f.state != nil {
		return nil, f.state
	}
	if v == 0 {
		v = f.current
	}
	k, ok := f.versions[v]
	if !ok {
		return nil, ErrNoVersion
	}
	return &Material{ID: id, Version: v, Generation: f.gen, Key: enc.HoldKey(k)}, nil
}

func (f *fakeSource) Generation(context.Context, int64) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.genChecks++
	return f.gen, nil
}

type clock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *clock) Now() time.Time { c.mu.Lock(); defer c.mu.Unlock(); return c.now }
func (c *clock) advance(d time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(d)
	c.mu.Unlock()
}

func seal(t *testing.T, k Key, msg string) []byte {
	t.Helper()
	nonce := make([]byte, k.AEAD.NonceSize())
	return append(nonce, k.AEAD.Seal(nil, nonce, []byte(msg), nil)...)
}

func open(k Key, sealed []byte) (string, error) {
	n := k.AEAD.NonceSize()
	p, err := k.AEAD.Open(nil, sealed[:n], sealed[n:], nil)
	return string(p), err
}

// TestCacheLeaseAndRevocation: within the lease a key is used without
// reading the store; past it the generation is read once, and a change of
// state — disable, destroy — takes the key away; a rotation moves the
// current version while older versions still open.
func TestCacheLeaseAndRevocation(t *testing.T) {
	ctx := context.Background()
	f := newFake(t)
	clk := &clock{now: time.Unix(1000, 0)}
	c := New(f, Options{Lease: 2 * time.Second, Now: clk.Now})
	defer c.Close()

	k1, err := c.Current(ctx, 7)
	if err != nil || k1.Version != 1 {
		t.Fatalf("current: %+v %v", k1, err)
	}
	sealed := seal(t, k1, "hello")
	for i := 0; i < 100; i++ {
		if _, err := c.Current(ctx, 7); err != nil {
			t.Fatal(err)
		}
	}
	if f.unwraps != 1 || f.genChecks != 0 {
		t.Fatalf("within the lease: %d unwraps, %d generation reads", f.unwraps, f.genChecks)
	}

	// A rotation is seen after the lease, for new writes; v1 still opens.
	f.rotate(t)
	if k, _ := c.Current(ctx, 7); k.Version != 1 {
		t.Fatalf("rotation seen before the lease ran out: v%d", k.Version)
	}
	clk.advance(2 * time.Second)
	k2, err := c.Current(ctx, 7)
	if err != nil || k2.Version != 2 {
		t.Fatalf("after the lease: %+v %v", k2, err)
	}
	old, err := c.Version(ctx, 7, 1)
	if err != nil {
		t.Fatal(err)
	}
	if got, err := open(old, sealed); err != nil || got != "hello" {
		t.Fatalf("v1 after rotation: %q %v", got, err)
	}
	if _, err := c.Version(ctx, 7, 9); !errors.Is(err, ErrNoVersion) {
		t.Fatalf("a version that does not exist: %v", err)
	}

	// Unchanged generation: the lease renews with one read.
	checks := f.genChecks
	clk.advance(3 * time.Second)
	if _, err := c.Current(ctx, 7); err != nil {
		t.Fatal(err)
	}
	if f.genChecks != checks+1 {
		t.Fatalf("lease renewal read the generation %d times", f.genChecks-checks)
	}

	// Disabled: usable until the lease runs out, refused after.
	f.set(ErrDisabled)
	if _, err := c.Version(ctx, 7, 1); err != nil {
		t.Fatalf("within the lease after disable: %v", err)
	}
	clk.advance(2 * time.Second)
	if _, err := c.Version(ctx, 7, 1); !errors.Is(err, ErrDisabled) {
		t.Fatalf("after the lease, disabled: %v", err)
	}
	if c.Len() != 0 {
		t.Fatalf("a disabled key left %d versions cached", c.Len())
	}
	f.set(nil)
	if _, err := c.Version(ctx, 7, 1); err != nil {
		t.Fatalf("re-enabled: %v", err)
	}
	f.set(ErrDestroyed)
	c.Invalidate(7)
	if _, err := c.Current(ctx, 7); !errors.Is(err, ErrDestroyed) {
		t.Fatalf("destroyed, invalidated: %v", err)
	}
}

// TestCacheMaterialZeroed: material handed to WithMaterial is the key's,
// and it is zeroed when the key leaves the cache.
func TestCacheMaterialZeroed(t *testing.T) {
	ctx := context.Background()
	f := newFake(t)
	c := New(f, Options{})
	var held []byte
	if err := c.WithMaterial(ctx, 3, 0, func(v int, m []byte) error {
		if v != 1 || !bytes.Equal(m, f.versions[1]) {
			t.Fatalf("material v%d differs", v)
		}
		held = m
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	c.Close()
	if !bytes.Equal(held, make([]byte, enc.KeyLen)) {
		t.Fatal("released material is not zeroed")
	}
}

// TestCacheConcurrent exercises the cache from many goroutines while the
// lease keeps running out (for the race detector).
func TestCacheConcurrent(t *testing.T) {
	ctx := context.Background()
	f := newFake(t)
	c := New(f, Options{Lease: time.Millisecond})
	defer c.Close()
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 200; i++ {
				k, err := c.Current(ctx, 1)
				if err != nil {
					t.Error(err)
					return
				}
				if _, err := open(k, seal(t, k, "x")); err != nil {
					t.Error(err)
					return
				}
				_ = c.WithMaterial(ctx, 1, 1, func(int, []byte) error { return nil })
				if i%50 == 0 {
					f.rotate(t)
				}
			}
		}()
	}
	wg.Wait()
}
