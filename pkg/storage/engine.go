// Package storage implements datax's storage layer: a thin wrapper around
// Pebble plus MVCC (multi-version concurrency control) operations with
// write-intent support. See docs/architecture.md and docs/transactions.md.
package storage

import (
	"fmt"
	"sync"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/vfs"

	"github.com/sthorne/datax/pkg/storage/enc"
)

// Reader provides read access to an engine or batch.
type Reader interface {
	// Get returns a copy of the value for key, or nil if absent.
	Get(key []byte) ([]byte, error)
	// NewIter returns an iterator over [lower, upper).
	NewIter(lower, upper []byte) Iterator
}

// Writer provides write access. All writes are buffered in a batch and
// atomic on commit.
type Writer interface {
	Put(key, value []byte) error
	Delete(key []byte) error
}

// Iterator iterates over raw engine keys. Key/Value are only valid until the
// next positioning call.
type Iterator interface {
	SeekGE(key []byte) bool
	// SeekLT positions at the LARGEST key strictly below key (reverse
	// scans walk user keys backwards with it).
	SeekLT(key []byte) bool
	Next() bool
	Valid() bool
	Key() []byte
	Value() []byte
	Close() error
}

// Engine is a Pebble store.
type Engine struct {
	db     *pebble.DB
	health health
	// Encryption state (all zero for a plaintext store): the base
	// (unencrypted) FS and dir for registry/header access, the unsealed
	// key set, and the online-maintenance state (see reencrypt.go).
	encBase vfs.FS
	encDir  string
	encKeys *enc.KeySet
	encMu   sync.Mutex // serializes registry reseals
	reenc   reencStatusCache
}

// Open opens (creating if needed) a Pebble store in dir with the given
// options (zero value = balanced profile, plaintext). If dir is empty an
// in-memory store is used (tests, demo mode).
func Open(dir string, o Options) (*Engine, error) {
	e := &Engine{}
	opts := &pebble.Options{}
	e.health.gate = o.Profile.apply(opts)
	opts.EventListener = &pebble.EventListener{
		WriteStallBegin: func(info pebble.WriteStallBeginInfo) {
			e.health.stalls.Add(1)
			e.health.inStall.Store(true)
		},
		WriteStallEnd: func() { e.health.inStall.Store(false) },
		DiskSlow:      func(pebble.DiskSlowInfo) { e.health.diskSlow.Add(1) },
		BackgroundError: func(err error) {
			e.health.bgErrors.Add(1)
		},
	}
	base := vfs.Default
	if dir == "" {
		base = vfs.NewMem()
		dir = "in-mem"
	}
	fs, keys, err := maybeEncrypt(base, dir, o.EncryptionKey)
	if err != nil {
		return nil, err
	}
	if keys != nil {
		e.encBase, e.encDir, e.encKeys = base, dir, keys
	}
	opts.FS = fs
	db, err := pebble.Open(dir, opts)
	if err != nil {
		return nil, err
	}
	e.db = db
	return e, nil
}

// maybeEncrypt validates the store's encryption state against the provided
// key and returns the FS Pebble should use. The matrix:
//
//   - registry + correct key  -> encrypting FS (a fresh data key is minted)
//   - registry + wrong key    -> error (GCM authentication failure)
//   - registry + no key       -> error: key required
//   - no registry + key, but the store already has files -> error: refusing
//     to encrypt an existing plaintext store (there is no silent conversion;
//     the ciphertext-looking sstables would just be unreadable garbage)
//   - no registry + key + empty dir -> initialize encryption
//   - no key, no registry     -> plaintext, exactly as before
func maybeEncrypt(base vfs.FS, dir string, key []byte) (vfs.FS, *enc.KeySet, error) {
	encrypted := enc.RegistryExists(base, dir)
	if key == nil {
		if encrypted {
			return nil, nil, fmt.Errorf("store in %s is encrypted; --enc-key is required", dir)
		}
		return base, nil, nil
	}
	if !encrypted && storeHasFiles(base, dir) {
		return nil, nil, fmt.Errorf("store in %s exists unencrypted; refusing to open it with an encryption key (encrypting in place is not supported)", dir)
	}
	keys, err := enc.LoadOrInitRegistry(base, dir, key)
	if err != nil {
		return nil, nil, err
	}
	return enc.NewFS(base, keys), keys, nil
}

// storeHasFiles reports whether dir already holds a Pebble store (CURRENT /
// MANIFEST present).
func storeHasFiles(base vfs.FS, dir string) bool {
	names, err := base.List(dir)
	if err != nil {
		return false
	}
	for _, n := range names {
		if n == "CURRENT" || len(n) >= 8 && n[:8] == "MANIFEST" {
			return true
		}
	}
	return false
}

func (e *Engine) Close() error { return e.db.Close() }

// Flush synchronously flushes memtables to disk. Used by tests.
func (e *Engine) Flush() error { return e.db.Flush() }

func (e *Engine) Get(key []byte) ([]byte, error) {
	v, closer, err := e.db.Get(key)
	if err == pebble.ErrNotFound {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	out := append([]byte(nil), v...)
	_ = closer.Close()
	return out, nil
}

func (e *Engine) NewIter(lower, upper []byte) Iterator {
	it, err := e.db.NewIter(&pebble.IterOptions{LowerBound: lower, UpperBound: upper})
	if err != nil {
		return &errIter{err: err}
	}
	return &pebbleIter{it: it}
}

// Put writes directly (non-batched). Prefer batches for anything that must
// be atomic or durable with other writes.
func (e *Engine) Put(key, value []byte) error {
	return e.db.Set(key, value, pebble.NoSync)
}

func (e *Engine) Delete(key []byte) error {
	return e.db.Delete(key, pebble.NoSync)
}

// NewBatch returns an indexed batch: writes are visible to reads through the
// batch itself (required by MVCC read-modify-write sequences), and atomic on
// Commit.
func (e *Engine) NewBatch() *Batch {
	return &Batch{b: e.db.NewIndexedBatch()}
}

// NewSnapshot returns a consistent point-in-time read view. Range snapshots
// are captured through one of these so the applied index and the data it
// covers are mutually consistent.
func (e *Engine) NewSnapshot() *Snapshot {
	return &Snapshot{s: e.db.NewSnapshot()}
}

// Snapshot is a consistent read view of the engine.
type Snapshot struct {
	s *pebble.Snapshot
}

func (s *Snapshot) Get(key []byte) ([]byte, error) {
	v, closer, err := s.s.Get(key)
	if err == pebble.ErrNotFound {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	out := append([]byte(nil), v...)
	_ = closer.Close()
	return out, nil
}

func (s *Snapshot) NewIter(lower, upper []byte) Iterator {
	it, err := s.s.NewIter(&pebble.IterOptions{LowerBound: lower, UpperBound: upper})
	if err != nil {
		return &errIter{err: err}
	}
	return &pebbleIter{it: it}
}

func (s *Snapshot) Close() error { return s.s.Close() }

// Batch is an atomic, indexed write batch.
type Batch struct {
	b *pebble.Batch
}

func (b *Batch) Get(key []byte) ([]byte, error) {
	v, closer, err := b.b.Get(key)
	if err == pebble.ErrNotFound {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	out := append([]byte(nil), v...)
	_ = closer.Close()
	return out, nil
}

func (b *Batch) NewIter(lower, upper []byte) Iterator {
	it, err := b.b.NewIter(&pebble.IterOptions{LowerBound: lower, UpperBound: upper})
	if err != nil {
		return &errIter{err: err}
	}
	return &pebbleIter{it: it}
}

func (b *Batch) Put(key, value []byte) error { return b.b.Set(key, value, nil) }
func (b *Batch) Delete(key []byte) error     { return b.b.Delete(key, nil) }

// DeleteRange deletes every key in [start, end).
func (b *Batch) DeleteRange(start, end []byte) error { return b.b.DeleteRange(start, end, nil) }

// Commit applies the batch. sync=true forces an fsync before returning —
// required for Raft state (HardState, log entries) and applied state.
func (b *Batch) Commit(sync bool) error {
	opt := pebble.NoSync
	if sync {
		opt = pebble.Sync
	}
	return b.b.Commit(opt)
}

func (b *Batch) Close() error { return b.b.Close() }

// Repr returns the batch's serialized representation (valid before Commit).
// Used to ship a batch through Raft.
func (b *Batch) Repr() []byte { return b.b.Repr() }

type pebbleIter struct {
	it    *pebble.Iterator
	valid bool
}

func (i *pebbleIter) SeekGE(key []byte) bool { i.valid = i.it.SeekGE(key); return i.valid }
func (i *pebbleIter) SeekLT(key []byte) bool { i.valid = i.it.SeekLT(key); return i.valid }
func (i *pebbleIter) Next() bool             { i.valid = i.it.Next(); return i.valid }
func (i *pebbleIter) Valid() bool            { return i.valid }
func (i *pebbleIter) Key() []byte            { return i.it.Key() }
func (i *pebbleIter) Value() []byte          { return i.it.Value() }
func (i *pebbleIter) Close() error           { return i.it.Close() }

type errIter struct{ err error }

func (i *errIter) SeekGE([]byte) bool { return false }
func (i *errIter) SeekLT([]byte) bool { return false }
func (i *errIter) Next() bool         { return false }
func (i *errIter) Valid() bool        { return false }
func (i *errIter) Key() []byte        { return nil }
func (i *errIter) Value() []byte      { return nil }
func (i *errIter) Close() error       { return i.err }
