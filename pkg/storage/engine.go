// Package storage implements datax's storage layer: a thin wrapper around
// Pebble plus MVCC (multi-version concurrency control) operations with
// write-intent support. See docs/architecture.md and docs/transactions.md.
package storage

import (
	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/vfs"
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
	Next() bool
	Valid() bool
	Key() []byte
	Value() []byte
	Close() error
}

// Engine is a Pebble store.
type Engine struct {
	db *pebble.DB
}

// Open opens (creating if needed) a Pebble store in dir. If dir is empty an
// in-memory store is used (tests, demo mode).
func Open(dir string) (*Engine, error) {
	opts := &pebble.Options{}
	if dir == "" {
		opts.FS = vfs.NewMem()
		dir = "in-mem"
	}
	db, err := pebble.Open(dir, opts)
	if err != nil {
		return nil, err
	}
	return &Engine{db: db}, nil
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
func (i *pebbleIter) Next() bool             { i.valid = i.it.Next(); return i.valid }
func (i *pebbleIter) Valid() bool            { return i.valid }
func (i *pebbleIter) Key() []byte            { return i.it.Key() }
func (i *pebbleIter) Value() []byte          { return i.it.Value() }
func (i *pebbleIter) Close() error           { return i.it.Close() }

type errIter struct{ err error }

func (i *errIter) SeekGE([]byte) bool { return false }
func (i *errIter) Next() bool         { return false }
func (i *errIter) Valid() bool        { return false }
func (i *errIter) Key() []byte        { return nil }
func (i *errIter) Value() []byte      { return nil }
func (i *errIter) Close() error       { return i.err }
