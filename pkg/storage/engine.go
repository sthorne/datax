// Package storage implements datax's storage layer: a thin wrapper around
// Pebble plus MVCC (multi-version concurrency control) operations with
// write-intent support. See docs/architecture.md and docs/transactions.md.
package storage

import (
	"bytes"
	"fmt"
	"os"
	"sync"
	"sync/atomic"

	"github.com/sthorne/datax/pkg/util/faultpoint"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/vfs"

	"github.com/sthorne/datax/pkg/storage/enc"
	"github.com/sthorne/datax/pkg/util/hlc"
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
	// Prev steps to the previous key; false at the lower bound.
	Prev() bool
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
	// cacheHeld: this engine holds a reference on the shared block cache.
	cacheHeld bool
	// walDisabled: the engine runs without a WAL (see Options.DisableWAL);
	// flushedSeq is the largest sequence number an sstable holds, i.e.
	// what is durable.
	walDisabled bool
	flushedSeq  atomic.Uint64
	// explicitFlush is set around Engine.Flush: a flush datax asked for
	// (a split store's structural points, shutdown) is not the memtable
	// rotation the flush-begin fault point means to catch.
	explicitFlush atomic.Int32
}

// testingPebbleOptions, when set, adjusts the Pebble options after the
// profile applied them — tests shrink memtables and target file sizes so
// a few megabytes build a multi-level LSM. Never set in production.
var testingPebbleOptions func(*pebble.Options)

// Open opens (creating if needed) a Pebble store in dir with the given
// options (zero value = balanced profile, plaintext). If dir is empty an
// in-memory store is used (tests, demo mode).
func Open(dir string, o Options) (*Engine, error) {
	testingNoSyncOnce.Do(func() {
		if os.Getenv(testingNoSyncEnv) == "1" {
			TestingNoSync = true
			fmt.Fprintf(os.Stderr, "WARNING: %s=1: raft log commits are not synced; a crash loses acknowledged writes (measurement only)\n", testingNoSyncEnv)
		}
	})
	e := &Engine{walDisabled: o.DisableWAL}
	opts := &pebble.Options{}
	e.health.gate = o.Profile.apply(opts)
	if o.Raft {
		opts.MemTableSize = 16 << 20
		opts.MemTableStopWritesThreshold = 4
	}
	if o.MemTableSize > 0 {
		opts.MemTableSize = uint64(o.MemTableSize)
	}
	opts.DisableWAL = o.DisableWAL
	cacheSize := o.CacheSize
	if cacheSize <= 0 {
		cacheSize = DefaultCacheSize(o.Profile)
	}
	opts.Cache = acquireCache(cacheSize)
	e.cacheHeld = true
	if testingPebbleOptions != nil {
		testingPebbleOptions(opts)
	}
	opts.EventListener = &pebble.EventListener{
		WriteStallBegin: func(info pebble.WriteStallBeginInfo) {
			e.health.stalls.Add(1)
			e.health.inStall.Store(true)
		},
		WriteStallEnd: func() { e.health.inStall.Store(false) },
		FlushBegin: func(pebble.FlushInfo) {
			if e.explicitFlush.Load() == 0 {
				faultpoint.Hit("flush-begin")
			}
		},
		FlushEnd: func(info pebble.FlushInfo) {
			// Memtables flush in order, so the newest output's largest
			// sequence number is the durability watermark.
			var largest uint64
			for _, t := range info.Output {
				if t.LargestSeqNum > largest {
					largest = t.LargestSeqNum
				}
			}
			for {
				cur := e.flushedSeq.Load()
				if largest <= cur || e.flushedSeq.CompareAndSwap(cur, largest) {
					break
				}
			}
			faultpoint.Hit("flush-end")
		},
		DiskSlow: func(pebble.DiskSlowInfo) { e.health.diskSlow.Add(1) },
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
		releaseCache()
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

// TestingSkipFlushOnClose makes Close skip the WAL-less engine's final
// flush, so a test can close a store the way a crash would leave it.
var TestingSkipFlushOnClose bool

// TestingNoSync turns every synced commit into an unsynced one, so a
// measurement can separate the disk's sync rate from the protocol's own
// ceiling (issue #106). Durability is off while it is set: a crash loses
// acknowledged writes. The environment variable DATAX_TESTING_NOSYNC=1
// sets it at the first Open; a warning is logged.
var TestingNoSync bool

const testingNoSyncEnv = "DATAX_TESTING_NOSYNC"

var testingNoSyncOnce sync.Once

func (e *Engine) Close() error {
	if e.walDisabled && !TestingSkipFlushOnClose {
		// Without a WAL the memtable is the only copy of recent writes: a
		// clean shutdown persists them so the restart has nothing to
		// replay. A crash skips this, and the raft log replays the rest.
		if err := e.Flush(); err != nil {
			return err
		}
	}
	err := e.db.Close()
	if e.cacheHeld {
		e.cacheHeld = false
		releaseCache()
	}
	return err
}

// WALDisabled reports whether the engine runs without a write-ahead log
// (Options.DisableWAL): committed writes become durable at the next
// flush, not at commit.
func (e *Engine) WALDisabled() bool { return e.walDisabled }

// FlushedSeqNum is the largest sequence number durably flushed to an
// sstable (0 until the first flush). A batch whose SeqNum is at or
// below it has reached disk, whatever the WAL setting.
func (e *Engine) FlushedSeqNum() uint64 { return e.flushedSeq.Load() }

// WriteMetrics is what the engine has written since it opened: WAL
// bytes, bytes flushed from memtables, bytes written by compactions.
type WriteMetrics struct {
	WALBytes       uint64
	FlushedBytes   uint64
	CompactedBytes uint64
}

// WriteMetrics reads the engine's cumulative write counters (the write
// amplification an operator watches: WAL + flushed + compacted per
// logical byte).
func (e *Engine) WriteMetrics() WriteMetrics {
	m := e.db.Metrics()
	var w WriteMetrics
	w.WALBytes = m.WAL.BytesWritten
	for i := range m.Levels {
		w.FlushedBytes += m.Levels[i].BytesFlushed
		w.CompactedBytes += m.Levels[i].BytesCompacted
	}
	return w
}

// CacheSize is the block cache the engine shares with the process's
// other engines, in bytes.
func (e *Engine) CacheSize() int64 { return SharedCacheSize() }

// Flush synchronously flushes memtables to disk. Used by tests.
func (e *Engine) Flush() error {
	e.explicitFlush.Add(1)
	defer e.explicitFlush.Add(-1)
	return e.db.Flush()
}

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
	return &Batch{b: e.db.NewIndexedBatch(), eng: e}
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
	b   *pebble.Batch
	eng *Engine
	seq uint64
	// wit is the batch's reusable write iterator (see writeState).
	wit *pebble.Iterator
}

// writeState is what a write to a key would land on, from one bounded
// seek at the key's MVCC prefix [metaKey, upper): the first engine key
// there is the intent's metadata (rawMeta != nil) or, with no intent,
// the newest version (hasVersion, at vts) — or nothing. The seek runs on
// one iterator the batch keeps for all its writes: creating an iterator
// per key (the stack of memtable and sstable iterators behind it) was
// most of what a write cost (issue #106). SetOptions refreshes the
// iterator's view of the batch, so a key this batch already wrote is
// seen as written.
func (b *Batch) writeState(metaKey, upper []byte) (rawMeta []byte, vts hlc.Timestamp, hasVersion bool, err error) {
	opts := pebble.IterOptions{LowerBound: metaKey, UpperBound: upper}
	if b.wit == nil {
		if b.wit, err = b.b.NewIter(&opts); err != nil {
			return nil, hlc.Timestamp{}, false, err
		}
	} else {
		b.wit.SetOptions(&opts)
	}
	it := b.wit
	if !it.SeekGE(metaKey) {
		return nil, hlc.Timestamp{}, false, it.Error()
	}
	if bytes.Equal(it.Key(), metaKey) {
		v, err := it.ValueAndErr()
		if err != nil {
			return nil, hlc.Timestamp{}, false, err
		}
		return append([]byte(nil), v...), hlc.Timestamp{}, false, nil
	}
	_, vts, err = DecodeMVCCKey(it.Key())
	if err != nil {
		return nil, hlc.Timestamp{}, false, err
	}
	return nil, vts, true, nil
}

// closeWriteIter releases the write iterator (before the batch commits
// or closes: the iterator borrows the batch's memory).
func (b *Batch) closeWriteIter() error {
	if b.wit == nil {
		return nil
	}
	err := b.wit.Close()
	b.wit = nil
	return err
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
	if err := b.closeWriteIter(); err != nil {
		return err
	}
	opt := pebble.NoSync
	if sync && !b.eng.walDisabled && !TestingNoSync {
		// Without a WAL there is nothing to sync: durability is the next
		// flush (Engine.FlushedSeqNum), and Pebble refuses a Sync commit.
		opt = pebble.Sync
	}
	if err := b.b.Commit(opt); err != nil {
		return err
	}
	b.seq = b.b.SeqNum()
	return nil
}

// SeqNum is the sequence number the batch committed at (0 before
// Commit); compare with Engine.FlushedSeqNum to learn whether it is
// durable on a WAL-less engine.
func (b *Batch) SeqNum() uint64 { return b.seq }

func (b *Batch) Close() error {
	err := b.closeWriteIter()
	if cerr := b.b.Close(); err == nil {
		err = cerr
	}
	return err
}

// Empty reports whether nothing has been staged.
func (b *Batch) Empty() bool { return b.b.Empty() }

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
func (i *pebbleIter) Prev() bool             { i.valid = i.it.Prev(); return i.valid }
func (i *pebbleIter) Valid() bool            { return i.valid }
func (i *pebbleIter) Key() []byte            { return i.it.Key() }
func (i *pebbleIter) Value() []byte          { return i.it.Value() }
func (i *pebbleIter) Close() error           { return i.it.Close() }

type errIter struct{ err error }

func (i *errIter) SeekGE([]byte) bool { return false }
func (i *errIter) SeekLT([]byte) bool { return false }
func (i *errIter) Next() bool         { return false }
func (i *errIter) Prev() bool         { return false }
func (i *errIter) Valid() bool        { return false }
func (i *errIter) Key() []byte        { return nil }
func (i *errIter) Value() []byte      { return nil }
func (i *errIter) Close() error       { return i.err }
