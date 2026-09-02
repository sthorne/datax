package storage

import (
	"bytes"
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/sthorne/datax/pkg/storage/enc"
)

// Online encryption maintenance. Store-key rotation is a registry reseal
// — data keys, and so file contents, are untouched, which is what makes
// it safe on a live engine (Pebble never rewrites the registry after
// Open; the engine lock only serializes concurrent rotations).
// Re-encryption schedules manual compactions over live sstables still
// encrypted with retired data keys; a rewritten file is encrypted with
// the ACTIVE data key, so repeated passes converge to "no live sstable
// under a retired key". Only sstables can be stale: the WAL, MANIFEST,
// and OPTIONS are recreated on every Open under that session's fresh
// active key.

// Encrypted reports whether the store is encrypted at rest.
func (e *Engine) Encrypted() bool { return e.encKeys != nil }

// RotateStoreKeyLive re-seals the key registry under newKey on a LIVE
// engine. oldKey is verified against the on-disk registry (GCM
// authentication), so rotating the wrong node — or racing an earlier
// rotation — fails cleanly. Atomic on disk (tmp + rename).
func (e *Engine) RotateStoreKeyLive(oldKey, newKey []byte) error {
	if !e.Encrypted() {
		return fmt.Errorf("store is not encrypted")
	}
	e.encMu.Lock()
	defer e.encMu.Unlock()
	return enc.RotateStoreKey(e.encBase, e.encDir, oldKey, newKey)
}

// reencStatusCache caches the stale-table sweep (~one small pread per
// live sstable) behind a TTL, for the /metrics gauge.
type reencStatusCache struct {
	mu        sync.Mutex
	at        time.Time
	bytes     int64
	files     int
	sweepErrs int64
}

const reencStatusTTL = 10 * time.Second

// staleTable is one live sstable still encrypted under a retired key.
type staleTable struct {
	size     uint64
	smallest []byte
	largest  []byte
}

// staleTables sweeps the live sstables and returns those whose data key
// is not the active one. Files deleted mid-sweep are skipped.
func (e *Engine) staleTables() ([]staleTable, error) {
	if !e.Encrypted() {
		return nil, nil
	}
	activeID, _ := e.encKeys.Active()
	levels, err := e.db.SSTables()
	if err != nil {
		return nil, err
	}
	var out []staleTable
	for _, level := range levels {
		for _, t := range level {
			name := fmt.Sprintf("%06d.sst", t.FileNum)
			id, ok, err := enc.FileKeyID(e.encBase, e.encBase.PathJoin(e.encDir, name))
			if err != nil || !ok {
				continue // deleted under us, or headerless
			}
			if id != activeID {
				out = append(out, staleTable{
					size:     t.Size,
					smallest: append([]byte(nil), t.Smallest.UserKey...),
					largest:  append([]byte(nil), t.Largest.UserKey...),
				})
			}
		}
	}
	return out, nil
}

// ReencryptionStatus reports live sstable bytes and files still encrypted
// under retired data keys, refreshed at most every reencStatusTTL.
func (e *Engine) ReencryptionStatus() (remainingBytes int64, remainingFiles int) {
	e.reenc.mu.Lock()
	defer e.reenc.mu.Unlock()
	if time.Since(e.reenc.at) < reencStatusTTL {
		return e.reenc.bytes, e.reenc.files
	}
	stale, err := e.staleTables()
	if err != nil {
		e.reenc.sweepErrs++
		return e.reenc.bytes, e.reenc.files // serve the previous reading
	}
	e.reenc.at = time.Now()
	e.reenc.bytes, e.reenc.files = 0, 0
	for _, t := range stale {
		e.reenc.bytes += int64(t.size)
		e.reenc.files++
	}
	return e.reenc.bytes, e.reenc.files
}

// ReencryptPass compacts up to maxBytes (0 = unlimited) of stale-key
// sstables through Pebble's manual compaction, then re-sweeps; callers
// loop until remaining hits zero. Two Pebble behaviors would otherwise
// leave files stale forever: DB.Compact's level loop stops ABOVE the
// bottom level (a file resting in L6 is never an input), and a
// single-file compaction with an empty output level is a trivial MOVE
// that keeps the original bytes. Both are defeated by seeding: before
// compacting a stale file, the pass writes a Pebble point tombstone at
// smallest+0x00 — Compact then flushes it (an L0 input overlapping the
// file), turning the manual compaction into a real rewrite that also
// elides the seed at the bottom.
//
// The seed key is a logical no-op by construction: every real engine key
// is either escape(K)+"\x00\x01" (+12-byte version suffix) — where the
// terminator "\x00\x01" cannot occur inside escape(K) — or lives in a
// differently-prefixed local keyspace. A VALID key followed by one extra
// byte therefore decodes as no valid key at any length, so nothing can
// ever write it, the tombstone shadows nothing, and no reader (MVCC
// iteration included) can observe it. Files whose smallest key is not a
// valid MVCC key (raft state) are compacted unseeded — that keyspace
// rewrites on every apply anyway.
func (e *Engine) ReencryptPass(ctx context.Context, maxBytes int64) (targeted int64, remainingBytes int64, remainingFiles int, err error) {
	stale, err := e.staleTables()
	if err != nil {
		return 0, 0, 0, err
	}
	for _, t := range stale {
		if err := ctx.Err(); err != nil {
			return targeted, 0, 0, err
		}
		if maxBytes > 0 && targeted >= maxBytes {
			break
		}
		if _, _, derr := DecodeMVCCKey(t.smallest); derr == nil {
			seed := append(append([]byte(nil), t.smallest...), 0)
			// The seed must fall inside the file's bounds to overlap it (a
			// file holding a single user key admits none; other overlaps or
			// natural churn have to retire those).
			if bytes.Compare(seed, t.largest) <= 0 {
				if derr := e.db.Delete(seed, nil); derr != nil {
					return targeted, 0, 0, derr
				}
			}
		}
		// Compact's span end is exclusive; cover the largest key. The call
		// flushes the overlapping memtable (our seed) first.
		end := append(append([]byte(nil), t.largest...), 0)
		if cerr := e.db.Compact(t.smallest, end, false); cerr != nil {
			return targeted, 0, 0, cerr
		}
		targeted += int64(t.size)
	}
	stale, err = e.staleTables()
	if err != nil {
		return targeted, 0, 0, err
	}
	for _, t := range stale {
		remainingBytes += int64(t.size)
		remainingFiles++
	}
	e.reenc.mu.Lock()
	e.reenc.at, e.reenc.bytes, e.reenc.files = time.Now(), remainingBytes, remainingFiles
	e.reenc.mu.Unlock()
	return targeted, remainingBytes, remainingFiles, nil
}
