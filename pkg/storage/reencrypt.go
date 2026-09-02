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

// MatchStoreKey reports which of the candidate store keys seals this
// engine's key registry (see enc.MatchStoreKey); 0 for a plaintext engine.
func (e *Engine) MatchStoreKey(candidates [][]byte) (int, error) {
	if !e.Encrypted() {
		return 0, nil
	}
	return enc.MatchStoreKey(e.encBase, e.encDir, candidates)
}

// reencStatusCache caches the stale-table sweep (~one small pread per
// live sstable) behind a TTL, for the /metrics gauge.
type reencStatusCache struct {
	mu        sync.Mutex
	at        time.Time
	bytes     int64
	files     int
	sweepErrs int64
	// lastErr is the most recent sweep failure, cleared by a successful
	// sweep; surfaced so a failed sweep never reads as "nothing stale".
	lastErr error
}

const reencStatusTTL = 10 * time.Second

// staleTable is one live sstable still encrypted under a retired key.
type staleTable struct {
	fileNum  uint64
	level    int
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
	for l, level := range levels {
		for _, t := range level {
			name := fmt.Sprintf("%06d.sst", t.FileNum)
			id, ok, err := enc.FileKeyID(e.encBase, e.encBase.PathJoin(e.encDir, name))
			if err != nil || !ok {
				continue // deleted under us, or headerless
			}
			if id != activeID {
				out = append(out, staleTable{
					fileNum:  uint64(t.FileNum),
					level:    l,
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
// under retired data keys, refreshed at most every reencStatusTTL. A
// failed sweep returns the previous reading together with the error (and
// no reading at all — zero bytes would falsely attest completion — when
// no sweep has ever succeeded); callers must not treat a status carrying
// an error as an attestation.
func (e *Engine) ReencryptionStatus() (remainingBytes int64, remainingFiles int, err error) {
	e.reenc.mu.Lock()
	defer e.reenc.mu.Unlock()
	if !e.reenc.at.IsZero() && time.Since(e.reenc.at) < reencStatusTTL {
		return e.reenc.bytes, e.reenc.files, e.reenc.lastErr
	}
	stale, err := e.staleTables()
	if err != nil {
		e.reenc.sweepErrs++
		e.reenc.lastErr = err
		return e.reenc.bytes, e.reenc.files, err // serve the previous reading
	}
	e.reenc.at = time.Now()
	e.reenc.lastErr = nil
	e.reenc.bytes, e.reenc.files = 0, 0
	for _, t := range stale {
		e.reenc.bytes += int64(t.size)
		e.reenc.files++
	}
	return e.reenc.bytes, e.reenc.files, nil
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
//
// Manual compaction is by key range, not by file, and Pebble v1.1.5 has
// no file-targeted form: Compact(start, end) walks every level, each step
// taking every file overlapping [start, end] as an input. What a stale
// file costs is therefore set by the span passed, and a file is an input
// whole whenever any key of it is in the span. The span depends on where
// the file rests (issue #69):
//
//   - An L0 file's key span is unbounded — a flush covers whatever the
//     memtable held, and a small pre-shutdown flush of scattered writes
//     spans most of the keyspace — so it is compacted by the NARROWEST
//     span that overlaps it, [smallest, smallest+0x00] (the seed
//     included). Its own bounds would make every level's overlap an
//     input: measured on a 63 MiB store, one 21 KiB flush file spanning a
//     third of the keyspace rewrote 30 MiB that way. The narrow span walks
//     a column instead — the file with its Lbase overlap (inherent: that
//     overlap is whatever the file spans), then one file per level below,
//     since Pebble splits compaction outputs at the next level's target
//     size and grandparent overlap.
//   - A file below L0 is bounded by construction (target file size,
//     grandparent limit), so its own bounds are a narrow column already;
//     they are used as is, which also retires the stale neighbours the
//     column crosses in the same compaction. Measured on the same store,
//     narrowing these too cost 15% more bytes and half again as many
//     compactions for the same largest compaction.
//
// The budget and the rewritten total count what Pebble actually wrote
// during each compaction (its per-level compacted-bytes counters, so a
// background compaction running at the same time is credited too), not
// an estimate over the span. Files a compaction leaves in place (a
// single-user-key file admits no seed, a local-key-only file resting in
// L6 is never an input) are recorded in attempted and skipped by later
// passes of the same run, so they neither starve the files behind them
// nor inflate the rewritten count; they retire with natural churn.
func (e *Engine) ReencryptPass(ctx context.Context, maxBytes int64, attempted map[uint64]bool) (targeted int64, remainingBytes int64, remainingFiles int, err error) {
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
		if attempted[t.fileNum] {
			continue
		}
		if attempted != nil {
			attempted[t.fileNum] = true
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
		// The span: the file's own bounds below L0, the narrowest overlapping
		// one — smallest through the seed — for an L0 file (Compact's end is
		// inclusive in this Pebble). The call flushes the overlapping
		// memtable (our seed) first.
		var end []byte
		if t.level == 0 {
			end = append(append([]byte(nil), t.smallest...), 0)
		} else {
			end = append(append([]byte(nil), t.largest...), 0)
		}
		before := e.compactedBytes()
		if cerr := e.db.Compact(t.smallest, end, false); cerr != nil {
			return targeted, 0, 0, cerr
		}
		targeted += e.compactedBytes() - before
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
	e.reenc.at, e.reenc.bytes, e.reenc.files, e.reenc.lastErr = time.Now(), remainingBytes, remainingFiles, nil
	e.reenc.mu.Unlock()
	return targeted, remainingBytes, remainingFiles, nil
}

// compactedBytes is the running total of bytes Pebble has written through
// compactions (all levels; trivial moves excluded) — the honest measure of
// what one manual compaction cost.
func (e *Engine) compactedBytes() int64 {
	m := e.db.Metrics()
	var total int64
	for i := range m.Levels {
		total += int64(m.Levels[i].BytesCompacted)
	}
	return total
}
