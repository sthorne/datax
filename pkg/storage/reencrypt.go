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

// Authenticated encryption (issue #220). The switch from the CTR format
// to the chunked AES-GCM one happens in two steps a store records in its
// registry. First, new files are written authenticated
// (EnableAuthenticatedFiles): the node asks for it once the cluster
// version allows, and from then every open creates its files that way.
// Second, once no file of the old format remains (the sweep rewrites the
// sstables; the WAL, MANIFEST and OPTIONS are recreated by the next
// open), the store refuses the old format for good
// (CompleteAuthenticatedCutover) — until then a reader that accepted both
// would accept a file rewritten as the old format, which is the downgrade
// the whole change exists to close.

// AuthenticatedFiles reports whether new files are written in the
// authenticated format.
func (e *Engine) AuthenticatedFiles() bool {
	return e.encFS != nil && e.encFS.AuthenticatedWrites()
}

// AuthenticatedOnly reports whether the store refuses the unauthenticated
// format (the cutover is complete).
func (e *Engine) AuthenticatedOnly() bool {
	return e.encFS != nil && e.encFS.RefusesLegacy()
}

// EnableAuthenticatedFiles makes the store write new files in the
// authenticated format, now and at every later open (recorded in the
// registry under storeKey). A no-op on a plaintext store or one already
// switched.
func (e *Engine) EnableAuthenticatedFiles(storeKey []byte) error {
	if e.encFS == nil || e.encFS.AuthenticatedWrites() {
		return nil
	}
	e.encMu.Lock()
	defer e.encMu.Unlock()
	if err := enc.MarkAuthenticatedWrites(e.encBase, e.encDir, storeKey); err != nil {
		return err
	}
	e.encFS.EnableAuthenticatedWrites()
	return nil
}

// LegacyEncryptedFiles lists the store's files still in the
// unauthenticated format.
func (e *Engine) LegacyEncryptedFiles() ([]string, error) {
	if e.encFS == nil {
		return nil, nil
	}
	return enc.LegacyFiles(e.encBase, e.encDir)
}

// CompleteAuthenticatedCutover makes the store refuse the unauthenticated
// format if no file of it remains, recording that in the registry under
// storeKey; otherwise it reports how many remain. done is also true when
// the cutover was already complete.
func (e *Engine) CompleteAuthenticatedCutover(storeKey []byte) (remaining int, done bool, err error) {
	if e.encFS == nil {
		return 0, false, nil
	}
	if e.encFS.RefusesLegacy() {
		return 0, true, nil
	}
	if !e.encFS.AuthenticatedWrites() {
		return 0, false, fmt.Errorf("the store still writes unauthenticated files; enable authenticated files first")
	}
	legacy, err := e.LegacyEncryptedFiles()
	if err != nil {
		return 0, false, err
	}
	if len(legacy) > 0 {
		return len(legacy), false, nil
	}
	e.encMu.Lock()
	defer e.encMu.Unlock()
	if err := enc.MarkAuthenticated(e.encBase, e.encDir, storeKey); err != nil {
		return 0, false, err
	}
	e.encFS.RefuseLegacy()
	return 0, true, nil
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

// record stores a fresh reading.
func (c *reencStatusCache) record(bytes int64, files int) {
	c.mu.Lock()
	c.at, c.bytes, c.files, c.lastErr = time.Now(), bytes, files, nil
	c.mu.Unlock()
}

// status serves the cached reading while it is fresh, else re-sweeps
// with sweep; a failed sweep returns the previous reading with the error.
func (c *reencStatusCache) status(sweep func() ([]staleTable, error)) (remainingBytes int64, remainingFiles int, err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.at.IsZero() && time.Since(c.at) < reencStatusTTL {
		return c.bytes, c.files, c.lastErr
	}
	stale, err := sweep()
	if err != nil {
		c.sweepErrs++
		c.lastErr = err
		return c.bytes, c.files, err // serve the previous reading
	}
	c.at = time.Now()
	c.lastErr = nil
	c.bytes, c.files = sumStale(stale)
	return c.bytes, c.files, nil
}

// staleTable is one live sstable still encrypted under a retired key.
type staleTable struct {
	fileNum  uint64
	level    int
	size     uint64
	smallest []byte
	largest  []byte
}

// staleTables sweeps the live sstables and returns those whose data key
// is not the active one — and, once the store writes authenticated files,
// those still in the unauthenticated format, which the same rewrite
// retires. Files deleted mid-sweep are skipped.
func (e *Engine) staleTables() ([]staleTable, error) {
	if !e.Encrypted() {
		return nil, nil
	}
	wantAuthenticated := e.AuthenticatedFiles()
	activeID, _ := e.encKeys.Active()
	levels, err := e.db.SSTables()
	if err != nil {
		return nil, err
	}
	var out []staleTable
	for l, level := range levels {
		for _, t := range level {
			name := fmt.Sprintf("%06d.sst", t.FileNum)
			path := e.encBase.PathJoin(e.encDir, name)
			id, ok, err := enc.FileKeyID(e.encBase, path)
			if err != nil || !ok {
				continue // deleted under us, or headerless
			}
			legacy := false
			if wantAuthenticated {
				if authenticated, ok, err := enc.FileAuthenticated(e.encBase, path); err == nil && ok {
					legacy = !authenticated
				}
			}
			if id != activeID || legacy {
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
	return e.reenc.status(e.staleTables)
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
	if targeted, err = e.compactStale(ctx, stale, maxBytes, attempted); err != nil {
		return targeted, 0, 0, err
	}
	stale, err = e.staleTables()
	if err != nil {
		return targeted, 0, 0, err
	}
	remainingBytes, remainingFiles = sumStale(stale)
	e.reenc.record(remainingBytes, remainingFiles)
	return targeted, remainingBytes, remainingFiles, nil
}

// compactStale rewrites up to maxBytes (0 = unlimited) of the given
// live sstables through manual compactions, as ReencryptPass describes;
// files in attempted are skipped and every file targeted is added to it.
func (e *Engine) compactStale(ctx context.Context, stale []staleTable, maxBytes int64, attempted map[uint64]bool) (targeted int64, err error) {
	for _, t := range stale {
		if err := ctx.Err(); err != nil {
			return targeted, err
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
					return targeted, derr
				}
				// Pebble v2's Compact no longer flushes an overlapping
				// memtable first (v1 did): without this flush the seed
				// stays in memory and a lone file is moved, not rewritten.
				if ferr := e.db.Flush(); ferr != nil {
					return targeted, ferr
				}
			}
		}
		// The span: the file's own bounds below L0, the narrowest overlapping
		// one — smallest through the seed — for an L0 file (Compact's end is
		// inclusive in this Pebble).
		var end []byte
		if t.level == 0 {
			end = append(append([]byte(nil), t.smallest...), 0)
		} else {
			end = append(append([]byte(nil), t.largest...), 0)
		}
		before := e.compactedBytes()
		if cerr := e.db.Compact(context.Background(), t.smallest, end, false); cerr != nil {
			return targeted, cerr
		}
		targeted += e.compactedBytes() - before
	}
	return targeted, nil
}

// sumStale totals a sweep's bytes and files.
func sumStale(stale []staleTable) (bytes int64, files int) {
	for _, t := range stale {
		bytes += int64(t.size)
		files++
	}
	return bytes, files
}

// compactedBytes is the running total of bytes Pebble has written through
// compactions (all levels; trivial moves excluded) — the honest measure of
// what one manual compaction cost.
func (e *Engine) compactedBytes() int64 {
	m := e.db.Metrics()
	var total int64
	for i := range m.Levels {
		total += int64(m.Levels[i].TableBytesCompacted)
	}
	return total
}
