package storage

import (
	"bytes"
	"context"
	"crypto/rand"
	"fmt"
	"strings"
	"testing"

	"github.com/cockroachdb/pebble"

	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/storage/enc"
	"github.com/sthorne/datax/pkg/util/hlc"
)

func testKey(t *testing.T) []byte {
	t.Helper()
	k := make([]byte, enc.KeyLen)
	if _, err := rand.Read(k); err != nil {
		t.Fatal(err)
	}
	return k
}

// TestRotateStoreKeyLive: the store key rotates on a LIVE engine under
// concurrent writes; the wrong old key is refused; after a clean close
// only the new key opens the store, and every row survives. Issue #48.
func TestRotateStoreKeyLive(t *testing.T) {
	dir := t.TempDir()
	key1, key2 := testKey(t), testKey(t)
	e, err := Open(dir, Options{EncryptionKey: key1})
	if err != nil {
		t.Fatal(err)
	}
	if !e.Encrypted() {
		t.Fatal("engine does not report encrypted")
	}

	// Writes continue right through the rotation.
	stop := make(chan struct{})
	writeErr := make(chan error, 1)
	go func() {
		defer close(writeErr)
		for i := 0; ; i++ {
			select {
			case <-stop:
				return
			default:
			}
			if err := e.Put([]byte(fmt.Sprintf("live-%06d", i)), []byte("v")); err != nil {
				writeErr <- err
				return
			}
		}
	}()

	if err := e.RotateStoreKeyLive(key2, key2); err == nil ||
		!strings.Contains(err.Error(), "does not match") {
		t.Fatalf("rotation with the wrong old key: %v", err)
	}
	if err := e.RotateStoreKeyLive(key1, key2); err != nil {
		t.Fatalf("live rotation: %v", err)
	}
	// A second rotation must need the NEW key — proof the reseal landed.
	if err := e.RotateStoreKeyLive(key1, testKey(t)); err == nil {
		t.Fatal("stale old key accepted after rotation")
	}
	close(stop)
	if err := <-writeErr; err != nil {
		t.Fatalf("write failed during rotation: %v", err)
	}
	if err := e.Put([]byte("post-rotate"), []byte("v")); err != nil {
		t.Fatal(err)
	}
	if err := e.Flush(); err != nil {
		t.Fatal(err)
	}
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}

	if _, err := Open(dir, Options{EncryptionKey: key1}); err == nil {
		t.Fatal("old store key still opens the store")
	}
	e2, err := Open(dir, Options{EncryptionKey: key2})
	if err != nil {
		t.Fatalf("new store key rejected: %v", err)
	}
	defer func() { _ = e2.Close() }()
	if v, err := e2.Get([]byte("post-rotate")); err != nil || string(v) != "v" {
		t.Fatalf("data lost across rotation: %q %v", v, err)
	}
}

// TestReencryptPass: after a restart mints a fresh active data key, the
// previous session's sstables read as stale; re-encryption passes rewrite
// them until none remain, data intact, and every live sstable's header
// then carries the active key. Issue #48.
func TestReencryptPass(t *testing.T) {
	dir := t.TempDir()
	key := testKey(t)
	e, err := Open(dir, Options{EncryptionKey: key})
	if err != nil {
		t.Fatal(err)
	}
	// MVCC-encoded keys — the shape real data has, and what the pass's
	// overlap seeding keys off.
	mvccKey := func(i int) []byte {
		return EncodeMVCCKey(keys.Key(fmt.Sprintf("k-%06d", i)), hlc.Timestamp{WallTime: int64(i + 1)})
	}
	val := make([]byte, 100)
	for i := 0; i < 1000; i++ {
		if err := e.Put(mvccKey(i), val); err != nil {
			t.Fatal(err)
		}
	}
	if err := e.Flush(); err != nil {
		t.Fatal(err)
	}
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}

	// Reopen: a fresh data key becomes active; the old ssts are now stale.
	e2, err := Open(dir, Options{EncryptionKey: key})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = e2.Close() }()
	remaining, files, _ := e2.ReencryptionStatus()
	if remaining == 0 || files == 0 {
		t.Fatalf("no stale files after restart: %d bytes / %d files", remaining, files)
	}

	ctx := context.Background()
	var rewritten int64
	for pass := 0; ; pass++ {
		if pass > 20 {
			t.Fatalf("re-encryption did not converge; %d bytes remain", remaining)
		}
		targeted, rem, _, err := e2.ReencryptPass(ctx, 0, nil)
		if err != nil {
			t.Fatal(err)
		}
		rewritten += targeted
		remaining = rem
		if remaining == 0 {
			break
		}
	}
	if rewritten == 0 {
		t.Fatal("no bytes were rewritten")
	}
	if rb, rf, serr := e2.ReencryptionStatus(); rb != 0 || rf != 0 || serr != nil {
		t.Fatalf("status after convergence: %d bytes / %d files", rb, rf)
	}

	// Data intact, and every remaining encrypted file (registry aside)
	// carries the active data key.
	for _, i := range []int{0, 500, 999} {
		if v, err := e2.Get(mvccKey(i)); err != nil || len(v) != len(val) {
			t.Fatalf("row %d after re-encryption: %d bytes, %v", i, len(v), err)
		}
	}
	activeID, _ := e2.encKeys.Active()
	stale, err := e2.staleTables()
	if err != nil {
		t.Fatal(err)
	}
	if len(stale) != 0 {
		t.Fatalf("%d live sstables still stale (active key %d)", len(stale), activeID)
	}
}

// TestReencryptPassMultiLevel: the shapes a real store leaves behind after
// a restart — a file resting in the bottom level (never a manual-
// compaction input without the seed), a single-user-key file (admits no
// seed), a local-key-only file (compacted unseeded), and the pre-shutdown
// flush files — driven with a small budget and a run-long attempted map.
// The remaining stale set shrinks monotonically and stops at exactly the
// files a compaction cannot rewrite; the seed tombstone is never visible
// to a reader; data survives intact. Issue #68.
func TestReencryptPassMultiLevel(t *testing.T) {
	// Only the test's own compactions move files, so each flush below is
	// exactly one file of a known shape when the passes start.
	testingPebbleOptions = func(o *pebble.Options) { o.DisableAutomaticCompactions = true }
	defer func() { testingPebbleOptions = nil }()
	dir := t.TempDir()
	key := testKey(t)
	e, err := Open(dir, Options{EncryptionKey: key})
	if err != nil {
		t.Fatal(err)
	}
	mvccKey := func(prefix string, i int) []byte {
		return EncodeMVCCKey(keys.Key(fmt.Sprintf("%s-%06d", prefix, i)), hlc.Timestamp{WallTime: int64(i + 1)})
	}
	val := make([]byte, 100)
	put := func(k []byte) {
		t.Helper()
		if err := e.Put(k, val); err != nil {
			t.Fatal(err)
		}
	}
	flush := func() {
		t.Helper()
		if err := e.Flush(); err != nil {
			t.Fatal(err)
		}
	}
	// Push everything overlapping [lo, hi] to the bottom level: each
	// Compact call moves the span one level down.
	sink := func(lo, hi []byte) {
		t.Helper()
		end := append(append([]byte(nil), hi...), 0)
		for i := 0; i < 6; i++ {
			if err := e.db.Compact(lo, end, false); err != nil {
				t.Fatal(err)
			}
		}
	}
	// A region resting in L6.
	for i := 0; i < 500; i++ {
		put(mvccKey("a", i))
	}
	flush()
	sink(mvccKey("a", 0), mvccKey("a", 499))
	// A single-user-key file (no interior seed possible), above every
	// other region so nothing else ever overlaps it.
	put(mvccKey("~", 0))
	flush()
	// A local-key-only file: raft/replica state keys are not MVCC-encoded.
	for i := 0; i < 50; i++ {
		put([]byte(fmt.Sprintf("\x01local-%03d", i)))
	}
	flush()
	// Pre-shutdown flush files spanning both regions.
	for i := 0; i < 300; i++ {
		put(mvccKey("a", 1000+i))
		put(mvccKey("z", i))
	}
	flush()
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}

	e, err = Open(dir, Options{EncryptionKey: key})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = e.Close() }()
	levelOf := func(fileNum uint64) int {
		t.Helper()
		levels, err := e.db.SSTables()
		if err != nil {
			t.Fatal(err)
		}
		for l, files := range levels {
			for _, f := range files {
				if uint64(f.FileNum) == fileNum {
					return l
				}
			}
		}
		return -1
	}
	stale, err := e.staleTables()
	if err != nil {
		t.Fatal(err)
	}
	var bottom, single, local []uint64
	for _, f := range stale {
		switch {
		case levelOf(f.fileNum) == 6:
			bottom = append(bottom, f.fileNum)
		case bytes.Equal(f.smallest, f.largest):
			single = append(single, f.fileNum)
		case f.smallest[0] == 1:
			local = append(local, f.fileNum)
		}
	}
	if len(bottom) == 0 || len(single) != 1 || len(local) != 1 {
		t.Fatalf("stale set lacks the shapes under test: bottom=%v single=%v local=%v (all: %+v)", bottom, single, local, stale)
	}

	// Drive passes with a tiny budget (one compaction each) and the
	// worker's attempted map; the remaining set must never grow.
	ctx := context.Background()
	attempted := map[uint64]bool{}
	prev := int64(-1)
	var rewritten int64
	for pass := 0; ; pass++ {
		if pass > 40 {
			t.Fatalf("re-encryption did not settle; %d bytes remain", prev)
		}
		targeted, rem, files, err := e.ReencryptPass(ctx, 1, attempted)
		if err != nil {
			t.Fatal(err)
		}
		rewritten += targeted
		if prev >= 0 && rem > prev {
			t.Fatalf("pass %d: remaining grew from %d to %d bytes", pass, prev, rem)
		}
		prev = rem
		if targeted == 0 {
			// Nothing this pass could rewrite: only the unrewritable files
			// may remain, and every one of them was attempted exactly once.
			left, err := e.staleTables()
			if err != nil {
				t.Fatal(err)
			}
			if len(left) != files {
				t.Fatalf("status says %d files remain, sweep finds %d", files, len(left))
			}
			for _, f := range left {
				if f.fileNum != single[0] && f.fileNum != local[0] {
					t.Fatalf("rewritable file %d (%q..%q, level %d) left stale", f.fileNum, f.smallest, f.largest, levelOf(f.fileNum))
				}
				if !attempted[f.fileNum] {
					t.Fatalf("file %d left stale without being attempted", f.fileNum)
				}
			}
			break
		}
	}
	if rewritten == 0 {
		t.Fatal("no bytes were rewritten")
	}
	for _, fn := range bottom {
		if levelOf(fn) != -1 {
			t.Fatalf("bottom-level stale file %d still live after the passes", fn)
		}
	}

	// Data intact, and the seed tombstones (smallest+0x00 of every seeded
	// file) never surface to a reader positioned at the file's smallest.
	for _, f := range stale {
		if _, _, derr := DecodeMVCCKey(f.smallest); derr != nil {
			continue
		}
		seed := append(append([]byte(nil), f.smallest...), 0)
		it := e.NewIter(f.smallest, append(append([]byte(nil), f.largest...), 0, 0))
		for ok := it.SeekGE(f.smallest); ok; ok = it.Next() {
			if bytes.Equal(it.Key(), seed) {
				t.Fatalf("seed tombstone %q visible to an iterator", seed)
			}
			if _, _, derr := DecodeMVCCKey(it.Key()); derr != nil {
				t.Fatalf("non-MVCC key %q visible in an MVCC span", it.Key())
			}
		}
		_ = it.Close()
	}
	for _, i := range []int{0, 250, 499, 1000, 1299} {
		if v, err := e.Get(mvccKey("a", i)); err != nil || len(v) != len(val) {
			t.Fatalf("row a-%d after re-encryption: %d bytes, %v", i, len(v), err)
		}
	}
	if v, err := e.Get(mvccKey("~", 0)); err != nil || len(v) != len(val) {
		t.Fatalf("single-key row after re-encryption: %d bytes, %v", len(v), err)
	}
}
