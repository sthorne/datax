package storage

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/storage/enc"
	"github.com/sthorne/datax/pkg/util/hlc"
)

// mvccTestKey is the shape real data has — what the re-encryption pass's
// overlap seeding keys off (see TestReencryptPass).
func mvccTestKey(prefix string, i int) []byte {
	return EncodeMVCCKey(keys.Key(fmt.Sprintf("%s-%06d", prefix, i)), hlc.Timestamp{WallTime: int64(i + 1)})
}

// TestAuthenticatedCutover walks a store through the switch to the
// authenticated format (issue #220): files from before are unauthenticated;
// EnableAuthenticatedFiles makes new ones authenticated now and after a
// reopen; the cutover refuses while old-format files remain; once the
// sweep and a reopen have retired them it completes, the registry records
// it, and a reopened store refuses the old format while reading everything.
func TestAuthenticatedCutover(t *testing.T) {
	dir := t.TempDir()
	key := testStoreKey(t)
	e, err := Open(dir, Options{EncryptionKey: key})
	if err != nil {
		t.Fatal(err)
	}
	if e.AuthenticatedFiles() || e.AuthenticatedOnly() {
		t.Fatal("a fresh store writes authenticated files before being told to")
	}
	for i := 0; i < 500; i++ {
		if err := e.Put(mvccTestKey("old", i), []byte("v")); err != nil {
			t.Fatal(err)
		}
	}
	if err := e.Flush(); err != nil {
		t.Fatal(err)
	}
	legacy, err := e.LegacyEncryptedFiles()
	if err != nil || len(legacy) == 0 {
		t.Fatalf("before the switch: %d legacy files, %v", len(legacy), err)
	}
	if _, _, err := e.CompleteAuthenticatedCutover(key); err == nil {
		t.Fatal("cutover accepted before authenticated writes were enabled")
	}

	// The switch: new files authenticated, old ones still read.
	if err := e.EnableAuthenticatedFiles(key); err != nil {
		t.Fatal(err)
	}
	if !e.AuthenticatedFiles() {
		t.Fatal("not writing authenticated files after the switch")
	}
	if err := e.Put(mvccTestKey("new", 1), []byte("w")); err != nil {
		t.Fatal(err)
	}
	if err := e.Flush(); err != nil {
		t.Fatal(err)
	}
	remaining, done, err := e.CompleteAuthenticatedCutover(key)
	if err != nil || done || remaining == 0 {
		t.Fatalf("cutover with old files present: remaining=%d done=%v err=%v", remaining, done, err)
	}
	// The sweep counts the old-format sstables as stale and rewrites them.
	for pass := 0; pass < 20; pass++ {
		_, rem, _, err := e.ReencryptPass(t.Context(), 0, map[uint64]bool{})
		if err != nil {
			t.Fatal(err)
		}
		if rem == 0 {
			break
		}
	}
	if _, files, err := e.ReencryptionStatus(); err != nil || files != 0 {
		t.Fatalf("after the sweep: %d stale files, %v", files, err)
	}
	if v, err := e.Get(mvccTestKey("old", 7)); err != nil || string(v) != "v" {
		t.Fatalf("old row after the sweep: %q %v", v, err)
	}
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}

	// Reopened, the registry says authenticated writes: Pebble's own files
	// (OPTIONS, MANIFEST, WAL) are created authenticated from the first,
	// and with the sstables rewritten nothing of the old format is left.
	e, err = Open(dir, Options{EncryptionKey: key})
	if err != nil {
		t.Fatal(err)
	}
	if !e.AuthenticatedFiles() {
		t.Fatal("reopened store forgot it writes authenticated files")
	}
	if e.AuthenticatedOnly() {
		t.Fatal("reopened store refuses the old format before the cutover completed")
	}
	legacy, _ = e.LegacyEncryptedFiles()
	if len(legacy) != 0 {
		t.Fatalf("old-format files after sweep and reopen: %v", legacy)
	}
	remaining, done, err = e.CompleteAuthenticatedCutover(key)
	if err != nil || !done || remaining != 0 {
		t.Fatalf("cutover: remaining=%d done=%v err=%v", remaining, done, err)
	}
	if !e.AuthenticatedOnly() {
		t.Fatal("cutover complete but the old format is still accepted")
	}
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}

	// Strict from the registry at the next open; everything reads.
	e, err = Open(dir, Options{EncryptionKey: key})
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	if !e.AuthenticatedOnly() || !e.AuthenticatedFiles() {
		t.Fatal("reopened store does not refuse the old format after the cutover")
	}
	if v, err := e.Get(mvccTestKey("old", 7)); err != nil || string(v) != "v" {
		t.Fatalf("old row: %q %v", v, err)
	}
	if v, err := e.Get(mvccTestKey("new", 1)); err != nil || string(v) != "w" {
		t.Fatalf("new row: %q %v", v, err)
	}
	// Every file on disk carries the authenticated header.
	entries, _ := os.ReadDir(dir)
	for _, ent := range entries {
		if ent.IsDir() || ent.Name() == enc.RegistryName || ent.Name() == "LOCK" {
			continue
		}
		info, err := ent.Info()
		if errors.Is(err, os.ErrNotExist) {
			continue // an obsolete file Pebble deleted under us
		}
		if err != nil {
			t.Fatal(err)
		}
		if info.Size() < 4 {
			continue
		}
		a, ok, err := enc.FileAuthenticated(e.encBase, filepath.Join(dir, ent.Name()))
		if errors.Is(err, os.ErrNotExist) {
			continue // deleted since ReadDir, as above
		}
		if err != nil || !ok || !a {
			t.Fatalf("%s: authenticated=%v ok=%v err=%v", ent.Name(), a, ok, err)
		}
	}
}

// TestAuthenticatedStoreRefusesAnAlteredTable: the test issue #220 asked
// for first — a flipped ciphertext byte in an sstable is refused at read,
// where the unauthenticated format served it as data (CTR is malleable and
// Pebble's checksum sits inside the same keystream). The flip lands in the
// file's data region, not its header.
func TestAuthenticatedStoreRefusesAnAlteredTable(t *testing.T) {
	dir := t.TempDir()
	key := testStoreKey(t)
	e, err := Open(dir, Options{EncryptionKey: key})
	if err != nil {
		t.Fatal(err)
	}
	if err := e.EnableAuthenticatedFiles(key); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 200; i++ {
		if err := e.Put(mvccTestKey("k", i), []byte(strings.Repeat("value", 10))); err != nil {
			t.Fatal(err)
		}
	}
	if err := e.Flush(); err != nil {
		t.Fatal(err)
	}
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}
	ssts, _ := filepath.Glob(filepath.Join(dir, "*.sst"))
	if len(ssts) == 0 {
		t.Fatal("no sstable written")
	}
	f, err := os.OpenFile(ssts[0], os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	var b [1]byte
	const off = 32 + 4 + 200 // header, chunk 0's generation, 200 bytes into its ciphertext
	if _, err := f.ReadAt(b[:], off); err != nil {
		t.Fatal(err)
	}
	b[0] ^= 0x40
	if _, err := f.WriteAt(b[:], off); err != nil {
		t.Fatal(err)
	}
	_ = f.Close()

	e, err = Open(dir, Options{EncryptionKey: key})
	if err != nil {
		// Pebble may read the table while opening (it does not here, but
		// a refusal at open is the same refusal).
		if !strings.Contains(err.Error(), "does not authenticate") {
			t.Fatalf("open: %v", err)
		}
		return
	}
	defer e.Close()
	var sawRefusal bool
	for i := 0; i < 200; i++ {
		k := mvccTestKey("k", i)
		v, err := e.Get(k)
		if err != nil {
			if !strings.Contains(err.Error(), "does not authenticate") {
				t.Fatalf("Get(%q): %v", k, err)
			}
			sawRefusal = true
			continue
		}
		if string(v) != strings.Repeat("value", 10) {
			t.Fatalf("Get(%q) served altered data: %q", k, v)
		}
	}
	if !sawRefusal {
		t.Fatal("the altered table was read without a refusal")
	}
}

// TestWALTornTailIsRecoveredAndMidWALDamageRefused is the engine-level
// shape of QA's finding on #247: acknowledged writes live only in the raft
// engine's WAL until a flush, and a crash in the middle of an append must
// cost at most the record being appended. The WAL is framed, so a torn
// final frame — cut short, or whole but not authenticating — is the end
// of the log and every acknowledged write replays; a frame damaged with
// frames after it is no crash, and the store refuses to open rather than
// serve it. The control, the store untouched, proves the WAL is what
// carries the writes.
func TestWALTornTailIsRecoveredAndMidWALDamageRefused(t *testing.T) {
	const commits = 40
	key := testStoreKey(t)
	build := func(t *testing.T) string {
		dir := t.TempDir()
		e, err := Open(dir, Options{EncryptionKey: key, Raft: true})
		if err != nil {
			t.Fatal(err)
		}
		if err := e.EnableAuthenticatedFiles(key); err != nil {
			t.Fatal(err)
		}
		if err := e.Close(); err != nil {
			t.Fatal(err)
		}
		// Reopened, Pebble's own files are authenticated from the first.
		e, err = Open(dir, Options{EncryptionKey: key, Raft: true})
		if err != nil {
			t.Fatal(err)
		}
		for i := 0; i < commits; i++ {
			b := e.NewBatch()
			if err := b.Put(mvccTestKey("ack", i), []byte("acked")); err != nil {
				t.Fatal(err)
			}
			if err := b.Commit(true); err != nil {
				t.Fatal(err)
			}
		}
		if e.FlushedSeqNum() != 0 {
			t.Fatal("a flush happened; the WAL is not the only copy and the test proves nothing")
		}
		if err := e.Close(); err != nil {
			t.Fatal(err)
		}
		return dir
	}
	wal := func(t *testing.T, dir string) string {
		entries, err := os.ReadDir(dir)
		if err != nil {
			t.Fatal(err)
		}
		var logs []string
		for _, ent := range entries {
			if strings.HasSuffix(ent.Name(), ".log") {
				logs = append(logs, filepath.Join(dir, ent.Name()))
			}
		}
		if len(logs) != 1 {
			t.Fatalf("want one WAL segment, have %v", logs)
		}
		return logs[0]
	}
	expectAll := func(t *testing.T, dir string) {
		e, err := Open(dir, Options{EncryptionKey: key, Raft: true})
		if err != nil {
			t.Fatalf("the store did not open: %v", err)
		}
		defer e.Close()
		for i := 0; i < commits; i++ {
			if v, err := e.Get(mvccTestKey("ack", i)); err != nil || string(v) != "acked" {
				t.Fatalf("acknowledged write %d after the crash: %q %v", i, v, err)
			}
		}
	}
	t.Run("control", func(t *testing.T) { expectAll(t, build(t)) })
	t.Run("last frame cut short", func(t *testing.T) {
		dir := build(t)
		raw, err := os.ReadFile(wal(t, dir))
		if err != nil {
			t.Fatal(err)
		}
		// The sync after the last commit is the whole point: the commit is
		// acknowledged, so its frame is complete, and only a frame after it
		// can be torn. Append a frame header and a few bytes of a body that
		// never arrived, then cut into the body.
		torn := append(raw, 0, 0, 1, 0, 9, 9, 9, 9, 9, 9, 9, 9, 1, 2, 3, 4, 5)
		if err := os.WriteFile(wal(t, dir), torn, 0o600); err != nil {
			t.Fatal(err)
		}
		expectAll(t, dir)
	})
	t.Run("last frame whole but altered", func(t *testing.T) {
		dir := build(t)
		raw, err := os.ReadFile(wal(t, dir))
		if err != nil {
			t.Fatal(err)
		}
		// A write whose pages reached the disk out of order: the frame is
		// there in full and does not authenticate. The frame carries the
		// last commit, which the writer was told was durable — but that is
		// what a lost final write is on any store, authenticated or not,
		// and it is one record, not the log.
		raw[len(raw)-5] ^= 1
		if err := os.WriteFile(wal(t, dir), raw, 0o600); err != nil {
			t.Fatal(err)
		}
		e, err := Open(dir, Options{EncryptionKey: key, Raft: true})
		if err != nil {
			t.Fatalf("the store did not open after a torn final frame: %v", err)
		}
		defer e.Close()
		for i := 0; i < commits-1; i++ {
			if v, err := e.Get(mvccTestKey("ack", i)); err != nil || string(v) != "acked" {
				t.Fatalf("acknowledged write %d after the crash: %q %v", i, v, err)
			}
		}
	})
	t.Run("a middle frame altered", func(t *testing.T) {
		dir := build(t)
		raw, err := os.ReadFile(wal(t, dir))
		if err != nil {
			t.Fatal(err)
		}
		// Inside a middle frame's ciphertext (a damaged length field would
		// instead end the log there, which the layer cannot tell from a
		// crash: see the format's comment). Frames are
		// [len u32][nonce 8][ciphertext][tag 16] after the 32-byte header.
		const header, frameOverhead = 32, 4 + 8 + 16
		pos, frames := int64(header), 0
		for frames < commits/2 {
			n := int64(raw[pos])<<24 | int64(raw[pos+1])<<16 | int64(raw[pos+2])<<8 | int64(raw[pos+3])
			pos += frameOverhead + n
			frames++
		}
		raw[pos+12+2] ^= 1
		if err := os.WriteFile(wal(t, dir), raw, 0o600); err != nil {
			t.Fatal(err)
		}
		e, err := Open(dir, Options{EncryptionKey: key, Raft: true})
		if err == nil {
			_ = e.Close()
			t.Fatal("a WAL altered in the middle opened")
		}
		if !strings.Contains(err.Error(), "does not authenticate") {
			t.Fatalf("refusal names something else: %v", err)
		}
	})
}
