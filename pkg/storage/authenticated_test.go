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
