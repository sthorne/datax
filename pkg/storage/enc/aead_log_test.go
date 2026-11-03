package enc

import (
	"bytes"
	"errors"
	"io"
	mrand "math/rand/v2"
	"os"
	"strings"
	"sync"
	"testing"

	"github.com/cockroachdb/pebble/v2/vfs"
)

// recordingFS wraps a base FS so that every file it creates refuses, in
// the test's name, a write that lands below the file's synced high-water
// mark: the property a log needs for a crash to cost at most the record
// in flight, and the one QA's review of #247 measured the chunked layout
// breaking (1194 durable bytes rewritten by 20 synced appends).
type recordingFS struct {
	vfs.FS
	t *testing.T
}

type recordingFile struct {
	vfs.File
	t      *testing.T
	name   string
	mu     sync.Mutex
	end    int64 // highest physical offset written
	synced int64 // physical prefix made durable so far
}

func (r *recordingFS) Create(name string, cat vfs.DiskWriteCategory) (vfs.File, error) {
	f, err := r.FS.Create(name, cat)
	if err != nil {
		return nil, err
	}
	return &recordingFile{File: f, t: r.t, name: name}, nil
}

func (f *recordingFile) wrote(off int64, n int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if off < f.synced {
		f.t.Errorf("%s: write of %d bytes at physical %d, below the synced prefix %d: a durable byte rewritten", f.name, n, off, f.synced)
	}
	if end := off + int64(n); end > f.end {
		f.end = end
	}
}

func (f *recordingFile) WriteAt(p []byte, off int64) (int, error) {
	f.wrote(off, len(p))
	return f.File.WriteAt(p, off)
}

func (f *recordingFile) Write(p []byte) (int, error) {
	f.mu.Lock()
	end := f.end
	f.mu.Unlock()
	f.wrote(end, len(p))
	return f.File.Write(p)
}

func (f *recordingFile) Sync() error {
	f.mu.Lock()
	f.synced = f.end
	f.mu.Unlock()
	return f.File.Sync()
}

func (f *recordingFile) SyncData() error {
	f.mu.Lock()
	f.synced = f.end
	f.mu.Unlock()
	return f.File.SyncData()
}

func (f *recordingFile) SyncTo(off int64) (bool, error) {
	f.mu.Lock()
	if off > f.end {
		off = f.end
	}
	if off > f.synced {
		f.synced = off
	}
	f.mu.Unlock()
	return f.File.SyncTo(off)
}

// TestLogFramesNeverRewriteSyncedBytes: a WAL segment and a MANIFEST,
// appended to and synced record by record the way Pebble does it, never
// have a byte written below their synced prefix — so a crash during an
// append can damage only the record being appended, which no caller was
// told was durable.
func TestLogFramesNeverRewriteSyncedBytes(t *testing.T) {
	for _, name := range []string{"000005.log", "MANIFEST-000003"} {
		t.Run(name, func(t *testing.T) {
			base := &recordingFS{FS: vfs.NewMem(), t: t}
			fs := NewFS(base, testKeySet(t))
			fs.EnableAuthenticatedWrites()
			f, err := fs.Create(name, vfs.WriteCategoryUnspecified)
			if err != nil {
				t.Fatal(err)
			}
			rng := mrand.New(mrand.NewPCG(9, 9))
			var ref []byte
			logical := int64(0)
			for i := 0; i < 300; i++ {
				rec := make([]byte, 1+rng.IntN(300))
				for j := range rec {
					rec[j] = byte(rng.Uint32())
				}
				ref = append(ref, rec...)
				if _, err := f.Write(rec); err != nil {
					t.Fatal(err)
				}
				logical += int64(len(rec))
				switch i % 3 {
				case 0:
					err = f.SyncData()
				case 1:
					err = f.Sync()
				default:
					_, err = f.SyncTo(logical)
				}
				if err != nil {
					t.Fatal(err)
				}
			}
			if err := f.Close(); err != nil {
				t.Fatal(err)
			}
			rf, err := fs.Open(name)
			if err != nil {
				t.Fatal(err)
			}
			defer rf.Close()
			got, err := io.ReadAll(rf)
			if err != nil || !bytes.Equal(got, ref) {
				t.Fatalf("log does not read back: %v", err)
			}
		})
	}
}

// writeLogFrames writes n frames of frameLen distinct bytes each to a new
// log, syncing after each, and returns the plaintext.
func writeLogFrames(t *testing.T, fs *FS, name string, n, frameLen int) []byte {
	t.Helper()
	f, err := fs.Create(name, vfs.WriteCategoryUnspecified)
	if err != nil {
		t.Fatal(err)
	}
	var ref []byte
	for i := 0; i < n; i++ {
		rec := bytes.Repeat([]byte{byte('a' + i)}, frameLen)
		ref = append(ref, rec...)
		if _, err := f.Write(rec); err != nil {
			t.Fatal(err)
		}
		if err := f.SyncData(); err != nil {
			t.Fatal(err)
		}
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	return ref
}

// rewritePhysical replaces a file's bytes through the base FS (Pebble's
// vfs has no truncate).
func rewritePhysical(t *testing.T, base vfs.FS, name string, content []byte) {
	t.Helper()
	_ = base.Remove(name)
	w, err := base.Create(name, vfs.WriteCategoryUnspecified)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.Write(content); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
}

func physicalBytes(t *testing.T, base vfs.FS, name string) []byte {
	t.Helper()
	f, err := base.Open(name)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	st, _ := f.Stat()
	all := make([]byte, st.Size())
	if _, err := f.ReadAt(all, 0); err != nil && err != io.EOF {
		t.Fatal(err)
	}
	return all
}

// TestLogTornTailIsACrashNotACorruption: what a crash can do to a log —
// cut its last frame short, land its pages out of order so the frame is
// whole but does not authenticate, or leave the header of a frame whose
// body never arrived — ends the log at the last good frame, the way
// Pebble's own checksum treats a torn record; the log opens, Stat says
// what is there, and a writer reopening it appends over the damage. A
// frame that fails with frames after it is no crash, and is refused.
func TestLogTornTailIsACrashNotACorruption(t *testing.T) {
	const frames, frameLen = 5, 1000
	framePhys := int64(frameOverhead + frameLen)
	cases := []struct {
		name    string
		tamper  func(all []byte) []byte
		want    int    // frames that read back
		refused string // "" when the log opens and reads to its end
	}{
		{"last frame cut short", func(all []byte) []byte { return all[:len(all)-7] }, 4, ""},
		{"last frame whole but altered", func(all []byte) []byte {
			all[headerLenV2+4*framePhys+frameLenLen+chunkNonceLen+100] ^= 1
			return all
		}, 4, ""},
		{"a frame header with no body", func(all []byte) []byte {
			return append(all, 0, 0, 3, 0xe8, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
		}, 5, ""},
		{"cut at a frame boundary", func(all []byte) []byte { return all[:headerLenV2+4*framePhys] }, 4, ""},
		{"a middle frame altered", func(all []byte) []byte {
			all[headerLenV2+2*framePhys+frameLenLen+chunkNonceLen+100] ^= 1
			return all
		}, 2, "frame 2 of the encrypted log does not authenticate"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			fs, base := authenticatedFS(t)
			ref := writeLogFrames(t, fs, "000030.log", frames, frameLen)
			rewritePhysical(t, base, "000030.log", c.tamper(physicalBytes(t, base, "000030.log")))
			if c.refused == "" {
				if st, err := fs.Stat("000030.log"); err != nil || st.Size() != int64(c.want*frameLen) {
					t.Fatalf("fs.Stat: %v size=%d want %d", err, st.Size(), c.want*frameLen)
				}
			}
			f, err := fs.Open("000030.log")
			if err != nil {
				t.Fatalf("a damaged log did not open: %v", err)
			}
			got, err := io.ReadAll(f)
			_ = f.Close()
			if c.refused != "" {
				if err == nil || !strings.Contains(err.Error(), c.refused) {
					t.Fatalf("read: %v, want the refusal %q", err, c.refused)
				}
				if !bytes.Equal(got, ref[:c.want*frameLen]) {
					t.Fatal("the frames before the damaged one did not read back")
				}
				return
			}
			if err != nil {
				t.Fatalf("read: %v", err)
			}
			if !bytes.Equal(got, ref[:c.want*frameLen]) {
				t.Fatalf("read %d bytes, want the first %d frames", len(got), c.want)
			}
			// Reopened for writing, the log continues after its last good
			// frame, over whatever the crash left, and reads whole.
			w, err := fs.OpenReadWrite("000030.log", vfs.WriteCategoryUnspecified)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := w.Write([]byte("after the crash")); err != nil {
				t.Fatal(err)
			}
			_ = w.Close()
			f, err = fs.Open("000030.log")
			if err != nil {
				t.Fatal(err)
			}
			got, err = io.ReadAll(f)
			_ = f.Close()
			if err != nil || !bytes.Equal(got, append(ref[:c.want*frameLen:c.want*frameLen], "after the crash"...)) {
				t.Fatalf("after appending to the recovered log: %v (%d bytes)", err, len(got))
			}
		})
	}
}

// TestAEADHeaderIsAuthenticated: the header is in every chunk's and
// frame's additional data (InfoSec on #247). A header field whose edit
// changes neither the key nor the framing — an unused flag bit, a framed
// file's unused chunk-size field — is the edit only the AAD can catch,
// and it is refused. The transplant the header's salt exists to prevent,
// one file's chunk authenticating in another, now needs a byte-identical
// header, which makes the other file a copy of the first: whole-file
// substitution, the class the format documents as not caught.
func TestAEADHeaderIsAuthenticated(t *testing.T) {
	t.Run("an unused flag bit on a chunked file", func(t *testing.T) {
		fs, base := authenticatedFS(t)
		writeAuthenticated(t, fs, "OPTIONS-000001", bytes.Repeat([]byte{'A'}, 100))
		all := physicalBytes(t, base, "OPTIONS-000001")
		all[11] ^= 0x80
		rewritePhysical(t, base, "OPTIONS-000001", all)
		if _, err := fs.Open("OPTIONS-000001"); err == nil || !strings.Contains(err.Error(), "does not authenticate") {
			t.Fatalf("edited header: %v, want refusal", err)
		}
	})
	t.Run("the chunk-size field of a log", func(t *testing.T) {
		fs, base := authenticatedFS(t)
		writeLogFrames(t, fs, "000031.log", 3, 50)
		all := physicalBytes(t, base, "000031.log")
		all[15] ^= 1
		rewritePhysical(t, base, "000031.log", all)
		f, err := fs.Open("000031.log")
		if err != nil {
			t.Fatal(err)
		}
		defer f.Close()
		if _, err := io.ReadAll(f); err == nil || !strings.Contains(err.Error(), "does not authenticate") {
			t.Fatalf("edited header: %v, want refusal", err)
		}
	})
	t.Run("a chunk transplanted with its header is a copy", func(t *testing.T) {
		fs, base := authenticatedFS(t)
		writeAuthenticated(t, fs, "OPTIONS-000001", bytes.Repeat([]byte{'A'}, 100))
		writeAuthenticated(t, fs, "OPTIONS-000002", bytes.Repeat([]byte{'B'}, 100))
		a := physicalBytes(t, base, "OPTIONS-000001")
		b := physicalBytes(t, base, "OPTIONS-000002")
		// A's key id and salt into B's header, A's chunk 0 into B. B's
		// header is now A's header, byte for byte, and B is A.
		copy(b[4:8], a[4:8])
		copy(b[16:32], a[16:32])
		copy(b[headerLenV2:], a[headerLenV2:])
		if !bytes.Equal(a, b) {
			t.Fatal("the transplant did not make B a copy of A; the test's premise is off")
		}
		rewritePhysical(t, base, "OPTIONS-000002", b)
		got, err := readAllThrough(fs, "OPTIONS-000002", 100)
		if err != nil || !bytes.Equal(got, bytes.Repeat([]byte{'A'}, 100)) {
			t.Fatalf("a copy of A under B's name: %v", err)
		}
	})
}

// TestAEADRewriteDrawsAFreshNonce: a chunk written twice — the tail chunk
// an sstable's writer extends — is sealed under a different nonce each
// time, so no two plaintexts ever share one under the file key, with
// nothing to remember across a crash.
func TestAEADRewriteDrawsAFreshNonce(t *testing.T) {
	fs, base := authenticatedFS(t)
	f, err := fs.Create("000040.sst", vfs.WriteCategoryUnspecified)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write([]byte("first ten.")); err != nil {
		t.Fatal(err)
	}
	first := append([]byte(nil), physicalBytes(t, base, "000040.sst")[headerLenV2:headerLenV2+chunkNonceLen]...)
	if _, err := f.Write([]byte("second ten")); err != nil {
		t.Fatal(err)
	}
	_ = f.Close()
	second := physicalBytes(t, base, "000040.sst")[headerLenV2 : headerLenV2+chunkNonceLen]
	if bytes.Equal(first, second) {
		t.Fatal("chunk 0 was rewritten under the nonce it was first sealed with")
	}
	if got, err := readAllThrough(fs, "000040.sst", 20); err != nil || string(got) != "first ten.second ten" {
		t.Fatalf("read back: %q %v", got, err)
	}
}

// failingFS makes one file unexaminable: Open fails with fail.
type failingFS struct {
	vfs.FS
	name string
	fail error
}

func (f *failingFS) Open(name string, opts ...vfs.OpenOption) (vfs.File, error) {
	if name == f.name {
		return nil, f.fail
	}
	return f.FS.Open(name, opts...)
}

// TestLegacyFilesFailClosed: the list that gates the one-way cutover
// treats a file it cannot examine as an error, not as absent (QA on
// #247); a file gone since the directory was listed is no file.
func TestLegacyFilesFailClosed(t *testing.T) {
	mem := vfs.NewMem()
	fs := NewFS(mem, testKeySet(t))
	for _, name := range []string{"000001.sst", "000002.sst"} {
		f, err := fs.Create(name, vfs.WriteCategoryUnspecified)
		if err != nil {
			t.Fatal(err)
		}
		_, _ = f.Write([]byte("legacy"))
		_ = f.Close()
	}
	if names, err := LegacyFiles(mem, ""); err != nil || len(names) != 2 {
		t.Fatalf("plain listing: %v %v", names, err)
	}
	broken := &failingFS{FS: mem, name: "000002.sst", fail: errors.New("input/output error")}
	if names, err := LegacyFiles(broken, ""); err == nil || !strings.Contains(err.Error(), "000002.sst") {
		t.Fatalf("an unexaminable file was passed over: %v %v", names, err)
	}
	gone := &failingFS{FS: mem, name: "000002.sst", fail: os.ErrNotExist}
	if names, err := LegacyFiles(gone, ""); err != nil || len(names) != 1 || names[0] != "000001.sst" {
		t.Fatalf("a file deleted under the listing: %v %v", names, err)
	}
}

// TestConvertSweepsLeftoverTemporaries: a conversion that crashed between
// writing its temporary and renaming it leaves a *.dxe2tmp behind; the
// next conversion pass removes it before doing anything else.
func TestConvertSweepsLeftoverTemporaries(t *testing.T) {
	mem := vfs.NewMem()
	keys := testKeySet(t)
	w, _ := mem.Create("OPTIONS-000001"+convertTmpSuffix, vfs.WriteCategoryUnspecified)
	_, _ = w.Write([]byte("half-written"))
	_ = w.Close()
	if _, err := ConvertLegacyFiles(mem, "", keys, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := mem.Stat("OPTIONS-000001" + convertTmpSuffix); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("leftover temporary still there: %v", err)
	}
}
