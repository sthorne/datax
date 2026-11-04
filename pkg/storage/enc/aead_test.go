package enc

import (
	"bytes"
	"io"
	mrand "math/rand/v2"
	"strings"
	"testing"

	"github.com/cockroachdb/pebble/v2/vfs"
)

func authenticatedFS(t *testing.T) (*FS, vfs.FS) {
	t.Helper()
	base := vfs.NewMem()
	fs := NewFS(base, testKeySet(t))
	fs.EnableAuthenticatedWrites()
	return fs, base
}

// TestAEADFileRoundtrip is TestEncFileRoundtrip's shape on the
// authenticated format: odd-sized sequential writes, random overwrites
// through WriteAt, random unaligned reads, sequential reads, ciphertext on
// disk, and logical sizes from both Stat paths — across chunk boundaries
// throughout (the file is sixteen chunks).
func TestAEADFileRoundtrip(t *testing.T) {
	fs, base := authenticatedFS(t)
	f, err := fs.Create("000001.sst", vfs.WriteCategoryUnspecified)
	if err != nil {
		t.Fatal(err)
	}
	const size = 16 * defaultChunkSize
	ref := make([]byte, size)
	rng := mrand.New(mrand.NewPCG(3, 4))
	off := 0
	for off < size {
		n := 1 + rng.IntN(3*defaultChunkSize)
		if off+n > size {
			n = size - off
		}
		chunk := make([]byte, n)
		for i := range chunk {
			chunk[i] = byte(rng.Uint32())
		}
		copy(ref[off:], chunk)
		if _, err := f.Write(chunk); err != nil {
			t.Fatal(err)
		}
		off += n
	}
	for i := 0; i < 50; i++ {
		o := rng.IntN(size - 2*defaultChunkSize)
		n := 1 + rng.IntN(2*defaultChunkSize)
		chunk := make([]byte, n)
		for j := range chunk {
			chunk[j] = byte(rng.Uint32())
		}
		copy(ref[o:], chunk)
		if _, err := f.WriteAt(chunk, int64(o)); err != nil {
			t.Fatal(err)
		}
	}
	if st, err := f.Stat(); err != nil || st.Size() != size {
		t.Fatalf("open file Stat: %v size=%d want %d", err, st.Size(), size)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}

	rf, err := fs.Open("000001.sst")
	if err != nil {
		t.Fatal(err)
	}
	defer rf.Close()
	got := make([]byte, size)
	if _, err := rf.ReadAt(got, 0); err != nil && err != io.EOF {
		t.Fatal(err)
	}
	if !bytes.Equal(got, ref) {
		t.Fatal("full read does not match plaintext reference")
	}
	for i := 0; i < 200; i++ {
		o := rng.IntN(size - 1)
		n := 1 + rng.IntN(2*defaultChunkSize)
		if o+n > size {
			n = size - o
		}
		buf := make([]byte, n)
		if _, err := rf.ReadAt(buf, int64(o)); err != nil && err != io.EOF {
			t.Fatal(err)
		}
		if !bytes.Equal(buf, ref[o:o+n]) {
			t.Fatalf("ReadAt(%d, %d) mismatch", o, n)
		}
	}
	// A read running past the end returns what there is and io.EOF.
	tailBuf := make([]byte, 100)
	if n, err := rf.ReadAt(tailBuf, size-40); n != 40 || err != io.EOF {
		t.Fatalf("ReadAt past end: n=%d err=%v, want 40, EOF", n, err)
	}
	if n, err := rf.ReadAt(tailBuf, size); n != 0 || err != io.EOF {
		t.Fatalf("ReadAt at end: n=%d err=%v", n, err)
	}
	all, err := io.ReadAll(io.NewSectionReader(readerAtOnly{rf}, 0, size))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(all, ref) {
		t.Fatal("sequential read mismatch")
	}
	raw, err := base.Open("000001.sst")
	if err != nil {
		t.Fatal(err)
	}
	defer raw.Close()
	rawBuf := make([]byte, size)
	if _, err := raw.ReadAt(rawBuf, headerLenV2); err != nil && err != io.EOF {
		t.Fatal(err)
	}
	if bytes.Contains(rawBuf, ref[:256]) {
		t.Fatal("plaintext visible in the underlying file")
	}
	if st, err := fs.Stat("000001.sst"); err != nil || st.Size() != size {
		t.Fatalf("fs.Stat: %v size=%d want %d", err, st.Size(), size)
	}
	if st, err := rf.Stat(); err != nil || st.Size() != size {
		t.Fatalf("file.Stat: %v size=%d want %d", err, st.Size(), size)
	}
	// The physical file is the header plus sixteen framed chunks.
	if st, err := base.Stat("000001.sst"); err != nil || st.Size() != int64(headerLenV2+16*(defaultChunkSize+chunkOverhead)) {
		t.Fatalf("physical size %d", st.Size())
	}
}

// TestAEADAppendLikeAWAL: many small appends, each synced, then the file
// reopened for append (OpenReadWrite) and extended, then read whole. A
// log takes the framed layout: every append is a frame of its own, and
// what comes back is what went in.
func TestAEADAppendLikeAWAL(t *testing.T) {
	fs, base := authenticatedFS(t)
	f, err := fs.Create("000002.log", vfs.WriteCategoryUnspecified)
	if err != nil {
		t.Fatal(err)
	}
	var ref []byte
	rng := mrand.New(mrand.NewPCG(5, 6))
	for i := 0; i < 400; i++ {
		rec := make([]byte, 1+rng.IntN(200))
		for j := range rec {
			rec[j] = byte(rng.Uint32())
		}
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
	rf, err := fs.OpenReadWrite("000002.log", vfs.WriteCategoryUnspecified)
	if err != nil {
		t.Fatal(err)
	}
	if st, err := rf.Stat(); err != nil || st.Size() != int64(len(ref)) {
		t.Fatalf("reopened Stat: %v size=%d want %d", err, st.Size(), len(ref))
	}
	more := bytes.Repeat([]byte("appended-after-reopen."), 300)
	if _, err := rf.Write(more); err != nil {
		t.Fatal(err)
	}
	ref = append(ref, more...)
	if err := rf.Close(); err != nil {
		t.Fatal(err)
	}
	if st, err := fs.Stat("000002.log"); err != nil || st.Size() != int64(len(ref)) {
		t.Fatalf("fs.Stat of a log: %v size=%d want %d", err, st.Size(), len(ref))
	}
	rf, err = fs.Open("000002.log")
	if err != nil {
		t.Fatal(err)
	}
	defer rf.Close()
	// Sequentially, as Pebble replays a log …
	got, err := io.ReadAll(rf)
	if err != nil || !bytes.Equal(got, ref) {
		t.Fatalf("WAL-shaped file does not read back sequentially: %v", err)
	}
	// … and at an offset behind the cursor, which rewinds.
	at := make([]byte, 500)
	if _, err := rf.ReadAt(at, 1000); err != nil && err != io.EOF {
		t.Fatal(err)
	}
	if !bytes.Equal(at, ref[1000:1500]) {
		t.Fatal("ReadAt behind the cursor does not match")
	}
	// On disk the file is framed — 400 frames for the appends and one for
	// the extension — and nothing in it is plaintext.
	raw, _ := base.Open("000002.log")
	defer raw.Close()
	st, _ := raw.Stat()
	frames, size := scanFrames(t, raw, st.Size())
	if frames != 401 || size != int64(len(ref)) {
		t.Fatalf("on disk: %d frames holding %d bytes; want 401 holding %d", frames, size, len(ref))
	}
	rawBuf := make([]byte, st.Size())
	_, _ = raw.ReadAt(rawBuf, 0)
	if bytes.Contains(rawBuf, more[:64]) {
		t.Fatal("plaintext visible in the underlying log")
	}
}

// scanFrames counts a framed file's complete frames and their plaintext,
// through the base FS.
func scanFrames(t *testing.T, raw vfs.File, physical int64) (frames, size int64) {
	t.Helper()
	e := &frameFile{f: raw}
	if err := e.scan(physical); err != nil {
		t.Fatal(err)
	}
	return e.frames, e.size
}

// writeFile writes content as a fresh authenticated file and returns it.
func writeAuthenticated(t *testing.T, fs *FS, name string, content []byte) {
	t.Helper()
	f, err := fs.Create(name, vfs.WriteCategoryUnspecified)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write(content); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
}

func readAllThrough(fs *FS, name string, n int) ([]byte, error) {
	f, err := fs.Open(name)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	buf := make([]byte, n)
	_, err = f.ReadAt(buf, 0)
	if err == io.EOF {
		err = nil
	}
	return buf, err
}

// flipByte flips one byte of the physical file at off.
func flipByte(t *testing.T, base vfs.FS, name string, off int64) {
	t.Helper()
	f, err := base.OpenReadWrite(name, vfs.WriteCategoryUnspecified)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	var b [1]byte
	if _, err := f.ReadAt(b[:], off); err != nil {
		t.Fatal(err)
	}
	b[0] ^= 0x01
	if _, err := f.WriteAt(b[:], off); err != nil {
		t.Fatal(err)
	}
}

// TestAEADRefusesTampering: the distinguishing cases from issue #220's
// test list. A flipped ciphertext byte, a flipped tag byte and a changed
// nonce are each refused at read; two chunks swapped are refused at
// read; a file truncated inside a chunk is refused at open; a bound file
// renamed is refused at open. The same bytes untouched read back.
func TestAEADRefusesTampering(t *testing.T) {
	const size = 3*defaultChunkSize + 100
	content := bytes.Repeat([]byte("row-value-"), size/10+1)[:size]
	phys := int64(defaultChunkSize + chunkOverhead)
	cases := []struct {
		name   string
		tamper func(t *testing.T, base vfs.FS, name string)
		at     string // "open" or "read"
	}{
		{"ciphertext byte in chunk 1", func(t *testing.T, base vfs.FS, name string) {
			flipByte(t, base, name, headerLenV2+phys+chunkNonceLen+17)
		}, "read"},
		{"tag byte of chunk 2", func(t *testing.T, base vfs.FS, name string) {
			flipByte(t, base, name, headerLenV2+3*phys-1)
		}, "read"},
		{"nonce of chunk 0", func(t *testing.T, base vfs.FS, name string) {
			flipByte(t, base, name, headerLenV2+3)
		}, "read"},
		{"chunks 0 and 1 swapped", func(t *testing.T, base vfs.FS, name string) {
			f, err := base.OpenReadWrite(name, vfs.WriteCategoryUnspecified)
			if err != nil {
				t.Fatal(err)
			}
			defer f.Close()
			a, b := make([]byte, phys), make([]byte, phys)
			if _, err := f.ReadAt(a, headerLenV2); err != nil {
				t.Fatal(err)
			}
			if _, err := f.ReadAt(b, headerLenV2+phys); err != nil {
				t.Fatal(err)
			}
			if _, err := f.WriteAt(b, headerLenV2); err != nil {
				t.Fatal(err)
			}
			if _, err := f.WriteAt(a, headerLenV2+phys); err != nil {
				t.Fatal(err)
			}
		}, "read"},
		{"truncated inside the last chunk", func(t *testing.T, base vfs.FS, name string) {
			// Pebble's vfs has no truncate: rewrite the file shorter.
			f, err := base.Open(name)
			if err != nil {
				t.Fatal(err)
			}
			st, _ := f.Stat()
			all := make([]byte, st.Size()-7)
			if _, err := f.ReadAt(all, 0); err != nil {
				t.Fatal(err)
			}
			_ = f.Close()
			_ = base.Remove(name)
			w, err := base.Create(name, vfs.WriteCategoryUnspecified)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := w.Write(all); err != nil {
				t.Fatal(err)
			}
			_ = w.Close()
		}, "open"},
		{"bound file renamed", func(t *testing.T, base vfs.FS, name string) {
			if err := base.Rename(name, "000099.sst"); err != nil {
				t.Fatal(err)
			}
		}, "open"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			fs, base := authenticatedFS(t)
			writeAuthenticated(t, fs, "000007.sst", content)
			if got, err := readAllThrough(fs, "000007.sst", size); err != nil || !bytes.Equal(got, content) {
				t.Fatalf("untampered read: %v", err)
			}
			c.tamper(t, base, "000007.sst")
			name := "000007.sst"
			if c.name == "bound file renamed" {
				name = "000099.sst"
			}
			f, err := fs.Open(name)
			if c.at == "open" {
				if err == nil {
					_ = f.Close()
					t.Fatal("tampered file opened; want refusal at open")
				}
				return
			}
			if err != nil {
				t.Fatalf("open: %v (want the refusal at read)", err)
			}
			defer f.Close()
			buf := make([]byte, size)
			if _, err := f.ReadAt(buf, 0); err == nil || err == io.EOF {
				t.Fatal("tampered content read back without error")
			} else if !strings.Contains(err.Error(), "does not authenticate") {
				t.Fatalf("refusal names something else: %v", err)
			}
		})
	}
}

// TestAEADUnboundFileSurvivesRename: files Pebble writes under a temporary
// name and renames (OPTIONS, markers) are not name-bound and read back
// under the new name; a bound sstable does not (TestAEADRefusesTampering).
func TestAEADUnboundFileSurvivesRename(t *testing.T) {
	fs, base := authenticatedFS(t)
	content := []byte("options-content")
	writeAuthenticated(t, fs, "OPTIONS-000003.tmp", content)
	if err := base.Rename("OPTIONS-000003.tmp", "OPTIONS-000003"); err != nil {
		t.Fatal(err)
	}
	got, err := readAllThrough(fs, "OPTIONS-000003", len(content))
	if err != nil || !bytes.Equal(got, content) {
		t.Fatalf("renamed unbound file: %q, %v", got, err)
	}
}

// TestAEADTruncationAtAChunkBoundaryReadsShort documents the one truncation
// the file layer cannot see: cut exactly between chunks, the file is a
// valid shorter file. Pebble's own length checks (sstable footer, MANIFEST
// records) are what catch it above this layer.
func TestAEADTruncationAtAChunkBoundaryReadsShort(t *testing.T) {
	fs, base := authenticatedFS(t)
	content := bytes.Repeat([]byte{7}, 2*defaultChunkSize+50)
	writeAuthenticated(t, fs, "000011.sst", content)
	f, _ := base.Open("000011.sst")
	cut := int64(headerLenV2 + 2*(defaultChunkSize+chunkOverhead))
	all := make([]byte, cut)
	if _, err := f.ReadAt(all, 0); err != nil {
		t.Fatal(err)
	}
	_ = f.Close()
	_ = base.Remove("000011.sst")
	w, _ := base.Create("000011.sst", vfs.WriteCategoryUnspecified)
	_, _ = w.Write(all)
	_ = w.Close()
	st, err := fs.Stat("000011.sst")
	if err != nil || st.Size() != 2*defaultChunkSize {
		t.Fatalf("stat after boundary truncation: %v size=%d", err, st.Size())
	}
	got, err := readAllThrough(fs, "000011.sst", 2*defaultChunkSize)
	if err != nil || !bytes.Equal(got, content[:2*defaultChunkSize]) {
		t.Fatalf("boundary-truncated file: %v", err)
	}
}

// TestFormatsCoexistUntilStrict: a store mid-switch reads both formats;
// once strict, a legacy file is refused and new files are authenticated.
func TestFormatsCoexistUntilStrict(t *testing.T) {
	base := vfs.NewMem()
	keys := testKeySet(t)
	fs := NewFS(base, keys)
	legacy := []byte("legacy content")
	f, err := fs.Create("000020.sst", vfs.WriteCategoryUnspecified)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write(legacy); err != nil {
		t.Fatal(err)
	}
	_ = f.Close()
	if a, ok, err := FileAuthenticated(base, "000020.sst"); err != nil || !ok || a {
		t.Fatalf("legacy file reports authenticated=%v ok=%v err=%v", a, ok, err)
	}

	fs.EnableAuthenticatedWrites()
	writeAuthenticated(t, fs, "000021.sst", []byte("new content"))
	if a, ok, err := FileAuthenticated(base, "000021.sst"); err != nil || !ok || !a {
		t.Fatalf("new file reports authenticated=%v ok=%v err=%v", a, ok, err)
	}
	if got, err := readAllThrough(fs, "000020.sst", len(legacy)); err != nil || !bytes.Equal(got, legacy) {
		t.Fatalf("legacy file mid-switch: %q, %v", got, err)
	}
	names, err := LegacyFiles(base, "")
	if err != nil || len(names) != 1 || names[0] != "000020.sst" {
		t.Fatalf("LegacyFiles: %v, %v", names, err)
	}

	fs.RefuseLegacy()
	if _, err := fs.Open("000020.sst"); err == nil || !strings.Contains(err.Error(), "unauthenticated (DXE1)") {
		t.Fatalf("legacy file under strict: %v, want refusal", err)
	}
	if got, err := readAllThrough(fs, "000021.sst", len("new content")); err != nil || string(got) != "new content" {
		t.Fatalf("authenticated file under strict: %q, %v", got, err)
	}
	// Stat tells the formats apart without opening them through the FS.
	if st, err := fs.Stat("000021.sst"); err != nil || st.Size() != int64(len("new content")) {
		t.Fatalf("Stat of an authenticated file: %v %d", err, st.Size())
	}
}

func TestLogicalSize(t *testing.T) {
	phys := int64(defaultChunkSize + chunkOverhead)
	cases := []struct {
		physical int64
		want     int64
		ok       bool
	}{
		{headerLenV2, 0, true},
		{headerLenV2 + phys, defaultChunkSize, true},
		{headerLenV2 + phys + chunkOverhead + 1, defaultChunkSize + 1, true},
		{headerLenV2 + phys + chunkOverhead, 0, false}, // a frame with no byte in it
		{headerLenV2 + 5, 0, false},
		{headerLenV2 - 1, 0, false},
		{headerLenV2 + 3*phys + chunkOverhead + 100, 3*defaultChunkSize + 100, true},
	}
	for _, c := range cases {
		got, ok := logicalSize(c.physical, defaultChunkSize)
		if got != c.want || ok != c.ok {
			t.Errorf("logicalSize(%d) = %d, %v; want %d, %v", c.physical, got, ok, c.want, c.ok)
		}
	}
}
