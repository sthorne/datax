package enc

import (
	"bytes"
	"crypto/rand"
	"fmt"
	"io"
	mrand "math/rand/v2"
	"testing"

	"github.com/cockroachdb/pebble/vfs"
)

func testKeySet(t *testing.T) *KeySet {
	t.Helper()
	key := make([]byte, KeyLen)
	if _, err := rand.Read(key); err != nil {
		t.Fatal(err)
	}
	return &KeySet{activeID: 1, keys: map[uint32][]byte{1: key}}
}

// TestEncFileRoundtrip writes through the encrypting FS with a mix of
// sequential writes and WriteAt at random offsets, mirroring every write
// into a plaintext reference buffer, then reads the whole file back and at
// random offsets and compares.
func TestEncFileRoundtrip(t *testing.T) {
	fs := NewFS(vfs.NewMem(), testKeySet(t))
	f, err := fs.Create("f")
	if err != nil {
		t.Fatal(err)
	}

	const size = 1 << 16
	ref := make([]byte, size)
	rng := mrand.New(mrand.NewPCG(1, 2))

	// Sequential writes of odd sizes (crossing AES block boundaries).
	off := 0
	for off < size {
		n := 1 + rng.IntN(300)
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
	// Random-offset overwrites via WriteAt.
	for i := 0; i < 50; i++ {
		o := rng.IntN(size - 512)
		n := 1 + rng.IntN(512)
		chunk := make([]byte, n)
		for j := range chunk {
			chunk[j] = byte(rng.Uint32())
		}
		copy(ref[o:], chunk)
		if _, err := f.WriteAt(chunk, int64(o)); err != nil {
			t.Fatal(err)
		}
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}

	rf, err := fs.Open("f")
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
	// Random-offset reads, including unaligned ones.
	for i := 0; i < 100; i++ {
		o := rng.IntN(size - 512)
		n := 1 + rng.IntN(512)
		buf := make([]byte, n)
		if _, err := rf.ReadAt(buf, int64(o)); err != nil && err != io.EOF {
			t.Fatal(err)
		}
		if !bytes.Equal(buf, ref[o:o+n]) {
			t.Fatalf("ReadAt(%d, %d) mismatch", o, n)
		}
	}
	// Sequential Read sees the same bytes.
	all, err := io.ReadAll(io.NewSectionReader(readerAtOnly{rf}, 0, size))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(all, ref) {
		t.Fatal("sequential read mismatch")
	}

	// The stored bytes are actually ciphertext.
	raw, err := fs.FS.Open("f")
	if err != nil {
		t.Fatal(err)
	}
	defer raw.Close()
	rawBuf := make([]byte, size)
	if _, err := raw.ReadAt(rawBuf, headerLen); err != nil && err != io.EOF {
		t.Fatal(err)
	}
	if bytes.Contains(rawBuf, ref[:256]) {
		t.Fatal("plaintext visible in the underlying file")
	}

	// Stat hides the header on both FS and file.
	if st, err := fs.Stat("f"); err != nil || st.Size() != size {
		t.Fatalf("fs.Stat: %v size=%d want %d", err, st.Size(), size)
	}
	if st, err := rf.Stat(); err != nil || st.Size() != size {
		t.Fatalf("file.Stat: %v size=%d want %d", err, st.Size(), size)
	}
}

type readerAtOnly struct{ f vfs.File }

func (r readerAtOnly) ReadAt(p []byte, off int64) (int, error) { return r.f.ReadAt(p, off) }

// TestOpenReadWriteAppend: reopening an existing file for write continues
// the keystream where the content left off.
func TestOpenReadWriteAppend(t *testing.T) {
	fs := NewFS(vfs.NewMem(), testKeySet(t))
	f, err := fs.Create("wal")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write([]byte("first half ")); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}

	f, err = fs.OpenReadWrite("wal")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write([]byte("second half")); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}

	rf, err := fs.Open("wal")
	if err != nil {
		t.Fatal(err)
	}
	defer rf.Close()
	buf := make([]byte, len("first half second half"))
	if _, err := rf.ReadAt(buf, 0); err != nil && err != io.EOF {
		t.Fatal(err)
	}
	if string(buf) != "first half second half" {
		t.Fatalf("append roundtrip: %q", buf)
	}

	// OpenReadWrite on a missing/empty file takes the create path.
	f, err = fs.OpenReadWrite("fresh")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write([]byte("x")); err != nil {
		t.Fatal(err)
	}
	_ = f.Close()
}

// TestReuseForWrite: the recycled file gets a fresh IV — identical
// plaintext must produce different ciphertext (CTR keystream reuse would
// be a confidentiality break).
func TestReuseForWrite(t *testing.T) {
	fs := NewFS(vfs.NewMem(), testKeySet(t))
	plain := bytes.Repeat([]byte("secret"), 100)

	write := func(create func() (vfs.File, error), name string) []byte {
		t.Helper()
		f, err := create()
		if err != nil {
			t.Fatal(err)
		}
		if _, err := f.Write(plain); err != nil {
			t.Fatal(err)
		}
		if err := f.Close(); err != nil {
			t.Fatal(err)
		}
		raw, err := fs.FS.Open(name)
		if err != nil {
			t.Fatal(err)
		}
		defer raw.Close()
		buf := make([]byte, len(plain))
		if _, err := raw.ReadAt(buf, headerLen); err != nil && err != io.EOF {
			t.Fatal(err)
		}
		return buf
	}

	c1 := write(func() (vfs.File, error) { return fs.Create("000001.log") }, "000001.log")
	c2 := write(func() (vfs.File, error) { return fs.ReuseForWrite("000001.log", "000002.log") }, "000002.log")
	if bytes.Equal(c1, c2) {
		t.Fatal("ReuseForWrite reused the keystream: identical ciphertext for identical plaintext")
	}
	if _, err := fs.FS.Stat("000001.log"); err == nil {
		t.Fatal("old file still present after ReuseForWrite")
	}
}

// TestWrongAndUnknownKey: opening with a KeySet that lacks the file's key
// fails cleanly, as does a plaintext file in an encrypted store.
func TestWrongAndUnknownKey(t *testing.T) {
	base := vfs.NewMem()
	fs := NewFS(base, testKeySet(t))
	f, err := fs.Create("f")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write([]byte("data")); err != nil {
		t.Fatal(err)
	}
	_ = f.Close()

	other := NewFS(base, &KeySet{activeID: 9, keys: map[uint32][]byte{9: make([]byte, KeyLen)}})
	if _, err := other.Open("f"); err == nil {
		t.Fatal("open with unknown data key succeeded")
	}

	pf, err := base.Create("plain")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pf.Write([]byte("this file has no DXE1 header, just bytes")); err != nil {
		t.Fatal(err)
	}
	_ = pf.Close()
	if _, err := fs.Open("plain"); err == nil {
		t.Fatal("plaintext file opened as encrypted")
	}
}

// TestRegistry: LoadOrInitRegistry initializes, reopens with the right key,
// rejects the wrong key, mints a fresh active key per open while keeping
// old keys readable, and survives store-key rotation.
func TestRegistry(t *testing.T) {
	base := vfs.NewMem()
	storeKey := make([]byte, KeyLen)
	if _, err := rand.Read(storeKey); err != nil {
		t.Fatal(err)
	}

	ks1, err := LoadOrInitRegistry(base, "store", storeKey)
	if err != nil {
		t.Fatal(err)
	}
	id1, k1 := ks1.Active()
	if len(k1) != KeyLen {
		t.Fatalf("active key len %d", len(k1))
	}
	if !RegistryExists(base, "store") {
		t.Fatal("registry not written")
	}

	ks2, err := LoadOrInitRegistry(base, "store", storeKey)
	if err != nil {
		t.Fatal(err)
	}
	id2, k2 := ks2.Active()
	if id2 == id1 || bytes.Equal(k1, k2) {
		t.Fatal("second open did not mint a fresh active key")
	}
	if got, ok := ks2.Lookup(id1); !ok || !bytes.Equal(got, k1) {
		t.Fatal("old data key lost on reopen")
	}

	wrong := make([]byte, KeyLen)
	if _, err := LoadOrInitRegistry(base, "store", wrong); err == nil {
		t.Fatal("wrong store key accepted")
	}

	newKey := make([]byte, KeyLen)
	if _, err := rand.Read(newKey); err != nil {
		t.Fatal(err)
	}
	if err := RotateStoreKey(base, "store", storeKey, newKey); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadOrInitRegistry(base, "store", storeKey); err == nil {
		t.Fatal("old store key still works after rotation")
	}
	ks3, err := LoadOrInitRegistry(base, "store", newKey)
	if err != nil {
		t.Fatal(err)
	}
	if got, ok := ks3.Lookup(id1); !ok || !bytes.Equal(got, k1) {
		t.Fatal("data keys lost across store-key rotation")
	}
	if err := RotateStoreKey(base, "store", storeKey, newKey); err == nil {
		t.Fatal("rotation with wrong old key succeeded")
	}
	if err := RotateStoreKey(base, "empty", newKey, newKey); err == nil {
		t.Fatal("rotation of unencrypted dir succeeded")
	}
}

// TestSealUnseal covers the generic sealing used for the metadata backup.
func TestSealUnseal(t *testing.T) {
	key := make([]byte, KeyLen)
	if _, err := rand.Read(key); err != nil {
		t.Fatal(err)
	}
	blob := []byte(`{"some":"metadata"}`)
	sealed, err := Seal("DXMB1", key, blob)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(sealed, blob) {
		t.Fatal("sealed blob contains plaintext")
	}
	got, err := Unseal("DXMB1", key, sealed)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, blob) {
		t.Fatal("unseal mismatch")
	}
	if _, err := Unseal("DXMB1", make([]byte, KeyLen), sealed); err == nil {
		t.Fatal("wrong key unsealed")
	}
	if _, err := Unseal("DXR1", key, sealed); err == nil {
		t.Fatal("wrong magic unsealed")
	}
	sealed[len(sealed)-1] ^= 1
	if _, err := Unseal("DXMB1", key, sealed); err == nil {
		t.Fatal("tampered blob unsealed")
	}
}

// TestLoadKeyFile accepts 32 raw bytes or 64 hex chars and rejects others.
func TestLoadKeyFile(t *testing.T) {
	dir := t.TempDir()
	raw := make([]byte, KeyLen)
	for i := range raw {
		raw[i] = byte(i)
	}

	write := func(name string, b []byte) string {
		t.Helper()
		p := dir + "/" + name
		if err := writeOSFile(p, b); err != nil {
			t.Fatal(err)
		}
		return p
	}

	if k, err := LoadKeyFile(write("raw", raw)); err != nil || !bytes.Equal(k, raw) {
		t.Fatalf("raw key: %v", err)
	}
	if k, err := LoadKeyFile(write("hex", []byte(fmt.Sprintf("%x\n", raw)))); err != nil || !bytes.Equal(k, raw) {
		t.Fatalf("hex key: %v", err)
	}
	if _, err := LoadKeyFile(write("short", raw[:16])); err == nil {
		t.Fatal("short key accepted")
	}
	if _, err := LoadKeyFile(write("junk", bytes.Repeat([]byte("zz"), KeyLen))); err == nil {
		t.Fatal("non-hex 64-byte key accepted")
	}
}

func writeOSFile(path string, b []byte) error {
	f, err := vfs.Default.Create(path)
	if err != nil {
		return err
	}
	if _, err := f.Write(b); err != nil {
		_ = f.Close()
		return err
	}
	return f.Close()
}
