package keystore

import (
	"bytes"
	"encoding/binary"
	"errors"
	"testing"
)

func testMaterial(b byte) []byte { return bytes.Repeat([]byte{b}, 32) }

// TestBlobRoundTripAndBinding (issue #310): a blob opens under the key,
// version material and context it was made with, and as the kind of blob
// it is. Every other way of opening it — another context, another key id,
// another version's material, the other format, a flipped bit anywhere,
// a truncation, a forged header — gives the same ErrBlob, never a wrong
// plaintext.
func TestBlobRoundTripAndBinding(t *testing.T) {
	v1, v2 := testMaterial(0x11), testMaterial(0x22)
	dataKey := bytes.Repeat([]byte{0xd4}, 32)
	ctx := []byte("tenant:4f6c")
	blob, err := SealBlob(BlobWrapped, 77, 1, v1, dataKey, ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(blob) != len(dataKey)+BlobOverhead {
		t.Fatalf("blob is %d bytes, want %d", len(blob), len(dataKey)+BlobOverhead)
	}
	if bytes.Contains(blob, dataKey) || bytes.Contains(blob, ctx) {
		t.Fatal("the blob carries the data key or the context in the clear")
	}
	h, err := ParseBlobHeader(blob)
	if err != nil || h != (BlobHeader{Format: BlobWrapped, KeyID: 77, Version: 1}) {
		t.Fatalf("header: %+v %v", h, err)
	}
	got, err := OpenBlob(BlobWrapped, 77, v1, blob, ctx)
	if err != nil || !bytes.Equal(got, dataKey) {
		t.Fatalf("round trip: %x %v", got, err)
	}
	// An empty context is a context like any other.
	empty, _ := SealBlob(BlobEncrypted, 77, 1, v1, []byte("x"), nil)
	if _, err := OpenBlob(BlobEncrypted, 77, v1, empty, []byte{}); err != nil {
		t.Fatalf("empty context: %v", err)
	}
	if _, err := OpenBlob(BlobEncrypted, 77, v1, empty, []byte{0}); !errors.Is(err, ErrBlob) {
		t.Fatalf("a context of one zero byte opened a blob made with none: %v", err)
	}

	bad := map[string]func() ([]byte, error){
		"another context":  func() ([]byte, error) { return OpenBlob(BlobWrapped, 77, v1, blob, []byte("tenant:4f6d")) },
		"no context":       func() ([]byte, error) { return OpenBlob(BlobWrapped, 77, v1, blob, nil) },
		"another key id":   func() ([]byte, error) { return OpenBlob(BlobWrapped, 78, v1, blob, ctx) },
		"another version":  func() ([]byte, error) { return OpenBlob(BlobWrapped, 77, v2, blob, ctx) },
		"the other format": func() ([]byte, error) { return OpenBlob(BlobEncrypted, 77, v1, blob, ctx) },
		"truncated":        func() ([]byte, error) { return OpenBlob(BlobWrapped, 77, v1, blob[:BlobOverhead-1], ctx) },
		"tag cut":          func() ([]byte, error) { return OpenBlob(BlobWrapped, 77, v1, blob[:len(blob)-1], ctx) },
		"empty":            func() ([]byte, error) { return OpenBlob(BlobWrapped, 77, v1, nil, ctx) },
		"unknown format": func() ([]byte, error) {
			b := append([]byte(nil), blob...)
			b[0] = 9
			return OpenBlob(9, 77, v1, b, ctx)
		},
		"version relabeled": func() ([]byte, error) {
			b := append([]byte(nil), blob...)
			b[12] = 2
			return OpenBlob(BlobWrapped, 77, v1, b, ctx)
		},
		"format relabeled": func() ([]byte, error) {
			b := append([]byte(nil), blob...)
			b[0] = BlobEncrypted
			return OpenBlob(BlobEncrypted, 77, v1, b, ctx)
		},
		"id relabeled": func() ([]byte, error) {
			b := append([]byte(nil), blob...)
			binary.BigEndian.PutUint64(b[1:9], 78)
			return OpenBlob(BlobWrapped, 78, v1, b, ctx)
		},
	}
	for i := range blob {
		i := i
		bad["bit flipped at "+itoa(i)] = func() ([]byte, error) {
			b := append([]byte(nil), blob...)
			b[i] ^= 0x01
			return OpenBlob(BlobWrapped, 77, v1, b, ctx)
		}
	}
	for name, open := range bad {
		if got, err := open(); !errors.Is(err, ErrBlob) || got != nil {
			t.Fatalf("%s: %x %v, want ErrBlob", name, got, err)
		}
	}
}

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	var b []byte
	for ; i > 0; i /= 10 {
		b = append([]byte{byte('0' + i%10)}, b...)
	}
	return string(b)
}

// TestBlobRotation: a blob names the version it was made under, so after
// a rotation it is opened with that version's material while new blobs
// use the new one; the two versions do not open each other's blobs.
func TestBlobRotation(t *testing.T) {
	versions := map[int][]byte{1: testMaterial(1), 2: testMaterial(2)}
	old, _ := SealBlob(BlobWrapped, 5, 1, versions[1], []byte("k1"), []byte("c"))
	cur, _ := SealBlob(BlobWrapped, 5, 2, versions[2], []byte("k2"), []byte("c"))
	for _, b := range [][]byte{old, cur} {
		h, err := ParseBlobHeader(b)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := OpenBlob(BlobWrapped, 5, versions[h.Version], b, []byte("c")); err != nil {
			t.Fatalf("version %d blob: %v", h.Version, err)
		}
		if _, err := OpenBlob(BlobWrapped, 5, versions[3-h.Version], b, []byte("c")); !errors.Is(err, ErrBlob) {
			t.Fatalf("version %d blob opened under the other version: %v", h.Version, err)
		}
	}
}

// TestBlobCaps: plaintexts and contexts are capped at 64 KiB; the cap
// itself is allowed.
func TestBlobCaps(t *testing.T) {
	m := testMaterial(3)
	if _, err := SealBlob(BlobEncrypted, 1, 1, m, make([]byte, MaxBlobPlaintext), nil); err != nil {
		t.Fatalf("a value at the cap: %v", err)
	}
	if _, err := SealBlob(BlobEncrypted, 1, 1, m, make([]byte, MaxBlobPlaintext+1), nil); !errors.Is(err, ErrBlobTooLarge) {
		t.Fatalf("a value over the cap: %v", err)
	}
	if _, err := SealBlob(BlobWrapped, 1, 1, m, []byte("k"), make([]byte, MaxBlobContext+1)); !errors.Is(err, ErrBlobTooLarge) {
		t.Fatalf("a context over the cap: %v", err)
	}
	if _, err := SealBlob(BlobWrapped, 1, 0, m, []byte("k"), nil); err == nil {
		t.Fatal("version 0 was taken")
	}
	if _, err := SealBlob(BlobWrapped, 1, 1, m[:16], []byte("k"), nil); err == nil {
		t.Fatal("a short key was taken")
	}
	if _, err := SealBlob(7, 1, 1, m, []byte("k"), nil); err == nil {
		t.Fatal("an unknown format was taken")
	}
}
