package enc

import (
	"bytes"
	"io"
	mrand "math/rand/v2"
	"strings"
	"testing"

	"github.com/cockroachdb/pebble/v2/vfs"
)

func sealStream(t *testing.T, key []byte, name string, chunk int, plain []byte) []byte {
	t.Helper()
	var out bytes.Buffer
	w, err := newStreamWriter(&out, key, name, chunk)
	if err != nil {
		t.Fatal(err)
	}
	// Odd-sized writes, so chunk boundaries fall inside and between them.
	for rest := plain; len(rest) > 0; {
		n := 1 + mrand.IntN(3*chunk)
		if n > len(rest) {
			n = len(rest)
		}
		if _, err := w.Write(rest[:n]); err != nil {
			t.Fatal(err)
		}
		rest = rest[n:]
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	return out.Bytes()
}

func openStream(key []byte, name string, sealed []byte) ([]byte, error) {
	r, err := NewStreamReader(bytes.NewReader(sealed), key, name)
	if err != nil {
		return nil, err
	}
	return io.ReadAll(r)
}

// TestStreamRoundtrip: every length around the chunk boundaries opens to
// what was sealed, and the plaintext is nowhere in the sealed bytes.
func TestStreamRoundtrip(t *testing.T) {
	key := bytes.Repeat([]byte{3}, KeyLen)
	const chunk = 64
	for _, n := range []int{0, 1, chunk - 1, chunk, chunk + 1, 2 * chunk, 2*chunk + 7, 10 * chunk} {
		plain := bytes.Repeat([]byte("sealed-stream-plaintext/"), n/24+1)[:n]
		sealed := sealStream(t, key, "table_7.dxbk", chunk, plain)
		got, err := openStream(key, "table_7.dxbk", sealed)
		if err != nil {
			t.Fatalf("%d bytes: %v", n, err)
		}
		if !bytes.Equal(got, plain) {
			t.Fatalf("%d bytes: opened to %d different bytes", n, len(got))
		}
		if n >= 24 && bytes.Contains(sealed, []byte("sealed-stream-plaintext/")) {
			t.Fatalf("%d bytes: plaintext in the sealed stream", n)
		}
		chunks := (n + chunk - 1) / chunk
		if chunks == 0 {
			chunks = 1
		}
		if want := headerLenV2 + n + chunks*chunkOverhead; len(sealed) != want {
			t.Fatalf("%d bytes sealed to %d, want %d", n, len(sealed), want)
		}
	}
	// The default chunk size, across a boundary.
	plain := make([]byte, streamChunkSize+100)
	var out bytes.Buffer
	w, err := NewStreamWriter(&out, key, "f")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.Write(plain); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	if got, err := openStream(key, "f", out.Bytes()); err != nil || !bytes.Equal(got, plain) {
		t.Fatalf("default chunk size: %d bytes, %v", len(got), err)
	}
}

// TestStreamRefusesTampering: what a store file's format lets through —
// a cut exactly between chunks — is refused here along with the rest: a
// changed byte anywhere, chunks exchanged, a chunk dropped from the
// middle or the end, bytes appended, another name, another key.
func TestStreamRefusesTampering(t *testing.T) {
	key := bytes.Repeat([]byte{3}, KeyLen)
	const chunk = 64
	const phys = chunk + chunkOverhead
	plain := make([]byte, 4*chunk+10)
	for i := range plain {
		plain[i] = byte(i)
	}
	sealed := sealStream(t, key, "a", chunk, plain)
	exact := sealStream(t, key, "a", chunk, plain[:3*chunk])

	refused := func(what string, name string, k, data []byte) {
		t.Helper()
		got, err := openStream(k, name, data)
		if err == nil {
			t.Fatalf("%s: opened (%d bytes)", what, len(got))
		}
		if !strings.Contains(err.Error(), "does not authenticate") && !strings.Contains(err.Error(), "sealed stream") {
			t.Fatalf("%s: %v", what, err)
		}
	}
	for off := 0; off < len(sealed); off += 7 {
		mod := append([]byte(nil), sealed...)
		mod[off] ^= 0x40
		refused("a flipped byte", "a", key, mod)
	}
	for _, cut := range []int{0, headerLenV2 - 1, headerLenV2, headerLenV2 + phys, headerLenV2 + 2*phys, headerLenV2 + 4*phys, len(sealed) - 1, headerLenV2 + phys + 5} {
		refused("truncated", "a", key, sealed[:cut])
	}
	// A stream whose last chunk is a full one, cut before it.
	refused("truncated at the last full chunk", "a", key, exact[:headerLenV2+2*phys])
	if _, err := openStream(key, "a", exact); err != nil {
		t.Fatalf("a stream ending on a full chunk: %v", err)
	}
	swapped := append([]byte(nil), sealed...)
	copy(swapped[headerLenV2:], sealed[headerLenV2+phys:headerLenV2+2*phys])
	copy(swapped[headerLenV2+phys:], sealed[headerLenV2:headerLenV2+phys])
	refused("two chunks exchanged", "a", key, swapped)
	dropped := append(append([]byte(nil), sealed[:headerLenV2+phys]...), sealed[headerLenV2+2*phys:]...)
	refused("a chunk dropped", "a", key, dropped)
	refused("bytes appended", "a", key, append(append([]byte(nil), sealed...), sealed[headerLenV2:headerLenV2+phys]...))
	refused("another name", "b", key, sealed)
	refused("another key", "a", bytes.Repeat([]byte{4}, KeyLen), sealed)

	// A reader that has met a refusal keeps refusing, and hands out no
	// plaintext from the chunk that failed.
	mod := append([]byte(nil), sealed...)
	mod[headerLenV2+phys+20] ^= 1
	r, err := NewStreamReader(bytes.NewReader(mod), key, "a")
	if err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(r)
	if err == nil || len(got) != chunk {
		t.Fatalf("read %d bytes past a damaged second chunk: %v", len(got), err)
	}
	if _, err := r.Read(make([]byte, 1)); err == nil {
		t.Fatal("the reader recovered after a refusal")
	}
}

// A sealed stream is not a store file, and the store's reader says so
// instead of trying its tags.
func TestStoreRefusesASealedStream(t *testing.T) {
	fs, base := authenticatedFS(t)
	_, dataKey := fs.keys.Active()
	sealed := sealStream(t, dataKey, "000009.sst", 64, []byte("hello"))
	f, err := base.Create("000009.sst", vfs.WriteCategoryUnspecified)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write(sealed); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := fs.Open("000009.sst"); err == nil || !strings.Contains(err.Error(), "sealed stream") {
		t.Fatalf("store opened a sealed stream: %v", err)
	}
}
