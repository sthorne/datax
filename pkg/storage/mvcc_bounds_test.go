package storage

import (
	"bytes"
	"math/rand"
	"testing"

	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/util/hlc"
)

// TestMVCCKeyBoundsSpanExactlyTheKey (issue #163): the iterator bounds of
// a user key contain every engine key of that key — its metadata key and
// every version — and no engine key of any other user key, including the
// keys that extend it (with a NUL, with a byte at or above the
// terminator) and the keys it extends.
func TestMVCCKeyBoundsSpanExactlyTheKey(t *testing.T) {
	rng := rand.New(rand.NewSource(163))
	inside := func(lower, upper, k []byte) bool {
		return bytes.Compare(k, lower) >= 0 && bytes.Compare(k, upper) < 0
	}
	versions := []hlc.Timestamp{{}, {WallTime: 1}, {WallTime: 1, Logical: 5}, {WallTime: 1 << 60}, {WallTime: -1}}
	for i := 0; i < 2000; i++ {
		k := make(keys.Key, 1+rng.Intn(12))
		for j := range k {
			// Plenty of NULs and terminator-adjacent bytes.
			k[j] = byte([]int{0x00, 0x01, 0x02, 0xff, rng.Intn(256)}[rng.Intn(5)])
		}
		lower, upper := mvccKeyBounds(k)
		if !bytes.Equal(lower, EncodeMVCCKey(k, hlc.Timestamp{})) {
			t.Fatalf("%x: lower %x is not the metadata key", k, lower)
		}
		for _, ts := range versions {
			if !inside(lower, upper, EncodeMVCCKey(k, ts)) {
				t.Fatalf("%x@%s: version key outside [%x, %x)", k, ts, lower, upper)
			}
		}
		others := []keys.Key{
			append(k.Clone(), 0x00), append(k.Clone(), 0x01), append(k.Clone(), 0x02), append(k.Clone(), 0xff),
			k.Next(), k[:len(k)-1],
		}
		if b := k.Clone(); b[len(b)-1] != 0xff {
			b[len(b)-1]++
			others = append(others, b)
		}
		for _, o := range others {
			if len(o) == 0 {
				continue
			}
			for _, ts := range versions {
				if inside(lower, upper, EncodeMVCCKey(o, ts)) {
					t.Fatalf("%x: engine key of %x@%s inside [%x, %x)", k, o, ts, lower, upper)
				}
			}
		}
	}
}

// TestGetterReadsThroughOneIterator (issue #163): a Getter over an engine
// answers for many keys in turn; over an indexed batch it sees the
// batch's own writes made between its Gets.
func TestGetterReadsThroughOneIterator(t *testing.T) {
	eng := openTestEngine(t)
	b := eng.NewBatch()
	for i := 0; i < 10; i++ {
		if err := MVCCPutCommitted(b, keys.Key{'k', byte(i)}, hlc.Timestamp{WallTime: 10}, []byte{'v', byte(i)}); err != nil {
			t.Fatal(err)
		}
	}
	if err := b.Commit(false); err != nil {
		t.Fatal(err)
	}
	readTS := hlc.Timestamp{WallTime: 20}
	g := NewGetter(eng)
	defer g.Close()
	for round := 0; round < 3; round++ {
		for i := 9; i >= 0; i-- {
			v, err := g.Get(keys.Key{'k', byte(i)}, readTS, MVCCGetOptions{})
			if err != nil || !bytes.Equal(v, []byte{'v', byte(i)}) {
				t.Fatalf("k%d: %q, %v", i, v, err)
			}
		}
		if v, err := g.Get(keys.Key{'k', 200}, readTS, MVCCGetOptions{}); err != nil || v != nil {
			t.Fatalf("absent key: %q, %v", v, err)
		}
	}

	// Over a batch: a Get, a write, and the next Get sees the write.
	wb := eng.NewBatch()
	defer func() { _ = wb.Close() }()
	bg := NewGetter(wb)
	defer bg.Close()
	if v, err := bg.Get(keys.Key{'k', 3}, readTS, MVCCGetOptions{}); err != nil || !bytes.Equal(v, []byte("v\x03")) {
		t.Fatalf("before the batch write: %q, %v", v, err)
	}
	if err := MVCCPutCommitted(wb, keys.Key{'k', 3}, hlc.Timestamp{WallTime: 15}, []byte("new")); err != nil {
		t.Fatal(err)
	}
	if err := MVCCPutCommitted(wb, keys.Key{'k', 100}, hlc.Timestamp{WallTime: 15}, []byte("fresh")); err != nil {
		t.Fatal(err)
	}
	if v, err := bg.Get(keys.Key{'k', 3}, readTS, MVCCGetOptions{}); err != nil || string(v) != "new" {
		t.Fatalf("after the batch write: %q, %v", v, err)
	}
	if v, err := bg.Get(keys.Key{'k', 100}, readTS, MVCCGetOptions{}); err != nil || string(v) != "fresh" {
		t.Fatalf("a key first written in the batch: %q, %v", v, err)
	}
}
