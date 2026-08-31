package storage

import (
	"bytes"
	"testing"

	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/util/hlc"
)

func TestMVCCKeyOrdering(t *testing.T) {
	ts := func(w int64, l int32) hlc.Timestamp { return hlc.Timestamp{WallTime: w, Logical: l} }

	// Expected engine order for user keys "a" < "a\x00" < "b":
	// per key: metadata first, then versions newest → oldest.
	ordered := [][]byte{
		EncodeMVCCKey(keys.Key("a"), hlc.Timestamp{}), // metadata
		EncodeMVCCKey(keys.Key("a"), ts(9, 1)),
		EncodeMVCCKey(keys.Key("a"), ts(9, 0)),
		EncodeMVCCKey(keys.Key("a"), ts(5, 0)),
		EncodeMVCCKey(keys.Key("a"), ts(1, 7)),
		EncodeMVCCKey(keys.Key("a\x00"), hlc.Timestamp{}),
		EncodeMVCCKey(keys.Key("a\x00"), ts(100, 0)),
		EncodeMVCCKey(keys.Key("b"), hlc.Timestamp{}),
		EncodeMVCCKey(keys.Key("b"), ts(2, 0)),
	}
	for i := 1; i < len(ordered); i++ {
		if bytes.Compare(ordered[i-1], ordered[i]) >= 0 {
			t.Fatalf("ordering violated between %x and %x (index %d)", ordered[i-1], ordered[i], i)
		}
	}
}

func TestMVCCKeyRoundTrip(t *testing.T) {
	cases := []struct {
		key keys.Key
		ts  hlc.Timestamp
	}{
		{keys.Key("simple"), hlc.Timestamp{}},
		{keys.Key("simple"), hlc.Timestamp{WallTime: 123456789, Logical: 42}},
		{keys.Key("\x00\x01\xff"), hlc.Timestamp{WallTime: 1}},
		{keys.Key{}, hlc.Timestamp{WallTime: 99, Logical: 1}},
	}
	for _, c := range cases {
		k, ts, err := DecodeMVCCKey(EncodeMVCCKey(c.key, c.ts))
		if err != nil {
			t.Fatalf("%q: %v", c.key, err)
		}
		if !bytes.Equal(k, c.key) || !ts.Equal(c.ts) {
			t.Fatalf("round trip %q@%s: got %q@%s", c.key, c.ts, k, ts)
		}
	}
}

func TestMVCCValueRoundTrip(t *testing.T) {
	for _, data := range [][]byte{nil, {}, []byte("hello"), {0x00}} {
		got, tomb, err := decodeMVCCValue(encodeMVCCValue(data, false))
		if err != nil || tomb || !bytes.Equal(got, data) {
			t.Fatalf("value %q: got %q tomb=%v err=%v", data, got, tomb, err)
		}
	}
	got, tomb, err := decodeMVCCValue(encodeMVCCValue(nil, true))
	if err != nil || !tomb || got != nil {
		t.Fatalf("tombstone: got %q tomb=%v err=%v", got, tomb, err)
	}
}
