package encoding

import (
	"bytes"
	"encoding/hex"
	"sort"
	"testing"

	"github.com/sthorne/datax/pkg/util/decimal"
)

// TestDecimalTextKeyOrder (PG-T-013): numeric keys order as
// PostgreSQL orders the values: -Infinity, the negatives, zero, the
// positives, Infinity, NaN; each special value round-trips.
func TestDecimalTextKeyOrder(t *testing.T) {
	vals := []string{"-Infinity", "-1e9", "-1.5", "0", "0.001", "7", "123456789012345678901234567890", "Infinity", "NaN"}
	var enc [][]byte
	for _, v := range vals {
		k, err := EncodeDecimalText([]byte{0xAA}, v)
		if err != nil {
			t.Fatalf("encode %q: %v", v, err)
		}
		rest, back, err := DecodeDecimalText(k[1:])
		if err != nil || len(rest) != 0 {
			t.Fatalf("decode %q: %v (rest %x)", v, err, rest)
		}
		if want := v; v == "-1e9" {
			want = "-1000000000"
			if back != want {
				t.Fatalf("round trip %q = %q", v, back)
			}
		} else if back != want {
			t.Fatalf("round trip %q = %q", v, back)
		}
		enc = append(enc, k)
	}
	if !sort.SliceIsSorted(enc, func(i, j int) bool { return bytes.Compare(enc[i], enc[j]) < 0 }) {
		t.Fatal("keys out of order")
	}
	if _, err := EncodeDecimalText(nil, "nope"); err == nil {
		t.Fatal("bad decimal text encoded")
	}
}

// TestDecimalKeyBytesPinned: a finite decimal's key bytes are what they
// were before the special values got markers (the bytes below are the
// encoding at 356415d), through EncodeDecimal and EncodeDecimalText
// alike, so index keys already stored stay valid.
func TestDecimalKeyBytesPinned(t *testing.T) {
	for _, c := range []struct{ v, key string }{
		{"0", "15"},
		{"1", "1680000000000000010200"},
		{"-1", "147ffffffffffffffefdff"},
		{"0.001", "167ffffffffffffffe0200"},
		{"-0.001", "148000000000000001fdff"},
		{"7", "1680000000000000010800"},
		{"10", "1680000000000000020200"},
		{"123.45", "168000000000000003020304050600"},
		{"-123.45", "147ffffffffffffffcfdfcfbfaf9ff"},
		{"1e20", "1680000000000000150200"},
		{"-1e20", "147fffffffffffffeafdff"},
		{"99999999999999999999.999999", "1680000000000000140a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a00"},
		{"0.5", "1680000000000000000600"},
		{"-0.55", "147ffffffffffffffff9f9ff"},
	} {
		d, err := decimal.Parse(c.v)
		if err != nil {
			t.Fatal(err)
		}
		if got := hex.EncodeToString(EncodeDecimal(nil, d)); got != c.key {
			t.Errorf("EncodeDecimal(%s) = %s, want %s", c.v, got, c.key)
		}
		k, err := EncodeDecimalText(nil, d.String())
		if err != nil || hex.EncodeToString(k) != c.key {
			t.Errorf("EncodeDecimalText(%s) = %x, %v; want %s", c.v, k, err, c.key)
		}
	}
}
