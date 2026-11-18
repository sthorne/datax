package encoding

import (
	"bytes"
	"sort"
	"testing"
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
