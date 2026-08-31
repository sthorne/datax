package encoding

import (
	"bytes"
	"fmt"
	"math/rand/v2"
	"testing"

	"github.com/sthorne/datax/pkg/util/decimal"
)

func dec(t *testing.T, s string) decimal.Dec {
	t.Helper()
	d, err := decimal.Parse(s)
	if err != nil {
		t.Fatal(err)
	}
	return d
}

// TestDecimalKeyOrderAndRoundTrip: byte order matches numeric order for a
// curated boundary set and random values; encoding round-trips; encodings
// are self-delimiting (a suffix never changes the decode).
func TestDecimalKeyOrderAndRoundTrip(t *testing.T) {
	curated := []string{
		"-12000", "-1000", "-999.999", "-1", "-0.55", "-0.5", "-0.0001",
		"0",
		"0.0001", "0.5", "0.55", "1", "1.0000001", "2", "999.999", "1000", "12000",
	}
	var vals []decimal.Dec
	for _, s := range curated {
		vals = append(vals, dec(t, s))
	}
	rng := rand.New(rand.NewPCG(3, 4))
	for i := 0; i < 500; i++ {
		coeff := rng.Int64N(2_000_000_000) - 1_000_000_000
		exp := int32(rng.Int64N(21) - 10)
		vals = append(vals, decimal.New(coeff, exp))
	}

	type pair struct {
		d   decimal.Dec
		enc []byte
	}
	var ps []pair
	for _, v := range vals {
		enc := EncodeDecimal(nil, v)
		rest, back, err := DecodeDecimal(enc)
		if err != nil {
			t.Fatalf("decode %s: %v", v, err)
		}
		if len(rest) != 0 || decimal.Cmp(back, v) != 0 || back.String() != v.String() {
			t.Fatalf("round-trip %s -> %s (rest %d)", v, back, len(rest))
		}
		// Self-delimiting: decoding with a suffix leaves it intact.
		withSuffix := append(append([]byte(nil), enc...), 0xAB, 0xCD)
		rest, back, err = DecodeDecimal(withSuffix)
		if err != nil || len(rest) != 2 || decimal.Cmp(back, v) != 0 {
			t.Fatalf("suffix decode %s: %v rest=%d", v, err, len(rest))
		}
		ps = append(ps, pair{d: v, enc: enc})
	}
	for i := 0; i < len(ps); i++ {
		for j := 0; j < len(ps); j++ {
			nc := decimal.Cmp(ps[i].d, ps[j].d)
			bc := bytes.Compare(ps[i].enc, ps[j].enc)
			if sign(nc) != sign(bc) {
				t.Fatalf("order mismatch: %s vs %s → numeric %d, bytes %d\n% x\n% x",
					ps[i].d, ps[j].d, nc, bc, ps[i].enc, ps[j].enc)
			}
		}
	}
}

func sign(c int) int {
	switch {
	case c < 0:
		return -1
	case c > 0:
		return 1
	}
	return 0
}

// TestDecimalKeyPrefixProperty: 0.5 vs 0.55 (and their negatives) exercise
// the terminator-below-digits rule explicitly.
func TestDecimalKeyPrefixProperty(t *testing.T) {
	for _, tc := range [][2]string{{"0.5", "0.55"}, {"-0.55", "-0.5"}, {"1", "1.0000001"}} {
		a, b := EncodeDecimal(nil, dec(t, tc[0])), EncodeDecimal(nil, dec(t, tc[1]))
		if bytes.Compare(a, b) >= 0 {
			t.Fatalf("%s !< %s in bytes:\n% x\n% x", tc[0], tc[1], a, b)
		}
	}
}

func BenchmarkEncodeDecimal(b *testing.B) {
	d, _ := decimal.Parse("12345.6789")
	buf := make([]byte, 0, 32)
	for i := 0; i < b.N; i++ {
		buf = EncodeDecimal(buf[:0], d)
	}
	_ = fmt.Sprint(len(buf))
}

func BenchmarkDecodeDecimal(b *testing.B) {
	d, _ := decimal.Parse("12345.6789")
	enc := EncodeDecimal(nil, d)
	for i := 0; i < b.N; i++ {
		if _, _, err := DecodeDecimal(enc); err != nil {
			b.Fatal(err)
		}
	}
}
