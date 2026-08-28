package encoding

import (
	"bytes"
	"math"
	"math/rand"
	"testing"
)

func TestInt64Ordering(t *testing.T) {
	vals := []int64{math.MinInt64, -1 << 40, -256, -2, -1, 0, 1, 2, 255, 1 << 40, math.MaxInt64}
	for i := 1; i < len(vals); i++ {
		a := EncodeInt64(nil, vals[i-1])
		b := EncodeInt64(nil, vals[i])
		if bytes.Compare(a, b) >= 0 {
			t.Fatalf("Encode(%d) >= Encode(%d)", vals[i-1], vals[i])
		}
	}
	for _, v := range vals {
		rest, got, err := DecodeInt64(EncodeInt64(nil, v))
		if err != nil || len(rest) != 0 || got != v {
			t.Fatalf("round trip %d: got %d, rest %d, err %v", v, got, len(rest), err)
		}
	}
}

func TestFloat64Ordering(t *testing.T) {
	vals := []float64{math.Inf(-1), -math.MaxFloat64, -1.5, -math.SmallestNonzeroFloat64,
		math.Copysign(0, -1), 0, math.SmallestNonzeroFloat64, 1.5, math.MaxFloat64, math.Inf(1)}
	for i := 1; i < len(vals); i++ {
		a := EncodeFloat64(nil, vals[i-1])
		b := EncodeFloat64(nil, vals[i])
		if bytes.Compare(a, b) > 0 {
			t.Fatalf("Encode(%v) > Encode(%v)", vals[i-1], vals[i])
		}
	}
	for _, v := range vals {
		rest, got, err := DecodeFloat64(EncodeFloat64(nil, v))
		if err != nil || len(rest) != 0 || got != v {
			t.Fatalf("round trip %v: got %v, err %v", v, got, err)
		}
	}
}

func TestBytesOrderingAndRoundTrip(t *testing.T) {
	cases := [][]byte{
		{}, {0x00}, {0x00, 0x00}, {0x00, 0x01}, {0x00, 0xff}, {0x01},
		[]byte("a"), []byte("a\x00"), []byte("a\x00b"), []byte("aa"), []byte("b"),
		{0xff}, {0xff, 0x00}, {0xff, 0xff},
	}
	for i := 0; i < len(cases); i++ {
		enc := EncodeBytes(nil, cases[i])
		rest, dec, err := DecodeBytes(enc)
		if err != nil || len(rest) != 0 || !bytes.Equal(dec, cases[i]) {
			t.Fatalf("round trip %q: got %q rest %q err %v", cases[i], dec, rest, err)
		}
		for j := i + 1; j < len(cases); j++ {
			cmpRaw := bytes.Compare(cases[i], cases[j])
			cmpEnc := bytes.Compare(enc, EncodeBytes(nil, cases[j]))
			if (cmpRaw < 0) != (cmpEnc < 0) || (cmpRaw == 0) != (cmpEnc == 0) {
				t.Fatalf("order mismatch %q vs %q: raw %d enc %d", cases[i], cases[j], cmpRaw, cmpEnc)
			}
		}
	}
}

func TestBytesOrderingRandom(t *testing.T) {
	rng := rand.New(rand.NewSource(42))
	for iter := 0; iter < 5000; iter++ {
		a := make([]byte, rng.Intn(12))
		b := make([]byte, rng.Intn(12))
		rng.Read(a)
		rng.Read(b)
		cmpRaw := bytes.Compare(a, b)
		cmpEnc := bytes.Compare(EncodeBytes(nil, a), EncodeBytes(nil, b))
		if (cmpRaw < 0) != (cmpEnc < 0) || (cmpRaw == 0) != (cmpEnc == 0) {
			t.Fatalf("order mismatch %x vs %x: raw %d enc %d", a, b, cmpRaw, cmpEnc)
		}
	}
}

func TestDecodeBytesWithTrailingData(t *testing.T) {
	enc := EncodeBytes(nil, []byte("key"))
	enc = append(enc, 0xde, 0xad)
	rest, dec, err := DecodeBytes(enc)
	if err != nil || string(dec) != "key" || !bytes.Equal(rest, []byte{0xde, 0xad}) {
		t.Fatalf("got %q rest %x err %v", dec, rest, err)
	}
}
