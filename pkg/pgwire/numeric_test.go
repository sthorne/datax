package pgwire

import (
	"bytes"
	"encoding/binary"
	"testing"
)

// golden builds the wire bytes from the documented fields.
func golden(ndigits, weight int, neg bool, dscale int, groups ...uint16) []byte {
	out := make([]byte, 0, 8+2*len(groups))
	out = binary.BigEndian.AppendUint16(out, uint16(ndigits))
	out = binary.BigEndian.AppendUint16(out, uint16(int16(weight)))
	s := uint16(pgNumericPos)
	if neg {
		s = pgNumericNeg
	}
	out = binary.BigEndian.AppendUint16(out, s)
	out = binary.BigEndian.AppendUint16(out, uint16(dscale))
	for _, g := range groups {
		out = binary.BigEndian.AppendUint16(out, g)
	}
	return out
}

// TestPGNumericGoldenVectors: hand-computed vectors of PostgreSQL's binary
// NUMERIC layout — the digit-group/weight math is where silent 10^4 shifts
// hide.
func TestPGNumericGoldenVectors(t *testing.T) {
	cases := []struct {
		text string
		wire []byte
	}{
		{"0", golden(0, 0, false, 0)},
		{"1", golden(1, 0, false, 0, 1)},
		{"-1", golden(1, 0, true, 0, 1)},
		{"0.5", golden(1, -1, false, 1, 5000)},
		{"12345.6789", golden(3, 1, false, 4, 1, 2345, 6789)},
		{"0.00001", golden(1, -2, false, 5, 1000)},
		{"10000", golden(1, 1, false, 0, 1)},
		{"99999999", golden(2, 1, false, 0, 9999, 9999)},
		// 0.00012 = 0001|2000 in base-10000 starting at 10000^-1.
		{"-0.00012", golden(2, -1, true, 5, 1, 2000)},
	}
	for _, tc := range cases {
		in := tc.text
		enc, err := encodePGNumeric(in)
		if err != nil {
			t.Fatalf("encode %q: %v", in, err)
		}
		if !bytes.Equal(enc, tc.wire) {
			t.Fatalf("encode %q:\n got % x\nwant % x", in, enc, tc.wire)
		}
		back, err := decodePGNumeric(tc.wire)
		if err != nil {
			t.Fatalf("decode %q: %v", in, err)
		}
		if back != in {
			t.Fatalf("decode(% x) = %q, want %q", tc.wire, back, in)
		}
	}
}

// TestPGNumericRoundTrip: canonical strings survive encode/decode across
// group boundaries and signs.
func TestPGNumericRoundTrip(t *testing.T) {
	for _, s := range []string{
		"0", "1", "-1", "9999", "10000", "10001", "99999999", "100000000",
		"0.1", "0.0001", "0.00001", "-0.00001", "1234.5", "0.12345678",
		"123456789012345678901234567890.000000000001",
	} {
		enc, err := encodePGNumeric(s)
		if err != nil {
			t.Fatalf("%q: %v", s, err)
		}
		back, err := decodePGNumeric(enc)
		if err != nil {
			t.Fatalf("%q: %v", s, err)
		}
		if back != s {
			t.Fatalf("round-trip %q -> %q", s, back)
		}
	}
	if _, err := decodePGNumeric(golden(1, 0, false, 0)[:6]); err == nil {
		t.Fatal("truncated numeric accepted")
	}
	nan := golden(0, 0, false, 0)
	binary.BigEndian.PutUint16(nan[4:6], 0xC000)
	if _, err := decodePGNumeric(nan); err == nil {
		t.Fatal("NaN numeric accepted")
	}
}
