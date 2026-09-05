package pgwire

import (
	"bytes"
	"encoding/binary"
	"strings"
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
		// DECIMAL(p,s) fixed-scale rendering: padded text carries its
		// declared scale as dscale; all-zero fraction groups are trimmed
		// from the digits but survive in dscale.
		{"1.00", golden(1, 0, false, 2, 1)},
		{"9.90", golden(2, 0, false, 2, 9, 9000)},
		{"0.10", golden(1, -1, false, 2, 1000)},
		{"-2.50", golden(2, 0, true, 2, 2, 5000)},
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

// TestPGNumericBounds (issue #140): weight and dscale off the wire are
// bounded like NUMERIC(p, s) is, so an eight-byte parameter cannot
// expand into hundreds of kilobytes of zeros; the encoder refuses what
// the wire format cannot carry.
func TestPGNumericBounds(t *testing.T) {
	for _, c := range []struct {
		name string
		wire []byte
	}{
		{"weight 32767, no digits", golden(0, 32767, false, 0)},
		{"weight -32768", golden(0, -32768, false, 0)},
		{"dscale 65535, no digits", golden(0, 0, false, 65535)},
		{"weight just past the bound", golden(1, pgNumericMaxWeight+1, false, 0, 1)},
		{"dscale just past the bound", golden(1, 0, false, pgNumericMaxDigits+1, 1)},
	} {
		if s, err := decodePGNumeric(c.wire); err == nil {
			t.Fatalf("%s: decoded to %d bytes, want an error", c.name, len(s))
		}
	}
	// At the bound: accepted.
	if _, err := decodePGNumeric(golden(1, pgNumericMaxWeight, false, pgNumericMaxDigits, 1)); err != nil {
		t.Fatalf("at the bound: %v", err)
	}
	if _, err := encodePGNumeric("1" + strings.Repeat("0", 1100)); err == nil {
		t.Fatal("encoded 1,101 integer digits")
	}
	if _, err := encodePGNumeric("0." + strings.Repeat("0", 1000) + "1"); err == nil {
		t.Fatal("encoded a 1,001-digit scale")
	}
	if _, err := encodePGNumeric("1" + strings.Repeat("0", 1000)); err != nil {
		t.Fatalf("1,001 integer digits (weight at the bound): %v", err)
	}
}
