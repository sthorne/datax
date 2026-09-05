package pgwire

import (
	"encoding/binary"
	"fmt"
	"strings"
)

// PostgreSQL binary NUMERIC: int16 ndigits, int16 weight, int16 sign,
// int16 dscale, then ndigits base-10000 groups. weight is the exponent (in
// groups) of the FIRST group: value = Σ digits[i] × 10000^(weight−i).
// Getting weight wrong shifts the value by powers of 10⁴ silently, which
// is why decode(encode(x)) round-trips are unit-tested against golden
// vectors below in numeric_test.go.

const (
	pgNumericPos = 0x0000
	pgNumericNeg = 0x4000

	// pgNumericMaxDigits bounds a value's integer digits and its scale,
	// as PostgreSQL's NUMERIC(p, s) does (1,000 each): weight and dscale
	// come off the wire unchecked otherwise, and an eight-byte parameter
	// with no digit groups would otherwise expand to ~200 KB of zeros
	// (issue #140).
	pgNumericMaxDigits = 1000
	pgNumericMaxWeight = pgNumericMaxDigits/4 + 1
)

// encodePGNumeric converts a canonical decimal string (as produced by
// util/decimal — plain form, no exponent) to the binary wire format.
func encodePGNumeric(s string) ([]byte, error) {
	neg := strings.HasPrefix(s, "-")
	s = strings.TrimPrefix(s, "-")
	intPart, fracPart := s, ""
	if i := strings.IndexByte(s, '.'); i >= 0 {
		intPart, fracPart = s[:i], s[i+1:]
	}
	if intPart == "" || strings.IndexFunc(intPart+fracPart, func(r rune) bool { return r < '0' || r > '9' }) >= 0 {
		return nil, fmt.Errorf("non-canonical decimal %q", s)
	}
	dscale := len(fracPart)

	// Left-pad the integer part and right-pad the fraction to whole
	// base-10000 groups.
	for len(intPart)%4 != 0 {
		intPart = "0" + intPart
	}
	for len(fracPart)%4 != 0 {
		fracPart += "0"
	}
	var groups []uint16
	for i := 0; i < len(intPart); i += 4 {
		groups = append(groups, toGroup(intPart[i:i+4]))
	}
	weight := len(groups) - 1
	for i := 0; i < len(fracPart); i += 4 {
		groups = append(groups, toGroup(fracPart[i:i+4]))
	}
	// Trim leading zero groups (adjusting weight) and trailing zero groups.
	for len(groups) > 0 && groups[0] == 0 {
		groups = groups[1:]
		weight--
	}
	for len(groups) > 0 && groups[len(groups)-1] == 0 {
		groups = groups[:len(groups)-1]
	}
	sign := uint16(pgNumericPos)
	if neg && len(groups) > 0 {
		sign = pgNumericNeg
	}
	if len(groups) == 0 {
		weight = 0
	}
	if weight > pgNumericMaxWeight || weight < -pgNumericMaxWeight || dscale > pgNumericMaxDigits {
		return nil, fmt.Errorf("decimal %q exceeds the wire format's bounds (%d digits of integer part or scale)", s, pgNumericMaxDigits)
	}
	out := make([]byte, 0, 8+2*len(groups))
	out = binary.BigEndian.AppendUint16(out, uint16(len(groups)))
	out = binary.BigEndian.AppendUint16(out, uint16(int16(weight)))
	out = binary.BigEndian.AppendUint16(out, sign)
	out = binary.BigEndian.AppendUint16(out, uint16(dscale))
	for _, g := range groups {
		out = binary.BigEndian.AppendUint16(out, g)
	}
	return out, nil
}

func toGroup(four string) uint16 {
	var v uint16
	for i := 0; i < 4; i++ {
		v = v*10 + uint16(four[i]-'0')
	}
	return v
}

// decodePGNumeric converts the binary wire format to a plain decimal
// string (not yet canonicalized — callers run it through types.ParseDecimal).
func decodePGNumeric(raw []byte) (string, error) {
	if len(raw) < 8 {
		return "", fmt.Errorf("bad binary numeric length %d", len(raw))
	}
	ndigits := int(binary.BigEndian.Uint16(raw[0:2]))
	weight := int(int16(binary.BigEndian.Uint16(raw[2:4])))
	sign := binary.BigEndian.Uint16(raw[4:6])
	dscale := int(binary.BigEndian.Uint16(raw[6:8]))
	if len(raw) != 8+2*ndigits {
		return "", fmt.Errorf("binary numeric length %d does not match ndigits %d", len(raw), ndigits)
	}
	switch sign {
	case pgNumericPos, pgNumericNeg:
	default:
		return "", fmt.Errorf("unsupported numeric sign 0x%04x (NaN?)", sign)
	}
	if weight > pgNumericMaxWeight || weight < -pgNumericMaxWeight {
		return "", fmt.Errorf("binary numeric weight %d out of range (at most %d digits of integer part)", weight, pgNumericMaxDigits)
	}
	if dscale > pgNumericMaxDigits {
		return "", fmt.Errorf("binary numeric scale %d out of range (at most %d)", dscale, pgNumericMaxDigits)
	}
	group := func(i int) int {
		if i < 0 || i >= ndigits {
			return 0
		}
		return int(binary.BigEndian.Uint16(raw[8+2*i : 10+2*i]))
	}

	var b strings.Builder
	if sign == pgNumericNeg {
		b.WriteByte('-')
	}
	// Integer part: groups 0..weight (weight < 0 means no integer part).
	if weight < 0 {
		b.WriteByte('0')
	} else {
		for i := 0; i <= weight; i++ {
			if i == 0 {
				fmt.Fprintf(&b, "%d", group(i))
			} else {
				fmt.Fprintf(&b, "%04d", group(i))
			}
		}
	}
	if dscale > 0 {
		b.WriteByte('.')
		// Fraction: groups weight+1 onward, 4 digits each, cut to dscale.
		written := 0
		for gi := weight + 1; written < dscale; gi++ {
			four := fmt.Sprintf("%04d", group(gi))
			take := dscale - written
			if take > 4 {
				take = 4
			}
			b.WriteString(four[:take])
			written += take
		}
	}
	return b.String(), nil
}
