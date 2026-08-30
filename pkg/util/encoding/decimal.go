package encoding

import (
	"fmt"

	"github.com/sthorne/datax/pkg/util/decimal"
)

// Decimal keys are order-preserving and self-delimiting. The value is
// normalized to scientific form ±0.digits × 10^E (Mantissa) and encoded as
//
//	marker | E (EncodeInt64, 8 bytes) | digit bytes (d+1 each) | 0x00
//
// with marker 0x14 for negative, 0x15 for zero, 0x16 for positive. For
// positives, a larger E means a larger value and, at equal E, the digit
// bytes compare lexicographically with the 0x00 terminator sorting below
// any digit byte (prefix property: 0.5 < 0.55). For negatives every byte
// after the marker is bitwise complemented, which exactly reverses the
// order (terminator becomes 0xFF, sorting above), and the marker itself
// puts all negatives below zero below all positives. -0 normalizes to
// zero in decimal.Parse, so it cannot reach the encoder distinct from 0.

const (
	decMarkerNeg  byte = 0x14
	decMarkerZero byte = 0x15
	decMarkerPos  byte = 0x16
	decTerminator byte = 0x00
)

// EncodeDecimal appends the order-preserving encoding of d.
func EncodeDecimal(b []byte, d decimal.Dec) []byte {
	neg, digits, e := d.Mantissa()
	if digits == "" {
		return append(b, decMarkerZero)
	}
	marker := decMarkerPos
	if neg {
		marker = decMarkerNeg
	}
	b = append(b, marker)
	start := len(b)
	b = EncodeInt64(b, e)
	for i := 0; i < len(digits); i++ {
		b = append(b, digits[i]-'0'+1)
	}
	b = append(b, decTerminator)
	if neg {
		for i := start; i < len(b); i++ {
			b[i] = ^b[i]
		}
	}
	return b
}

// DecodeDecimal consumes one encoded decimal from b.
func DecodeDecimal(b []byte) (rest []byte, d decimal.Dec, err error) {
	if len(b) == 0 {
		return nil, decimal.Dec{}, fmt.Errorf("decode decimal: empty input")
	}
	marker := b[0]
	b = b[1:]
	if marker == decMarkerZero {
		return b, decimal.New(0, 0), nil
	}
	neg := marker == decMarkerNeg
	if !neg && marker != decMarkerPos {
		return nil, decimal.Dec{}, fmt.Errorf("decode decimal: bad marker 0x%02x", marker)
	}
	flip := func(c byte) byte {
		if neg {
			return ^c
		}
		return c
	}
	if len(b) < 8 {
		return nil, decimal.Dec{}, fmt.Errorf("decode decimal: truncated exponent")
	}
	expBytes := make([]byte, 8)
	for i := 0; i < 8; i++ {
		expBytes[i] = flip(b[i])
	}
	_, e, err := DecodeInt64(expBytes)
	if err != nil {
		return nil, decimal.Dec{}, err
	}
	b = b[8:]
	var digits []byte
	for {
		if len(b) == 0 {
			return nil, decimal.Dec{}, fmt.Errorf("decode decimal: unterminated digits")
		}
		c := flip(b[0])
		b = b[1:]
		if c == decTerminator {
			break
		}
		if c < 1 || c > 10 {
			return nil, decimal.Dec{}, fmt.Errorf("decode decimal: bad digit byte 0x%02x", c)
		}
		digits = append(digits, c-1+'0')
	}
	d, err = decimal.FromMantissa(neg, string(digits), e)
	if err != nil {
		return nil, decimal.Dec{}, err
	}
	return b, d, nil
}
