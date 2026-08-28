// Package encoding provides order-preserving binary encodings: for any two
// values a < b of the same type, Encode(a) sorts before Encode(b) under
// bytes.Compare. These encodings are the foundation of datax keys, so their
// ordering property is load-bearing and covered by property tests.
package encoding

import (
	"encoding/binary"
	"fmt"
	"math"
)

// Uint64 is encoded as 8 bytes big-endian: fixed width, naturally ordered.

func EncodeUint64(b []byte, v uint64) []byte {
	return binary.BigEndian.AppendUint64(b, v)
}

func DecodeUint64(b []byte) (rest []byte, v uint64, err error) {
	if len(b) < 8 {
		return nil, 0, fmt.Errorf("decode uint64: need 8 bytes, have %d", len(b))
	}
	return b[8:], binary.BigEndian.Uint64(b[:8]), nil
}

// Int64 flips the sign bit so negative values sort before positive ones.

func EncodeInt64(b []byte, v int64) []byte {
	return binary.BigEndian.AppendUint64(b, uint64(v)^(1<<63))
}

func DecodeInt64(b []byte) (rest []byte, v int64, err error) {
	rest, u, err := DecodeUint64(b)
	if err != nil {
		return nil, 0, fmt.Errorf("decode int64: %w", err)
	}
	return rest, int64(u ^ (1 << 63)), nil
}

// Float64 uses the standard IEEE-754 total-order trick: positive floats get
// the sign bit set; negative floats are bitwise inverted. NaN is rejected at
// a higher layer (SQL) and encodes after +Inf here.

func EncodeFloat64(b []byte, v float64) []byte {
	u := math.Float64bits(v)
	if u&(1<<63) != 0 {
		u = ^u
	} else {
		u |= 1 << 63
	}
	return binary.BigEndian.AppendUint64(b, u)
}

func DecodeFloat64(b []byte) (rest []byte, v float64, err error) {
	rest, u, err := DecodeUint64(b)
	if err != nil {
		return nil, 0, fmt.Errorf("decode float64: %w", err)
	}
	if u&(1<<63) != 0 {
		u &^= 1 << 63
	} else {
		u = ^u
	}
	return rest, math.Float64frombits(u), nil
}

// Bool encodes as one byte, false < true.

func EncodeBool(b []byte, v bool) []byte {
	if v {
		return append(b, 1)
	}
	return append(b, 0)
}

func DecodeBool(b []byte) (rest []byte, v bool, err error) {
	if len(b) < 1 {
		return nil, false, fmt.Errorf("decode bool: empty input")
	}
	return b[1:], b[0] != 0, nil
}

// Bytes are escaped so that arbitrary content (including 0x00) is
// order-preserving and self-terminating:
//
//	0x00        → 0x00 0xff
//	terminator  → 0x00 0x01
//
// The terminator (0x01) sorts below the escape continuation (0xff), which
// gives the prefix property: Encode("a") < Encode("a\x00...").

const (
	escape     byte = 0x00
	escapedNul byte = 0xff
	terminator byte = 0x01
)

func EncodeBytes(b []byte, data []byte) []byte {
	for _, c := range data {
		if c == escape {
			b = append(b, escape, escapedNul)
		} else {
			b = append(b, c)
		}
	}
	return append(b, escape, terminator)
}

func DecodeBytes(b []byte) (rest []byte, data []byte, err error) {
	for i := 0; i < len(b); {
		c := b[i]
		if c != escape {
			data = append(data, c)
			i++
			continue
		}
		if i+1 >= len(b) {
			return nil, nil, fmt.Errorf("decode bytes: truncated escape")
		}
		switch b[i+1] {
		case escapedNul:
			data = append(data, 0x00)
			i += 2
		case terminator:
			return b[i+2:], data, nil
		default:
			return nil, nil, fmt.Errorf("decode bytes: invalid escape 0x00 0x%02x", b[i+1])
		}
	}
	return nil, nil, fmt.Errorf("decode bytes: missing terminator")
}

func EncodeString(b []byte, s string) []byte {
	return EncodeBytes(b, []byte(s))
}

func DecodeString(b []byte) (rest []byte, s string, err error) {
	rest, data, err := DecodeBytes(b)
	return rest, string(data), err
}
