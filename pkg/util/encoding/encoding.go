// Package encoding provides order-preserving binary encodings: for any two
// values a < b of the same type, Encode(a) sorts before Encode(b) under
// bytes.Compare. These encodings are the foundation of datax keys, so their
// ordering property is load-bearing and covered by property tests.
package encoding

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"slices"
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

// Uvarint is a plain (non-order-preserving) varint, for value encodings
// where compactness matters and ordering does not.

func EncodeUvarint(b []byte, v uint64) []byte {
	return binary.AppendUvarint(b, v)
}

func DecodeUvarint(b []byte) (rest []byte, v uint64, err error) {
	v, n := binary.Uvarint(b)
	if n <= 0 {
		return nil, 0, fmt.Errorf("decode uvarint: malformed input")
	}
	return b[n:], v, nil
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
	// Sized up front (issue #163): the encoding is the data plus one byte
	// per NUL plus the terminator, so one growth step covers it instead
	// of a chain of appends from an empty slice.
	b = slices.Grow(b, EncodedBytesLen(data))
	for _, c := range data {
		if c == escape {
			b = append(b, escape, escapedNul)
		} else {
			b = append(b, c)
		}
	}
	return append(b, escape, terminator)
}

// EncodedBytesLen is the length EncodeBytes appends for data.
func EncodedBytesLen(data []byte) int {
	return len(data) + 2 + bytes.Count(data, []byte{escape})
}

func DecodeBytes(b []byte) (rest []byte, data []byte, err error) {
	// One pass finds the terminator and counts escaped NULs, so the
	// decoded bytes are allocated once at their exact size (issue #163);
	// the common key without a NUL is then a single copy.
	end, escapes := -1, 0
	for i := 0; i < len(b) && end < 0; i++ {
		if b[i] != escape {
			continue
		}
		if i+1 >= len(b) {
			return nil, nil, fmt.Errorf("decode bytes: truncated escape")
		}
		switch b[i+1] {
		case escapedNul:
			escapes++
			i++
		case terminator:
			end = i
		default:
			return nil, nil, fmt.Errorf("decode bytes: invalid escape 0x00 0x%02x", b[i+1])
		}
	}
	if end < 0 {
		return nil, nil, fmt.Errorf("decode bytes: missing terminator")
	}
	data = make([]byte, end-escapes)
	if escapes == 0 {
		copy(data, b[:end])
		return b[end+2:], data, nil
	}
	for i, j := 0, 0; i < end; i, j = i+1, j+1 {
		data[j] = b[i] // an escaped NUL decodes to its first byte, 0x00
		if b[i] == escape {
			i++
		}
	}
	return b[end+2:], data, nil
}

func EncodeString(b []byte, s string) []byte {
	return EncodeBytes(b, []byte(s))
}

func DecodeString(b []byte) (rest []byte, s string, err error) {
	rest, data, err := DecodeBytes(b)
	return rest, string(data), err
}
