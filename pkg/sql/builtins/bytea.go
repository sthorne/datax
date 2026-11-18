package builtins

import (
	"bytes"

	"github.com/sthorne/datax/pkg/sql/types"
)

// btrim, ltrim and rtrim over bytea (PG-T-003); the byte and bit
// accessors and the encoding conversions are in bytea_more.go and
// strings_more.go.

// trimBytea is btrim, ltrim and rtrim over bytea: the bytes of the
// second argument removed from the chosen ends of the first.
func trimBytea(name string, a []types.Datum, left, right bool) (types.Datum, error) {
	if len(a) < 2 {
		return types.Datum{}, errf(CodeUndefined, "function %s(bytea) does not exist", name)
	}
	s, err := byteaArg(a[0])
	if err != nil {
		return types.Datum{}, err
	}
	set, err := byteaArg(a[1])
	if err != nil {
		return types.Datum{}, err
	}
	return types.NewBytes(trimBytesExact(s, set, left, right)), nil
}

// trimBytesExact trims bytes, not runes (bytes.Trim reads its cutset
// as UTF-8, which a cutset of bytes above 0x7f is not).
func trimBytesExact(s, set []byte, left, right bool) []byte {
	in := func(c byte) bool { return bytes.IndexByte(set, c) >= 0 }
	lo, hi := 0, len(s)
	if left {
		for lo < hi && in(s[lo]) {
			lo++
		}
	}
	if right {
		for hi > lo && in(s[hi-1]) {
			hi--
		}
	}
	return s[lo:hi]
}
