package builtins

import (
	"bytes"
	"fmt"
	"strings"
	"unicode/utf8"

	"github.com/sthorne/datax/pkg/sql/types"
)

// The bytea functions beyond length, substring, position and md5
// (PG-T-003): byte and bit access, trimming, and conversion between
// bytes and text in an encoding.

// CodeCharacterNotInRepertoire is 22021, an invalid byte sequence for
// the encoding.
const CodeCharacterNotInRepertoire = "22021"

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

// byteIndexArg checks a byte or bit index against the valid range.
func byteIndexArg(n, limit int64) error {
	if n < 0 || n >= limit {
		return errf(CodeArraySubscript, "index %d out of valid range, 0..%d", n, limit-1)
	}
	return nil
}

// encodingName canonicalizes an encoding name as PostgreSQL matches
// them (case and punctuation ignored); ok is false for one datax does
// not convert.
func encodingName(s string) (string, bool) {
	k := strings.Map(func(r rune) rune {
		if r == '_' || r == '-' || r == ' ' {
			return -1
		}
		return r
	}, strings.ToUpper(s))
	switch k {
	case "UTF8", "UNICODE":
		return "UTF8", true
	case "LATIN1", "ISO88591":
		return "LATIN1", true
	case "SQLASCII":
		return "SQL_ASCII", true
	}
	return "", false
}

// decodeAs reads bytes in an encoding into UTF-8 text.
func decodeAs(b []byte, enc string) (string, error) {
	switch enc {
	case "LATIN1":
		var sb strings.Builder
		for _, c := range b {
			sb.WriteRune(rune(c))
		}
		return sb.String(), nil
	case "SQL_ASCII":
		return string(b), nil
	}
	for i := 0; i < len(b); {
		r, n := utf8.DecodeRune(b[i:])
		if r == utf8.RuneError && n <= 1 || b[i] == 0 {
			return "", errf(CodeCharacterNotInRepertoire, "invalid byte sequence for encoding \"UTF8\": %s", hexBytes(b[i:min(len(b), i+2)]))
		}
		i += n
	}
	return string(b), nil
}

// encodeAs writes UTF-8 text as bytes in an encoding.
func encodeAs(s string, enc string) ([]byte, error) {
	if enc != "LATIN1" {
		return []byte(s), nil
	}
	out := make([]byte, 0, len(s))
	for _, r := range s {
		if r > 0xff {
			return nil, errf(CodeUntranslatable, "character with byte sequence %s in encoding \"UTF8\" has no equivalent in encoding \"LATIN1\"", hexBytes([]byte(string(r))))
		}
		out = append(out, byte(r))
	}
	return out, nil
}

// CodeUntranslatable is 22P05, a character with no equivalent in the
// target encoding.
const CodeUntranslatable = "22P05"

func hexBytes(b []byte) string {
	parts := make([]string, len(b))
	for i, c := range b {
		parts[i] = fmt.Sprintf("0x%02x", c)
	}
	return strings.Join(parts, " ")
}

func init() {
	b, i, t := types.Bytes, types.Int, types.String
	register(&Builtin{Name: "get_byte", Args: []types.Family{b, i}, MinArgs: 2, Ret: i, Category: catString,
		Doc: "The n-th byte (0-based) of a bytea, 0 to 255 (2202E out of range).",
		Fn: func(a []types.Datum) (types.Datum, error) {
			if err := byteIndexArg(a[1].I, int64(len(a[0].S))); err != nil {
				return types.Datum{}, err
			}
			return i64(int64(a[0].S[a[1].I])), nil
		}})
	register(&Builtin{Name: "set_byte", Args: []types.Family{b, i, i}, MinArgs: 3, Ret: b, Category: catString,
		Doc: "The bytea with its n-th byte (0-based) set to the value's low 8 bits.",
		Fn: func(a []types.Datum) (types.Datum, error) {
			if err := byteIndexArg(a[1].I, int64(len(a[0].S))); err != nil {
				return types.Datum{}, err
			}
			out := []byte(a[0].S)
			out[a[1].I] = byte(a[2].I)
			return types.NewBytes(out), nil
		}})
	register(&Builtin{Name: "get_bit", Args: []types.Family{b, i}, MinArgs: 2, Ret: i, Category: catString,
		Doc: "The n-th bit (0-based, least significant first within each byte) of a bytea: 0 or 1.",
		Fn: func(a []types.Datum) (types.Datum, error) {
			n := a[1].I
			if err := byteIndexArg(n, int64(len(a[0].S))*8); err != nil {
				return types.Datum{}, err
			}
			return i64(int64(a[0].S[n/8]>>(n%8)) & 1), nil
		}})
	register(&Builtin{Name: "set_bit", Args: []types.Family{b, i, i}, MinArgs: 3, Ret: b, Category: catString,
		Doc: "The bytea with its n-th bit (as get_bit numbers them) set to 0 or 1.",
		Fn: func(a []types.Datum) (types.Datum, error) {
			n := a[1].I
			if err := byteIndexArg(n, int64(len(a[0].S))*8); err != nil {
				return types.Datum{}, err
			}
			if a[2].I != 0 && a[2].I != 1 {
				return types.Datum{}, errf(CodeInvalidArgument, "new bit must be 0 or 1")
			}
			out := []byte(a[0].S)
			if a[2].I == 1 {
				out[n/8] |= 1 << (n % 8)
			} else {
				out[n/8] &^= 1 << (n % 8)
			}
			return types.NewBytes(out), nil
		}})
	register(&Builtin{Name: "bit_length", Args: []types.Family{t}, MinArgs: 1, Ret: i, Category: catString, Bytea: true,
		Doc: "The number of bits in the string's bytes (or a bytea's): octet_length × 8.",
		Fn:  func(a []types.Datum) (types.Datum, error) { return i64(int64(len(a[0].S)) * 8), nil }})
	register(&Builtin{Name: "convert_from", Args: []types.Family{b, t}, MinArgs: 2, Ret: t, Category: catString,
		Doc: "The bytes read as text in the named encoding (UTF8, LATIN1 or SQL_ASCII); 22021 for bytes invalid in it.",
		Fn: func(a []types.Datum) (types.Datum, error) {
			enc, ok := encodingName(a[1].S)
			if !ok {
				return types.Datum{}, errf(CodeInvalidArgument, "invalid source encoding name \"%s\"", a[1].S)
			}
			s, err := decodeAs([]byte(a[0].S), enc)
			if err != nil {
				return types.Datum{}, err
			}
			return str(s), nil
		}})
	register(&Builtin{Name: "convert_to", Args: []types.Family{t, t}, MinArgs: 2, Ret: b, Category: catString,
		Doc: "The text's bytes in the named encoding (UTF8, LATIN1 or SQL_ASCII).",
		Fn: func(a []types.Datum) (types.Datum, error) {
			enc, ok := encodingName(a[1].S)
			if !ok {
				return types.Datum{}, errf(CodeInvalidArgument, "invalid destination encoding name \"%s\"", a[1].S)
			}
			out, err := encodeAs(a[0].S, enc)
			if err != nil {
				return types.Datum{}, err
			}
			return types.NewBytes(out), nil
		}})
	register(&Builtin{Name: "convert", Args: []types.Family{b, t, t}, MinArgs: 3, Ret: b, Category: catString,
		Doc: "The bytes converted from the first named encoding to the second.",
		Fn: func(a []types.Datum) (types.Datum, error) {
			src, ok := encodingName(a[1].S)
			if !ok {
				return types.Datum{}, errf(CodeInvalidArgument, "invalid source encoding name \"%s\"", a[1].S)
			}
			dst, ok := encodingName(a[2].S)
			if !ok {
				return types.Datum{}, errf(CodeInvalidArgument, "invalid destination encoding name \"%s\"", a[2].S)
			}
			s, err := decodeAs([]byte(a[0].S), src)
			if err != nil {
				return types.Datum{}, err
			}
			out, err := encodeAs(s, dst)
			if err != nil {
				return types.Datum{}, err
			}
			return types.NewBytes(out), nil
		}})
}
