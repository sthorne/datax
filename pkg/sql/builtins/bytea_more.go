package builtins

import (
	"crypto/sha256"
	"crypto/sha512"
	"hash/crc32"

	"github.com/sthorne/datax/pkg/sql/types"
)

// The binary-string functions of PG-F-009: the SHA-2 hashes besides
// sha256, crc32 / crc32c (PostgreSQL 18), and the byte and bit
// accessors.

// CodeArraySubscript is array_subscript_error (get_byte out of range).
const CodeArraySubscript = "2202E"

var crc32cTable = crc32.MakeTable(crc32.Castagnoli)

// byteIndex checks a byte or bit index against a value of n units.
func byteIndex(i int64, n int) error {
	if i < 0 || i >= int64(n) {
		return errf(CodeArraySubscript, "index %d out of valid range, 0..%d", i, n-1)
	}
	return nil
}

func init() {
	hash := func(name, doc string, sum func([]byte) []byte) {
		register(&Builtin{Name: name, Args: []types.Family{Any}, MinArgs: 1, Ret: types.Bytes, Category: catString, Doc: doc,
			Fn: func(a []types.Datum) (types.Datum, error) {
				b, err := byteaArg(a[0])
				if err != nil {
					return types.Datum{}, err
				}
				return types.NewBytes(sum(b)), nil
			}})
	}
	hash("sha224", "The SHA-224 hash, as bytes.", func(b []byte) []byte { s := sha256.Sum224(b); return s[:] })
	hash("sha384", "The SHA-384 hash, as bytes.", func(b []byte) []byte { s := sha512.Sum384(b); return s[:] })
	hash("sha512", "The SHA-512 hash, as bytes.", func(b []byte) []byte { s := sha512.Sum512(b); return s[:] })
	crc := func(name, doc string, table *crc32.Table) {
		register(&Builtin{Name: name, Args: []types.Family{Any}, MinArgs: 1, Ret: types.Int, Category: catString, Doc: doc,
			Fn: func(a []types.Datum) (types.Datum, error) {
				b, err := byteaArg(a[0])
				if err != nil {
					return types.Datum{}, err
				}
				return i64(int64(crc32.Checksum(b, table))), nil
			}})
	}
	crc("crc32", "The CRC-32 checksum of the bytes.", crc32.IEEETable)
	crc("crc32c", "The CRC-32C (Castagnoli) checksum of the bytes.", crc32cTable)
	register(&Builtin{Name: "get_byte", Args: []types.Family{Any, types.Int}, MinArgs: 2, Ret: types.Int, Category: catString,
		Doc: "The byte at the 0-based offset, as an integer.",
		Fn: func(a []types.Datum) (types.Datum, error) {
			b, err := byteaArg(a[0])
			if err != nil {
				return types.Datum{}, err
			}
			if err := byteIndex(a[1].I, len(b)); err != nil {
				return types.Datum{}, err
			}
			return i64(int64(b[a[1].I])), nil
		}})
	register(&Builtin{Name: "set_byte", Args: []types.Family{Any, types.Int, types.Int}, MinArgs: 3, Ret: types.Bytes, Category: catString,
		Doc: "The bytes with the one at the 0-based offset set to the value (its low 8 bits).",
		Fn: func(a []types.Datum) (types.Datum, error) {
			b, err := byteaArg(a[0])
			if err != nil {
				return types.Datum{}, err
			}
			if err := byteIndex(a[1].I, len(b)); err != nil {
				return types.Datum{}, err
			}
			out := append([]byte(nil), b...)
			out[a[1].I] = byte(a[2].I)
			return types.NewBytes(out), nil
		}})
	register(&Builtin{Name: "get_bit", Args: []types.Family{Any, types.Int}, MinArgs: 2, Ret: types.Int, Category: catString,
		Doc: "The bit at the 0-based offset (bit n is bit n mod 8 of byte n / 8, from the least significant).",
		Fn: func(a []types.Datum) (types.Datum, error) {
			b, err := byteaArg(a[0])
			if err != nil {
				return types.Datum{}, err
			}
			n := a[1].I
			if err := byteIndex(n, len(b)*8); err != nil {
				return types.Datum{}, err
			}
			return i64(int64(b[n/8]>>(n%8)) & 1), nil
		}})
	register(&Builtin{Name: "set_bit", Args: []types.Family{Any, types.Int, types.Int}, MinArgs: 3, Ret: types.Bytes, Category: catString,
		Doc: "The bytes with the bit at the 0-based offset set to 0 or 1.",
		Fn: func(a []types.Datum) (types.Datum, error) {
			b, err := byteaArg(a[0])
			if err != nil {
				return types.Datum{}, err
			}
			n := a[1].I
			if err := byteIndex(n, len(b)*8); err != nil {
				return types.Datum{}, err
			}
			if a[2].I != 0 && a[2].I != 1 {
				return types.Datum{}, errf(CodeInvalidArgument, "new bit must be 0 or 1")
			}
			out := append([]byte(nil), b...)
			if a[2].I == 1 {
				out[n/8] |= 1 << (n % 8)
			} else {
				out[n/8] &^= 1 << (n % 8)
			}
			return types.NewBytes(out), nil
		}})
}
