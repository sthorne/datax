package builtins

import (
	"bytes"
	"crypto/md5"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"math"
	"strconv"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/sthorne/datax/pkg/sql/types"
)

const catString = "Strings"

func str(s string) types.Datum { return types.NewString(s) }
func i64(i int64) types.Datum  { return types.NewInt(i) }

// text renders any datum as text, the way an implicit cast to text
// would (concat, format and friends accept anything).
func text(d types.Datum) string {
	if d.Null {
		return ""
	}
	return d.Text()
}

func init() {
	register(&Builtin{Name: "length", Args: []types.Family{types.String}, MinArgs: 1, Ret: types.Int, Category: catString,
		Doc: "Number of characters in the string (of bytes in a bytea).", Aliases: []string{"char_length", "character_length"},
		Bytea: true,
		Fn: func(a []types.Datum) (types.Datum, error) {
			if a[0].Fam == types.Bytes {
				return i64(int64(len(a[0].S))), nil
			}
			return i64(int64(utf8.RuneCountInString(a[0].S))), nil
		}})
	register(&Builtin{Name: "octet_length", Args: []types.Family{types.String}, MinArgs: 1, Ret: types.Int, Category: catString,
		Doc: "Number of bytes in the string or bytea.", Bytea: true,
		Fn: func(a []types.Datum) (types.Datum, error) { return i64(int64(len(a[0].S))), nil }})
	register(&Builtin{Name: "lower", Args: []types.Family{types.String}, MinArgs: 1, Ret: types.String, Category: catString,
		Doc: "The string in lower case.",
		Fn:  func(a []types.Datum) (types.Datum, error) { return str(strings.ToLower(a[0].S)), nil }})
	register(&Builtin{Name: "upper", Args: []types.Family{types.String}, MinArgs: 1, Ret: types.String, Category: catString,
		Doc: "The string in upper case.",
		Fn:  func(a []types.Datum) (types.Datum, error) { return str(strings.ToUpper(a[0].S)), nil }})
	register(&Builtin{Name: "initcap", Args: []types.Family{types.String}, MinArgs: 1, Ret: types.String, Category: catString,
		Doc: "The first letter of each word upper-cased, the rest lower-cased.",
		Fn: func(a []types.Datum) (types.Datum, error) {
			var b strings.Builder
			inWord := false
			for _, r := range a[0].S {
				if unicode.IsLetter(r) || unicode.IsDigit(r) {
					if inWord {
						b.WriteRune(unicode.ToLower(r))
					} else {
						b.WriteRune(unicode.ToUpper(r))
					}
					inWord = true
				} else {
					b.WriteRune(r)
					inWord = false
				}
			}
			return str(b.String()), nil
		}})
	register(&Builtin{Name: "concat", Args: []types.Family{Any}, MinArgs: 1, Variadic: true, Ret: types.String, NotStrict: true, Category: catString,
		Doc: "Concatenates the arguments as text; NULLs are skipped.",
		Fn: func(a []types.Datum) (types.Datum, error) {
			var b strings.Builder
			for _, d := range a {
				b.WriteString(text(d))
			}
			return str(b.String()), nil
		}})
	register(&Builtin{Name: "concat_ws", Args: []types.Family{types.String, Any}, MinArgs: 2, Variadic: true, Ret: types.String, NotStrict: true, Category: catString,
		Doc: "Concatenates the arguments after the first with it as the separator; NULLs are skipped.",
		Fn: func(a []types.Datum) (types.Datum, error) {
			if a[0].Null {
				return types.DNull, nil
			}
			var parts []string
			for _, d := range a[1:] {
				if !d.Null {
					parts = append(parts, text(d))
				}
			}
			return str(strings.Join(parts, a[0].S)), nil
		}})
	register(&Builtin{Name: "substring", Args: []types.Family{types.String, types.Int, types.Int}, MinArgs: 2, Ret: types.String, Category: catString,
		Doc: "The substring starting at a 1-based position, optionally of a given length (also substring(s FROM n [FOR m])); over a bytea, the bytes. With a text pattern in place of the position, substring(s FROM pattern) is the part the first match of the POSIX pattern covers (of its first parenthesized subexpression, if any), and substring(s SIMILAR pattern ESCAPE e) (or substring(s FROM pattern FOR e)) the part between the escape-double-quote separators of a SIMILAR TO pattern matching the whole string; NULL when it does not match.", Aliases: []string{"substr"},
		Bytea: true, RetOf: stringOrBytes, Alt: substringPattern,
		Fn: func(a []types.Datum) (types.Datum, error) {
			if a[0].Fam == types.Bytes {
				from, to, err := substringWindow(a, int64(len(a[0].S)))
				if err != nil {
					return types.Datum{}, err
				}
				return types.NewBytes([]byte(a[0].S[from-1 : to-1])), nil
			}
			runes := []rune(a[0].S)
			from, to, err := substringWindow(a, int64(len(runes)))
			if err != nil {
				return types.Datum{}, err
			}
			return str(string(runes[from-1 : to-1])), nil
		}})
	register(&Builtin{Name: "left", Args: []types.Family{types.String, types.Int}, MinArgs: 2, Ret: types.String, Category: catString,
		Doc: "The first n characters (all but the last -n when negative).",
		Fn: func(a []types.Datum) (types.Datum, error) {
			r := []rune(a[0].S)
			n := a[1].I
			if n < 0 {
				n = int64(len(r)) + n
			}
			n = clamp(n, 0, int64(len(r)))
			return str(string(r[:n])), nil
		}})
	register(&Builtin{Name: "right", Args: []types.Family{types.String, types.Int}, MinArgs: 2, Ret: types.String, Category: catString,
		Doc: "The last n characters (all but the first -n when negative).",
		Fn: func(a []types.Datum) (types.Datum, error) {
			r := []rune(a[0].S)
			n := a[1].I
			if n < 0 {
				n = int64(len(r)) + n
			}
			n = clamp(n, 0, int64(len(r)))
			return str(string(r[int64(len(r))-n:])), nil
		}})
	register(&Builtin{Name: "position", Args: []types.Family{types.String, types.String}, MinArgs: 2, Ret: types.Int, Category: catString,
		Doc:   "1-based position of the first argument's first occurrence in the second, 0 when absent (also position(needle IN haystack); over bytea, the byte offset); strpos(haystack, needle) takes them the other way round.",
		Bytea: true,
		Fn: func(a []types.Datum) (types.Datum, error) {
			if bytea(a) {
				return byteaIndex(a[1], a[0])
			}
			return i64(runeIndex(a[1].S, a[0].S)), nil
		}})
	register(&Builtin{Name: "strpos", Args: []types.Family{types.String, types.String}, MinArgs: 2, Ret: types.Int, Category: catString, Hidden: true,
		Doc:   "1-based position of the second argument in the first, 0 when absent.",
		Bytea: true,
		Fn: func(a []types.Datum) (types.Datum, error) {
			if bytea(a) {
				return byteaIndex(a[0], a[1])
			}
			return i64(runeIndex(a[0].S, a[1].S)), nil
		}})
	register(&Builtin{Name: "replace", Args: []types.Family{types.String, types.String, types.String}, MinArgs: 3, Ret: types.String, Category: catString,
		Doc: "Replaces every occurrence of the second argument with the third.",
		Fn: func(a []types.Datum) (types.Datum, error) {
			return str(strings.ReplaceAll(a[0].S, a[1].S, a[2].S)), nil
		}})
	register(&Builtin{Name: "trim", Args: []types.Family{types.String, types.String}, MinArgs: 1, Ret: types.String, Category: catString,
		Doc: "Removes the given characters (spaces by default) from both ends (also trim([BOTH | LEADING | TRAILING] [chars] FROM s)).", Aliases: []string{"btrim"},
		Fn: func(a []types.Datum) (types.Datum, error) { return str(strings.Trim(a[0].S, cutset(a))), nil }})
	register(&Builtin{Name: "ltrim", Args: []types.Family{types.String, types.String}, MinArgs: 1, Ret: types.String, Category: catString,
		Doc: "Removes the given characters (spaces by default) from the start.",
		Fn:  func(a []types.Datum) (types.Datum, error) { return str(strings.TrimLeft(a[0].S, cutset(a))), nil }})
	register(&Builtin{Name: "rtrim", Args: []types.Family{types.String, types.String}, MinArgs: 1, Ret: types.String, Category: catString,
		Doc: "Removes the given characters (spaces by default) from the end.",
		Fn:  func(a []types.Datum) (types.Datum, error) { return str(strings.TrimRight(a[0].S, cutset(a))), nil }})
	register(&Builtin{Name: "lpad", Args: []types.Family{types.String, types.Int, types.String}, MinArgs: 2, Ret: types.String, Category: catString,
		Doc: "Pads the string on the left to the length with the fill (spaces by default), truncating when longer.",
		Fn:  func(a []types.Datum) (types.Datum, error) { return str(pad(a, true)), nil }})
	register(&Builtin{Name: "rpad", Args: []types.Family{types.String, types.Int, types.String}, MinArgs: 2, Ret: types.String, Category: catString,
		Doc: "Pads the string on the right to the length with the fill (spaces by default), truncating when longer.",
		Fn:  func(a []types.Datum) (types.Datum, error) { return str(pad(a, false)), nil }})
	register(&Builtin{Name: "repeat", Args: []types.Family{types.String, types.Int}, MinArgs: 2, Ret: types.String, Category: catString,
		Doc: "The string repeated n times.",
		Fn: func(a []types.Datum) (types.Datum, error) {
			if a[1].I <= 0 {
				return str(""), nil
			}
			if int64(len(a[0].S))*a[1].I > 64<<20 {
				return types.Datum{}, errf(CodeStringLength, "requested length too large")
			}
			return str(strings.Repeat(a[0].S, int(a[1].I))), nil
		}})
	register(&Builtin{Name: "reverse", Args: []types.Family{types.String}, MinArgs: 1, Ret: types.String, Category: catString,
		Doc: "The characters in reverse order.",
		Fn: func(a []types.Datum) (types.Datum, error) {
			r := []rune(a[0].S)
			for i, j := 0, len(r)-1; i < j; i, j = i+1, j-1 {
				r[i], r[j] = r[j], r[i]
			}
			return str(string(r)), nil
		}})
	register(&Builtin{Name: "split_part", Args: []types.Family{types.String, types.String, types.Int}, MinArgs: 3, Ret: types.String, Category: catString,
		Doc: "The n-th field (1-based; negative counts from the end) after splitting on the delimiter.",
		Fn: func(a []types.Datum) (types.Datum, error) {
			if a[2].I == 0 {
				return types.Datum{}, errf(CodeInvalidArgument, "field position must not be zero")
			}
			parts := strings.Split(a[0].S, a[1].S)
			n := a[2].I
			if n < 0 {
				n = int64(len(parts)) + n + 1
			}
			if n < 1 || n > int64(len(parts)) {
				return str(""), nil
			}
			return str(parts[n-1]), nil
		}})
	register(&Builtin{Name: "starts_with", Args: []types.Family{types.String, types.String}, MinArgs: 2, Ret: types.Bool, Category: catString,
		Doc: "Whether the string starts with the prefix.",
		Fn: func(a []types.Datum) (types.Datum, error) {
			return types.NewBool(strings.HasPrefix(a[0].S, a[1].S)), nil
		}})
	register(&Builtin{Name: "ascii", Args: []types.Family{types.String}, MinArgs: 1, Ret: types.Int, Category: catString,
		Doc: "The code point of the first character (0 for the empty string).",
		Fn: func(a []types.Datum) (types.Datum, error) {
			if a[0].S == "" {
				return i64(0), nil
			}
			r, _ := utf8.DecodeRuneInString(a[0].S)
			return i64(int64(r)), nil
		}})
	register(&Builtin{Name: "chr", Args: []types.Family{types.Int}, MinArgs: 1, Ret: types.String, Category: catString,
		Doc: "The character with the code point.",
		Fn: func(a []types.Datum) (types.Datum, error) {
			if a[0].I <= 0 || a[0].I > unicode.MaxRune || !utf8.ValidRune(rune(a[0].I)) {
				return types.Datum{}, errf(CodeInvalidArgument, "character not in repertoire: %d", a[0].I)
			}
			return str(string(rune(a[0].I))), nil
		}})
	register(&Builtin{Name: "md5", Args: []types.Family{types.String}, MinArgs: 1, Ret: types.String, Category: catString,
		Doc: "The MD5 hash as 32 hex characters (of a bytea's bytes).", Bytea: true,
		Fn: func(a []types.Datum) (types.Datum, error) {
			sum := md5.Sum([]byte(a[0].S))
			return str(hex.EncodeToString(sum[:])), nil
		}})
	register(&Builtin{Name: "sha256", Args: []types.Family{Any}, MinArgs: 1, Ret: types.Bytes, Category: catString,
		Doc: "The SHA-256 hash, as bytes (encode(sha256(x), 'hex') for the hex text).",
		Fn: func(a []types.Datum) (types.Datum, error) {
			sum := sha256.Sum256([]byte(bytesOf(a[0])))
			return types.NewBytes(sum[:]), nil
		}})
	register(&Builtin{Name: "encode", Args: []types.Family{Any, types.String}, MinArgs: 2, Ret: types.String, Category: catString,
		Doc: "Encodes bytes (or text) as 'hex', 'base64' or 'escape' text.",
		Fn: func(a []types.Datum) (types.Datum, error) {
			b := bytesOf(a[0])
			switch strings.ToLower(a[1].S) {
			case "hex":
				return str(hex.EncodeToString(b)), nil
			case "base64":
				return str(base64.StdEncoding.EncodeToString(b)), nil
			case "escape":
				return str(escapeBytes(b)), nil
			}
			return types.Datum{}, errf(CodeInvalidArgument, "unrecognized encoding: %q", a[1].S)
		}})
	register(&Builtin{Name: "decode", Args: []types.Family{types.String, types.String}, MinArgs: 2, Ret: types.Bytes, Category: catString,
		Doc: "Decodes 'hex', 'base64' or 'escape' text into bytes.",
		Fn: func(a []types.Datum) (types.Datum, error) {
			var b []byte
			var err error
			switch strings.ToLower(a[1].S) {
			case "hex":
				b, err = hex.DecodeString(a[0].S)
			case "base64":
				b, err = base64.StdEncoding.DecodeString(strings.Map(func(r rune) rune {
					if r == '\n' || r == '\r' || r == ' ' {
						return -1
					}
					return r
				}, a[0].S))
			case "escape":
				b, err = []byte(a[0].S), nil
			default:
				return types.Datum{}, errf(CodeInvalidArgument, "unrecognized encoding: %q", a[1].S)
			}
			if err != nil {
				return types.Datum{}, errf(CodeInvalidText, "invalid %s input: %v", a[1].S, err)
			}
			return types.NewBytes(b), nil
		}})
	register(&Builtin{Name: "to_hex", Args: []types.Family{types.Int}, MinArgs: 1, Ret: types.String, Category: catString,
		Doc: "The integer in hexadecimal.",
		Fn:  func(a []types.Datum) (types.Datum, error) { return str(strconv.FormatUint(uint64(a[0].I), 16)), nil }})
	register(&Builtin{Name: "format", Args: []types.Family{types.String, Any}, MinArgs: 1, Variadic: true, Ret: types.String, NotStrict: true, Category: catString,
		Doc: "Formats with %s (text), %I (an identifier, quoted when needed), %L (a literal, quoted; NULL for NULL) and %%; a specifier may take an argument position (%2$s), the flag - (left-justify) and a width (%10s, %-10s, %*s, %*3$s).",
		Fn: func(a []types.Datum) (types.Datum, error) {
			if a[0].Null {
				return types.DNull, nil
			}
			return formatSQL(a[0].S, a[1:])
		}})
	register(&Builtin{Name: "quote_literal", Args: []types.Family{Any}, MinArgs: 1, Ret: types.String, Category: catString,
		Doc: "The value as a quoted SQL literal.",
		Fn:  func(a []types.Datum) (types.Datum, error) { return str(quoteLiteral(text(a[0]))), nil }})
	register(&Builtin{Name: "quote_nullable", Args: []types.Family{Any}, MinArgs: 1, Ret: types.String, NotStrict: true, Category: catString,
		Doc: "The value as a quoted SQL literal, or NULL unquoted.",
		Fn: func(a []types.Datum) (types.Datum, error) {
			if a[0].Null {
				return str("NULL"), nil
			}
			return str(quoteLiteral(text(a[0]))), nil
		}})
	register(&Builtin{Name: "translate", Args: []types.Family{types.String, types.String, types.String}, MinArgs: 3, Ret: types.String, Category: catString,
		Doc: "Replaces each character found in the second argument with the one at the same position in the third (dropped when the third is shorter).",
		Fn: func(a []types.Datum) (types.Datum, error) {
			from, to := []rune(a[1].S), []rune(a[2].S)
			var b strings.Builder
			for _, r := range a[0].S {
				idx := -1
				for i, f := range from {
					if f == r {
						idx = i
						break
					}
				}
				switch {
				case idx < 0:
					b.WriteRune(r)
				case idx < len(to):
					b.WriteRune(to[idx])
				}
			}
			return str(b.String()), nil
		}})
}

func clamp(v, lo, hi int64) int64 {
	if v < lo {
		return lo
	}
	if v > hi {
		return hi
	}
	return v
}

// runeIndex is the 1-based character position of needle in s, 0 when
// absent.
// substringWindow is substring's [from, to) in 1-based positions over
// a value of n characters (or bytes): from the start for the length,
// clipped to the value, as PostgreSQL computes it; from == to is empty.
func substringWindow(a []types.Datum, n int64) (from, to int64, err error) {
	start := a[1].I
	length := n + 1
	if len(a) > 2 {
		if a[2].I < 0 {
			return 0, 0, errf(CodeInvalidArgument, "negative substring length not allowed")
		}
		length = a[2].I
	}
	from, to = start, start+length
	if length > 0 && to < start {
		to = n + 1 // start+length overflowed
	}
	if from < 1 {
		from = 1
	}
	if to > n+1 {
		to = n + 1
	}
	if to <= from {
		return 1, 1, nil
	}
	return from, to, nil
}

// stringOrBytes types substring: bytea in, bytea out.
func stringOrBytes(args []types.Family) types.Family {
	if len(args) > 0 && args[0] == types.Bytes {
		return types.Bytes
	}
	return types.String
}

// bytea reports whether a Bytea builtin got a bytea argument: its call
// is then the bytea form.
func bytea(a []types.Datum) bool {
	for _, d := range a {
		if d.Fam == types.Bytes {
			return true
		}
	}
	return false
}

// byteaArg is an argument of a bytea call: a bytea, or text read as
// bytea input (an untyped literal beside a bytea takes its type).
func byteaArg(d types.Datum) ([]byte, error) {
	if d.Fam == types.Bytes {
		return []byte(d.S), nil
	}
	b, err := types.ParseBytes(d.S)
	if err != nil {
		return nil, errf(CodeInvalidText, "invalid input syntax for type bytea")
	}
	return b, nil
}

// byteaIndex is position over bytea: the 1-based byte offset of
// needle's first occurrence in s, 0 when absent.
func byteaIndex(s, needle types.Datum) (types.Datum, error) {
	h, err := byteaArg(s)
	if err != nil {
		return types.Datum{}, err
	}
	n, err := byteaArg(needle)
	if err != nil {
		return types.Datum{}, err
	}
	return i64(int64(bytes.Index(h, n)) + 1), nil
}

func runeIndex(s, needle string) int64 {
	i := strings.Index(s, needle)
	if i < 0 {
		return 0
	}
	return int64(utf8.RuneCountInString(s[:i])) + 1
}

func cutset(a []types.Datum) string {
	if len(a) > 1 {
		return a[1].S
	}
	return " "
}

func pad(a []types.Datum, left bool) string {
	r := []rune(a[0].S)
	n := a[1].I
	fill := []rune(" ")
	if len(a) > 2 {
		fill = []rune(a[2].S)
	}
	if n <= 0 {
		return ""
	}
	if int64(len(r)) >= n {
		return string(r[:n])
	}
	if len(fill) == 0 {
		return string(r)
	}
	padding := make([]rune, 0, n-int64(len(r)))
	for int64(len(padding)) < n-int64(len(r)) {
		padding = append(padding, fill[len(padding)%len(fill)])
	}
	if left {
		return string(padding) + string(r)
	}
	return string(r) + string(padding)
}

func bytesOf(d types.Datum) []byte {
	if d.Fam == types.Bytes {
		return []byte(d.S)
	}
	return []byte(text(d))
}

func escapeBytes(b []byte) string {
	var sb strings.Builder
	for _, c := range b {
		switch {
		case c == '\\':
			sb.WriteString(`\\`)
		case c < 32 || c > 126:
			fmt.Fprintf(&sb, `\%03o`, c)
		default:
			sb.WriteByte(c)
		}
	}
	return sb.String()
}

func quoteLiteral(s string) string {
	return "'" + strings.ReplaceAll(s, "'", "''") + "'"
}

func quoteIdent(s string) string { return QuoteIdent(s) }

// QuoteIdent renders a name as PostgreSQL's quote_identifier does
// (quote_ident, format's %I, pg_get_indexdef, format_type): bare when
// it is lower-case letters, digits and underscores, starting with no
// digit, and no keyword PostgreSQL reserves in any way (reserved,
// type or function name, column name); double-quoted otherwise, with
// each " inside doubled ("Role", "a""b", "select", "time").
func QuoteIdent(s string) string {
	plain := s != ""
	for i := 0; i < len(s); i++ {
		c := s[i]
		if !(c == '_' || c >= 'a' && c <= 'z' || i > 0 && c >= '0' && c <= '9') {
			plain = false
			break
		}
	}
	if plain && !quotedKeywords[s] {
		return s
	}
	return `"` + strings.ReplaceAll(s, `"`, `""`) + `"`
}

// IsQuotedKeyword reports whether quote_ident quotes the word because
// it is a keyword PostgreSQL does not leave unreserved.
func IsQuotedKeyword(s string) bool { return quotedKeywords[s] }

// quotedKeywords are PostgreSQL 16's keywords of the reserved,
// type_func_name and col_name categories (kwlist.h): every keyword but
// the unreserved ones, which quote_identifier quotes.
var quotedKeywords = map[string]bool{}

func init() {
	for _, w := range strings.Fields(`
		all analyse analyze and any array as asc asymmetric both case cast check collate column constraint create
		current_catalog current_date current_role current_time current_timestamp current_user default deferrable
		desc distinct do else end except false fetch for foreign from grant group having in initially intersect
		into lateral leading limit localtime localtimestamp not null offset on only or order placing primary
		references returning select session_user some symmetric system_user table then to trailing true union
		unique user using variadic when where window with
		authorization binary collation concurrently cross current_schema freeze full ilike inner is isnull join
		left like natural notnull outer overlaps right similar tablesample verbose
		between bigint bit boolean char character coalesce dec decimal exists extract float greatest grouping
		inout int integer interval json json_array json_arrayagg json_object json_objectagg json_scalar
		json_serialize least national nchar none normalize nullif numeric out overlay position precision real
		row setof smallint substring time timestamp treat trim values varchar xmlattributes xmlconcat
		xmlelement xmlexists xmlforest xmlnamespaces xmlparse xmlpi xmlroot xmlserialize xmltable`) {
		quotedKeywords[w] = true
	}
}

// formatSQL implements format() as PostgreSQL's text_format does:
// %[position$][flags][width]type with the types s, I and L, the flag -
// (left-justify), a width given directly, as * (the next argument) or
// as *n$ (the n-th), and %%. A position also moves the sequence: the
// next plain specifier takes the argument after it.
func formatSQL(f string, args []types.Datum) (types.Datum, error) {
	var b strings.Builder
	next := 0 // the next argument, 0-based
	unterminated := func() error { return errf(CodeInvalidArgument, "unterminated format() type specifier") }
	digits := func(i int) (n int, end int, ok bool, err error) {
		end = i
		for end < len(f) && f[end] >= '0' && f[end] <= '9' {
			if n > (math.MaxInt32-int(f[end]-'0'))/10 {
				return 0, end, false, errf(CodeOutOfRange, "number is out of range")
			}
			n = n*10 + int(f[end]-'0')
			end++
		}
		return n, end, end > i, nil
	}
	for i := 0; i < len(f); i++ {
		c := f[i]
		if c != '%' {
			b.WriteByte(c)
			continue
		}
		i++
		if i >= len(f) {
			return types.Datum{}, unterminated()
		}
		if f[i] == '%' {
			b.WriteByte('%')
			continue
		}
		argpos, widthpos, width, minus := -1, -1, 0, false
		n, end, ok, err := digits(i)
		if err != nil {
			return types.Datum{}, err
		}
		if ok && end < len(f) && f[end] != '$' {
			width, i = n, end // a width, then the type
		} else {
			if ok {
				if end >= len(f) {
					return types.Datum{}, unterminated()
				}
				if n == 0 {
					return types.Datum{}, errf(CodeInvalidArgument, "format specifies argument 0, but arguments are numbered from 1")
				}
				argpos, i = n, end+1
			}
			for i < len(f) && f[i] == '-' {
				minus = true
				i++
			}
			if i < len(f) && f[i] == '*' {
				i++
				n, end, ok, err := digits(i)
				if err != nil {
					return types.Datum{}, err
				}
				widthpos = 0
				if ok {
					if end >= len(f) || f[end] != '$' {
						if end >= len(f) {
							return types.Datum{}, unterminated()
						}
						return types.Datum{}, errf(CodeInvalidArgument, "width argument position must be ended by \"$\"")
					}
					if n == 0 {
						return types.Datum{}, errf(CodeInvalidArgument, "format specifies argument 0, but arguments are numbered from 1")
					}
					widthpos, i = n, end+1
				}
			} else if n, end, ok, err := digits(i); err != nil {
				return types.Datum{}, err
			} else if ok {
				width, i = n, end
			}
		}
		if i >= len(f) {
			return types.Datum{}, unterminated()
		}
		conv := f[i]
		if conv != 's' && conv != 'I' && conv != 'L' {
			r, _ := utf8.DecodeRuneInString(f[i:])
			return types.Datum{}, errf(CodeInvalidArgument, "unrecognized format() type specifier \"%c\"", r)
		}
		if widthpos >= 0 {
			if widthpos > 0 {
				next = widthpos - 1
			}
			if next >= len(args) {
				return types.Datum{}, errf(CodeInvalidArgument, "too few arguments for format()")
			}
			w := args[next]
			next++
			width = 0
			if !w.Null {
				v, err := toInt(w)
				if err != nil {
					return types.Datum{}, err
				}
				if v.I > math.MaxInt32 || v.I < math.MinInt32 {
					return types.Datum{}, errf(CodeOutOfRange, "integer out of range")
				}
				width = int(v.I)
				if width < 0 {
					if width == math.MinInt32 {
						return types.Datum{}, errf(CodeOutOfRange, "number is out of range")
					}
					minus, width = true, -width
				}
			}
		}
		if argpos > 0 {
			next = argpos - 1
		}
		if next >= len(args) {
			return types.Datum{}, errf(CodeInvalidArgument, "too few arguments for format()")
		}
		arg := args[next]
		next++
		var out string
		switch conv {
		case 's':
			out = text(arg)
		case 'I':
			if arg.Null {
				return types.Datum{}, errf(CodeNullNotAllowed, "null values cannot be formatted as an SQL identifier")
			}
			out = quoteIdent(text(arg))
		case 'L':
			if arg.Null {
				out = "NULL"
			} else {
				out = quoteLiteral(text(arg))
			}
		}
		if pad := width - utf8.RuneCountInString(out); pad > 0 {
			if minus {
				out += strings.Repeat(" ", pad)
			} else {
				out = strings.Repeat(" ", pad) + out
			}
		}
		b.WriteString(out)
	}
	return str(b.String()), nil
}
