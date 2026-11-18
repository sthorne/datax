package builtins

import (
	"fmt"
	"strconv"
	"strings"
	"unicode/utf8"

	"golang.org/x/text/encoding/charmap"
	"golang.org/x/text/unicode/norm"

	"github.com/sthorne/datax/pkg/sql/types"
)

// The string functions of PG-F-009: overlay, bit_length, to_ascii,
// convert_from / convert_to / convert, unistr, normalize and
// is_normalized, parse_ident.

// SQLSTATEs of the encoding conversions.
const (
	CodeCharNotInRepertoire = "22021" // character_not_in_repertoire
	CodeUntranslatable      = "22P05" // untranslatable_character
	CodeSubstringError      = "22011" // substring_error
	CodeNullNotAllowed      = "22004" // null_value_not_allowed
	CodeSyntaxError         = "42601"
	CodeUndefinedObject     = "42704"
)

// encodingInfo is a server encoding datax converts to and from: its
// PostgreSQL name and its single-byte table (nil for UTF8 and
// SQL_ASCII).
type encodingInfo struct {
	name string
	cm   *charmap.Charmap
}

// encodings maps PostgreSQL's cleaned encoding names and aliases
// (lower-case, without punctuation, as pg_char_to_encoding reads them)
// to the encodings. The multi-byte Asian encodings are not here.
var encodings = map[string]encodingInfo{}

func init() {
	add := func(cm *charmap.Charmap, name string, aliases ...string) {
		e := encodingInfo{name: name, cm: cm}
		encodings[cleanEncodingName(name)] = e
		for _, a := range aliases {
			encodings[a] = e
		}
	}
	add(nil, "UTF8", "unicode")
	add(nil, "SQL_ASCII")
	add(charmap.ISO8859_1, "LATIN1", "iso88591")
	add(charmap.ISO8859_2, "LATIN2", "iso88592")
	add(charmap.ISO8859_3, "LATIN3", "iso88593")
	add(charmap.ISO8859_4, "LATIN4", "iso88594")
	add(charmap.ISO8859_9, "LATIN5", "iso88599")
	add(charmap.ISO8859_10, "LATIN6", "iso885910")
	add(charmap.ISO8859_13, "LATIN7", "iso885913")
	add(charmap.ISO8859_14, "LATIN8", "iso885914")
	add(charmap.ISO8859_15, "LATIN9", "iso885915")
	add(charmap.ISO8859_16, "LATIN10", "iso885916")
	add(charmap.ISO8859_5, "ISO_8859_5")
	add(charmap.ISO8859_6, "ISO_8859_6")
	add(charmap.ISO8859_7, "ISO_8859_7")
	add(charmap.ISO8859_8, "ISO_8859_8")
	add(charmap.KOI8R, "KOI8R", "koi8")
	add(charmap.KOI8U, "KOI8U")
	add(charmap.CodePage866, "WIN866", "alt", "cp866")
	add(charmap.Windows874, "WIN874", "cp874")
	add(charmap.Windows1250, "WIN1250", "cp1250", "windows1250")
	add(charmap.Windows1251, "WIN1251", "cp1251", "win", "windows1251")
	add(charmap.Windows1252, "WIN1252", "cp1252", "windows1252")
	add(charmap.Windows1253, "WIN1253", "cp1253", "windows1253")
	add(charmap.Windows1254, "WIN1254", "cp1254", "windows1254")
	add(charmap.Windows1255, "WIN1255", "cp1255", "windows1255")
	add(charmap.Windows1256, "WIN1256", "cp1256", "windows1256")
	add(charmap.Windows1257, "WIN1257", "cp1257", "windows1257")
	add(charmap.Windows1258, "WIN1258", "cp1258", "windows1258")
}

// cleanEncodingName is PostgreSQL's clean_encoding_name: lower case,
// letters and digits only.
func cleanEncodingName(s string) string {
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case c >= 'a' && c <= 'z', c >= '0' && c <= '9':
			b.WriteByte(c)
		case c >= 'A' && c <= 'Z':
			b.WriteByte(c + 'a' - 'A')
		}
	}
	return b.String()
}

// lookupEncoding finds an encoding by name; role is "source" or
// "destination" for the error.
func lookupEncoding(name, role string) (encodingInfo, error) {
	e, ok := encodings[cleanEncodingName(name)]
	if !ok {
		return e, errf(CodeInvalidArgument, "invalid %s encoding name \"%s\"", role, name)
	}
	return e, nil
}

// byteSeq renders bytes as PostgreSQL's conversion errors do.
func byteSeq(b []byte) string {
	parts := make([]string, len(b))
	for i, c := range b {
		parts[i] = fmt.Sprintf("0x%02x", c)
	}
	return strings.Join(parts, " ")
}

// fromEncoding decodes bytes in e into UTF-8 text, refusing an invalid
// or untranslatable byte as PostgreSQL does.
func fromEncoding(b []byte, e encodingInfo) (string, error) {
	if e.cm == nil {
		// UTF8 and SQL_ASCII: the bytes are the text, which must be
		// valid UTF-8 (the database's encoding) and hold no NUL.
		for i := 0; i < len(b); {
			r, n := utf8.DecodeRune(b[i:])
			if r == 0 || (r == utf8.RuneError && n <= 1) {
				end := min(i+max(n, 1), len(b))
				return "", errf(CodeCharNotInRepertoire, "invalid byte sequence for encoding \"UTF8\": %s", byteSeq(b[i:end]))
			}
			i += n
		}
		return string(b), nil
	}
	var sb strings.Builder
	for _, c := range b {
		if c == 0 {
			return "", errf(CodeCharNotInRepertoire, "invalid byte sequence for encoding \"%s\": 0x00", e.name)
		}
		r := e.cm.DecodeByte(c)
		if r == utf8.RuneError {
			return "", errf(CodeUntranslatable, "character with byte sequence 0x%02x in encoding \"%s\" has no equivalent in encoding \"UTF8\"", c, e.name)
		}
		sb.WriteRune(r)
	}
	return sb.String(), nil
}

// toEncoding encodes UTF-8 text into e.
func toEncoding(s string, e encodingInfo) ([]byte, error) {
	if e.cm == nil {
		return []byte(s), nil
	}
	out := make([]byte, 0, len(s))
	for i, r := range s {
		if r < utf8.RuneSelf {
			out = append(out, byte(r))
			continue
		}
		c, ok := e.cm.EncodeRune(r)
		if !ok {
			return nil, errf(CodeUntranslatable, "character with byte sequence %s in encoding \"UTF8\" has no equivalent in encoding \"%s\"", byteSeq([]byte(string(r))), e.name)
		}
		_ = i
		out = append(out, c)
	}
	return out, nil
}

// asciiTables are PostgreSQL's to_ascii transliterations (ascii.c): the
// ASCII letter for each byte from 160 (128 for WIN1250) up.
var asciiTables = map[string]struct {
	from  int
	table string
}{
	"LATIN1":  {160, `  cL Y  "Ca  -R     'u .,      ?AAAAAAACEEEEIIII NOOOOOx0UUUUYTBaaaaaaaceeeeiiii nooooo/0uuuuyty`},
	"LATIN2":  {160, ` A L LS "SSTZ-ZZ a,l'ls ,sstz"zzRAAAALCCCEEEEIIDDNNOOOOxRUUUUYTBraaaalccceeeeiiddnnoooo/ruuuuyt.`},
	"LATIN9":  {160, `  cL YS sCa  -R     Zu .z   EeY?AAAAAAACEEEEIIII NOOOOOx0UUUUYTBaaaaaaaceeeeiiii nooooo/0uuuuyty`},
	"WIN1250": {128, `  ' "    %S<STZZ ` + "`" + `'"".--  s>stzz   L A  "CS  -RZ  ,l'u .,as L"lzRAAAALCCCEEEEIIDDNNOOOOxRUUUUYTBraaaalccceeeeiiddnnoooo/ruuuuyt `},
}

// toASCII is to_ascii over the bytes of the value read in the named
// encoding: ASCII stays, the bytes of the encoding's upper half become
// the letter without its accent, the rest a space.
func toASCII(b []byte, encName string) (types.Datum, error) {
	e, ok := encodings[cleanEncodingName(encName)]
	if !ok {
		return types.Datum{}, errf(CodeUndefinedObject, "%s is not a valid encoding name", encName)
	}
	t, ok := asciiTables[e.name]
	if !ok {
		return types.Datum{}, errf(CodeNotSupported, "encoding conversion from %s to ASCII not supported", e.name)
	}
	out := make([]byte, len(b))
	for i, c := range b {
		switch {
		case c < 128:
			out[i] = c
		case int(c) < t.from:
			out[i] = ' '
		default:
			out[i] = t.table[int(c)-t.from]
		}
	}
	return str(string(out)), nil
}

// unistr decodes the Unicode escapes \XXXX, \+XXXXXX, \uXXXX and
// \UXXXXXXXX (a surrogate pair combines) and \\.
func unistr(s string) (types.Datum, error) {
	var b strings.Builder
	var hi rune // a pending high surrogate
	bad := func() error {
		return errf(CodeSyntaxError, "invalid Unicode escape")
	}
	for i := 0; i < len(s); {
		if s[i] != '\\' {
			if hi != 0 {
				return types.Datum{}, errf(CodeSyntaxError, "invalid Unicode surrogate pair")
			}
			b.WriteByte(s[i])
			i++
			continue
		}
		if i+1 < len(s) && s[i+1] == '\\' {
			if hi != 0 {
				return types.Datum{}, errf(CodeSyntaxError, "invalid Unicode surrogate pair")
			}
			b.WriteByte('\\')
			i += 2
			continue
		}
		var digits, skip int
		switch {
		case i+1 < len(s) && s[i+1] == '+':
			digits, skip = 6, 2
		case i+1 < len(s) && s[i+1] == 'u':
			digits, skip = 4, 2
		case i+1 < len(s) && s[i+1] == 'U':
			digits, skip = 8, 2
		default:
			digits, skip = 4, 1
		}
		if i+skip+digits > len(s) {
			return types.Datum{}, bad()
		}
		hex := s[i+skip : i+skip+digits]
		v, err := strconv.ParseUint(hex, 16, 32)
		if err != nil || strings.ContainsAny(hex, "+-") {
			return types.Datum{}, bad()
		}
		i += skip + digits
		r := rune(v)
		switch {
		case r >= 0xD800 && r <= 0xDBFF:
			if hi != 0 {
				return types.Datum{}, errf(CodeSyntaxError, "invalid Unicode surrogate pair")
			}
			hi = r
			continue
		case r >= 0xDC00 && r <= 0xDFFF:
			if hi == 0 {
				return types.Datum{}, errf(CodeSyntaxError, "invalid Unicode surrogate pair")
			}
			r = 0x10000 + (hi-0xD800)<<10 + (r - 0xDC00)
			hi = 0
		case hi != 0:
			return types.Datum{}, errf(CodeSyntaxError, "invalid Unicode surrogate pair")
		}
		if v > utf8.MaxRune || r == 0 {
			return types.Datum{}, errf(CodeInvalidArgument, "invalid Unicode code point: %04X", v)
		}
		b.WriteRune(r)
	}
	if hi != 0 {
		return types.Datum{}, errf(CodeSyntaxError, "invalid Unicode surrogate pair")
	}
	return str(b.String()), nil
}

// normForm reads a Unicode normal form's name (NFC by default).
func normForm(a []types.Datum, i int) (norm.Form, error) {
	name := "NFC"
	if i < len(a) {
		name = strings.ToUpper(strings.TrimSpace(a[i].Text()))
	}
	switch name {
	case "NFC":
		return norm.NFC, nil
	case "NFD":
		return norm.NFD, nil
	case "NFKC":
		return norm.NFKC, nil
	case "NFKD":
		return norm.NFKD, nil
	}
	return 0, errf(CodeInvalidArgument, "invalid normalization form: %s", a[i].Text())
}

// overlay replaces the run of n elements (runes or bytes) of s from
// the 1-based start with r: s[:start-1] || r || s[start-1+n:], n
// defaulting to r's length, as PostgreSQL's text_overlay.
func overlay[T rune | byte](s, r []T, start, n int64) ([]T, error) {
	if start <= 0 {
		return nil, errf(CodeSubstringError, "negative substring length not allowed")
	}
	end := start + n
	if (n > 0 && end < start) || (n < 0 && end > start) {
		return nil, errf(CodeOutOfRange, "integer out of range")
	}
	head := s[:min(start-1, int64(len(s)))]
	var tail []T
	switch {
	case end < 1:
		tail = s
	case end-1 < int64(len(s)):
		tail = s[end-1:]
	}
	out := make([]T, 0, len(head)+len(r)+len(tail))
	out = append(out, head...)
	out = append(out, r...)
	return append(out, tail...), nil
}

// parseIdent is parse_ident: the parts of a qualified SQL identifier,
// unquoted parts folded to lower case.
func parseIdent(s string, strict bool) (types.Datum, error) {
	invalid := func(detail string) error {
		// PostgreSQL gives the detail separately; the message is the
		// same for every case.
		_ = detail
		return errf(CodeInvalidArgument, "string is not a valid identifier: \"%s\"", s)
	}
	isSpace := func(c byte) bool { return c == ' ' || c == '\t' || c == '\n' || c == '\r' || c == '\f' || c == '\v' }
	isStart := func(c byte) bool { return c == '_' || c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= 0x80 }
	isCont := func(c byte) bool { return isStart(c) || c >= '0' && c <= '9' || c == '$' }
	var parts []types.Datum
	i := 0
	for i < len(s) && isSpace(s[i]) {
		i++
	}
	afterDot := false
	for {
		missing := true
		switch {
		case i < len(s) && s[i] == '"':
			var b strings.Builder
			i++
			for {
				j := strings.IndexByte(s[i:], '"')
				if j < 0 {
					return types.Datum{}, invalid("String has unclosed double quotes.")
				}
				b.WriteString(s[i : i+j])
				i += j + 1
				if i < len(s) && s[i] == '"' {
					b.WriteByte('"')
					i++
					continue
				}
				break
			}
			if b.Len() == 0 {
				return types.Datum{}, invalid("Quoted identifier must not be empty.")
			}
			parts = append(parts, str(b.String()))
			missing = false
		case i < len(s) && isStart(s[i]):
			j := i + 1
			for j < len(s) && isCont(s[j]) {
				j++
			}
			parts = append(parts, str(asciiLower(s[i:j])))
			i, missing = j, false
		}
		if missing {
			switch {
			case i < len(s) && s[i] == '.':
				return types.Datum{}, invalid("No valid identifier before \".\".")
			case afterDot:
				return types.Datum{}, invalid("No valid identifier after \".\".")
			}
			return types.Datum{}, invalid("")
		}
		for i < len(s) && isSpace(s[i]) {
			i++
		}
		if i < len(s) && s[i] == '.' {
			afterDot = true
			i++
			for i < len(s) && isSpace(s[i]) {
				i++
			}
			continue
		}
		if i >= len(s) {
			break
		}
		if strict {
			return types.Datum{}, invalid("")
		}
		break
	}
	return types.NewArray(types.String, parts), nil
}

// asciiLower folds A-Z only, as PostgreSQL's downcase_identifier does
// for a multi-byte encoding.
func asciiLower(s string) string {
	b := []byte(s)
	for i, c := range b {
		if c >= 'A' && c <= 'Z' {
			b[i] = c + 'a' - 'A'
		}
	}
	return string(b)
}

func init() {
	register(&Builtin{Name: "overlay", Args: []types.Family{types.String, types.String, types.Int, types.Int}, MinArgs: 3, Ret: types.String, Category: catString,
		Doc:   "overlay(s PLACING r FROM start [FOR n]): s with the n characters (r's length by default) from the 1-based start replaced by r; over bytea, bytes.",
		Bytea: true, RetOf: stringOrBytes,
		Fn: func(a []types.Datum) (types.Datum, error) {
			start := a[2].I
			if bytea(a[:2]) {
				s, err := byteaArg(a[0])
				if err != nil {
					return types.Datum{}, err
				}
				r, err := byteaArg(a[1])
				if err != nil {
					return types.Datum{}, err
				}
				n := int64(len(r))
				if len(a) > 3 {
					n = a[3].I
				}
				out, err := overlay(s, r, start, n)
				if err != nil {
					return types.Datum{}, err
				}
				return types.NewBytes(out), nil
			}
			r := []rune(a[1].S)
			n := int64(len(r))
			if len(a) > 3 {
				n = a[3].I
			}
			out, err := overlay([]rune(a[0].S), r, start, n)
			if err != nil {
				return types.Datum{}, err
			}
			return str(string(out)), nil
		}})
	register(&Builtin{Name: "bit_length", Args: []types.Family{types.String}, MinArgs: 1, Ret: types.Int, Category: catString,
		Doc: "Number of bits in the string or bytea (8 × octet_length).", Bytea: true,
		Fn: func(a []types.Datum) (types.Datum, error) { return i64(8 * int64(len(a[0].S))), nil }})
	register(&Builtin{Name: "to_ascii", Args: []types.Family{types.String, Any}, MinArgs: 1, Ret: types.String, Category: catString, Bytea: true,
		Doc: "Transliterates the value's bytes, read in the given encoding (LATIN1, LATIN2, LATIN9 or WIN1250; also by its number), to ASCII by dropping accents, as PostgreSQL does. Without an encoding it reads the database's, UTF8, which PostgreSQL does not convert (0A000).",
		Fn: func(a []types.Datum) (types.Datum, error) {
			if len(a) < 2 {
				return types.Datum{}, errf(CodeNotSupported, "encoding conversion from UTF8 to ASCII not supported")
			}
			name := a[1].Text()
			if a[1].Fam == types.Int {
				if name = encodingNameByID(a[1].I); name == "" {
					return types.Datum{}, errf(CodeUndefinedObject, "%d is not a valid encoding code", a[1].I)
				}
			}
			return toASCII([]byte(a[0].S), name)
		}})
	register(&Builtin{Name: "convert_from", Args: []types.Family{types.Bytes, types.String}, MinArgs: 2, Ret: types.String, Category: catString,
		Doc: "The bytes decoded from the named encoding (UTF8, SQL_ASCII, LATIN1 to LATIN10, ISO_8859_5 to 8, WIN866, WIN874, WIN1250 to WIN1258, KOI8R, KOI8U) into text.",
		Fn: func(a []types.Datum) (types.Datum, error) {
			e, err := lookupEncoding(a[1].S, "source")
			if err != nil {
				return types.Datum{}, err
			}
			s, err := fromEncoding([]byte(a[0].S), e)
			if err != nil {
				return types.Datum{}, err
			}
			return str(s), nil
		}})
	register(&Builtin{Name: "convert_to", Args: []types.Family{types.String, types.String}, MinArgs: 2, Ret: types.Bytes, Category: catString,
		Doc: "The text encoded into the named encoding, as bytes (22P05 for a character the encoding lacks).",
		Fn: func(a []types.Datum) (types.Datum, error) {
			e, err := lookupEncoding(a[1].S, "destination")
			if err != nil {
				return types.Datum{}, err
			}
			b, err := toEncoding(a[0].S, e)
			if err != nil {
				return types.Datum{}, err
			}
			return types.NewBytes(b), nil
		}})
	register(&Builtin{Name: "convert", Args: []types.Family{types.Bytes, types.String, types.String}, MinArgs: 3, Ret: types.Bytes, Category: catString,
		Doc: "convert(bytes, source, destination): the bytes re-encoded from one encoding into another.",
		Fn: func(a []types.Datum) (types.Datum, error) {
			src, err := lookupEncoding(a[1].S, "source")
			if err != nil {
				return types.Datum{}, err
			}
			dst, err := lookupEncoding(a[2].S, "destination")
			if err != nil {
				return types.Datum{}, err
			}
			s, err := fromEncoding([]byte(a[0].S), src)
			if err != nil {
				return types.Datum{}, err
			}
			b, err := toEncoding(s, dst)
			if err != nil {
				return types.Datum{}, err
			}
			return types.NewBytes(b), nil
		}})
	register(&Builtin{Name: "unistr", Args: []types.Family{types.String}, MinArgs: 1, Ret: types.String, Category: catString,
		Doc: `The string with its Unicode escapes decoded: \XXXX, \+XXXXXX, \uXXXX, \UXXXXXXXX (hex code points; a UTF-16 surrogate pair combines) and \\ for a backslash.`,
		Fn:  func(a []types.Datum) (types.Datum, error) { return unistr(a[0].S) }})
	register(&Builtin{Name: "normalize", Args: []types.Family{types.String, types.String}, MinArgs: 1, Ret: types.String, Category: catString,
		Doc: "The string in a Unicode normal form: normalize(s [, NFC | NFD | NFKC | NFKD]), NFC by default.",
		Fn: func(a []types.Datum) (types.Datum, error) {
			f, err := normForm(a, 1)
			if err != nil {
				return types.Datum{}, err
			}
			return str(f.String(a[0].S)), nil
		}})
	register(&Builtin{Name: "is_normalized", Args: []types.Family{types.String, types.String}, MinArgs: 1, Ret: types.Bool, Category: catString,
		Doc: "Whether the string is in the Unicode normal form (NFC by default); also s IS [NOT] [NFC | NFD | NFKC | NFKD] NORMALIZED.",
		Fn: func(a []types.Datum) (types.Datum, error) {
			f, err := normForm(a, 1)
			if err != nil {
				return types.Datum{}, err
			}
			return types.NewBool(f.IsNormalString(a[0].S)), nil
		}})
	register(&Builtin{Name: "parse_ident", Args: []types.Family{types.String, types.Bool}, MinArgs: 1, Ret: types.ArrayOf(types.String), Category: catString,
		Doc: "The parts of a qualified SQL identifier as a text array ('Sch.\"Tbl\"' is {sch,Tbl}); with strict false, text after the identifier is ignored.",
		Fn: func(a []types.Datum) (types.Datum, error) {
			strict := true
			if len(a) > 1 {
				strict = a[1].B
			}
			return parseIdent(a[0].S, strict)
		}})
}

// encodingNameByID names an encoding by PostgreSQL's number for it, as
// pg_encoding_to_char does for the ones to_ascii takes.
func encodingNameByID(id int64) string {
	switch id {
	case 6:
		return "UTF8"
	case 8:
		return "LATIN1"
	case 9:
		return "LATIN2"
	case 16:
		return "LATIN9"
	case 29:
		return "WIN1250"
	}
	if id >= 0 && id <= 41 {
		return "SQL_ASCII" // a valid code to_ascii does not convert from
	}
	return ""
}
