package parser

import (
	"fmt"
	"strings"
	"unicode"
	"unicode/utf8"
)

type tokenKind int

const (
	tkEOF tokenKind = iota
	tkIdent
	tkKeyword
	tkNumber
	tkString
	tkParam // $1, $2, ...
	tkOp    // punctuation and operators
)

type token struct {
	kind tokenKind
	text string // keywords upper-cased, identifiers lower-cased
	pos  int
	// end is the offset just past the token in the source.
	end int
	// quoted marks a double-quoted identifier: its text is the name
	// exactly as written (case kept, "" read as one "), and it is never
	// a keyword, however it is spelled ("select", "concurrently").
	quoted bool
}

// isWord reports whether t is the unquoted word w (lower case): a
// keyword this grammar reads as an identifier token. A quoted
// identifier is never one.
func (t token) isWord(w string) bool { return t.kind == tkIdent && !t.quoted && t.text == w }

var keywords = map[string]bool{
	"CREATE": true, "TABLE": true, "DROP": true, "IF": true, "NOT": true,
	"EXISTS": true, "NULL": true, "PRIMARY": true, "KEY": true,
	"INSERT": true, "INTO": true, "VALUES": true,
	"SELECT": true, "FROM": true, "WHERE": true, "AND": true, "OR": true, "LIMIT": true, "AS": true,
	"UPDATE": true, "SET": true, "DELETE": true,
	"BEGIN": true, "START": true, "TRANSACTION": true, "COMMIT": true, "ROLLBACK": true, "ABORT": true, "END": true,
	"SHOW": true, "TABLES": true, "TRUE": true, "FALSE": true,
	"SESSION": true, "LOCAL": true, "TO": true,
	"UNIQUE": true, "INDEX": true, "ON": true, "EXPLAIN": true,
	"ORDER": true, "BY": true, "ASC": true, "DESC": true,
	"ALTER": true, "ADD": true, "COLUMN": true,
	"USER": true, "PASSWORD": true,
	"CONSTRAINT": true, "CHECK": true, "REFERENCES": true, "FOREIGN": true,
}

type lexer struct {
	src  string
	pos  int
	toks []token
}

// lex tokenizes src. SQL identifiers are lower-cased (quoted identifiers
// keep their case), keywords upper-cased.
func lex(src string) ([]token, error) {
	l := &lexer{src: src}
	for {
		if n := len(l.toks); n > 0 && l.toks[n-1].end == 0 {
			l.toks[n-1].end = l.pos
		}
		l.skipSpace()
		if l.pos >= len(l.src) {
			l.toks = append(l.toks, token{kind: tkEOF, pos: l.pos, end: l.pos})
			return l.toks, nil
		}
		start := l.pos
		c := l.src[l.pos]
		switch {
		case c == '-' && l.peekAt(1) == '-': // line comment
			for l.pos < len(l.src) && l.src[l.pos] != '\n' {
				l.pos++
			}
		case (unicode.IsLetter(rune(c)) || c == '_') && !((c == 'E' || c == 'e') && l.peekAt(1) == '\''):
			for l.pos < len(l.src) && (unicode.IsLetter(rune(l.src[l.pos])) || unicode.IsDigit(rune(l.src[l.pos])) || l.src[l.pos] == '_') {
				l.pos++
			}
			word := l.src[start:l.pos]
			if keywords[strings.ToUpper(word)] {
				l.toks = append(l.toks, token{kind: tkKeyword, text: strings.ToUpper(word), pos: start})
			} else {
				l.toks = append(l.toks, token{kind: tkIdent, text: strings.ToLower(word), pos: start})
			}
		case unicode.IsDigit(rune(c)) || (c == '.' && unicode.IsDigit(rune(l.peekAt(1)))):
			for l.pos < len(l.src) && (unicode.IsDigit(rune(l.src[l.pos])) || l.src[l.pos] == '.' || l.src[l.pos] == 'e' || l.src[l.pos] == 'E' ||
				((l.src[l.pos] == '+' || l.src[l.pos] == '-') && l.pos > start && (l.src[l.pos-1] == 'e' || l.src[l.pos-1] == 'E'))) {
				l.pos++
			}
			l.toks = append(l.toks, token{kind: tkNumber, text: l.src[start:l.pos], pos: start})
		case c == '\'' || ((c == 'E' || c == 'e') && l.peekAt(1) == '\''):
			text, err := l.stringLiteral(start, c != '\'')
			if err != nil {
				return nil, err
			}
			l.toks = append(l.toks, token{kind: tkString, text: text, pos: start})
		case c == '"':
			// A quoted identifier: its case is kept and "" stands for
			// one " (PostgreSQL's delimited identifier).
			l.pos++
			var sb strings.Builder
			for {
				if l.pos >= len(l.src) {
					return nil, &SyntaxError{Msg: `unterminated quoted identifier at or near "` + l.src[start:] + `"`, Pos: start, Code: "42601"}
				}
				if l.src[l.pos] == '"' {
					if l.peekAt(1) == '"' {
						sb.WriteByte('"')
						l.pos += 2
						continue
					}
					l.pos++
					break
				}
				sb.WriteByte(l.src[l.pos])
				l.pos++
			}
			if sb.Len() == 0 {
				return nil, &SyntaxError{Msg: `zero-length delimited identifier at or near """"`, Pos: start, Code: "42601"}
			}
			l.toks = append(l.toks, token{kind: tkIdent, text: sb.String(), pos: start, quoted: true})
		case c == '$':
			l.pos++
			numStart := l.pos
			for l.pos < len(l.src) && unicode.IsDigit(rune(l.src[l.pos])) {
				l.pos++
			}
			if l.pos == numStart {
				return nil, fmt.Errorf("invalid parameter at position %d", start)
			}
			l.toks = append(l.toks, token{kind: tkParam, text: l.src[numStart:l.pos], pos: start})
		default:
			// Multi-char operators first (longest match: ->> before ->,
			// which the comment case above already can't shadow).
			if l.pos+3 <= len(l.src) && l.src[l.pos:l.pos+3] == "->>" {
				l.toks = append(l.toks, token{kind: tkOp, text: "->>", pos: start})
				l.pos += 3
				continue
			}
			if strings.HasPrefix(l.src[l.pos:], "#>>") {
				l.toks = append(l.toks, token{kind: tkOp, text: "#>>", pos: start})
				l.pos += 3
				continue
			}
			// The vector distance operators (<-> Euclidean, <=> cosine,
			// <#> negative inner product). None of the three is a prefix
			// of valid SQL otherwise: after "<-", "<=" or "<" + "#", a
			// ">" could only begin an operand, and no operand begins so.
			if three := l.src[l.pos:]; strings.HasPrefix(three, "<->") || strings.HasPrefix(three, "<=>") || strings.HasPrefix(three, "<#>") {
				l.toks = append(l.toks, token{kind: tkOp, text: three[:3], pos: start})
				l.pos += 3
				continue
			}
			two := ""
			if l.pos+1 < len(l.src) {
				two = l.src[l.pos : l.pos+2]
			}
			switch two {
			case "!=", "<>", "<=", ">=", "::", "->", "@>", "!~", "~*", "||", "#>", "<@", "?|", "?&", "&&", "@@":
				op := two
				if op == "<>" {
					op = "!="
				}
				if op == "!~" && l.peekAt(2) == '*' {
					op = "!~*"
					l.pos++
				}
				l.toks = append(l.toks, token{kind: tkOp, text: op, pos: start})
				l.pos += 2
			default:
				switch c {
				case '(', ')', ',', ';', '=', '<', '>', '*', '+', '-', '/', '.', '~', '%', '^', '?', '[', ']', ':':
					l.toks = append(l.toks, token{kind: tkOp, text: string(c), pos: start})
					l.pos++
				default:
					return nil, fmt.Errorf("unexpected character %q at position %d", c, start)
				}
			}
		}
	}
}

func (l *lexer) peekAt(off int) byte {
	if l.pos+off < len(l.src) {
		return l.src[l.pos+off]
	}
	return 0
}

func (l *lexer) skipSpace() {
	for l.pos < len(l.src) && unicode.IsSpace(rune(l.src[l.pos])) {
		l.pos++
	}
}

// stringLiteral reads a string literal starting at the opening quote
// (after an E prefix when escapes is set, which l.pos is still on):
// a doubled quote is one quote, and in an E-string the backslash escapes
// PostgreSQL's scanner reads: \b \f \n \r \t, \o \oo \ooo (octal byte),
// \xh \xhh (hex byte), \uXXXX and \UXXXXXXXX (a Unicode code point, a
// UTF-16 surrogate pair as two \u escapes), and any other character
// after a backslash standing for itself. The bytes an octal or hex
// escape writes must form valid UTF-8, \0 included (22021), as in a
// UTF8 database.
func (l *lexer) stringLiteral(start int, escapes bool) (string, error) {
	if escapes {
		l.pos++ // the E prefix
	}
	l.pos++ // the opening quote
	var sb strings.Builder
	rawBytes := false
	var surrogate rune
	for {
		if l.pos >= len(l.src) {
			return "", fmt.Errorf("unterminated string literal at position %d", start)
		}
		c := l.src[l.pos]
		if surrogate != 0 && !(c == '\\' && (l.peekAt(1) == 'u' || l.peekAt(1) == 'U')) {
			return "", &SyntaxError{Msg: "invalid Unicode surrogate pair", Pos: l.pos, Code: "42601"}
		}
		if c == '\'' {
			if l.peekAt(1) == '\'' { // escaped quote
				sb.WriteByte('\'')
				l.pos += 2
				continue
			}
			l.pos++
			break
		}
		if !escapes || c != '\\' || l.pos+1 >= len(l.src) {
			sb.WriteByte(c)
			l.pos++
			continue
		}
		e := l.src[l.pos+1]
		switch {
		case e == 'b':
			sb.WriteByte('\b')
			l.pos += 2
		case e == 'f':
			sb.WriteByte('\f')
			l.pos += 2
		case e == 'n':
			sb.WriteByte('\n')
			l.pos += 2
		case e == 'r':
			sb.WriteByte('\r')
			l.pos += 2
		case e == 't':
			sb.WriteByte('\t')
			l.pos += 2
		case e >= '0' && e <= '7':
			n, v := 0, 0
			for n < 3 && l.pos+1+n < len(l.src) && l.src[l.pos+1+n] >= '0' && l.src[l.pos+1+n] <= '7' {
				v = v*8 + int(l.src[l.pos+1+n]-'0')
				n++
			}
			sb.WriteByte(byte(v))
			rawBytes = true
			l.pos += 1 + n
		case e == 'x' && isHexDigit(l.peekAt(2)):
			n, v := 0, 0
			for n < 2 && isHexDigit(l.peekAt(2+n)) {
				v = v*16 + hexVal(l.peekAt(2+n))
				n++
			}
			sb.WriteByte(byte(v))
			rawBytes = true
			l.pos += 2 + n
		case e == 'u' || e == 'U':
			want := 4
			if e == 'U' {
				want = 8
			}
			var v rune
			for n := 0; n < want; n++ {
				h := l.peekAt(2 + n)
				if !isHexDigit(h) {
					return "", &SyntaxError{Msg: "invalid Unicode escape", Hint: `Unicode escapes must be \uXXXX or \UXXXXXXXX.`, Pos: l.pos, Code: "22025"}
				}
				v = v*16 + rune(hexVal(h))
			}
			l.pos += 2 + want
			switch {
			case surrogate != 0:
				if v < 0xDC00 || v > 0xDFFF {
					return "", &SyntaxError{Msg: "invalid Unicode surrogate pair", Pos: l.pos, Code: "42601"}
				}
				v = 0x10000 + (surrogate-0xD800)<<10 + (v - 0xDC00)
				surrogate = 0
			case v >= 0xD800 && v <= 0xDBFF:
				surrogate = v
				continue
			case v >= 0xDC00 && v <= 0xDFFF:
				return "", &SyntaxError{Msg: "invalid Unicode surrogate pair", Pos: l.pos, Code: "42601"}
			}
			if v == 0 || v > 0x10FFFF {
				return "", &SyntaxError{Msg: "invalid Unicode escape value", Pos: l.pos, Code: "42601"}
			}
			sb.WriteRune(v)
		default:
			// Any other character after a backslash stands for itself
			// (\\, \', \x without a hex digit, a multibyte character).
			_, size := utf8.DecodeRuneInString(l.src[l.pos+1:])
			sb.WriteString(l.src[l.pos+1 : l.pos+1+size])
			l.pos += 1 + size
		}
	}
	s := sb.String()
	if rawBytes {
		if err := checkUTF8(s); err != nil {
			return "", err
		}
	}
	return s, nil
}

// checkUTF8 refuses text that is not valid UTF-8 or holds a NUL, with
// PostgreSQL's message and code (22021).
func checkUTF8(s string) error {
	for i := 0; i < len(s); {
		r, size := utf8.DecodeRuneInString(s[i:])
		if r == 0 || r == utf8.RuneError && size <= 1 {
			end := i + 1
			if r != 0 {
				// The bytes of the would-be sequence, as PostgreSQL's
				// report_invalid_encoding prints them.
				want := 1
				switch b := s[i]; {
				case b >= 0xF0:
					want = 4
				case b >= 0xE0:
					want = 3
				case b >= 0xC0:
					want = 2
				}
				end = i + want
				if end > len(s) {
					end = len(s)
				}
			}
			var hex strings.Builder
			for j := i; j < end; j++ {
				if j > i {
					hex.WriteByte(' ')
				}
				fmt.Fprintf(&hex, "0x%02x", s[j])
			}
			return &SyntaxError{Msg: fmt.Sprintf("invalid byte sequence for encoding \"UTF8\": %s", hex.String()), Code: "22021"}
		}
		i += size
	}
	return nil
}

func isHexDigit(c byte) bool {
	return c >= '0' && c <= '9' || c >= 'a' && c <= 'f' || c >= 'A' && c <= 'F'
}

func hexVal(c byte) int {
	switch {
	case c >= '0' && c <= '9':
		return int(c - '0')
	case c >= 'a' && c <= 'f':
		return int(c-'a') + 10
	}
	return int(c-'A') + 10
}
