package parser

import (
	"fmt"
	"strings"
	"unicode"
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
}

var keywords = map[string]bool{
	"CREATE": true, "TABLE": true, "DROP": true, "IF": true, "NOT": true,
	"EXISTS": true, "NULL": true, "PRIMARY": true, "KEY": true,
	"INSERT": true, "INTO": true, "VALUES": true,
	"SELECT": true, "FROM": true, "WHERE": true, "AND": true, "LIMIT": true, "AS": true,
	"UPDATE": true, "SET": true, "DELETE": true,
	"BEGIN": true, "START": true, "TRANSACTION": true, "COMMIT": true, "ROLLBACK": true, "ABORT": true, "END": true,
	"SHOW": true, "TABLES": true, "TRUE": true, "FALSE": true,
	"SESSION": true, "LOCAL": true, "TO": true,
	"UNIQUE": true, "INDEX": true, "ON": true, "EXPLAIN": true,
	"ORDER": true, "BY": true, "ASC": true, "DESC": true,
	"ALTER": true, "ADD": true, "COLUMN": true,
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
		l.skipSpace()
		if l.pos >= len(l.src) {
			l.toks = append(l.toks, token{kind: tkEOF, pos: l.pos})
			return l.toks, nil
		}
		start := l.pos
		c := l.src[l.pos]
		switch {
		case c == '-' && l.peekAt(1) == '-': // line comment
			for l.pos < len(l.src) && l.src[l.pos] != '\n' {
				l.pos++
			}
		case unicode.IsLetter(rune(c)) || c == '_':
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
		case c == '\'':
			var sb strings.Builder
			l.pos++
			for {
				if l.pos >= len(l.src) {
					return nil, fmt.Errorf("unterminated string literal at position %d", start)
				}
				if l.src[l.pos] == '\'' {
					if l.peekAt(1) == '\'' { // escaped quote
						sb.WriteByte('\'')
						l.pos += 2
						continue
					}
					l.pos++
					break
				}
				sb.WriteByte(l.src[l.pos])
				l.pos++
			}
			l.toks = append(l.toks, token{kind: tkString, text: sb.String(), pos: start})
		case c == '"':
			l.pos++
			qstart := l.pos
			for l.pos < len(l.src) && l.src[l.pos] != '"' {
				l.pos++
			}
			if l.pos >= len(l.src) {
				return nil, fmt.Errorf("unterminated quoted identifier at position %d", start)
			}
			l.toks = append(l.toks, token{kind: tkIdent, text: l.src[qstart:l.pos], pos: start})
			l.pos++
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
			// Multi-char operators first.
			two := ""
			if l.pos+1 < len(l.src) {
				two = l.src[l.pos : l.pos+2]
			}
			switch two {
			case "!=", "<>", "<=", ">=", "::":
				op := two
				if op == "<>" {
					op = "!="
				}
				l.toks = append(l.toks, token{kind: tkOp, text: op, pos: start})
				l.pos += 2
			default:
				switch c {
				case '(', ')', ',', ';', '=', '<', '>', '*', '+', '-', '.':
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
