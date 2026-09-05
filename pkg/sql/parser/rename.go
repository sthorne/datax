package parser

import (
	"strings"
	"unicode"
)

// RenameColumnRefs rewrites every reference to column old in the SQL
// text of a stored expression (a CHECK constraint) to name new, keeping
// everything else byte for byte: ALTER TABLE ... RENAME COLUMN keeps the
// constraints that mention the column valid. A word followed by "(" is a
// function call and a word after "." is a qualified reference's tail
// (rewritten too, since it names the column); string literals and
// quoted identifiers of other names are untouched.
func RenameColumnRefs(text, old, new string) (string, error) {
	toks, err := lex(text)
	if err != nil {
		return "", err
	}
	var b strings.Builder
	last := 0
	for i, t := range toks {
		if t.kind != tkIdent || t.text != old {
			continue
		}
		if i+1 < len(toks) && toks[i+1].kind == tkOp && toks[i+1].text == "(" {
			continue // a call
		}
		end := t.pos + 1
		if text[t.pos] == '"' {
			for end < len(text) && text[end] != '"' {
				end++
			}
			end++ // the closing quote
		} else {
			for end < len(text) && (unicode.IsLetter(rune(text[end])) || unicode.IsDigit(rune(text[end])) || text[end] == '_') {
				end++
			}
		}
		b.WriteString(text[last:t.pos])
		b.WriteString(quoteIdentIfNeeded(new))
		last = end
	}
	b.WriteString(text[last:])
	return b.String(), nil
}

// quoteIdentIfNeeded renders an identifier as SQL: bare when it is a
// plain lower-case word, double-quoted otherwise.
func quoteIdentIfNeeded(name string) string {
	plain := name != ""
	for i, r := range name {
		if !(r == '_' || unicode.IsLower(r) || (i > 0 && unicode.IsDigit(r))) {
			plain = false
			break
		}
	}
	if plain && !keywords[strings.ToUpper(name)] {
		return name
	}
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}
