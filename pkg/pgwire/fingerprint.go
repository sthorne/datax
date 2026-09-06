package pgwire

import "strings"

// Fingerprint collapses a statement's literals so that statements of the
// same shape share one key: "UPDATE accounts SET balance = 5 WHERE id =
// 3" and "UPDATE accounts SET balance = 9 WHERE id = 7" both become
// "UPDATE accounts SET balance = ? WHERE id = ?".
//
// It exists so the console can say which statement shapes are producing
// serialization failures (issue #154) rather than listing a thousand
// distinct texts. It is lexical, not a parse: identifiers, keywords and
// punctuation survive as written, and only literals are replaced. That
// is enough to group a retry storm and cheap enough to run on the
// statement path.
//
// The shape still carries the table and column names the statement
// named, so it is treated as statement text is treated everywhere else:
// admin-gated.
func Fingerprint(text string) string {
	var b strings.Builder
	b.Grow(len(text))
	runes := []rune(text)
	space := false // a run of whitespace is pending
	for i := 0; i < len(runes); {
		c := runes[i]
		switch {
		case c == ' ' || c == '\t' || c == '\n' || c == '\r':
			space = true
			i++
			continue
		case c == '\'' || c == '"' || c == '`':
			// A quoted run. Single quotes are string literals and
			// collapse; double quotes and backticks are quoted
			// identifiers and are kept.
			j := i + 1
			for j < len(runes) {
				if runes[j] == c {
					// A doubled quote is an escaped one, not the end.
					if j+1 < len(runes) && runes[j+1] == c {
						j += 2
						continue
					}
					break
				}
				if c == '\'' && runes[j] == '\\' && j+1 < len(runes) {
					j += 2
					continue
				}
				j++
			}
			if j < len(runes) {
				j++ // the closing quote
			}
			flushSpace(&b, &space)
			if c == '\'' {
				b.WriteByte('?')
			} else {
				b.WriteString(string(runes[i:j]))
			}
			i = j
			continue
		case c >= '0' && c <= '9':
			// A number, but only where one can start: "t1" is an
			// identifier, not a literal.
			if i > 0 && isIdentRune(runes[i-1]) {
				break
			}
			j := i
			for j < len(runes) && (runes[j] >= '0' && runes[j] <= '9' || runes[j] == '.' ||
				runes[j] == 'e' || runes[j] == 'E' ||
				((runes[j] == '+' || runes[j] == '-') && j > i && (runes[j-1] == 'e' || runes[j-1] == 'E'))) {
				j++
			}
			flushSpace(&b, &space)
			b.WriteByte('?')
			i = j
			continue
		case c == '$':
			// A placeholder is already a shape; render it as one so a
			// prepared statement and its inlined twin agree.
			j := i + 1
			for j < len(runes) && runes[j] >= '0' && runes[j] <= '9' {
				j++
			}
			if j > i+1 {
				flushSpace(&b, &space)
				b.WriteByte('?')
				i = j
				continue
			}
		}
		flushSpace(&b, &space)
		b.WriteRune(c)
		i++
	}
	return collapseLists(b.String())
}

func flushSpace(b *strings.Builder, space *bool) {
	if *space && b.Len() > 0 {
		b.WriteByte(' ')
	}
	*space = false
}

func isIdentRune(c rune) bool {
	return c == '_' || c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9'
}

// collapseLists folds "(?, ?, ?)" down to "(?)" so an IN list of three
// and an IN list of three hundred are one shape rather than two.
func collapseLists(s string) string {
	for {
		i := strings.Index(s, "?, ?")
		if i < 0 {
			return s
		}
		j := i + 1
		for strings.HasPrefix(s[j:], ", ?") {
			j += 3
		}
		s = s[:i+1] + s[j:]
	}
}
