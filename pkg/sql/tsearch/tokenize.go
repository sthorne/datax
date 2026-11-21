package tsearch

import (
	"unicode"
	"unicode/utf8"
)

// TokenKind says what the tokenizer recognized.
type TokenKind int

const (
	// TokWord is a run of letters, digits and combining marks.
	TokWord TokenKind = iota
	// TokNumber is an unsigned decimal number, with an optional
	// fraction ("3.14").
	TokNumber
	// TokEmail is an e-mail address.
	TokEmail
	// TokHost is a dotted host or file name ("example.com", "a.txt").
	TokHost
)

// Token is one token of a text: its lower-cased text and where it was
// found (byte offsets, for headlines).
type Token struct {
	Text       string
	Kind       TokenKind
	Start, End int
}

// Tokenize splits a text into tokens. The rules, in the order they are
// tried at each position:
//
//   - An e-mail address (ASCII letters, digits and ._+- before an @,
//     a host name after it) is one token.
//   - A host name — two or more labels of ASCII letters, digits and
//     inner hyphens joined by single dots, the last label two or more
//     letters — is one token ("example.com", "notes.txt").
//   - A run of ASCII digits, optionally followed by a dot and more
//     digits, is one token ("42", "3.14"), unless letters follow it.
//   - Any other run of letters, digits and combining marks (in the
//     Unicode sense) is one token.
//
// Everything else — spaces, punctuation, symbols, underscores, hyphens,
// apostrophes — separates tokens and is dropped. Tokens are lower-cased
// with Unicode case mapping; accents are kept.
//
// PostgreSQL's parser recognizes more (URLs with their paths, signed
// and scientific numbers, tags, hyphenated compounds emitted both whole
// and in parts). Here "well-known" is the two tokens "well" and
// "known", and a URL is its scheme, its host and the words of its path.
func Tokenize(s string) []Token {
	var out []Token
	t := tokenizer{s: s}
	for t.next() {
		out = append(out, Token{Text: string(t.lower(nil)), Kind: t.kind, Start: t.start, End: t.end})
	}
	return out
}

// tokenizer walks a text token by token without allocating; lower
// writes the current token's text to a buffer the caller provides (and
// may reuse). Tokenize is the same walk, collected.
type tokenizer struct {
	s string
	i int
	// The current token: its kind and its byte offsets in s.
	kind       TokenKind
	start, end int
}

// next advances to the next token; false at the end of the text.
func (t *tokenizer) next() bool {
	s := t.s
	for i := t.i; i < len(s); {
		if c := s[i]; c < utf8.RuneSelf {
			// ASCII: a letter or a digit starts a token, anything else
			// separates.
			if !isASCIIAlnum(c) {
				i++
				continue
			}
		} else if r, size := utf8.DecodeRuneInString(s[i:]); !isWordRune(r) || (r == utf8.RuneError && size == 1) {
			i += size
			continue
		}
		if s[i] < utf8.RuneSelf {
			// The common token: a run of ASCII letters and digits that
			// ends the text or ends at an ASCII byte that cannot carry
			// it on into an address, a host name or a fraction. Then it
			// is a number or a word, and the scans below need not run.
			j, digits := i, true
			for j < len(s) && isASCIIAlnum(s[j]) {
				digits = digits && s[j] <= '9'
				j++
			}
			if j == len(s) || s[j] < utf8.RuneSelf && s[j] != '@' && s[j] != '.' && s[j] != '_' && s[j] != '+' && s[j] != '-' {
				if digits {
					t.set(TokNumber, i, j)
				} else {
					t.set(TokWord, i, j)
				}
				return true
			}
		}
		if end := scanEmail(s, i); end > 0 {
			t.set(TokEmail, i, end)
			return true
		}
		if end := scanHost(s, i); end > 0 && !wordRuneAt(s, end) {
			t.set(TokHost, i, end)
			return true
		}
		if end := scanNumber(s, i); end > 0 {
			t.set(TokNumber, i, end)
			return true
		}
		j := i
		for j < len(s) {
			if c := s[j]; c < utf8.RuneSelf {
				if !isASCIIAlnum(c) {
					break
				}
				j++
				continue
			}
			r, size := utf8.DecodeRuneInString(s[j:])
			if !isWordRune(r) || (r == utf8.RuneError && size == 1) {
				break
			}
			j += size
		}
		t.set(TokWord, i, j)
		return true
	}
	t.i = len(s)
	return false
}

func (t *tokenizer) set(kind TokenKind, start, end int) {
	t.kind, t.start, t.end, t.i = kind, start, end, end
}

// lower appends the current token's text to dst: lower-cased as
// strings.ToLower lower-cases it (a token is valid UTF-8, so rune by
// rune with unicode.ToLower), but a number as it is written.
func (t *tokenizer) lower(dst []byte) []byte {
	if t.kind == TokNumber {
		return append(dst, t.s[t.start:t.end]...)
	}
	for k := t.start; k < t.end; {
		c := t.s[k]
		if c < utf8.RuneSelf {
			if 'A' <= c && c <= 'Z' {
				c += 'a' - 'A'
			}
			dst = append(dst, c)
			k++
			continue
		}
		r, size := utf8.DecodeRuneInString(t.s[k:])
		dst = utf8.AppendRune(dst, unicode.ToLower(r))
		k += size
	}
	return dst
}

func isWordRune(r rune) bool {
	return unicode.IsLetter(r) || unicode.IsDigit(r) || unicode.IsMark(r)
}

func wordRuneAt(s string, i int) bool {
	if i >= len(s) {
		return false
	}
	r, size := utf8.DecodeRuneInString(s[i:])
	return isWordRune(r) && !(r == utf8.RuneError && size == 1)
}

func isASCIIAlnum(c byte) bool {
	return c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9'
}

func isASCIILetter(c byte) bool {
	return c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z'
}

// The longest host name and local part looked for (DNS's and SMTP's
// limits). They also bound the look-ahead: without them a megabyte of
// "a.a.a." would be scanned to its end from every one of its letters.
const (
	maxHostLen  = 253
	maxLocalLen = 64
)

// scanHost returns the end of the host name starting at i, or 0.
func scanHost(s string, i int) int {
	labels := 0
	end := 0
	j := i
	for {
		start := j
		for j < len(s) && j-i < maxHostLen && (isASCIIAlnum(s[j]) || (s[j] == '-' && j > start)) {
			j++
		}
		// A label does not end with a hyphen.
		for j > start && s[j-1] == '-' {
			j--
		}
		if j == start {
			break
		}
		labels++
		lastAlpha := j-start >= 2
		for k := start; k < j && lastAlpha; k++ {
			lastAlpha = isASCIILetter(s[k])
		}
		if labels >= 2 && lastAlpha {
			end = j
		}
		if j+1 < len(s) && s[j] == '.' && isASCIIAlnum(s[j+1]) {
			j++
			continue
		}
		break
	}
	return end
}

// scanEmail returns the end of the e-mail address starting at i, or 0.
func scanEmail(s string, i int) int {
	j := i
	for j < len(s) && j-i <= maxLocalLen && (isASCIIAlnum(s[j]) || s[j] == '.' || s[j] == '_' || s[j] == '+' || s[j] == '-') {
		j++
	}
	if j == i || j >= len(s) || s[j] != '@' {
		return 0
	}
	end := scanHost(s, j+1)
	if end == 0 || wordRuneAt(s, end) {
		return 0
	}
	return end
}

// scanNumber returns the end of the number starting at i, or 0 when
// what starts there is not a number on its own ("3rd", "42nd").
func scanNumber(s string, i int) int {
	j := i
	for j < len(s) && s[j] >= '0' && s[j] <= '9' {
		j++
	}
	if j == i {
		return 0
	}
	if j+1 < len(s) && s[j] == '.' && s[j+1] >= '0' && s[j+1] <= '9' {
		k := j + 1
		for k < len(s) && s[k] >= '0' && s[k] <= '9' {
			k++
		}
		if !wordRuneAt(s, k) {
			return k
		}
	}
	if wordRuneAt(s, j) {
		return 0
	}
	return j
}
