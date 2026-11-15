package tsearch

import (
	"strings"
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
	for i := 0; i < len(s); {
		r, size := utf8.DecodeRuneInString(s[i:])
		if !isWordRune(r) || (r == utf8.RuneError && size == 1) {
			i += size
			continue
		}
		if end := scanEmail(s, i); end > 0 {
			out = append(out, Token{Text: strings.ToLower(s[i:end]), Kind: TokEmail, Start: i, End: end})
			i = end
			continue
		}
		if end := scanHost(s, i); end > 0 && !wordRuneAt(s, end) {
			out = append(out, Token{Text: strings.ToLower(s[i:end]), Kind: TokHost, Start: i, End: end})
			i = end
			continue
		}
		if end := scanNumber(s, i); end > 0 {
			out = append(out, Token{Text: s[i:end], Kind: TokNumber, Start: i, End: end})
			i = end
			continue
		}
		j := i
		for j < len(s) {
			r, size := utf8.DecodeRuneInString(s[j:])
			if !isWordRune(r) || (r == utf8.RuneError && size == 1) {
				break
			}
			j += size
		}
		out = append(out, Token{Text: strings.ToLower(s[i:j]), Kind: TokWord, Start: i, End: j})
		i = j
	}
	return out
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
