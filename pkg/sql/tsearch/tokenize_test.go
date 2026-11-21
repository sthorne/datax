package tsearch

import (
	"math/rand"
	"strings"
	"testing"
	"unicode/utf8"
)

// tokenizeRef is Tokenize as it was before the tokenizer walked without
// allocating: the reference for TestTokenizeMatchesReference.
func tokenizeRef(s string) []Token {
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

// TestTokenizeMatchesReference: the same tokens, kinds, texts and
// offsets as the reference, over random runs of letters, digits,
// punctuation, addresses, host names, numbers, combining marks, invalid
// UTF-8 and the letters whose lower case is longer.
func TestTokenizeMatchesReference(t *testing.T) {
	r := rand.New(rand.NewSource(5))
	pieces := []string{"a", "B", "z9", "42", "3.14", "1.", ".", "-", "+", "_", "@", "x@y.com", "a.b.cd", "a-b.cd", "A.TXT", " ", "'",
		"é", "Ⱥ", "İ", "ß", "\xff", "́", "日本", "K", "١٢", "..", "-x", "word", "Word42", "q", "\t"}
	for i := 0; i < 100000; i++ {
		var b strings.Builder
		for k := r.Intn(15); k >= 0; k-- {
			b.WriteString(pieces[r.Intn(len(pieces))])
		}
		s := b.String()
		got, want := Tokenize(s), tokenizeRef(s)
		if len(got) != len(want) {
			t.Fatalf("Tokenize(%q):\n got  %+v\n want %+v", s, got, want)
		}
		for j := range got {
			if got[j] != want[j] {
				t.Fatalf("Tokenize(%q):\n got  %+v\n want %+v", s, got, want)
			}
		}
	}
}
