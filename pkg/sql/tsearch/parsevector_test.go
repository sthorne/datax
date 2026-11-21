package tsearch

import (
	"fmt"
	"math/rand"
	"strconv"
	"strings"
	"testing"
)

// parseVectorRef is ParseVector as it was before its sorted fast path:
// every entry through a builder. The reference for TestParseVectorFast.
func parseVectorRef(s string) (Vector, error) {
	b := newBuilder()
	i := 0
	for {
		for i < len(s) && isSpace(s[i]) {
			i++
		}
		if i >= len(s) {
			break
		}
		lex, next, err := scanLexeme(s, i, false)
		if err != nil {
			return nil, fmt.Errorf("syntax error in tsvector: %q", s)
		}
		i = next
		if len(lex) > MaxLexemeLen {
			return nil, fmt.Errorf("word is too long (%d bytes, max %d bytes)", len(lex), MaxLexemeLen)
		}
		var pos []Pos
		if i < len(s) && s[i] == ':' {
			i++
			for {
				start := i
				for i < len(s) && s[i] >= '0' && s[i] <= '9' {
					i++
				}
				n, err := strconv.Atoi(s[start:i])
				if err != nil || n < 1 {
					return nil, fmt.Errorf("syntax error in tsvector: %q", s)
				}
				if n > MaxPos {
					n = MaxPos
				}
				p := Pos{P: uint16(n)}
				if i < len(s) {
					if w, ok := ParseWeight(s[i]); ok {
						p.W = w
						i++
					}
				}
				pos = append(pos, p)
				if i < len(s) && s[i] == ',' {
					i++
					continue
				}
				break
			}
			if i < len(s) && !isSpace(s[i]) {
				return nil, fmt.Errorf("syntax error in tsvector: %q", s)
			}
		}
		if err := b.add(lex, pos...); err != nil {
			return nil, err
		}
	}
	return b.vector(), nil
}

// TestParseVectorFast: ParseVector reads every literal as the builder
// does — sorted or not, lexemes repeated, positions out of order,
// repeated or past MaxPositions, quoted and escaped — with the same
// errors.
func TestParseVectorFast(t *testing.T) {
	r := rand.New(rand.NewSource(3))
	lexemes := []string{"a", "b", "cat", "cats", "dog", "'it''s'", `'back\\slash'`, "'two words'", `x\:y`, "zz", "é"}
	for n := 0; n < 20000; n++ {
		var b strings.Builder
		k := r.Intn(8)
		for i := 0; i < k; i++ {
			if i > 0 {
				b.WriteString(" ")
			}
			b.WriteString(lexemes[r.Intn(len(lexemes))])
			if r.Intn(3) > 0 {
				b.WriteByte(':')
				np := 1 + r.Intn(4)
				if r.Intn(30) == 0 {
					np = 300
				}
				for j := 0; j < np; j++ {
					if j > 0 {
						b.WriteByte(',')
					}
					b.WriteString(strconv.Itoa(1 + r.Intn(20000)))
					if r.Intn(3) == 0 {
						b.WriteByte("ABCDabcd"[r.Intn(8)])
					}
				}
			}
		}
		in := b.String()
		if r.Intn(10) == 0 && len(in) > 0 {
			in = in[:r.Intn(len(in))] // often malformed
		}
		got, gerr := ParseVector(in)
		want, werr := parseVectorRef(in)
		if fmt.Sprint(gerr) != fmt.Sprint(werr) {
			t.Fatalf("ParseVector(%q): error %v, want %v", in, gerr, werr)
		}
		if gerr == nil && got.String() != want.String() {
			t.Fatalf("ParseVector(%q)\n got  %s\n want %s", in, got, want)
		}
	}
	// The size limit, in order and out of it.
	var b strings.Builder
	for i := 0; b.Len() <= MaxVectorBytes*5/4; i++ {
		fmt.Fprintf(&b, "w%07d ", i)
	}
	for _, in := range []string{b.String(), "zzz " + b.String()} {
		_, gerr := ParseVector(in)
		_, werr := parseVectorRef(in)
		if gerr == nil || fmt.Sprint(gerr) != fmt.Sprint(werr) {
			t.Fatalf("over the limit: error %v, want %v", gerr, werr)
		}
	}
}
