package tsearch

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
)

// Weight labels a position: D (the default, not written) up to A.
type Weight uint8

const (
	WeightD Weight = iota
	WeightC
	WeightB
	WeightA
)

// ParseWeight reads a weight letter (either case).
func ParseWeight(c byte) (Weight, bool) {
	switch c {
	case 'A', 'a':
		return WeightA, true
	case 'B', 'b':
		return WeightB, true
	case 'C', 'c':
		return WeightC, true
	case 'D', 'd':
		return WeightD, true
	}
	return 0, false
}

func (w Weight) letter() byte { return "DCBA"[w] }

// Pos is one occurrence of a lexeme.
type Pos struct {
	P uint16
	W Weight
}

// Entry is a lexeme and where it occurs. Pos is sorted by position and
// distinct; it is empty for a lexeme without positions (a stripped
// vector, a literal written without them).
type Entry struct {
	Lexeme string
	Pos    []Pos
}

// Vector is a tsvector: entries sorted by lexeme (byte order), each
// lexeme once.
type Vector []Entry

// builder accumulates a vector in any order.
type builder struct {
	m     map[string][]Pos
	bytes int
}

func newBuilder() *builder { return &builder{m: map[string][]Pos{}} }

func (b *builder) add(lexeme string, pos ...Pos) error {
	cur, seen := b.m[lexeme]
	if !seen {
		b.bytes += len(lexeme)
		if b.bytes > MaxVectorBytes {
			return fmt.Errorf("string is too long for tsvector (%d bytes, max %d bytes)", b.bytes, MaxVectorBytes)
		}
	}
	b.m[lexeme] = append(cur, pos...)
	return nil
}

func (b *builder) vector() Vector {
	out := make(Vector, 0, len(b.m))
	for lex, pos := range b.m {
		out = append(out, Entry{Lexeme: lex, Pos: normalizePos(pos)})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Lexeme < out[j].Lexeme })
	return out
}

// normalizePos sorts positions, keeps one (the heaviest) per position
// and at most MaxPositions of them.
func normalizePos(pos []Pos) []Pos {
	if len(pos) == 0 {
		return nil
	}
	sort.Slice(pos, func(i, j int) bool {
		if pos[i].P != pos[j].P {
			return pos[i].P < pos[j].P
		}
		return pos[i].W > pos[j].W
	})
	out := pos[:1]
	for _, p := range pos[1:] {
		if p.P != out[len(out)-1].P {
			out = append(out, p)
		}
	}
	if len(out) > MaxPositions {
		out = out[:MaxPositions]
	}
	return out
}

// ToVector reduces a text to a vector under a configuration
// (to_tsvector).
func ToVector(cfg *Config, text string) (Vector, error) {
	b := newBuilder()
	for i, tok := range Tokenize(text) {
		lex, ok := cfg.Lexeme(tok)
		if !ok || len(lex) > MaxLexemeLen {
			continue // a stop word, or a word too long to be indexed
		}
		pos := i + 1
		if pos > MaxPos {
			pos = MaxPos
		}
		if err := b.add(lex, Pos{P: uint16(pos)}); err != nil {
			return nil, err
		}
	}
	return b.vector(), nil
}

// ParseVector reads the text form of a tsvector: lexemes separated by
// white space, each bare or in single quotes (a quote is doubled, a
// backslash takes the next character literally), optionally followed by
// a colon and a comma-separated list of positions, each with an
// optional weight letter:
//
//	'a fat':2,5A cat:3 rat
//
// The lexemes are taken as they are written: no normalization.
func ParseVector(s string) (Vector, error) {
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

func isSpace(c byte) bool {
	return c == ' ' || c == '\t' || c == '\n' || c == '\r' || c == '\f' || c == '\v'
}

// scanLexeme reads one lexeme starting at i: quoted, or bare up to
// white space, a colon, or (in a query) an operator character.
func scanLexeme(s string, i int, query bool) (string, int, error) {
	var b strings.Builder
	if s[i] == '\'' {
		i++
		for {
			if i >= len(s) {
				return "", 0, fmt.Errorf("unterminated quoted lexeme")
			}
			switch c := s[i]; {
			case c == '\'' && i+1 < len(s) && s[i+1] == '\'':
				b.WriteByte('\'')
				i += 2
			case c == '\'':
				if b.Len() == 0 {
					return "", 0, fmt.Errorf("empty lexeme")
				}
				return b.String(), i + 1, nil
			case c == '\\' && i+1 < len(s):
				b.WriteByte(s[i+1])
				i += 2
			default:
				b.WriteByte(c)
				i++
			}
		}
	}
	for i < len(s) {
		c := s[i]
		if isSpace(c) || c == ':' || c == '\'' {
			break
		}
		if query && (c == '&' || c == '|' || c == '!' || c == '(' || c == ')' || c == '<') {
			break
		}
		if c == '\\' && i+1 < len(s) {
			b.WriteByte(s[i+1])
			i += 2
			continue
		}
		b.WriteByte(c)
		i++
	}
	if b.Len() == 0 {
		return "", 0, fmt.Errorf("empty lexeme")
	}
	return b.String(), i, nil
}

func quoteLexeme(b *strings.Builder, lex string) {
	b.WriteByte('\'')
	for i := 0; i < len(lex); i++ {
		switch c := lex[i]; c {
		case '\'':
			b.WriteString("''")
		case '\\':
			b.WriteString(`\\`)
		default:
			b.WriteByte(c)
		}
	}
	b.WriteByte('\'')
}

// String renders the vector in PostgreSQL's text form:
// 'cat':3 'fat':2,5A.
func (v Vector) String() string {
	var b strings.Builder
	for i, e := range v {
		if i > 0 {
			b.WriteByte(' ')
		}
		quoteLexeme(&b, e.Lexeme)
		for j, p := range e.Pos {
			if j == 0 {
				b.WriteByte(':')
			} else {
				b.WriteByte(',')
			}
			b.WriteString(strconv.Itoa(int(p.P)))
			if p.W != WeightD {
				b.WriteByte(p.W.letter())
			}
		}
	}
	return b.String()
}

// find returns the index of the lexeme's entry, or -1.
func (v Vector) find(lexeme string) int {
	i := sort.Search(len(v), func(i int) bool { return v[i].Lexeme >= lexeme })
	if i < len(v) && v[i].Lexeme == lexeme {
		return i
	}
	return -1
}

// prefixRange returns the entries [lo, hi) whose lexemes start with
// prefix.
func (v Vector) prefixRange(prefix string) (lo, hi int) {
	lo = sort.Search(len(v), func(i int) bool { return v[i].Lexeme >= prefix })
	hi = lo
	for hi < len(v) && strings.HasPrefix(v[hi].Lexeme, prefix) {
		hi++
	}
	return lo, hi
}

// Lexemes lists the vector's lexemes, sorted.
func (v Vector) Lexemes() []string {
	out := make([]string, len(v))
	for i, e := range v {
		out[i] = e.Lexeme
	}
	return out
}

// SetWeight labels every position with w (setweight). Lexemes without
// positions have nothing to label.
func (v Vector) SetWeight(w Weight) Vector {
	out := make(Vector, len(v))
	for i, e := range v {
		pos := make([]Pos, len(e.Pos))
		for j, p := range e.Pos {
			pos[j] = Pos{P: p.P, W: w}
		}
		out[i] = Entry{Lexeme: e.Lexeme, Pos: pos}
	}
	return out
}

// Strip drops positions and weights (strip).
func (v Vector) Strip() Vector {
	out := make(Vector, len(v))
	for i, e := range v {
		out[i] = Entry{Lexeme: e.Lexeme}
	}
	return out
}

// Concat appends o to v (the || operator): o's positions follow v's
// last one, so the result is the vector of the two texts joined.
func (v Vector) Concat(o Vector) (Vector, error) {
	shift := 0
	for _, e := range v {
		if n := len(e.Pos); n > 0 && int(e.Pos[n-1].P) > shift {
			shift = int(e.Pos[n-1].P)
		}
	}
	b := newBuilder()
	for _, e := range v {
		if err := b.add(e.Lexeme, append([]Pos(nil), e.Pos...)...); err != nil {
			return nil, err
		}
	}
	for _, e := range o {
		pos := make([]Pos, len(e.Pos))
		for j, p := range e.Pos {
			n := int(p.P) + shift
			if n > MaxPos {
				n = MaxPos
			}
			pos[j] = Pos{P: uint16(n), W: p.W}
		}
		if err := b.add(e.Lexeme, pos...); err != nil {
			return nil, err
		}
	}
	return b.vector(), nil
}
