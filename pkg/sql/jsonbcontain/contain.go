// Package jsonbcontain evaluates PostgreSQL's jsonb containment (@> and
// <@) against a document's stored text without decoding it.
//
// The right operand (the pattern) is decoded once into a small tree,
// with its object keys sorted and its numbers reduced to a comparable
// form. The left operand (the document) is walked as text, left to
// right, and the walk stops as soon as the answer is known: a required
// key whose value does not match fails the object, an array is done once
// every pattern element has found a containing element, and values the
// pattern does not name are skipped over without being decoded.
//
// The semantics are PostgreSQL's:
//
//   - an object contains an object when every pattern key is present
//     with a value that contains the pattern's value;
//   - an array contains an array when every pattern element is
//     contained by some element (order and multiplicity do not matter);
//   - at the top level only, an array contains a scalar it has as an
//     element ('[1,2]' @> '1'), but a scalar never contains an array;
//   - scalars compare by value, and numbers numerically (1 = 1.0 =
//     1e0), exactly, without going through float64.
//
// Stored jsonb text is canonical: compact, and with each object's keys
// unique (the last of duplicate keys won when it was parsed). The walk
// relies on that only for the early exits; when a document turns out to
// be malformed, ContainedIn reports that it could not decide, and the
// caller falls back to decoding.
package jsonbcontain

import (
	"encoding/json"
	"sort"
	"strings"
	"unicode/utf8"

	"github.com/sthorne/datax/pkg/util/decimal"
)

type kind uint8

const (
	kNull kind = iota
	kFalse
	kTrue
	kNum
	kStr
	kArr
	kObj
)

// node is one value of a compiled pattern.
type node struct {
	kind kind
	// str is a string's decoded value; fffd marks a string holding
	// U+FFFD, which a document string with invalid UTF-8 decodes to.
	str  string
	fffd bool
	// num is a number's comparable form, and numText its text.
	num     numKey
	numText string
	// keys (sorted, unique) and vals are an object's members; keysFFFD
	// marks an object one of whose keys holds U+FFFD.
	keys     []string
	vals     []node
	keysFFFD bool
	// elems are an array's elements.
	elems []node
}

// Pattern is a compiled right operand of @>.
type Pattern struct {
	root node
	v    any
}

// Value is the pattern as decoded: maps, slices, json.Number, string,
// bool and nil. Callers must not modify it.
func (p *Pattern) Value() any { return p.v }

// Compile decodes a jsonb value into a pattern. Duplicate object keys
// keep the last value, as jsonb input does.
func Compile(text string) (*Pattern, error) {
	var v any
	dec := json.NewDecoder(strings.NewReader(text))
	dec.UseNumber()
	if err := dec.Decode(&v); err != nil {
		return nil, err
	}
	p := &Pattern{v: v}
	build(&p.root, v)
	return p, nil
}

func build(n *node, v any) {
	switch t := v.(type) {
	case nil:
		n.kind = kNull
	case bool:
		n.kind = kFalse
		if t {
			n.kind = kTrue
		}
	case json.Number:
		n.kind, n.numText = kNum, string(t)
		n.num, _ = parseNum(n.numText)
		n.num.keepDigits(n.numText)
	case string:
		n.kind, n.str = kStr, t
		n.fffd = strings.ContainsRune(t, utf8.RuneError)
	case []any:
		n.kind = kArr
		n.elems = make([]node, len(t))
		for i, e := range t {
			build(&n.elems[i], e)
		}
	case map[string]any:
		n.kind = kObj
		n.keys = make([]string, 0, len(t))
		for k := range t {
			n.keys = append(n.keys, k)
			if strings.ContainsRune(k, utf8.RuneError) {
				n.keysFFFD = true
			}
		}
		sort.Strings(n.keys)
		n.vals = make([]node, len(n.keys))
		for i, k := range n.keys {
			build(&n.vals[i], t[k])
		}
	}
}

// ContainedIn reports whether the jsonb document (its stored text)
// contains the pattern. decided is false when the document is not
// well-formed JSON text where the walk looked; the caller then decides
// by decoding it.
func (p *Pattern) ContainedIn(doc string) (ok, decided bool) {
	m := matcher{s: doc}
	i := m.ws(0)
	if i >= len(doc) {
		return false, false
	}
	ok, _ = m.match(i, &p.root, true, false)
	if m.bad {
		return false, false
	}
	return ok, true
}

// Contains is doc @> pattern for two jsonb texts. decided is false when
// doc is malformed (see ContainedIn); err is pattern's decode error.
func Contains(doc, pattern string) (ok, decided bool, err error) {
	p, err := Compile(pattern)
	if err != nil {
		return false, false, err
	}
	ok, decided = p.ContainedIn(doc)
	return ok, decided, nil
}

// matcher walks one document's text.
type matcher struct {
	s   string
	bad bool
}

func (m *matcher) ws(i int) int {
	for i < len(m.s) {
		switch m.s[i] {
		case ' ', '\t', '\n', '\r':
			i++
		default:
			return i
		}
	}
	return i
}

// match reports whether the value at i contains p. end is the index
// just past the value; when needEnd is false it may be -1 (unknown).
func (m *matcher) match(i int, p *node, top, needEnd bool) (bool, int) {
	ok, end := m.match1(i, p, top)
	if end < 0 && needEnd && !m.bad {
		end = m.skipValue(i)
	}
	return ok, end
}

func (m *matcher) match1(i int, p *node, top bool) (bool, int) {
	if i >= len(m.s) {
		m.bad = true
		return false, -1
	}
	c := m.s[i]
	switch p.kind {
	case kObj:
		if c != '{' {
			return false, -1
		}
		return m.matchObject(i, p)
	case kArr:
		if c != '[' {
			return false, -1
		}
		return m.matchArray(i, p)
	}
	// A scalar pattern.
	switch c {
	case '{':
		return false, -1
	case '[':
		if !top {
			return false, -1
		}
		return m.arrayHasScalar(i, p)
	}
	return m.scalarEq(i, p)
}

// matchObject: the object at i (a '{') contains the object pattern p.
func (m *matcher) matchObject(i int, p *node) (bool, int) {
	n := len(p.keys)
	if n == 0 {
		return true, -1
	}
	var mask uint64
	var seen []bool
	if n > 64 {
		seen = make([]bool, n)
	}
	left := n
	pos := m.ws(i + 1)
	if pos < len(m.s) && m.s[pos] == '}' {
		return false, pos + 1
	}
	for {
		if pos >= len(m.s) || m.s[pos] != '"' {
			m.bad = true
			return false, -1
		}
		kend, esc := m.skipString(pos)
		if kend < 0 {
			return false, -1
		}
		key := m.s[pos+1 : kend-1]
		pos = m.ws(kend)
		if pos >= len(m.s) || m.s[pos] != ':' {
			m.bad = true
			return false, -1
		}
		pos = m.ws(pos + 1)
		idx := p.lookup(key, esc)
		if idx >= 0 {
			if seen != nil {
				if seen[idx] {
					idx = -1
				}
			} else if mask&(1<<uint(idx)) != 0 {
				idx = -1
			}
		}
		if idx < 0 {
			pos = m.skipValue(pos)
			if pos < 0 {
				return false, -1
			}
		} else {
			ok, end := m.match(pos, &p.vals[idx], false, true)
			if !ok || m.bad {
				return false, -1
			}
			if seen != nil {
				seen[idx] = true
			} else {
				mask |= 1 << uint(idx)
			}
			left--
			if left == 0 {
				return true, -1
			}
			pos = end
		}
		pos = m.ws(pos)
		if pos >= len(m.s) {
			m.bad = true
			return false, -1
		}
		switch m.s[pos] {
		case ',':
			pos = m.ws(pos + 1)
		case '}':
			return false, pos + 1
		default:
			m.bad = true
			return false, -1
		}
	}
}

// lookup finds a document key (raw text between its quotes; esc when it
// holds an escape) among the pattern object's keys.
func (p *node) lookup(raw string, esc bool) int {
	key := raw
	if esc || (p.keysFFFD && !utf8.ValidString(raw)) {
		var ok bool
		if key, ok = unquote(raw); !ok {
			return -1
		}
	}
	if len(p.keys) <= 8 {
		for i, k := range p.keys {
			if k == key {
				return i
			}
		}
		return -1
	}
	i := sort.SearchStrings(p.keys, key)
	if i < len(p.keys) && p.keys[i] == key {
		return i
	}
	return -1
}

// matchArray: the array at i (a '[') contains the array pattern p.
func (m *matcher) matchArray(i int, p *node) (bool, int) {
	n := len(p.elems)
	if n == 0 {
		return true, -1
	}
	var mask uint64
	var found []bool
	if n > 64 {
		found = make([]bool, n)
	}
	left := n
	pos := m.ws(i + 1)
	if pos < len(m.s) && m.s[pos] == ']' {
		return false, pos + 1
	}
	for {
		start, end := pos, -1
		for j := 0; j < n; j++ {
			if found != nil {
				if found[j] {
					continue
				}
			} else if mask&(1<<uint(j)) != 0 {
				continue
			}
			ok, e := m.match(start, &p.elems[j], false, end < 0)
			if m.bad {
				return false, -1
			}
			if end < 0 {
				end = e
			}
			if ok {
				if found != nil {
					found[j] = true
				} else {
					mask |= 1 << uint(j)
				}
				left--
				if left == 0 {
					return true, -1
				}
			}
		}
		pos = m.ws(end)
		if pos >= len(m.s) {
			m.bad = true
			return false, -1
		}
		switch m.s[pos] {
		case ',':
			pos = m.ws(pos + 1)
		case ']':
			return false, pos + 1
		default:
			m.bad = true
			return false, -1
		}
	}
}

// arrayHasScalar: the top-level array at i has an element equal to the
// scalar pattern p.
func (m *matcher) arrayHasScalar(i int, p *node) (bool, int) {
	pos := m.ws(i + 1)
	if pos < len(m.s) && m.s[pos] == ']' {
		return false, pos + 1
	}
	for {
		var ok bool
		var end int
		if pos < len(m.s) && (m.s[pos] == '{' || m.s[pos] == '[') {
			end = m.skipValue(pos)
		} else {
			ok, end = m.scalarEq(pos, p)
		}
		if m.bad || end < 0 {
			m.bad = true
			return false, -1
		}
		if ok {
			return true, -1
		}
		pos = m.ws(end)
		if pos >= len(m.s) {
			m.bad = true
			return false, -1
		}
		switch m.s[pos] {
		case ',':
			pos = m.ws(pos + 1)
		case ']':
			return false, pos + 1
		default:
			m.bad = true
			return false, -1
		}
	}
}

// scalarEq compares the scalar at i with the scalar pattern p; end is
// always known.
func (m *matcher) scalarEq(i int, p *node) (bool, int) {
	if i >= len(m.s) {
		m.bad = true
		return false, -1
	}
	switch c := m.s[i]; {
	case c == '"':
		end, esc := m.skipString(i)
		if end < 0 {
			return false, -1
		}
		if p.kind != kStr {
			return false, end
		}
		raw := m.s[i+1 : end-1]
		if !esc && raw == p.str {
			return true, end
		}
		if esc || (p.fffd && !utf8.ValidString(raw)) {
			s, ok := unquote(raw)
			return ok && s == p.str, end
		}
		return false, end
	case c == 't':
		return m.literal(i, "true", p.kind == kTrue)
	case c == 'f':
		return m.literal(i, "false", p.kind == kFalse)
	case c == 'n':
		return m.literal(i, "null", p.kind == kNull)
	case c == '-' || (c >= '0' && c <= '9'):
		end := i + 1
		for end < len(m.s) && isNumByte(m.s[end]) {
			end++
		}
		if p.kind != kNum {
			return false, end
		}
		tok := m.s[i:end]
		if tok == p.numText {
			return true, end
		}
		k, ok := parseNum(tok)
		if !ok {
			m.bad = true
			return false, -1
		}
		if k.slow || p.num.slow {
			return numEqualSlow(tok, p.numText), end
		}
		return k.equal(tok, &p.num), end
	}
	m.bad = true
	return false, -1
}

func (m *matcher) literal(i int, lit string, want bool) (bool, int) {
	if !strings.HasPrefix(m.s[i:], lit) {
		m.bad = true
		return false, -1
	}
	return want, i + len(lit)
}

func isNumByte(c byte) bool {
	return (c >= '0' && c <= '9') || c == '.' || c == 'e' || c == 'E' || c == '+' || c == '-'
}

// skipString returns the index just past the string starting at i (a
// '"'), and whether it holds an escape.
func (m *matcher) skipString(i int) (int, bool) {
	esc := false
	j := i + 1
	for {
		k := strings.IndexByte(m.s[j:], '"')
		if k < 0 {
			m.bad = true
			return -1, false
		}
		q := j + k
		if strings.IndexByte(m.s[j:q], '\\') >= 0 {
			esc = true
			// The quote is escaped when an odd run of backslashes
			// precedes it.
			b := 0
			for q-1-b > i && m.s[q-1-b] == '\\' {
				b++
			}
			if b%2 == 1 {
				j = q + 1
				continue
			}
		}
		return q + 1, esc
	}
}

// skipValue returns the index just past the value starting at i.
func (m *matcher) skipValue(i int) int {
	if i >= len(m.s) {
		m.bad = true
		return -1
	}
	switch m.s[i] {
	case '"':
		end, _ := m.skipString(i)
		return end
	case '{', '[':
		depth := 0
		for j := i; j < len(m.s); j++ {
			switch m.s[j] {
			case '"':
				end, _ := m.skipString(j)
				if end < 0 {
					return -1
				}
				j = end - 1
			case '{', '[':
				depth++
			case '}', ']':
				depth--
				if depth == 0 {
					return j + 1
				}
			}
		}
		m.bad = true
		return -1
	}
	j := i
	for j < len(m.s) {
		switch m.s[j] {
		case ',', '}', ']', ':', ' ', '\t', '\n', '\r':
			if j == i {
				m.bad = true
				return -1
			}
			return j
		}
		j++
	}
	return j
}

// unquote decodes the raw text of a JSON string (between its quotes).
func unquote(raw string) (string, bool) {
	var s string
	if err := json.Unmarshal([]byte(`"`+raw+`"`), &s); err != nil {
		return "", false
	}
	return s, true
}

// numKey is a number's comparable form: the value is ±digits × 10^exp,
// with digits free of leading and trailing zeros (empty for zero). slow
// marks a number whose exponent is beyond what the decimal type
// accepts; such a number compares as the decimal type would.
type numKey struct {
	neg  bool
	zero bool
	slow bool
	// The significant digits of a parsed text: [first, last] index into
	// the mantissa's digits (the integer part then the fraction), with
	// intStart/intLen/fracStart locating them in the text.
	intStart, intLen, fracStart int
	first, last                 int
	exp                         int64
	// digits holds the significant digits of a pattern number.
	digits string
}

// maxExp mirrors the decimal type's exponent range: a number past it
// fails decimal.Parse, and jsonb then compares it by its text.
const maxExp = 2000

// parseNum reduces a JSON number token. ok is false when tok is not a
// JSON number.
func parseNum(tok string) (k numKey, ok bool) {
	i := 0
	if i < len(tok) && tok[i] == '-' {
		k.neg = true
		i++
	}
	k.intStart = i
	for i < len(tok) && tok[i] >= '0' && tok[i] <= '9' {
		i++
	}
	k.intLen = i - k.intStart
	if k.intLen == 0 {
		return k, false
	}
	fracLen := 0
	k.fracStart = i
	if i < len(tok) && tok[i] == '.' {
		i++
		k.fracStart = i
		for i < len(tok) && tok[i] >= '0' && tok[i] <= '9' {
			i++
		}
		fracLen = i - k.fracStart
		if fracLen == 0 {
			return k, false
		}
	}
	var exp int64
	if i < len(tok) && (tok[i] == 'e' || tok[i] == 'E') {
		i++
		eneg := false
		if i < len(tok) && (tok[i] == '+' || tok[i] == '-') {
			eneg = tok[i] == '-'
			i++
		}
		es := i
		for i < len(tok) && tok[i] >= '0' && tok[i] <= '9' {
			if i-es < 18 {
				exp = exp*10 + int64(tok[i]-'0')
			}
			i++
		}
		if i == es {
			return k, false
		}
		if i-es >= 18 {
			k.slow = true
		}
		if eneg {
			exp = -exp
		}
	}
	if i != len(tok) {
		return k, false
	}
	// decimal.Parse bounds the exponent of the unscaled digits.
	if e := exp - int64(fracLen); e > maxExp || e < -maxExp {
		k.slow = true
	}
	n := k.intLen + fracLen
	k.first, k.last = -1, -1
	for d := 0; d < n; d++ {
		if k.digit(tok, d) != '0' {
			if k.first < 0 {
				k.first = d
			}
			k.last = d
		}
	}
	if k.first < 0 {
		k.zero = true
		k.neg = false
		return k, true
	}
	k.exp = exp - int64(fracLen) + int64(n-1-k.last)
	return k, true
}

// keepDigits copies a pattern number's significant digits out of its
// text (a document number is compared in place, by equal).
func (k *numKey) keepDigits(tok string) {
	if k.zero || k.first < 0 {
		return
	}
	b := make([]byte, 0, k.last-k.first+1)
	for d := k.first; d <= k.last; d++ {
		b = append(b, k.digit(tok, d))
	}
	k.digits = string(b)
}

func (k *numKey) digit(tok string, d int) byte {
	if d < k.intLen {
		return tok[k.intStart+d]
	}
	return tok[k.fracStart+d-k.intLen]
}

// equal compares a parsed document number (its token tok) with a
// pattern number.
func (k *numKey) equal(tok string, p *numKey) bool {
	if k.zero || p.zero {
		return k.zero && p.zero
	}
	if k.neg != p.neg || k.exp != p.exp || k.last-k.first+1 != len(p.digits) {
		return false
	}
	for d := k.first; d <= k.last; d++ {
		if k.digit(tok, d) != p.digits[d-k.first] {
			return false
		}
	}
	return true
}

// numEqualSlow compares two numbers as the decimal type does, by text
// when it refuses either.
func numEqualSlow(a, b string) bool {
	if a == b {
		return true
	}
	ad, aerr := decimal.Parse(a)
	bd, berr := decimal.Parse(b)
	if aerr != nil || berr != nil {
		return false
	}
	return decimal.Cmp(ad, bd) == 0
}
