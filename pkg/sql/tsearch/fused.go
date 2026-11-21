package tsearch

import "bytes"

// Matching a text against a query without building its vector: the
// filter `to_tsvector(cfg, text) @@ query` needs only to know whether
// the query holds, so the text's lexemes are tested against the query's
// as they come, and the walk stops as soon as the answer is known.
//
// The answer is Match(ToVector(cfg, text), query), exactly. Outside a
// phrase a query asks only which of its lexemes the text holds, and
// every position of to_tsvector's vector weighs D, so an operand holds
// when its lexeme (or, for a prefix, some lexeme with it) occurs and it
// is not restricted to weights that leave D out. While the text is
// being read an operand not seen yet is unknown rather than false, and
// the query is evaluated in three-valued logic each time an operand is
// first seen: once it is TRUE or FALSE whatever the unknowns turn out
// to be, the rest of the text cannot change it.
//
// A phrase needs positions. For a query with one, the occurrences of
// the lexemes the query names are kept, positions and all, and Match
// runs on the vector they make: the only entries Match ever looks up.

// Matcher is a query prepared for MatchText. It is immutable, and safe
// to share between goroutines.
type Matcher struct {
	q Query
	// terms are the query's distinct lexemes, a prefix and an exact
	// lexeme of the same text apart.
	terms []matchTerm
	// exact indexes the exact terms by lexeme, when there are enough of
	// them for a map to beat a scan.
	exact map[string]int
	// nodes is the query in post-order; the root is last.
	nodes  []fusedNode
	phrase bool
	// first holds the first bytes of the terms. Normalizing keeps a
	// token's first byte — lower case is applied before, stemming only
	// rewrites the end of a word, and the one rule that touches the
	// start (a leading apostrophe goes) never applies to a token, which
	// has none — so a token starting with any other byte cannot become
	// a lexeme the query names, and is not normalized at all.
	first [256]bool
}

type matchTerm struct {
	lexeme string
	prefix bool
}

type fusedNode struct {
	kind NodeKind
	// term is a NodeVal's term; never is set when the operand's weights
	// leave out D, which no position of to_tsvector's carries.
	term  int
	never bool
	l, r  int
}

// exactMapMin is the number of exact terms from which they are looked
// up in a map rather than compared one by one.
const exactMapMin = 8

// NewMatcher prepares a query for MatchText.
func NewMatcher(q Query) *Matcher {
	m := &Matcher{q: q}
	if q.Root == nil {
		return m
	}
	idx := map[matchTerm]int{}
	var walk func(n *Node) int
	walk = func(n *Node) int {
		cn := fusedNode{kind: n.Kind}
		switch n.Kind {
		case NodeVal:
			t := matchTerm{lexeme: n.Lexeme, prefix: n.Prefix}
			i, ok := idx[t]
			if !ok {
				i = len(m.terms)
				idx[t] = i
				m.terms = append(m.terms, t)
			}
			cn.term = i
			cn.never = n.Weights != 0 && n.Weights&(1<<WeightD) == 0
		case NodeNot:
			cn.l = walk(n.L)
		default:
			if n.Kind == NodePhrase {
				m.phrase = true
			}
			cn.l = walk(n.L)
			cn.r = walk(n.R)
		}
		m.nodes = append(m.nodes, cn)
		return len(m.nodes) - 1
	}
	walk(q.Root)
	for _, t := range m.terms {
		if t.lexeme == "" {
			// No query parses to one; a node built by hand may.
			for c := range m.first {
				m.first[c] = true
			}
			continue
		}
		m.first[t.lexeme[0]] = true
	}
	exact := 0
	for _, t := range m.terms {
		if !t.prefix {
			exact++
		}
	}
	if exact >= exactMapMin {
		m.exact = make(map[string]int, exact)
		for i, t := range m.terms {
			if !t.prefix {
				m.exact[t.lexeme] = i
			}
		}
	}
	return m
}

// Query is the query the matcher was made from.
func (m *Matcher) Query() Query { return m.q }

// fusedMaxText is the longest text matched without its vector. A
// lexeme is at most half again as long as the text it came from (lower
// case can take more UTF-8 bytes than upper: Ⱥ is two, ⱥ three), so a
// text this short cannot reduce to more than MaxVectorBytes of lexemes,
// and to_tsvector could not have refused it. A longer one goes through
// ToVector, which reports the error when there is one.
const fusedMaxText = MaxVectorBytes / 2

// MatchableText reports whether MatchText reads the text itself: when
// it is short enough that to_tsvector could not refuse it.
func MatchableText(text string) bool { return len(text) <= fusedMaxText }

// MatchText reports whether the text, reduced under the configuration,
// satisfies the query: Match(ToVector(cfg, text), q), error included.
func (m *Matcher) MatchText(cfg *Config, text string) (bool, error) {
	if !MatchableText(text) {
		v, err := ToVector(cfg, text)
		if err != nil {
			return false, err
		}
		return Match(v, m.q), nil
	}
	if m.q.Root == nil {
		return false, nil
	}
	if m.phrase {
		return m.matchPhrase(cfg, text), nil
	}
	return m.matchBool(cfg, text), nil
}

// Three-valued truth, ordered so that AND is the minimum and OR the
// maximum.
const (
	kFalse uint8 = iota
	kUnknown
	kTrue
)

// matchBool is MatchText for a query without a phrase.
func (m *Matcher) matchBool(cfg *Config, text string) bool {
	var small [64]bool
	var found []bool
	if len(m.terms) <= len(small) {
		found = small[:len(m.terms)]
	} else {
		found = make([]bool, len(m.terms))
	}
	var vals [32]uint8
	verdict := m.eval(found, kUnknown, vals[:0])
	if verdict != kUnknown {
		return verdict == kTrue
	}
	var buf [256]byte
	w := newLexemes(cfg, text)
	w.only = &m.first
	for verdict == kUnknown {
		lex, _, ok := w.next(buf[:])
		if !ok {
			break
		}
		if m.see(lex, found) {
			verdict = m.eval(found, kUnknown, vals[:0])
		}
	}
	if verdict == kUnknown {
		verdict = m.eval(found, kFalse, vals[:0])
	}
	return verdict == kTrue
}

// see marks the terms a lexeme satisfies, and reports whether one of
// them was not marked before.
func (m *Matcher) see(lex []byte, found []bool) bool {
	fresh := false
	if m.exact != nil {
		if i, ok := m.exact[string(lex)]; ok && !found[i] {
			found[i], fresh = true, true
		}
	}
	for i, t := range m.terms {
		if found[i] || (m.exact != nil && !t.prefix) {
			continue
		}
		if t.prefix {
			if bytes.HasPrefix(lex, []byte(t.lexeme)) {
				found[i], fresh = true, true
			}
		} else if string(lex) == t.lexeme {
			found[i], fresh = true, true
		}
	}
	return fresh
}

// names reports whether the query names a lexeme: one of its terms is
// the lexeme or a prefix of it.
func (m *Matcher) names(lex []byte) bool {
	if m.exact != nil {
		if _, ok := m.exact[string(lex)]; ok {
			return true
		}
	}
	for _, t := range m.terms {
		if t.prefix {
			if bytes.HasPrefix(lex, []byte(t.lexeme)) {
				return true
			}
		} else if m.exact == nil && string(lex) == t.lexeme {
			return true
		}
	}
	return false
}

// eval evaluates the query over the terms found so far; a term not
// found is unseen (kUnknown while the text is being read, kFalse once
// it all has been). vals is scratch space.
func (m *Matcher) eval(found []bool, unseen uint8, vals []uint8) uint8 {
	for _, n := range m.nodes {
		var v uint8
		switch n.kind {
		case NodeVal:
			switch {
			case n.never:
				v = kFalse
			case found[n.term]:
				v = kTrue
			default:
				v = unseen
			}
		case NodeNot:
			v = kTrue - vals[n.l]
		case NodeAnd:
			v = min(vals[n.l], vals[n.r])
		case NodeOr:
			v = max(vals[n.l], vals[n.r])
		}
		vals = append(vals, v)
	}
	return vals[len(vals)-1]
}

// matchPhrase is MatchText for a query with a phrase: Match over the
// vector of the lexemes the query names.
func (m *Matcher) matchPhrase(cfg *Config, text string) bool {
	var occ occurrences
	var buf [256]byte
	w := newLexemes(cfg, text)
	w.only = &m.first
	for {
		lex, pos, ok := w.next(buf[:])
		if !ok {
			break
		}
		if m.names(lex) {
			occ.add(lex, pos)
		}
	}
	v, _ := occ.vector()
	return Match(v, m.q)
}
