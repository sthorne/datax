package tsearch

import (
	"fmt"
	"strconv"
	"strings"
)

// NodeKind is what a tsquery node is.
type NodeKind int

const (
	// NodeVal is an operand: a lexeme, possibly a prefix (:*) and
	// possibly restricted to weights.
	NodeVal NodeKind = iota
	// NodeNot is !x.
	NodeNot
	// NodeAnd is l & r.
	NodeAnd
	// NodeOr is l | r.
	NodeOr
	// NodePhrase is l <N> r: r starts Distance positions after l.
	NodePhrase
)

// Node is a node of a tsquery.
type Node struct {
	Kind   NodeKind
	Lexeme string
	Prefix bool
	// Weights is a set of weights (bit 1<<Weight) the operand is
	// restricted to; 0 is no restriction.
	Weights  uint8
	Distance int
	L, R     *Node
}

// Query is a tsquery. A nil Root is the empty query, which is what a
// search made only of stop words reduces to; it matches nothing.
type Query struct {
	Root *Node
}

// priority orders the operators for parsing and printing: | binds
// loosest, then &, then <->, then !.
func (n *Node) priority() int {
	switch n.Kind {
	case NodeOr:
		return 1
	case NodeAnd:
		return 2
	case NodePhrase:
		return 3
	case NodeNot:
		return 4
	}
	return 5
}

// String renders the query in PostgreSQL's text form:
// 'fat' & ( 'rat' | 'cat' ) <-> !'dog':*A.
func (q Query) String() string {
	if q.Root == nil {
		return ""
	}
	var b strings.Builder
	q.Root.write(&b, 0)
	return b.String()
}

func (n *Node) write(b *strings.Builder, parent int) {
	switch n.Kind {
	case NodeVal:
		quoteLexeme(b, n.Lexeme)
		if n.Prefix || n.Weights != 0 {
			b.WriteByte(':')
			if n.Prefix {
				b.WriteByte('*')
			}
			for _, w := range []Weight{WeightA, WeightB, WeightC, WeightD} {
				if n.Weights&(1<<w) != 0 {
					b.WriteByte(w.letter())
				}
			}
		}
	case NodeNot:
		b.WriteByte('!')
		if n.L.Kind == NodeVal || n.L.Kind == NodeNot {
			n.L.write(b, n.priority())
		} else {
			b.WriteString("( ")
			n.L.write(b, 0)
			b.WriteString(" )")
		}
	default:
		paren := n.priority() < parent
		if paren {
			b.WriteString("( ")
		}
		n.L.write(b, n.priority())
		switch n.Kind {
		case NodeAnd:
			b.WriteString(" & ")
		case NodeOr:
			b.WriteString(" | ")
		case NodePhrase:
			if n.Distance == 1 {
				b.WriteString(" <-> ")
			} else {
				fmt.Fprintf(b, " <%d> ", n.Distance)
			}
		}
		// A phrase is not associative (the distances are measured from
		// different words), so a phrase on the right keeps its
		// parentheses.
		if n.Kind == NodePhrase && n.R.Kind == NodePhrase {
			b.WriteString("( ")
			n.R.write(b, 0)
			b.WriteString(" )")
		} else {
			n.R.write(b, n.priority())
		}
		if paren {
			b.WriteString(" )")
		}
	}
}

// Operands lists the query's distinct operands in the order they are
// written. Two operands are the same when their lexeme and prefix flag
// are.
func (q Query) Operands() []*Node {
	var out []*Node
	var walk func(n *Node)
	walk = func(n *Node) {
		if n == nil {
			return
		}
		if n.Kind == NodeVal {
			for _, o := range out {
				if o.Lexeme == n.Lexeme && o.Prefix == n.Prefix {
					return
				}
			}
			out = append(out, n)
			return
		}
		walk(n.L)
		walk(n.R)
	}
	walk(q.Root)
	return out
}

// NumNodes counts the query's nodes, operators included (numnode).
func (q Query) NumNodes() int {
	var count func(n *Node) int
	count = func(n *Node) int {
		if n == nil {
			return 0
		}
		return 1 + count(n.L) + count(n.R)
	}
	return count(q.Root)
}

// ---------------------------------------------------------------------------
// Parsing.

// operandFunc turns the text of one operand into a query fragment. For
// a tsquery literal that is the lexeme itself; for to_tsquery it is the
// operand normalized by a configuration, which may be several lexemes
// (a phrase) or none (a stop word: a nil node with ok).
type operandFunc func(text string, prefix bool, weights uint8) (*Node, error)

// maxQueryDepth bounds nesting, so a hostile query cannot exhaust the
// stack.
const maxQueryDepth = 200

type queryParser struct {
	s       string
	i       int
	operand operandFunc
	depth   int
}

func (p *queryParser) errf() error {
	return fmt.Errorf("syntax error in tsquery: %q", p.s)
}

func (p *queryParser) skipSpace() {
	for p.i < len(p.s) && isSpace(p.s[p.i]) {
		p.i++
	}
}

// stop marks an operand that normalized to nothing. It stays in the
// tree until the whole query is read, so that the phrase distances
// around it can be widened when it is removed (see clean).
var stopNode = &Node{Kind: NodeVal}

func (p *queryParser) parseOr() (*Node, error) {
	l, err := p.parseAnd()
	if err != nil {
		return nil, err
	}
	for {
		p.skipSpace()
		if p.i >= len(p.s) || p.s[p.i] != '|' {
			return l, nil
		}
		p.i++
		r, err := p.parseAnd()
		if err != nil {
			return nil, err
		}
		l = &Node{Kind: NodeOr, L: l, R: r}
	}
}

func (p *queryParser) parseAnd() (*Node, error) {
	l, err := p.parsePhrase()
	if err != nil {
		return nil, err
	}
	for {
		p.skipSpace()
		if p.i >= len(p.s) || p.s[p.i] != '&' {
			return l, nil
		}
		p.i++
		r, err := p.parsePhrase()
		if err != nil {
			return nil, err
		}
		l = &Node{Kind: NodeAnd, L: l, R: r}
	}
}

func (p *queryParser) parsePhrase() (*Node, error) {
	l, err := p.parseUnary()
	if err != nil {
		return nil, err
	}
	for {
		p.skipSpace()
		if p.i >= len(p.s) || p.s[p.i] != '<' {
			return l, nil
		}
		end := strings.IndexByte(p.s[p.i:], '>')
		if end < 0 {
			return nil, p.errf()
		}
		dist := 1
		if inner := p.s[p.i+1 : p.i+end]; inner != "-" {
			n, err := strconv.Atoi(inner)
			if err != nil || n < 0 {
				return nil, p.errf()
			}
			if n > MaxDistance {
				return nil, fmt.Errorf("distance in phrase operator must be an integer value between zero and %d inclusive", MaxDistance)
			}
			dist = n
		}
		p.i += end + 1
		r, err := p.parseUnary()
		if err != nil {
			return nil, err
		}
		l = &Node{Kind: NodePhrase, Distance: dist, L: l, R: r}
	}
}

func (p *queryParser) parseUnary() (*Node, error) {
	p.skipSpace()
	if p.i >= len(p.s) {
		return nil, p.errf()
	}
	switch p.s[p.i] {
	case '!':
		p.i++
		p.depth++
		if p.depth > maxQueryDepth {
			return nil, fmt.Errorf("tsquery is nested too deeply")
		}
		kid, err := p.parseUnary()
		p.depth--
		if err != nil {
			return nil, err
		}
		return &Node{Kind: NodeNot, L: kid}, nil
	case '(':
		p.i++
		p.depth++
		if p.depth > maxQueryDepth {
			return nil, fmt.Errorf("tsquery is nested too deeply")
		}
		n, err := p.parseOr()
		p.depth--
		if err != nil {
			return nil, err
		}
		p.skipSpace()
		if p.i >= len(p.s) || p.s[p.i] != ')' {
			return nil, p.errf()
		}
		p.i++
		return n, nil
	case '&', '|', ')', '<', ':':
		return nil, p.errf()
	}
	text, next, err := scanLexeme(p.s, p.i, true)
	if err != nil {
		return nil, p.errf()
	}
	p.i = next
	prefix := false
	var weights uint8
	if p.i < len(p.s) && p.s[p.i] == ':' {
		p.i++
		start := p.i
		for p.i < len(p.s) {
			c := p.s[p.i]
			if c == '*' {
				prefix = true
			} else if w, ok := ParseWeight(c); ok {
				weights |= 1 << w
			} else {
				break
			}
			p.i++
		}
		if p.i == start {
			return nil, p.errf()
		}
	}
	return p.operand(text, prefix, weights)
}

func parseQueryWith(s string, operand operandFunc) (Query, error) {
	if strings.TrimSpace(s) == "" {
		return Query{}, nil
	}
	p := &queryParser{s: s, operand: operand}
	root, err := p.parseOr()
	if err != nil {
		return Query{}, err
	}
	p.skipSpace()
	if p.i != len(p.s) {
		return Query{}, p.errf()
	}
	root, _, _ = clean(root)
	return Query{Root: root}, nil
}

// ParseQuery reads the text form of a tsquery. Operands are lexemes as
// written (bare or quoted like a tsvector's), optionally followed by
// :* for a prefix match and weight letters; the operators are ! (not),
// <-> and <N> (followed by, N positions later), & and |, in that order
// of binding, with parentheses. No normalization is applied.
func ParseQuery(s string) (Query, error) {
	return parseQueryWith(s, func(text string, prefix bool, weights uint8) (*Node, error) {
		if len(text) > MaxLexemeLen {
			return nil, fmt.Errorf("word is too long in tsquery: %q", s)
		}
		return &Node{Kind: NodeVal, Lexeme: text, Prefix: prefix, Weights: weights}, nil
	})
}

// phraseOf chains the lexemes of one piece of text into a phrase, each
// as far from the previous one as their tokens were; stop words widen
// the gap. Nothing but stop words gives stopNode.
func phraseOf(cfg *Config, text string, prefix bool, weights uint8) *Node {
	var root *Node
	last := 0
	for i, tok := range Tokenize(text) {
		lex, ok := cfg.Lexeme(tok)
		if !ok || len(lex) > MaxLexemeLen {
			continue
		}
		val := &Node{Kind: NodeVal, Lexeme: lex, Prefix: prefix, Weights: weights}
		if root == nil {
			root = val
		} else {
			root = &Node{Kind: NodePhrase, Distance: i - last, L: root, R: val}
		}
		last = i
	}
	if root == nil {
		return stopNode
	}
	return root
}

// ToQuery parses a tsquery whose operands are normalized by the
// configuration (to_tsquery). An operand that tokenizes to several
// lexemes becomes a phrase of them; a stop word is removed, and the
// phrase distances around it grow to keep the remaining words as far
// apart as they were written.
func ToQuery(cfg *Config, s string) (Query, error) {
	return parseQueryWith(s, func(text string, prefix bool, weights uint8) (*Node, error) {
		return phraseOf(cfg, text, prefix, weights), nil
	})
}

// PlainToQuery is plainto_tsquery: the text's lexemes, all required.
func PlainToQuery(cfg *Config, text string) Query {
	var root *Node
	for _, tok := range Tokenize(text) {
		lex, ok := cfg.Lexeme(tok)
		if !ok || len(lex) > MaxLexemeLen {
			continue
		}
		val := &Node{Kind: NodeVal, Lexeme: lex}
		if root == nil {
			root = val
		} else {
			root = &Node{Kind: NodeAnd, L: root, R: val}
		}
	}
	return Query{Root: root}
}

// PhraseToQuery is phraseto_tsquery: the text's lexemes in order, at
// the distances they were written.
func PhraseToQuery(cfg *Config, text string) Query {
	root, _, _ := clean(phraseOf(cfg, text, false, 0))
	return Query{Root: root}
}

// WebsearchToQuery is websearch_to_tsquery, the syntax of a search box:
// words are required, "quoted text" is a phrase, the word or between
// two terms makes them alternatives, and a - in front of a word or a
// quoted phrase excludes it. Any other punctuation separates words. It
// never fails: what cannot be understood is ignored.
func WebsearchToQuery(cfg *Config, s string) Query {
	var ors []*Node // the alternatives
	var cur *Node   // the conjunction being read
	flush := func() {
		if cur != nil {
			ors = append(ors, cur)
			cur = nil
		}
	}
	add := func(n *Node, negate bool) {
		n, _, _ = clean(n)
		if n == nil {
			return
		}
		if negate {
			n = &Node{Kind: NodeNot, L: n}
		}
		if cur == nil {
			cur = n
		} else {
			cur = &Node{Kind: NodeAnd, L: cur, R: n}
		}
	}
	i := 0
	for i < len(s) {
		c := s[i]
		if isSpace(c) {
			i++
			continue
		}
		negate := false
		if c == '-' && i+1 < len(s) && (s[i+1] == '"' || wordRuneAt(s, i+1)) {
			negate = true
			i++
			c = s[i]
		}
		if c == '"' {
			end := strings.IndexByte(s[i+1:], '"')
			if end < 0 {
				// A quote that is never closed quotes nothing: what
				// follows it is read as plain words.
				i++
				continue
			}
			add(phraseOf(cfg, s[i+1:i+1+end], false, 0), negate)
			i += end + 2
			continue
		}
		// A run up to the next space or quote: one or more words.
		j := i
		for j < len(s) && !isSpace(s[j]) && s[j] != '"' {
			j++
		}
		word := s[i:j]
		i = j
		if !negate && strings.EqualFold(word, "or") {
			flush()
			continue
		}
		for _, tok := range Tokenize(word) {
			if lex, ok := cfg.Lexeme(tok); ok && len(lex) <= MaxLexemeLen {
				add(&Node{Kind: NodeVal, Lexeme: lex}, negate)
			}
		}
	}
	flush()
	var root *Node
	for _, n := range ors {
		if root == nil {
			root = n
		} else {
			root = &Node{Kind: NodeOr, L: root, R: n}
		}
	}
	return Query{Root: root}
}

// clean removes the stop-word placeholders. Removing a word from a
// phrase must not move its neighbours closer: ladd and radd are the
// positions a removed word occupied at the left and right edge of what
// is returned, which the enclosing phrase adds to its distance.
func clean(n *Node) (out *Node, ladd, radd int) {
	if n == nil || n == stopNode {
		return nil, 0, 0
	}
	switch n.Kind {
	case NodeVal:
		return n, 0, 0
	case NodeNot:
		kid, _, _ := clean(n.L)
		if kid == nil {
			return nil, 0, 0
		}
		return &Node{Kind: NodeNot, L: kid}, 0, 0
	}
	dist := 0
	if n.Kind == NodePhrase {
		dist = n.Distance
	}
	l, lladd, lradd := clean(n.L)
	r, rladd, rradd := clean(n.R)
	switch {
	case l == nil && r == nil:
		if n.Kind != NodePhrase {
			return nil, 0, 0
		}
		gap := lladd + dist + rladd
		return nil, gap, gap
	case l == nil:
		if n.Kind != NodePhrase {
			return r, 0, 0
		}
		return r, lladd + dist + rladd, rradd
	case r == nil:
		if n.Kind != NodePhrase {
			return l, 0, 0
		}
		return l, lladd, lradd + dist + rradd
	}
	if n.Kind != NodePhrase {
		return &Node{Kind: n.Kind, L: l, R: r}, 0, 0
	}
	return &Node{Kind: NodePhrase, Distance: lradd + dist + rladd, L: l, R: r}, lladd, rradd
}
