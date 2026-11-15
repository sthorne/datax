package tsearch

// Matching a vector against a query (the @@ operator), after
// PostgreSQL's TS_execute.
//
// Outside a phrase the query is plain boolean logic over "the vector
// holds this lexeme". An operand restricted to weights holds when one
// of the lexeme's positions carries one of them — and, as PostgreSQL
// has always had it, when the lexeme has no positions at all (a
// stripped vector matches whatever the weights).
//
// A phrase needs positions. Each side is evaluated to the set of
// positions it ENDS at, and "l <N> r" keeps the positions of r that
// have an l exactly N before them (N plus r's own width, when r is a
// phrase itself). A negation inside a phrase is the complement of a
// position set, kept as the set and a flag; the complement includes the
// positions before the first word and after the last, so "cat <-> !hat"
// matches a document that ends in "cat". A lexeme without positions
// cannot be placed, so a phrase never matches a stripped vector.
//
// One oddity is PostgreSQL's and is kept: when the two sides of an & or
// | inside a phrase span different widths ("(a <-> b | c) <-> d"), they
// are lined up by where they start, and a side that matches nowhere
// lends no width. to_tsquery never builds such a query in PostgreSQL
// (it rewrites it into a | of phrases first); here it can be written,
// and means what PostgreSQL's executor would make of it.

// Match reports whether the vector satisfies the query. The empty query
// matches nothing.
func Match(v Vector, q Query) bool {
	if q.Root == nil {
		return false
	}
	return matchNode(v, q.Root)
}

func matchNode(v Vector, n *Node) bool {
	switch n.Kind {
	case NodeVal:
		return matchVal(v, n)
	case NodeNot:
		return !matchNode(v, n.L)
	case NodeAnd:
		return matchNode(v, n.L) && matchNode(v, n.R)
	case NodeOr:
		return matchNode(v, n.L) || matchNode(v, n.R)
	case NodePhrase:
		d, ok := phraseExec(v, n)
		return ok && (len(d.pos) > 0 || d.negate)
	}
	return false
}

// weightOK: the entry satisfies the operand's weight restriction.
func weightOK(e Entry, weights uint8) bool {
	if weights == 0 || len(e.Pos) == 0 {
		return true
	}
	for _, p := range e.Pos {
		if weights&(1<<p.W) != 0 {
			return true
		}
	}
	return false
}

// entriesFor returns the range of entries an operand names: one for an
// exact lexeme, every lexeme with the prefix for a prefix operand.
func entriesFor(v Vector, n *Node) (lo, hi int) {
	if n.Prefix {
		return v.prefixRange(n.Lexeme)
	}
	if i := v.find(n.Lexeme); i >= 0 {
		return i, i + 1
	}
	return 0, 0
}

func matchVal(v Vector, n *Node) bool {
	lo, hi := entriesFor(v, n)
	for i := lo; i < hi; i++ {
		if weightOK(v[i], n.Weights) {
			return true
		}
	}
	return false
}

// phraseData is the positions a phrase operand ends at. With negate it
// stands for every position EXCEPT pos. width is how many positions the
// operand spans before its last one.
type phraseData struct {
	pos    []int
	negate bool
	width  int
}

// valPositions collects the operand's positions (several lexemes for a
// prefix), sorted and distinct.
func valPositions(v Vector, n *Node) []int {
	lo, hi := entriesFor(v, n)
	var out []int
	for i := lo; i < hi; i++ {
		for _, p := range v[i].Pos {
			if n.Weights == 0 || n.Weights&(1<<p.W) != 0 {
				out = append(out, int(p.P))
			}
		}
	}
	if hi-lo > 1 {
		out = sortDedupInts(out)
	}
	return out
}

func sortDedupInts(a []int) []int {
	if len(a) < 2 {
		return a
	}
	// Insertion sort: the lists are a few positions long.
	for i := 1; i < len(a); i++ {
		for j := i; j > 0 && a[j] < a[j-1]; j-- {
			a[j], a[j-1] = a[j-1], a[j]
		}
	}
	out := a[:1]
	for _, x := range a[1:] {
		if x != out[len(out)-1] {
			out = append(out, x)
		}
	}
	return out
}

const (
	emitBoth  = 1
	emitLeft  = 2
	emitRight = 4
)

// phraseOutput merges two position lists, l shifted by loff and r by
// roff, and keeps the positions found in both, only in l, or only in r,
// as emit says.
func phraseOutput(l, r []int, loff, roff, emit int) []int {
	var out []int
	i, j := 0, 0
	for i < len(l) || j < len(r) {
		// An exhausted side sorts after everything.
		const inf = int(^uint(0) >> 1)
		lp, rp := inf, inf
		if i < len(l) {
			lp = l[i] + loff
		}
		if j < len(r) {
			rp = r[j] + roff
		}
		switch {
		case lp < rp:
			if emit&emitLeft != 0 {
				out = append(out, lp)
			}
			i++
		case lp == rp:
			if emit&emitBoth != 0 {
				out = append(out, rp)
			}
			i++
			j++
		default:
			if emit&emitRight != 0 {
				out = append(out, rp)
			}
			j++
		}
	}
	return out
}

// phraseExec evaluates a node in a phrase context. ok is false when the
// node cannot match anywhere.
func phraseExec(v Vector, n *Node) (phraseData, bool) {
	switch n.Kind {
	case NodeVal:
		pos := valPositions(v, n)
		return phraseData{pos: pos}, len(pos) > 0
	case NodeNot:
		d, ok := phraseExec(v, n.L)
		if !ok {
			// Nothing to exclude: every position qualifies.
			return phraseData{negate: true}, true
		}
		if len(d.pos) > 0 {
			d.negate = !d.negate
			return d, true
		}
		if d.negate {
			// The complement of "everywhere".
			return phraseData{}, false
		}
		return phraseData{negate: true}, true
	case NodePhrase, NodeAnd:
		l, lok := phraseExec(v, n.L)
		if !lok {
			return phraseData{}, false
		}
		r, rok := phraseExec(v, n.R)
		if !rok {
			return phraseData{}, false
		}
		var loff, roff, width int
		if n.Kind == NodePhrase {
			// l must end Distance before r starts.
			loff = n.Distance + r.width
			width = l.width + n.Distance + r.width
		} else {
			// Both at the same place, measured from their ends.
			width = max(l.width, r.width)
			loff, roff = width-l.width, width-r.width
		}
		out := phraseData{width: width}
		switch {
		case l.negate && r.negate:
			// !l & !r is !(l | r)
			out.pos = phraseOutput(l.pos, r.pos, loff, roff, emitBoth|emitLeft|emitRight)
			out.negate = true
			return out, true
		case l.negate:
			out.pos = phraseOutput(l.pos, r.pos, loff, roff, emitRight)
		case r.negate:
			out.pos = phraseOutput(l.pos, r.pos, loff, roff, emitLeft)
		default:
			out.pos = phraseOutput(l.pos, r.pos, loff, roff, emitBoth)
		}
		return out, len(out.pos) > 0
	case NodeOr:
		l, lok := phraseExec(v, n.L)
		r, rok := phraseExec(v, n.R)
		if !lok && !rok {
			return phraseData{}, false
		}
		if !lok {
			l = phraseData{}
		}
		if !rok {
			r = phraseData{}
		}
		width := max(l.width, r.width)
		loff, roff := width-l.width, width-r.width
		out := phraseData{width: width}
		switch {
		case l.negate && r.negate:
			// !l | !r is !(l & r)
			out.pos = phraseOutput(l.pos, r.pos, loff, roff, emitBoth)
			out.negate = true
		case l.negate:
			// !l | r is !(l & !r)
			out.pos = phraseOutput(l.pos, r.pos, loff, roff, emitLeft)
			out.negate = true
		case r.negate:
			out.pos = phraseOutput(l.pos, r.pos, loff, roff, emitRight)
			out.negate = true
		default:
			out.pos = phraseOutput(l.pos, r.pos, loff, roff, emitBoth|emitLeft|emitRight)
		}
		return out, len(out.pos) > 0 || out.negate
	}
	return phraseData{}, false
}
