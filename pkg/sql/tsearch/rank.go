package tsearch

import "math"

// Ranking, after PostgreSQL's ts_rank and ts_rank_cd.

// DefaultWeights are the scores of positions labelled D, C, B and A.
var DefaultWeights = [4]float64{0.1, 0.2, 0.4, 1.0}

// The bits of the normalization argument.
const (
	NormLogLength  = 1  // divide by 1 + log2 of the document length
	NormLength     = 2  // divide by the document length
	NormExtentDist = 4  // divide by the mean harmonic distance between extents (ts_rank_cd only)
	NormUnique     = 8  // divide by the number of unique words
	NormLogUnique  = 16 // divide by 1 + log2 of the number of unique words
	NormSelf       = 32 // divide by itself + 1
)

// A lexeme without positions ranks as one occurrence of weight D at the
// last possible position.
var noPositions = []Pos{{P: MaxPos, W: WeightD}}

func positionsOf(e Entry) []Pos {
	if len(e.Pos) == 0 {
		return noPositions
	}
	return e.Pos
}

// length is the number of words the vector records.
func (v Vector) length() int {
	n := 0
	for _, e := range v {
		n += len(positionsOf(e))
	}
	return n
}

func normalize(res float64, v Vector, norm int) float64 {
	if norm&NormLogLength != 0 && len(v) > 0 {
		res /= math.Log2(float64(v.length() + 1))
	}
	if norm&NormLength != 0 {
		if n := v.length(); n > 0 {
			res /= float64(n)
		}
	}
	if norm&NormUnique != 0 && len(v) > 0 {
		res /= float64(len(v))
	}
	if norm&NormLogUnique != 0 && len(v) > 0 {
		res /= math.Log2(float64(len(v) + 1))
	}
	if norm&NormSelf != 0 {
		res /= res + 1
	}
	return res
}

func wordDistance(w int) float64 {
	if w > 100 {
		return 1e-30
	}
	return 1.0 / (1.005 + 0.05*math.Exp(float64(w)/1.5-2))
}

func rankOr(w [4]float64, v Vector, ops []*Node) float64 {
	res := 0.0
	for _, op := range ops {
		lo, hi := entriesFor(v, op)
		for i := lo; i < hi; i++ {
			pos := positionsOf(v[i])
			resj, wjm, jm := 0.0, -1.0, 0
			for j, p := range pos {
				resj += w[p.W] / float64((j+1)*(j+1))
				if w[p.W] > wjm {
					wjm, jm = w[p.W], j
				}
			}
			// The heaviest occurrence counts in full; the others fall
			// off as 1/n², a series that sums to pi²/6.
			res += (wjm + resj - wjm/float64((jm+1)*(jm+1))) / 1.64493406685
		}
	}
	if len(ops) > 0 {
		res /= float64(len(ops))
	}
	return res
}

func rankAnd(w [4]float64, v Vector, ops []*Node) float64 {
	if len(ops) < 2 {
		return rankOr(w, v, ops)
	}
	res := -1.0
	seen := make([][]Pos, len(ops))
	for i, op := range ops {
		lo, hi := entriesFor(v, op)
		for e := lo; e < hi; e++ {
			seen[i] = positionsOf(v[e])
			stripped := len(v[e].Pos) == 0
			for k := 0; k < i; k++ {
				if seen[k] == nil {
					continue
				}
				kStripped := len(seen[k]) == 1 && &seen[k][0] == &noPositions[0]
				for _, pl := range seen[i] {
					for _, pk := range seen[k] {
						dist := int(pl.P) - int(pk.P)
						if dist < 0 {
							dist = -dist
						}
						if dist == 0 && !stripped && !kStripped {
							continue
						}
						if dist == 0 {
							dist = MaxPos
						}
						cur := math.Sqrt(w[pl.W] * w[pk.W] * wordDistance(dist))
						if res < 0 {
							res = cur
						} else {
							res = 1 - (1-res)*(1-cur)
						}
					}
				}
			}
		}
	}
	return res
}

// Rank is ts_rank: a score from how often the query's lexemes occur
// and, when the query requires them together, how close to each other.
// weights are the scores of D, C, B and A positions.
func Rank(weights [4]float64, v Vector, q Query, norm int) float64 {
	if len(v) == 0 || q.Root == nil {
		return 0
	}
	ops := q.Operands()
	var res float64
	if q.Root.Kind == NodeAnd || q.Root.Kind == NodePhrase {
		res = rankAnd(weights, v, ops)
	} else {
		res = rankOr(weights, v, ops)
	}
	if res < 0 {
		res = 1e-20
	}
	return normalize(res, v, norm)
}

// docPos is one occurrence in the document of a lexeme the query names.
type docPos struct {
	pos   int
	w     Weight
	entry int
}

// RankCD is ts_rank_cd, cover density ranking: the document is scanned
// for covers — shortest stretches of it that satisfy the query on their
// own — and each adds its weight divided by one plus the number of
// words in it the query did not ask for.
func RankCD(weights [4]float64, v Vector, q Query, norm int) float64 {
	if len(v) == 0 || q.Root == nil {
		return 0
	}
	var doc []docPos
	used := map[int]bool{}
	for _, op := range q.Operands() {
		lo, hi := entriesFor(v, op)
		for e := lo; e < hi; e++ {
			if used[e] {
				continue
			}
			used[e] = true
			for _, p := range positionsOf(v[e]) {
				doc = append(doc, docPos{pos: int(p.P), w: p.W, entry: e})
			}
		}
	}
	if len(doc) == 0 {
		return 0
	}
	// By position; a handful of entries, so a simple sort.
	for i := 1; i < len(doc); i++ {
		for j := i; j > 0 && doc[j].pos < doc[j-1].pos; j-- {
			doc[j], doc[j-1] = doc[j-1], doc[j]
		}
	}
	// satisfied: the words doc[lo..hi] alone satisfy the query.
	satisfied := func(lo, hi int) bool {
		b := newBuilder()
		for _, d := range doc[lo : hi+1] {
			_ = b.add(v[d.entry].Lexeme, Pos{P: uint16(d.pos), W: d.w})
		}
		return Match(b.vector(), q)
	}
	var wdoc, sumDist, prevExt float64
	extents := 0
	for start := 0; start < len(doc); {
		// The shortest stretch from start that satisfies the query,
		// then its latest beginning.
		end := -1
		for e := start; e < len(doc); e++ {
			if satisfied(start, e) {
				end = e
				break
			}
		}
		if end < 0 {
			break
		}
		begin := end
		for begin > start && !satisfied(begin, end) {
			begin--
		}
		invSum := 0.0
		for _, d := range doc[begin : end+1] {
			invSum += 1 / weights[d.w]
		}
		cpos := float64(end-begin+1) / invSum
		noise := (doc[end].pos - doc[begin].pos) - (end - begin)
		if noise < 0 {
			noise = (end - begin) / 2
		}
		wdoc += cpos / float64(1+noise)
		cur := float64(doc[end].pos+doc[begin].pos) / 2
		if extents > 0 && cur > prevExt {
			sumDist += 1 / (cur - prevExt)
		}
		prevExt = cur
		extents++
		start = begin + 1
	}
	if norm&NormExtentDist != 0 && extents > 1 && sumDist > 0 {
		wdoc /= float64(extents) / sumDist
	}
	return normalize(wdoc, v, norm&^NormExtentDist)
}
