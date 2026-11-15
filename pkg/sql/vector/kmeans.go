package vector

import (
	"math"
	"math/rand"
	"sync"
)

// Centroids is the trained state of one IVF index build: the cluster
// centres every row's vector is filed under and every query is routed
// by. It is immutable for the life of the build — a row's entry is
// deleted by recomputing which list it was filed under, so a centre
// that moved would strand the entries filed before it did.
type Centroids struct {
	Metric Metric
	// Centers holds one vector per list, all of the index's dimension.
	// For the cosine and inner-product metrics they are unit vectors:
	// the clustering is by direction (spherical k-means), and both
	// filing and probing rank lists by inner product with the centre.
	Centers [][]float32
}

// score is how well list i suits v, larger being better.
func (c *Centroids) score(i int, v []float32) float64 {
	if c.Metric == L2 {
		return -L2Squared(v, c.Centers[i])
	}
	return Dot(v, c.Centers[i])
}

// Assign is the list a vector is filed under: the nearest centre, the
// lowest-numbered one on a tie. It is a pure function of the vector
// and the centres, which is what lets a delete find the entry an
// insert wrote.
func (c *Centroids) Assign(v []float32) int {
	best, bestScore := 0, math.Inf(-1)
	for i := range c.Centers {
		if s := c.score(i, v); s > bestScore {
			best, bestScore = i, s
		}
	}
	return best
}

// Probe returns the n lists nearest q, nearest first (all of them when
// n is not below the number of lists).
func (c *Centroids) Probe(q []float32, n int) []int {
	if n >= len(c.Centers) {
		n = len(c.Centers)
	}
	type scored struct {
		list  int
		score float64
	}
	// A bounded insertion keeps the n best without sorting every list.
	top := make([]scored, 0, n)
	for i := range c.Centers {
		s := c.score(i, q)
		if len(top) == n && s <= top[n-1].score {
			continue
		}
		if len(top) < n {
			top = append(top, scored{})
		}
		j := len(top) - 1
		for ; j > 0 && top[j-1].score < s; j-- {
			top[j] = top[j-1]
		}
		top[j] = scored{i, s}
	}
	out := make([]int, len(top))
	for i, t := range top {
		out[i] = t.list
	}
	return out
}

// trainIterations bounds Lloyd's algorithm. The centres stop moving
// much after the first few rounds, and an index build should not be
// held up polishing them: recall is decided far more by how many
// lists a query probes.
const trainIterations = 12

// Train clusters the sample into at most k centres for the metric.
// Fewer distinct sample vectors than k give fewer centres; an empty
// sample gives the single centre at the origin, so that an index built
// on an empty table has one list holding everything — exact, and no
// faster than a scan until it is rebuilt over real data. stop is
// polled between rounds so a cancelled statement does not finish its
// training first. The result depends only on the sample and the seed.
func Train(sample [][]float32, dims, k int, metric Metric, seed int64, stop func() bool) *Centroids {
	out := &Centroids{Metric: metric}
	pts := sample
	if metric != L2 {
		// Direction is all that matters: train on unit vectors, and
		// leave out the zero vector, which has none.
		pts = make([][]float32, 0, len(sample))
		for _, v := range sample {
			if Norm(v) > 0 {
				pts = append(pts, Normalize(v))
			}
		}
	}
	if len(pts) == 0 || k < 1 {
		out.Centers = [][]float32{make([]float32, dims)}
		return out
	}
	if k > len(pts) {
		k = len(pts)
	}
	rng := rand.New(rand.NewSource(seed))
	sq := func(a, b []float32) float64 { return L2Squared(a, b) }

	// k-means++ seeding: each next centre is drawn with probability
	// proportional to its squared distance from the centres so far,
	// which spreads them over the clusters instead of the densest one.
	centers := make([][]float32, 0, k)
	centers = append(centers, clone(pts[rng.Intn(len(pts))]))
	dist := make([]float64, len(pts))
	for i, p := range pts {
		dist[i] = sq(p, centers[0])
	}
	for len(centers) < k {
		var total float64
		for _, d := range dist {
			total += d
		}
		if total == 0 {
			break // every remaining point coincides with a centre
		}
		r := rng.Float64() * total
		pick := len(pts) - 1
		for i, d := range dist {
			if r -= d; r <= 0 {
				pick = i
				break
			}
		}
		c := clone(pts[pick])
		centers = append(centers, c)
		for i, p := range pts {
			if d := sq(p, c); d < dist[i] {
				dist[i] = d
			}
		}
	}
	k = len(centers)

	assign := make([]int, len(pts))
	for i := range assign {
		assign[i] = -1
	}
	sums := make([][]float64, k)
	for i := range sums {
		sums[i] = make([]float64, dims)
	}
	counts := make([]int, k)
	for iter := 0; iter < trainIterations; iter++ {
		if stop != nil && stop() {
			break
		}
		moved := assignAll(pts, centers, assign)
		if iter > 0 && moved*1000 < len(pts) {
			break // fewer than one point in a thousand changed list
		}
		for i := range sums {
			counts[i] = 0
			for j := range sums[i] {
				sums[i][j] = 0
			}
		}
		for i, p := range pts {
			a := assign[i]
			counts[a]++
			for j, x := range p {
				sums[a][j] += float64(x)
			}
		}
		for i := range centers {
			if counts[i] == 0 {
				// A centre that lost every point takes a random one,
				// rather than staying where nothing is.
				copy(centers[i], pts[rng.Intn(len(pts))])
				continue
			}
			for j := range centers[i] {
				centers[i][j] = float32(sums[i][j] / float64(counts[i]))
			}
			if metric != L2 {
				if n := Norm(centers[i]); n > 0 {
					copy(centers[i], Normalize(centers[i]))
				}
			}
		}
	}
	out.Centers = centers
	return out
}

// assignAll files every point under its nearest centre and returns how
// many changed list. The points are split across the processors: this
// loop is the whole cost of training.
func assignAll(pts, centers [][]float32, assign []int) int {
	const workers = 4
	chunk := (len(pts) + workers - 1) / workers
	moved := make([]int, workers)
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		lo, hi := w*chunk, (w+1)*chunk
		if hi > len(pts) {
			hi = len(pts)
		}
		if lo >= hi {
			break
		}
		wg.Add(1)
		go func(w, lo, hi int) {
			defer wg.Done()
			for i := lo; i < hi; i++ {
				best, bestD := 0, math.Inf(1)
				for c := range centers {
					if d := L2Squared(pts[i], centers[c]); d < bestD {
						best, bestD = c, d
					}
				}
				if assign[i] != best {
					assign[i] = best
					moved[w]++
				}
			}
		}(w, lo, hi)
	}
	wg.Wait()
	total := 0
	for _, m := range moved {
		total += m
	}
	return total
}

func clone(v []float32) []float32 { return append([]float32(nil), v...) }

// The centroids of every index build this process has used, by build
// ID. A build ID is random and names one training run, whose centres
// never change, so an entry is never stale; it is dropped when the
// index is dropped on this node and otherwise stays for the life of
// the process — a few hundred kilobytes per index — because the write
// path (pkg/sql/rowenc) looks centres up without a transaction to
// reload them through.
var (
	registryMu sync.RWMutex
	registry   = map[string]*Centroids{}
)

// Lookup returns the centroids of a build, once Register has seen them.
func Lookup(build string) (*Centroids, bool) {
	registryMu.RLock()
	c, ok := registry[build]
	registryMu.RUnlock()
	return c, ok
}

// Register records the centroids of a build.
func Register(build string, c *Centroids) {
	registryMu.Lock()
	registry[build] = c
	registryMu.Unlock()
}

// Forget drops a build's centroids (its index is gone).
func Forget(build string) {
	registryMu.Lock()
	delete(registry, build)
	registryMu.Unlock()
}
