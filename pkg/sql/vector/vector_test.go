package vector

import (
	"fmt"
	"math"
	"math/rand"
	"strings"
	"testing"
)

func near(a, b float64) bool { return math.Abs(a-b) <= 1e-9*(1+math.Abs(b)) }

// TestTextRoundTrip: the text form parses to the elements written and
// renders back to the shortest text that reads as the same float32s.
func TestTextRoundTrip(t *testing.T) {
	for _, tc := range []struct{ in, out string }{
		{"[1,2,3]", "[1,2,3]"},
		{" [ 1 , 2.5,\t-3e2 ] ", "[1,2.5,-300]"},
		{"[0.1]", "[0.1]"},
		{"[-0.000001,1e10]", "[-1e-06,1e+10]"},
		{"[3.4028235e38]", "[3.4028235e+38]"},
		{"[1.17549435e-38]", "[1.1754944e-38]"},
		{"[16777217]", "[1.6777216e+07]"}, // not representable: rounds as float4 does
	} {
		v, err := Parse(tc.in)
		if err != nil {
			t.Fatalf("Parse(%q): %v", tc.in, err)
		}
		if got := Format(v); got != tc.out {
			t.Errorf("Format(Parse(%q)) = %q, want %q", tc.in, got, tc.out)
		}
		again, err := Parse(Format(v))
		if err != nil || Compare(again, v) != 0 {
			t.Errorf("%q does not survive a second round trip: %v %v", tc.in, again, err)
		}
	}
	r := rand.New(rand.NewSource(1))
	for i := 0; i < 200; i++ {
		v := make([]float32, 1+r.Intn(40))
		for j := range v {
			v[j] = float32(r.NormFloat64() * math.Pow(10, float64(r.Intn(12)-6)))
		}
		back, err := Parse(Format(v))
		if err != nil || Compare(back, v) != 0 {
			t.Fatalf("random vector %v: text round trip gave %v (%v)", v, back, err)
		}
		if got := Decode(string(Encode(v))); Compare(got, v) != 0 {
			t.Fatalf("random vector %v: stored round trip gave %v", v, got)
		}
		stored, err := DecodeBinary(EncodeBinary(string(Encode(v))))
		if err != nil || stored != string(Encode(v)) {
			t.Fatalf("random vector %v: binary round trip (%v)", v, err)
		}
	}
}

// TestRefusals: what is not a vector is refused, by what is wrong.
func TestRefusals(t *testing.T) {
	big := "[" + strings.Repeat("1,", MaxDims) + "1]"
	for _, tc := range []struct{ in, want string }{
		{"", "invalid input syntax"},
		{"1,2,3", "invalid input syntax"},
		{"[1,2,3", "invalid input syntax"},
		{"[]", "at least 1 dimension"},
		{"[ ]", "at least 1 dimension"},
		{"[1,,2]", "invalid input syntax"},
		{"[1,2,]", "invalid input syntax"},
		{"[1,a]", "invalid input syntax"},
		{"[1 2]", "invalid input syntax"},
		{"[NaN]", "NaN not allowed"},
		{"[1,Infinity]", "infinite value not allowed"},
		{"[-inf]", "infinite value not allowed"},
		{"[1e39]", "out of range"},
		{big, "more than 16000 dimensions"},
	} {
		if _, err := Parse(tc.in); err == nil || !strings.Contains(err.Error(), tc.want) {
			shown := tc.in
			if len(shown) > 40 {
				shown = shown[:40] + "..."
			}
			t.Errorf("Parse(%q) = %v, want an error containing %q", shown, err, tc.want)
		}
	}
	if _, err := Parse("[" + strings.Repeat("1,", MaxDims-1) + "1]"); err != nil {
		t.Errorf("a vector of exactly %d dimensions is refused: %v", MaxDims, err)
	}
	// The binary form is held to the same rules.
	nan := EncodeBinary(string(Encode([]float32{1, float32(math.NaN())})))
	if _, err := DecodeBinary(nan); err == nil || !strings.Contains(err.Error(), "NaN") {
		t.Errorf("binary NaN: %v", err)
	}
	inf := EncodeBinary(string(Encode([]float32{float32(math.Inf(1))})))
	if _, err := DecodeBinary(inf); err == nil || !strings.Contains(err.Error(), "infinite") {
		t.Errorf("binary infinity: %v", err)
	}
	for name, raw := range map[string][]byte{
		"short header":      {0, 1},
		"dimension too big": {0, 2, 0, 0, 0, 0, 0, 0},
		"unused set":        {0, 1, 0, 1, 0, 0, 0, 0},
		"no dimensions":     {0, 0, 0, 0},
	} {
		if _, err := DecodeBinary(raw); err == nil {
			t.Errorf("binary %s: accepted", name)
		}
	}
}

// TestBinaryLayout: pgvector's wire form, byte for byte.
func TestBinaryLayout(t *testing.T) {
	got := EncodeBinary(string(Encode([]float32{1, -2.5})))
	want := []byte{0, 2, 0, 0, 0x3f, 0x80, 0, 0, 0xc0, 0x20, 0, 0}
	if string(got) != string(want) {
		t.Fatalf("binary [1,-2.5] = % x, want % x", got, want)
	}
}

// TestDistances: each operator against values worked by hand.
func TestDistances(t *testing.T) {
	a, b := []float32{1, 2, 3}, []float32{4, 6, 3}
	// (3,4,0): length 5.
	if got := L2.Distance(a, b); got != 5 {
		t.Errorf("[1,2,3] <-> [4,6,3] = %v, want 5", got)
	}
	// 4 + 12 + 9 = 25, negated.
	if got := IP.Distance(a, b); got != -25 {
		t.Errorf("[1,2,3] <#> [4,6,3] = %v, want -25", got)
	}
	// 1 − 25 / (√14 · √61).
	if got, want := Cosine.Distance(a, b), 1-25/(math.Sqrt(14)*math.Sqrt(61)); !near(got, want) {
		t.Errorf("[1,2,3] <=> [4,6,3] = %v, want %v", got, want)
	}
	if got := L1(a, b); got != 7 {
		t.Errorf("l1 = %v, want 7", got)
	}
	if got := Norm([]float32{3, 4}); got != 5 {
		t.Errorf("norm [3,4] = %v, want 5", got)
	}
	for _, tc := range []struct {
		a, b []float32
		want float64
	}{
		{[]float32{1, 0}, []float32{2, 0}, 0},  // same direction
		{[]float32{1, 0}, []float32{0, 7}, 1},  // orthogonal
		{[]float32{1, 0}, []float32{-3, 0}, 2}, // opposite
		{[]float32{1, 1}, []float32{1, 0}, 1 - 1/math.Sqrt2},
	} {
		if got := CosineDistance(tc.a, tc.b); !near(got, tc.want) {
			t.Errorf("%v <=> %v = %v, want %v", tc.a, tc.b, got, tc.want)
		}
	}
	if got := CosineDistance([]float32{0, 0}, []float32{1, 2}); !math.IsNaN(got) {
		t.Errorf("cosine distance to the zero vector = %v, want NaN", got)
	}
	if err := Dims([]float32{1}, []float32{1, 2}); err == nil || err.Error() != "different vector dimensions 1 and 2" {
		t.Errorf("Dims: %v", err)
	}
	if got := Normalize([]float32{3, 4}); got[0] != 0.6 || got[1] != 0.8 {
		t.Errorf("normalize [3,4] = %v", got)
	}
	if c := Compare([]float32{1, 2}, []float32{1, 3}); c != -1 {
		t.Errorf("compare by element: %d", c)
	}
	if c := Compare([]float32{1, 2}, []float32{1, 2, 0}); c != -1 {
		t.Errorf("a prefix sorts first: %d", c)
	}
}

// TestStoredDistance: the distance over stored forms is the distance
// over the elements, bit for bit.
func TestStoredDistance(t *testing.T) {
	r := rand.New(rand.NewSource(2))
	for i := 0; i < 200; i++ {
		a, b := make([]float32, 1+i%40), make([]float32, 1+i%40)
		for j := range a {
			a[j], b[j] = float32(r.NormFloat64()), float32(r.NormFloat64())
		}
		if i%50 == 0 {
			a = make([]float32, len(a)) // the zero vector
		}
		if i%7 == 3 {
			copy(b, a) // a tie with itself: distance zero, or -|a|² under <#>
		}
		same := func(x, y float64) bool {
			return math.Float64bits(x) == math.Float64bits(y) || (math.IsNaN(x) && math.IsNaN(y))
		}
		for _, m := range []Metric{L2, Cosine, IP} {
			want := m.Distance(a, b)
			if got := m.StoredDistance(string(Encode(a)), string(Encode(b))); !same(got, want) {
				t.Fatalf("%s over %v and %v: stored %v, decoded %v", m, a, b, got, want)
			}
			if got := m.StoredDistanceBytes(Encode(a), string(Encode(b))); !same(got, want) {
				t.Fatalf("%s over %v and %v: stored bytes %v, decoded %v", m, a, b, got, want)
			}
			// The operands the other way round: the three distances are
			// symmetric to the bit, which lets a caller put the column on
			// either side.
			if got := m.StoredDistanceBytes(Encode(b), string(Encode(a))); !same(got, want) {
				t.Fatalf("%s over %v and %v swapped: %v, want %v", m, a, b, got, want)
			}
		}
	}
}

// BenchmarkStoredDistance is the per-row arithmetic of the exact k-NN
// scan and of ivfflat candidate ranking: one distance between a stored
// row vector (bytes as read from the store) and the stored query, at
// 64 and 1536 dimensions. It must not allocate.
//
//	go test ./pkg/sql/vector -run - -bench StoredDistance -benchmem
func BenchmarkStoredDistance(b *testing.B) {
	for _, dims := range []int{64, 1536} {
		r := rand.New(rand.NewSource(1))
		x, y := make([]float32, dims), make([]float32, dims)
		for j := range x {
			x[j], y[j] = float32(r.NormFloat64()), float32(r.NormFloat64())
		}
		row, q := Encode(x), string(Encode(y))
		for _, m := range []Metric{L2, Cosine, IP} {
			b.Run(fmt.Sprintf("%s/%d", m, dims), func(b *testing.B) {
				b.ReportAllocs()
				var sink float64
				for i := 0; i < b.N; i++ {
					sink += m.StoredDistanceBytes(row, q)
				}
				if math.IsInf(sink, 0) {
					b.Fatal(sink)
				}
			})
		}
	}
}

func TestMetricNames(t *testing.T) {
	for _, m := range []Metric{L2, Cosine, IP} {
		if got, ok := MetricForOperator(m.Operator()); !ok || got != m {
			t.Errorf("%s: operator %s maps to %q", m, m.Operator(), got)
		}
		if got, ok := MetricForOpClass(m.OpClass()); !ok || got != m {
			t.Errorf("%s: operator class %s maps to %q", m, m.OpClass(), got)
		}
	}
	if _, ok := MetricForOpClass("vector_l1_ops"); ok {
		t.Error("vector_l1_ops is not an indexable metric")
	}
}

// clustered returns n points around k well-separated centres.
func clustered(r *rand.Rand, n, k, dims int) (pts, centres [][]float32) {
	centres = make([][]float32, k)
	for i := range centres {
		centres[i] = make([]float32, dims)
		for j := range centres[i] {
			centres[i][j] = float32(r.NormFloat64() * 10)
		}
	}
	pts = make([][]float32, n)
	for i := range pts {
		c := centres[i%k]
		pts[i] = make([]float32, dims)
		for j := range pts[i] {
			pts[i][j] = c[j] + float32(r.NormFloat64())
		}
	}
	return pts, centres
}

// TestTrain: the centres found are the clusters the data has, the
// result is a function of the sample and the seed, and the degenerate
// samples train to something an index can use.
func TestTrain(t *testing.T) {
	r := rand.New(rand.NewSource(7))
	pts, truth := clustered(r, 2000, 8, 16)
	c := Train(pts, 16, 8, L2, 1, nil)
	if len(c.Centers) != 8 {
		t.Fatalf("trained %d centres, want 8", len(c.Centers))
	}
	// Every true centre has a trained one close by (the noise has
	// standard deviation 1 per element; the centres are ~56 apart).
	for i, tr := range truth {
		best := math.Inf(1)
		for _, got := range c.Centers {
			best = math.Min(best, math.Sqrt(L2Squared(tr, got)))
		}
		if best > 1.5 {
			t.Errorf("true centre %d is %.2f from the nearest trained one", i, best)
		}
	}
	// Points of one cluster are filed under one list.
	lists := map[int]int{}
	for i := 0; i < len(pts); i += 8 {
		lists[c.Assign(pts[i])]++
	}
	if len(lists) != 1 {
		t.Errorf("the points of one cluster were filed under %d lists", len(lists))
	}
	again := Train(pts, 16, 8, L2, 1, nil)
	for i := range c.Centers {
		if Compare(c.Centers[i], again.Centers[i]) != 0 {
			t.Fatalf("training is not deterministic: centre %d differs", i)
		}
	}

	if got := Train(nil, 4, 100, L2, 1, nil); len(got.Centers) != 1 || len(got.Centers[0]) != 4 {
		t.Errorf("an empty sample trains to %d centres", len(got.Centers))
	}
	if got := Train(pts[:5], 16, 100, L2, 1, nil); len(got.Centers) != 5 {
		t.Errorf("5 points and 100 lists train to %d centres, want 5", len(got.Centers))
	}
	same := [][]float32{{1, 1}, {1, 1}, {1, 1}}
	if got := Train(same, 2, 3, L2, 1, nil); len(got.Centers) != 1 {
		t.Errorf("three identical points train to %d centres, want 1", len(got.Centers))
	}
	// Cosine and inner product cluster by direction: unit centres, and
	// a vector and its multiple land in the same list.
	for _, m := range []Metric{Cosine, IP} {
		sc := Train(pts, 16, 8, m, 1, nil)
		for i, ctr := range sc.Centers {
			if n := Norm(ctr); math.Abs(n-1) > 1e-5 {
				t.Errorf("%s centre %d has norm %v", m, i, n)
			}
		}
		scaled := make([]float32, 16)
		for j := range scaled {
			scaled[j] = pts[3][j] * 40
		}
		if sc.Assign(pts[3]) != sc.Assign(scaled) {
			t.Errorf("%s: a vector and its multiple are filed apart", m)
		}
	}
	if got := Train([][]float32{{0, 0}, {0, 0}}, 2, 4, Cosine, 1, nil); len(got.Centers) != 1 {
		t.Errorf("only zero vectors, cosine: %d centres", len(got.Centers))
	}
}

// TestProbe: the lists come nearest first, the first is the one the
// vector itself would be filed under, and asking for more than exist
// gives them all.
func TestProbe(t *testing.T) {
	r := rand.New(rand.NewSource(3))
	pts, _ := clustered(r, 1000, 10, 8)
	for _, m := range []Metric{L2, Cosine, IP} {
		c := Train(pts, 8, 10, m, 1, nil)
		for i := 0; i < 50; i++ {
			q := pts[r.Intn(len(pts))]
			got := c.Probe(q, 4)
			if len(got) != 4 || got[0] != c.Assign(q) {
				t.Fatalf("%s: Probe = %v, Assign = %d", m, got, c.Assign(q))
			}
			for j := 1; j < len(got); j++ {
				if c.score(got[j-1], q) < c.score(got[j], q) {
					t.Fatalf("%s: probe order %v is not nearest first", m, got)
				}
			}
			seen := map[int]bool{}
			for _, l := range c.Probe(q, 99) {
				seen[l] = true
			}
			if len(seen) != len(c.Centers) {
				t.Fatalf("%s: probing every list returned %d of %d", m, len(seen), len(c.Centers))
			}
		}
	}
}

func TestRegistry(t *testing.T) {
	c := &Centroids{Metric: L2, Centers: [][]float32{{0}}}
	if _, ok := Lookup("test-build"); ok {
		t.Fatal("unregistered build found")
	}
	Register("test-build", c)
	if got, ok := Lookup("test-build"); !ok || got != c {
		t.Fatal("registered build not found")
	}
	Forget("test-build")
	if _, ok := Lookup("test-build"); ok {
		t.Fatal("forgotten build found")
	}
}
