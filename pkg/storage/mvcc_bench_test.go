package storage

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"testing"

	"github.com/cockroachdb/pebble/v2"

	"github.com/sthorne/datax/pkg/keys"
)

// Read-path benchmarks over an on-disk engine whose rows are in sstables
// (an unflushed memtable would make every read a memtable hit). Keys are
// precomputed; only even indices are written, so an absent-key probe
// (odd) interleaves with present keys and cannot be pruned by sstable
// bounds. Shape borrowed from the measurements in issues #160–#163.
const (
	benchRows     = 200000
	benchValueLen = 128
)

var benchKeys = func() []keys.Key {
	ks := make([]keys.Key, benchRows)
	for i := range ks {
		ks[i] = keys.Key(fmt.Sprintf("/Table/1/%09d", i))
	}
	return ks
}()

func benchKey(i int) keys.Key { return benchKeys[i] }

// buildBenchEngine fills an on-disk engine with benchRows/2 rows of
// versionsPerKey versions each and flushes.
func buildBenchEngine(b *testing.B, versionsPerKey int) *Engine {
	b.Helper()
	// DATAX_BENCH_FORMAT=19 builds the store at Pebble's columnar-block
	// format (what cluster version v14 ratchets a store to, issue #166)
	// instead of the base format, for a format A/B on these benchmarks.
	if f, _ := strconv.Atoi(os.Getenv("DATAX_BENCH_FORMAT")); f > 0 {
		testingPebbleOptions = func(o *pebble.Options) { o.FormatMajorVersion = pebble.FormatMajorVersion(f) }
		b.Cleanup(func() { testingPebbleOptions = nil })
	}
	// DATAX_BENCH_PREFIX_BLOOM=1 opens the store in prefix mode (cluster
	// version v15, issue #161) for a filter A/B.
	prefix := os.Getenv("DATAX_BENCH_PREFIX_BLOOM") == "1"
	// DATAX_BENCH_L6_FILTERS=0 leaves L6 tables' filters unconsulted.
	if os.Getenv("DATAX_BENCH_L6_FILTERS") == "0" {
		prefixL6Filters = false
		b.Cleanup(func() { prefixL6Filters = true })
	}
	eng, err := Open(b.TempDir(), Options{PrefixBloom: prefix})
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = eng.Close() })
	val := make([]byte, benchValueLen)
	for i := range val {
		val[i] = byte('a' + i%26)
	}
	batch := eng.NewBatch()
	n := 0
	for i := 0; i < benchRows; i += 2 {
		k := benchKey(i)
		for v := 0; v < versionsPerKey; v++ {
			if err := MVCCPut(batch, k, ts(int64(100+v), 0), val, nil); err != nil {
				b.Fatal(err)
			}
		}
		if n++; n%2000 == 0 {
			if err := batch.Commit(false); err != nil {
				b.Fatal(err)
			}
			_ = batch.Close()
			batch = eng.NewBatch()
		}
	}
	if err := batch.Commit(false); err != nil {
		b.Fatal(err)
	}
	_ = batch.Close()
	if err := eng.Flush(); err != nil {
		b.Fatal(err)
	}
	// DATAX_BENCH_COMPACT=1 moves the rows to L6, where a store's bulk
	// rests (Pebble skips L6 filters unless asked; prefixL6Filters).
	if os.Getenv("DATAX_BENCH_COMPACT") == "1" {
		if err := eng.db.Compact(context.Background(), []byte{0}, []byte{0xff, 0xff}, false); err != nil {
			b.Fatal(err)
		}
	}
	return eng
}

// BenchmarkMVCCScan: 1,000-row scans at random offsets, version chains of
// 1, 3 and 5 (issue #160), forward and reverse.
func BenchmarkMVCCScan(b *testing.B) {
	for _, versions := range []int{1, 3, 5} {
		eng := buildBenchEngine(b, versions)
		readTS := ts(1000, 0)
		const rows = 1000
		for _, reverse := range []bool{false, true} {
			name := fmt.Sprintf("versions=%d", versions)
			if reverse {
				name += "/reverse"
			}
			b.Run(name, func(b *testing.B) {
				rng := rand.New(rand.NewSource(5))
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					start := rng.Intn(benchRows/2-rows) * 2
					var res ScanResult
					var err error
					if reverse {
						res, err = MVCCReverseScan(eng, benchKey(start), benchKey(start+2*rows), readTS, 0, MVCCGetOptions{})
					} else {
						res, err = MVCCScan(eng, benchKey(start), benchKey(start+2*rows), readTS, 0, MVCCGetOptions{})
					}
					if err != nil {
						b.Fatal(err)
					}
					if len(res.KVs) != rows {
						b.Fatalf("got %d rows", len(res.KVs))
					}
				}
			})
		}
	}
}

// BenchmarkMVCCGet: point reads of present and absent keys (issues #161,
// #163).
func BenchmarkMVCCGet(b *testing.B) {
	eng := buildBenchEngine(b, 1)
	readTS := ts(1000, 0)
	b.Run("hit", func(b *testing.B) {
		rng := rand.New(rand.NewSource(1))
		b.ReportAllocs()
		defer reportFilterUse(b, eng)()
		for i := 0; i < b.N; i++ {
			if _, err := MVCCGet(eng, benchKey(rng.Intn(benchRows/2)*2), readTS, MVCCGetOptions{}); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("miss", func(b *testing.B) {
		rng := rand.New(rand.NewSource(3))
		b.ReportAllocs()
		defer reportFilterUse(b, eng)()
		for i := 0; i < b.N; i++ {
			if _, err := MVCCGet(eng, benchKey(rng.Intn(benchRows/2)*2+1), readTS, MVCCGetOptions{}); err != nil {
				b.Fatal(err)
			}
		}
	})
}

// reportFilterUse reports Pebble's bloom filter consultations per
// operation — tables excluded ("filtered/op") and tables the filter
// admitted ("admitted/op") — over the timed section (issue #161).
func reportFilterUse(b *testing.B, eng *Engine) func() {
	h0, m0 := eng.FilterMetrics()
	b.ResetTimer()
	return func() {
		b.StopTimer()
		h1, m1 := eng.FilterMetrics()
		b.ReportMetric(float64(h1-h0)/float64(b.N), "filtered/op")
		b.ReportMetric(float64(m1-m0)/float64(b.N), "admitted/op")
	}
}

// BenchmarkMVCCGetReused: point reads through one Getter — the shape of
// a batch of Gets on the server (issue #163).
func BenchmarkMVCCGetReused(b *testing.B) {
	eng := buildBenchEngine(b, 1)
	readTS := ts(1000, 0)
	for _, tc := range []struct {
		name string
		off  int
	}{{"hit", 0}, {"miss", 1}} {
		b.Run(tc.name, func(b *testing.B) {
			rng := rand.New(rand.NewSource(int64(tc.off)))
			g := NewGetter(eng)
			defer g.Close()
			b.ReportAllocs()
			defer reportFilterUse(b, eng)()
			for i := 0; i < b.N; i++ {
				if _, err := g.Get(benchKey(rng.Intn(benchRows/2)*2+tc.off), readTS, MVCCGetOptions{}); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
