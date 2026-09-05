package storage

import (
	"fmt"
	"math/rand"
	"testing"

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
	eng, err := Open(b.TempDir(), Options{})
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
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if _, err := MVCCGet(eng, benchKey(rng.Intn(benchRows/2)*2), readTS, MVCCGetOptions{}); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("miss", func(b *testing.B) {
		rng := rand.New(rand.NewSource(3))
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if _, err := MVCCGet(eng, benchKey(rng.Intn(benchRows/2)*2+1), readTS, MVCCGetOptions{}); err != nil {
				b.Fatal(err)
			}
		}
	})
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
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := g.Get(benchKey(rng.Intn(benchRows/2)*2+tc.off), readTS, MVCCGetOptions{}); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
