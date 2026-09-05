package storage

import (
	"bytes"
	"math/rand/v2"
	"testing"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/bloom"
	"github.com/cockroachdb/pebble/vfs"

	"github.com/sthorne/datax/pkg/util/hlc"
)

// A/B for the prefix-bloom proposal, on datax's exact MVCC key layout.
//
//	A "seekge-default"  today: DefaultComparer (Split = whole key), reads go
//	                    through NewIter + SeekGE, which consults NO bloom
//	                    filter, so the filters configured in profile.go are
//	                    dead weight for every version read.
//	B "seekprefix-split" proposed: a Comparer whose Split strips the 12-byte
//	                    version suffix, reads go through SeekPrefixGE, which
//	                    does consult the filter.
//
// Both DBs get identical data, identical options, and identical bloom
// filters. The only variables are the comparer and the seek call.

// splitMVCCKey returns the length of the user-key prefix of an encoded MVCC
// key: everything through the 0x00 0x01 terminator that EncodeBytes appends
// (0x00 in user data is escaped to 0x00 0xff, so the first 0x00 0x01 pair is
// unambiguously the terminator). O(len(key)) — see splitMVCCKeyFixed for the
// O(1) variant a trailing suffix-length byte would allow.
func splitMVCCKey(k []byte) int {
	for i := 0; i+1 < len(k); i++ {
		if k[i] == 0x00 && k[i+1] == 0x01 {
			return i + 2
		}
	}
	return len(k)
}

func prefixComparer() *pebble.Comparer {
	c := *pebble.DefaultComparer
	c.Split = splitMVCCKey
	// Conservative: never shorten a separator/successor, so the shortening
	// cannot cross a prefix boundary. Production would use CockroachDB-style
	// prefix-aware versions; this only costs a little index-block size and
	// keeps the measurement honest.
	c.Separator = func(dst, a, b []byte) []byte { return append(dst, a...) }
	c.Successor = func(dst, a []byte) []byte { return append(dst, a...) }
	c.Name = "datax.MVCCKeyComparer"
	return &c
}

func buildPebble(b *testing.B, cmp *pebble.Comparer, versionsPerKey int) *pebble.DB {
	b.Helper()
	opts := &pebble.Options{FS: vfs.Default, Comparer: cmp}
	opts.Levels = make([]pebble.LevelOptions, 7)
	for i := range opts.Levels {
		opts.Levels[i].FilterPolicy = bloom.FilterPolicy(BloomBitsPerKey)
		opts.Levels[i].FilterType = pebble.TableFilter
	}
	opts.FormatMajorVersion = pebble.FormatNewest
	opts.Cache = pebble.NewCache(64 << 20)
	defer opts.Cache.Unref()

	db, err := pebble.Open(b.TempDir(), opts)
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = db.Close() })

	val := make([]byte, benchValueLen)
	batch := db.NewBatch()
	for i := 0; i < benchRows; i += 2 { // even indices only; odd ones are the misses
		for v := 0; v < versionsPerKey; v++ {
			ek := EncodeMVCCKey(benchKey(i), hlc.Timestamp{WallTime: int64(100 + v)})
			if err := batch.Set(ek, encodeMVCCValue(val, false), nil); err != nil {
				b.Fatal(err)
			}
		}
		if batch.Len() > 4<<20 {
			if err := batch.Commit(pebble.NoSync); err != nil {
				b.Fatal(err)
			}
			_ = batch.Close()
			batch = db.NewBatch()
		}
	}
	if err := batch.Commit(pebble.NoSync); err != nil {
		b.Fatal(err)
	}
	_ = batch.Close()
	if err := db.Flush(); err != nil {
		b.Fatal(err)
	}
	return db
}

// filterDelta reports bloom-filter hits/misses accumulated across a sub-
// benchmark, which is how we prove the filter was actually consulted.
func filterStats(db *pebble.DB) (hits, misses int64) {
	m := db.Metrics()
	return m.Filter.Hits, m.Filter.Misses
}

func BenchmarkPrefixBloom(b *testing.B) {
	const versions = 5
	readTS := hlc.Timestamp{WallTime: 1000}

	// A: today's shape — default comparer, plain SeekGE over a bounded iter.
	b.Run("seekge-default", func(b *testing.B) {
		db := buildPebble(b, pebble.DefaultComparer, versions)
		for _, mode := range []string{"hit", "miss"} {
			b.Run(mode, func(b *testing.B) {
				rng := rand.New(rand.NewPCG(7, 8))
				h0, m0 := filterStats(db)
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					idx := rng.IntN(benchRows/2) * 2
					if mode == "miss" {
						idx++
					}
					k := benchKey(idx)
					lower := EncodeMVCCKey(k, hlc.Timestamp{})
					upper := EncodeMVCCKey(k.Next(), hlc.Timestamp{})
					it, err := db.NewIter(&pebble.IterOptions{LowerBound: lower, UpperBound: upper})
					if err != nil {
						b.Fatal(err)
					}
					if it.SeekGE(EncodeMVCCKey(k, readTS)) {
						_ = it.Value()
					}
					if err := it.Close(); err != nil {
						b.Fatal(err)
					}
				}
				b.StopTimer()
				h1, m1 := filterStats(db)
				b.ReportMetric(float64(h1-h0)/float64(b.N), "filterhit/op")
				b.ReportMetric(float64(m1-m0)/float64(b.N), "filtermiss/op")
			})
		}
	})

	// B: proposed shape — Split comparer, SeekPrefixGE.
	b.Run("seekprefix-split", func(b *testing.B) {
		db := buildPebble(b, prefixComparer(), versions)
		for _, mode := range []string{"hit", "miss"} {
			b.Run(mode, func(b *testing.B) {
				rng := rand.New(rand.NewPCG(7, 8))
				h0, m0 := filterStats(db)
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					idx := rng.IntN(benchRows/2) * 2
					if mode == "miss" {
						idx++
					}
					k := benchKey(idx)
					lower := EncodeMVCCKey(k, hlc.Timestamp{})
					upper := EncodeMVCCKey(k.Next(), hlc.Timestamp{})
					it, err := db.NewIter(&pebble.IterOptions{LowerBound: lower, UpperBound: upper})
					if err != nil {
						b.Fatal(err)
					}
					if it.SeekPrefixGE(EncodeMVCCKey(k, readTS)) {
						_ = it.Value()
					}
					if err := it.Close(); err != nil {
						b.Fatal(err)
					}
				}
				b.StopTimer()
				h1, m1 := filterStats(db)
				b.ReportMetric(float64(h1-h0)/float64(b.N), "filterhit/op")
				b.ReportMetric(float64(m1-m0)/float64(b.N), "filtermiss/op")
			})
		}
	})

	// C: proposed shape PLUS iterator reuse, to separate the two effects.
	// One long-lived iterator, re-bounded per lookup with SetOptions.
	b.Run("seekprefix-split-reuse", func(b *testing.B) {
		db := buildPebble(b, prefixComparer(), versions)
		for _, mode := range []string{"hit", "miss"} {
			b.Run(mode, func(b *testing.B) {
				rng := rand.New(rand.NewPCG(7, 8))
				it, err := db.NewIter(nil)
				if err != nil {
					b.Fatal(err)
				}
				defer func() { _ = it.Close() }()
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					idx := rng.IntN(benchRows/2) * 2
					if mode == "miss" {
						idx++
					}
					k := benchKey(idx)
					lower := EncodeMVCCKey(k, hlc.Timestamp{})
					upper := EncodeMVCCKey(k.Next(), hlc.Timestamp{})
					it.SetOptions(&pebble.IterOptions{LowerBound: lower, UpperBound: upper})
					if it.SeekPrefixGE(EncodeMVCCKey(k, readTS)) {
						_ = it.Value()
					}
				}
			})
		}
	})
}

// Guard: the Split function must agree with the encoder for keys containing
// the bytes it escapes, or the comparer silently corrupts the LSM.
func TestSplitMVCCKey(t *testing.T) {
	for _, k := range []string{"", "a", "/Table/1/000000001", "a\x00b", "\x00\x00", "\x01\x00\x01", "\xff\x00"} {
		user := []byte(k)
		metaKey := EncodeMVCCKey(user, hlc.Timestamp{})
		verKey := EncodeMVCCKey(user, hlc.Timestamp{WallTime: 100, Logical: 3})
		if got := splitMVCCKey(metaKey); got != len(metaKey) {
			t.Errorf("split(meta %q) = %d, want %d", k, got, len(metaKey))
		}
		if got := splitMVCCKey(verKey); got != len(verKey)-versionSuffixLen {
			t.Errorf("split(version %q) = %d, want %d", k, got, len(verKey)-versionSuffixLen)
		}
		if !bytes.Equal(verKey[:splitMVCCKey(verKey)], metaKey) {
			t.Errorf("split(version %q) prefix != meta key", k)
		}
	}
}
