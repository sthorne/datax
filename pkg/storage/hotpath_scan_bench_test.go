package storage

import (
	"bytes"
	"fmt"
	"math/rand/v2"
	"testing"

	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/util/hlc"
)

// Prototype of the "advance with Next, fall back to Seek" scanner, to size
// the prize from replacing MVCCScan's two-seeks-per-row structure:
//
//	mvcc.go:427  it.SeekGE(EncodeMVCCKey(cur.Next(), {}))  -- advance a row
//	mvcc.go:~487 it.SeekGE(EncodeMVCCKey(cur, ts))         -- inside
//	             mvccScanKey, redundant when the iterator already sits on
//	             the newest version of cur
//
// Both are full LSM seeks. Versions sort newest-first under the same user
// key, so for the common shallow-version row a bounded run of Next() lands
// on the next row far more cheaply. This covers only the plain read path
// (no txn, no uncertainty, no intents) -- enough to measure, and it asserts
// equality with MVCCScan on the same data so the comparison is honest.
//
// maxNextsBeforeSeek bounds the walk so a key with a deep version chain
// (a hot row rewritten thousands of times) still falls back to a seek
// rather than walking every version.
const maxNextsBeforeSeek = 8

func mvccScanNextFirst(r Reader, start, end keys.Key, ts hlc.Timestamp, max int64) (ScanResult, error) {
	lower := EncodeMVCCKey(start, hlc.Timestamp{})
	upper := EncodeMVCCKey(end, hlc.Timestamp{})
	it := r.NewIter(lower, upper)
	defer func() { _ = it.Close() }()

	var res ScanResult
	ok := it.SeekGE(lower)
	for ok {
		userKey, vts, err := DecodeMVCCKey(it.Key())
		if err != nil {
			return ScanResult{}, err
		}
		if vts.IsEmpty() { // intent metadata: out of this prototype's scope
			return ScanResult{}, errPrototypeIntent
		}

		// Walk this key's versions (newest first) to the first at or below
		// ts, then walk off the key -- all with Next, up to the bound.
		var value []byte
		var found bool
		nexts := 0
		for {
			if vts.LessEq(ts) {
				if !found {
					data, tombstone, derr := decodeMVCCValue(it.Value())
					if derr != nil {
						return ScanResult{}, derr
					}
					if !tombstone {
						value = append([]byte(nil), data...)
					}
					found = true
				}
			}
			if !it.Next() {
				ok = false
				break
			}
			nexts++
			nk, nvts, derr := DecodeMVCCKey(it.Key())
			if derr != nil {
				return ScanResult{}, derr
			}
			if !bytes.Equal(nk, userKey) {
				break // landed on the next row: no seek needed at all
			}
			vts = nvts
			if nexts >= maxNextsBeforeSeek {
				// Deep version chain: seek past this key instead of walking.
				ok = it.SeekGE(EncodeMVCCKey(keys.Key(userKey).Next(), hlc.Timestamp{}))
				break
			}
		}

		if found && value != nil {
			res.KVs = append(res.KVs, KeyValue{Key: keys.Key(userKey).Clone(), Value: value})
			if max > 0 && int64(len(res.KVs)) == max {
				res.Resume = keys.Key(userKey).Clone().Next()
				return res, nil
			}
		}
	}
	return res, nil
}

var errPrototypeIntent = errIntentSentinel{}

type errIntentSentinel struct{}

func (errIntentSentinel) Error() string { return "prototype scanner: intent encountered" }

// Equivalence check: the prototype must agree with MVCCScan, or the speed
// comparison is meaningless.
func TestScanNextFirstMatchesMVCCScan(t *testing.T) {
	eng := openTestEngine(t)
	for i := 0; i < 500; i++ {
		k := keys.Key([]byte{byte('a' + i%26), byte('0' + i/26%10), byte(i % 7)})
		for v := 1; v <= 1+i%4; v++ {
			mustPut(t, eng, string(k), ts(int64(100+v), 0), string(k)+"v", nil)
		}
		if i%11 == 0 {
			mustDelete(t, eng, string(k), ts(int64(200), 0), nil)
		}
	}
	for _, readAt := range []hlc.Timestamp{ts(101, 0), ts(103, 0), ts(150, 0), ts(1000, 0)} {
		want, err := MVCCScan(eng, keys.Key("a"), keys.Key("zzz"), readAt, 0, MVCCGetOptions{})
		if err != nil {
			t.Fatal(err)
		}
		got, err := mvccScanNextFirst(eng, keys.Key("a"), keys.Key("zzz"), readAt, 0)
		if err != nil {
			t.Fatal(err)
		}
		if len(got.KVs) != len(want.KVs) {
			t.Fatalf("ts=%s: got %d rows, want %d", readAt, len(got.KVs), len(want.KVs))
		}
		for i := range want.KVs {
			if !bytes.Equal(got.KVs[i].Key, want.KVs[i].Key) || !bytes.Equal(got.KVs[i].Value, want.KVs[i].Value) {
				t.Fatalf("ts=%s row %d: got %q=%q, want %q=%q", readAt, i,
					got.KVs[i].Key, got.KVs[i].Value, want.KVs[i].Key, want.KVs[i].Value)
			}
		}
	}
}

func BenchmarkScanSeekVsNext(b *testing.B) {
	for _, versions := range []int{1, 3, 5} {
		eng := buildEngine(b, versions)
		readTS := ts(1000, 0)
		const rows = 1000
		b.Run(fmt.Sprintf("seek-per-row/versions=%d", versions), func(b *testing.B) {
			rng := rand.New(rand.NewPCG(5, 6))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				start := rng.IntN(benchRows/2-rows) * 2
				res, err := MVCCScan(eng, benchKey(start), benchKey(start+2*rows), readTS, 0, MVCCGetOptions{})
				if err != nil {
					b.Fatal(err)
				}
				if len(res.KVs) != rows {
					b.Fatalf("got %d rows", len(res.KVs))
				}
			}
		})
		b.Run(fmt.Sprintf("next-first/versions=%d", versions), func(b *testing.B) {
			rng := rand.New(rand.NewPCG(5, 6))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				start := rng.IntN(benchRows/2-rows) * 2
				res, err := mvccScanNextFirst(eng, benchKey(start), benchKey(start+2*rows), readTS, 0)
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
