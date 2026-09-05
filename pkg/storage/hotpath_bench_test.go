package storage

import (
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"testing"

	"github.com/google/uuid"
	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/storage/enginepb"
)

// Hot-path benchmarks used to rank storage-layer optimizations. Not part of
// the shipped test set's assertions: they exist to be profiled.

const (
	benchRows     = 200000
	benchValueLen = 128
)

// Keys are precomputed: fmt.Sprintf inside a benchmark loop shows up as
// ~2 % of the profile and hides what the storage layer actually costs.
//
// Only EVEN indices are written. Odd indices are absent but INTERLEAVED with
// present keys, so an absent-key probe cannot be pruned by sstable key
// ranges — it is the case bloom filters exist to serve (uniqueness probes,
// intent lookups on the write path).
var benchKeys = func() []keys.Key {
	ks := make([]keys.Key, benchRows)
	for i := range ks {
		ks[i] = keys.Key(fmt.Sprintf("/Table/1/%09d", i))
	}
	return ks
}()

func benchKey(i int) keys.Key { return benchKeys[i] }

// buildEngine fills an on-disk engine with benchRows single-version rows and
// flushes/compacts so reads actually touch sstables (an unflushed memtable
// makes every read a memtable hit and measures nothing).
func buildEngine(b *testing.B, versionsPerKey int) *Engine {
	b.Helper()
	dir := b.TempDir()
	eng, err := Open(dir, Options{})
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
	for i := 0; i < benchRows; i += 2 { // even indices only; see benchKeys
		k := benchKey(i)
		for v := 0; v < versionsPerKey; v++ {
			if err := MVCCPut(batch, k, ts(int64(100+v), 0), val, nil); err != nil {
				b.Fatal(err)
			}
		}
		n++
		if n%2000 == 0 {
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

// ---------------------------------------------------------------- point reads

func BenchmarkMVCCGet(b *testing.B) {
	for _, versions := range []int{1, 5} {
		eng := buildEngine(b, versions)
		readTS := ts(1000, 0)
		b.Run(fmt.Sprintf("hit/versions=%d", versions), func(b *testing.B) {
			rng := rand.New(rand.NewPCG(1, 2))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				k := benchKey(rng.IntN(benchRows/2) * 2)
				if _, err := MVCCGet(eng, k, readTS, MVCCGetOptions{}); err != nil {
					b.Fatal(err)
				}
			}
		})
		// Absent keys INTERLEAVED with present ones (odd indices): the case
		// the bloom filters in profile.go are meant to serve. A disjoint
		// key prefix would be pruned by sstable bounds and measure nothing.
		b.Run(fmt.Sprintf("miss/versions=%d", versions), func(b *testing.B) {
			rng := rand.New(rand.NewPCG(3, 4))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				k := benchKey(rng.IntN(benchRows/2)*2 + 1)
				if _, err := MVCCGet(eng, k, readTS, MVCCGetOptions{}); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// ---------------------------------------------------------------------- scans

func BenchmarkMVCCScan(b *testing.B) {
	for _, versions := range []int{1, 5} {
		eng := buildEngine(b, versions)
		readTS := ts(1000, 0)
		for _, rows := range []int{100, 1000} {
			b.Run(fmt.Sprintf("rows=%d/versions=%d", rows, versions), func(b *testing.B) {
				rng := rand.New(rand.NewPCG(5, 6))
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					start := rng.IntN(benchRows/2-rows) * 2 // land on a present key
					res, err := MVCCScan(eng, benchKey(start), benchKey(start+2*rows),
						readTS, 0, MVCCGetOptions{})
					if err != nil {
						b.Fatal(err)
					}
					if len(res.KVs) != rows {
						b.Fatalf("got %d rows, want %d", len(res.KVs), rows)
					}
				}
			})
		}
	}
}

// --------------------------------------------------------------- intent write

func benchTxn() *enginepb.TxnMeta {
	return &enginepb.TxnMeta{
		ID:             uuid.New(),
		Key:            []byte("/Table/1/000000001"),
		Epoch:          0,
		WriteTimestamp: ts(500, 1),
		MinTimestamp:   ts(500, 0),
		Priority:       42,
		Sequence:       3,
	}
}

func BenchmarkMVCCPut(b *testing.B) {
	val := make([]byte, benchValueLen)
	txn := benchTxn()

	b.Run("txn-intent", func(b *testing.B) {
		eng, err := Open(b.TempDir(), Options{})
		if err != nil {
			b.Fatal(err)
		}
		defer func() { _ = eng.Close() }()
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			batch := eng.NewBatch()
			if err := MVCCPut(batch, benchKey(i%benchRows), ts(500, 1), val, txn); err != nil {
				b.Fatal(err)
			}
			if err := batch.Commit(false); err != nil {
				b.Fatal(err)
			}
			_ = batch.Close()
		}
	})

	b.Run("nontxn", func(b *testing.B) {
		eng, err := Open(b.TempDir(), Options{})
		if err != nil {
			b.Fatal(err)
		}
		defer func() { _ = eng.Close() }()
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			batch := eng.NewBatch()
			if err := MVCCPut(batch, benchKey(i%benchRows), ts(500, 1), val, nil); err != nil {
				b.Fatal(err)
			}
			if err := batch.Commit(false); err != nil {
				b.Fatal(err)
			}
			_ = batch.Close()
		}
	})
}

// --------------------------------------------------- intent metadata encoding

func BenchmarkIntentMetaCodec(b *testing.B) {
	meta := enginepb.MVCCMetadata{Txn: *benchTxn(), Timestamp: ts(500, 1)}
	encoded := encodeMeta(meta)
	b.Logf("JSON-encoded intent metadata: %d bytes", len(encoded))

	b.Run("encode-json", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_ = encodeMeta(meta)
		}
	})
	b.Run("decode-json", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			if _, err := decodeMeta(encoded); err != nil {
				b.Fatal(err)
			}
		}
	})
	// Reference point: a hand-rolled fixed-layout binary encoding of the
	// same fields, to size the prize from dropping encoding/json.
	b.Run("encode-binary", func(b *testing.B) {
		b.ReportAllocs()
		buf := make([]byte, 0, 128)
		for i := 0; i < b.N; i++ {
			buf = appendMetaBinary(buf[:0], meta)
		}
		_ = buf
	})
	b.Run("decode-binary", func(b *testing.B) {
		bin := appendMetaBinary(nil, meta)
		b.Logf("binary-encoded intent metadata: %d bytes", len(bin))
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			if _, err := decodeMetaBinary(bin); err != nil {
				b.Fatal(err)
			}
		}
	})
}

// Sanity: the JSON round-trip we are comparing against is real.
func TestIntentMetaCodecRoundTrip(t *testing.T) {
	meta := enginepb.MVCCMetadata{Txn: *benchTxn(), Timestamp: ts(500, 1)}
	var got enginepb.MVCCMetadata
	if err := json.Unmarshal(encodeMeta(meta), &got); err != nil {
		t.Fatal(err)
	}
	if got.Txn.ID != meta.Txn.ID || got.Timestamp != meta.Timestamp {
		t.Fatalf("round trip mismatch: %+v", got)
	}
	bin, err := decodeMetaBinary(appendMetaBinary(nil, meta))
	if err != nil {
		t.Fatal(err)
	}
	if bin.Txn.ID != meta.Txn.ID || bin.Timestamp != meta.Timestamp ||
		bin.Txn.Sequence != meta.Txn.Sequence || bin.Txn.Priority != meta.Txn.Priority {
		t.Fatalf("binary round trip mismatch: %+v", bin)
	}
}

func BenchmarkMVCCKeyEncode(b *testing.B) {
	k := benchKey(12345)
	at := ts(500, 1)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = EncodeMVCCKey(k, at)
	}
}

// In-situ cost of the JSON intent metadata: a read that lands on an intent
// (the contended shape the `bank` workload drives) must decodeMeta, and a
// transaction rewriting its own key must decodeMeta + encodeMeta.
func BenchmarkIntentPathInSitu(b *testing.B) {
	val := make([]byte, benchValueLen)
	txn := benchTxn()

	setup := func(b *testing.B) *Engine {
		eng, err := Open(b.TempDir(), Options{})
		if err != nil {
			b.Fatal(err)
		}
		b.Cleanup(func() { _ = eng.Close() })
		batch := eng.NewBatch()
		for i := 0; i < 1000; i++ {
			if err := MVCCPut(batch, benchKey(i), ts(500, 1), val, txn); err != nil {
				b.Fatal(err)
			}
		}
		if err := batch.Commit(false); err != nil {
			b.Fatal(err)
		}
		_ = batch.Close()
		return eng
	}

	// Read-your-writes: the transaction reads a key it holds an intent on.
	b.Run("get-own-intent", func(b *testing.B) {
		eng := setup(b)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if _, err := MVCCGet(eng, benchKey(i%1000), ts(500, 1),
				MVCCGetOptions{Txn: txn}); err != nil {
				b.Fatal(err)
			}
		}
	})

	// Same-epoch intent rewrite: a later statement overwrites a key the
	// transaction already wrote (decodeMeta + encodeMeta + history append).
	b.Run("rewrite-own-intent", func(b *testing.B) {
		eng := setup(b)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			batch := eng.NewBatch()
			if err := MVCCPut(batch, benchKey(i%1000), ts(500, 1), val, txn); err != nil {
				b.Fatal(err)
			}
			if err := batch.Commit(false); err != nil {
				b.Fatal(err)
			}
			_ = batch.Close()
		}
	})
}

// Intent history growth: mvccWrite appends the SUPERSEDED provisional value
// to meta.History on every same-epoch rewrite of a key (mvcc.go ~:264),
// unconditionally. So a transaction that writes one key K times stores K
// copies of the value inside a single JSON blob that is re-encoded on each
// write -- O(K^2) bytes. This measures the per-write cost as K grows.
func BenchmarkIntentRewriteDepth(b *testing.B) {
	val := make([]byte, benchValueLen)
	for _, depth := range []int{1, 4, 16, 64} {
		b.Run("rewrites="+fmt.Sprint(depth), func(b *testing.B) {
			eng, err := Open(b.TempDir(), Options{})
			if err != nil {
				b.Fatal(err)
			}
			defer func() { _ = eng.Close() }()
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				txn := benchTxn()
				k := benchKey(i % benchRows)
				for j := 0; j < depth; j++ {
					batch := eng.NewBatch()
					if err := MVCCPut(batch, k, ts(500, 1), val, txn); err != nil {
						b.Fatal(err)
					}
					if err := batch.Commit(false); err != nil {
						b.Fatal(err)
					}
					_ = batch.Close()
				}
			}
			b.ReportMetric(float64(depth), "writes/op")
		})
	}
}

// metaSizeAtDepth reports the encoded intent-metadata size after K rewrites,
// making the growth visible in bytes rather than nanoseconds.
func TestIntentMetaGrowth(t *testing.T) {
	eng := openTestEngine(t)
	txn := benchTxn()
	val := make([]byte, benchValueLen)
	k := keys.Key("/Table/1/row")
	metaKey, _ := mvccKeyBounds(k)
	for j := 1; j <= 64; j++ {
		b := eng.NewBatch()
		if err := MVCCPut(b, k, ts(500, 1), val, txn); err != nil {
			t.Fatal(err)
		}
		if err := b.Commit(false); err != nil {
			t.Fatal(err)
		}
		_ = b.Close()
		if j == 1 || j == 4 || j == 16 || j == 64 {
			raw, err := eng.Get(metaKey)
			if err != nil {
				t.Fatal(err)
			}
			m, err := decodeMeta(raw)
			if err != nil {
				t.Fatal(err)
			}
			t.Logf("after %2d writes to one key: intent metadata = %6d bytes, history = %d entries",
				j, len(raw), len(m.History))
		}
	}
}
