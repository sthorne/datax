package storage

import (
	"context"
	"fmt"
	"testing"

	"github.com/cockroachdb/pebble/v2"

	"github.com/sthorne/datax/pkg/keys"
)

// TestPrefixBloomReadsLegacyTables (issue #161): a store written without
// prefix mode — row-block tables at format 16 and columnar tables at 19,
// each with whole-key filters under the old policy name — reopens in
// prefix mode and reads the same rows, intents and scans; its tables
// carry no consultable filter until FilterRewritePass rewrites them,
// after which every live table carries a prefix filter and absent-key
// reads are answered by the filters.
func TestPrefixBloomReadsLegacyTables(t *testing.T) {
	dir := t.TempDir()
	keyOf := func(i int) keys.Key { return append(keys.TablePrefix.Clone(), fmt.Sprintf("/%05d", i)...) }
	const rows = 600
	// L0 thresholds out of reach: the flushes below must stay as written
	// (Pebble would otherwise merge them into one table before the
	// reopen); FilterRewritePass compacts manually.
	testingPebbleOptions = func(o *pebble.Options) {
		o.L0CompactionThreshold, o.L0CompactionFileThreshold, o.L0StopWritesThreshold = 1000, 1000, 2000
	}
	t.Cleanup(func() { testingPebbleOptions = nil })
	eng, err := Open(dir, Options{})
	if err != nil {
		t.Fatal(err)
	}
	// Three tables: the first two at format 16 (row blocks), the third
	// at 19 (columnar), all with whole-key filters.
	write := func(from, to int) {
		b := eng.NewBatch()
		for i := from; i < to; i += 2 {
			for v := 1; v <= 2; v++ {
				if err := MVCCPut(b, keyOf(i), ts(int64(v), 0), []byte(fmt.Sprintf("%d@%d", i, v)), nil); err != nil {
					t.Fatal(err)
				}
			}
		}
		if err := b.Commit(true); err != nil {
			t.Fatal(err)
		}
		_ = b.Close()
		if err := eng.Flush(); err != nil {
			t.Fatal(err)
		}
	}
	write(0, rows/3)
	write(rows/3, 2*rows/3)
	if err := eng.RatchetFormat(FormatColumnarBlocks); err != nil {
		t.Fatal(err)
	}
	write(2*rows/3, rows)
	// An intent left open across the reopen.
	txn := newTxn(ts(10, 0))
	b := eng.NewBatch()
	if err := MVCCPut(b, keyOf(4), ts(10, 0), []byte("prov"), txn); err != nil {
		t.Fatal(err)
	}
	if err := b.Commit(true); err != nil {
		t.Fatal(err)
	}
	_ = b.Close()
	if err := eng.Flush(); err != nil {
		t.Fatal(err)
	}
	if err := eng.Close(); err != nil {
		t.Fatal(err)
	}

	eng, err = Open(dir, Options{PrefixBloom: true})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = eng.Close() }()
	check := func(stage string) {
		t.Helper()
		for i := 0; i < rows; i++ {
			v, err := MVCCGet(eng, keyOf(i), ts(5, 0), MVCCGetOptions{Inconsistent: true})
			if i%2 == 1 {
				if err != nil || v != nil {
					t.Fatalf("%s: %s: %q, %v; want absent", stage, keyOf(i), v, err)
				}
				continue
			}
			if want := fmt.Sprintf("%d@2", i); err != nil || string(v) != want {
				t.Fatalf("%s: %s: %q, %v; want %s", stage, keyOf(i), v, err, want)
			}
			if v, err := MVCCGet(eng, keyOf(i), ts(1, 0), MVCCGetOptions{Inconsistent: true}); err != nil || string(v) != fmt.Sprintf("%d@1", i) {
				t.Fatalf("%s: %s at 1: %q, %v", stage, keyOf(i), v, err)
			}
		}
		if _, err := MVCCGet(eng, keyOf(4), ts(20, 0), MVCCGetOptions{}); err == nil {
			t.Fatalf("%s: the intent on %s did not conflict", stage, keyOf(4))
		}
		if v, err := MVCCGet(eng, keyOf(4), ts(20, 0), MVCCGetOptions{Txn: txn}); err != nil || string(v) != "prov" {
			t.Fatalf("%s: own intent: %q, %v", stage, v, err)
		}
		res, err := MVCCScan(eng, keyOf(0), keyOf(rows), ts(5, 0), 0, MVCCGetOptions{Inconsistent: true})
		if err != nil || len(res.KVs) != rows/2 {
			t.Fatalf("%s: scan: %d rows, %v; want %d", stage, len(res.KVs), err, rows/2)
		}
		for j, kv := range res.KVs {
			if want := keyOf(2 * j); !kv.Key.Equal(want) {
				t.Fatalf("%s: scan row %d is %s, want %s", stage, j, kv.Key, want)
			}
		}
	}
	check("legacy tables under prefix mode")

	// The legacy filters are not consulted (no prefix filter to ask).
	h0, m0 := eng.FilterMetrics()
	for i := 1; i < rows; i += 2 {
		if v, err := MVCCGet(eng, keyOf(i), ts(5, 0), MVCCGetOptions{}); err != nil || v != nil {
			t.Fatal(err)
		}
	}
	h1, m1 := eng.FilterMetrics()
	if h1 != h0 || m1 != m0 {
		t.Fatalf("legacy tables' filters were consulted: %d filtered, %d admitted", h1-h0, m1-m0)
	}
	if _, files, err := eng.FilterRewriteStatus(); err != nil || files != 4 {
		t.Fatalf("legacy tables to rewrite: %d, %v; want the 4 flushed", files, err)
	}

	attempted := map[uint64]bool{}
	for pass := 0; ; pass++ {
		if stale, err := eng.filterStaleTables(); err == nil {
			for _, st := range stale {
				t.Logf("pass %d: table %d at L%d, %d bytes, %s .. %s", pass, st.fileNum, st.level, st.size, keys.Key(st.smallest), keys.Key(st.largest))
			}
		}
		targeted, remaining, files, err := eng.FilterRewritePass(context.Background(), 0, attempted)
		if err != nil {
			t.Fatal(err)
		}
		if remaining == 0 {
			break
		}
		if targeted == 0 || pass > 20 {
			t.Fatalf("rewrite stalled after %d passes: %d bytes in %d files remain", pass+1, remaining, files)
		}
	}
	levels, err := eng.db.SSTables(pebble.WithProperties())
	if err != nil {
		t.Fatal(err)
	}
	tables := 0
	for _, level := range levels {
		for _, tb := range level {
			tables++
			if tb.Properties == nil || tb.Properties.FilterPolicyName != mvccPrefixBloomName {
				t.Fatalf("table %d after the rewrite: filter policy %q", tb.FileNum, tb.Properties.FilterPolicyName)
			}
		}
	}
	if tables == 0 {
		t.Fatal("no live tables after the rewrite")
	}
	check("rewritten tables")
	h2, m2 := eng.FilterMetrics()
	for i := 1; i < rows; i += 2 {
		if v, err := MVCCGet(eng, keyOf(i), ts(5, 0), MVCCGetOptions{}); err != nil || v != nil {
			t.Fatal(err)
		}
	}
	h3, _ := eng.FilterMetrics()
	if h3-h2 < rows/2 {
		t.Fatalf("after the rewrite absent reads were filtered %d times over %d reads", h3-h2, rows/2)
	}
	_ = m2
}

// TestPlainOpenReadsPrefixModeTables: a store written in prefix mode
// (columnar tables under the MVCC key schema, prefix filters) opens and
// reads in plain mode — the node's first open at start is one, before
// it has read the store's cluster version — with the prefix filters
// left unconsulted, and returns to prefix mode with everything intact.
func TestPlainOpenReadsPrefixModeTables(t *testing.T) {
	dir := t.TempDir()
	keyOf := func(i int) keys.Key { return append(keys.TablePrefix.Clone(), fmt.Sprintf("/%05d", i)...) }
	const rows = 300
	testingPebbleOptions = func(o *pebble.Options) { o.FormatMajorVersion = pebble.FormatColumnarBlocks }
	t.Cleanup(func() { testingPebbleOptions = nil })
	write := func(eng *Engine, from, to int) {
		b := eng.NewBatch()
		for i := from; i < to; i += 2 {
			if err := MVCCPut(b, keyOf(i), ts(1, 0), []byte(fmt.Sprintf("%d", i)), nil); err != nil {
				t.Fatal(err)
			}
		}
		if err := b.Commit(true); err != nil {
			t.Fatal(err)
		}
		_ = b.Close()
		if err := eng.Flush(); err != nil {
			t.Fatal(err)
		}
	}
	check := func(eng *Engine, stage string, to int) {
		t.Helper()
		for i := 0; i < rows; i++ {
			v, err := MVCCGet(eng, keyOf(i), ts(5, 0), MVCCGetOptions{})
			if err != nil {
				t.Fatalf("%s: %s: %v", stage, keyOf(i), err)
			}
			if want := i%2 == 0 && i < to; (v != nil) != want || (want && string(v) != fmt.Sprintf("%d", i)) {
				t.Fatalf("%s: %s: %q, want present=%v", stage, keyOf(i), v, want)
			}
		}
	}
	eng, err := Open(dir, Options{PrefixBloom: true})
	if err != nil {
		t.Fatal(err)
	}
	write(eng, 0, rows/2)
	check(eng, "prefix mode", rows/2)
	if err := eng.Close(); err != nil {
		t.Fatal(err)
	}

	eng, err = Open(dir, Options{})
	if err != nil {
		t.Fatalf("plain open of a prefix-mode store: %v", err)
	}
	h0, m0 := eng.FilterMetrics()
	check(eng, "plain mode", rows/2)
	if h1, m1 := eng.FilterMetrics(); h1 != h0 || m1 != m0 {
		t.Fatalf("plain mode consulted prefix filters: %d filtered, %d admitted", h1-h0, m1-m0)
	}
	write(eng, rows/2, rows)
	check(eng, "plain mode after writing", rows)
	if err := eng.Close(); err != nil {
		t.Fatal(err)
	}

	eng, err = Open(dir, Options{PrefixBloom: true})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = eng.Close() }()
	check(eng, "prefix mode again", rows)
	if _, files, err := eng.FilterRewriteStatus(); err != nil || files == 0 {
		t.Fatalf("the plain-mode table is not up for rewrite: %d files, %v", files, err)
	}
}
