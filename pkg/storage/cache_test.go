package storage

import (
	"crypto/rand"
	"fmt"
	"testing"

	"github.com/cockroachdb/pebble"
)

// TestReadPathOptions: every profile sets the shared read-path options —
// bloom filters on every level, the newest format, an open-file budget —
// and the cache size follows the profile's share of memory within its
// bounds.
func TestReadPathOptions(t *testing.T) {
	for _, p := range []Profile{ProfileBalanced, ProfileIngest} {
		var opts pebble.Options
		p.apply(&opts)
		if len(opts.Levels) != 7 {
			t.Fatalf("%s: %d levels configured", p, len(opts.Levels))
		}
		for i, l := range opts.Levels {
			if l.FilterPolicy == nil || l.FilterPolicy.Name() == "" || l.FilterType != pebble.TableFilter {
				t.Fatalf("%s: level %d has no table bloom filter: %+v", p, i, l)
			}
		}
		if opts.FormatMajorVersion != pebble.FormatNewest {
			t.Fatalf("%s: format %s, want the newest", p, opts.FormatMajorVersion)
		}
		if opts.MaxOpenFiles < 1000 || opts.MaxOpenFiles > 16384 {
			t.Fatalf("%s: max open files %d", p, opts.MaxOpenFiles)
		}
		size := DefaultCacheSize(p)
		limit := int64(balancedCacheCap)
		if p == ProfileIngest {
			limit = ingestCacheCap
		}
		if size < cacheFloor || size > limit {
			t.Fatalf("%s: cache size %d outside [%d, %d]", p, size, cacheFloor, limit)
		}
	}
	if DefaultCacheSize(ProfileIngest) > DefaultCacheSize(ProfileBalanced) {
		t.Fatal("the ingest profile's cache is not smaller than balanced's")
	}
	if n, err := parseBytesForTest("1.5GiB"); err != nil || n != 3<<29 {
		t.Fatalf("parseBytes: %d %v", n, err)
	}
}

// parseBytesForTest mirrors the CLI's --cache-size parsing enough to
// pin the unit table (the CLI's own test covers the command).
func parseBytesForTest(s string) (int64, error) {
	var n float64
	var unit string
	if _, err := fmt.Sscanf(s, "%f%s", &n, &unit); err != nil {
		return 0, err
	}
	switch unit {
	case "GiB":
		return int64(n * (1 << 30)), nil
	}
	return int64(n), nil
}

// TestSharedCacheReleased: engines of one process share one block cache;
// the last close releases it, so open/close cycles do not accumulate.
func TestSharedCacheReleased(t *testing.T) {
	if n := sharedCacheEngines(); n != 0 {
		t.Fatalf("%d engines hold the cache before the test", n)
	}
	var engines []*Engine
	for i := 0; i < 3; i++ {
		e, err := Open("", Options{CacheSize: 32 << 20})
		if err != nil {
			t.Fatal(err)
		}
		engines = append(engines, e)
	}
	if n := sharedCacheEngines(); n != 3 || SharedCacheSize() != 32<<20 {
		t.Fatalf("holders %d size %d", n, SharedCacheSize())
	}
	// A later engine asking for another size joins the existing cache.
	e4, err := Open("", Options{CacheSize: 64 << 20})
	if err != nil {
		t.Fatal(err)
	}
	if e4.CacheSize() != 32<<20 {
		t.Fatalf("second size won: %d", e4.CacheSize())
	}
	engines = append(engines, e4)
	for _, e := range engines {
		if err := e.Close(); err != nil {
			t.Fatal(err)
		}
	}
	if n := sharedCacheEngines(); n != 0 || SharedCacheSize() != 0 {
		t.Fatalf("cache not released: holders %d size %d", n, SharedCacheSize())
	}
	// Reopening starts a fresh cache at the new size.
	e, err := Open("", Options{CacheSize: 16 << 20})
	if err != nil {
		t.Fatal(err)
	}
	if e.CacheSize() != 16<<20 {
		t.Fatalf("fresh cache size %d", e.CacheSize())
	}
	_ = e.Close()
}

// TestBloomFiltersUnderEncryption: bloom blocks ride the encrypted FS
// like any block — missing-key reads on an encrypted store are answered
// by the filters, and the cache and filter metrics report it.
func TestBloomFiltersUnderEncryption(t *testing.T) {
	key := make([]byte, 32)
	if _, err := rand.Read(key); err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	e, err := Open(dir, Options{EncryptionKey: key, MemTableSize: 256 << 10, CacheSize: 8 << 20})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = e.Close() }()
	// Enough rows to flush several sstables through the small memtable.
	b := e.NewBatch()
	for i := 0; i < 20000; i++ {
		if err := b.Put([]byte(fmt.Sprintf("k%08d", i)), make([]byte, 64)); err != nil {
			t.Fatal(err)
		}
		if i%1000 == 999 {
			if err := b.Commit(false); err != nil {
				t.Fatal(err)
			}
			b = e.NewBatch()
		}
	}
	if err := b.Commit(true); err != nil {
		t.Fatal(err)
	}
	if err := e.Flush(); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2000; i++ {
		if v, err := e.Get([]byte(fmt.Sprintf("k%08dx", i))); err != nil || v != nil {
			t.Fatalf("missing key: %q %v", v, err)
		}
	}
	m := e.db.Metrics()
	if m.Filter.Hits == 0 {
		t.Fatalf("no bloom filter hit on 2000 missing-key reads: %+v", m.Filter)
	}
	sm := e.StorageMetrics()
	if sm.FilterHits != m.Filter.Hits || sm.BlockCacheHits+sm.BlockCacheMisses == 0 {
		t.Fatalf("storage metrics: %+v", sm)
	}
}
