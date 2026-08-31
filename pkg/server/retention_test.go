package server

import (
	"testing"
	"time"

	"github.com/sthorne/datax/pkg/keys"
)

// TestRetentionOverrideMatrix exercises the span→TTL mapping against a
// fixed table set: full containment takes the table's TTL with expiry;
// partial overlap takes max(default, retentions) without expiry; no
// overlap defers to the default.
func TestRetentionOverrideMatrix(t *testing.T) {
	shortLo, shortHi := keys.TableDataSpan(10) // 1h retention
	longLo, longHi := keys.TableDataSpan(11)   // 100h retention
	p := &retentionProvider{defaultTTL: 24 * time.Hour}
	p.spans = []retentionSpan{
		{start: shortLo, end: shortHi, ttl: time.Hour},
		{start: longLo, end: longHi, ttl: 100 * time.Hour},
	}
	p.fetchedAt = time.Now() // pin the cache; no node/db behind this test

	// Range fully inside the short-retention table: its TTL, with expiry —
	// even though it is shorter than the default.
	if ttl, expire, ok := p.override(shortLo, shortHi); !ok || !expire || ttl != time.Hour {
		t.Fatalf("full containment: %v %v %v", ttl, expire, ok)
	}
	// A sub-span (a bucket range after splits) too.
	if ttl, expire, ok := p.override(shortLo.Next(), shortLo.Next().Next()); !ok || !expire || ttl != time.Hour {
		t.Fatalf("sub-span containment: %v %v %v", ttl, expire, ok)
	}

	// Partial overlap (range straddles the table's start): never expire,
	// never below the default.
	mixedStart := keys.Key{0x04} // before table 10's prefix
	if ttl, expire, ok := p.override(mixedStart, shortHi); !ok || expire || ttl != 24*time.Hour {
		t.Fatalf("mixed short: %v %v %v", ttl, expire, ok)
	}
	// Partial overlap with the long-retention table: the retention wins
	// over the default (keep longer).
	if ttl, expire, ok := p.override(mixedStart, longHi); !ok || expire || ttl != 100*time.Hour {
		t.Fatalf("mixed long: %v %v %v", ttl, expire, ok)
	}

	// No overlap: no override.
	if _, _, ok := p.override(keys.Key{0x02}, keys.Key{0x03}); ok {
		t.Fatalf("no-overlap span got an override")
	}
}
