package server

import (
	"testing"
	"time"

	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/sql/rowenc"
	"github.com/sthorne/datax/pkg/sql/types"
	"github.com/sthorne/datax/pkg/util/hlc"
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

// TestRowExpiryPredicate: the mixed-range row-level predicate ages a
// version out only when the row's timestamp column AND the version's
// write timestamp are both past the table's retention, and never touches
// foreign keys, index entries, or fully-contained ranges.
func TestRowExpiryPredicate(t *testing.T) {
	tsLo, tsHi := keys.TableDataSpan(10)
	p := &retentionProvider{defaultTTL: 24 * time.Hour}
	p.spans = []retentionSpan{{
		start: tsLo, end: tsHi, ttl: time.Hour,
		prefixes: []keys.Key{keys.TableIndexPrefix(10, 1)},
		pkFams:   []types.Family{types.Int, types.Timestamp},
	}}
	p.fetchedAt = time.Now()

	pkKey := func(id, atNanos int64) keys.Key {
		k, err := rowenc.AppendKeyDatum(keys.TableIndexPrefix(10, 1), types.Int, types.NewInt(id))
		if err != nil {
			t.Fatal(err)
		}
		k, err = rowenc.AppendKeyDatum(k, types.Timestamp, types.NewTimestamp(atNanos))
		if err != nil {
			t.Fatal(err)
		}
		return k
	}
	now := time.Now().UnixNano()
	oldTS := now - 2*time.Hour.Nanoseconds() // past the 1h retention
	freshTS := now - 10*time.Minute.Nanoseconds()
	oldWrite := hlc.Timestamp{WallTime: oldTS}
	freshWrite := hlc.Timestamp{WallTime: now}

	// A range straddling the table edge gets a predicate.
	pred, ok := p.rowExpiry(keys.Key("\x03"), tsHi)
	if !ok {
		t.Fatal("no predicate for a straddling range")
	}
	if !pred(pkKey(1, oldTS), oldWrite) {
		t.Fatal("old row with old write not expired")
	}
	if pred(pkKey(1, oldTS), freshWrite) {
		t.Fatal("freshly rewritten old row expired")
	}
	if pred(pkKey(1, freshTS), oldWrite) {
		t.Fatal("fresh row expired")
	}
	if pred(keys.Key("\x03zz"), oldWrite) {
		t.Fatal("foreign key expired")
	}
	// A key under the table but not a known primary generation (e.g. an
	// index entry at another index ID) is kept.
	idxKey, err := rowenc.AppendKeyDatum(keys.TableIndexPrefix(10, 2), types.Int, types.NewInt(1))
	if err != nil {
		t.Fatal(err)
	}
	if pred(idxKey, oldWrite) {
		t.Fatal("non-primary generation expired")
	}

	// Fully contained ranges are expire-mode GC's job: no predicate.
	if _, ok := p.rowExpiry(tsLo, tsHi); ok {
		t.Fatal("predicate offered for a contained range")
	}
	// No overlap: no predicate.
	if _, ok := p.rowExpiry(keys.Key("\x03"), keys.Key("\x03z")); ok {
		t.Fatal("predicate offered for a non-overlapping range")
	}
}
