package rowenc

import (
	"bytes"
	"testing"

	"github.com/sthorne/datax/pkg/sql/catalog"
	"github.com/sthorne/datax/pkg/sql/types"
)

// TestPrimaryIndexAware: EncodePK/DecodePK and the prefix helpers follow
// the descriptor's live primary index — a re-sharded table (PrimaryIndex
// != 1) round-trips at its new keyspace, and the old-layout keyspace
// stays addressable via EncodePKAt.
func TestPrimaryIndexAware(t *testing.T) {
	desc := shardedDesc(8)
	vals := []types.Datum{types.NewInt(3), types.NewString("cpu"), types.NewTimestamp(1_700_000_000_000_000_000)}

	oldKey, err := EncodePK(desc, vals)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.HasPrefix(oldKey, PrimaryKeyPrefixFor(desc)) || !bytes.HasPrefix(oldKey, PrimaryKeyPrefix(desc.ID)) {
		t.Fatal("default layout does not live at index 1")
	}

	// Re-shard: live primary moves to index 5.
	desc.PrimaryIndex = 5
	newKey, err := EncodePK(desc, vals)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Equal(oldKey, newKey) {
		t.Fatal("new layout collides with the old")
	}
	if !bytes.HasPrefix(newKey, PrimaryKeyPrefixFor(desc)) {
		t.Fatal("live prefix does not cover the new layout")
	}
	lo, hi := PrimarySpanFor(desc)
	if bytes.Compare(newKey, lo) < 0 || bytes.Compare(newKey, hi) >= 0 {
		t.Fatal("new key outside the live primary span")
	}

	// Round-trip decode at the new layout.
	got, err := DecodePK(desc, newKey)
	if err != nil {
		t.Fatal(err)
	}
	for i := range vals {
		if c, _ := got[i].Compare(vals[i]); c != 0 {
			t.Fatalf("decode mismatch at %d: %v vs %v", i, got[i], vals[i])
		}
	}

	// EncodePKAt still addresses the old layout explicitly (the backfill's
	// reader) and matches what EncodePK produced before the swap.
	atOld, err := EncodePKAt(desc, 1, vals)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(atOld, oldKey) {
		t.Fatal("EncodePKAt(1) does not reproduce the pre-swap key")
	}

	// Index entries on a re-sharded table point back at the LIVE layout.
	idx := &catalog.IndexDescriptor{ID: 6, Name: "by_series", ColumnIDs: []catalog.ColumnID{1}}
	desc.Indexes = []catalog.IndexDescriptor{*idx}
	row := map[catalog.ColumnID]types.Datum{1: vals[1], 2: vals[2], 3: vals[0]}
	key, value, skip, err := EncodeIndexEntry(desc, idx, row)
	if err != nil || skip {
		t.Fatalf("index entry: %v skip=%v", err, skip)
	}
	pk, err := IndexEntryPrimaryKey(desc, idx, key, value)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(pk, newKey) {
		t.Fatalf("index entry points at %x, live row is %x", pk, newKey)
	}
}

// TestShardBucketAt: the default-count path delegates (identical values),
// and different counts redistribute while staying in range.
func TestShardBucketAt(t *testing.T) {
	desc := shardedDesc(8)
	vals := []types.Datum{types.NewString("cpu"), types.NewTimestamp(1_700_000_000_000_000_000)}
	d1, err := ShardBucket(desc, vals)
	if err != nil {
		t.Fatal(err)
	}
	d2, err := ShardBucketAt(desc, vals, 8)
	if err != nil {
		t.Fatal(err)
	}
	if d1.I != d2.I {
		t.Fatalf("delegation mismatch: %d vs %d", d1.I, d2.I)
	}
	for _, m := range []int32{2, 16, 256} {
		d, err := ShardBucketAt(desc, vals, m)
		if err != nil {
			t.Fatal(err)
		}
		if d.I < 0 || d.I >= int64(m) {
			t.Fatalf("bucket %d out of range for %d", d.I, m)
		}
	}
}
