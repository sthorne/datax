package rowenc

import (
	"bytes"
	"testing"

	"github.com/sthorne/datax/pkg/keys"
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

// TestEncodeIndexEntryAt: the shadow-ID variant places the entry in the
// override ID's keyspace, and the entry's primary-key suffix follows the
// row map — a shadow row carrying a re-shard's new bucket yields an entry
// that resolves to the new layout's primary key.
func TestEncodeIndexEntryAt(t *testing.T) {
	desc := shardedDesc(8)
	idx := &catalog.IndexDescriptor{ID: 6, Name: "by_series", ColumnIDs: []catalog.ColumnID{1}}
	desc.Indexes = []catalog.IndexDescriptor{*idx}
	row := map[catalog.ColumnID]types.Datum{
		1: types.NewString("cpu"),
		2: types.NewTimestamp(1_700_000_000_000_000_000),
		3: types.NewInt(3), // _shard
	}

	liveKey, _, _, err := EncodeIndexEntry(desc, idx, row)
	if err != nil {
		t.Fatal(err)
	}
	atKey, _, skip, err := EncodeIndexEntryAt(desc, idx, 9, row)
	if err != nil || skip {
		t.Fatalf("entry at 9: %v skip=%v", err, skip)
	}
	if !bytes.HasPrefix(atKey, keys.TableIndexPrefix(desc.ID, 9)) {
		t.Fatal("shadow entry not under the override index ID")
	}
	if bytes.Equal(liveKey, atKey) {
		t.Fatal("shadow entry collides with the live entry")
	}
	// Same row, same suffix: only the 8-byte index ID differs.
	livePfx, atPfx := keys.TableIndexPrefix(desc.ID, idx.ID), keys.TableIndexPrefix(desc.ID, 9)
	if !bytes.Equal(liveKey[len(livePfx):], atKey[len(atPfx):]) {
		t.Fatal("suffixes differ between live and shadow entries of the same row")
	}

	// A shadow row with a different bucket changes the embedded suffix,
	// and a unique entry's value resolves to that bucket's primary key.
	shadow := map[catalog.ColumnID]types.Datum{1: row[1], 2: row[2], 3: types.NewInt(5)}
	uidx := &catalog.IndexDescriptor{ID: 7, Name: "u", Unique: true, ColumnIDs: []catalog.ColumnID{1}}
	_, uval, _, err := EncodeIndexEntryAt(desc, uidx, 10, shadow)
	if err != nil {
		t.Fatal(err)
	}
	wantPK, err := EncodePK(desc, []types.Datum{shadow[3], shadow[1], shadow[2]})
	if err != nil {
		t.Fatal(err)
	}
	if got := append(PrimaryKeyPrefixFor(desc), uval...); !bytes.Equal(got, wantPK) {
		t.Fatalf("unique shadow value resolves to %x, want %x", got, wantPK)
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
