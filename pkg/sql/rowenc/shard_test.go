package rowenc

import (
	"testing"

	"github.com/sthorne/datax/pkg/sql/catalog"
	"github.com/sthorne/datax/pkg/sql/types"
)

func shardedDesc(buckets int32) *catalog.TableDescriptor {
	return &catalog.TableDescriptor{
		ID:   42,
		Name: "m",
		Columns: []catalog.Column{
			{ID: 1, Name: "series", Type: types.String, NotNull: true},
			{ID: 2, Name: "ts", Type: types.Timestamp, NotNull: true},
			{ID: 3, Name: "_shard", Type: types.Int, NotNull: true, Hidden: true},
		},
		PrimaryKey:   []catalog.ColumnID{3, 1, 2},
		Timeseries:   true,
		ShardBuckets: buckets,
	}
}

// TestShardBucket pins the frozen on-disk hash: exact values for known
// inputs (a change here strands every existing sharded table), plus
// determinism, range, and a sanity check that buckets actually spread.
func TestShardBucket(t *testing.T) {
	desc := shardedDesc(8)

	// FROZEN: these exact values are baked into row keys. If this
	// assertion ever fails, the hash or the key encoding changed — that
	// is an on-disk format break, not a test to update.
	frozen := []struct {
		series string
		ts     int64
		want   int64
	}{
		{"cpu", 1_700_000_000_000_000_000, 2},
		{"cpu", 1_700_000_060_000_000_000, 0},
		{"mem", 1_700_000_000_000_000_000, 1},
	}
	for _, f := range frozen {
		d, err := ShardBucket(desc, []types.Datum{types.NewString(f.series), types.NewTimestamp(f.ts)})
		if err != nil {
			t.Fatal(err)
		}
		if d.I != f.want {
			t.Fatalf("FROZEN HASH CHANGED: bucket(%q, %d) = %d, want %d", f.series, f.ts, d.I, f.want)
		}
	}

	seen := map[int64]bool{}
	for i := int64(0); i < 64; i++ {
		d, err := ShardBucket(desc, []types.Datum{types.NewString("series-x"), types.NewTimestamp(i * 1e9)})
		if err != nil {
			t.Fatal(err)
		}
		if d.I < 0 || d.I >= 8 {
			t.Fatalf("bucket out of range: %d", d.I)
		}
		seen[d.I] = true
		again, _ := ShardBucket(desc, []types.Datum{types.NewString("series-x"), types.NewTimestamp(i * 1e9)})
		if again.I != d.I {
			t.Fatal("bucket not deterministic")
		}
	}
	if len(seen) < 4 {
		t.Fatalf("64 timestamps landed in only %d of 8 buckets", len(seen))
	}

	if _, err := ShardBucket(&catalog.TableDescriptor{Name: "p"}, nil); err == nil {
		t.Fatal("unsharded table accepted")
	}
	if _, err := ShardBucket(desc, []types.Datum{types.NewString("x")}); err == nil {
		t.Fatal("wrong datum count accepted")
	}
}
