package rowenc

import (
	"fmt"
	"hash/fnv"

	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/sql/catalog"
	"github.com/sthorne/datax/pkg/sql/types"
)

// ShardColumnName is the hidden bucket column of a sharded timeseries
// table. It leads the primary key so a monotone timestamp tail spreads
// across ShardBuckets key prefixes.
const ShardColumnName = "_shard"

// ShardBucket computes the hidden _shard value for a row of a sharded
// timeseries table: FNV-32a over the order-preserving key encodings
// (AppendKeyDatum) of the logical — non-hidden — primary-key datums, in
// primary-key order, mod ShardBuckets.
//
// FROZEN ON-DISK FORMAT: the hash function, the use of the key encoding
// as hash input, the column order, and the mod are all baked into every
// sharded table's row keys. Changing any of them strands existing rows
// (inserts and point lookups would compute different buckets). The key
// encoding as hash input is also what lets the planner recompute the
// bucket for a fully-pinned point lookup.
func ShardBucket(desc *catalog.TableDescriptor, logicalPK []types.Datum) (types.Datum, error) {
	return ShardBucketAt(desc, logicalPK, desc.ShardBuckets)
}

// ShardBucketAt computes the bucket for an explicit bucket count — the
// re-shard path hashes the same frozen encoding mod the NEW count. The
// hash and its input never vary; only the mod does, and only together
// with a full rewrite of the table.
func ShardBucketAt(desc *catalog.TableDescriptor, logicalPK []types.Datum, buckets int32) (types.Datum, error) {
	if buckets <= 0 || len(desc.PrimaryKey) < 2 {
		return types.Datum{}, fmt.Errorf("table %q is not shard-bucketed", desc.Name)
	}
	logical := desc.PrimaryKey[1:] // [0] is the hidden _shard column
	if len(logicalPK) != len(logical) {
		return types.Datum{}, fmt.Errorf("expected %d logical primary key values, got %d", len(logical), len(logicalPK))
	}
	h := fnv.New32a()
	var buf keys.Key
	for i, colID := range logical {
		col, ok := desc.ColByID(colID)
		if !ok {
			return types.Datum{}, fmt.Errorf("primary key column %d does not exist", colID)
		}
		enc, err := AppendKeyDatum(buf[:0], col.Type, logicalPK[i])
		if err != nil {
			return types.Datum{}, fmt.Errorf("column %q: %w", col.Name, err)
		}
		buf = enc
		_, _ = h.Write(buf)
	}
	return types.NewInt(int64(h.Sum32() % uint32(buckets))), nil
}
