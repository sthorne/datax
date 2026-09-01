package catalog

import (
	"encoding/json"
	"testing"

	"github.com/sthorne/datax/pkg/sql/types"
)

// Golden compatibility test: the frozen JSON encoding of TableDescriptor.
// Descriptors are persisted verbatim (and travel verbatim through backups),
// so per the rules in pkg/version old encodings must decode forever and the
// shape may only grow additively (omitempty, safe zero values). A failure
// here means a change that breaks rolling upgrades or restore of old
// backups.

const goldenTableDescriptor = `{"id":42,"name":"orders","columns":[{"id":1,"name":"id","type":1,"not_null":true},{"id":2,"name":"note","type":3,"default":{"fam":3,"s":"x"},"fill_default":true},{"id":3,"name":"_shard","type":1,"hidden":true},{"id":4,"name":"amt","type":9,"precision":10,"scale":2}],"primary_key":[3,1],"indexes":[{"id":2,"name":"by_note","unique":true,"column_ids":[2],"state":"write-only"}],"next_index_id":3,"next_column_id":5,"privileges":{"bob":["SELECT","INSERT"]},"version":7,"timeseries":true,"retention_seconds":3600,"shard_buckets":8,"primary_index":5,"reshard":{"new_index_id":6,"new_buckets":16,"new_index_ids":[7]},"resharded_at":123456789,"retired_layouts":[{"primary_index_id":1,"index_ids":[2],"buckets":4,"retired_at":123456789}]}`

// A v1-era descriptor: only the original fields. Zero values of every later
// field must keep it valid (PrimaryIndex 0 = index 1, etc.).
const goldenTableDescriptorV1 = `{"id":7,"name":"t","columns":[{"id":1,"name":"a","type":1,"not_null":true}],"primary_key":[1]}`

func TestGoldenTableDescriptor(t *testing.T) {
	var d TableDescriptor
	if err := json.Unmarshal([]byte(goldenTableDescriptor), &d); err != nil {
		t.Fatal(err)
	}
	if d.ID != 42 || d.Name != "orders" || len(d.Columns) != 4 ||
		d.Columns[1].Default == nil || d.Columns[1].Default.S != "x" ||
		!d.Columns[2].Hidden || len(d.PrimaryKey) != 2 ||
		d.Columns[3].Precision != 10 || d.Columns[3].Scale != 2 ||
		len(d.Indexes) != 1 || d.Indexes[0].State != IndexStateWriteOnly ||
		d.NextIndexID != 3 || d.NextColumnID != 5 ||
		len(d.Privileges["bob"]) != 2 || d.Version != 7 ||
		!d.Timeseries || d.RetentionSeconds != 3600 || d.ShardBuckets != 8 ||
		d.PrimaryIndex != 5 || d.Reshard == nil || d.Reshard.NewBuckets != 16 ||
		len(d.Reshard.NewIndexIDs) != 1 || d.Reshard.NewIndexIDs[0] != 7 ||
		d.ReshardedAt != 123456789 ||
		len(d.RetiredLayouts) != 1 || d.RetiredLayouts[0].PrimaryIndexID != 1 ||
		len(d.RetiredLayouts[0].IndexIDs) != 1 || d.RetiredLayouts[0].Buckets != 4 ||
		d.RetiredLayouts[0].RetiredAt != 123456789 {
		t.Fatalf("decoded %+v", d)
	}
	if d.Columns[0].Type != types.Int || d.Columns[1].Type != types.String {
		t.Fatalf("column types: %+v", d.Columns)
	}
	raw, err := json.Marshal(d)
	if err != nil {
		t.Fatal(err)
	}
	if string(raw) != goldenTableDescriptor {
		t.Fatalf("encoding changed:\n got %s\nwant %s", raw, goldenTableDescriptor)
	}

	var old TableDescriptor
	if err := json.Unmarshal([]byte(goldenTableDescriptorV1), &old); err != nil {
		t.Fatal(err)
	}
	if old.ID != 7 || len(old.Columns) != 1 || old.LivePrimaryIndex() != 1 ||
		old.Timeseries || old.Reshard != nil {
		t.Fatalf("v1 descriptor decoded %+v", old)
	}
}
