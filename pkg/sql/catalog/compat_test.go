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

// A descriptor carrying the v21 table options: a GC window and a row TTL.
const goldenTableDescriptorV21 = `{"id":9,"name":"q","columns":[{"id":1,"name":"a","type":1,"not_null":true},{"id":2,"name":"at","type":6}],"primary_key":[1],"gc_ttl_seconds":600,"row_ttl":{"column_id":2,"after_seconds":86400}}`

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

	// The v21 fields are additive: absent from the golden above, and
	// encoded as below when set.
	if d.GCTTLSeconds != 0 || d.RowTTL != nil {
		t.Fatalf("v21 fields set by an older descriptor: %+v", d)
	}
	var v21 TableDescriptor
	if err := json.Unmarshal([]byte(goldenTableDescriptorV21), &v21); err != nil {
		t.Fatal(err)
	}
	if v21.GCTTLSeconds != 600 || v21.RowTTL == nil || v21.RowTTL.ColumnID != 2 || v21.RowTTL.AfterSeconds != 86400 {
		t.Fatalf("decoded %+v", v21)
	}
	if c := v21.Clone(); c.RowTTL == v21.RowTTL || *c.RowTTL != *v21.RowTTL {
		t.Fatalf("Clone shares or loses the row TTL: %+v", c.RowTTL)
	}
	if raw, err := json.Marshal(v21); err != nil || string(raw) != goldenTableDescriptorV21 {
		t.Fatalf("encoding changed:\n got %s\nwant %s (%v)", raw, goldenTableDescriptorV21, err)
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

// goldenInvertedIndex is an index descriptor naming an inverted kind
// (cluster version v21). The field is omitempty: an ordinary index
// encodes exactly as it did before the field existed.
const goldenInvertedIndex = `{"id":3,"name":"docs_gin","column_ids":[2],"inverted":"jsonb_ops"}`

func TestGoldenInvertedIndexDescriptor(t *testing.T) {
	var idx IndexDescriptor
	if err := json.Unmarshal([]byte(goldenInvertedIndex), &idx); err != nil {
		t.Fatal(err)
	}
	if !idx.IsInverted() || idx.Inverted != "jsonb_ops" || idx.ID != 3 || len(idx.ColumnIDs) != 1 || !idx.Public() {
		t.Fatalf("decoded %+v", idx)
	}
	raw, err := json.Marshal(idx)
	if err != nil {
		t.Fatal(err)
	}
	if string(raw) != goldenInvertedIndex {
		t.Fatalf("encoding changed:\n got %s\nwant %s", raw, goldenInvertedIndex)
	}
	plain, err := json.Marshal(IndexDescriptor{ID: 2, Name: "by_city", ColumnIDs: []ColumnID{2}})
	if err != nil {
		t.Fatal(err)
	}
	if string(plain) != `{"id":2,"name":"by_city","column_ids":[2]}` || (&IndexDescriptor{}).IsInverted() {
		t.Fatalf("an ordinary index gained a field: %s", plain)
	}
}

// goldenTableDescriptorRLS is a table with row security (cluster version
// v21, issue #294). The fields are omitempty: a table without row
// security encodes exactly as it did before they existed, which the
// goldens above pin.
const goldenTableDescriptorRLS = `{"id":9,"name":"docs","columns":[{"id":1,"name":"tenant","type":3,"not_null":true}],"primary_key":[1],"owner":"app","row_security":true,"force_row_security":true,"policies":[{"name":"tenant","command":"all","using":"tenant = current_setting('app.tenant')"},{"name":"writers","restrictive":true,"command":"update","roles":["alice","bob"],"using":"true","check":"tenant != 'frozen'"}]}`

func TestGoldenRowSecurityDescriptor(t *testing.T) {
	var d TableDescriptor
	if err := json.Unmarshal([]byte(goldenTableDescriptorRLS), &d); err != nil {
		t.Fatal(err)
	}
	if !d.RowSecurity || !d.ForceRowSecurity || len(d.Policies) != 2 {
		t.Fatalf("decoded %+v", d)
	}
	p, ok := d.Policy("writers")
	if !ok || !p.Restrictive || p.Command != PolicyUpdate || len(p.Roles) != 2 || p.Using != "true" || p.Check != "tenant != 'frozen'" {
		t.Fatalf("policy %+v", p)
	}
	if p0, ok := d.Policy("tenant"); !ok || p0.Restrictive || p0.Command != PolicyAll || p0.Roles != nil || p0.Check != "" {
		t.Fatalf("policy %+v", p0)
	}
	if _, ok := d.Policy("nosuch"); ok {
		t.Fatal("found a policy that is not there")
	}
	raw, err := json.Marshal(d)
	if err != nil || string(raw) != goldenTableDescriptorRLS {
		t.Fatalf("encoding changed:\n got %s\nwant %s (%v)", raw, goldenTableDescriptorRLS, err)
	}
	// Clone shares nothing a later ALTER POLICY would write through.
	c := d.Clone()
	c.Policies[1].Roles[0] = "mallory"
	c.Policies[0].Using = "true"
	if d.Policies[1].Roles[0] != "alice" || d.Policies[0].Using == "true" || !c.RowSecurity || !c.ForceRowSecurity {
		t.Fatalf("Clone shares the policies: %+v", d.Policies)
	}
	// A descriptor from before the fields existed has no row security.
	var old TableDescriptor
	if err := json.Unmarshal([]byte(goldenTableDescriptorV21), &old); err != nil {
		t.Fatal(err)
	}
	if old.RowSecurity || old.ForceRowSecurity || old.Policies != nil {
		t.Fatalf("row security set by an older descriptor: %+v", old)
	}
}

// goldenTableDescriptorSealed is a sealed table (cluster version v22,
// issue #310). The field is omitempty: a table that is not sealed encodes
// as it did before.
const goldenTableDescriptorSealed = `{"id":12,"name":"notes","columns":[{"id":1,"name":"id","type":1,"not_null":true},{"id":2,"name":"body","type":3}],"primary_key":[1],"encryption_key":{"key_id":4242,"seal_id":12}}`

func TestGoldenSealedDescriptor(t *testing.T) {
	var d TableDescriptor
	if err := json.Unmarshal([]byte(goldenTableDescriptorSealed), &d); err != nil {
		t.Fatal(err)
	}
	if !d.Sealed() || d.EncryptionKey.KeyID != 4242 || d.EncryptionKey.SealID != 12 {
		t.Fatalf("decoded %+v", d)
	}
	raw, err := json.Marshal(d)
	if err != nil || string(raw) != goldenTableDescriptorSealed {
		t.Fatalf("encoding changed:\n got %s\nwant %s (%v)", raw, goldenTableDescriptorSealed, err)
	}
	c := d.Clone()
	c.EncryptionKey.KeyID = 1
	if d.EncryptionKey.KeyID != 4242 {
		t.Fatal("Clone shares the encryption key")
	}
	var old TableDescriptor
	if err := json.Unmarshal([]byte(goldenTableDescriptorV21), &old); err != nil {
		t.Fatal(err)
	}
	if old.Sealed() {
		t.Fatalf("an older descriptor reads as sealed: %+v", old)
	}
}

// TestGoldenRoleBypassRLS: the BYPASSRLS attribute is omitempty, so a
// role without it encodes as before.
func TestGoldenRoleBypassRLS(t *testing.T) {
	const golden = `{"name":"auditor","login":true,"bypassrls":true}`
	var r RoleDescriptor
	if err := json.Unmarshal([]byte(golden), &r); err != nil || !r.BypassRLS || !r.Login {
		t.Fatalf("decoded %+v (%v)", r, err)
	}
	if raw, err := json.Marshal(r); err != nil || string(raw) != golden {
		t.Fatalf("encoding changed: %s (%v)", raw, err)
	}
	if raw, _ := json.Marshal(RoleDescriptor{Name: "app", Login: true}); string(raw) != `{"name":"app","login":true}` {
		t.Fatalf("a role without the attribute gained a field: %s", raw)
	}
	if c := r.Clone(); !c.BypassRLS {
		t.Fatal("Clone lost the attribute")
	}
}

// TestRowSecurityConfines: who the console treats as confined by a
// table's policies, and that such a role is shown no row of the table
// (CanReadTable) while still seeing that it exists (CanSeeTable).
func TestRowSecurityConfines(t *testing.T) {
	d := &TableDescriptor{Name: "docs", Owner: "app_owner", Privileges: map[string][]string{"tenant": {"SELECT"}}}
	tenant := RoleSet{"tenant": true, PublicRole: true}
	owner := RoleSet{"app_owner": true, PublicRole: true}
	admin := RoleSet{"ops": true, AdminRole: true, PublicRole: true}
	root := RoleSet{RootRole: true, AdminRole: true, PublicRole: true}
	if RowSecurityConfines(tenant, d) || !CanReadTable(tenant, d) {
		t.Fatal("a table without row security confines a grantee")
	}
	d.RowSecurity = true
	if !RowSecurityConfines(tenant, d) || CanReadTable(tenant, d) || !CanSeeTable(tenant, d) {
		t.Fatal("a grantee of a row-secured table: must be confined, shown no row, and still see the table")
	}
	for name, set := range map[string]RoleSet{"owner": owner, "admin": admin, "root": root} {
		if RowSecurityConfines(set, d) || !CanReadTable(set, d) {
			t.Fatalf("%s is confined without FORCE", name)
		}
	}
	d.ForceRowSecurity = true
	if !RowSecurityConfines(owner, d) || !RowSecurityConfines(admin, d) || CanReadTable(owner, d) {
		t.Fatal("FORCE does not confine the owner and admins")
	}
	if RowSecurityConfines(root, d) || !CanReadTable(root, d) {
		t.Fatal("root is confined")
	}
}
