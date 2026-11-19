package vtable

import (
	"context"
	"strings"
	"testing"

	"github.com/sthorne/datax/pkg/sql/catalog"
	"github.com/sthorne/datax/pkg/sql/types"
)

// infoSchemaEnv is a database with a table of every kind of constraint,
// an identity column, an array column, a view over it, a sequence and
// grants.
func infoSchemaEnv(set catalog.RoleSet, admin bool) *Env {
	parent := &catalog.TableDescriptor{
		ID: 100, Name: "parent", DatabaseID: 1, Owner: "root",
		Columns: []catalog.Column{
			{ID: 1, Name: "id", Type: types.Int, Width: 4, NotNull: true},
			{ID: 2, Name: "code", Type: types.String, MaxLen: 10, NotNull: true},
			{ID: 3, Name: "tags", Type: types.ArrayOf(types.String)},
			{ID: 4, Name: "price", Type: types.Decimal, Precision: 10, Scale: 2},
		},
		PrimaryKey:  []catalog.ColumnID{1},
		Indexes:     []catalog.IndexDescriptor{{ID: 2, Name: "parent_code_key", Unique: true, ColumnIDs: []catalog.ColumnID{2}}},
		Constraints: []catalog.Constraint{{ID: 1, Name: "parent_price_check", Kind: catalog.ConstraintCheck, Columns: []catalog.ColumnID{4}, Expr: "price >= 0", Validated: true}},
		Privileges:  map[string][]string{"alice": {"SELECT"}, catalog.PublicRole: {"INSERT"}},
	}
	child := &catalog.TableDescriptor{
		ID: 101, Name: "child", DatabaseID: 1, Owner: "root",
		Columns: []catalog.Column{
			{ID: 1, Name: "cid", Type: types.Int, NotNull: true, Identity: "always", SequenceID: 201},
			{ID: 2, Name: "pid", Type: types.Int},
			{ID: 3, Name: "rowid", Type: types.Int, NotNull: true, Hidden: true},
		},
		PrimaryKey:  []catalog.ColumnID{1},
		Constraints: []catalog.Constraint{{ID: 1, Name: "child_pid_fkey", Kind: catalog.ConstraintForeign, Columns: []catalog.ColumnID{2}, RefTable: 100, RefColumns: []catalog.ColumnID{1}, Validated: true}},
	}
	keyless := &catalog.TableDescriptor{
		ID: 102, Name: "keyless", DatabaseID: 1, Owner: "alice",
		Columns:    []catalog.Column{{ID: 1, Name: "a", Type: types.Int}, {ID: 2, Name: "rowid", Type: types.Int, NotNull: true, Hidden: true}},
		PrimaryKey: []catalog.ColumnID{2},
	}
	view := &catalog.TableDescriptor{
		ID: 103, Name: "pv", DatabaseID: 1, Owner: "root",
		Columns:     []catalog.Column{{ID: 1, Name: "id", Type: types.Int}, {ID: 2, Name: "_ord", Type: types.Int, Hidden: true}},
		PrimaryKey:  []catalog.ColumnID{2},
		ViewQuery:   "SELECT id FROM parent",
		ViewDepends: []uint64{100},
	}
	// The session lists in information_schema the tables the reader
	// holds some privilege on.
	var tables []*catalog.TableDescriptor
	for _, d := range []*catalog.TableDescriptor{parent, child, keyless, view} {
		if catalog.CanSeeTable(set, d) {
			tables = append(tables, d)
		}
	}
	return &Env{
		Databases: []*catalog.DatabaseDescriptor{{ID: 1, Name: "db"}},
		Tables:    tables,
		Sequences: []*catalog.SequenceDescriptor{
			{ID: 200, Name: "s1", DatabaseID: 1, Start: 5, Increment: 2, MinValue: 1, MaxValue: 99, Privileges: map[string][]string{"alice": {"USAGE"}}},
			{ID: 201, Name: "child_cid_seq", DatabaseID: 1, Start: 1, Increment: 1, MinValue: 1, MaxValue: 99, OwnerTable: 101, OwnerColumn: 1},
		},
		Roles: []*catalog.RoleDescriptor{
			{Name: "root", Login: true},
			{Name: "alice", Login: true, MemberOf: []catalog.RoleMembership{{Role: "staff", Admin: true}}},
			{Name: "staff"},
		},
		RoleSet:  set,
		IsAdmin:  admin,
		Database: "db",
		User:     "alice",
	}
}

// TestInformationSchemaRowShapes runs every information_schema view
// over a database with something for each to list, as an admin and as
// a role with a few grants: each row is as wide as the view, and each
// view has the column list isSpecs gives it.
func TestInformationSchemaRowShapes(t *testing.T) {
	if len(informationSchemaTables) < len(isSpecs) {
		t.Fatalf("%d information_schema views registered, %d specified", len(informationSchemaTables), len(isSpecs))
	}
	for _, env := range []*Env{
		infoSchemaEnv(catalog.RoleSet{"root": true, catalog.AdminRole: true, catalog.PublicRole: true}, true),
		infoSchemaEnv(catalog.RoleSet{"alice": true, "staff": true, catalog.PublicRole: true}, false),
	} {
		for name, vt := range informationSchemaTables {
			if spec, ok := isSpecs[name]; ok {
				var names []string
				for _, c := range vt.Columns {
					names = append(names, c.Name)
				}
				if got := strings.Join(names, " "); got != stripKinds(spec) {
					t.Fatalf("%s columns:\n got %s\nwant %s", name, got, stripKinds(spec))
				}
			}
			rows, err := vt.Rows(context.Background(), env)
			if err != nil {
				t.Fatalf("%s: %v", name, err)
			}
			for _, r := range rows {
				if len(r) != len(vt.Columns) {
					t.Fatalf("%s: row of %d values, %d columns", name, len(r), len(vt.Columns))
				}
			}
		}
	}
}

func stripKinds(spec string) string {
	var out []string
	for _, f := range strings.Fields(spec) {
		name, _, _ := strings.Cut(f, ":")
		out = append(out, name)
	}
	return strings.Join(out, " ")
}

// TestInformationSchemaVisibility checks each view's rule for a role
// that owns one table and holds grants on others.
func TestInformationSchemaVisibility(t *testing.T) {
	env := infoSchemaEnv(catalog.RoleSet{"alice": true, "staff": true, catalog.PublicRole: true}, false)
	rows := func(view string, cols ...string) []string {
		t.Helper()
		vt := informationSchemaTables[view]
		at := map[string]int{}
		for i, c := range vt.Columns {
			at[c.Name] = i
		}
		rs, err := vt.Rows(context.Background(), env)
		if err != nil {
			t.Fatal(err)
		}
		var out []string
		for _, r := range rs {
			var parts []string
			for _, c := range cols {
				d := r[at[c]]
				if d.Null {
					parts = append(parts, "NULL")
				} else {
					parts = append(parts, d.Text())
				}
			}
			out = append(out, strings.Join(parts, "|"))
		}
		return out
	}
	want := func(got []string, want ...string) {
		t.Helper()
		if strings.Join(got, ";") != strings.Join(want, ";") {
			t.Fatalf("got %q, want %q", got, want)
		}
	}
	// table_privileges: alice's grant, PUBLIC's, and the table alice
	// owns — not root's own privileges on parent.
	want(rows("table_privileges", "grantee", "table_name", "privilege_type"),
		"alice|keyless|INSERT", "alice|keyless|SELECT", "alice|keyless|UPDATE", "alice|keyless|DELETE", "alice|keyless|TRUNCATE", "alice|keyless|REFERENCES", "alice|keyless|TRIGGER",
		"alice|parent|SELECT", "PUBLIC|parent|INSERT")
	// role_table_grants: no PUBLIC grant whose grantor is not alice.
	if got := strings.Join(rows("role_table_grants", "grantee", "table_name"), ","); strings.Contains(got, "PUBLIC") || !strings.Contains(got, "alice|parent") {
		t.Fatalf("role_table_grants: %v", got)
	}
	// check_constraints: only the owner's — keyless has no NOT NULL
	// column but its hidden rowid, which is not listed.
	want(rows("check_constraints", "constraint_name"))
	// table_constraints: parent through PUBLIC's INSERT (not alice's
	// SELECT); keyless has none (the rowid key is not one).
	want(rows("table_constraints", "table_name", "constraint_type"),
		"parent|PRIMARY KEY", "parent|UNIQUE", "parent|CHECK", "parent|CHECK", "parent|CHECK")
	want(rows("sequences", "sequence_name"), "s1")
	want(rows("enabled_roles", "role_name"), "alice", "staff")
	want(rows("applicable_roles", "grantee", "role_name", "is_grantable"), "alice|staff|YES")
	want(rows("view_table_usage", "view_name"))
	want(rows("views", "table_name", "view_definition"))
	want(rows("columns", "table_name", "column_name"), "keyless|a", "parent|id", "parent|code", "parent|tags", "parent|price")
	want(rows("element_types", "object_name", "data_type", "dtd_identifier"), "parent|text|a3")
}
