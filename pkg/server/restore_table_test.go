package server

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"

	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/sql/catalog"
	"github.com/sthorne/datax/pkg/sql/types"
)

// planManifest is a manifest of two databases: shop (id 10) with orders
// (60, a foreign key to customers), customers (61, a SERIAL id) and a
// view; and other (id 11) with orders (62).
func planManifest(t *testing.T) *backupManifest {
	t.Helper()
	kv := func(key keys.Key, v any) backupKV {
		raw, err := json.Marshal(v)
		if err != nil {
			t.Fatal(err)
		}
		return backupKV{Key: key, Value: raw}
	}
	man := &backupManifest{Magic: backupManifestMagic, ClusterID: "c", EndTS: hts(100)}
	for _, d := range []catalog.DatabaseDescriptor{{ID: 10, Name: "shop", Owner: "root"}, {ID: 11, Name: "other", Owner: "root"}} {
		man.Databases = append(man.Databases, kv(keys.DatabaseDescKey(d.ID), d),
			backupKV{Key: keys.DatabaseNamespaceKey(d.Name), Value: []byte("10")})
	}
	seq := catalog.SequenceDescriptor{ID: 70, Name: "customers_id_seq", DatabaseID: 10, Start: 1, Increment: 1, OwnerTable: 61, OwnerColumn: 1}
	man.Sequences = append(man.Sequences, kv(keys.SequenceDescKey(70), seq),
		backupKV{Key: keys.SequenceValueKey(70), Value: []byte("41")})
	cols := []catalog.Column{{ID: 1, Name: "id"}, {ID: 2, Name: "customer"}}
	orders := catalog.TableDescriptor{ID: 60, Name: "orders", DatabaseID: 10, Columns: cols, PrimaryKey: []catalog.ColumnID{1},
		Owner: "ann", Privileges: map[string][]string{"bob": {"SELECT"}, "gone": {"SELECT"}, catalog.PublicRole: {"SELECT"}},
		Indexes: []catalog.IndexDescriptor{{ID: 2, Name: "by_customer", ColumnIDs: []catalog.ColumnID{2}}},
		Constraints: []catalog.Constraint{
			{ID: 1, Name: "orders_customer_fk", Kind: catalog.ConstraintForeign, Columns: []catalog.ColumnID{2}, RefTable: 61, RefColumns: []catalog.ColumnID{1}, Validated: true},
			{ID: 2, Name: "positive", Kind: catalog.ConstraintCheck, Expr: "id > 0", Validated: true},
		}}
	customers := catalog.TableDescriptor{ID: 61, Name: "customers", DatabaseID: 10, PrimaryKey: []catalog.ColumnID{1},
		Columns:    []catalog.Column{{ID: 1, Name: "id", SequenceID: 70, DefaultExpr: "nextval('customers_id_seq')"}},
		InboundFKs: []catalog.ForeignKeyRef{{TableID: 60, ConstraintID: 1}, {TableID: 99, ConstraintID: 4}}}
	view := catalog.TableDescriptor{ID: 63, Name: "big_orders", DatabaseID: 10, ViewQuery: "SELECT * FROM orders", ViewDepends: []uint64{60}}
	other := catalog.TableDescriptor{ID: 62, Name: "orders", DatabaseID: 11, Columns: cols, PrimaryKey: []catalog.ColumnID{1}}
	for _, d := range []catalog.TableDescriptor{orders, customers, other, view} {
		man.Tables = append(man.Tables, backupTable{ID: d.ID, Name: d.Name, Descriptor: testTableDesc(t, d), File: backupDataFile(d.ID)})
	}
	return man
}

// TestPlanTableRestore: which tables a restore brings back and under
// what names, and what it refuses from the backup alone.
func TestPlanTableRestore(t *testing.T) {
	man := planManifest(t)
	tables := func(names ...string) []RestoreTable {
		var out []RestoreTable
		for _, n := range names {
			name, into, _ := strings.Cut(n, ">")
			out = append(out, RestoreTable{Name: name, Into: into})
		}
		return out
	}
	for _, c := range []struct {
		what string
		opts RestoreOptions
		want string
	}{
		{"a table the backup lacks", RestoreOptions{Tables: tables("shop.nope")}, "the backup has no table shop.nope (it has: other.orders, shop.big_orders, shop.customers, shop.orders)"},
		{"the default database when none is named", RestoreOptions{Tables: tables("orders")}, "the backup has no table " + catalog.DefaultDatabase + ".orders"},
		{"a foreign key to a table left behind", RestoreOptions{Tables: tables("shop.orders")}, "foreign key \"orders_customer_fk\" to a table that is not being restored with it"},
		{"a view", RestoreOptions{Tables: tables("shop.big_orders")}, "is a view"},
		{"two tables under one name", RestoreOptions{Tables: tables("shop.customers>x", "other.orders>shop.x")}, "two tables would be restored as shop.x"},
		{"tables and a database", RestoreOptions{Tables: tables("shop.customers"), Database: "shop"}, "two restores"},
		{"a database the backup lacks", RestoreOptions{Database: "nope"}, "no database \"nope\""},
		{"into the system database", RestoreOptions{Tables: tables("shop.customers>system.c")}, "belongs to the cluster"},
	} {
		_, err := planTableRestore(man, c.opts)
		if err == nil || !strings.Contains(err.Error(), c.want) {
			t.Errorf("%s: got %v, want a refusal saying %q", c.what, err, c.want)
		}
	}

	// A table restored with the table it references keeps the foreign
	// key, pointed at the new id; alone and told to, it loses it.
	plan, err := planTableRestore(man, RestoreOptions{Tables: tables("shop.orders>orders_r", "shop.customers>customers_r")})
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.tables) != 2 || plan.tables[0].into() != "shop.orders_r" || plan.tables[1].into() != "shop.customers_r" || len(plan.notes) != 0 {
		t.Fatalf("plan: %+v notes %v", plan.tables, plan.notes)
	}
	plan.tables[0].newID, plan.tables[1].newID = 160, 161
	ids := map[uint64]uint64{60: 160, 61: 161}
	roles := func(name string) bool { return name == "bob" || name == "root" }
	od, notes := restoredDescriptor(plan.tables[0], 10, ids, nil, nil, roles)
	if od.ID != 160 || od.Name != "orders_r" || od.Version != 1 || len(od.Constraints) != 2 || od.Constraints[0].RefTable != 161 {
		t.Fatalf("orders restored as %+v", od)
	}
	if od.Indexes[0].ID != 2 {
		t.Fatalf("the index id moved: %+v", od.Indexes)
	}
	// Grants only for roles the cluster has; the owner falls to root.
	if od.Owner != catalog.RootRole || !reflect.DeepEqual(od.Privileges, map[string][]string{"bob": {"SELECT"}, catalog.PublicRole: {"SELECT"}}) {
		t.Fatalf("orders' owner %q, grants %v", od.Owner, od.Privileges)
	}
	if len(notes) != 2 || !strings.Contains(notes[0], `grants to "gone" not restored`) || !strings.Contains(notes[1], `owner "ann" does not exist here`) {
		t.Fatalf("notes: %q", notes)
	}
	// The source plan is left as the backup had it (a retried
	// transaction builds the descriptor again from it).
	if len(plan.tables[0].srcDesc.Privileges) != 3 || plan.tables[0].srcDesc.Constraints[0].RefTable != 61 {
		t.Fatalf("building a descriptor changed the plan's source: %+v", plan.tables[0].srcDesc)
	}
	// customers: its sequence follows it under the new table's name, and
	// only the restored orders' foreign key still points at it.
	cp := plan.tables[1]
	if len(cp.seqs) != 1 || string(cp.seqs[0].value) != "41" || restoredSequenceName(cp, cp.seqs[0]) != "customers_r_id_seq" {
		t.Fatalf("customers' sequences: %+v", cp.seqs)
	}
	cd, _ := restoredDescriptor(cp, 10, ids, map[uint64]uint64{70: 170}, map[uint64]string{70: "customers_r_id_seq"}, func(string) bool { return true })
	if cd.Columns[0].SequenceID != 170 || cd.Columns[0].DefaultExpr != "nextval('customers_r_id_seq')" {
		t.Fatalf("customers' id column: %+v", cd.Columns[0])
	}
	if !reflect.DeepEqual(cd.InboundFKs, []catalog.ForeignKeyRef{{TableID: 160, ConstraintID: 1}}) {
		t.Fatalf("customers' inbound foreign keys: %+v", cd.InboundFKs)
	}

	plan, err = planTableRestore(man, RestoreOptions{Tables: tables("shop.orders>orders_r"), DropForeignKeys: true})
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.notes) != 1 || !strings.Contains(plan.notes[0], `foreign key "orders_customer_fk" dropped`) {
		t.Fatalf("notes: %q", plan.notes)
	}
	plan.tables[0].newID = 160
	od, _ = restoredDescriptor(plan.tables[0], 10, map[uint64]uint64{60: 160}, nil, nil, func(string) bool { return true })
	if len(od.Constraints) != 1 || od.Constraints[0].Name != "positive" || od.Owner != "ann" {
		t.Fatalf("orders without its foreign key: %+v (owner %q)", od.Constraints, od.Owner)
	}

	// A database: its tables under their own names in a database of the
	// new name, the view left out with its text in a note.
	plan, err = planTableRestore(man, RestoreOptions{Database: "shop", IntoDatabase: "shop_r"})
	if err != nil {
		t.Fatal(err)
	}
	if plan.newDB == nil || plan.newDB.Name != "shop_r" || plan.newDB.ID != 0 || len(plan.tables) != 2 ||
		plan.tables[0].into() != "shop_r.orders" || plan.tables[1].into() != "shop_r.customers" {
		t.Fatalf("database plan: %+v %+v", plan.newDB, plan.tables)
	}
	if len(plan.notes) != 1 || !strings.Contains(plan.notes[0], "view shop.big_orders was not restored") || !strings.Contains(plan.notes[0], "SELECT * FROM orders") {
		t.Fatalf("notes: %q", plan.notes)
	}
}

// TestTableRestoreRefusesFieldsItDoesNotKnow: a table whose descriptor
// carries a field table restore was not written against — a feature
// added to tables since — is refused by the field's name rather than
// restored without it. The lists are what the code was written against;
// a descriptor field that is on none of them is logged here, as the
// reminder that tables using it are refused until table restore is
// taught what bringing it back under a new table id takes.
func TestTableRestoreRefusesFieldsItDoesNotKnow(t *testing.T) {
	for _, c := range []struct {
		typ   reflect.Type
		known map[string]bool
	}{
		{reflect.TypeOf(catalog.TableDescriptor{}), restoreTableFields},
		{reflect.TypeOf(catalog.Column{}), restoreColumnFields},
		{reflect.TypeOf(catalog.IndexDescriptor{}), restoreIndexFields},
		{reflect.TypeOf(catalog.Constraint{}), restoreConstraintFields},
	} {
		for i := 0; i < c.typ.NumField(); i++ {
			tag := strings.Split(c.typ.Field(i).Tag.Get("json"), ",")[0]
			if tag == "-" {
				continue
			}
			if !c.known[tag] {
				t.Logf("%s.%s (%q) is not a field table restore knows: tables that use it are refused by a table restore until it is taught the field", c.typ.Name(), c.typ.Field(i).Name, tag)
			}
		}
	}
	man := planManifest(t)
	for _, c := range []struct{ old, new, want string }{
		{`"name":"customers"`, `"name":"customers","partition_by":{"column":"at"}`, `the descriptor field "partition_by"`},
		{`"name":"id"`, `"name":"id","generated":"x"`, `the column field "generated"`},
	} {
		m := *man
		m.Tables = append([]backupTable(nil), man.Tables...)
		raw := strings.Replace(string(m.Tables[1].Descriptor), c.old, c.new, 1)
		if raw == string(m.Tables[1].Descriptor) {
			t.Fatalf("the test's edit %q did not apply", c.old)
		}
		m.Tables[1].Descriptor = json.RawMessage(raw)
		_, err := planTableRestore(&m, RestoreOptions{Tables: []RestoreTable{{Name: "shop.customers", Into: "c2"}}})
		if err == nil || !strings.Contains(err.Error(), c.want) || !strings.Contains(err.Error(), "restore the whole backup into an empty cluster") {
			t.Errorf("got %v, want a refusal naming %s", err, c.want)
		}
	}
	// An index field, on the table that has an index.
	m := *man
	m.Tables = append([]backupTable(nil), man.Tables...)
	m.Tables[0].Descriptor = json.RawMessage(strings.Replace(string(m.Tables[0].Descriptor), `"name":"by_customer"`, `"name":"by_customer","storage_params":{"fillfactor":"70"}`, 1))
	_, err := planTableRestore(&m, RestoreOptions{Tables: []RestoreTable{{Name: "shop.orders"}}, DropForeignKeys: true})
	if err == nil || !strings.Contains(err.Error(), `the index field "storage_params"`) {
		t.Errorf("got %v, want a refusal naming the index field", err)
	}
}

// TestRestoredDescriptorKeepsRowSecurity (issue #294): a restored table
// comes back as protected as it was. Row security stays enabled and
// forced; a policy's roles are cut to the ones this cluster has, and a
// policy left with none is dropped rather than kept with an empty list
// (which would mean every role) — so a role sees no more of the
// restored table than of the original, and possibly less.
func TestRestoredDescriptorKeepsRowSecurity(t *testing.T) {
	src := &catalog.TableDescriptor{
		ID: 60, Name: "docs", DatabaseID: 10, Owner: "root",
		Columns:     []catalog.Column{{ID: 1, Name: "tenant", Type: types.String, NotNull: true}},
		PrimaryKey:  []catalog.ColumnID{1},
		RowSecurity: true, ForceRowSecurity: true,
		Policies: []catalog.Policy{
			{Name: "everyone", Command: catalog.PolicySelect, Using: "tenant = current_user"},
			{Name: "mixed", Command: catalog.PolicyAll, Roles: []string{"bob", "gone"}, Using: "true"},
			{Name: "orphan", Command: catalog.PolicyAll, Roles: []string{"gone"}, Using: "true"},
			{Name: "narrow", Restrictive: true, Command: catalog.PolicySelect, Roles: []string{"bob"}, Using: "tenant != 'x'"},
		},
	}
	p := &tableRestorePlan{srcDesc: src, srcDB: "shop", intoDB: "shop", intoName: "docs_r", newID: 160}
	roles := func(name string) bool { return name == "bob" || name == "root" }
	d, notes := restoredDescriptor(p, 10, map[uint64]uint64{60: 160}, nil, nil, roles)
	if !d.RowSecurity || !d.ForceRowSecurity {
		t.Fatalf("row security lost: %+v", d)
	}
	var names []string
	for _, pol := range d.Policies {
		names = append(names, pol.Name)
	}
	if !reflect.DeepEqual(names, []string{"everyone", "mixed", "narrow"}) {
		t.Fatalf("policies restored: %v", names)
	}
	if mixed, _ := d.Policy("mixed"); !reflect.DeepEqual(mixed.Roles, []string{"bob"}) {
		t.Fatalf("mixed restored for %v", mixed.Roles)
	}
	if every, _ := d.Policy("everyone"); every.Roles != nil || every.Using != "tenant = current_user" {
		t.Fatalf("a PUBLIC policy changed: %+v", every)
	}
	joined := strings.Join(notes, "\n")
	if !strings.Contains(joined, `policy "orphan" not restored`) || !strings.Contains(joined, `policy "mixed" restored for the roles that exist here (bob)`) {
		t.Fatalf("notes: %q", notes)
	}
	// The plan's source is untouched (a retried transaction rebuilds from it).
	if len(src.Policies) != 4 || len(src.Policies[1].Roles) != 2 {
		t.Fatalf("building the descriptor changed the source: %+v", src.Policies)
	}
}
