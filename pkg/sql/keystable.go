package sql

import (
	"context"
	"encoding/json"
	"errors"
	"sort"
	"strings"

	"github.com/sthorne/datax/pkg/kvclient"
	"github.com/sthorne/datax/pkg/sql/catalog"
	"github.com/sthorne/datax/pkg/sql/parser"
	"github.com/sthorne/datax/pkg/sql/rowenc"
	"github.com/sthorne/datax/pkg/sql/vtable"
	"github.com/sthorne/datax/pkg/version"
)

// The named keys' table (issue #310; pkg/keystore is what reads and
// writes it). One row per key: its metadata in plain columns, and in
// material every version of the key wrapped by the cluster key. The
// rows are written by the nodes alone (the table is sealed) and read by
// them alone: no SQL session, root's included, may read the table. A
// session that names it is given the datax_keys virtual table instead
// (pkg/sql/vtable), which renders the plain columns and has no column
// for the material.
//
// The table is an ordinary table below SQL — its rows are KV values in
// the table key space — so it rides in every backup as it is stored
// (wrapped), a whole-cluster restore brings it back, and a row's history
// can be purged like any table's.

// KeysTableDDL creates the named-keys table. IF NOT EXISTS makes the
// nodes' concurrent attempts idempotent.
const KeysTableDDL = `CREATE TABLE IF NOT EXISTS ` + catalog.KeysTableName + ` (
  id INT8 NOT NULL,
  name TEXT NOT NULL,
  kind TEXT NOT NULL,
  version INT8 NOT NULL,
  versions INT8 NOT NULL,
  state TEXT NOT NULL,
  generation INT8 NOT NULL,
  cluster_key TEXT,
  material BYTEA,
  created TIMESTAMPTZ NOT NULL,
  rotated TIMESTAMPTZ,
  external_root TEXT,
  owner TEXT,
  privileges TEXT,
  PRIMARY KEY (id),
  UNIQUE (name)
)`

// Column IDs of the named-keys table (the position in KeysTableDDL).
const (
	KeysColID           catalog.ColumnID = 1
	KeysColName         catalog.ColumnID = 2
	KeysColKind         catalog.ColumnID = 3
	KeysColVersion      catalog.ColumnID = 4
	KeysColVersions     catalog.ColumnID = 5
	KeysColState        catalog.ColumnID = 6
	KeysColGeneration   catalog.ColumnID = 7
	KeysColClusterKey   catalog.ColumnID = 8
	KeysColMaterial     catalog.ColumnID = 9
	KeysColCreated      catalog.ColumnID = 10
	KeysColRotated      catalog.ColumnID = 11
	KeysColExternalRoot catalog.ColumnID = 12
	// KeysColOwner and KeysColPrivileges are the key service's ownership
	// and grants (#310 step 2): the owning role (empty: root), and a JSON
	// object of grantee → privileges (WRAP, UNWRAP).
	KeysColOwner      catalog.ColumnID = 13
	KeysColPrivileges catalog.ColumnID = 14
)

// keysTableAddedColumns are the columns added to the table after it was
// first defined, with their DDL: a table made before them is given them
// by EnsureKeysTable.
var keysTableAddedColumns = []struct{ name, ddl string }{
	{"owner", "owner TEXT"},
	{"privileges", "privileges TEXT"},
}

// LookupKeysTable resolves the named-keys table's descriptor, or nil
// when it has not been created, in a transaction of its own.
func LookupKeysTable(ctx context.Context, db *kvclient.DB, cat *catalog.Accessor) (*catalog.TableDescriptor, error) {
	var d *catalog.TableDescriptor
	err := db.RunTxn(ctx, "keys-table", func(ctx context.Context, txn *kvclient.Txn) error {
		var err error
		d, err = LookupKeysTableIn(ctx, txn, cat)
		return err
	})
	return d, err
}

// LookupKeysTableIn is LookupKeysTable in txn. Work that is part of a
// statement's transaction must look the table up there: the lookup
// reads the default database's descriptor, and a transaction of its own
// would wait on the statement's write to it (DROP OWNED and DROP ROLE
// write it when the role holds default privileges there) while the
// statement waits on the lookup.
func LookupKeysTableIn(ctx context.Context, txn *kvclient.Txn, cat *catalog.Accessor) (*catalog.TableDescriptor, error) {
	d, err := cat.LookupIn(ctx, txn, catalog.DefaultDatabase, catalog.KeysTableName)
	if err != nil {
		var nf *catalog.ErrTableNotFound
		if errors.As(err, &nf) {
			return nil, nil
		}
		return nil, err
	}
	return d, nil
}

// ReadKeysTableIn is LookupKeysTableIn for the key service's work inside
// a caller's transaction: it reads the committed descriptor in txn and
// takes no lease. A leased lookup pins txn's commit deadline to the
// lease's expiry, and the key service's transactions can outlast one —
// a statement's first writes may make a tenant key per row, and the
// listings of DROP OWNED and DROP ROLE read every key — which then
// could never commit. Read in txn, a change to the table's descriptor
// orders against txn as any other write to what it read does.
func ReadKeysTableIn(ctx context.Context, txn *kvclient.Txn, cat *catalog.Accessor) (*catalog.TableDescriptor, error) {
	d, err := cat.LookupFreshIn(ctx, txn, catalog.DefaultDatabase, catalog.KeysTableName)
	if err != nil {
		var nf *catalog.ErrTableNotFound
		if errors.As(err, &nf) {
			return nil, nil
		}
		return nil, err
	}
	return d, nil
}

// EnsureKeysTable creates the named-keys table if it is not there and
// returns its descriptor. Refused before v22 (rule 4): a v21 node would
// treat the table as an ordinary one.
func EnsureKeysTable(ctx context.Context, db *kvclient.DB, cat *catalog.Accessor) (*catalog.TableDescriptor, error) {
	if db.ClusterVersion() < version.V22 {
		return nil, newErrf(CodeFeatureNotSupported, "named keys need cluster version %s: finalize the upgrade with `datax debug upgrade` first", version.V22)
	}
	d, err := LookupKeysTable(ctx, db, cat)
	if err != nil {
		return nil, err
	}
	if d != nil {
		return upgradeKeysTable(ctx, db, cat, d)
	}
	stmts, err := parser.Parse(KeysTableDDL)
	if err != nil {
		return nil, err
	}
	if _, serr := NewSystemSession(db, cat).Execute(ctx, stmts[0], nil); serr != nil {
		return nil, serr
	}
	if d, err = LookupKeysTable(ctx, db, cat); err != nil {
		return nil, err
	}
	if d == nil {
		return nil, newErrf(CodeInternal, "the %s table could not be created", catalog.KeysTableName)
	}
	return d, nil
}

// upgradeKeysTable adds to a table made before them the columns added
// since (keysTableAddedColumns), so that every row written can carry
// them. Idempotent: ADD COLUMN IF NOT EXISTS.
func upgradeKeysTable(ctx context.Context, db *kvclient.DB, cat *catalog.Accessor, d *catalog.TableDescriptor) (*catalog.TableDescriptor, error) {
	added := false
	for _, c := range keysTableAddedColumns {
		if _, ok := d.Col(c.name); ok {
			continue
		}
		stmts, err := parser.Parse("ALTER TABLE " + catalog.KeysTableName + " ADD COLUMN IF NOT EXISTS " + c.ddl)
		if err != nil {
			return nil, err
		}
		if _, serr := NewSystemSession(db, cat).Execute(ctx, stmts[0], nil); serr != nil {
			return nil, serr
		}
		added = true
	}
	if !added {
		return d, nil
	}
	cat.Invalidate(catalog.KeysTableName)
	return LookupKeysTable(ctx, db, cat)
}

// keysVirtualTable is what a session that names the named-keys table is
// given: the virtual table of its metadata.
func keysVirtualTable() (*catalog.TableDescriptor, bool) {
	vt, ok := vtable.Lookup(vtable.PgCatalog + "." + catalog.KeysTableName)
	if !ok {
		return nil, false
	}
	return vt.Descriptor(), true
}

// listKeys renders the named-keys table's plain columns for the virtual
// table. The material column is never decoded into what it returns.
func (s *Session) listKeys(ctx context.Context, txn *kvclient.Txn, set catalog.RoleSet) ([]vtable.KeyInfo, error) {
	if s.db.ClusterVersion() < version.V22 || !(s.actor() == catalog.RootRole || set.IsAdmin()) {
		return nil, nil
	}
	d, err := LookupKeysTableIn(ctx, txn, s.cat)
	if err != nil || d == nil {
		return nil, err
	}
	lo, hi := rowenc.PrimarySpanFor(d)
	kvs, err := txn.Scan(ctx, lo, hi, 0)
	if err != nil {
		return nil, err
	}
	out := make([]vtable.KeyInfo, 0, len(kvs))
	for _, kv := range kvs {
		pk, err := rowenc.DecodePK(d, kv.Key)
		if err != nil {
			return nil, err
		}
		row, err := rowenc.DecodeValue(d, kv.Value)
		if err != nil {
			return nil, err
		}
		k := vtable.KeyInfo{
			ID:           pk[0].I,
			Name:         row[KeysColName].S,
			Kind:         row[KeysColKind].S,
			Version:      row[KeysColVersion].I,
			Versions:     row[KeysColVersions].I,
			State:        row[KeysColState].S,
			Generation:   row[KeysColGeneration].I,
			ClusterKey:   row[KeysColClusterKey].S,
			Created:      row[KeysColCreated].I,
			Rotated:      row[KeysColRotated].I,
			ExternalRoot: row[KeysColExternalRoot].S,
		}
		if kind := row[KeysColKind].S; kind == "wrapping" {
			k.Owner = catalog.OwnerOf(row[KeysColOwner].S)
		}
		if raw := row[KeysColPrivileges].S; raw != "" {
			var m map[string][]string
			if err := json.Unmarshal([]byte(raw), &m); err == nil {
				k.Privileges = keyACLText(m)
			}
		}
		out = append(out, k)
	}
	return out, nil
}

// keyACLText renders a key's privileges as grantee=PRIV,PRIV;... sorted.
func keyACLText(m map[string][]string) string {
	names := make([]string, 0, len(m))
	for g := range m {
		names = append(names, g)
	}
	sort.Strings(names)
	parts := make([]string, 0, len(names))
	for _, g := range names {
		parts = append(parts, g+"="+strings.Join(m[g], ","))
	}
	return strings.Join(parts, ";")
}

// keysTableError is the refusal every attempt to read or change the
// named-keys table gets, whoever makes it.
func keysTableError(name string) *Error {
	return newErrf(CodeInsufficientPriv, "table %q holds the cluster's named keys: it is written and read by the nodes alone, by no SQL session of any role (SELECT * FROM %s shows the keys' metadata)", name, name)
}
