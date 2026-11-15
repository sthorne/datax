package sql

import (
	"context"
	"errors"

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
)

// LookupKeysTable resolves the named-keys table's descriptor, or nil
// when it has not been created, in a transaction of its own.
func LookupKeysTable(ctx context.Context, db *kvclient.DB, cat *catalog.Accessor) (*catalog.TableDescriptor, error) {
	var d *catalog.TableDescriptor
	err := db.RunTxn(ctx, "keys-table", func(ctx context.Context, txn *kvclient.Txn) error {
		d = nil
		got, err := cat.LookupIn(ctx, txn, catalog.DefaultDatabase, catalog.KeysTableName)
		if err != nil {
			var nf *catalog.ErrTableNotFound
			if errors.As(err, &nf) {
				return nil
			}
			return err
		}
		d = got
		return nil
	})
	return d, err
}

// EnsureKeysTable creates the named-keys table if it is not there and
// returns its descriptor. Refused before v22 (rule 4): a v21 node would
// treat the table as an ordinary one.
func EnsureKeysTable(ctx context.Context, db *kvclient.DB, cat *catalog.Accessor) (*catalog.TableDescriptor, error) {
	if db.ClusterVersion() < version.V22 {
		return nil, newErrf(CodeFeatureNotSupported, "named keys need cluster version %s: finalize the upgrade with `datax debug upgrade` first", version.V22)
	}
	d, err := LookupKeysTable(ctx, db, cat)
	if err != nil || d != nil {
		return d, err
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
	d, err := LookupKeysTable(ctx, s.db, s.cat)
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
		out = append(out, k)
	}
	return out, nil
}

// keysTableError is the refusal every attempt to read or change the
// named-keys table gets, whoever makes it.
func keysTableError(name string) *Error {
	return newErrf(CodeInsufficientPriv, "table %q holds the cluster's named keys: it is written and read by the nodes alone, by no SQL session of any role (SELECT * FROM %s shows the keys' metadata)", name, name)
}
