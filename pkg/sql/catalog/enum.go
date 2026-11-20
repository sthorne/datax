package catalog

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"

	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/kvclient"
	"github.com/sthorne/datax/pkg/util/encoding"
)

// TypeDescriptor is a user-defined type: an enum with its labels in
// declaration order (ALTER TYPE ... ADD VALUE appends; a label's
// ordinal is stable for the life of the type). A column of the type
// carries the type's ID and a copy of the labels (Column.EnumType,
// Column.EnumLabels), which ADD VALUE refreshes on every such column,
// so writes and reads never look the type up.
type TypeDescriptor struct {
	ID         uint64 `json:"id"`
	Name       string `json:"name"`
	DatabaseID uint64 `json:"database_id,omitempty"`
	// SchemaID is the schema the type is in (0 = public, gap PG-D-035),
	// filed as a sequence or table of the schema is.
	SchemaID uint64   `json:"schema_id,omitempty"`
	Labels   []string `json:"labels"`
	// Owner is the owning role (v11; empty = root).
	Owner string `json:"owner,omitempty"`
}

// ErrTypeNotFound / ErrTypeExists are the catalog's name errors.
type ErrTypeNotFound struct{ Name string }

func (e *ErrTypeNotFound) Error() string { return fmt.Sprintf("type %q does not exist", e.Name) }

type ErrTypeExists struct{ Name string }

func (e *ErrTypeExists) Error() string { return fmt.Sprintf("type %q already exists", e.Name) }

// NamespaceName is the type's name entry within its database.
func (d *TypeDescriptor) NamespaceName() string { return SchemaNamespaceName(d.SchemaID, d.Name) }

// typeNameTaken reports whether entry is taken in database dbID by a
// type, a table or a sequence (a relation has a row type of its name,
// in PostgreSQL).
func typeNameTaken(ctx context.Context, txn *kvclient.Txn, dbID uint64, entry string) (bool, error) {
	for _, k := range []keys.Key{keys.TypeNamespaceKey(dbID, entry), keys.TableNamespaceKey(dbID, entry), keys.SequenceNamespaceKey(dbID, entry)} {
		if existing, err := txn.Get(ctx, k); err != nil {
			return false, err
		} else if existing != nil {
			return true, nil
		}
	}
	return false, nil
}

// CreateType registers a type under its database and schema (the name
// must be free of tables, sequences and types alike in that schema).
func CreateType(ctx context.Context, txn *kvclient.Txn, d *TypeDescriptor) error {
	if err := checkObjectSchema(ctx, txn, d.DatabaseID, d.SchemaID); err != nil {
		return err
	}
	entry := d.NamespaceName()
	if taken, err := typeNameTaken(ctx, txn, d.DatabaseID, entry); err != nil {
		return err
	} else if taken {
		return &ErrTypeExists{Name: d.Name}
	}
	id, err := allocateDescID(ctx, txn)
	if err != nil {
		return err
	}
	d.ID = id
	if err := UpdateType(ctx, txn, d); err != nil {
		return err
	}
	return txn.Put(ctx, keys.TypeNamespaceKey(d.DatabaseID, entry), []byte(strconv.FormatUint(d.ID, 10)))
}

// UpdateType rewrites a type's descriptor (ALTER TYPE).
func UpdateType(ctx context.Context, txn *kvclient.Txn, d *TypeDescriptor) error {
	raw, err := json.Marshal(d)
	if err != nil {
		return err
	}
	return txn.Put(ctx, keys.TypeDescKey(d.ID), raw)
}

// LookupType resolves a type of the public schema of a database by its
// name entry.
func LookupType(ctx context.Context, txn *kvclient.Txn, dbID uint64, name string) (*TypeDescriptor, error) {
	raw, err := txn.Get(ctx, keys.TypeNamespaceKey(dbID, name))
	if err != nil {
		return nil, err
	}
	if raw == nil {
		return nil, &ErrTypeNotFound{Name: name}
	}
	id, err := strconv.ParseUint(string(raw), 10, 64)
	if err != nil {
		return nil, fmt.Errorf("corrupt type namespace entry for %q", name)
	}
	return ReadType(ctx, txn, id)
}

// ReadType reads a type descriptor by ID.
func ReadType(ctx context.Context, txn *kvclient.Txn, id uint64) (*TypeDescriptor, error) {
	raw, err := txn.Get(ctx, keys.TypeDescKey(id))
	if err != nil {
		return nil, err
	}
	if raw == nil {
		return nil, &ErrTypeNotFound{Name: strconv.FormatUint(id, 10)}
	}
	var d TypeDescriptor
	if err := json.Unmarshal(raw, &d); err != nil {
		return nil, fmt.Errorf("corrupt type descriptor %d: %w", id, err)
	}
	return &d, nil
}

// ListTypes lists a database's types (every database's when dbID is
// 0), by name.
func ListTypes(ctx context.Context, txn *kvclient.Txn, dbID uint64) ([]*TypeDescriptor, error) {
	lo, hi := keys.AllTypeNamespaceSpan()
	if dbID != 0 {
		lo, hi = keys.TypeNamespaceSpan(dbID)
	}
	rows, err := txn.Scan(ctx, lo, hi, 0)
	if err != nil {
		return nil, err
	}
	var out []*TypeDescriptor
	for _, kv := range rows {
		id, err := strconv.ParseUint(string(kv.Value), 10, 64)
		if err != nil {
			continue
		}
		d, err := ReadType(ctx, txn, id)
		if err != nil {
			return nil, err
		}
		out = append(out, d)
	}
	return out, nil
}

// LookupTypeIn resolves a type name as written ("x", "s.x", "db.s.x",
// or a TypeRelationName) in database db, by the rules tables and
// sequences follow (resolve).
func (a *Accessor) LookupTypeIn(ctx context.Context, txn *kvclient.Txn, db, name string) (*TypeDescriptor, error) {
	if IsReservedTableName(name) && !IsRelationName(name) {
		return nil, &ErrTypeNotFound{Name: name}
	}
	cands, err := a.resolve(ctx, txn, db, name)
	if err != nil {
		return nil, err
	}
	for _, c := range cands {
		raw, err := txn.Get(ctx, keys.TypeNamespaceKey(c.dbID, c.entry))
		if err != nil {
			return nil, err
		}
		if raw == nil {
			continue
		}
		id, err := strconv.ParseUint(string(raw), 10, 64)
		if err != nil {
			return nil, fmt.Errorf("corrupt type namespace entry for %q", c.entry)
		}
		return ReadType(ctx, txn, id)
	}
	return nil, &ErrTypeNotFound{Name: displayName(name)}
}

// TypeRelationName is a name that resolves to exactly type d whatever
// the search path (RelationName's form, with the type's name entry).
func TypeRelationName(d *TypeDescriptor) string {
	return relMarker + strconv.FormatUint(d.DatabaseID, 10) + "\x00" + d.NamespaceName()
}

// SetTypeSchema moves type d into schema schemaID of its database: its
// name entry moves; its ID, which every column of the type holds,
// stays.
func SetTypeSchema(ctx context.Context, txn *kvclient.Txn, d *TypeDescriptor, schemaID uint64) error {
	if err := checkObjectSchema(ctx, txn, d.DatabaseID, schemaID); err != nil {
		return err
	}
	newEntry := SchemaNamespaceName(schemaID, d.Name)
	if taken, err := typeNameTaken(ctx, txn, d.DatabaseID, newEntry); err != nil {
		return err
	} else if taken {
		return &ErrTypeExists{Name: d.Name}
	}
	if err := txn.Delete(ctx, keys.TypeNamespaceKey(d.DatabaseID, d.NamespaceName())); err != nil {
		return err
	}
	if err := txn.Put(ctx, keys.TypeNamespaceKey(d.DatabaseID, newEntry), []byte(strconv.FormatUint(d.ID, 10))); err != nil {
		return err
	}
	d.SchemaID = schemaID
	return UpdateType(ctx, txn, d)
}

// DropType removes a type's descriptor and name.
func DropType(ctx context.Context, txn *kvclient.Txn, d *TypeDescriptor) error {
	if err := txn.Delete(ctx, keys.TypeNamespaceKey(d.DatabaseID, d.NamespaceName())); err != nil {
		return err
	}
	return txn.Delete(ctx, keys.TypeDescKey(d.ID))
}

// EnumOID is the pg_type OID of a user-defined type: past PostgreSQL's
// builtin range, derived from the ID so it is stable across nodes.
func EnumOID(typeID uint64) int64 { return 16384 + int64(typeID) }

// EnumTypeID is EnumOID's inverse (ok when oid is in the range).
func EnumTypeID(oid int64) (uint64, bool) {
	if oid < 16384 {
		return 0, false
	}
	return uint64(oid - 16384), true
}

// TypeIDOfKey extracts the type id from a type descriptor key
// (keys.TypeDescKey); ok is false for any other key.
func TypeIDOfKey(k keys.Key) (uint64, bool) {
	lo, _ := keys.TypeDescSpan()
	if len(k) <= len(lo) || string(k[:len(lo)]) != string(lo) {
		return 0, false
	}
	_, id, err := encoding.DecodeUint64(k[len(lo):])
	if err != nil {
		return 0, false
	}
	return id, true
}
