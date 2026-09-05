package catalog

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"

	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/kvclient"
)

// TypeDescriptor is a user-defined type: an enum with its labels in
// declaration order (ALTER TYPE ... ADD VALUE appends; a label's
// ordinal is stable for the life of the type). A column of the type
// carries the type's ID and a copy of the labels (Column.EnumType,
// Column.EnumLabels), which ADD VALUE refreshes on every such column,
// so writes and reads never look the type up.
type TypeDescriptor struct {
	ID         uint64   `json:"id"`
	Name       string   `json:"name"`
	DatabaseID uint64   `json:"database_id,omitempty"`
	Labels     []string `json:"labels"`
	// Owner is the owning role (v11; empty = root).
	Owner string `json:"owner,omitempty"`
}

// ErrTypeNotFound / ErrTypeExists are the catalog's name errors.
type ErrTypeNotFound struct{ Name string }

func (e *ErrTypeNotFound) Error() string { return fmt.Sprintf("type %q does not exist", e.Name) }

type ErrTypeExists struct{ Name string }

func (e *ErrTypeExists) Error() string { return fmt.Sprintf("type %q already exists", e.Name) }

// CreateType registers a type under its database (the name must be
// free of tables, sequences and types alike).
func CreateType(ctx context.Context, txn *kvclient.Txn, d *TypeDescriptor) error {
	if existing, err := txn.Get(ctx, keys.TypeNamespaceKey(d.DatabaseID, d.Name)); err != nil {
		return err
	} else if existing != nil {
		return &ErrTypeExists{Name: d.Name}
	}
	for _, k := range []keys.Key{keys.TableNamespaceKey(d.DatabaseID, d.Name), keys.SequenceNamespaceKey(d.DatabaseID, d.Name)} {
		if existing, err := txn.Get(ctx, k); err != nil {
			return err
		} else if existing != nil {
			return &ErrTypeExists{Name: d.Name}
		}
	}
	id, err := txn.Increment(ctx, keys.DescIDGenKey(), 1)
	if err != nil {
		return err
	}
	d.ID = uint64(id)
	if err := UpdateType(ctx, txn, d); err != nil {
		return err
	}
	return txn.Put(ctx, keys.TypeNamespaceKey(d.DatabaseID, d.Name), []byte(strconv.FormatUint(d.ID, 10)))
}

// UpdateType rewrites a type's descriptor (ALTER TYPE).
func UpdateType(ctx context.Context, txn *kvclient.Txn, d *TypeDescriptor) error {
	raw, err := json.Marshal(d)
	if err != nil {
		return err
	}
	return txn.Put(ctx, keys.TypeDescKey(d.ID), raw)
}

// LookupType resolves a type by name within a database.
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

// DropType removes a type's descriptor and name.
func DropType(ctx context.Context, txn *kvclient.Txn, d *TypeDescriptor) error {
	if err := txn.Delete(ctx, keys.TypeNamespaceKey(d.DatabaseID, d.Name)); err != nil {
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
