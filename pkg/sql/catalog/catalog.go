// Package catalog manages SQL table descriptors, stored as JSON in the
// system keyspace (range 1) and manipulated transactionally.
package catalog

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"sync"

	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/kvclient"
	"github.com/sthorne/datax/pkg/sql/types"
)

// ColumnID identifies a column within a table (stable across renames —
// though v1 has none).
type ColumnID int32

// Column is one column of a table.
type Column struct {
	ID      ColumnID     `json:"id"`
	Name    string       `json:"name"`
	Type    types.Family `json:"type"`
	NotNull bool         `json:"not_null,omitempty"`
}

// TableDescriptor describes a table. Rows are stored at
// /t/<ID>/<encoded primary key> (see pkg/sql/rowenc).
type TableDescriptor struct {
	ID         uint64     `json:"id"`
	Name       string     `json:"name"`
	Columns    []Column   `json:"columns"`
	PrimaryKey []ColumnID `json:"primary_key"`
}

// Col returns the column with the given name.
func (d *TableDescriptor) Col(name string) (Column, bool) {
	for _, c := range d.Columns {
		if c.Name == name {
			return c, true
		}
	}
	return Column{}, false
}

// ColByID returns the column with the given ID.
func (d *TableDescriptor) ColByID(id ColumnID) (Column, bool) {
	for _, c := range d.Columns {
		if c.ID == id {
			return c, true
		}
	}
	return Column{}, false
}

// IsPKCol reports whether the column is part of the primary key.
func (d *TableDescriptor) IsPKCol(id ColumnID) bool {
	for _, pk := range d.PrimaryKey {
		if pk == id {
			return true
		}
	}
	return false
}

// ErrTableNotFound is returned (wrapped) for missing tables.
type ErrTableNotFound struct{ Name string }

func (e *ErrTableNotFound) Error() string { return fmt.Sprintf("table %q does not exist", e.Name) }

// ErrTableExists is returned for duplicate CREATE TABLE.
type ErrTableExists struct{ Name string }

func (e *ErrTableExists) Error() string { return fmt.Sprintf("table %q already exists", e.Name) }

// Accessor reads and writes descriptors through transactions, with a
// per-gateway cache (invalidated on miss and on DDL; no leases in v1 —
// concurrent DDL from other gateways is best-effort, see docs/sql.md).
type Accessor struct {
	mu    sync.Mutex
	cache map[string]*TableDescriptor
}

func NewAccessor() *Accessor {
	return &Accessor{cache: make(map[string]*TableDescriptor)}
}

// Lookup resolves a table by name within txn, using the cache first.
func (a *Accessor) Lookup(ctx context.Context, txn *kvclient.Txn, name string) (*TableDescriptor, error) {
	a.mu.Lock()
	if d, ok := a.cache[name]; ok {
		a.mu.Unlock()
		return d, nil
	}
	a.mu.Unlock()
	d, err := lookupUncached(ctx, txn, name)
	if err != nil {
		return nil, err
	}
	a.mu.Lock()
	a.cache[name] = d
	a.mu.Unlock()
	return d, nil
}

// Invalidate drops a cached entry (after DDL or a stale-descriptor error).
func (a *Accessor) Invalidate(name string) {
	a.mu.Lock()
	delete(a.cache, name)
	a.mu.Unlock()
}

func lookupUncached(ctx context.Context, txn *kvclient.Txn, name string) (*TableDescriptor, error) {
	idRaw, err := txn.Get(ctx, keys.NamespaceKey(name))
	if err != nil {
		return nil, err
	}
	if idRaw == nil {
		return nil, &ErrTableNotFound{Name: name}
	}
	id, err := strconv.ParseUint(string(idRaw), 10, 64)
	if err != nil {
		return nil, fmt.Errorf("corrupt namespace entry for %q", name)
	}
	raw, err := txn.Get(ctx, keys.TableDescKey(id))
	if err != nil {
		return nil, err
	}
	if raw == nil {
		return nil, fmt.Errorf("namespace entry for %q points at missing descriptor %d", name, id)
	}
	var d TableDescriptor
	if err := json.Unmarshal(raw, &d); err != nil {
		return nil, fmt.Errorf("corrupt descriptor %d: %w", id, err)
	}
	return &d, nil
}

// Create writes a new table descriptor within txn. The caller has validated
// the definition.
func (a *Accessor) Create(ctx context.Context, txn *kvclient.Txn, d *TableDescriptor) error {
	existing, err := txn.Get(ctx, keys.NamespaceKey(d.Name))
	if err != nil {
		return err
	}
	if existing != nil {
		return &ErrTableExists{Name: d.Name}
	}
	id, err := txn.Increment(ctx, keys.DescIDGenKey(), 1)
	if err != nil {
		return err
	}
	d.ID = uint64(id)
	raw, err := json.Marshal(d)
	if err != nil {
		return err
	}
	if err := txn.Put(ctx, keys.TableDescKey(d.ID), raw); err != nil {
		return err
	}
	if err := txn.Put(ctx, keys.NamespaceKey(d.Name), []byte(strconv.FormatUint(d.ID, 10))); err != nil {
		return err
	}
	a.Invalidate(d.Name)
	return nil
}

// Drop removes a table's descriptor and namespace entry. Row data is left
// behind (unreachable; space reclamation is a GC concern, out of scope).
func (a *Accessor) Drop(ctx context.Context, txn *kvclient.Txn, name string) (*TableDescriptor, error) {
	d, err := lookupUncached(ctx, txn, name)
	if err != nil {
		return nil, err
	}
	if err := txn.Delete(ctx, keys.NamespaceKey(name)); err != nil {
		return nil, err
	}
	if err := txn.Delete(ctx, keys.TableDescKey(d.ID)); err != nil {
		return nil, err
	}
	a.Invalidate(name)
	return d, nil
}

// List returns all table descriptors.
func (a *Accessor) List(ctx context.Context, txn *kvclient.Txn) ([]*TableDescriptor, error) {
	start, end := keys.TableDescSpan()
	rows, err := txn.Scan(ctx, start, end, 0)
	if err != nil {
		return nil, err
	}
	var out []*TableDescriptor
	for _, kv := range rows {
		var d TableDescriptor
		if err := json.Unmarshal(kv.Value, &d); err != nil {
			continue
		}
		out = append(out, &d)
	}
	return out, nil
}
