// Package catalog manages SQL table descriptors, stored as JSON in the
// system keyspace (range 1) and manipulated transactionally.
package catalog

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/kvclient"
	"github.com/sthorne/datax/pkg/sql/types"
	"github.com/sthorne/datax/pkg/util/hlc"
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
	// Default is the value INSERT uses when the column is omitted.
	Default *types.Datum `json:"default,omitempty"`
	// FillDefault marks a column added by ALTER TABLE ... DEFAULT: rows
	// written before the ADD lack the column entirely and decode as the
	// default (fill-on-read). Rows written afterwards store an explicit
	// NULL marker when the column is NULL, so NULL and "predates the
	// column" stay distinguishable.
	FillDefault bool `json:"fill_default,omitempty"`
}

// IndexDescriptor describes a secondary index. Entries live at
// /t/<tableID>/<ID>/ (see pkg/sql/rowenc): non-unique keys append the
// primary key columns after the indexed ones; unique keys carry the encoded
// primary key as the entry's value.
type IndexDescriptor struct {
	ID        uint64     `json:"id"`
	Name      string     `json:"name"`
	Unique    bool       `json:"unique,omitempty"`
	ColumnIDs []ColumnID `json:"column_ids"`
	// State is the index's lifecycle state: "" or "public" = readable;
	// "write-only" = maintained by writers but invisible to the planner
	// (the CREATE INDEX backfill window). See IndexStateWriteOnly.
	State string `json:"state,omitempty"`
}

// Index lifecycle states.
const (
	IndexStatePublic    = "public"
	IndexStateWriteOnly = "write-only"
)

// Public reports whether the index may serve reads.
func (idx *IndexDescriptor) Public() bool {
	return idx.State == "" || idx.State == IndexStatePublic
}

// TableDescriptor describes a table. Primary rows are stored at
// /t/<ID>/1/<encoded primary key> (see pkg/sql/rowenc).
type TableDescriptor struct {
	ID         uint64     `json:"id"`
	Name       string     `json:"name"`
	Columns    []Column   `json:"columns"`
	PrimaryKey []ColumnID `json:"primary_key"`
	// Indexes are the table's secondary indexes. NextIndexID is the next
	// index ID to allocate (primary rows are index 1; secondaries start at
	// 2; IDs are never reused).
	Indexes     []IndexDescriptor `json:"indexes,omitempty"`
	NextIndexID uint64            `json:"next_index_id,omitempty"`
	// NextColumnID is the next column ID to allocate; never reused, so a
	// dropped-then-re-added column gets a fresh ID and old bytes stay dead.
	NextColumnID ColumnID `json:"next_column_id,omitempty"`
	// Version increments on every descriptor change; gateway leases record
	// which version they may be using (see leasing in this package).
	Version uint64 `json:"version,omitempty"`
}

// Index returns the secondary index with the given name.
func (d *TableDescriptor) Index(name string) (IndexDescriptor, bool) {
	for _, idx := range d.Indexes {
		if idx.Name == name {
			return idx, true
		}
	}
	return IndexDescriptor{}, false
}

// Clone deep-copies the descriptor (mutate copies, never cached ones).
func (d *TableDescriptor) Clone() *TableDescriptor {
	out := *d
	out.Columns = append([]Column(nil), d.Columns...)
	out.PrimaryKey = append([]ColumnID(nil), d.PrimaryKey...)
	out.Indexes = make([]IndexDescriptor, len(d.Indexes))
	for i, idx := range d.Indexes {
		out.Indexes[i] = idx
		out.Indexes[i].ColumnIDs = append([]ColumnID(nil), idx.ColumnIDs...)
	}
	return &out
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
// per-gateway cache. With leasing enabled (StartLeasing; wired for real
// gateways, optional in tests), each cached descriptor is covered by a
// lease record at its version: cached entries expire with the lease, a
// background loop renews them (adopting new versions), and DDL drains
// against every gateway's lease before completing — see lease.go.
type Accessor struct {
	mu    sync.Mutex
	cache map[string]*cachedDesc

	// Leasing state; zero when disabled (bare accessors behave as a plain
	// cache that never expires, today's pre-lease semantics).
	leasing bool
	db      *kvclient.DB
	clock   *hlc.Clock
	gateway uuid.UUID
	ttl     time.Duration
}

type cachedDesc struct {
	desc *TableDescriptor
	// expireAt bounds cache use to the lease's lifetime (zero = forever,
	// leasing disabled).
	expireAt time.Time
}

func NewAccessor() *Accessor {
	return &Accessor{cache: make(map[string]*cachedDesc)}
}

// Lookup resolves a table by name within txn, using the cache while its
// lease (if any) is live.
func (a *Accessor) Lookup(ctx context.Context, txn *kvclient.Txn, name string) (*TableDescriptor, error) {
	a.mu.Lock()
	if c, ok := a.cache[name]; ok && (c.expireAt.IsZero() || time.Now().Before(c.expireAt)) {
		a.mu.Unlock()
		return c.desc, nil
	}
	a.mu.Unlock()
	d, err := lookupUncached(ctx, txn, name)
	if err != nil {
		return nil, err
	}
	entry := &cachedDesc{desc: d}
	if a.leasing {
		if err := a.writeLease(ctx, d); err != nil {
			// Without a lease the cache may not be trusted beyond this
			// statement; return the descriptor uncached.
			return d, nil
		}
		entry.expireAt = time.Now().Add(a.ttl)
	}
	a.mu.Lock()
	a.cache[name] = entry
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
	d.Version = 1
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

// Update rewrites an existing table's descriptor within txn (DDL like
// CREATE INDEX / ALTER TABLE), bumping its version.
func (a *Accessor) Update(ctx context.Context, txn *kvclient.Txn, d *TableDescriptor) error {
	d.Version++
	raw, err := json.Marshal(d)
	if err != nil {
		return err
	}
	if err := txn.Put(ctx, keys.TableDescKey(d.ID), raw); err != nil {
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
