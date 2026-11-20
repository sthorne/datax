package catalog

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"strconv"

	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/kvclient"
	"github.com/sthorne/datax/pkg/util/encoding"
)

// SequenceDescriptor is a sequence's definition. Its counter lives apart
// at keys.SequenceValueKey(ID): the last value handed out, advanced with
// Increment outside the caller's transaction (PostgreSQL semantics —
// never rolled back, gaps allowed). Cache is the block size a gateway
// takes per Increment: values are unique across gateways but not
// monotonic between them.
type SequenceDescriptor struct {
	ID         uint64 `json:"id"`
	Name       string `json:"name"`
	DatabaseID uint64 `json:"database_id,omitempty"`
	// SchemaID is the schema the sequence is in (0 = public, gap
	// PG-D-035). A public sequence's name entry is its name; one of
	// another schema is filed under SchemaNamespaceName, as a table is.
	SchemaID  uint64 `json:"schema_id,omitempty"`
	Start     int64  `json:"start"`
	Increment int64  `json:"increment"`
	MinValue  int64  `json:"min_value"`
	MaxValue  int64  `json:"max_value"`
	Cycle     bool   `json:"cycle,omitempty"`
	Cache     int64  `json:"cache"`
	// OwnerTable / OwnerColumn: the column that owns this sequence
	// (SERIAL, identity, OWNED BY); dropped with it.
	OwnerTable  uint64   `json:"owner_table,omitempty"`
	OwnerColumn ColumnID `json:"owner_column,omitempty"`
	// Owner is the owning role (v11; empty = root). Privileges maps a
	// grantee to USAGE / SELECT / UPDATE; GrantOptions the grant-option
	// subset.
	Owner        string              `json:"owner,omitempty"`
	Privileges   map[string][]string `json:"privileges,omitempty"`
	GrantOptions map[string][]string `json:"grant_options,omitempty"`
}

// DefaultSequenceCache is the block a gateway takes per Increment.
const DefaultSequenceCache = 32

// NewSequenceDescriptor applies PostgreSQL's defaults: increment 1,
// min 1 / max 2^63-1 ascending (or min -2^63+1 / max -1 descending),
// start at min (or max descending), no cycle, cache 32.
func NewSequenceDescriptor(name string, dbID uint64) *SequenceDescriptor {
	return &SequenceDescriptor{Name: name, DatabaseID: dbID, Increment: 1, MinValue: 1, MaxValue: math.MaxInt64, Start: 1, Cache: DefaultSequenceCache}
}

// Normalize fills the bounds and start left unset (0) from the
// increment's sign, and validates them.
func (d *SequenceDescriptor) Normalize(minSet, maxSet, startSet bool) error {
	if d.Increment == 0 {
		return fmt.Errorf("INCREMENT must not be zero")
	}
	if d.Increment < 0 {
		if !minSet {
			d.MinValue = math.MinInt64 + 1
		}
		if !maxSet {
			d.MaxValue = -1
		}
		if !startSet {
			d.Start = d.MaxValue
		}
	} else {
		if !minSet {
			d.MinValue = 1
		}
		if !maxSet {
			d.MaxValue = math.MaxInt64
		}
		if !startSet {
			d.Start = d.MinValue
		}
	}
	if d.MinValue >= d.MaxValue {
		return fmt.Errorf("MINVALUE (%d) must be less than MAXVALUE (%d)", d.MinValue, d.MaxValue)
	}
	if d.Start < d.MinValue || d.Start > d.MaxValue {
		return fmt.Errorf("START value (%d) cannot be outside [%d, %d]", d.Start, d.MinValue, d.MaxValue)
	}
	if d.Cache < 1 {
		return fmt.Errorf("CACHE (%d) must be at least 1", d.Cache)
	}
	return nil
}

// ErrSequenceNotFound / ErrSequenceExists are the catalog's name errors.
type ErrSequenceNotFound struct{ Name string }

func (e *ErrSequenceNotFound) Error() string {
	return fmt.Sprintf("sequence %q does not exist", e.Name)
}

type ErrSequenceExists struct{ Name string }

func (e *ErrSequenceExists) Error() string { return fmt.Sprintf("sequence %q already exists", e.Name) }

// NamespaceName is the sequence's name entry within its database: its
// name in public, SchemaNamespaceName's form in another schema.
func (d *SequenceDescriptor) NamespaceName() string { return SchemaNamespaceName(d.SchemaID, d.Name) }

// checkObjectSchema reads schema schemaID of database dbID in txn (so
// that a DROP SCHEMA committing alongside either sees the new object or
// fails this transaction): ErrSchemaNotFound when it is gone.
func checkObjectSchema(ctx context.Context, txn *kvclient.Txn, dbID, schemaID uint64) error {
	if schemaID == 0 {
		return nil
	}
	sd, err := ReadSchema(ctx, txn, schemaID)
	if err != nil {
		return err
	}
	if sd == nil || sd.DatabaseID != dbID {
		return &ErrSchemaNotFound{Name: fmt.Sprintf("#%d", schemaID)}
	}
	return nil
}

// CreateSequence registers a sequence under its database and schema
// (the name must be free of tables and sequences alike in that schema)
// and seeds its counter so the first nextval yields Start.
func CreateSequence(ctx context.Context, txn *kvclient.Txn, d *SequenceDescriptor) error {
	if err := checkObjectSchema(ctx, txn, d.DatabaseID, d.SchemaID); err != nil {
		return err
	}
	entry := d.NamespaceName()
	if existing, err := txn.Get(ctx, keys.SequenceNamespaceKey(d.DatabaseID, entry)); err != nil {
		return err
	} else if existing != nil {
		return &ErrSequenceExists{Name: d.Name}
	}
	if existing, err := txn.Get(ctx, keys.TableNamespaceKey(d.DatabaseID, entry)); err != nil {
		return err
	} else if existing != nil {
		return &ErrTableExists{Name: d.Name}
	}
	id, err := allocateDescID(ctx, txn)
	if err != nil {
		return err
	}
	d.ID = id
	raw, err := json.Marshal(d)
	if err != nil {
		return err
	}
	if err := txn.Put(ctx, keys.SequenceDescKey(d.ID), raw); err != nil {
		return err
	}
	if err := txn.Put(ctx, keys.SequenceNamespaceKey(d.DatabaseID, entry), []byte(strconv.FormatUint(d.ID, 10))); err != nil {
		return err
	}
	return txn.Put(ctx, keys.SequenceValueKey(d.ID), []byte(strconv.FormatInt(d.Start-d.Increment, 10)))
}

// UpdateSequence rewrites a sequence's descriptor (ALTER SEQUENCE).
func UpdateSequence(ctx context.Context, txn *kvclient.Txn, d *SequenceDescriptor) error {
	raw, err := json.Marshal(d)
	if err != nil {
		return err
	}
	return txn.Put(ctx, keys.SequenceDescKey(d.ID), raw)
}

// LookupSequence resolves a sequence of the public schema of a database
// by its name entry (SchemaNamespaceName's form reaches another
// schema's).
func LookupSequence(ctx context.Context, txn *kvclient.Txn, dbID uint64, name string) (*SequenceDescriptor, error) {
	raw, err := txn.Get(ctx, keys.SequenceNamespaceKey(dbID, name))
	if err != nil {
		return nil, err
	}
	if raw == nil {
		return nil, &ErrSequenceNotFound{Name: name}
	}
	id, err := strconv.ParseUint(string(raw), 10, 64)
	if err != nil {
		return nil, fmt.Errorf("corrupt sequence namespace entry for %q", name)
	}
	return ReadSequence(ctx, txn, id)
}

// ReadSequence reads a sequence descriptor by ID.
func ReadSequence(ctx context.Context, txn *kvclient.Txn, id uint64) (*SequenceDescriptor, error) {
	raw, err := txn.Get(ctx, keys.SequenceDescKey(id))
	if err != nil {
		return nil, err
	}
	if raw == nil {
		return nil, &ErrSequenceNotFound{Name: strconv.FormatUint(id, 10)}
	}
	var d SequenceDescriptor
	if err := json.Unmarshal(raw, &d); err != nil {
		return nil, fmt.Errorf("corrupt sequence descriptor %d: %w", id, err)
	}
	return &d, nil
}

// ListSequences lists a database's sequences (every database's when
// dbID is 0), by name.
func ListSequences(ctx context.Context, txn *kvclient.Txn, dbID uint64) ([]*SequenceDescriptor, error) {
	lo, hi := keys.AllSequenceNamespaceSpan()
	if dbID != 0 {
		lo, hi = keys.SequenceNamespaceSpan(dbID)
	}
	rows, err := txn.Scan(ctx, lo, hi, 0)
	if err != nil {
		return nil, err
	}
	var out []*SequenceDescriptor
	for _, kv := range rows {
		id, err := strconv.ParseUint(string(kv.Value), 10, 64)
		if err != nil {
			continue
		}
		d, err := ReadSequence(ctx, txn, id)
		if err != nil {
			return nil, err
		}
		out = append(out, d)
	}
	return out, nil
}

// LookupSequenceIn resolves a sequence name as written ("x", "s.x",
// "db.s.x", or a SequenceRelationName) in database db: through the
// search path in ctx for an unqualified name (public alone without
// one), where its qualifiers say otherwise — the rules tables follow
// (resolve).
func (a *Accessor) LookupSequenceIn(ctx context.Context, txn *kvclient.Txn, db, name string) (*SequenceDescriptor, error) {
	if IsReservedTableName(name) && !IsRelationName(name) {
		return nil, &ErrSequenceNotFound{Name: name}
	}
	cands, err := a.resolve(ctx, txn, db, name)
	if err != nil {
		return nil, err
	}
	for _, c := range cands {
		raw, err := txn.Get(ctx, keys.SequenceNamespaceKey(c.dbID, c.entry))
		if err != nil {
			return nil, err
		}
		if raw == nil {
			continue
		}
		id, err := strconv.ParseUint(string(raw), 10, 64)
		if err != nil {
			return nil, fmt.Errorf("corrupt sequence namespace entry for %q", c.entry)
		}
		return ReadSequence(ctx, txn, id)
	}
	return nil, &ErrSequenceNotFound{Name: displayName(name)}
}

// SequenceRelationName is a name that resolves to exactly sequence d
// whatever the search path (RelationName's form, with the sequence's
// name entry).
func SequenceRelationName(d *SequenceDescriptor) string {
	return relMarker + strconv.FormatUint(d.DatabaseID, 10) + "\x00" + d.NamespaceName()
}

// SetSequenceSchema moves sequence d into schema schemaID of its
// database: its name entry moves, its ID (and so its counter, its
// owner column and every default bound to it) stays.
func SetSequenceSchema(ctx context.Context, txn *kvclient.Txn, d *SequenceDescriptor, schemaID uint64) error {
	if err := checkObjectSchema(ctx, txn, d.DatabaseID, schemaID); err != nil {
		return err
	}
	newEntry := SchemaNamespaceName(schemaID, d.Name)
	for _, k := range []keys.Key{keys.SequenceNamespaceKey(d.DatabaseID, newEntry), keys.TableNamespaceKey(d.DatabaseID, newEntry)} {
		if existing, err := txn.Get(ctx, k); err != nil {
			return err
		} else if existing != nil {
			return &ErrSequenceExists{Name: d.Name}
		}
	}
	if err := txn.Delete(ctx, keys.SequenceNamespaceKey(d.DatabaseID, d.NamespaceName())); err != nil {
		return err
	}
	if err := txn.Put(ctx, keys.SequenceNamespaceKey(d.DatabaseID, newEntry), []byte(strconv.FormatUint(d.ID, 10))); err != nil {
		return err
	}
	d.SchemaID = schemaID
	return UpdateSequence(ctx, txn, d)
}

// DropSequence removes a sequence's descriptor, name and counter.
func DropSequence(ctx context.Context, txn *kvclient.Txn, d *SequenceDescriptor) error {
	if err := txn.Delete(ctx, keys.SequenceNamespaceKey(d.DatabaseID, d.NamespaceName())); err != nil {
		return err
	}
	if err := txn.Delete(ctx, keys.SequenceDescKey(d.ID)); err != nil {
		return err
	}
	return txn.Delete(ctx, keys.SequenceValueKey(d.ID))
}

// SequenceIDOfKey decodes the sequence ID a descriptor key refers to.
func SequenceIDOfKey(k keys.Key) (uint64, bool) {
	lo, _ := keys.SequenceDescSpan()
	if len(k) <= len(lo) || string(k[:len(lo)]) != string(lo) {
		return 0, false
	}
	_, id, err := encoding.DecodeUint64(k[len(lo):])
	if err != nil {
		return 0, false
	}
	return id, true
}
