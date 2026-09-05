// Package catalog manages SQL table descriptors, stored as JSON in the
// system keyspace (range 1) and manipulated transactionally.
package catalog

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode/utf8"

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
	ID   ColumnID     `json:"id"`
	Name string       `json:"name"`
	Type types.Family `json:"type"`
	// Precision/Scale enforce a DECIMAL(p,s) typmod: values are rescaled
	// to Scale (round-half-even) on write and rejected (22003) when their
	// integer digits exceed Precision−Scale. Zero Precision = bare
	// DECIMAL, unconstrained — the safe pre-existing meaning, so old
	// descriptors keep their behavior (append-only JSON, pkg/version
	// rule 1).
	Precision int32 `json:"precision,omitempty"`
	Scale     int32 `json:"scale,omitempty"`
	// Width is an Int column's declared width in bytes: 2 (INT2 /
	// SMALLINT), 4 (INT4 / INT / INTEGER) or 0 = 8 (INT8 / BIGINT).
	// Storage is the same varint either way; the width bounds the values
	// (22003) and picks the wire type (int2 / int4 / int8). Zero keeps the
	// pre-existing 64-bit meaning for old descriptors.
	Width int32 `json:"width,omitempty"`
	// MaxLen is a String column's VARCHAR(n) / CHAR(n) length in
	// characters (0 = unbounded TEXT); a longer value is refused (22001)
	// unless the excess is spaces.
	MaxLen int32 `json:"max_len,omitempty"`
	// Char marks CHAR(n): values are stored with trailing spaces trimmed
	// and render blank-padded to MaxLen.
	Char bool `json:"char,omitempty"`
	// NoTZ marks TIMESTAMP (without time zone): the value is UTC wall-
	// clock time, an offset in the input is ignored, and the output
	// carries no offset.
	NoTZ bool `json:"no_tz,omitempty"`
	// EnumType, EnumName and EnumLabels describe an enum column (Type
	// Enum): the type descriptor's ID and name, and a copy of its labels
	// in ordinal order (refreshed by ALTER TYPE ... ADD VALUE), so a
	// value's label converts to its ordinal and back without a lookup.
	EnumType   uint64   `json:"enum_type,omitempty"`
	EnumName   string   `json:"enum_name,omitempty"`
	EnumLabels []string `json:"enum_labels,omitempty"`
	// TimePrecision is TIMESTAMP(p) / TIMESTAMPTZ(p), stored as p+1 so
	// that 0 keeps meaning "undeclared" (full precision): values round to
	// p fractional digits on write. FracDigits decodes it.
	TimePrecision int32 `json:"time_precision,omitempty"`
	NotNull       bool  `json:"not_null,omitempty"`
	// Default is the value INSERT uses when the column is omitted.
	Default *types.Datum `json:"default,omitempty"`
	// DefaultExpr is an expression default (SQL text: now(),
	// gen_random_uuid(), unique_rowid(), nextval('s'), 1 + 2), evaluated
	// per row at insert time when the column is omitted or given
	// DEFAULT. Requires cluster version v7. Exclusive with Default.
	DefaultExpr string `json:"default_expr,omitempty"`
	// Identity marks GENERATED { ALWAYS | BY DEFAULT } AS IDENTITY
	// ("always" / "by default"): the column draws from its own sequence;
	// ALWAYS refuses an explicit value unless OVERRIDING SYSTEM VALUE.
	Identity string `json:"identity,omitempty"`
	// SequenceID is the sequence this column owns (SERIAL / identity):
	// dropped with the column or table; pg_attrdef renders nextval(it).
	SequenceID uint64 `json:"sequence_id,omitempty"`
	// FillDefault marks a column added by ALTER TABLE ... DEFAULT: rows
	// written before the ADD lack the column entirely and decode as the
	// default (fill-on-read). Rows written afterwards store an explicit
	// NULL marker when the column is NULL, so NULL and "predates the
	// column" stay distinguishable.
	FillDefault bool `json:"fill_default,omitempty"`
	// Hidden marks a system-managed column (the _shard bucket of a
	// sharded timeseries table, CREATE TABLE AS's rowid key, the shadow
	// of an ALTER COLUMN TYPE rewrite): invisible to SELECT *, not an
	// INSERT target, and filled by the executor.
	Hidden bool `json:"hidden,omitempty"`
	// Comment is COMMENT ON COLUMN's text ("" = none).
	Comment string `json:"comment,omitempty"`
	// RetypeFrom, on a hidden shadow column, names the column it is the
	// retyped copy of (ALTER COLUMN TYPE, pkg/sql/retype.go): every row
	// write derives the shadow's value from the original's, so the
	// backfill and concurrent writers converge; the swap makes the
	// shadow the column. Cluster version v9.
	RetypeFrom ColumnID `json:"retype_from,omitempty"`
}

// ValueError is a value the column's declared type refuses; Code is the
// SQLSTATE (22003 out of range, 22001 too long, 22007 bad timestamp).
type ValueError struct {
	Code string
	Msg  string
}

func (e *ValueError) Error() string { return e.Msg }

// HasTypmod reports whether the column carries a type modifier the
// write path must apply (DECIMAL(p,s), an integer width, a character
// length, CHAR padding, TIMESTAMP without time zone or TIMESTAMP(p)).
func (c *Column) HasTypmod() bool {
	return c.Precision > 0 || c.Width == 2 || c.Width == 4 || c.MaxLen > 0 || c.Char || c.NoTZ || c.TimePrecision > 0 || c.Type.IsArray() || c.Type == types.Enum
}

// EnumValue converts a label (or an enum datum of the type) to the
// column's enum datum: the ordinal and the label.
func (c *Column) EnumValue(d types.Datum) (types.Datum, error) {
	if d.Null {
		return d, nil
	}
	label := d.S
	if d.Fam != types.String && d.Fam != types.Enum {
		label = d.Text()
	}
	for i, l := range c.EnumLabels {
		if l == label {
			return types.NewEnum(int64(i), l), nil
		}
	}
	return d, &ValueError{Code: "22P02", Msg: fmt.Sprintf("invalid input value for enum %s: %q", c.EnumName, label)}
}

// FracDigits is the declared TIMESTAMP(p) precision, when there is one.
func (c *Column) FracDigits() (int32, bool) {
	if (c.Type == types.Timestamp || c.Type == types.Time) && c.TimePrecision > 0 {
		return c.TimePrecision - 1, true
	}
	return 0, false
}

// IntWidth is the column's integer width in bytes (8 unless declared
// narrower).
func (c *Column) IntWidth() int32 {
	if (c.Type == types.Int || c.Type == types.ArrayOf(types.Int)) && (c.Width == 2 || c.Width == 4) {
		return c.Width
	}
	return 8
}

// Conform applies the column's type modifiers other than DECIMAL(p,s)
// to a value about to be stored, and stamps the display hints: an
// integer is range-checked against the width; a string is checked
// against MaxLen (excess spaces are dropped, anything else is 22001)
// and, for CHAR(n), trailing spaces are trimmed; a timestamp text into a
// TIMESTAMP (without time zone) column is parsed ignoring its offset,
// and a TIMESTAMP(p) value rounds to p digits. NULLs and columns without
// modifiers pass through.
func (c *Column) Conform(d types.Datum) (types.Datum, error) {
	if d.Null {
		return d, nil
	}
	if c.Type.IsArray() {
		// Every element conforms to the element type (the column's
		// modifiers apply per element); text elements coerce first.
		if !d.Fam.IsArray() {
			return d, nil
		}
		elem := *c
		elem.Type = c.Type.Elem()
		out := make([]types.Datum, len(d.A))
		for i, e := range d.A {
			if e.Null {
				out[i] = e
				continue
			}
			v, err := e.Coerce(elem.Type)
			if err != nil {
				return d, &ValueError{Code: "22P02", Msg: fmt.Sprintf("column %q: array element %s: %v", c.Name, e.Text(), err)}
			}
			if v, err = elem.Conform(v); err != nil {
				return d, err
			}
			out[i] = v
		}
		return types.NewArray(elem.Type, out), nil
	}
	switch c.Type {
	case types.Enum:
		return c.EnumValue(d)
	case types.Int:
		if d.Fam != types.Int {
			return d, nil
		}
		switch c.Width {
		case 2:
			if d.I < -32768 || d.I > 32767 {
				return d, &ValueError{Code: "22003", Msg: fmt.Sprintf("value %d is out of range for type smallint (column %q)", d.I, c.Name)}
			}
		case 4:
			if d.I < -2147483648 || d.I > 2147483647 {
				return d, &ValueError{Code: "22003", Msg: fmt.Sprintf("value %d is out of range for type integer (column %q)", d.I, c.Name)}
			}
		}
	case types.String:
		if d.Fam != types.String || (c.MaxLen == 0 && !c.Char) {
			return d, nil
		}
		v := d.S
		if c.MaxLen > 0 {
			if n := int32(utf8.RuneCountInString(v)); n > c.MaxLen {
				runes := []rune(v)
				if strings.TrimRight(string(runes[c.MaxLen:]), " ") != "" {
					return d, &ValueError{Code: "22001", Msg: fmt.Sprintf("value too long for type %s (column %q)", c.TypeSQL(), c.Name)}
				}
				v = string(runes[:c.MaxLen])
			}
		}
		if c.Char {
			v = strings.TrimRight(v, " ")
		}
		out := types.NewString(v)
		if c.Char {
			out.Pad = c.MaxLen
		}
		return out, nil
	case types.Timestamp:
		if d.Fam == types.String && c.NoTZ {
			n, err := types.ParseTimestampNoTZ(d.S)
			if err != nil {
				return d, &ValueError{Code: "22007", Msg: fmt.Sprintf("column %q: %v", c.Name, err)}
			}
			d = types.NewTimestamp(n)
		}
		if d.Fam != types.Timestamp {
			return d, nil
		}
		if p, ok := c.FracDigits(); ok {
			d.I = types.RoundTimestamp(d.I, p)
		}
		d.NoTZ = c.NoTZ
		return d, nil
	case types.Time:
		if d.Fam != types.Time {
			return d, nil
		}
		if p, ok := c.FracDigits(); ok {
			d.I = types.RoundTimestamp(d.I, p)
		}
		return d, nil
	}
	return d, nil
}

// Stamp sets the display hints a stored value of this column carries
// (CHAR padding, TIMESTAMP without time zone); identity is untouched.
func (c *Column) Stamp(d types.Datum) types.Datum {
	if d.Null {
		return d
	}
	switch {
	case c.Char && d.Fam == types.String:
		d.Pad = c.MaxLen
	case c.NoTZ && d.Fam == types.Timestamp:
		d.NoTZ = true
	}
	return d
}

// TypeSQL is the column's declared type as datax spells it (INT4,
// VARCHAR(20), CHAR(4), TIMESTAMP(3), TIMESTAMPTZ, DECIMAL(10,2)).
func (c *Column) TypeSQL() string {
	if c.Type.IsArray() {
		elem := *c
		elem.Type = c.Type.Elem()
		return elem.TypeSQL() + "[]"
	}
	switch c.Type {
	case types.Enum:
		return c.EnumName
	case types.Int:
		switch c.Width {
		case 2:
			return "INT2"
		case 4:
			return "INT4"
		}
		return "INT8"
	case types.String:
		switch {
		case c.Char:
			return fmt.Sprintf("CHAR(%d)", c.MaxLen)
		case c.MaxLen > 0:
			return fmt.Sprintf("VARCHAR(%d)", c.MaxLen)
		}
		return "TEXT"
	case types.Timestamp:
		name := "TIMESTAMPTZ"
		if c.NoTZ {
			name = "TIMESTAMP"
		}
		if p, ok := c.FracDigits(); ok {
			return fmt.Sprintf("%s(%d)", name, p)
		}
		return name
	case types.Time:
		if p, ok := c.FracDigits(); ok {
			return fmt.Sprintf("TIME(%d)", p)
		}
		return "TIME"
	case types.Decimal:
		if c.Precision > 0 {
			return fmt.Sprintf("DECIMAL(%d,%d)", c.Precision, c.Scale)
		}
		return "DECIMAL"
	}
	return c.Type.String()
}

// Typmod is PostgreSQL's atttypmod for the column: ((p<<16)|s)+4 for
// DECIMAL(p,s), n+4 for VARCHAR(n) / CHAR(n), p for TIMESTAMP(p), and
// -1 (returned as 0) otherwise — TIMESTAMP(0)'s typmod 0 is therefore
// indistinguishable from none on the wire, which only loses the
// rounding hint.
func (c *Column) Typmod() int32 {
	if c.Type.IsArray() {
		return 0
	}
	switch c.Type {
	case types.Decimal:
		if c.Precision > 0 {
			return c.Precision<<16 | (c.Scale + 4)
		}
	case types.String:
		if c.MaxLen > 0 {
			return c.MaxLen + 4
		}
	case types.Timestamp, types.Time:
		if p, ok := c.FracDigits(); ok {
			return p
		}
	}
	return 0
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
	// Comment is COMMENT ON INDEX's text ("" = none).
	Comment string `json:"comment,omitempty"`
	// State is the index's lifecycle state: "" or "public" = readable;
	// "write-only" = maintained by writers but invisible to the planner
	// (the CREATE INDEX backfill window). See IndexStateWriteOnly.
	State string `json:"state,omitempty"`
}

// Constraint kinds.
const (
	ConstraintCheck   = "check"
	ConstraintForeign = "foreign"
	ConstraintUnique  = "unique"
)

// Foreign-key referential actions (ON DELETE / ON UPDATE). NO ACTION is
// stored as restrict: without deferred checking the two behave alike.
const (
	FKRestrict = "restrict"
	FKCascade  = "cascade"
	FKSetNull  = "set null"
)

// Constraint is a table constraint beyond the primary key and NOT NULL
// (cluster version v8): a CHECK expression, a foreign key, or a named
// UNIQUE constraint backed by a unique index. Constraint IDs are never
// reused.
type Constraint struct {
	ID   uint64 `json:"id"`
	Name string `json:"name"`
	Kind string `json:"kind"`
	// Columns are the constrained columns: the columns a CHECK
	// expression references, the referencing columns of a foreign key,
	// or the unique columns.
	Columns []ColumnID `json:"columns,omitempty"`
	// Expr is a CHECK constraint's expression, as SQL text.
	Expr string `json:"expr,omitempty"`
	// RefTable / RefColumns are a foreign key's referenced table and
	// columns (its primary key or a unique index), parallel to Columns.
	RefTable   uint64     `json:"ref_table,omitempty"`
	RefColumns []ColumnID `json:"ref_columns,omitempty"`
	// OnDelete / OnUpdate are a foreign key's referential actions
	// (FKRestrict, FKCascade, FKSetNull; "" = restrict).
	OnDelete string `json:"on_delete,omitempty"`
	OnUpdate string `json:"on_update,omitempty"`
	// IndexID is the unique index a UNIQUE constraint is (dropped with
	// it), or the index created for a foreign key's referencing columns
	// when none covered them (AutoIndex; dropped with the constraint).
	IndexID   uint64 `json:"index_id,omitempty"`
	AutoIndex bool   `json:"auto_index,omitempty"`
	// Validated is false for a CHECK or FOREIGN KEY added NOT VALID (or
	// still being validated): new writes are checked, existing rows
	// were not.
	Validated bool `json:"validated"`
}

// ForeignKeyRef points from a referenced table to a foreign key on one
// of its referencing tables, so a delete or key update on the parent
// finds its children without scanning the catalog.
type ForeignKeyRef struct {
	TableID      uint64 `json:"table_id"`
	ConstraintID uint64 `json:"constraint_id"`
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
	ID   uint64 `json:"id"`
	Name string `json:"name"`
	// Virtual names the virtual catalog table this descriptor renders
	// ("pg_catalog.pg_class"); never persisted. Its rows come from a
	// generator (pkg/sql/vtable), never from the key space.
	Virtual string `json:"-"`
	// DatabaseID is the owning database (see DatabaseDescriptor). 0 marks
	// a table created before the cluster finalized v6, which lives in the
	// flat namespace and belongs to the default database until the
	// migration stamps it.
	DatabaseID uint64     `json:"database_id,omitempty"`
	Columns    []Column   `json:"columns"`
	PrimaryKey []ColumnID `json:"primary_key"`
	// Indexes are the table's secondary indexes. NextIndexID is the next
	// index ID to allocate (primary rows are index 1; secondaries start at
	// 2; IDs are never reused).
	Indexes     []IndexDescriptor `json:"indexes,omitempty"`
	NextIndexID uint64            `json:"next_index_id,omitempty"`
	// Constraints are the table's CHECK, FOREIGN KEY and named UNIQUE
	// constraints (v8); NextConstraintID the next ID to allocate.
	// InboundFKs are the foreign keys of other tables that reference
	// this one.
	Constraints      []Constraint    `json:"constraints,omitempty"`
	NextConstraintID uint64          `json:"next_constraint_id,omitempty"`
	InboundFKs       []ForeignKeyRef `json:"inbound_fks,omitempty"`
	// NextColumnID is the next column ID to allocate; never reused, so a
	// dropped-then-re-added column gets a fresh ID and old bytes stay dead.
	NextColumnID ColumnID `json:"next_column_id,omitempty"`
	// Comment is COMMENT ON TABLE / VIEW's text ("" = none).
	Comment string `json:"comment,omitempty"`
	// ViewQuery is the SELECT a view stands for, as SQL text; a
	// descriptor carrying one is a view (cluster version v9): it owns no
	// rows and no primary key, Columns describe the query's output, and
	// a statement that names it runs the query (pkg/sql/view.go).
	ViewQuery string `json:"view_query,omitempty"`
	// ViewDepends are the tables and views the view's query reads, by
	// ID: dropping one of them is refused while the view exists.
	ViewDepends []uint64 `json:"view_depends,omitempty"`
	// Owner is the role that owns the table or view (v11): it holds every
	// privilege on it and may alter or drop it. Empty means root (an
	// object that predates ownership).
	Owner string `json:"owner,omitempty"`
	// Privileges maps a grantee role (or "public") to its granted
	// privileges (SELECT/INSERT/UPDATE/DELETE/TRUNCATE, upper-cased; ALL
	// is stored expanded). root, admins and the owner bypass the map.
	Privileges map[string][]string `json:"privileges,omitempty"`
	// GrantOptions maps a grantee to the privileges it holds WITH GRANT
	// OPTION (a subset of Privileges).
	GrantOptions map[string][]string `json:"grant_options,omitempty"`
	// Version increments on every descriptor change; gateway leases record
	// which version they may be using (see leasing in this package).
	Version uint64 `json:"version,omitempty"`
	// Timeseries marks a table created WITH (timeseries=true): its last
	// primary-key column is a TIMESTAMPTZ, it may carry a retention TTL,
	// and it may be sharded across buckets by a hidden leading PK column.
	Timeseries bool `json:"timeseries,omitempty"`
	// RetentionSeconds (timeseries only): rows older than this are
	// eligible for GC — reads below the cutoff fail like any read below
	// the GC threshold. 0 = keep forever (default GC policy applies).
	RetentionSeconds int64 `json:"retention_seconds,omitempty"`
	// ShardBuckets (timeseries only): when > 0, a hidden _shard column
	// (fnv32a of the logical PK encoding, mod this) leads the primary
	// key, spreading a monotone timestamp tail across ShardBuckets
	// ranges. It defines the key layout: changing it requires rewriting
	// every row (ALTER TABLE ... SET (shards = N), the online re-shard).
	ShardBuckets int32 `json:"shard_buckets,omitempty"`
	// PrimaryIndex is the index ID primary rows live at; 0 means the
	// original rowenc.PrimaryIndexID (1) — the zero value keeps every
	// pre-existing descriptor valid. A re-shard rewrites the table at a
	// fresh index ID and swaps this atomically with ShardBuckets.
	PrimaryIndex uint64 `json:"primary_index,omitempty"`
	// Reshard, when non-nil, marks an in-flight re-shard: every writer
	// dual-writes rows to NewIndexID with the bucket recomputed mod
	// NewBuckets while the backfill copies history.
	Reshard *ReshardState `json:"reshard,omitempty"`
	// ReshardedAt is the HLC wall time of the last completed re-shard
	// swap. Historical reads below it route through the retired layout
	// current at their timestamp (the historical descriptor read supplies
	// it) while that layout is still retained, and are rejected once the
	// janitor has wiped it.
	ReshardedAt int64 `json:"resharded_at,omitempty"`
	// RetiredLayouts records superseded re-shard layouts kept on disk for
	// historical reads: the pre-swap primary index and secondary-index
	// generations stay readable until RetiredAt ages past the historical
	// window (the GC TTL by default), then the re-shard janitor wipes
	// them and removes the entry.
	RetiredLayouts []RetiredLayout `json:"retired_layouts,omitempty"`
}

// RetiredLayout is one superseded re-shard layout awaiting reclamation.
type RetiredLayout struct {
	PrimaryIndexID uint64   `json:"primary_index_id"`
	IndexIDs       []uint64 `json:"index_ids,omitempty"` // secondary-index generations
	Buckets        int32    `json:"buckets"`
	RetiredAt      int64    `json:"retired_at"` // wall nanos of the swap
}

// ReshardState is the descriptor marker for an in-flight re-shard.
type ReshardState struct {
	NewIndexID uint64 `json:"new_index_id"`
	NewBuckets int32  `json:"new_buckets"`
	// NewIndexIDs are the shadow IDs the table's secondary indexes are
	// rebuilt at, parallel to Indexes: index entries embed the shard
	// bucket in their primary-key suffix, so a re-shard rewrites every
	// entry under a fresh ID and the swap adopts them together with the
	// primary layout. Empty for tables without secondary indexes (and on
	// descriptors written before this field existed — such re-shards
	// carried no indexes by the old guard).
	NewIndexIDs []uint64 `json:"new_index_ids,omitempty"`
}

// LivePrimaryIndex is the index ID primary rows are read and written at.
func (d *TableDescriptor) LivePrimaryIndex() uint64 {
	if d.PrimaryIndex != 0 {
		return d.PrimaryIndex
	}
	return 1 // rowenc.PrimaryIndexID; literal to avoid an import cycle
}

// VisibleColumns returns the columns SELECT * expands to and INSERT
// implicitly targets — every column except Hidden ones.
func (d *TableDescriptor) VisibleColumns() []Column {
	out := make([]Column, 0, len(d.Columns))
	for _, c := range d.Columns {
		if !c.Hidden {
			out = append(out, c)
		}
	}
	return out
}

// IsView reports whether the descriptor is a view (it carries a query
// and no rows).
func (d *TableDescriptor) IsView() bool { return d.ViewQuery != "" }

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
	if d.Constraints != nil {
		out.Constraints = make([]Constraint, len(d.Constraints))
		for i, c := range d.Constraints {
			out.Constraints[i] = c
			out.Constraints[i].Columns = append([]ColumnID(nil), c.Columns...)
			out.Constraints[i].RefColumns = append([]ColumnID(nil), c.RefColumns...)
		}
	}
	out.InboundFKs = append([]ForeignKeyRef(nil), d.InboundFKs...)
	out.ViewDepends = append([]uint64(nil), d.ViewDepends...)
	out.Privileges = ClonePrivileges(d.Privileges)
	out.GrantOptions = ClonePrivileges(d.GrantOptions)
	if d.Reshard != nil {
		rs := *d.Reshard
		rs.NewIndexIDs = append([]uint64(nil), d.Reshard.NewIndexIDs...)
		out.Reshard = &rs
	}
	if d.RetiredLayouts != nil {
		out.RetiredLayouts = make([]RetiredLayout, len(d.RetiredLayouts))
		for i, rl := range d.RetiredLayouts {
			out.RetiredLayouts[i] = rl
			out.RetiredLayouts[i].IndexIDs = append([]uint64(nil), rl.IndexIDs...)
		}
	}
	return &out
}

// Constraint returns the named constraint.
func (d *TableDescriptor) Constraint(name string) (*Constraint, bool) {
	for i := range d.Constraints {
		if d.Constraints[i].Name == name {
			return &d.Constraints[i], true
		}
	}
	return nil, false
}

// ConstraintByID returns the constraint with the given ID.
func (d *TableDescriptor) ConstraintByID(id uint64) (*Constraint, bool) {
	for i := range d.Constraints {
		if d.Constraints[i].ID == id {
			return &d.Constraints[i], true
		}
	}
	return nil, false
}

// IndexByID returns the index with the given ID.
func (d *TableDescriptor) IndexByID(id uint64) (*IndexDescriptor, bool) {
	for i := range d.Indexes {
		if d.Indexes[i].ID == id {
			return &d.Indexes[i], true
		}
	}
	return nil, false
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
	cache map[string]*cachedDesc // keyed by cacheKey(dbID, name)
	// dbs caches database name → ID (0 = the default database before
	// the v6 migration); invalidated by database DDL on this gateway and
	// re-read on a miss, so another gateway's CREATE DATABASE is seen on
	// first use.
	dbs map[string]uint64

	// Leasing state; zero when disabled (bare accessors behave as a plain
	// cache that never expires, today's pre-lease semantics).
	leasing bool
	db      *kvclient.DB
	clock   *hlc.Clock
	gateway uuid.UUID
	ttl     time.Duration
	// renewalPaused makes the renewal loop skip its ticks (tests only).
	renewalPaused atomic.Bool

	// Statistics cache (stats.go); statsDB nil = stats disabled.
	statsMu    sync.Mutex
	statsDB    *kvclient.DB
	statsCache map[uint64]*cachedStats
}

type cachedDesc struct {
	desc *TableDescriptor
	// expiration is the HLC wall time at which this gateway's lease on
	// the descriptor expires — the very value written into the lease
	// record, so the cache never outlives what a DDL drain will wait for
	// (zero = forever, an unleased accessor). Transactions that plan
	// against the entry take it as their commit deadline.
	expiration int64
}

func NewAccessor() *Accessor {
	return &Accessor{cache: make(map[string]*cachedDesc)}
}

// Lookup resolves a table by name within txn, using the cache while its
// lease (if any) is live.
//
// A HISTORICAL transaction bypasses both: the descriptor key is an
// ordinary MVCC value, so reading it through the pinned-timestamp txn
// yields the descriptor version current AT that timestamp — exactly what
// a historical query must plan against (a re-shard's pre-swap layout
// included). The cache would serve the CURRENT descriptor instead, and
// writing a lease for a backdated version would poison the gateway's
// cache and stall concurrent DDL drains waiting for every live lease to
// adopt the new version.
func (a *Accessor) Lookup(ctx context.Context, txn *kvclient.Txn, name string) (*TableDescriptor, error) {
	return a.LookupIn(ctx, txn, DefaultDatabase, name)
}

// LookupIn resolves a table by name within txn, in database db unless
// name is qualified ("otherdb.t"), using the cache while its lease (if
// any) is live.
func (a *Accessor) LookupIn(ctx context.Context, txn *kvclient.Txn, db, name string) (*TableDescriptor, error) {
	if q, bare := SplitTableName(name); q != "" {
		db, name = q, bare
	}
	dbID, err := a.databaseID(ctx, txn, db)
	if err != nil {
		return nil, err
	}
	if txn != nil && txn.Historical() {
		return lookupUncached(ctx, txn, dbID, name, isDefaultDatabase(db))
	}
	key := cacheKey(dbID, name)
	a.mu.Lock()
	if c, ok := a.cache[key]; ok && (c.expiration == 0 || a.nowWallLocked() < c.expiration) {
		a.mu.Unlock()
		pinDeadline(txn, c.expiration)
		return c.desc, nil
	}
	a.mu.Unlock()
	d, err := lookupUncached(ctx, txn, dbID, name, isDefaultDatabase(db))
	if err != nil {
		return nil, err
	}
	entry := &cachedDesc{desc: d}
	if a.leasing {
		exp, err := a.writeLease(ctx, d)
		if err != nil {
			// Without a lease the cache may not be trusted beyond this
			// statement; return the descriptor uncached.
			return d, nil
		}
		entry.expiration = exp
	}
	a.mu.Lock()
	a.cache[key] = entry
	a.mu.Unlock()
	pinDeadline(txn, entry.expiration)
	return d, nil
}

// cacheKey is the cache's key for a table: its database ID and name.
func cacheKey(dbID uint64, name string) string { return strconv.FormatUint(dbID, 10) + "/" + name }

func splitCacheKey(key string) (uint64, string) {
	i := strings.IndexByte(key, '/')
	if i < 0 {
		return 0, key
	}
	id, _ := strconv.ParseUint(key[:i], 10, 64)
	return id, key[i+1:]
}

// databaseID resolves a database name (the default database is ID 0
// until the v6 migration creates its descriptor).
func (a *Accessor) databaseID(ctx context.Context, txn *kvclient.Txn, db string) (uint64, error) {
	if db == "" {
		db = DefaultDatabase
	}
	a.mu.Lock()
	id, ok := a.dbs[db]
	a.mu.Unlock()
	if ok && (id != 0 || db != DefaultDatabase) {
		return id, nil
	}
	d, err := LookupDatabase(ctx, txn, db)
	if err != nil {
		return 0, err
	}
	a.mu.Lock()
	if a.dbs == nil {
		a.dbs = map[string]uint64{}
	}
	a.dbs[db] = d.ID
	a.mu.Unlock()
	return d.ID, nil
}

// Database resolves a database descriptor by name.
func (a *Accessor) Database(ctx context.Context, txn *kvclient.Txn, db string) (*DatabaseDescriptor, error) {
	if db == "" {
		db = DefaultDatabase
	}
	return LookupDatabase(ctx, txn, db)
}

func (a *Accessor) invalidateDatabase(name string) {
	a.mu.Lock()
	delete(a.dbs, name)
	a.mu.Unlock()
}

// InvalidateAll drops every cached descriptor (database rename: the
// names' owners changed underneath the cache keys).
func (a *Accessor) InvalidateAll() {
	a.mu.Lock()
	a.cache = map[string]*cachedDesc{}
	a.dbs = nil
	a.mu.Unlock()
}

// nowWallLocked is the wall clock the lease records are stamped with
// (the node's HLC when leasing; the process clock otherwise). Callers
// hold a.mu or have no leasing state to race with.
func (a *Accessor) nowWallLocked() int64 {
	if a.clock != nil {
		return a.clock.Now().WallTime
	}
	return time.Now().UnixNano()
}

// pinDeadline bounds txn's commit by a lease expiration: a statement
// planned under a lease must commit before the lease ends, or a DDL
// drain that wrote the lease off could take its backfill boundary before
// the statement's writes (issue #110). The server commits at the write
// timestamp the client sends, so the check lives in the client.
func pinDeadline(txn *kvclient.Txn, expiration int64) {
	if txn == nil || expiration == 0 {
		return
	}
	txn.UpdateDeadline(hlc.Timestamp{WallTime: expiration})
}

// LookupFresh resolves a table by name within txn, bypassing the cache
// and taking no lease: callers that must see the current COMMITTED
// descriptor (the re-shard historical-read guard) rather than a leased
// snapshot.
func (a *Accessor) LookupFresh(ctx context.Context, txn *kvclient.Txn, name string) (*TableDescriptor, error) {
	return a.LookupFreshIn(ctx, txn, DefaultDatabase, name)
}

// LookupFreshIn is LookupFresh within a database (or name's qualifier).
func (a *Accessor) LookupFreshIn(ctx context.Context, txn *kvclient.Txn, db, name string) (*TableDescriptor, error) {
	if q, bare := SplitTableName(name); q != "" {
		db, name = q, bare
	}
	dbID, err := a.databaseID(ctx, txn, db)
	if err != nil {
		return nil, err
	}
	return lookupUncached(ctx, txn, dbID, name, isDefaultDatabase(db))
}

// isDefaultID reports whether dbID is the default database's (0 before
// the migration; its allocated ID after, once this accessor has seen it).
func (a *Accessor) isDefaultID(dbID uint64) bool {
	if dbID == 0 {
		return true
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.dbs[DefaultDatabase] == dbID
}

// DatabaseID resolves a database name to its ID (0 = the default
// database before the v6 migration).
func (a *Accessor) DatabaseID(ctx context.Context, txn *kvclient.Txn, db string) (uint64, error) {
	return a.databaseID(ctx, txn, db)
}

// Invalidate drops a cached entry (after DDL or a stale-descriptor error).
func (a *Accessor) Invalidate(name string) {
	_, bare := SplitTableName(name)
	a.mu.Lock()
	for k := range a.cache {
		if _, n := splitCacheKey(k); n == bare {
			delete(a.cache, k)
		}
	}
	a.mu.Unlock()
}

// namespaceLookup finds a table's ID in database dbID: the v6 layout
// first, then — for the default database only — the flat pre-v6 layout,
// where tables created before finalize still live (and where a
// historical read below the migration's timestamp finds every table).
func namespaceLookup(ctx context.Context, txn *kvclient.Txn, dbID uint64, name string, isDefault bool) ([]byte, error) {
	if dbID != 0 {
		idRaw, err := txn.Get(ctx, keys.TableNamespaceKey(dbID, name))
		if err != nil || idRaw != nil {
			return idRaw, err
		}
		if !isDefault {
			return nil, nil
		}
	}
	return txn.Get(ctx, keys.NamespaceKey(name))
}

// isDefaultDatabase reports whether a database name is the default
// database (whose pre-migration tables sit in the flat namespace).
func isDefaultDatabase(db string) bool { return db == "" || db == DefaultDatabase }

func lookupUncached(ctx context.Context, txn *kvclient.Txn, dbID uint64, name string, isDefault bool) (*TableDescriptor, error) {
	idRaw, err := namespaceLookup(ctx, txn, dbID, name, isDefault)
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

// ReadTable reads a table descriptor by ID, uncached (nil, nil when
// there is none).
func ReadTable(ctx context.Context, txn *kvclient.Txn, id uint64) (*TableDescriptor, error) {
	raw, err := txn.Get(ctx, keys.TableDescKey(id))
	if err != nil || raw == nil {
		return nil, err
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
	existing, err := namespaceLookup(ctx, txn, d.DatabaseID, d.Name, a.isDefaultID(d.DatabaseID))
	if err != nil {
		return err
	}
	if existing != nil {
		return &ErrTableExists{Name: d.Name}
	}
	if d.ID == 0 {
		id, err := txn.Increment(ctx, keys.DescIDGenKey(), 1)
		if err != nil {
			return err
		}
		d.ID = uint64(id)
	} else if !IsSystemTableID(d.ID) {
		return fmt.Errorf("table %q: only system tables carry a preset ID", d.Name)
	}
	d.Version = 1
	raw, err := json.Marshal(d)
	if err != nil {
		return err
	}
	if err := txn.Put(ctx, keys.TableDescKey(d.ID), raw); err != nil {
		return err
	}
	if err := txn.Put(ctx, namespaceKey(d.DatabaseID, d.Name), []byte(strconv.FormatUint(d.ID, 10))); err != nil {
		return err
	}
	a.Invalidate(d.Name)
	return nil
}

// namespaceKey is where a table's name entry lives: the v6 layout under
// its database, or the flat layout for a pre-migration table (ID 0).
func namespaceKey(dbID uint64, name string) keys.Key {
	if dbID == 0 {
		return keys.NamespaceKey(name)
	}
	return keys.TableNamespaceKey(dbID, name)
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

// RenameTable moves a table's name entry and rewrites its descriptor
// under the new name (bumping the version, like Update). The ID is
// unchanged, so every reference by ID — foreign keys, owned sequences,
// gateway leases — still holds; a gateway caching the old name drops
// that entry at its next renewal (the old name resolves to nothing).
func (a *Accessor) RenameTable(ctx context.Context, txn *kvclient.Txn, d *TableDescriptor, newName string) error {
	existing, err := namespaceLookup(ctx, txn, d.DatabaseID, newName, a.isDefaultID(d.DatabaseID))
	if err != nil {
		return err
	}
	if existing != nil {
		return &ErrTableExists{Name: newName}
	}
	oldName := d.Name
	if err := txn.Delete(ctx, namespaceKey(d.DatabaseID, oldName)); err != nil {
		return err
	}
	if err := txn.Put(ctx, namespaceKey(d.DatabaseID, newName), []byte(strconv.FormatUint(d.ID, 10))); err != nil {
		return err
	}
	d.Name = newName
	if err := a.Update(ctx, txn, d); err != nil {
		return err
	}
	a.Invalidate(oldName)
	return nil
}

// Drop removes a table's descriptor and namespace entry. Row data is left
// behind (unreachable; space reclamation is a GC concern, out of scope).
func (a *Accessor) Drop(ctx context.Context, txn *kvclient.Txn, name string) (*TableDescriptor, error) {
	return a.DropIn(ctx, txn, DefaultDatabase, name)
}

// DropIn drops a table by name in database db (or the qualifier in name).
func (a *Accessor) DropIn(ctx context.Context, txn *kvclient.Txn, db, name string) (*TableDescriptor, error) {
	if q, bare := SplitTableName(name); q != "" {
		db, name = q, bare
	}
	dbID, err := a.databaseID(ctx, txn, db)
	if err != nil {
		return nil, err
	}
	d, err := lookupUncached(ctx, txn, dbID, name, isDefaultDatabase(db))
	if err != nil {
		return nil, err
	}
	// The name entry is wherever the descriptor says it is.
	if err := txn.Delete(ctx, namespaceKey(d.DatabaseID, name)); err != nil {
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
