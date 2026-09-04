// Package vtable defines the virtual catalog tables (pg_catalog and
// information_schema) that psql, drivers and ORMs introspect (issue #89).
// Each is a column list plus a generator that produces rows from the
// real catalog at the statement's read timestamp, under the session's
// privileges: a non-admin sees the tables it can read, as PostgreSQL
// shows what is visible. They are read-only and never indexed: the
// executor treats one as a full scan with a fixed row estimate.
//
// OIDs are derived from IDs so they are stable across nodes and
// restarts: a table's pg_class OID is its table ID, an index's is
// table ID × 65536 + index ID, a database's is its database ID, and the
// ten types carry PostgreSQL's own OIDs so drivers' type maps work.
package vtable

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/sthorne/datax/pkg/sql/catalog"
	"github.com/sthorne/datax/pkg/sql/types"
)

// Schemas the virtual tables live in.
const (
	PgCatalog         = "pg_catalog"
	InformationSchema = "information_schema"
)

// Fixed namespace (schema) OIDs, PostgreSQL's own for the two builtin
// schemas.
const (
	OIDPgCatalog         = 11
	OIDPublic            = 2200
	OIDInformationSchema = 12
)

// Env is what a generator may consult: the catalog as of the statement,
// and the session's identity.
type Env struct {
	Databases []*catalog.DatabaseDescriptor
	// Tables holds every table the session may see, in every database.
	Tables []*catalog.TableDescriptor
	// Stats maps table ID → statistics (nil when uncollected).
	Stats map[uint64]*catalog.TableStatistics
	Users []string // every SQL user, root first
	// Admins is the set of admin-role members (root is implicit).
	Admins map[string]bool
	// Settings are the session variables (name → value).
	Settings [][2]string

	User     string
	Database string // the session's current database
	IsAdmin  bool
}

// Row is one virtual row, by column position.
type Row []types.Datum

// Table is one virtual table.
type Table struct {
	Schema  string
	Name    string
	Columns []catalog.Column
	Rows    func(ctx context.Context, env *Env) ([]Row, error)
}

// Descriptor renders the table as a catalog descriptor the planner and
// executor understand: a table with a primary key on a hidden ordinal
// column (every real table has one; nothing ever encodes it).
func (t *Table) Descriptor() *catalog.TableDescriptor {
	cols := make([]catalog.Column, 0, len(t.Columns)+1)
	for i, c := range t.Columns {
		c.ID = catalog.ColumnID(i + 1)
		cols = append(cols, c)
	}
	ord := catalog.Column{ID: catalog.ColumnID(len(cols) + 1), Name: "_ord", Type: types.Int, NotNull: true, Hidden: true}
	cols = append(cols, ord)
	return &catalog.TableDescriptor{
		ID:         VirtualTableID,
		Name:       t.Name,
		Columns:    cols,
		PrimaryKey: []catalog.ColumnID{ord.ID},
		Virtual:    t.Schema + "." + t.Name,
	}
}

// VirtualTableID is the descriptor ID every virtual table reports; it
// never appears in the key space.
const VirtualTableID uint64 = 1<<40 + 1

// Lookup resolves a table name — bare, or schema-qualified as
// pg_catalog.x / information_schema.x — to a virtual table. Bare names
// resolve only to pg_catalog tables (PostgreSQL's search path has
// pg_catalog first, information_schema is always qualified).
func Lookup(name string) (*Table, bool) {
	schema, bare := "", name
	if i := strings.IndexByte(name, '.'); i > 0 {
		schema, bare = name[:i], name[i+1:]
	}
	switch schema {
	case "":
		t, ok := pgCatalogTables[bare]
		return t, ok
	case PgCatalog:
		t, ok := pgCatalogTables[bare]
		return t, ok
	case InformationSchema:
		t, ok := informationSchemaTables[bare]
		return t, ok
	}
	return nil, false
}

// IsSchema reports whether name is a virtual schema (so "pg_catalog.x"
// is a virtual table, not a table in a database called pg_catalog).
func IsSchema(name string) bool { return name == PgCatalog || name == InformationSchema }

// Names lists every virtual table as schema.name, sorted (SHOW and docs).
func Names() []string {
	var out []string
	for n := range pgCatalogTables {
		out = append(out, PgCatalog+"."+n)
	}
	for n := range informationSchemaTables {
		out = append(out, InformationSchema+"."+n)
	}
	sort.Strings(out)
	return out
}

// OIDs.

// TableOID is a table's pg_class OID.
func TableOID(d *catalog.TableDescriptor) int64 { return int64(d.ID) }

// IndexOID is an index's pg_class OID (the primary index is index 1).
func IndexOID(d *catalog.TableDescriptor, indexID uint64) int64 {
	return int64(d.ID)<<16 | int64(indexID)
}

// DatabaseOID is a database's pg_database OID (the default database
// before the v6 migration has ID 0; it reports 1).
func DatabaseOID(d *catalog.DatabaseDescriptor) int64 {
	if d.ID == 0 {
		return 1
	}
	return int64(d.ID)
}

// TypeOID is PostgreSQL's OID for a datax type family.
func TypeOID(f types.Family) int64 {
	switch f {
	case types.Bool:
		return 16
	case types.Bytes:
		return 17
	case types.Int:
		return 20
	case types.String:
		return 25
	case types.Float:
		return 701
	case types.Date:
		return 1082
	case types.Timestamp:
		return 1184
	case types.Uuid:
		return 2950
	case types.Decimal:
		return 1700
	case types.Jsonb:
		return 3802
	}
	return 25
}

// TypeName is PostgreSQL's name for a datax type family (pg_type.typname).
func TypeName(f types.Family) string {
	switch f {
	case types.Bool:
		return "bool"
	case types.Bytes:
		return "bytea"
	case types.Int:
		return "int8"
	case types.String:
		return "text"
	case types.Float:
		return "float8"
	case types.Date:
		return "date"
	case types.Timestamp:
		return "timestamptz"
	case types.Uuid:
		return "uuid"
	case types.Decimal:
		return "numeric"
	case types.Jsonb:
		return "jsonb"
	}
	return "text"
}

// FormatType is format_type(): the SQL-standard spelling of a column's
// type, with its typmod (numeric(p,s)).
func FormatType(c *catalog.Column) string {
	switch c.Type {
	case types.Bool:
		return "boolean"
	case types.Int:
		return "bigint"
	case types.Float:
		return "double precision"
	case types.Timestamp:
		return "timestamp with time zone"
	case types.Decimal:
		if c.Precision > 0 {
			return fmt.Sprintf("numeric(%d,%d)", c.Precision, c.Scale)
		}
		return "numeric"
	}
	return TypeName(c.Type)
}

// FormatTypeOID is format_type(oid, typmod) for a bare type OID.
func FormatTypeOID(oid int64) string {
	for _, f := range []types.Family{types.Bool, types.Bytes, types.Int, types.String, types.Float, types.Date, types.Timestamp, types.Uuid, types.Decimal, types.Jsonb} {
		if TypeOID(f) == oid {
			return FormatType(&catalog.Column{Type: f})
		}
	}
	return "-"
}

// IndexDef renders pg_get_indexdef() for an index.
func IndexDef(d *catalog.TableDescriptor, idx *catalog.IndexDescriptor) string {
	cols := make([]string, 0, len(idx.ColumnIDs))
	for _, id := range idx.ColumnIDs {
		cols = append(cols, columnName(d, id))
	}
	unique := ""
	if idx.Unique {
		unique = "UNIQUE "
	}
	return fmt.Sprintf("CREATE %sINDEX %s ON public.%s USING btree (%s)", unique, idx.Name, d.Name, strings.Join(cols, ", "))
}

// PrimaryKeyDef renders pg_get_constraintdef() for the primary key.
func PrimaryKeyDef(d *catalog.TableDescriptor) string {
	cols := make([]string, 0, len(d.PrimaryKey))
	for _, id := range d.PrimaryKey {
		if c, ok := columnByID(d, id); ok && c.Hidden {
			continue
		}
		cols = append(cols, columnName(d, id))
	}
	return "PRIMARY KEY (" + strings.Join(cols, ", ") + ")"
}

// UniqueDef renders pg_get_constraintdef() for a unique index.
func UniqueDef(d *catalog.TableDescriptor, idx *catalog.IndexDescriptor) string {
	cols := make([]string, 0, len(idx.ColumnIDs))
	for _, id := range idx.ColumnIDs {
		cols = append(cols, columnName(d, id))
	}
	return "UNIQUE (" + strings.Join(cols, ", ") + ")"
}

func columnByID(d *catalog.TableDescriptor, id catalog.ColumnID) (*catalog.Column, bool) {
	for i := range d.Columns {
		if d.Columns[i].ID == id {
			return &d.Columns[i], true
		}
	}
	return nil, false
}

func columnName(d *catalog.TableDescriptor, id catalog.ColumnID) string {
	if c, ok := columnByID(d, id); ok {
		return c.Name
	}
	return fmt.Sprintf("col%d", id)
}

// CreateTableDef renders SHOW CREATE TABLE.
func CreateTableDef(d *catalog.TableDescriptor) string {
	var b strings.Builder
	fmt.Fprintf(&b, "CREATE TABLE %s (\n", d.Name)
	for _, c := range d.Columns {
		if c.Hidden {
			continue
		}
		fmt.Fprintf(&b, "  %s %s", c.Name, strings.ToUpper(TypeName(c.Type)))
		if c.Type == types.Decimal && c.Precision > 0 {
			fmt.Fprintf(&b, "(%d,%d)", c.Precision, c.Scale)
		}
		if c.NotNull {
			b.WriteString(" NOT NULL")
		}
		if c.Default != nil {
			fmt.Fprintf(&b, " DEFAULT %s", renderDefault(*c.Default))
		}
		b.WriteString(",\n")
	}
	fmt.Fprintf(&b, "  %s", PrimaryKeyDef(d))
	for _, idx := range d.Indexes {
		unique := ""
		if idx.Unique {
			unique = "UNIQUE "
		}
		cols := make([]string, 0, len(idx.ColumnIDs))
		for _, id := range idx.ColumnIDs {
			cols = append(cols, columnName(d, id))
		}
		fmt.Fprintf(&b, ",\n  %sINDEX %s (%s)", unique, idx.Name, strings.Join(cols, ", "))
	}
	b.WriteString("\n)")
	if d.Timeseries {
		fmt.Fprintf(&b, " WITH (timeseries = true")
		if d.RetentionSeconds > 0 {
			fmt.Fprintf(&b, ", retention = '%ds'", d.RetentionSeconds)
		}
		if d.ShardBuckets > 0 {
			fmt.Fprintf(&b, ", shards = %d", d.ShardBuckets)
		}
		b.WriteString(")")
	}
	return b.String()
}

func renderDefault(d types.Datum) string {
	if d.Null {
		return "NULL"
	}
	switch d.Fam {
	case types.String, types.Bytes, types.Uuid, types.Jsonb, types.Decimal:
		return "'" + strings.ReplaceAll(d.Text(), "'", "''") + "'"
	}
	return d.Text()
}

// Datum helpers for generators.
func str(s string) types.Datum   { return types.NewString(s) }
func i64(v int64) types.Datum    { return types.NewInt(v) }
func boolean(b bool) types.Datum { return types.NewBool(b) }
func null() types.Datum          { return types.DNull }

// col declares a column.
func col(name string, fam types.Family) catalog.Column { return catalog.Column{Name: name, Type: fam} }

// HiddenColumnFor names the hidden column that carries a catalog
// function's rendering on the rows it applies to: format_type on
// pg_attribute, pg_get_indexdef on pg_index, pg_get_constraintdef on
// pg_constraint, pg_get_expr on pg_attrdef.
func HiddenColumnFor(fn string) string {
	switch fn {
	case "format_type":
		return "__format_type"
	case "pg_get_indexdef":
		return "__indexdef"
	case "pg_get_constraintdef":
		return "__condef"
	case "pg_get_expr":
		return "__expr"
	}
	return "__" + fn
}

func hidden(name string) catalog.Column {
	return catalog.Column{Name: name, Type: types.String, Hidden: true}
}
