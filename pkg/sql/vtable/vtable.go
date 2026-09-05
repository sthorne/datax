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
	"reflect"
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
	OIDPgCatalog = 11
	// OIDPgClass is pg_class's own OID: the classoid of relation, index
	// and column comments in pg_description.
	OIDPgClass           = 1259
	OIDPublic            = 2200
	OIDInformationSchema = 12
)

// Env is what a generator may consult: the catalog as of the statement,
// and the session's identity.
type Env struct {
	Databases []*catalog.DatabaseDescriptor
	// Tables holds every table the session may see, in every database.
	Tables []*catalog.TableDescriptor
	// Sequences holds every sequence, in every database.
	Sequences []*catalog.SequenceDescriptor
	// Types holds every user-defined type (enum), in every database.
	Types []*catalog.TypeDescriptor
	// SequenceValue reads a sequence's last value (called reports whether
	// nextval has run); nil when the caller cannot read counters.
	SequenceValue func(*catalog.SequenceDescriptor) (value int64, called bool, err error)
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

// IsAlwaysEmpty reports whether name is a catalog that never has rows
// (the stand-in for a feature datax lacks): a select over it can answer
// empty without planning whatever shape the tool's query takes.
func IsAlwaysEmpty(name string) bool {
	t, ok := Lookup(name)
	if !ok {
		return false
	}
	return reflect.ValueOf(t.Rows).Pointer() == reflect.ValueOf(empty).Pointer()
}

// EachTable calls fn for every virtual table, pg_catalog's first.
func EachTable(fn func(*Table)) {
	for _, t := range pgCatalogTables {
		fn(t)
	}
	for _, t := range informationSchemaTables {
		fn(t)
	}
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

// arrayTypeOIDs maps an element type's OID to its array type's.
var arrayTypeOIDs = map[int64]int64{
	16: 1000, 17: 1001, 21: 1005, 23: 1007, 20: 1016, 25: 1009, 1043: 1015, 1042: 1014, 701: 1022, 1700: 1231,
	1082: 1182, 1114: 1115, 1184: 1185, 2950: 2951, 3802: 3807, 1186: 1187, 1083: 1183,
}

// ArrayTypeOID is the OID of the array type over an element type's OID.
func ArrayTypeOID(elem int64) int64 {
	if oid, ok := arrayTypeOIDs[elem]; ok {
		return oid
	}
	return 1009
}

// TypeOID is PostgreSQL's OID for a datax type family.
func TypeOID(f types.Family) int64 {
	if f.IsArray() {
		return ArrayTypeOID(TypeOID(f.Elem()))
	}
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
	case types.IntervalFam:
		return 1186
	case types.Time:
		return 1083
	}
	return 25
}

// ColumnTypeOID is PostgreSQL's OID for a column's declared type: the
// family's, refined by the modifiers (int2 21 / int4 23, varchar 1043 /
// bpchar 1042, timestamp 1114).
func ColumnTypeOID(c *catalog.Column) int64 {
	if c.Type == types.Enum {
		return catalog.EnumOID(c.EnumType)
	}
	if c.Type.IsArray() {
		elem := *c
		elem.Type = c.Type.Elem()
		return ArrayTypeOID(ColumnTypeOID(&elem))
	}
	switch c.Type {
	case types.Int:
		switch c.Width {
		case 2:
			return 21
		case 4:
			return 23
		}
	case types.String:
		switch {
		case c.Char:
			return 1042
		case c.MaxLen > 0:
			return 1043
		}
	case types.Timestamp:
		if c.NoTZ {
			return 1114
		}
	}
	return TypeOID(c.Type)
}

// ColumnTypeName is pg_type.typname for a column's declared type
// (int2 / int4 / int8, varchar / bpchar / text, timestamp /
// timestamptz).
func ColumnTypeName(c *catalog.Column) string {
	if c.Type == types.Enum {
		return c.EnumName
	}
	return typeNameOID(ColumnTypeOID(c))
}

// ColumnTypeLen is pg_type.typlen for a column's declared type.
func ColumnTypeLen(c *catalog.Column) int64 {
	if c.Type.IsArray() || c.Type == types.Enum {
		return -1
	}
	switch ColumnTypeOID(c) {
	case 16:
		return 1
	case 21:
		return 2
	case 23, 1082:
		return 4
	case 20, 701, 1114, 1184, 1083:
		return 8
	case 1186:
		return 16
	case 2950:
		return 16
	}
	return -1
}

// ExtraTypeOIDs are the pg_type rows beyond the ten families: the
// modifier-refined types a column can declare.
var ExtraTypeOIDs = []int64{21, 23, 1043, 1042, 1114}

func typeNameOID(oid int64) string {
	for elem, arr := range arrayTypeOIDs {
		if arr == oid {
			return "_" + typeNameOID(elem)
		}
	}
	switch oid {
	case 21:
		return "int2"
	case 23:
		return "int4"
	case 1043:
		return "varchar"
	case 1042:
		return "bpchar"
	case 1114:
		return "timestamp"
	}
	for _, f := range families {
		if TypeOID(f) == oid {
			return TypeName(f)
		}
	}
	return "text"
}

var families = []types.Family{types.Bool, types.Bytes, types.Int, types.String, types.Float, types.Date, types.Timestamp, types.Uuid, types.Decimal, types.Jsonb, types.IntervalFam, types.Time}

// Families lists every type family, in pg_type order.
func Families() []types.Family { return families }

// TypeName is PostgreSQL's name for a datax type family (pg_type.typname).
func TypeName(f types.Family) string {
	if f.IsArray() {
		return "_" + TypeName(f.Elem())
	}
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
	case types.IntervalFam:
		return "interval"
	case types.Time:
		return "time"
	}
	return "text"
}

// FormatType is format_type(): the SQL-standard spelling of a column's
// type, with its typmod (numeric(p,s)).
func FormatType(c *catalog.Column) string {
	if c.Type == types.Enum {
		return c.EnumName
	}
	if c.Type.IsArray() {
		elem := *c
		elem.Type = c.Type.Elem()
		return FormatType(&elem) + "[]"
	}
	switch c.Type {
	case types.Bool:
		return "boolean"
	case types.Int:
		switch c.Width {
		case 2:
			return "smallint"
		case 4:
			return "integer"
		}
		return "bigint"
	case types.String:
		switch {
		case c.Char:
			return fmt.Sprintf("character(%d)", c.MaxLen)
		case c.MaxLen > 0:
			return fmt.Sprintf("character varying(%d)", c.MaxLen)
		}
		return "text"
	case types.Float:
		return "double precision"
	case types.Timestamp:
		p := ""
		if fd, ok := c.FracDigits(); ok {
			p = fmt.Sprintf("(%d)", fd)
		}
		if c.NoTZ {
			return "timestamp" + p + " without time zone"
		}
		return "timestamp" + p + " with time zone"
	case types.Time:
		if p, ok := c.FracDigits(); ok {
			return fmt.Sprintf("time(%d) without time zone", p)
		}
		return "time without time zone"
	case types.Decimal:
		if c.Precision > 0 {
			return fmt.Sprintf("numeric(%d,%d)", c.Precision, c.Scale)
		}
		return "numeric"
	}
	return TypeName(c.Type)
}

// ColumnTypeSQL is the column's type as SHOW CREATE TABLE spells it:
// the declared modifiers (INT4, VARCHAR(20), CHAR(4), TIMESTAMP(3),
// NUMERIC(10,2)) over the pg_type name.
func ColumnTypeSQL(c *catalog.Column) string {
	if c.Type == types.Enum {
		return c.EnumName
	}
	if c.Type.IsArray() {
		elem := *c
		elem.Type = c.Type.Elem()
		return ColumnTypeSQL(&elem) + "[]"
	}
	switch c.Type {
	case types.Int, types.String, types.Timestamp, types.Time:
		return c.TypeSQL()
	case types.Decimal:
		if c.Precision > 0 {
			return fmt.Sprintf("NUMERIC(%d,%d)", c.Precision, c.Scale)
		}
	}
	return strings.ToUpper(TypeName(c.Type))
}

// FormatTypeOID is format_type(oid, typmod) for a bare type OID.
func FormatTypeOID(oid int64) string {
	for elem, arr := range arrayTypeOIDs {
		if arr == oid {
			return FormatTypeOID(elem) + "[]"
		}
	}
	switch oid {
	case 21:
		return "smallint"
	case 23:
		return "integer"
	case 1043:
		return "character varying"
	case 1042:
		return "character"
	case 1114:
		return "timestamp without time zone"
	}
	for _, f := range families {
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

// ConstraintOID is a constraint's pg_constraint OID (a bit above the
// index OIDs of the same table, so the two never collide).
func ConstraintOID(d *catalog.TableDescriptor, c *catalog.Constraint) int64 {
	return int64(d.ID)<<16 | 1<<15 | int64(c.ID)
}

// ConstraintDef renders pg_get_constraintdef() for a CHECK, FOREIGN KEY
// or UNIQUE constraint. byID resolves a foreign key's referenced table
// (nil renders its ID).
func ConstraintDef(d *catalog.TableDescriptor, c *catalog.Constraint, byID func(uint64) *catalog.TableDescriptor) string {
	names := func(t *catalog.TableDescriptor, ids []catalog.ColumnID) string {
		cols := make([]string, 0, len(ids))
		for _, id := range ids {
			cols = append(cols, columnName(t, id))
		}
		return strings.Join(cols, ", ")
	}
	switch c.Kind {
	case catalog.ConstraintCheck:
		return "CHECK (" + c.Expr + ")"
	case catalog.ConstraintUnique:
		return "UNIQUE (" + names(d, c.Columns) + ")"
	case catalog.ConstraintForeign:
		ref := d
		if c.RefTable != d.ID {
			ref = nil
			if byID != nil {
				ref = byID(c.RefTable)
			}
		}
		refName, refCols := fmt.Sprintf("<%d>", c.RefTable), ""
		if ref != nil {
			refName, refCols = ref.Name, names(ref, c.RefColumns)
		}
		def := "FOREIGN KEY (" + names(d, c.Columns) + ") REFERENCES " + refName + "(" + refCols + ")"
		if c.OnUpdate != "" && c.OnUpdate != catalog.FKRestrict {
			def += " ON UPDATE " + strings.ToUpper(c.OnUpdate)
		}
		if c.OnDelete != "" && c.OnDelete != catalog.FKRestrict {
			def += " ON DELETE " + strings.ToUpper(c.OnDelete)
		}
		if !c.Validated {
			def += " NOT VALID"
		}
		return def
	}
	return ""
}

// tableByID finds a table among those the session sees.
func (env *Env) tableByID(id uint64) *catalog.TableDescriptor {
	for _, t := range env.Tables {
		if t.ID == id {
			return t
		}
	}
	return nil
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
	return CreateTableDefWith(d, nil)
}

// CreateTableDefWith is CreateTableDef with a resolver for the tables
// the table's foreign keys reference.
func CreateTableDefWith(d *catalog.TableDescriptor, byID func(uint64) *catalog.TableDescriptor) string {
	var b strings.Builder
	fmt.Fprintf(&b, "CREATE TABLE %s (\n", d.Name)
	for _, c := range d.Columns {
		if c.Hidden {
			continue
		}
		fmt.Fprintf(&b, "  %s %s", c.Name, ColumnTypeSQL(&c))
		if c.NotNull {
			b.WriteString(" NOT NULL")
		}
		if c.Identity != "" {
			fmt.Fprintf(&b, " GENERATED %s AS IDENTITY", strings.ToUpper(c.Identity))
		} else if def := ColumnDefault(&c); def != "" {
			fmt.Fprintf(&b, " DEFAULT %s", def)
		}
		b.WriteString(",\n")
	}
	// A primary key of hidden columns alone (CREATE TABLE AS's rowid)
	// is not written; the trailing comma of the last column goes with
	// it when nothing follows.
	var items []string
	if pk := PrimaryKeyDef(d); pk != "PRIMARY KEY ()" {
		items = append(items, pk)
	}
	owned := map[uint64]bool{} // indexes a constraint owns render as the constraint
	for i := range d.Constraints {
		if c := &d.Constraints[i]; c.Kind == catalog.ConstraintUnique || c.AutoIndex {
			owned[c.IndexID] = true
		}
	}
	for _, idx := range d.Indexes {
		if owned[idx.ID] {
			continue
		}
		unique := ""
		if idx.Unique {
			unique = "UNIQUE "
		}
		cols := make([]string, 0, len(idx.ColumnIDs))
		for _, id := range idx.ColumnIDs {
			cols = append(cols, columnName(d, id))
		}
		items = append(items, fmt.Sprintf("%sINDEX %s (%s)", unique, idx.Name, strings.Join(cols, ", ")))
	}
	for i := range d.Constraints {
		c := &d.Constraints[i]
		items = append(items, fmt.Sprintf("CONSTRAINT %s %s", c.Name, ConstraintDef(d, c, byID)))
	}
	if len(items) > 0 {
		fmt.Fprintf(&b, "  %s", strings.Join(items, ",\n  "))
	} else {
		text := strings.TrimSuffix(b.String(), ",\n")
		b.Reset()
		b.WriteString(text)
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

// ColumnDefault renders a column's default as the catalogs show it:
// the expression text (nextval('t_id_seq') for an owned sequence), or
// the constant as a literal; "" when the column has none.
func ColumnDefault(c *catalog.Column) string {
	switch {
	case c.DefaultExpr == "NULL":
		// DROP DEFAULT on a fill-on-read column: the constant stays as
		// the fill value, the expression says inserts get NULL.
		return ""
	case c.DefaultExpr != "":
		return c.DefaultExpr
	case c.Default != nil:
		return renderDefault(*c.Default)
	}
	return ""
}

func renderDefault(d types.Datum) string {
	if d.Null {
		return "NULL"
	}
	switch d.Fam {
	case types.String, types.Bytes, types.Uuid, types.Jsonb, types.Decimal, types.Enum, types.Timestamp, types.Date, types.Time, types.IntervalFam:
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
// pg_constraint, pg_get_expr on pg_attrdef, pg_get_viewdef on pg_class.
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
	case "pg_get_viewdef":
		return "__viewdef"
	case "obj_description":
		return "__obj_description"
	case "col_description":
		return "__col_description"
	}
	return "__" + fn
}

func hidden(name string) catalog.Column {
	return catalog.Column{Name: name, Type: types.String, Hidden: true}
}
