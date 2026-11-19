// Package vtable defines the virtual catalog tables (pg_catalog and
// information_schema) that psql, drivers and ORMs introspect (issue #89).
// Each is a column list plus a generator that produces rows from the
// real catalog at the statement's read timestamp. Every role may read
// every one of them, as in PostgreSQL (issue #312), and they list what
// PostgreSQL's list: pg_catalog every table, whoever asks;
// information_schema the tables the reader holds some privilege on.
// What would reveal row data — statistics, a sequence's counter,
// another role's statement text — is shown only to a role that may
// read it, and no catalog carries a password or verifier. They are read-only and never indexed: the
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
	"hash/fnv"
	"reflect"
	"sort"
	"strings"
	"time"

	"github.com/sthorne/datax/pkg/sql/builtins"
	"github.com/sthorne/datax/pkg/sql/catalog"
	"github.com/sthorne/datax/pkg/sql/inverted"
	"github.com/sthorne/datax/pkg/sql/types"
	"github.com/sthorne/datax/pkg/sql/vector"
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
	OIDPgClass = 1259
	// OIDPgConstraint is pg_constraint's own OID: the classoid of a
	// constraint comment.
	OIDPgConstraint      = 2606
	OIDPublic            = 2200
	OIDInformationSchema = 12
)

// schemaOIDBase is where the OIDs of user schemas start: a schema's
// pg_namespace OID is the base plus its ID (IDs come from the shared
// descriptor counter, so no two schemas share one).
const schemaOIDBase = 16384

// SchemaOID is the pg_namespace OID of user schema id (0 = public).
func SchemaOID(id uint64) int64 {
	if id == 0 {
		return OIDPublic
	}
	return schemaOIDBase + int64(id)
}

// SchemaIDOfOID inverts SchemaOID for a user schema.
func SchemaIDOfOID(oid int64) (uint64, bool) {
	if oid <= schemaOIDBase {
		return 0, false
	}
	return uint64(oid - schemaOIDBase), true
}

// schemaName is the name of schema id of the current database (public
// for 0).
func (env *Env) schemaName(id uint64) string {
	if id == 0 {
		return catalog.PublicSchema
	}
	for _, sd := range env.Schemas {
		if sd.ID == id {
			return sd.Name
		}
	}
	return fmt.Sprintf("pg_schema_%d", id)
}

// Env is what a generator may consult: the catalog as of the statement,
// and the session's identity.
type Env struct {
	Databases []*catalog.DatabaseDescriptor
	// Tables holds the tables of the current database the catalog lists
	// (every one for pg_catalog, those the reader holds a privilege on
	// for information_schema). Every virtual table describes the current
	// database only, as PostgreSQL's catalogs do (issue #322), so its Env
	// is built from that database alone and reading it costs the same
	// however many tables the rest of the cluster holds (issue #311).
	// Generators read it through currentTables.
	Tables []*catalog.TableDescriptor
	// ReadTable resolves a table that Tables does not hold — the
	// referenced table of a foreign key into another database — as the
	// session may see it (nil when it may not, or there is none).
	ReadTable func(id uint64) *catalog.TableDescriptor
	// Sequences holds the sequences, Types the user-defined types (enums)
	// of the current database (generators read them through
	// currentSequences and currentTypes).
	Sequences []*catalog.SequenceDescriptor
	Types     []*catalog.TypeDescriptor
	// TypeNames maps a user-defined type's ID to its name as
	// format_type prints it: qualified when the search path does not
	// resolve the bare name to it (nil: every name bare).
	TypeNames map[uint64]string
	// Schemas are the current database's schemas other than public
	// (gap PG-D-001), by name.
	Schemas []*catalog.SchemaDescriptor
	// SequenceValue reads a sequence's last value (called reports whether
	// nextval has run); nil when the caller cannot read counters.
	SequenceValue func(*catalog.SequenceDescriptor) (value int64, called bool, err error)
	// LargeObjects lists the large objects the session may open (every
	// one for an admin, its own otherwise); nil when the caller cannot
	// read them. A function, because most catalog queries never ask.
	LargeObjects func() ([]LargeObjectInfo, error)
	// AdvisoryLocks lists the advisory locks held across the cluster
	// (pg_locks); nil when the session has none to list.
	AdvisoryLocks func() ([]AdvisoryLockInfo, error)
	// Keys lists the named keys of the key service (issue #310) as the
	// datax_keys virtual table shows them: metadata only, for admins;
	// nil when the caller cannot read them.
	Keys func() ([]KeyInfo, error)
	// SealJobs lists the seal jobs in progress (issue #310, step 5) as
	// datax_encryption_jobs shows them; nil when the caller cannot read
	// them.
	SealJobs func() ([]SealJobInfo, error)
	// Stats maps table ID → statistics (nil when uncollected).
	Stats map[uint64]*catalog.TableStatistics
	// Roles holds every role (catalog.ListRoles: root first, then by
	// name, the built-in roles included).
	Roles []*catalog.RoleDescriptor
	// Settings are the session variables (name → value).
	Settings [][2]string
	// SettingDefaults maps a setting's name to the value a new session
	// has (pg_settings' boot_val; a setting whose value differs reports
	// source 'session'). A setting it lacks is a custom one the session
	// set (app.tenant).
	SettingDefaults map[string]string
	// SettingTypes maps a built-in setting's name to its pg_settings
	// vartype and unit ("" for none), from the session's registry of
	// settings (issue #333).
	SettingTypes map[string][2]string
	// SettingRaw renders a setting's SHOW value as pg_settings reports
	// it, in the setting's base unit (work_mem's 4MB is 4096); nil
	// leaves every value as SHOW has it.
	SettingRaw func(name, shown string) string
	// SettingContexts maps a built-in setting whose pg_settings context
	// is not user to it (internal, superuser).
	SettingContexts map[string]string
	// Sessions are this node's SQL sessions (pg_stat_activity).
	Sessions []SessionInfo

	// RoleSet is the reader's effective roles (nil: unknown, treated as
	// an admin's by the few views that filter by it).
	RoleSet catalog.RoleSet

	// Now is the statement's time (Unix nanoseconds; 0 = the clock's):
	// the instant pg_timezone_names describes each zone at.
	Now int64

	// graph and effective cache role membership for roleHasRole.
	graph     *catalog.RoleGraph
	effective map[string]catalog.RoleSet

	User        string // the current role (current_user)
	SessionUser string // the authenticated user (session_user)
	Database    string // the session's current database
	IsAdmin     bool
}

// procOIDBase is where pg_proc's OIDs start: a builtin's OID is the
// base plus its position in builtins.All().
const procOIDBase = int64(1 << 28)

// ProcName resolves a pg_proc OID back to the function it was listed
// for (the fast-path protocol names a function by the OID a client read
// from pg_proc).
func ProcName(oid int64) (string, bool) {
	all := builtins.All()
	if i := oid - procOIDBase; i >= 0 && i < int64(len(all)) {
		return all[i].Name, true
	}
	return "", false
}

// KeyInfo is one named key as the datax_keys virtual table lists it:
// everything but the key material, which no SQL session can read.
type KeyInfo struct {
	ID           int64
	Name         string
	Kind         string
	Version      int64
	Versions     int64
	State        string
	Generation   int64
	ClusterKey   string
	Created      int64 // Unix nanoseconds
	Rotated      int64 // Unix nanoseconds; 0 = never
	ExternalRoot string
	// Owner is the owning role; Privileges the grants on the key, as
	// grantee=PRIV,PRIV;... ("" when none).
	Owner      string
	Privileges string
}

// SealJobInfo is one seal job — the rewrite of a sealed table's rows
// after it was sealed or its key rotated — as datax_encryption_jobs
// lists it.
type SealJobInfo struct {
	TableID uint64
	Table   string
	Kind    string // seal | rotate
	Tenant  bool   // the rotation of one tenant's key
	KeyID   int64
	Version int
	KeepOld bool
	State   string
	Pass    int
	// Rewritten counts every row the job rewrote; Unreadable the rows of
	// the current pass it stepped over because their key is gone.
	Rewritten, Unreadable int64
	Created, Updated      int64 // Unix nanoseconds; Updated 0 = never
	LastError             string
}

// LargeObjectInfo is one large object as pg_largeobject_metadata lists
// it.
type LargeObjectInfo struct {
	OID   int64
	Owner string
}

// AdvisoryLockInfo is one granted advisory lock as pg_locks lists it
// (issue #353): the lock's database and PostgreSQL's three numbers for
// it, the holding session's process ID and the mode.
type AdvisoryLockInfo struct {
	Database       string
	ClassID, ObjID uint32
	ObjSubID       uint16
	PID            int32
	Mode           string
}

// RoleOID is a role's stable catalog OID (pg_roles.oid, pg_auth_members,
// pg_class.relowner): a hash of the name in the range above every
// descriptor OID; root is 10 as in PostgreSQL.
func RoleOID(name string) int64 {
	if name == catalog.RootRole {
		return 10
	}
	h := fnv.New32a()
	_, _ = h.Write([]byte(name))
	return int64(h.Sum32()&0x3fffffff) | 0x40000000
}

// QueryHidden stands in for the statement text of another role's
// session when the reader is not an admin (PostgreSQL's wording).
const QueryHidden = "<insufficient privilege>"

// SessionInfo is one session of this node as SHOW SESSIONS and
// pg_stat_activity report it.
type SessionInfo struct {
	PID          int32
	User         string
	Database     string
	Application  string
	ClientAddr   string
	State        string // active | idle | idle in transaction
	Query        string
	BackendStart time.Time
	QueryStart   time.Time
	XactStart    time.Time
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
func TableOID(d *catalog.TableDescriptor) int64 { return relOID(d.ID) }

// SystemTableOIDBase is where the cluster's own tables' OIDs start. Their
// descriptor IDs (catalog.MetricsTableID and up, above 2^40) are no OID
// a PostgreSQL client can hold — libpq reads an OID as 32 bits, and
// pg_dump took datax_largeobject (2^40 + 5) for the table whose OID is 5
// — so each takes one of the 256 OIDs from here instead, below the
// 65536 the index OIDs are scaled by.
const SystemTableOIDBase = 0xFF00

// relOID is the pg_class OID of the table with descriptor ID id.
func relOID(id uint64) int64 {
	if id >= catalog.MetricsTableID && id < catalog.MetricsTableID+256 {
		return SystemTableOIDBase + int64(id-catalog.MetricsTableID)
	}
	return int64(id)
}

// TableIDOfOID inverts TableOID: the descriptor ID a table OID stands for.
func TableIDOfOID(oid int64) uint64 {
	if oid >= SystemTableOIDBase && oid < SystemTableOIDBase+256 {
		return catalog.MetricsTableID + uint64(oid-SystemTableOIDBase)
	}
	return uint64(oid)
}

// IndexOID is an index's pg_class OID (the primary index is index 1).
func IndexOID(d *catalog.TableDescriptor, indexID uint64) int64 {
	return relOID(d.ID)<<16 | int64(indexID)
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
	case types.Tsvector:
		return 3614
	case types.Tsquery:
		return 3615
	case types.IntervalFam:
		return 1186
	case types.Time:
		return 1083
	case types.Vector:
		return vector.OID
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

var families = []types.Family{types.Bool, types.Bytes, types.Int, types.String, types.Float, types.Date, types.Timestamp, types.Uuid, types.Decimal, types.Jsonb, types.IntervalFam, types.Time, types.Tsvector, types.Tsquery, types.Vector}

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
	case types.Tsvector:
		return "tsvector"
	case types.Tsquery:
		return "tsquery"
	case types.IntervalFam:
		return "interval"
	case types.Time:
		return "time"
	case types.Vector:
		return "vector"
	}
	return "text"
}

// formatType is FormatType with an enum type named as the session's
// search path sees it (env.TypeNames).
func (env *Env) formatType(c *catalog.Column) string {
	if c.Type == types.Enum {
		if n, ok := env.TypeNames[c.EnumType]; ok {
			return n
		}
	}
	return FormatType(c)
}

// typeName is how format_type prints user-defined type t.
func (env *Env) typeName(t *catalog.TypeDescriptor) string {
	if n, ok := env.TypeNames[t.ID]; ok {
		return n
	}
	return QuoteIdent(t.Name)
}

// FormatType is format_type(): the SQL-standard spelling of a column's
// type, with its typmod (numeric(p,s)).
func FormatType(c *catalog.Column) string {
	if c.Type == types.Enum {
		return QuoteIdent(c.EnumName)
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
	case types.Vector:
		if c.MaxLen > 0 {
			return fmt.Sprintf("vector(%d)", c.MaxLen)
		}
		return "vector"
	}
	return TypeName(c.Type)
}

// ColumnTypeSQL is the column's type as SHOW CREATE TABLE spells it:
// the declared modifiers (INT4, VARCHAR(20), CHAR(4), TIMESTAMP(3),
// NUMERIC(10,2)) over the pg_type name.
func ColumnTypeSQL(c *catalog.Column) string {
	if c.Type == types.Enum {
		return QuoteIdent(c.EnumName)
	}
	if c.Type.IsArray() {
		elem := *c
		elem.Type = c.Type.Elem()
		return ColumnTypeSQL(&elem) + "[]"
	}
	switch c.Type {
	case types.Int, types.String, types.Timestamp, types.Time, types.Vector:
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

// IndexKeyDef renders key column i of an index as pg_get_indexdef
// shows it: the column or expression, then a declared collation,
// operator class and order (name COLLATE "C" text_pattern_ops DESC).
func IndexKeyDef(d *catalog.TableDescriptor, idx *catalog.IndexDescriptor, i int) string {
	key := quotedColumnName(d, idx.ColumnIDs[i])
	if idx.IsExprKey(i) {
		key = ExprKeyText(idx.Exprs[i])
	}
	if len(idx.Collations) == len(idx.ColumnIDs) && idx.Collations[i] != "" {
		key += ` COLLATE "` + idx.Collations[i] + `"`
	}
	if len(idx.OpClasses) == len(idx.ColumnIDs) && idx.OpClasses[i] != "" {
		key += " " + idx.OpClasses[i]
	}
	if len(idx.Directions) == len(idx.ColumnIDs) && idx.Directions[i] != "" {
		key += " " + idx.Directions[i]
	}
	return key
}

// ExprKeyText renders an expression key as PostgreSQL prints one: a
// function call as it is, lower(email); anything else in parentheses,
// ((b + 1)).
func ExprKeyText(text string) string {
	for strings.HasPrefix(text, "(") && closingParen(text, 0) == len(text)-1 {
		text = text[1 : len(text)-1]
	}
	if isCallText(text) {
		return text
	}
	return "((" + text + "))"
}

// IndexExprName is the name of an expression key as an attribute of the
// index (pg_attribute.attname): the function's name for a call, "expr"
// otherwise, as PostgreSQL names it.
func IndexExprName(text string) string {
	if isCallText(text) {
		return text[:strings.IndexByte(text, '(')]
	}
	return "expr"
}

// isCallText reports whether an expression's text is one function call
// (name(...) with the parentheses closing at the end).
func isCallText(text string) bool {
	open := strings.IndexByte(text, '(')
	if open <= 0 || !strings.HasSuffix(text, ")") {
		return false
	}
	for _, r := range text[:open] {
		if !(r == '_' || r >= 'a' && r <= 'z' || r >= '0' && r <= '9') {
			return false
		}
	}
	return closingParen(text, open) == len(text)-1
}

// parenthesized wraps text in parentheses unless one pair already
// encloses all of it.
func parenthesized(text string) string {
	if strings.HasPrefix(text, "(") && closingParen(text, 0) == len(text)-1 {
		return text
	}
	return "(" + text + ")"
}

// closingParen is the index of the parenthesis closing the one at open,
// skipping string literals (-1 when there is none).
func closingParen(text string, open int) int {
	depth := 0
	inStr := false
	for i := open; i < len(text); i++ {
		c := text[i]
		switch {
		case inStr:
			if c == '\'' {
				inStr = false
			}
		case c == '\'':
			inStr = true
		case c == '(':
			depth++
		case c == ')':
			depth--
			if depth == 0 {
				return i
			}
		}
	}
	return -1
}

// IndexPredicateDef renders a partial index's WHERE clause as
// pg_get_indexdef ends with it: " WHERE (deleted_at IS NULL)".
func IndexPredicateDef(idx *catalog.IndexDescriptor) string {
	if idx.Predicate == "" {
		return ""
	}
	return " WHERE " + parenthesized(idx.Predicate)
}

// IndexDef renders pg_get_indexdef() for an index.
func IndexDef(d *catalog.TableDescriptor, idx *catalog.IndexDescriptor) string {
	cols := make([]string, 0, len(idx.ColumnIDs))
	for i := range idx.ColumnIDs {
		cols = append(cols, IndexKeyDef(d, idx, i))
	}
	if kind, ok := inverted.Lookup(idx.Inverted); ok && len(cols) == 1 {
		if c, ok := kind.(inverted.Computed); ok {
			if fn, arg := c.Computes(); fn != "" {
				// An index on to_tsvector('english', col), as PostgreSQL
				// prints one.
				cols[0] = fmt.Sprintf("%s('%s'::regconfig, %s)", fn, arg, cols[0])
			}
		}
	}
	unique := ""
	if idx.Unique {
		unique = "UNIQUE "
	}
	if v := idx.Vector; v != nil {
		// As pgvector renders it: the operator class names the distance
		// and lists is the storage parameter.
		return fmt.Sprintf("CREATE INDEX %s ON public.%s USING %s (%s %s) WITH (lists='%d')", QuoteIdent(idx.Name), QuoteIdent(d.Name), v.Method, strings.Join(cols, ", "), vector.Metric(v.Metric).OpClass(), v.Lists)
	}
	nulls := ""
	if idx.Unique && idx.NullsNotDistinct {
		nulls = " NULLS NOT DISTINCT"
	}
	return fmt.Sprintf("CREATE %sINDEX %s ON public.%s USING %s (%s)%s%s", unique, QuoteIdent(idx.Name), QuoteIdent(d.Name), IndexMethod(idx), strings.Join(cols, ", "), nulls, IndexPredicateDef(idx))
}

// IsConstraintIndex reports whether a unique index is also listed as a
// UNIQUE constraint (pg_constraint, information_schema): one with an
// expression key or a predicate is not, as in PostgreSQL, where such an
// index can only be made by CREATE UNIQUE INDEX.
func IsConstraintIndex(idx *catalog.IndexDescriptor) bool {
	return idx.Unique && !idx.Computed()
}

// Access method OIDs, as PostgreSQL numbers them.
const (
	OIDBtree = 403
	OIDGin   = 2742
)

// IndexMethod names an index's access method: btree, or the method of
// its inverted kind (gin).
func IndexMethod(idx *catalog.IndexDescriptor) string {
	if idx.IsVector() {
		return idx.Vector.Method
	}
	if kind, ok := inverted.Lookup(idx.Inverted); ok {
		return kind.Method()
	}
	return "btree"
}

// IndexMethodOID is pg_class.relam for an index.
func IndexMethodOID(idx *catalog.IndexDescriptor) int64 {
	if IndexMethod(idx) == "gin" {
		return OIDGin
	}
	return OIDBtree
}

// PrimaryKeyDef renders pg_get_constraintdef() for the primary key.
func PrimaryKeyDef(d *catalog.TableDescriptor) string {
	cols := make([]string, 0, len(d.PrimaryKey))
	for _, id := range d.PrimaryKey {
		if c, ok := columnByID(d, id); ok && c.Hidden {
			continue
		}
		cols = append(cols, quotedColumnName(d, id))
	}
	return "PRIMARY KEY (" + strings.Join(cols, ", ") + ")"
}

// UniqueDef renders pg_get_constraintdef() for a unique index.
func UniqueDef(d *catalog.TableDescriptor, idx *catalog.IndexDescriptor) string {
	cols := make([]string, 0, len(idx.ColumnIDs))
	for _, id := range idx.ColumnIDs {
		cols = append(cols, quotedColumnName(d, id))
	}
	if idx.Primary {
		return "PRIMARY KEY (" + strings.Join(cols, ", ") + ")"
	}
	return uniqueKeyword(idx) + " (" + strings.Join(cols, ", ") + ")"
}

// uniqueKeyword is UNIQUE, with PostgreSQL's NULLS NOT DISTINCT when the
// index was declared so.
func uniqueKeyword(idx *catalog.IndexDescriptor) string {
	if idx != nil && idx.NullsNotDistinct {
		return "UNIQUE NULLS NOT DISTINCT"
	}
	return "UNIQUE"
}

// ConstraintOID is a constraint's pg_constraint OID (a bit above the
// index OIDs of the same table, so the two never collide).
func ConstraintOID(d *catalog.TableDescriptor, c *catalog.Constraint) int64 {
	return relOID(d.ID)<<16 | 1<<15 | int64(c.ID)
}

// ConstraintDef renders pg_get_constraintdef() for a CHECK, FOREIGN KEY
// or UNIQUE constraint. byID resolves a foreign key's referenced table
// (nil renders its ID).
func ConstraintDef(d *catalog.TableDescriptor, c *catalog.Constraint, byID func(uint64) *catalog.TableDescriptor) string {
	names := func(t *catalog.TableDescriptor, ids []catalog.ColumnID) string {
		cols := make([]string, 0, len(ids))
		for _, id := range ids {
			cols = append(cols, quotedColumnName(t, id))
		}
		return strings.Join(cols, ", ")
	}
	switch c.Kind {
	case catalog.ConstraintCheck:
		return "CHECK (" + c.Expr + ")"
	case catalog.ConstraintUnique:
		idx, _ := d.IndexByID(c.IndexID)
		return uniqueKeyword(idx) + " (" + names(d, c.Columns) + ")"
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
			refName, refCols = QuoteIdent(ref.Name), names(ref, c.RefColumns)
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
	if env.ReadTable != nil {
		return env.ReadTable(id)
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

// QuoteIdent renders a name as PostgreSQL's catalog functions print
// one (quote_identifier): bare when plain, double-quoted otherwise.
func QuoteIdent(name string) string { return builtins.QuoteIdent(name) }

// quotedColumnName is columnName as SQL text: quoted where needed.
func quotedColumnName(d *catalog.TableDescriptor, id catalog.ColumnID) string {
	return QuoteIdent(columnName(d, id))
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
	fmt.Fprintf(&b, "CREATE TABLE %s (\n", QuoteIdent(d.Name))
	for _, c := range d.Columns {
		if c.Hidden {
			continue
		}
		fmt.Fprintf(&b, "  %s %s", QuoteIdent(c.Name), ColumnTypeSQL(&c))
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
		for i := range idx.ColumnIDs {
			if idx.IsExprKey(i) || len(idx.Directions) > i && idx.Directions[i] != "" {
				cols = append(cols, IndexKeyDef(d, &idx, i))
			} else {
				cols = append(cols, quotedColumnName(d, idx.ColumnIDs[i]))
			}
		}
		nulls := ""
		if idx.Unique && idx.NullsNotDistinct {
			nulls = " NULLS NOT DISTINCT"
		}
		items = append(items, fmt.Sprintf("%sINDEX %s (%s)%s%s", unique, QuoteIdent(idx.Name), strings.Join(cols, ", "), nulls, IndexPredicateDef(&idx)))
	}
	for i := range d.Constraints {
		c := &d.Constraints[i]
		items = append(items, fmt.Sprintf("CONSTRAINT %s %s", QuoteIdent(c.Name), ConstraintDef(d, c, byID)))
	}
	if len(items) > 0 {
		fmt.Fprintf(&b, "  %s", strings.Join(items, ",\n  "))
	} else {
		text := strings.TrimSuffix(b.String(), ",\n")
		b.Reset()
		b.WriteString(text)
	}
	b.WriteString("\n)")
	var opts []string
	if d.Timeseries {
		opts = append(opts, "timeseries = true")
		if d.RetentionSeconds > 0 {
			opts = append(opts, fmt.Sprintf("retention = '%ds'", d.RetentionSeconds))
		}
		if d.ShardBuckets > 0 {
			opts = append(opts, fmt.Sprintf("shards = %d", d.ShardBuckets))
		}
	}
	if d.GCTTLSeconds > 0 {
		opts = append(opts, fmt.Sprintf("gc_ttl = '%ds'", d.GCTTLSeconds))
	}
	if d.RowTTL != nil {
		if d.RowTTL.AfterSeconds > 0 {
			opts = append(opts, fmt.Sprintf("ttl_column = %s", quotedColumnName(d, d.RowTTL.ColumnID)), fmt.Sprintf("ttl = '%ds'", d.RowTTL.AfterSeconds))
		} else {
			opts = append(opts, fmt.Sprintf("ttl_expiration = %s", quotedColumnName(d, d.RowTTL.ColumnID)))
		}
	}
	if len(opts) > 0 {
		fmt.Fprintf(&b, " WITH (%s)", strings.Join(opts, ", "))
	}
	return b.String()
}

// PolicyCmdChar is pg_policy.polcmd for a policy command: r, a, w, d,
// or * for ALL.
func PolicyCmdChar(cmd string) string {
	switch cmd {
	case catalog.PolicySelect:
		return "r"
	case catalog.PolicyInsert:
		return "a"
	case catalog.PolicyUpdate:
		return "w"
	case catalog.PolicyDelete:
		return "d"
	}
	return "*"
}

// PolicyDef renders a policy as the CREATE POLICY statement that makes it.
func PolicyDef(d *catalog.TableDescriptor, p *catalog.Policy) string {
	var b strings.Builder
	fmt.Fprintf(&b, "CREATE POLICY %s ON %s", p.Name, d.Name)
	if p.Restrictive {
		b.WriteString(" AS RESTRICTIVE")
	}
	if p.Command != catalog.PolicyAll {
		fmt.Fprintf(&b, " FOR %s", strings.ToUpper(p.Command))
	}
	if len(p.Roles) > 0 {
		fmt.Fprintf(&b, " TO %s", strings.Join(p.Roles, ", "))
	}
	if p.Using != "" {
		fmt.Fprintf(&b, " USING (%s)", p.Using)
	}
	if p.Check != "" {
		fmt.Fprintf(&b, " WITH CHECK (%s)", p.Check)
	}
	return b.String()
}

// RowSecurityDefs renders the statements that reproduce a table's row
// security after its CREATE TABLE: ENABLE / FORCE and each policy.
func RowSecurityDefs(d *catalog.TableDescriptor) []string {
	var out []string
	if d.RowSecurity {
		out = append(out, fmt.Sprintf("ALTER TABLE %s ENABLE ROW LEVEL SECURITY", d.Name))
	}
	if d.ForceRowSecurity {
		out = append(out, fmt.Sprintf("ALTER TABLE %s FORCE ROW LEVEL SECURITY", d.Name))
	}
	for i := range d.Policies {
		out = append(out, PolicyDef(d, &d.Policies[i]))
	}
	return out
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
	case types.String, types.Bytes, types.Uuid, types.Jsonb, types.Tsvector, types.Tsquery, types.Decimal, types.Enum, types.Timestamp, types.Date, types.Time, types.IntervalFam, types.Vector:
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

// pkIndex is the primary key as an index (pg_index lists it with ID 1);
// nil for a table whose only key is the hidden rowid, which PostgreSQL
// would not have.
func pkIndex(d *catalog.TableDescriptor) *catalog.IndexDescriptor {
	if !hasVisiblePK(d) {
		return nil
	}
	return &catalog.IndexDescriptor{ID: 1, Name: d.Name + "_pkey", Unique: true, Primary: true, ColumnIDs: visiblePK(d)}
}

// indexByOID finds the index (the primary key's included) of d with
// pg_class OID oid.
func indexByOID(d *catalog.TableDescriptor, oid int64) *catalog.IndexDescriptor {
	if oid>>16 != relOID(d.ID) || oid&(1<<15) != 0 {
		return nil
	}
	id := uint64(oid & 0xffff)
	if id == 1 {
		return pkIndex(d)
	}
	for i := range d.Indexes {
		if d.Indexes[i].ID == id {
			return &d.Indexes[i]
		}
	}
	return nil
}

// IndexOIDByName is the pg_class OID of d's index named name.
func IndexOIDByName(d *catalog.TableDescriptor, name string) (int64, bool) {
	if pk := pkIndex(d); pk != nil && pk.Name == name {
		return IndexOID(d, 1), true
	}
	for i := range d.Indexes {
		if d.Indexes[i].Name == name {
			return IndexOID(d, d.Indexes[i].ID), true
		}
	}
	return 0, false
}

// IndexDefByOID is pg_get_indexdef(oid, col) for an index of d: the
// whole definition (col 0) or key column col's (from 1); false when d
// has no such index or key column.
func IndexDefByOID(d *catalog.TableDescriptor, oid int64, col int) (string, bool) {
	idx := indexByOID(d, oid)
	if idx == nil {
		return "", false
	}
	if col == 0 {
		return IndexDef(d, idx), true
	}
	if col < 1 || col > len(idx.ColumnIDs) {
		return "", true // PostgreSQL's: an empty string past the last key
	}
	return IndexKeyDef(d, idx, col-1), true
}

// ConstraintDefByOID is pg_get_constraintdef(oid) for a constraint of d
// (its primary key, a unique index that is a constraint, a CHECK or a
// FOREIGN KEY); false when d has none with that OID.
func ConstraintDefByOID(d *catalog.TableDescriptor, oid int64, byID func(uint64) *catalog.TableDescriptor) (string, bool) {
	if oid&(1<<15) != 0 {
		for i := range d.Constraints {
			if c := &d.Constraints[i]; ConstraintOID(d, c) == oid {
				return ConstraintDef(d, c, byID), true
			}
		}
		return "", false
	}
	idx := indexByOID(d, oid)
	switch {
	case idx == nil:
		return "", false
	case idx.ID == 1:
		return PrimaryKeyDef(d), true
	case IsConstraintIndex(idx):
		return UniqueDef(d, idx), true
	}
	return "", false
}
