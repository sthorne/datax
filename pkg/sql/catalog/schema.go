package catalog

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"sort"
	"strconv"
	"strings"

	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/kvclient"
	"github.com/sthorne/datax/pkg/kvpb"
	"github.com/sthorne/datax/pkg/util/encoding"
)

// Schemas (gap PG-D-001, docs/design/schemas.md).
//
// A schema is a namespace inside a database. Its descriptor lives at
// /system/schemadesc/<schemaID> and its name at
// /system/schemans/<dbID>/<name> → schemaID. The public schema of every
// database is implicit: schema ID 0, never stored, owned by the
// database's owner, with its privileges on the database descriptor
// (SchemaPrivileges, UsageRestricted), as before schemas existed.
//
// A table records its schema in TableDescriptor.SchemaID (0 = public).
// A public table's name entry is its name, as always; a table of
// another schema is filed, in the same per-database table namespace,
// under SchemaNamespaceName(schemaID, name), a name holding NUL bytes
// that no SQL text can spell (like a temporary table's, temp.go). So no
// existing key moves, every by-name entry point that refuses such names
// keeps refusing them, the gateway cache and the leases key a schema
// table as they key any table, and a rename of the schema moves one
// name entry and none of its tables'.
//
// Names are resolved by resolve (below), with the session's search
// path carried in the context (WithResolution). A caller that attaches
// none resolves through public alone: exactly the behaviour from before
// schemas.

// SchemaDescriptor describes a schema other than public.
type SchemaDescriptor struct {
	ID         uint64 `json:"id"`
	Name       string `json:"name"`
	DatabaseID uint64 `json:"database_id"`
	Owner      string `json:"owner,omitempty"`
	// Privileges maps a grantee role (or "public") to its privileges on
	// the schema (USAGE, CREATE); GrantOptions the subset held WITH
	// GRANT OPTION. The owner and admins bypass the map.
	Privileges   map[string][]string `json:"privileges,omitempty"`
	GrantOptions map[string][]string `json:"grant_options,omitempty"`
	Version      uint64              `json:"version,omitempty"`
}

// Clone returns a deep copy.
func (d *SchemaDescriptor) Clone() *SchemaDescriptor {
	out := *d
	out.Privileges = ClonePrivileges(d.Privileges)
	out.GrantOptions = ClonePrivileges(d.GrantOptions)
	return &out
}

// IsPublic reports whether d is the implicit public schema.
func (d *SchemaDescriptor) IsPublic() bool { return d.ID == 0 }

// ErrSchemaNotFound is an unknown schema (SQLSTATE 3F000).
type ErrSchemaNotFound struct{ Name string }

func (e *ErrSchemaNotFound) Error() string { return fmt.Sprintf("schema %q does not exist", e.Name) }

// ErrSchemaExists is a schema name already taken (42P06).
type ErrSchemaExists struct{ Name string }

func (e *ErrSchemaExists) Error() string { return fmt.Sprintf("schema %q already exists", e.Name) }

// ErrCrossDatabase is a three-part name into another database's schema
// other than public (0A000, PostgreSQL's wording).
type ErrCrossDatabase struct{ Name string }

func (e *ErrCrossDatabase) Error() string {
	return fmt.Sprintf("cross-database references are not implemented: %q", e.Name)
}

// ErrNoCreationSchema is an unqualified CREATE when no schema of the
// search path exists (3F000).
type ErrNoCreationSchema struct{}

func (e *ErrNoCreationSchema) Error() string { return "no schema has been selected to create in" }

// System schemas: never user schemas, never in a path's table lookup.
const (
	CatalogSchema     = "pg_catalog"
	InformationSchema = "information_schema"
)

// IsSystemSchemaName reports whether name is one of the schemas datax
// provides itself (pg_catalog, information_schema, pg_temp and
// PostgreSQL's pg_temp_N / pg_toast spellings), which hold no user
// tables.
func IsSystemSchemaName(name string) bool {
	return name == CatalogSchema || name == InformationSchema || strings.HasPrefix(name, "pg_")
}

// schemaMarker begins the name entry of a table outside public.
const schemaMarker = "\x00schema\x00"

// SchemaNamespaceName is the name entry of table name in schema
// schemaID (the name itself in public).
func SchemaNamespaceName(schemaID uint64, name string) string {
	if schemaID == 0 {
		return name
	}
	return schemaMarker + strconv.FormatUint(schemaID, 10) + "\x00" + name
}

// splitSchemaEntry decodes a name entry of SchemaNamespaceName's form
// (ok false for any other).
func splitSchemaEntry(entry string) (schemaID uint64, name string, ok bool) {
	rest, found := strings.CutPrefix(entry, schemaMarker)
	if !found {
		return 0, "", false
	}
	idText, name, found := strings.Cut(rest, "\x00")
	if !found {
		return 0, "", false
	}
	id, err := strconv.ParseUint(idText, 10, 64)
	if err != nil {
		return 0, "", false
	}
	return id, name, true
}

// NamespaceName is the name entry of d within its database: what the
// gateway cache and the leases key it by.
func (d *TableDescriptor) NamespaceName() string { return d.namespaceName() }

// publicSchema is the implicit public schema of database dbID.
func publicSchema(dbID uint64) *SchemaDescriptor {
	return &SchemaDescriptor{ID: 0, Name: PublicSchema, DatabaseID: dbID}
}

// LookupSchema resolves a schema of database dbID by name within txn;
// public always exists.
func LookupSchema(ctx context.Context, txn *kvclient.Txn, dbID uint64, name string) (*SchemaDescriptor, error) {
	if name == PublicSchema {
		return publicSchema(dbID), nil
	}
	if name == "" || IsSystemSchemaName(name) || IsReservedTableName(name) {
		return nil, &ErrSchemaNotFound{Name: name}
	}
	raw, err := txn.Get(ctx, keys.SchemaNamespaceKey(dbID, name))
	if err != nil {
		return nil, err
	}
	if raw == nil {
		return nil, &ErrSchemaNotFound{Name: name}
	}
	id, err := strconv.ParseUint(string(raw), 10, 64)
	if err != nil {
		return nil, fmt.Errorf("corrupt schema namespace entry for %q", name)
	}
	d, err := ReadSchema(ctx, txn, id)
	if err != nil {
		return nil, err
	}
	if d == nil {
		return nil, fmt.Errorf("schema namespace entry for %q points at missing descriptor %d", name, id)
	}
	return d, nil
}

// ReadSchema reads a schema descriptor by ID: the public schema's
// stand-in for 0 is not returned (nil, nil, as for a missing one).
func ReadSchema(ctx context.Context, txn *kvclient.Txn, id uint64) (*SchemaDescriptor, error) {
	if id == 0 {
		return nil, nil
	}
	raw, err := txn.Get(ctx, keys.SchemaDescKey(id))
	if err != nil || raw == nil {
		return nil, err
	}
	var d SchemaDescriptor
	if err := json.Unmarshal(raw, &d); err != nil {
		return nil, fmt.Errorf("corrupt schema descriptor %d: %w", id, err)
	}
	return &d, nil
}

// ListSchemas returns the schemas of database dbID other than public,
// by name.
func ListSchemas(ctx context.Context, txn *kvclient.Txn, dbID uint64) ([]*SchemaDescriptor, error) {
	lo, hi := keys.SchemaNamespaceSpan(dbID)
	rows, err := txn.Scan(ctx, lo, hi, 0)
	if err != nil {
		return nil, err
	}
	var out []*SchemaDescriptor
	for _, kv := range rows {
		id, err := strconv.ParseUint(string(kv.Value), 10, 64)
		if err != nil {
			continue
		}
		d, err := ReadSchema(ctx, txn, id)
		if err != nil {
			return nil, err
		}
		if d != nil {
			out = append(out, d)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out, nil
}

// CreateSchema writes a new schema within txn (the caller has checked
// the cluster version, the name and the privileges).
func (a *Accessor) CreateSchema(ctx context.Context, txn *kvclient.Txn, d *SchemaDescriptor) error {
	if d.Name == PublicSchema || IsSystemSchemaName(d.Name) {
		return &ErrSchemaExists{Name: d.Name}
	}
	// The database is read in this transaction, as for a table: a DROP
	// DATABASE committing alongside either sees the schema or fails it.
	db, err := ReadDatabase(ctx, txn, d.DatabaseID)
	if err != nil {
		return err
	}
	if db == nil {
		return &ErrDatabaseNotFound{Name: fmt.Sprintf("#%d", d.DatabaseID)}
	}
	existing, err := txn.Get(ctx, keys.SchemaNamespaceKey(d.DatabaseID, d.Name))
	if err != nil {
		return err
	}
	if existing != nil {
		return &ErrSchemaExists{Name: d.Name}
	}
	id, err := allocateDescID(ctx, txn)
	if err != nil {
		return err
	}
	d.ID = id
	d.Version = 1
	if err := putSchema(ctx, txn, d); err != nil {
		return err
	}
	if err := txn.Put(ctx, keys.SchemaNamespaceKey(d.DatabaseID, d.Name), []byte(strconv.FormatUint(d.ID, 10))); err != nil {
		return err
	}
	return bumpSchemaGen(ctx, txn, d.DatabaseID)
}

func putSchema(ctx context.Context, txn *kvclient.Txn, d *SchemaDescriptor) error {
	raw, err := json.Marshal(d)
	if err != nil {
		return err
	}
	return txn.Put(ctx, keys.SchemaDescKey(d.ID), raw)
}

// UpdateSchema rewrites a schema descriptor (owner, privileges),
// refusing one made from another version of it (a concurrent change).
// The database's schema generation moves with it, so gateways caching
// the schema set re-read it (a USAGE grant or revoke decides whether
// the schema is searched).
func (a *Accessor) UpdateSchema(ctx context.Context, txn *kvclient.Txn, d *SchemaDescriptor) error {
	cur, err := ReadSchema(ctx, txn, d.ID)
	if err != nil {
		return err
	}
	if cur == nil {
		return &ErrSchemaNotFound{Name: d.Name}
	}
	if cur.Version != d.Version {
		return &kvclient.RetryableError{Cause: &kvpb.Error{Message: fmt.Sprintf("schema %q was changed by a concurrent statement", d.Name)}}
	}
	d.Version++
	if err := putSchema(ctx, txn, d); err != nil {
		return err
	}
	return bumpSchemaGen(ctx, txn, d.DatabaseID)
}

// RenameSchema moves a schema's name entry. Its tables are filed by
// the schema's ID, so none of their entries, leases or cache keys
// change.
func (a *Accessor) RenameSchema(ctx context.Context, txn *kvclient.Txn, d *SchemaDescriptor, newName string) error {
	if newName == PublicSchema || IsSystemSchemaName(newName) {
		return &ErrSchemaExists{Name: newName}
	}
	existing, err := txn.Get(ctx, keys.SchemaNamespaceKey(d.DatabaseID, newName))
	if err != nil {
		return err
	}
	if existing != nil {
		return &ErrSchemaExists{Name: newName}
	}
	if err := txn.Delete(ctx, keys.SchemaNamespaceKey(d.DatabaseID, d.Name)); err != nil {
		return err
	}
	if err := txn.Put(ctx, keys.SchemaNamespaceKey(d.DatabaseID, newName), []byte(strconv.FormatUint(d.ID, 10))); err != nil {
		return err
	}
	d.Name = newName
	return a.UpdateSchema(ctx, txn, d)
}

// DropSchema removes an empty schema's descriptor and name (the caller
// dropped its tables first).
func (a *Accessor) DropSchema(ctx context.Context, txn *kvclient.Txn, d *SchemaDescriptor) error {
	if err := txn.Delete(ctx, keys.SchemaNamespaceKey(d.DatabaseID, d.Name)); err != nil {
		return err
	}
	if err := txn.Delete(ctx, keys.SchemaDescKey(d.ID)); err != nil {
		return err
	}
	return bumpSchemaGen(ctx, txn, d.DatabaseID)
}

// dropSchemasOf removes every schema of database dbID (DROP DATABASE,
// whose tables are gone by then).
func dropSchemasOf(ctx context.Context, txn *kvclient.Txn, dbID uint64) error {
	lo, hi := keys.SchemaNamespaceSpan(dbID)
	rows, err := txn.Scan(ctx, lo, hi, 0)
	if err != nil {
		return err
	}
	for _, kv := range rows {
		if id, err := strconv.ParseUint(string(kv.Value), 10, 64); err == nil {
			if err := txn.Delete(ctx, keys.SchemaDescKey(id)); err != nil {
				return err
			}
		}
		if err := txn.Delete(ctx, kv.Key); err != nil {
			return err
		}
	}
	if len(rows) == 0 {
		return nil
	}
	return bumpSchemaGen(ctx, txn, dbID)
}

// TablesInSchema returns the table and view descriptors of schema
// schemaID of database db, read in txn through the database's name
// entries.
func (a *Accessor) TablesInSchema(ctx context.Context, txn *kvclient.Txn, db *DatabaseDescriptor, schemaID uint64) ([]*TableDescriptor, error) {
	all, err := a.ListIn(ctx, txn, db)
	if err != nil {
		return nil, err
	}
	var out []*TableDescriptor
	for _, d := range all {
		if d.SchemaID == schemaID && d.Temp == nil {
			out = append(out, d)
		}
	}
	return out, nil
}

// SetTableSchema moves table d into schema schemaID of its database:
// the name entry moves, the descriptor keeps its ID (indexes,
// constraints, foreign keys and view dependencies hold) and is
// rewritten with the new schema, bumping its version.
func (a *Accessor) SetTableSchema(ctx context.Context, txn *kvclient.Txn, d *TableDescriptor, schemaID uint64) error {
	if d.Temp != nil {
		return fmt.Errorf("a temporary table cannot change schema")
	}
	if schemaID != 0 {
		sd, err := ReadSchema(ctx, txn, schemaID)
		if err != nil {
			return err
		}
		if sd == nil || sd.DatabaseID != d.DatabaseID {
			return &ErrSchemaNotFound{Name: fmt.Sprintf("#%d", schemaID)}
		}
	}
	newEntry := SchemaNamespaceName(schemaID, d.Name)
	existing, err := namespaceLookup(ctx, txn, d.DatabaseID, newEntry, schemaID == 0 && a.isDefaultID(d.DatabaseID))
	if err != nil {
		return err
	}
	if existing != nil {
		return &ErrTableExists{Name: d.Name}
	}
	if existing, err := txn.Get(ctx, keys.SequenceNamespaceKey(d.DatabaseID, newEntry)); err != nil {
		return err
	} else if existing != nil {
		return &ErrTableExists{Name: d.Name}
	}
	oldEntry := d.namespaceName()
	if err := txn.Delete(ctx, namespaceKey(d.DatabaseID, oldEntry)); err != nil {
		return err
	}
	if err := txn.Put(ctx, namespaceKey(d.DatabaseID, newEntry), []byte(strconv.FormatUint(d.ID, 10))); err != nil {
		return err
	}
	d.SchemaID = schemaID
	if err := a.Update(ctx, txn, d); err != nil {
		return err
	}
	a.invalidateID(d.ID)
	return nil
}

// Resolution is what name resolution needs of the session: its
// search_path entries, as SET wrote them ("$user" unexpanded), and the
// current role that "$user" names.
type Resolution struct {
	Path []string
	User string
	// CanUse reports whether the role may use a schema of the path
	// other than public (USAGE): one it may not is skipped, as
	// PostgreSQL skips it, for resolution and as the creation target.
	// nil: every schema is usable.
	CanUse func(ctx context.Context, txn *kvclient.Txn, sd *SchemaDescriptor) (bool, error)
}

type resolutionKey struct{}

// WithResolution attaches the session's resolution context to ctx, for
// every by-name catalog call made with it.
func WithResolution(ctx context.Context, r *Resolution) context.Context {
	return context.WithValue(ctx, resolutionKey{}, r)
}

// ResolutionOf is the resolution context of ctx (nil: public alone).
func ResolutionOf(ctx context.Context) *Resolution {
	r, _ := ctx.Value(resolutionKey{}).(*Resolution)
	return r
}

// ParseSearchPath splits a search_path value into its schema names:
// comma-separated, each optionally double-quoted ("$user" stays as
// written). Unquoted names are folded to lower case, as SET folds them.
func ParseSearchPath(v string) []string {
	var out []string
	for _, part := range splitPathList(v) {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		if len(part) >= 2 && part[0] == '"' && part[len(part)-1] == '"' {
			part = strings.ReplaceAll(part[1:len(part)-1], `""`, `"`)
		} else {
			part = strings.ToLower(part)
		}
		out = append(out, part)
	}
	return out
}

// splitPathList splits on commas outside double quotes.
func splitPathList(v string) []string {
	var out []string
	inQuote := false
	start := 0
	for i := 0; i < len(v); i++ {
		switch v[i] {
		case '"':
			inQuote = !inQuote
		case ',':
			if !inQuote {
				out = append(out, v[start:i])
				start = i + 1
			}
		}
	}
	return append(out, v[start:])
}

// UserPathEntry is the search_path entry that names the current role.
const UserPathEntry = "$user"

// schemaLookup looks up schemas of one database by name: from the
// gateway's leased schema set when there is one to use (schemacache.go),
// leased on first need, and otherwise in txn.
type schemaLookup struct {
	a      *Accessor // nil: always in txn
	txn    *kvclient.Txn
	dbID   uint64
	set    *schemaSet
	leased bool // set was asked for
}

// get returns the schema called name, or found false when there is no
// such schema. Public always exists.
func (l *schemaLookup) get(ctx context.Context, name string) (sd *SchemaDescriptor, found bool, err error) {
	if name == PublicSchema {
		return publicSchema(l.dbID), true, nil
	}
	if name == "" || IsSystemSchemaName(name) || IsReservedTableName(name) {
		return nil, false, nil
	}
	if l.a != nil && !l.leased {
		l.set, l.leased = l.a.schemaSnapshot(ctx, l.txn, l.dbID), true
	}
	if l.set != nil {
		sd = l.set.byName[name]
		return sd, sd != nil, nil
	}
	sd, err = LookupSchema(ctx, l.txn, l.dbID, name)
	if err != nil {
		var nf *ErrSchemaNotFound
		if errors.As(err, &nf) {
			return nil, false, nil
		}
		return nil, false, err
	}
	return sd, true, nil
}

// lookupSchemaErr is get with a missing schema as ErrSchemaNotFound.
func (l *schemaLookup) lookupSchemaErr(ctx context.Context, name string) (*SchemaDescriptor, error) {
	sd, found, err := l.get(ctx, name)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, &ErrSchemaNotFound{Name: name}
	}
	return sd, nil
}

// schemas is the lookup of database dbID's schemas for a by-name lookup
// of an existing object in txn: through the leased schema set when
// cached is true, in txn otherwise.
func (a *Accessor) schemas(txn *kvclient.Txn, dbID uint64, cached bool) *schemaLookup {
	l := &schemaLookup{txn: txn, dbID: dbID}
	if cached {
		l.a = a
	}
	return l
}

// PathSchemas resolves the path of r (public alone for nil) to the
// schemas of database dbID that exist, in order and without repeats:
// "$user" names the current role's schema, a schema that does not exist
// is skipped, and the system schemas (pg_catalog, pg_temp, ...) are
// left out, being the session's to search. It reads the schemas in txn;
// Accessor.PathSchemas answers from the gateway's leased schema set.
func PathSchemas(ctx context.Context, txn *kvclient.Txn, dbID uint64, r *Resolution) ([]*SchemaDescriptor, error) {
	return pathSchemas(ctx, &schemaLookup{txn: txn, dbID: dbID}, r)
}

// PathSchemas is catalog.PathSchemas through this gateway's leased
// schema set, when it can answer for txn (schemacache.go): for a lookup
// of existing objects, not for where one is created (ResolveTarget).
// The descriptors returned may be shared and must not be modified.
func (a *Accessor) PathSchemas(ctx context.Context, txn *kvclient.Txn, dbID uint64, r *Resolution) ([]*SchemaDescriptor, error) {
	return pathSchemas(ctx, a.schemas(txn, dbID, true), r)
}

func pathSchemas(ctx context.Context, l *schemaLookup, r *Resolution) ([]*SchemaDescriptor, error) {
	txn := l.txn
	path := []string{PublicSchema}
	user := ""
	var canUse func(context.Context, *kvclient.Txn, *SchemaDescriptor) (bool, error)
	if r != nil {
		path, user, canUse = r.Path, r.User, r.CanUse
	}
	var out []*SchemaDescriptor
	var seen []string
	for _, e := range path {
		if e == UserPathEntry {
			e = user
		}
		if e == "" || slices.Contains(seen, e) || IsSystemSchemaName(e) {
			continue
		}
		seen = append(seen, e)
		sd, found, err := l.get(ctx, e)
		if err != nil {
			return nil, err
		}
		if !found {
			continue
		}
		if !sd.IsPublic() && canUse != nil {
			ok, err := canUse(ctx, txn, sd)
			if err != nil {
				return nil, err
			}
			if !ok {
				continue
			}
		}
		out = append(out, sd)
	}
	return out, nil
}

// relMarker begins a relation name (RelationName).
const relMarker = "\x00rel\x00"

// RelationName is a name that resolves to exactly table d, in whatever
// database or schema, whatever the search path: the database's ID and
// the table's name entry, behind a marker no SQL text can spell. The
// session uses it where it names a table it found by other means (a
// listing, a descriptor) to an entry point that takes a name, and for
// the tables its post-commit drain is to wait on. A temporary table is
// named pg_temp.name, which its session resolves.
func RelationName(d *TableDescriptor) string {
	if d.Temp != nil {
		return TempSchema + "." + d.Name
	}
	return relMarker + strconv.FormatUint(d.DatabaseID, 10) + "\x00" + d.namespaceName()
}

// IsRelationName reports whether name is a RelationName.
func IsRelationName(name string) bool { return strings.HasPrefix(name, relMarker) }

// splitRelationName decodes a RelationName.
func splitRelationName(name string) (dbID uint64, entry string, ok bool) {
	rest, found := strings.CutPrefix(name, relMarker)
	if !found {
		return 0, "", false
	}
	idText, entry, found := strings.Cut(rest, "\x00")
	if !found {
		return 0, "", false
	}
	id, err := strconv.ParseUint(idText, 10, 64)
	if err != nil {
		return 0, "", false
	}
	return id, entry, true
}

// BareName is the table's own name in any name form: the last part of
// a qualified name, the name inside a RelationName.
func BareName(name string) string {
	if dbID, entry, ok := splitRelationName(name); ok {
		_ = dbID
		if _, n, ok := splitSchemaEntry(entry); ok {
			return n
		}
		return entry
	}
	parts := SplitName(name)
	return parts[len(parts)-1]
}

// LookupPathSchemas is PathSchemas for resolving an existing object's
// unqualified name, with one datax extension: a path that names no
// schema but the system ones (pg_dump sets an empty search_path, or pg_catalog)
// resolves through public. datax renders the names in view and
// constraint definitions unqualified, and a database whose objects are
// all in public must restore from a dump as it did before schemas.
// Creation (ResolveTarget) and current_schema() do not fall back.
func LookupPathSchemas(ctx context.Context, txn *kvclient.Txn, dbID uint64, r *Resolution) ([]*SchemaDescriptor, error) {
	return lookupPathSchemas(ctx, &schemaLookup{txn: txn, dbID: dbID}, r)
}

// LookupPathSchemas is catalog.LookupPathSchemas through this gateway's
// leased schema set, as Accessor.PathSchemas.
func (a *Accessor) LookupPathSchemas(ctx context.Context, txn *kvclient.Txn, dbID uint64, r *Resolution) ([]*SchemaDescriptor, error) {
	return lookupPathSchemas(ctx, a.schemas(txn, dbID, true), r)
}

func lookupPathSchemas(ctx context.Context, l *schemaLookup, r *Resolution) ([]*SchemaDescriptor, error) {
	dbID := l.dbID
	schemas, err := pathSchemas(ctx, l, r)
	if err != nil || len(schemas) > 0 || r == nil {
		return schemas, err
	}
	for _, e := range r.Path {
		if e == UserPathEntry || !IsSystemSchemaName(e) {
			return schemas, nil // it names a schema, which does not exist
		}
	}
	return []*SchemaDescriptor{publicSchema(dbID)}, nil
}

// SplitName splits a name as the parser keeps it ("x", "a.b",
// "a.b.c") into its parts.
func SplitName(name string) []string {
	if IsReservedTableName(name) || !strings.Contains(name, ".") {
		return []string{name}
	}
	return strings.Split(name, ".")
}

// nameCandidate is one place a table name may live.
type nameCandidate struct {
	db        string // the database's name, as resolved
	dbID      uint64
	entry     string // the name entry within the database
	isDefault bool   // the default database's flat namespace may hold it
}

// Target is where a name resolves to be created, or where an existing
// object of that name lives: a database, a schema of it, and the bare
// name.
type Target struct {
	Database string
	DBID     uint64
	Schema   *SchemaDescriptor
	Name     string
}

// resolveSchemaQualified resolves the two-part qualifier q of the
// current database cur: public, a schema of cur, or — the datax
// extension kept for compatibility (docs/design/schemas.md) — a
// database q, whose public schema is meant. ok is false when q names
// neither.
func (a *Accessor) resolveSchemaQualified(ctx context.Context, txn *kvclient.Txn, cur, q string, cached bool) (db string, dbID uint64, sd *SchemaDescriptor, ok bool, err error) {
	curID, err := a.databaseID(ctx, txn, cur)
	if err != nil {
		return "", 0, nil, false, err
	}
	if q == PublicSchema {
		return cur, curID, publicSchema(curID), true, nil
	}
	if !IsSystemSchemaName(q) {
		sd, found, err := a.schemas(txn, curID, cached).get(ctx, q)
		if err != nil {
			return "", 0, nil, false, err
		}
		if found {
			return cur, curID, sd, true, nil
		}
	}
	dbID, err = a.databaseID(ctx, txn, q)
	if err != nil {
		var nf *ErrDatabaseNotFound
		if errors.As(err, &nf) {
			return "", 0, nil, false, nil
		}
		return "", 0, nil, false, err
	}
	return q, dbID, publicSchema(dbID), true, nil
}

// resolveQualified resolves a qualified name's schema: two parts as
// resolveSchemaQualified, three parts as database.schema, where the
// database must be the current one unless the schema is public (the
// datax extension: db.public.t reads another database's table). A
// schema that does not exist is ErrSchemaNotFound. With cached true the
// schema may come from the leased schema set (a lookup of an existing
// object; the Target's descriptor is then shared, not to be modified).
func (a *Accessor) resolveQualified(ctx context.Context, txn *kvclient.Txn, cur string, parts []string, cached bool) (*Target, error) {
	full := strings.Join(parts, ".")
	if cur == "" {
		cur = DefaultDatabase
	}
	switch len(parts) {
	case 2:
		db, dbID, sd, ok, err := a.resolveSchemaQualified(ctx, txn, cur, parts[0], cached)
		if err != nil {
			return nil, err
		}
		if !ok {
			return nil, &ErrSchemaNotFound{Name: parts[0]}
		}
		return &Target{Database: db, DBID: dbID, Schema: sd, Name: parts[1]}, nil
	case 3:
		db, schema := parts[0], parts[1]
		if db != cur && schema != PublicSchema {
			return nil, &ErrCrossDatabase{Name: full}
		}
		dbID, err := a.databaseID(ctx, txn, db)
		if err != nil {
			return nil, err
		}
		sd, err := a.schemas(txn, dbID, cached).lookupSchemaErr(ctx, schema)
		if err != nil {
			return nil, err
		}
		return &Target{Database: db, DBID: dbID, Schema: sd, Name: parts[2]}, nil
	}
	return nil, &ErrTableNotFound{Name: full}
}

// resolve lists where table name may live, most preferred first: for
// an unqualified name the schemas of the search path in ctx (public
// alone without one) in database db; for a qualified one the single
// place its qualifiers name. A qualifier that names nothing yields no
// candidate (the table does not exist); a three-part name into another
// database's non-public schema is ErrCrossDatabase. Schemas are looked
// up through the gateway's leased schema set where it can answer
// (schemacache.go).
func (a *Accessor) resolve(ctx context.Context, txn *kvclient.Txn, db, name string) ([]nameCandidate, error) {
	return a.resolveWith(ctx, txn, db, name, true)
}

// resolveWith is resolve, reading every schema in txn when cached is
// false (LookupFresh, which must see the committed catalog).
func (a *Accessor) resolveWith(ctx context.Context, txn *kvclient.Txn, db, name string, cached bool) ([]nameCandidate, error) {
	if dbID, entry, ok := splitRelationName(name); ok {
		_, _, inSchema := splitSchemaEntry(entry)
		return []nameCandidate{{dbID: dbID, entry: entry, isDefault: !inSchema && a.isDefaultID(dbID)}}, nil
	}
	if IsReservedTableName(name) {
		return nil, nil
	}
	if db == "" {
		db = DefaultDatabase
	}
	// Unqualified (SplitName's one part, without its allocation: a
	// reserved name has returned above).
	if strings.IndexByte(name, '.') < 0 {
		dbID, err := a.databaseID(ctx, txn, db)
		if err != nil {
			return nil, err
		}
		r := ResolutionOf(ctx)
		if r == nil {
			// No session path: public alone, read without a schema
			// lookup, exactly as before schemas existed.
			return []nameCandidate{{db: db, dbID: dbID, entry: name, isDefault: isDefaultDatabase(db)}}, nil
		}
		schemas, err := lookupPathSchemas(ctx, a.schemas(txn, dbID, cached), r)
		if err != nil {
			return nil, err
		}
		out := make([]nameCandidate, 0, len(schemas))
		for _, sd := range schemas {
			out = append(out, nameCandidate{db: db, dbID: dbID, entry: SchemaNamespaceName(sd.ID, name), isDefault: sd.ID == 0 && isDefaultDatabase(db)})
		}
		return out, nil
	}
	parts := SplitName(name)
	if len(parts) == 2 && parts[0] == TempSchema {
		return nil, nil // the session's temporary tables are its own to resolve
	}
	t, err := a.resolveQualified(ctx, txn, db, parts, cached)
	if err != nil {
		var snf *ErrSchemaNotFound
		var tnf *ErrTableNotFound
		if errors.As(err, &snf) || errors.As(err, &tnf) {
			return nil, nil
		}
		return nil, err
	}
	return []nameCandidate{{db: t.Database, dbID: t.DBID, entry: SchemaNamespaceName(t.Schema.ID, t.Name), isDefault: t.Schema.ID == 0 && isDefaultDatabase(t.Database)}}, nil
}

// ResolveTarget resolves where name lives or is to be created in
// database db: a qualified name where its qualifiers say; an
// unqualified one in the first schema of the search path in ctx that
// exists (ErrNoCreationSchema when none does). The schema is read in
// txn, never from the leased schema set: what is created there records
// its ID, so a DROP SCHEMA committing concurrently must conflict with
// the creation.
func (a *Accessor) ResolveTarget(ctx context.Context, txn *kvclient.Txn, db, name string) (*Target, error) {
	if IsReservedTableName(name) {
		return nil, fmt.Errorf("table name %q is reserved", name)
	}
	if db == "" {
		db = DefaultDatabase
	}
	parts := SplitName(name)
	if len(parts) == 1 {
		dbID, err := a.databaseID(ctx, txn, db)
		if err != nil {
			return nil, err
		}
		schemas, err := PathSchemas(ctx, txn, dbID, ResolutionOf(ctx))
		if err != nil {
			return nil, err
		}
		if len(schemas) == 0 {
			return nil, &ErrNoCreationSchema{}
		}
		return &Target{Database: db, DBID: dbID, Schema: schemas[0], Name: name}, nil
	}
	return a.resolveQualified(ctx, txn, db, parts, false)
}

// ListAllSchemas returns every schema of every database (ownership
// changes, DROP ROLE's checks).
func ListAllSchemas(ctx context.Context, txn *kvclient.Txn) ([]*SchemaDescriptor, error) {
	lo, hi := keys.SchemaDescSpan()
	rows, err := txn.Scan(ctx, lo, hi, 0)
	if err != nil {
		return nil, err
	}
	out := make([]*SchemaDescriptor, 0, len(rows))
	for _, kv := range rows {
		var d SchemaDescriptor
		if err := json.Unmarshal(kv.Value, &d); err != nil {
			continue
		}
		out = append(out, &d)
	}
	return out, nil
}

// SchemaIDOfKey decodes the schema ID a schema descriptor key refers to.
func SchemaIDOfKey(k keys.Key) (uint64, bool) {
	lo, hi := keys.SchemaDescSpan()
	if k.Compare(lo) < 0 || k.Compare(hi) >= 0 {
		return 0, false
	}
	_, id, err := encoding.DecodeUint64(k[len(lo):])
	if err != nil {
		return 0, false
	}
	return id, true
}

// DisplayName is how a name is reported in an error: as written, but a
// RelationName (of a table, a sequence or a type) by the object's name.
func DisplayName(name string) string { return displayName(name) }
