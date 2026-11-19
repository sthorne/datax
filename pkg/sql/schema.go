package sql

import (
	"context"
	"strings"

	"github.com/sthorne/datax/pkg/kvclient"
	"github.com/sthorne/datax/pkg/sql/builtins"
	"github.com/sthorne/datax/pkg/sql/catalog"
	"github.com/sthorne/datax/pkg/sql/parser"
	"github.com/sthorne/datax/pkg/sql/vtable"
	"github.com/sthorne/datax/pkg/util/log"
	"github.com/sthorne/datax/pkg/version"
)

// Schemas (gap PG-D-001, docs/design/schemas.md): CREATE / DROP / ALTER
// SCHEMA, ALTER ... SET SCHEMA, and the session's side of name
// resolution — its search path, which every by-name catalog call
// carries (rctx).

// defaultSearchPath is PostgreSQL's default search_path. With no schema
// named after the role it resolves exactly as public alone, the only
// schema there was before.
const defaultSearchPath = `"$user", public`

// pathOf parses a search_path value, memoized for the session's value.
func (s *Session) pathOf(v string) []string {
	if s.pathMemo.raw == v && s.pathMemo.parsed != nil {
		return s.pathMemo.parsed
	}
	parsed := catalog.ParseSearchPath(v)
	if parsed == nil {
		parsed = []string{}
	}
	s.pathMemo.raw, s.pathMemo.parsed = v, parsed
	return parsed
}

// resolution is the session's resolution context: its search path (a
// view's own while the view's query runs) and its current role.
func (s *Session) resolution() *catalog.Resolution {
	path := s.viewPath
	if path == nil {
		path = s.pathOf(s.vars.searchPath)
	}
	return &catalog.Resolution{Path: path, User: s.user, CanUse: s.pathSchemaUsable}
}

// pathSchemaUsable reports whether the acting role holds USAGE on a
// schema of its search path (catalog.Resolution.CanUse).
func (s *Session) pathSchemaUsable(ctx context.Context, txn *kvclient.Txn, sd *catalog.SchemaDescriptor) (bool, error) {
	if s.actor() == catalog.RootRole || s.system {
		return true, nil
	}
	set, err := s.roleSet(ctx, txn)
	if err != nil {
		return false, err
	}
	return s.schemaPrivOn(ctx, txn, nil, sd, set, catalog.PrivUsage)
}

// rctx attaches the session's resolution context to ctx, for a by-name
// catalog call.
func (s *Session) rctx(ctx context.Context) context.Context {
	return catalog.WithResolution(ctx, s.resolution())
}

// formatSearchPath renders a search path as SHOW reports it: "$user"
// and names that need it quoted, joined by ", ".
func formatSearchPath(path []string) string {
	out := make([]string, len(path))
	for i, p := range path {
		if p == catalog.UserPathEntry {
			out[i] = `"$user"`
			continue
		}
		out[i] = builtins.QuoteIdent(p)
	}
	return strings.Join(out, ", ")
}

// pathSchemas is the schemas of the session's search path that exist
// in its current database, in order (current_schema, current_schemas,
// the creation target).
func (s *Session) pathSchemas(ctx context.Context, txn *kvclient.Txn) ([]*catalog.SchemaDescriptor, error) {
	dbID, err := s.cat.DatabaseID(ctx, txn, s.database)
	if err != nil {
		return nil, ToSQLError(err)
	}
	return catalog.PathSchemas(ctx, txn, dbID, s.resolution())
}

// lookupPathSchemas is pathSchemas for resolving existing objects:
// with catalog.LookupPathSchemas's fallback to public for a path that
// names no schema (pg_dump's empty one).
func (s *Session) lookupPathSchemas(ctx context.Context, txn *kvclient.Txn) ([]*catalog.SchemaDescriptor, error) {
	dbID, err := s.cat.DatabaseID(ctx, txn, s.database)
	if err != nil {
		return nil, ToSQLError(err)
	}
	return catalog.LookupPathSchemas(ctx, txn, dbID, s.resolution())
}

// requireSchemas gates the statements that create schemas on cluster
// version v22: before it, no node may write a schema descriptor or a
// table filed under one.
func (s *Session) requireSchemas() error { return s.requireV22For("schemas") }

// requireV22For refuses what writes v22 catalog state (schemas,
// temporary and unlogged tables) before the cluster has finalized v22.
func (s *Session) requireV22For(what string) error {
	if s.db.ClusterVersion() < version.V22 {
		return newErrf(CodeFeatureNotSupported, "%s need cluster version %s: finalize the upgrade with `datax debug upgrade` first", what, version.V22)
	}
	return nil
}

// drainName is the name the post-commit drain waits on for a table a
// statement changed: the table found now, in the transaction and under
// the search path the statement ran with, as a name no later SET (SET
// LOCAL ends with the block, before the drain) can redirect.
func (s *Session) drainName(ctx context.Context, txn *kvclient.Txn, name string) string {
	if txn == nil || catalog.IsRelationName(name) || catalog.IsReservedTableName(name) {
		return name
	}
	d, err := s.cat.LookupFreshIn(s.rctx(ctx), txn, s.database, name)
	if err != nil || d == nil || d.Temp != nil {
		return name
	}
	return catalog.RelationName(d)
}

// roleSpec resolves the role a statement names, CURRENT_USER and
// SESSION_USER included.
func (s *Session) roleSpec(name string) string {
	switch n := strings.ToLower(name); n {
	case "current_user", "current_role", "user":
		return s.user
	case "session_user":
		return s.sessionUser
	default:
		return n
	}
}

// checkSchemaName refuses a name no user schema may take (42939).
func checkSchemaName(name string) error {
	if strings.HasPrefix(name, "pg_") {
		return &Error{Code: CodeReservedName, Msg: "unacceptable schema name \"" + name + "\"", Detail: "The prefix \"pg_\" is reserved for system schemas."}
	}
	if catalog.IsReservedTableName(name) || name == "" {
		return newErrf(CodeReservedName, "unacceptable schema name %q", name)
	}
	return nil
}

// execCreateSchema is CREATE SCHEMA [IF NOT EXISTS] [name]
// [AUTHORIZATION role].
func (s *Session) execCreateSchema(ctx context.Context, txn *kvclient.Txn, t *parser.CreateSchema) (*Result, error) {
	const tag = "CREATE SCHEMA"
	if err := s.requireSchemas(); err != nil {
		return nil, err
	}
	db, err := catalog.LookupDatabase(ctx, txn, s.database)
	if err != nil {
		return nil, ToSQLError(err)
	}
	if db.ID == 0 {
		return nil, errBeforeV6()
	}
	if db.Name == catalog.SystemDatabase && !s.system {
		return nil, newErrf(CodeInsufficientPriv, "database %q is reserved for the cluster", db.Name)
	}
	owner := s.user
	if t.Authorization != "" {
		owner = s.roleSpec(t.Authorization)
		if err := s.checkNewOwner(ctx, txn, owner); err != nil {
			return nil, err
		}
	}
	name := t.Name
	if name == "" {
		name = owner
	}
	if name == catalog.PublicSchema || name == catalog.InformationSchema || name == catalog.CatalogSchema {
		if t.IfNotExists {
			return &Result{Tag: tag, Notices: []string{"schema \"" + name + "\" already exists, skipping"}}, nil
		}
		return nil, newErrf(CodeDuplicateSchema, "schema %q already exists", name)
	}
	if err := checkSchemaName(name); err != nil {
		return nil, err
	}
	ok, err := s.databasePrivAllowed(ctx, txn, db, catalog.PrivCreate)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, newErrf(CodeInsufficientPriv, "permission denied for database %s", db.Name)
	}
	sd := &catalog.SchemaDescriptor{Name: name, DatabaseID: db.ID, Owner: owner}
	if err := s.cat.CreateSchema(ctx, txn, sd); err != nil {
		var ex *catalog.ErrSchemaExists
		if asErr(err, &ex) {
			if t.IfNotExists {
				return &Result{Tag: tag, Notices: []string{"schema \"" + name + "\" already exists, skipping"}}, nil
			}
			return nil, newErrf(CodeDuplicateSchema, "schema %q already exists", name)
		}
		return nil, ToSQLError(err)
	}
	log.Audit("schema-ddl", "stmt", tag, "target", name, "database", db.Name, "owner", owner, "principal", s.sessionUser, "role", s.user)
	return &Result{Tag: tag}, nil
}

// lookupSchemaOrErr resolves a schema of the current database by name:
// 3F000 when there is none.
func (s *Session) lookupSchemaOrErr(ctx context.Context, txn *kvclient.Txn, name string) (*catalog.DatabaseDescriptor, *catalog.SchemaDescriptor, error) {
	db, err := catalog.LookupDatabase(ctx, txn, s.database)
	if err != nil {
		return nil, nil, ToSQLError(err)
	}
	sd, err := catalog.LookupSchema(ctx, txn, db.ID, name)
	if err != nil {
		return db, nil, ToSQLError(err)
	}
	return db, sd, nil
}

// execDropSchema is DROP SCHEMA [IF EXISTS] name [, ...] [CASCADE |
// RESTRICT]. CASCADE drops the schema's views, then its tables, each
// through DROP VIEW / DROP TABLE ... CASCADE, so that every rule those
// follow (dependencies, drains, the reclaim of the rows) applies.
func (s *Session) execDropSchema(ctx context.Context, txn *kvclient.Txn, t *parser.DropSchema) (*Result, error) {
	const tag = "DROP SCHEMA"
	type victim struct {
		db *catalog.DatabaseDescriptor
		sd *catalog.SchemaDescriptor
	}
	var victims []victim
	var notices []string
	for _, name := range t.Names {
		switch name {
		case catalog.PublicSchema:
			return nil, newErrf(CodeFeatureNotSupported, "the public schema cannot be dropped: it is part of every database")
		case catalog.CatalogSchema, catalog.InformationSchema, "pg_toast":
			return nil, newErrf(CodeDependentObjectsExist, "cannot drop schema %s because it is required by the database system", name)
		}
		db, sd, err := s.lookupSchemaOrErr(ctx, txn, name)
		if err != nil {
			if serr := ToSQLError(err); serr.Code == CodeInvalidSchemaName && t.IfExists {
				notices = append(notices, "schema \""+name+"\" does not exist, skipping")
				continue
			}
			return nil, err
		}
		if err := s.checkOwner(ctx, txn, "schema", name, sd.Owner); err != nil {
			return nil, err
		}
		// A view of another schema that resolves its names through this
		// one would resolve them through whatever schema took the name
		// next.
		if err := s.refuseIfSchemaInViewPath(ctx, txn, db, sd, "drop schema "+name); err != nil {
			return nil, err
		}
		victims = append(victims, victim{db, sd})
	}
	dropped := 0
	for _, v := range victims {
		tables, err := s.cat.TablesInSchema(ctx, txn, v.db, v.sd.ID)
		if err != nil {
			return nil, err
		}
		seqs, typs, err := schemaSequencesAndTypes(ctx, txn, v.db.ID, v.sd.ID)
		if err != nil {
			return nil, err
		}
		if !t.Cascade {
			kind, name := "", ""
			switch {
			case len(tables) > 0:
				kind, name = "table", tables[0].Name
				if tables[0].IsView() {
					kind = "view"
				}
			case len(seqs) > 0:
				kind, name = "sequence", seqs[0].Name
			case len(typs) > 0:
				kind, name = "type", typs[0].Name
			}
			if kind != "" {
				return nil, &Error{Code: CodeDependentObjectsExist,
					Msg:    "cannot drop schema " + v.sd.Name + " because other objects depend on it",
					Detail: kind + " " + v.sd.Name + "." + name + " depends on schema " + v.sd.Name,
					Hint:   "Use DROP ... CASCADE to drop the dependent objects too."}
			}
		}
		// Views first: a view of the schema may read its tables.
		for _, views := range []bool{true, false} {
			for _, d := range tables {
				if d.IsView() != views {
					continue
				}
				rel := catalog.RelationName(d)
				if views {
					_, err = s.execDropView(ctx, txn, &parser.DropView{Names: []string{rel}, Cascade: true, IfExists: true})
				} else {
					_, err = s.execDropTable(ctx, txn, &parser.DropTable{Name: rel, Cascade: true, IfExists: true})
				}
				if err != nil {
					return nil, err
				}
				dropped++
			}
		}
		// Then its sequences (those its tables owned went with them;
		// the defaults of other schemas' tables bound to one lose it,
		// as CASCADE drops them in PostgreSQL), and its types (one a
		// column of another schema's table still uses is refused).
		for _, sq := range seqs {
			if cur, err := catalog.ReadSequence(ctx, txn, sq.ID); err != nil || cur == nil {
				continue
			}
			if _, err := s.execDropSequence(ctx, txn, &parser.DropSequence{Name: catalog.SequenceRelationName(sq), Cascade: true, IfExists: true}); err != nil {
				return nil, err
			}
			dropped++
		}
		for _, td := range typs {
			if _, err := s.execDropType(ctx, txn, &parser.DropType{Name: catalog.TypeRelationName(td), IfExists: true}); err != nil {
				return nil, err
			}
			dropped++
		}
		if err := s.cat.DropSchema(ctx, txn, v.sd); err != nil {
			return nil, err
		}
		log.Audit("schema-ddl", "stmt", tag, "target", v.sd.Name, "database", v.db.Name, "cascade", t.Cascade, "dropped", dropped, "principal", s.sessionUser, "role", s.user)
	}
	return &Result{Tag: tag, Notices: notices}, nil
}

// schemaSequencesAndTypes lists the sequences and types of schema
// schemaID of database dbID.
func schemaSequencesAndTypes(ctx context.Context, txn *kvclient.Txn, dbID, schemaID uint64) ([]*catalog.SequenceDescriptor, []*catalog.TypeDescriptor, error) {
	allSeqs, err := catalog.ListSequences(ctx, txn, dbID)
	if err != nil {
		return nil, nil, err
	}
	var seqs []*catalog.SequenceDescriptor
	for _, sq := range allSeqs {
		if sq.SchemaID == schemaID {
			seqs = append(seqs, sq)
		}
	}
	allTypes, err := catalog.ListTypes(ctx, txn, dbID)
	if err != nil {
		return nil, nil, err
	}
	var typs []*catalog.TypeDescriptor
	for _, td := range allTypes {
		if td.SchemaID == schemaID {
			typs = append(typs, td)
		}
	}
	return seqs, typs, nil
}

// refuseIfSchemaViewed refuses a change to schema sd that would change
// what a view reads: a view that reads one of its relations (by name,
// in its stored text), or whose stored search path names the schema.
func (s *Session) refuseIfSchemaViewed(ctx context.Context, txn *kvclient.Txn, db *catalog.DatabaseDescriptor, sd *catalog.SchemaDescriptor, what string) error {
	all, err := s.cat.ListIn(ctx, txn, db)
	if err != nil {
		return err
	}
	inSchema := map[uint64]bool{}
	for _, d := range all {
		if d.SchemaID == sd.ID && d.Temp == nil {
			inSchema[d.ID] = true
		}
	}
	for _, v := range all {
		if !v.IsView() {
			continue
		}
		for _, p := range v.ViewSearchPath {
			if p == sd.Name {
				return newErrf(CodeDependentObjectsExist, "cannot %s: view %q resolves its names through schema %s (drop or replace the view first)", what, v.Name, sd.Name)
			}
		}
		for _, dep := range v.ViewDepends {
			if inSchema[dep] && !inSchema[v.ID] {
				return newErrf(CodeDependentObjectsExist, "cannot %s: view %q depends on a relation of schema %s (drop or replace the view first)", what, v.Name, sd.Name)
			}
			if inSchema[dep] && inSchema[v.ID] {
				return newErrf(CodeDependentObjectsExist, "cannot %s: view %q names a relation of schema %s in its query (drop or replace the view first)", what, v.Name, sd.Name)
			}
		}
	}
	return nil
}

// refuseIfSchemaInViewPath refuses a change to schema sd while a view
// outside it resolves its names through it (its stored search path).
func (s *Session) refuseIfSchemaInViewPath(ctx context.Context, txn *kvclient.Txn, db *catalog.DatabaseDescriptor, sd *catalog.SchemaDescriptor, what string) error {
	all, err := s.cat.ListIn(ctx, txn, db)
	if err != nil {
		return err
	}
	for _, v := range all {
		if !v.IsView() || v.SchemaID == sd.ID {
			continue
		}
		for _, p := range v.ViewSearchPath {
			if p == sd.Name {
				return newErrf(CodeDependentObjectsExist, "cannot %s: view %q resolves its names through schema %s (drop or replace the view first)", what, v.Name, sd.Name)
			}
		}
	}
	return nil
}

// execAlterSchemaRename is ALTER SCHEMA name RENAME TO new_name: the
// owner, holding CREATE on the database. The schema's tables are filed
// by its ID, so only its own name entry moves.
func (s *Session) execAlterSchemaRename(ctx context.Context, txn *kvclient.Txn, t *parser.AlterSchemaRename) (*Result, error) {
	const tag = "ALTER SCHEMA"
	if t.Name == catalog.PublicSchema {
		return nil, newErrf(CodeFeatureNotSupported, "the public schema cannot be renamed: it is part of every database")
	}
	db, sd, err := s.lookupSchemaOrErr(ctx, txn, t.Name)
	if err != nil {
		return nil, err
	}
	if err := s.checkOwner(ctx, txn, "schema", t.Name, sd.Owner); err != nil {
		return nil, err
	}
	ok, err := s.databasePrivAllowed(ctx, txn, db, catalog.PrivCreate)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, newErrf(CodeInsufficientPriv, "permission denied for database %s", db.Name)
	}
	if t.NewName == catalog.PublicSchema || t.NewName == catalog.InformationSchema || t.NewName == catalog.CatalogSchema {
		return nil, newErrf(CodeDuplicateSchema, "schema %q already exists", t.NewName)
	}
	if err := checkSchemaName(t.NewName); err != nil {
		return nil, err
	}
	// A view stores its query as text: renaming the schema under it
	// would change what it reads.
	if err := s.refuseIfSchemaViewed(ctx, txn, db, sd, "rename schema "+sd.Name); err != nil {
		return nil, err
	}
	// The column defaults bound to the schema's sequences keep drawing
	// from them; their text is rewritten to the new name.
	seqs, _, err := schemaSequencesAndTypes(ctx, txn, db.ID, sd.ID)
	if err != nil {
		return nil, err
	}
	var refs []seqDefaultRef
	if len(seqs) > 0 {
		ids := map[uint64]bool{}
		for _, sq := range seqs {
			ids[sq.ID] = true
		}
		if refs, err = s.seqDefaultRefs(ctx, txn, db.ID, func(id uint64) bool { return ids[id] }); err != nil {
			return nil, err
		}
	}
	sd = sd.Clone()
	if err := s.cat.RenameSchema(ctx, txn, sd, t.NewName); err != nil {
		var ex *catalog.ErrSchemaExists
		if asErr(err, &ex) {
			return nil, newErrf(CodeDuplicateSchema, "schema %q already exists", t.NewName)
		}
		return nil, ToSQLError(err)
	}
	if err := s.rewriteSeqDefaults(ctx, txn, refs); err != nil {
		return nil, err
	}
	log.Audit("schema-ddl", "stmt", "ALTER SCHEMA RENAME", "target", t.Name, "new_name", t.NewName, "principal", s.sessionUser, "role", s.user)
	return &Result{Tag: tag}, nil
}

// alterSchemaOwner is ALTER SCHEMA name OWNER TO role for a schema
// other than public.
func (s *Session) alterSchemaOwner(ctx context.Context, txn *kvclient.Txn, name, owner string) error {
	_, sd, err := s.lookupSchemaOrErr(ctx, txn, name)
	if err != nil {
		return err
	}
	if err := s.checkOwner(ctx, txn, "schema", name, sd.Owner); err != nil {
		return err
	}
	if err := s.checkNewOwner(ctx, txn, owner); err != nil {
		return err
	}
	sd = sd.Clone()
	sd.Owner = owner
	return s.cat.UpdateSchema(ctx, txn, sd)
}

// schemaPrivOn reports whether set holds priv (USAGE, CREATE) on
// schema sd: admins and the owner hold everything; others what is
// granted to them or to PUBLIC. For public: USAGE is PUBLIC's unless
// revoked, CREATE comes with CREATE on the database (or on public).
func (s *Session) schemaPrivOn(ctx context.Context, txn *kvclient.Txn, db *catalog.DatabaseDescriptor, sd *catalog.SchemaDescriptor, set catalog.RoleSet, priv string) (bool, error) {
	if set.IsAdmin() {
		return true, nil
	}
	if sd.IsPublic() {
		if priv == catalog.PrivCreate {
			return s.databasePrivAllowed(ctx, txn, db, catalog.PrivCreate)
		}
		if db == nil || db.ID == 0 || !db.UsageRestricted || set.Has(catalog.OwnerOf(db.Owner)) {
			return true, nil
		}
		return catalog.HasPrivilege(db.SchemaPrivileges, set, priv), nil
	}
	if set.Has(catalog.OwnerOf(sd.Owner)) {
		return true, nil
	}
	return catalog.HasPrivilege(sd.Privileges, set, priv), nil
}

// checkCreateInSchema returns 42501 unless the actor may create in the
// schema t names (CREATE on it; for public, CREATE on the database or on
// public, as before schemas).
func (s *Session) checkCreateInSchema(ctx context.Context, txn *kvclient.Txn, t *catalog.Target) error {
	if s.actor() == catalog.RootRole {
		return nil
	}
	db, err := catalog.ReadDatabase(ctx, txn, t.DBID)
	if err != nil {
		return err
	}
	if db == nil {
		db = &catalog.DatabaseDescriptor{Name: t.Database}
	}
	set, err := s.roleSet(ctx, txn)
	if err != nil {
		return err
	}
	ok, err := s.schemaPrivOn(ctx, txn, db, t.Schema, set, catalog.PrivCreate)
	if err != nil {
		return err
	}
	if !ok {
		if t.Schema.IsPublic() {
			return newErrf(CodeInsufficientPriv, "permission denied: %q needs the admin role or CREATE on database %q", s.actor(), t.Database)
		}
		return newErrf(CodeInsufficientPriv, "permission denied for schema %s", t.Schema.Name)
	}
	return nil
}

// createTarget resolves where a CREATE of name puts the object: 3F000
// when the qualifier names no schema (or no schema of the path exists),
// 0A000 for another database's schema other than public.
func (s *Session) createTarget(ctx context.Context, txn *kvclient.Txn, name string) (*catalog.Target, error) {
	t, err := s.cat.ResolveTarget(s.rctx(ctx), txn, s.database, name)
	if err != nil {
		return nil, ToSQLError(err)
	}
	return t, nil
}

// objectTarget resolves where CREATE SEQUENCE / TYPE (or a SERIAL
// column's sequence) puts name, as createTarget, and checks that the
// actor may create there (CREATE on the schema; for public, as
// before schemas). A schema other than public needs v22, as every
// object filed under one does.
func (s *Session) objectTarget(ctx context.Context, txn *kvclient.Txn, name string) (*catalog.Target, error) {
	t, err := s.createTarget(ctx, txn, name)
	if err != nil {
		return nil, err
	}
	if !t.Schema.IsPublic() {
		if err := s.requireSchemas(); err != nil {
			return nil, err
		}
	}
	if err := s.checkCreateInSchema(ctx, txn, t); err != nil {
		return nil, err
	}
	return t, nil
}

// execSetSchema is ALTER TABLE | VIEW | SEQUENCE [IF EXISTS] name SET
// SCHEMA s, and ALTER TYPE name SET SCHEMA s.
func (s *Session) execSetSchema(ctx context.Context, txn *kvclient.Txn, t *parser.SetSchema) (*Result, error) {
	tag := "ALTER " + strings.ToUpper(t.Kind)
	switch t.Kind {
	case "sequence":
		return s.setSequenceSchema(ctx, txn, t, tag)
	case "type":
		return s.setTypeSchema(ctx, txn, t, tag)
	}
	shared, err := s.lookup(ctx, txn, t.Name)
	if err != nil {
		var nf *catalog.ErrTableNotFound
		if asErr(err, &nf) && t.IfExists {
			return &Result{Tag: tag, Notices: []string{"relation \"" + t.Name + "\" does not exist, skipping"}}, nil
		}
		return nil, err
	}
	if shared.Virtual != "" {
		return nil, newErrf(CodeInsufficientPriv, "%s is a system catalog and cannot be modified", shared.Virtual)
	}
	if t.Kind == "view" && !shared.IsView() {
		return nil, newErrf(CodeWrongObjectType, "%q is not a view", t.Name)
	}
	if shared.Temp != nil {
		return nil, newErrf(CodeFeatureNotSupported, "cannot move objects into or out of temporary schemas")
	}
	if catalog.IsSystemTable(shared.Name) && !s.system {
		return nil, newErrf(CodeInsufficientPriv, "table %q belongs to the cluster", shared.Name)
	}
	if err := s.checkTableOwner(ctx, txn, shared); err != nil {
		return nil, err
	}
	target, err := catalog.LookupSchema(ctx, txn, shared.DatabaseID, t.Schema)
	if err != nil {
		return nil, ToSQLError(err)
	}
	if target.ID == shared.SchemaID {
		return &Result{Tag: tag}, nil
	}
	db, err := catalog.ReadDatabase(ctx, txn, shared.DatabaseID)
	if err != nil {
		return nil, err
	}
	if db == nil {
		db = &catalog.DatabaseDescriptor{Name: s.database}
	}
	if err := s.checkCreateInSchema(ctx, txn, &catalog.Target{Database: db.Name, DBID: shared.DatabaseID, Schema: target, Name: shared.Name}); err != nil {
		return nil, err
	}
	// A view stores its query as text, by name: moving a relation it
	// reads would change what it reads.
	if err := s.refuseIfViewed(ctx, txn, shared, "move "+shared.Name+" to schema "+target.Name); err != nil {
		return nil, err
	}
	// The sequences the table owns (SERIAL, identity, OWNED BY) move
	// with it, as in PostgreSQL; the defaults naming them are found
	// now, while their names still resolve, and renamed after.
	owned, err := s.ownedSequences(ctx, txn, shared)
	if err != nil {
		return nil, err
	}
	var refs []seqDefaultRef
	if len(owned) > 0 {
		ids := map[uint64]bool{}
		for _, sq := range owned {
			ids[sq.ID] = true
		}
		if refs, err = s.seqDefaultRefs(ctx, txn, shared.DatabaseID, func(id uint64) bool { return ids[id] }); err != nil {
			return nil, err
		}
	}
	desc := shared.Clone()
	if err := s.cat.SetTableSchema(ctx, txn, desc, target.ID); err != nil {
		var ex *catalog.ErrTableExists
		if asErr(err, &ex) {
			return nil, newErrf(CodeDuplicateTable, "relation %q already exists in schema %q", shared.Name, target.Name)
		}
		return nil, ToSQLError(err)
	}
	s.noteDDL(catalog.RelationName(desc))
	for _, sq := range owned {
		if err := catalog.SetSequenceSchema(ctx, txn, sq, target.ID); err != nil {
			var ex *catalog.ErrSequenceExists
			if asErr(err, &ex) {
				return nil, newErrf(CodeDuplicateTable, "relation %q already exists in schema %q", sq.Name, target.Name)
			}
			return nil, ToSQLError(err)
		}
	}
	if err := s.rewriteSeqDefaults(ctx, txn, refs); err != nil {
		return nil, err
	}
	log.Audit("schema-ddl", "stmt", tag+" SET SCHEMA", "target", t.Name, "schema", target.Name, "principal", s.sessionUser, "role", s.user)
	return &Result{Tag: tag}, nil
}

// ownedSequences lists the sequences table d owns: its SERIAL and
// identity columns', and those made OWNED BY one of its columns.
func (s *Session) ownedSequences(ctx context.Context, txn *kvclient.Txn, d *catalog.TableDescriptor) ([]*catalog.SequenceDescriptor, error) {
	seqs, err := catalog.ListSequences(ctx, txn, d.DatabaseID)
	if err != nil {
		return nil, err
	}
	byCol := map[uint64]bool{}
	for _, c := range d.Columns {
		if c.SequenceID != 0 {
			byCol[c.SequenceID] = true
		}
	}
	var out []*catalog.SequenceDescriptor
	for _, sq := range seqs {
		if sq.OwnerTable == d.ID || byCol[sq.ID] {
			out = append(out, sq)
		}
	}
	return out, nil
}

// setSequenceSchema is ALTER SEQUENCE [IF EXISTS] name SET SCHEMA s:
// the owner, holding CREATE on s. A sequence a column owns moves only
// with its table (0A000, as in PostgreSQL). The column defaults bound
// to the sequence keep drawing from it, and are rewritten to name it
// where it now is.
func (s *Session) setSequenceSchema(ctx context.Context, txn *kvclient.Txn, t *parser.SetSchema, tag string) (*Result, error) {
	sd, err := s.lookupSequence(ctx, txn, t.Name)
	if err != nil {
		if serr := ToSQLError(err); serr.Code == CodeUndefinedTable && t.IfExists {
			return &Result{Tag: tag, Notices: []string{"relation \"" + t.Name + "\" does not exist, skipping"}}, nil
		}
		return nil, err
	}
	if err := s.checkSequenceOwner(ctx, txn, sd); err != nil {
		return nil, err
	}
	target, err := catalog.LookupSchema(ctx, txn, sd.DatabaseID, t.Schema)
	if err != nil {
		return nil, ToSQLError(err)
	}
	if target.ID == sd.SchemaID {
		return &Result{Tag: tag}, nil
	}
	if sd.OwnerTable != 0 {
		detail := ""
		if owner, err := catalog.ReadTable(ctx, txn, sd.OwnerTable); err == nil && owner != nil {
			detail = "Sequence \"" + sd.Name + "\" is linked to table \"" + owner.Name + "\"."
		}
		return nil, &Error{Code: CodeFeatureNotSupported, Msg: "cannot move an owned sequence into another schema", Detail: detail}
	}
	db, err := catalog.ReadDatabase(ctx, txn, sd.DatabaseID)
	if err != nil {
		return nil, err
	}
	if db == nil {
		db = &catalog.DatabaseDescriptor{Name: s.database}
	}
	if err := s.checkCreateInSchema(ctx, txn, &catalog.Target{Database: db.Name, DBID: sd.DatabaseID, Schema: target, Name: sd.Name}); err != nil {
		return nil, err
	}
	refs, err := s.seqDefaultRefs(ctx, txn, sd.DatabaseID, func(id uint64) bool { return id == sd.ID })
	if err != nil {
		return nil, err
	}
	if err := catalog.SetSequenceSchema(ctx, txn, sd, target.ID); err != nil {
		var ex *catalog.ErrSequenceExists
		if asErr(err, &ex) {
			return nil, newErrf(CodeDuplicateTable, "relation %q already exists in schema %q", sd.Name, target.Name)
		}
		return nil, ToSQLError(err)
	}
	if err := s.rewriteSeqDefaults(ctx, txn, refs); err != nil {
		return nil, err
	}
	log.Audit("schema-ddl", "stmt", tag+" SET SCHEMA", "target", t.Name, "schema", target.Name, "principal", s.sessionUser, "role", s.user)
	return &Result{Tag: tag}, nil
}

// setTypeSchema is ALTER TYPE name SET SCHEMA s: the owner, holding
// CREATE on s. The columns of the type hold its ID, so nothing else
// changes.
func (s *Session) setTypeSchema(ctx context.Context, txn *kvclient.Txn, t *parser.SetSchema, tag string) (*Result, error) {
	td, err := s.lookupType(ctx, txn, t.Name)
	if err != nil {
		return nil, err
	}
	if err := s.checkOwner(ctx, txn, "type", td.Name, td.Owner); err != nil {
		return nil, err
	}
	target, err := catalog.LookupSchema(ctx, txn, td.DatabaseID, t.Schema)
	if err != nil {
		return nil, ToSQLError(err)
	}
	if target.ID == td.SchemaID {
		return &Result{Tag: tag}, nil
	}
	db, err := catalog.ReadDatabase(ctx, txn, td.DatabaseID)
	if err != nil {
		return nil, err
	}
	if db == nil {
		db = &catalog.DatabaseDescriptor{Name: s.database}
	}
	if err := s.checkCreateInSchema(ctx, txn, &catalog.Target{Database: db.Name, DBID: td.DatabaseID, Schema: target, Name: td.Name}); err != nil {
		return nil, err
	}
	if err := catalog.SetTypeSchema(ctx, txn, td, target.ID); err != nil {
		var ex *catalog.ErrTypeExists
		if asErr(err, &ex) {
			return nil, newErrf(CodeDuplicateObject, "type %q already exists in schema %q", td.Name, target.Name)
		}
		return nil, ToSQLError(err)
	}
	log.Audit("schema-ddl", "stmt", tag+" SET SCHEMA", "target", t.Name, "schema", target.Name, "principal", s.sessionUser, "role", s.user)
	return &Result{Tag: tag}, nil
}

// schemaOID is the pg_namespace OID of schema name of the current
// database: the built-in schemas' fixed OIDs, a user schema's derived
// from its ID.
func (s *Session) schemaOID(ctx context.Context, txn *kvclient.Txn, name string) (int64, bool, error) {
	if oid, ok := schemaOIDs[name]; ok {
		return oid, true, nil
	}
	if txn == nil || catalog.IsSystemSchemaName(name) {
		return 0, false, nil
	}
	_, sd, err := s.lookupSchemaOrErr(ctx, txn, name)
	if err != nil {
		if ToSQLError(err).Code == CodeInvalidSchemaName {
			return 0, false, nil
		}
		return 0, false, err
	}
	return vtable.SchemaOID(sd.ID), true, nil
}

// schemaByOID is the name of the schema of the current database with
// pg_namespace OID oid.
func (s *Session) schemaByOID(ctx context.Context, txn *kvclient.Txn, oid int64) (string, bool, error) {
	for name, o := range schemaOIDs {
		if o == oid {
			return name, true, nil
		}
	}
	id, ok := vtable.SchemaIDOfOID(oid)
	if !ok || txn == nil {
		return "", false, nil
	}
	sd, err := catalog.ReadSchema(ctx, txn, id)
	if err != nil || sd == nil {
		return "", false, err
	}
	dbID, err := s.cat.DatabaseID(ctx, txn, s.database)
	if err != nil || sd.DatabaseID != dbID {
		return "", false, nil
	}
	return sd.Name, true, nil
}

// viewResolutionPath is the search path a view's query resolves
// through: the one stored with it, or public alone for a view made
// before schemas (how every such view resolved).
func viewResolutionPath(stored []string) []string {
	if len(stored) == 0 {
		return []string{catalog.PublicSchema}
	}
	return stored
}

// pathSchemasAnyTxn is pathSchemas in txn, or in a transaction of its
// own when there is none (a constant folded at plan time).
func (s *Session) pathSchemasAnyTxn(ctx context.Context, txn *kvclient.Txn) ([]*catalog.SchemaDescriptor, error) {
	if txn != nil {
		return s.pathSchemas(ctx, txn)
	}
	var out []*catalog.SchemaDescriptor
	err := s.db.RunTxn(ctx, "search-path", func(ctx context.Context, txn *kvclient.Txn) error {
		var err error
		out, err = s.pathSchemas(ctx, txn)
		return err
	})
	return out, err
}
