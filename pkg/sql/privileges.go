package sql

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/sthorne/datax/pkg/kvclient"
	"github.com/sthorne/datax/pkg/sql/catalog"
	"github.com/sthorne/datax/pkg/sql/parser"
	"github.com/sthorne/datax/pkg/util/log"
	"github.com/sthorne/datax/pkg/version"
)

// Authorization (issue #98). Every check runs as the session's current
// role (SET ROLE changes it; a view's query runs as the view's owner) and
// resolves that role's effective set: itself, public, and — following
// INHERIT — every role it belongs to. root and the admin role's members
// may do anything; an object's owner holds every privilege on it and may
// alter or drop it; everyone else needs a grant on the object (to
// itself, one of its roles, or PUBLIC), or membership in read_all /
// write_all. In insecure (trust) mode the session user is client-claimed,
// so enforcement is advisory there — documented in docs/user/security.md.

// The grantable privileges by object kind, in storage order.
var (
	tablePrivOrder    = []string{"SELECT", "INSERT", "UPDATE", "DELETE", "TRUNCATE"}
	sequencePrivOrder = []string{catalog.PrivUsage, "SELECT", "UPDATE"}
	databasePrivOrder = []string{catalog.PrivCreate, catalog.PrivConnect}
	schemaPrivOrder   = []string{catalog.PrivUsage, catalog.PrivCreate}
)

func privOrderFor(kind string) []string {
	switch kind {
	case "sequence":
		return sequencePrivOrder
	case "database":
		return databasePrivOrder
	case "schema":
		return schemaPrivOrder
	}
	return tablePrivOrder
}

// expandPrivileges validates a privilege list for an object kind and
// expands ALL; the result is in storage order.
func expandPrivileges(privs []string, kind string) ([]string, error) {
	order := privOrderFor(kind)
	set := map[string]bool{}
	for _, p := range privs {
		if p == "ALL" {
			for _, o := range order {
				set[o] = true
			}
			continue
		}
		known := false
		for _, o := range order {
			if o == p {
				known = true
			}
		}
		if !known {
			return nil, newErrf(CodeInvalidGrantOperation, "invalid privilege type %s for %s (%s privileges: %s, ALL)", p, kind, kind, strings.Join(order, ", "))
		}
		set[p] = true
	}
	var out []string
	for _, o := range order {
		if set[o] {
			out = append(out, o)
		}
	}
	return out, nil
}

// requireV11 gates the statements that write v11 role and ownership
// state before the cluster version is finalized.
func (s *Session) requireV11(what string) error {
	if s.db.ClusterVersion() < version.V11 {
		return newErrf(CodeFeatureNotSupported, "%s need cluster version v11: finalize the upgrade with `datax debug upgrade` first", what)
	}
	return nil
}

func (s *Session) rolesFinalized() bool { return s.db.ClusterVersion() >= version.V11 }

// actor is the identity privilege checks run as: the view owner while a
// view's query executes, otherwise the current role.
func (s *Session) actor() string {
	if s.privAs != "" {
		return s.privAs
	}
	return s.user
}

// roleSet returns the actor's effective roles, resolved once per
// transaction (role DDL invalidates the cache).
func (s *Session) roleSet(ctx context.Context, txn *kvclient.Txn) (catalog.RoleSet, error) {
	name := s.actor()
	if s.roleCache == nil || s.roleCacheTxn != txn {
		s.roleCache = map[string]catalog.RoleSet{}
		s.roleCacheTxn = txn
	}
	if set, ok := s.roleCache[name]; ok {
		return set, nil
	}
	set, err := catalog.LazyRoleGraph(ctx, txn).Effective(name)
	if err != nil {
		return nil, err
	}
	s.roleCache[name] = set
	return set, nil
}

// invalidateRoles forgets the resolved role sets (after role DDL).
func (s *Session) invalidateRoles() {
	s.roleCache = nil
	s.roleCacheTxn = nil
}

// requiresAdmin reports whether a statement is restricted to the admin
// role outright: creating databases and cluster-wide maintenance.
// Everything else is checked against ownership and grants by its
// executor.
func requiresAdmin(stmt parser.Statement) bool {
	switch stmt.(type) {
	case *parser.CreateDatabase, *parser.Analyze:
		return true
	}
	return false
}

// isAdmin reports whether the actor is root or holds the admin role.
func (s *Session) isAdmin(ctx context.Context, txn *kvclient.Txn) (bool, error) {
	if s.actor() == catalog.RootRole {
		return true, nil
	}
	set, err := s.roleSet(ctx, txn)
	if err != nil {
		return false, err
	}
	return set.IsAdmin(), nil
}

// checkAdmin returns 42501 unless the actor is root or an admin.
func (s *Session) checkAdmin(ctx context.Context, txn *kvclient.Txn) error {
	ok, err := s.isAdmin(ctx, txn)
	if err != nil {
		return err
	}
	if !ok {
		return newErrf(CodeInsufficientPriv, "permission denied: %q is not an admin", s.actor())
	}
	return nil
}

// isOwner reports whether the actor is root, an admin, or holds the
// owning role (directly or through membership).
func (s *Session) isOwner(ctx context.Context, txn *kvclient.Txn, owner string) (bool, error) {
	if s.actor() == catalog.RootRole {
		return true, nil
	}
	set, err := s.roleSet(ctx, txn)
	if err != nil {
		return false, err
	}
	return set.IsAdmin() || set.Has(catalog.OwnerOf(owner)), nil
}

// checkOwner returns 42501 unless the actor owns the object (or is an
// admin): the gate on ALTER, DROP and GRANT of an object.
func (s *Session) checkOwner(ctx context.Context, txn *kvclient.Txn, kind, name, owner string) error {
	ok, err := s.isOwner(ctx, txn, owner)
	if err != nil {
		return err
	}
	if !ok {
		return newErrf(CodeInsufficientPriv, "must be owner of %s %s (owner %q; %q is not a member)", kind, name, catalog.OwnerOf(owner), s.actor())
	}
	return nil
}

// checkTableOwner resolves a table or view and applies checkOwner.
func (s *Session) checkTableOwner(ctx context.Context, txn *kvclient.Txn, desc *catalog.TableDescriptor) error {
	kind := "table"
	if desc.IsView() {
		kind = "view"
	}
	return s.checkOwner(ctx, txn, kind, desc.Name, desc.Owner)
}

// checkTableOwnerNoTxn is checkTableOwner for the online DDL paths that
// run outside a statement transaction.
func (s *Session) checkTableOwnerNoTxn(ctx context.Context, name string) *Error {
	var perr error
	if err := s.db.RunTxn(ctx, "owner-check", func(ctx context.Context, txn *kvclient.Txn) error {
		desc, err := s.lookup(ctx, txn, name)
		if err != nil {
			perr = err
			return nil
		}
		perr = s.checkTableOwner(ctx, txn, desc)
		return nil
	}); err != nil {
		return ToSQLError(err)
	}
	if perr != nil {
		return ToSQLError(perr)
	}
	return nil
}

// tablePrivAllowed reports whether the actor may exercise priv on the
// table or view.
func (s *Session) tablePrivAllowed(ctx context.Context, txn *kvclient.Txn, desc *catalog.TableDescriptor, priv string) (bool, error) {
	if s.actor() == catalog.RootRole {
		return true, nil
	}
	if strings.HasPrefix(desc.Virtual, relationPrefix) && priv == "SELECT" {
		return true, nil // a bound relation: its sources were checked when it was made
	}
	set, err := s.roleSet(ctx, txn)
	if err != nil {
		return false, err
	}
	if set.IsAdmin() {
		return true, nil
	}
	if catalog.IsSystemTable(desc.Name) && priv != "SELECT" {
		// Only the cluster and its admins write the metrics table; a
		// SELECT grant is how a reporting user reads it.
		return false, nil
	}
	allowed := false
	switch {
	case set.Has(catalog.OwnerOf(desc.Owner)):
		allowed = true
	case catalog.HasPrivilege(desc.Privileges, set, priv):
		allowed = true
	case priv == "SELECT" && set.Has(catalog.ReadAllRole):
		allowed = true
	case priv != "SELECT" && set.Has(catalog.WriteAllRole):
		allowed = true
	case priv == "TRUNCATE" && catalog.HasPrivilege(desc.Privileges, set, "DELETE"):
		allowed = true // DELETE covered TRUNCATE before v11; ALL still grants both
	}
	if !allowed {
		return false, nil
	}
	// The schema: USAGE on public is everyone's unless revoked from PUBLIC.
	usage, err := s.schemaUsageAllowed(ctx, txn, desc.DatabaseID, set)
	if err != nil {
		return false, err
	}
	return usage, nil
}

// schemaUsageAllowed reports whether set may reach the objects of the
// public schema of the database (USAGE): always, unless REVOKE USAGE ON
// SCHEMA public FROM PUBLIC restricted it to holders and admins.
func (s *Session) schemaUsageAllowed(ctx context.Context, txn *kvclient.Txn, dbID uint64, set catalog.RoleSet) (bool, error) {
	if dbID == 0 || set.IsAdmin() {
		return true, nil
	}
	db, err := catalog.ReadDatabase(ctx, txn, dbID)
	if err != nil || db == nil {
		return err == nil, err
	}
	if !db.UsageRestricted || set.Has(catalog.OwnerOf(db.Owner)) {
		return true, nil
	}
	return catalog.HasPrivilege(db.SchemaPrivileges, set, catalog.PrivUsage), nil
}

// checkTablePriv returns 42501 unless the actor holds priv on the table.
func (s *Session) checkTablePriv(ctx context.Context, txn *kvclient.Txn, desc *catalog.TableDescriptor, priv string) error {
	ok, err := s.tablePrivAllowed(ctx, txn, desc, priv)
	if err != nil {
		return err
	}
	if !ok {
		if catalog.IsSystemTable(desc.Name) && priv != "SELECT" {
			return newErrf(CodeInsufficientPriv, "permission denied for table %q: only admins may %s it", desc.Name, priv)
		}
		kind := "table"
		if desc.IsView() {
			kind = "view"
		}
		return newErrf(CodeInsufficientPriv, "permission denied for %s %q (%s as user %q)", kind, desc.Name, priv, s.actor())
	}
	return nil
}

// sequencePrivAllowed reports whether the actor may exercise priv
// (USAGE, SELECT, UPDATE) on the sequence: root, admins, the owner,
// grantees, read_all (SELECT) and write_all (USAGE, UPDATE) — and, for a
// sequence a column owns, whoever may write the table (INSERT for
// nextval, UPDATE for setval; SELECT for currval), so SERIAL columns
// need no separate grant.
func (s *Session) sequencePrivAllowed(ctx context.Context, txn *kvclient.Txn, d *catalog.SequenceDescriptor, priv string) (bool, error) {
	if s.actor() == catalog.RootRole {
		return true, nil
	}
	set, err := s.roleSet(ctx, txn)
	if err != nil {
		return false, err
	}
	switch {
	case set.IsAdmin(), set.Has(catalog.OwnerOf(d.Owner)):
		return true, nil
	case catalog.HasPrivilege(d.Privileges, set, priv):
		return true, nil
	case priv != "UPDATE" && catalog.HasPrivilege(d.Privileges, set, catalog.PrivUsage):
		return true, nil // USAGE covers currval and nextval
	case priv == "SELECT" && set.Has(catalog.ReadAllRole):
		return true, nil
	case priv != "SELECT" && set.Has(catalog.WriteAllRole):
		return true, nil
	}
	if d.OwnerTable != 0 {
		owner, err := catalog.ReadTable(ctx, txn, d.OwnerTable)
		if err == nil && owner != nil {
			tablePriv := map[string]string{catalog.PrivUsage: "INSERT", "SELECT": "SELECT", "UPDATE": "UPDATE"}[priv]
			return s.tablePrivAllowed(ctx, txn, owner, tablePriv)
		}
	}
	return false, nil
}

// checkSequencePriv returns 42501 unless the actor holds priv on the
// sequence.
func (s *Session) checkSequencePriv(ctx context.Context, txn *kvclient.Txn, d *catalog.SequenceDescriptor, priv string) error {
	ok, err := s.sequencePrivAllowed(ctx, txn, d, priv)
	if err != nil {
		return err
	}
	if !ok {
		return newErrf(CodeInsufficientPriv, "permission denied for sequence %q (%s as user %q)", d.Name, priv, s.actor())
	}
	return nil
}

// databasePrivAllowed reports whether the actor holds priv (CREATE,
// CONNECT) on the database: root, admins, the owner, grantees; CONNECT
// is everyone's unless revoked from PUBLIC; CREATE is also conferred by
// CREATE on the public schema.
func (s *Session) databasePrivAllowed(ctx context.Context, txn *kvclient.Txn, db *catalog.DatabaseDescriptor, priv string) (bool, error) {
	if s.actor() == catalog.RootRole {
		return true, nil
	}
	set, err := s.roleSet(ctx, txn)
	if err != nil {
		return false, err
	}
	switch {
	case set.IsAdmin(), set.Has(catalog.OwnerOf(db.Owner)):
		return true, nil
	case catalog.HasPrivilege(db.Privileges, set, priv):
		return true, nil
	case priv == catalog.PrivConnect && !db.ConnectRestricted:
		return true, nil
	case priv == catalog.PrivCreate && catalog.HasPrivilege(db.SchemaPrivileges, set, catalog.PrivCreate):
		return true, nil
	}
	return false, nil
}

// checkCreateInDatabase admits CREATE TABLE / VIEW / SEQUENCE / TYPE for
// root, admins, the database's owner, and roles holding CREATE on the
// database or its public schema.
func (s *Session) checkCreateInDatabase(ctx context.Context, txn *kvclient.Txn, table string) error {
	if s.actor() == catalog.RootRole {
		return nil
	}
	dbName, _ := catalog.SplitTableName(table)
	if dbName == "" {
		dbName = s.database
	}
	db, err := s.cat.Database(ctx, txn, dbName)
	if err != nil {
		return ToSQLError(err)
	}
	ok, err := s.databasePrivAllowed(ctx, txn, db, catalog.PrivCreate)
	if err != nil {
		return err
	}
	if !ok {
		return newErrf(CodeInsufficientPriv, "permission denied: %q needs the admin role or CREATE on database %q", s.actor(), dbName)
	}
	return nil
}

// canGrantOn reports whether the actor may grant privs on an object:
// root, admins, the owner, or a holder of every one of them WITH GRANT
// OPTION.
func (s *Session) canGrantOn(ctx context.Context, txn *kvclient.Txn, owner string, grantOptions map[string][]string, privs []string) (bool, error) {
	ok, err := s.isOwner(ctx, txn, owner)
	if err != nil || ok {
		return ok, err
	}
	set, err := s.roleSet(ctx, txn)
	if err != nil {
		return false, err
	}
	for _, p := range privs {
		if !catalog.HasPrivilege(grantOptions, set, p) {
			return false, nil
		}
	}
	return true, nil
}

// applyGrant edits a privilege map pair for one grantee.
func applyGrant(privs, options *map[string][]string, grantee string, set []string, order []string, revoke, optionOnly, withOption bool) bool {
	changed := false
	switch {
	case revoke && optionOnly:
		changed = catalog.RemovePrivileges(*options, grantee, set, order)
	case revoke:
		changed = catalog.RemovePrivileges(*privs, grantee, set, order)
		if catalog.RemovePrivileges(*options, grantee, set, order) {
			changed = true
		}
	default:
		var c bool
		*privs, c = catalog.AddPrivileges(*privs, grantee, set, order)
		changed = c
		if withOption {
			*options, c = catalog.AddPrivileges(*options, grantee, set, order)
			changed = changed || c
		}
	}
	if len(*privs) == 0 {
		*privs = nil
	}
	if len(*options) == 0 {
		*options = nil
	}
	return changed
}

// resolveGrantees checks that every grantee names an existing role (or
// PUBLIC) and returns them lower-cased.
func (s *Session) resolveGrantees(ctx context.Context, txn *kvclient.Txn, names []string) ([]string, error) {
	out := make([]string, 0, len(names))
	for _, n := range names {
		n = strings.ToLower(n)
		if n != catalog.PublicRole {
			d, err := catalog.LookupRole(ctx, txn, n)
			if err != nil {
				return nil, ToSQLError(err)
			}
			if d == nil {
				if !s.rolesFinalized() {
					// Before v11 a grantee is any name: the user may be
					// created later, as before roles existed.
					out = append(out, n)
					continue
				}
				return nil, newErrf(CodeUndefinedObject, "role %q does not exist", n)
			}
		}
		out = append(out, n)
	}
	return out, nil
}

// execGrantRevoke applies GRANT / REVOKE: role membership, or privileges
// on tables, views, sequences, databases and the public schema.
func (s *Session) execGrantRevoke(ctx context.Context, txn *kvclient.Txn, t *parser.GrantRevoke) (*Result, error) {
	tag := "GRANT"
	if t.Revoke {
		tag = "REVOKE"
	}
	if len(t.Roles) > 0 {
		return s.execGrantRole(ctx, txn, t, tag)
	}
	if t.GrantOption || t.ObjectKind == "sequence" || t.ObjectKind == "schema" || t.AllInSchema {
		if err := s.requireV11("grant options, sequence and schema grants, and ALL TABLES grants"); err != nil {
			return nil, err
		}
	}
	privs, err := expandPrivileges(t.Privileges, t.ObjectKind)
	if err != nil {
		return nil, err
	}
	grantees, err := s.resolveGrantees(ctx, txn, t.Grantees)
	if err != nil {
		return nil, err
	}
	var targets []string
	switch t.ObjectKind {
	case "database":
		for _, name := range t.Objects {
			if err := s.grantOnDatabase(ctx, txn, name, privs, grantees, t); err != nil {
				return nil, err
			}
			targets = append(targets, name)
		}
	case "schema":
		if err := s.grantOnSchema(ctx, txn, privs, grantees, t); err != nil {
			return nil, err
		}
		targets = append(targets, s.database+".public")
	case "sequence":
		var seqs []*catalog.SequenceDescriptor
		if t.AllInSchema {
			dbID, err := s.cat.DatabaseID(ctx, txn, s.database)
			if err != nil {
				return nil, ToSQLError(err)
			}
			if seqs, err = catalog.ListSequences(ctx, txn, dbID); err != nil {
				return nil, err
			}
		} else {
			for _, name := range t.Objects {
				d, err := s.lookupSequence(ctx, txn, name)
				if err != nil {
					return nil, err
				}
				seqs = append(seqs, d)
			}
		}
		for _, d := range seqs {
			ok, err := s.canGrantOn(ctx, txn, d.Owner, d.GrantOptions, privs)
			if err != nil {
				return nil, err
			}
			if !ok {
				return nil, newErrf(CodeInsufficientPriv, "permission denied to %s on sequence %q (%q is not its owner and holds no grant option)", strings.ToLower(tag), d.Name, s.actor())
			}
			changed := false
			for _, g := range grantees {
				if applyGrant(&d.Privileges, &d.GrantOptions, g, privs, sequencePrivOrder, t.Revoke, t.GrantOption && t.Revoke, t.GrantOption) {
					changed = true
				}
			}
			if changed {
				if err := catalog.UpdateSequence(ctx, txn, d); err != nil {
					return nil, err
				}
			}
			targets = append(targets, d.Name)
		}
	default:
		var descs []*catalog.TableDescriptor
		var given []string // the names as the statement spelled them, for the lease drain
		if t.AllInSchema {
			db, err := s.cat.Database(ctx, txn, s.database)
			if err != nil {
				return nil, ToSQLError(err)
			}
			all, err := s.cat.ListIn(ctx, txn, db)
			if err != nil {
				return nil, err
			}
			for _, d := range all {
				if !catalog.IsSystemTable(d.Name) {
					descs = append(descs, d)
					given = append(given, d.Name)
				}
			}
		} else {
			for _, name := range t.Objects {
				shared, err := s.lookup(ctx, txn, name)
				if err != nil {
					return nil, err
				}
				if shared.Virtual != "" {
					return nil, newErrf(CodeInsufficientPriv, "%s is a system catalog and cannot be granted on", shared.Virtual)
				}
				descs = append(descs, shared)
				given = append(given, name)
			}
		}
		for i, shared := range descs {
			ok, err := s.canGrantOn(ctx, txn, shared.Owner, shared.GrantOptions, privs)
			if err != nil {
				return nil, err
			}
			if !ok {
				return nil, newErrf(CodeInsufficientPriv, "permission denied to %s on table %q (%q is not its owner and holds no grant option)", strings.ToLower(tag), shared.Name, s.actor())
			}
			desc := shared.Clone()
			changed := false
			for _, g := range grantees {
				if applyGrant(&desc.Privileges, &desc.GrantOptions, g, privs, tablePrivOrder, t.Revoke, t.GrantOption && t.Revoke, t.GrantOption) {
					changed = true
				}
			}
			if changed {
				if err := s.cat.Update(ctx, txn, desc); err != nil {
					return nil, err
				}
				if t.AllInSchema || len(t.Objects) > 1 {
					s.extraDDL = append(s.extraDDL, given[i])
				}
			}
			targets = append(targets, desc.Name)
		}
	}
	log.Audit("privilege-ddl", "stmt", tag, "privileges", strings.Join(t.Privileges, ","), "kind", t.ObjectKind,
		"targets", strings.Join(targets, ","), "grantees", strings.Join(grantees, ","), "grant_option", t.GrantOption,
		"principal", s.sessionUser, "role", s.user)
	return &Result{Tag: tag}, nil
}

// grantOnDatabase applies a database grant. CONNECT to PUBLIC is the
// default everyone has, revocable (ConnectRestricted).
func (s *Session) grantOnDatabase(ctx context.Context, txn *kvclient.Txn, name string, privs, grantees []string, t *parser.GrantRevoke) error {
	if err := s.requireV6(ctx, txn); err != nil {
		return err
	}
	db, err := catalog.LookupDatabase(ctx, txn, name)
	if err != nil {
		return ToSQLError(err)
	}
	if db.ID == 0 {
		return errBeforeV6()
	}
	ok, err := s.canGrantOn(ctx, txn, db.Owner, db.GrantOptions, privs)
	if err != nil {
		return err
	}
	if !ok {
		return newErrf(CodeInsufficientPriv, "permission denied for database %q (%q is not its owner and holds no grant option)", name, s.actor())
	}
	db = db.Clone()
	changed := false
	for _, g := range grantees {
		set := privs
		if g == catalog.PublicRole {
			var rest []string
			for _, p := range privs {
				if p == catalog.PrivConnect {
					if db.ConnectRestricted != t.Revoke {
						db.ConnectRestricted = t.Revoke
						changed = true
					}
					continue
				}
				rest = append(rest, p)
			}
			set = rest
		}
		if applyGrant(&db.Privileges, &db.GrantOptions, g, set, databasePrivOrder, t.Revoke, t.GrantOption && t.Revoke, t.GrantOption) {
			changed = true
		}
	}
	if changed {
		return s.cat.UpdateDatabase(ctx, txn, db)
	}
	return nil
}

// grantOnSchema applies a grant on the public schema of the current
// database. USAGE to PUBLIC is the default, revocable (UsageRestricted).
func (s *Session) grantOnSchema(ctx context.Context, txn *kvclient.Txn, privs, grantees []string, t *parser.GrantRevoke) error {
	if err := s.requireV6(ctx, txn); err != nil {
		return err
	}
	db, err := catalog.LookupDatabase(ctx, txn, s.database)
	if err != nil {
		return ToSQLError(err)
	}
	if db.ID == 0 {
		return errBeforeV6()
	}
	ok, err := s.canGrantOn(ctx, txn, db.Owner, db.SchemaGrantOptions, privs)
	if err != nil {
		return err
	}
	if !ok {
		return newErrf(CodeInsufficientPriv, "permission denied for schema public of database %q (%q is not its owner and holds no grant option)", db.Name, s.actor())
	}
	db = db.Clone()
	changed := false
	for _, g := range grantees {
		set := privs
		if g == catalog.PublicRole {
			var rest []string
			for _, p := range privs {
				if p == catalog.PrivUsage {
					if db.UsageRestricted != t.Revoke {
						db.UsageRestricted = t.Revoke
						changed = true
					}
					continue
				}
				rest = append(rest, p)
			}
			set = rest
		}
		if applyGrant(&db.SchemaPrivileges, &db.SchemaGrantOptions, g, set, schemaPrivOrder, t.Revoke, t.GrantOption && t.Revoke, t.GrantOption) {
			changed = true
		}
	}
	if changed {
		return s.cat.UpdateDatabase(ctx, txn, db)
	}
	return nil
}

// applyDefaultPrivileges stamps the grants ALTER DEFAULT PRIVILEGES
// prepared for objects owner creates in the database: kind is "TABLES"
// or "SEQUENCES".
func (s *Session) applyDefaultPrivileges(ctx context.Context, txn *kvclient.Txn, dbID uint64, owner, kind string, privs, options *map[string][]string) error {
	db, err := catalog.ReadDatabase(ctx, txn, dbID)
	if err != nil || db == nil {
		return err
	}
	order := tablePrivOrder
	if kind == "SEQUENCES" {
		order = sequencePrivOrder
	}
	for _, dp := range db.DefaultPrivileges {
		if dp.Object != kind || dp.ForRole != catalog.OwnerOf(owner) {
			continue
		}
		applyGrant(privs, options, dp.Grantee, dp.Privileges, order, false, false, dp.GrantOption)
	}
	return nil
}

// execAlterDefaultPrivileges records (or removes) a default-privilege
// rule of the current database.
func (s *Session) execAlterDefaultPrivileges(ctx context.Context, txn *kvclient.Txn, t *parser.AlterDefaultPrivileges) (*Result, error) {
	if err := s.requireV11("default privileges"); err != nil {
		return nil, err
	}
	kind := "TABLES"
	if t.ObjectKind == "sequences" {
		kind = "SEQUENCES"
	}
	privs, err := expandPrivileges(t.Privileges, strings.TrimSuffix(strings.ToLower(kind), "s"))
	if err != nil {
		return nil, err
	}
	forRoles := t.ForRoles
	if len(forRoles) == 0 {
		forRoles = []string{s.user}
	}
	set, err := s.roleSet(ctx, txn)
	if err != nil {
		return nil, err
	}
	for i, r := range forRoles {
		r = strings.ToLower(r)
		forRoles[i] = r
		d, err := catalog.LookupRole(ctx, txn, r)
		if err != nil {
			return nil, ToSQLError(err)
		}
		if d == nil {
			return nil, newErrf(CodeUndefinedObject, "role %q does not exist", r)
		}
		if !set.IsAdmin() && !set.Has(r) {
			return nil, newErrf(CodeInsufficientPriv, "permission denied to change default privileges for role %q (%q is not a member)", r, s.actor())
		}
	}
	grantees, err := s.resolveGrantees(ctx, txn, t.Grantees)
	if err != nil {
		return nil, err
	}
	db, err := catalog.LookupDatabase(ctx, txn, s.database)
	if err != nil {
		return nil, ToSQLError(err)
	}
	if db.ID == 0 {
		return nil, errBeforeV6()
	}
	db = db.Clone()
	order := privOrderFor(strings.TrimSuffix(strings.ToLower(kind), "s"))
	for _, r := range forRoles {
		for _, g := range grantees {
			idx := -1
			for i, dp := range db.DefaultPrivileges {
				if dp.ForRole == r && dp.Object == kind && dp.Grantee == g {
					idx = i
					break
				}
			}
			if t.Revoke {
				if idx < 0 {
					continue
				}
				dp := &db.DefaultPrivileges[idx]
				if t.GrantOption {
					dp.GrantOption = false
					continue
				}
				m := map[string][]string{g: dp.Privileges}
				catalog.RemovePrivileges(m, g, privs, order)
				dp.Privileges = m[g]
				if len(dp.Privileges) == 0 {
					db.DefaultPrivileges = append(db.DefaultPrivileges[:idx], db.DefaultPrivileges[idx+1:]...)
				}
				continue
			}
			if idx < 0 {
				db.DefaultPrivileges = append(db.DefaultPrivileges, catalog.DefaultPrivilege{ForRole: r, Object: kind, Grantee: g})
				idx = len(db.DefaultPrivileges) - 1
			}
			dp := &db.DefaultPrivileges[idx]
			m := map[string][]string{g: dp.Privileges}
			m, _ = catalog.AddPrivileges(m, g, privs, order)
			dp.Privileges = m[g]
			dp.GrantOption = dp.GrantOption || t.GrantOption
		}
	}
	if len(db.DefaultPrivileges) == 0 {
		db.DefaultPrivileges = nil
	}
	if err := s.cat.UpdateDatabase(ctx, txn, db); err != nil {
		return nil, err
	}
	tag := "ALTER DEFAULT PRIVILEGES"
	log.Audit("privilege-ddl", "stmt", tag, "for", strings.Join(forRoles, ","), "kind", kind, "privileges", strings.Join(t.Privileges, ","),
		"grantees", strings.Join(grantees, ","), "revoke", t.Revoke, "principal", s.sessionUser, "role", s.user)
	return &Result{Tag: tag}, nil
}

// canSeeTable reports whether a role set may see a table in the
// catalogs: admins, the owner, any grantee, read_all and write_all.
func canSeeTable(set catalog.RoleSet, d *catalog.TableDescriptor) bool {
	if set.IsAdmin() || set.Has(catalog.OwnerOf(d.Owner)) || set.Has(catalog.ReadAllRole) || set.Has(catalog.WriteAllRole) {
		return true
	}
	for g := range d.Privileges {
		if set.Has(g) {
			return true
		}
	}
	return false
}

// grantRows renders SHOW GRANTS: database_name, schema_name,
// relation_name, grantee, privilege_type, is_grantable.
func (s *Session) grantRows(ctx context.Context, txn *kvclient.Txn, t *parser.Show) ([][]string, error) {
	var rows [][]string
	add := func(dbName, schema, rel string, privs, options map[string][]string) {
		grantees := make([]string, 0, len(privs))
		for g := range privs {
			grantees = append(grantees, g)
		}
		sort.Strings(grantees)
		for _, g := range grantees {
			if t.User != "" && g != strings.ToLower(t.User) {
				continue
			}
			opt := map[string]bool{}
			for _, p := range options[g] {
				opt[p] = true
			}
			for _, p := range privs[g] {
				grantable := "NO"
				if opt[p] {
					grantable = "YES"
				}
				rows = append(rows, []string{dbName, schema, rel, g, p, grantable})
			}
		}
	}
	switch {
	case t.Database != "":
		db, err := catalog.LookupDatabase(ctx, txn, t.Database)
		if err != nil {
			return nil, ToSQLError(err)
		}
		add(db.Name, "", "", db.Privileges, db.GrantOptions)
		add(db.Name, catalog.PublicSchema, "", db.SchemaPrivileges, db.SchemaGrantOptions)
		return rows, nil
	case t.Table != "":
		d, err := s.lookup(ctx, txn, t.Table)
		if err != nil {
			return nil, err
		}
		names, err := s.databaseNames(ctx, txn)
		if err != nil {
			return nil, err
		}
		dbName := names[d.DatabaseID]
		if dbName == "" {
			dbName = s.database
		}
		add(dbName, catalog.PublicSchema, d.Name, d.Privileges, d.GrantOptions)
		return rows, nil
	}
	// Everything in the current database: its own grants, the schema's,
	// every visible table's and sequence's.
	db, err := s.cat.Database(ctx, txn, s.database)
	if err != nil {
		return nil, ToSQLError(err)
	}
	add(db.Name, "", "", db.Privileges, db.GrantOptions)
	add(db.Name, catalog.PublicSchema, "", db.SchemaPrivileges, db.SchemaGrantOptions)
	set, err := s.roleSet(ctx, txn)
	if err != nil {
		return nil, err
	}
	tables, err := s.cat.ListIn(ctx, txn, db)
	if err != nil {
		return nil, err
	}
	for _, d := range tables {
		if canSeeTable(set, d) {
			add(db.Name, catalog.PublicSchema, d.Name, d.Privileges, d.GrantOptions)
		}
	}
	seqs, err := catalog.ListSequences(ctx, txn, db.ID)
	if err != nil {
		return nil, err
	}
	for _, sd := range seqs {
		add(db.Name, catalog.PublicSchema, sd.Name, sd.Privileges, sd.GrantOptions)
	}
	return rows, nil
}

// roleGrantRows renders SHOW GRANTS ON ROLE [r] [FOR member]: role_name,
// member, is_admin.
func (s *Session) roleGrantRows(ctx context.Context, txn *kvclient.Txn, t *parser.Show) ([][]string, error) {
	roles, err := catalog.ListRoles(ctx, txn)
	if err != nil {
		return nil, err
	}
	var rows [][]string
	if t.Role == "" || t.Role == catalog.AdminRole {
		if t.User == "" || t.User == catalog.RootRole {
			rows = append(rows, []string{catalog.AdminRole, catalog.RootRole, "YES"})
		}
	}
	for _, r := range roles {
		if t.User != "" && r.Name != strings.ToLower(t.User) {
			continue
		}
		for _, m := range r.MemberOf {
			if t.Role != "" && m.Role != strings.ToLower(t.Role) {
				continue
			}
			admin := "NO"
			if m.Admin {
				admin = "YES"
			}
			rows = append(rows, []string{m.Role, r.Name, admin})
		}
	}
	sort.Slice(rows, func(i, j int) bool {
		if rows[i][0] != rows[j][0] {
			return rows[i][0] < rows[j][0]
		}
		return rows[i][1] < rows[j][1]
	})
	return rows, nil
}

var _ = fmt.Sprintf
