package sql

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/kvclient"
	"github.com/sthorne/datax/pkg/security"
	"github.com/sthorne/datax/pkg/sql/catalog"
	"github.com/sthorne/datax/pkg/sql/parser"
	"github.com/sthorne/datax/pkg/sql/types"
	"github.com/sthorne/datax/pkg/util/log"
)

// Roles (issue #98): CREATE / ALTER / DROP ROLE and USER, membership
// (GRANT role TO role), ownership (OWNER TO, REASSIGN OWNED, DROP OWNED)
// and SET ROLE. Only SCRAM verifiers are ever stored (never plaintext).
// Before cluster version v11 the pre-role statements keep writing the
// old layout (see pkg/sql/catalog/role.go); the new ones are refused.

// reservedRoleName refuses the names a role cannot take.
func reservedRoleName(name string) error {
	switch {
	case name == security.NodePrincipal:
		// The node certificate's CommonName is an admin principal on the
		// HTTP and admin-RPC surfaces; a role of that name would gain
		// that authority through HTTP Basic auth and leave audit records
		// indistinguishable from the cluster's own.
		return newErrf(CodeInvalidParameterValue, "role name %q is reserved for the cluster's node identity", name)
	case name == catalog.PublicRole:
		return newErrf(CodeInvalidParameterValue, "role name %q is reserved", name)
	case catalog.IsBuiltinRole(name):
		return newErrf(CodeInvalidParameterValue, "role %q is built in and cannot be created, altered or dropped", name)
	case name == "":
		return newErrf(CodeSyntaxError, "role name must not be empty")
	}
	return nil
}

// lookupRoleOrErr reads a role, 42704 when missing.
func (s *Session) lookupRoleOrErr(ctx context.Context, txn *kvclient.Txn, name string) (*catalog.RoleDescriptor, error) {
	d, err := catalog.LookupRole(ctx, txn, name)
	if err != nil {
		return nil, ToSQLError(err)
	}
	if d == nil {
		return nil, newErrf(CodeUndefinedObject, "role %q does not exist", name)
	}
	return d, nil
}

func makeVerifier(password string) (json.RawMessage, error) {
	if password == "" {
		return nil, newErrf(CodeSyntaxError, "password must not be empty")
	}
	v, err := security.MakeScramVerifier(password)
	if err != nil {
		return nil, newErrf(CodeInternal, "%v", err)
	}
	raw, err := security.MarshalVerifier(v)
	if err != nil {
		return nil, newErrf(CodeInternal, "%v", err)
	}
	return raw, nil
}

// execCreateRole runs CREATE ROLE / USER and ALTER ROLE / USER. Admins
// only, except that a role may change its own password.
func (s *Session) execCreateRole(ctx context.Context, txn *kvclient.Txn, t *parser.CreateRole) (*Result, error) {
	tag := "CREATE ROLE"
	switch {
	case t.Alter && t.IsUser:
		tag = "ALTER USER"
	case t.Alter:
		tag = "ALTER ROLE"
	case t.IsUser:
		tag = "CREATE USER"
	}
	name := strings.ToLower(t.Name)
	if err := reservedRoleName(name); err != nil {
		return nil, err
	}
	// A role may change its own password and nothing else: every
	// attribute (SUPERUSER, CREATEDB, VALID UNTIL, ...) is an admin's to
	// set, so the statement must carry the password alone.
	selfPassword := t.Alter && name == s.user && t.Password != nil && *t.Password != "" && !roleAttributesSpelled(t)
	if !selfPassword {
		if err := s.checkAdmin(ctx, txn); err != nil {
			return nil, err
		}
	}
	if !s.rolesFinalized() {
		return s.execCreateRoleLegacy(ctx, txn, t, name, tag)
	}
	existing, err := catalog.LookupRole(ctx, txn, name)
	if err != nil {
		return nil, ToSQLError(err)
	}
	var d *catalog.RoleDescriptor
	if t.Alter {
		if existing == nil {
			return nil, newErrf(CodeUndefinedObject, "role %q does not exist", name)
		}
		d = existing.Clone()
	} else {
		if existing != nil {
			if t.IfNotExists {
				return &Result{Tag: tag}, nil
			}
			return nil, newErrf(CodeDuplicateObject, "role %q already exists", name)
		}
		d = &catalog.RoleDescriptor{Name: name, Login: t.IsUser}
	}
	if t.Login != nil {
		if name == catalog.RootRole && !*t.Login {
			return nil, newErrf(CodeFeatureNotSupported, "root must be able to log in")
		}
		d.Login = *t.Login
	}
	if t.Inherit != nil {
		d.NoInherit = !*t.Inherit
	}
	if t.BypassRLS != nil {
		// Admins only (checked above): the attribute lifts every table's
		// row-security policies for the role.
		if *t.BypassRLS {
			if err := s.requireV21RLS("the BYPASSRLS role attribute"); err != nil {
				return nil, err
			}
		}
		d.BypassRLS = *t.BypassRLS
	}
	if err := applyRoleAttributes(d, t); err != nil {
		return nil, err
	}
	if t.Password != nil {
		if *t.Password == "" {
			if name == catalog.RootRole {
				return nil, newErrf(CodeFeatureNotSupported, "root must keep a password")
			}
			d.Verifier = nil
		} else {
			raw, err := makeVerifier(*t.Password)
			if err != nil {
				return nil, err
			}
			d.Verifier = raw
		}
	}
	for _, r := range t.InRoles {
		r = strings.ToLower(r)
		if _, err := s.lookupRoleOrErr(ctx, txn, r); err != nil {
			return nil, err
		}
		if r == name {
			return nil, newErrf(CodeInvalidGrantOperation, "role %q cannot be a member of itself", name)
		}
		d.AddMembership(r, false)
	}
	if err := catalog.PutRole(ctx, txn, d); err != nil {
		return nil, err
	}
	s.invalidateRoles()
	superuser, _ := d.IsMemberOf(catalog.AdminRole)
	log.Audit("role-ddl", "stmt", tag, "target", name, "login", d.Login, "bypassrls", d.BypassRLS, "superuser", superuser, "createdb", d.CreateDB,
		"connlimit", d.ConnectionLimit(), "valid_until", d.ValidUntil, "principal", s.sessionUser, "role", s.user)
	return &Result{Tag: tag}, nil
}

// roleAttributesSpelled reports whether CREATE / ALTER ROLE names any
// attribute besides the password.
func roleAttributesSpelled(t *parser.CreateRole) bool {
	return t.Login != nil || t.Inherit != nil || t.BypassRLS != nil || len(t.InRoles) > 0 ||
		t.Superuser != nil || t.CreateDB != nil || t.CreateRole != nil || t.Replication != nil ||
		t.ConnLimit != nil || t.ValidUntil != nil
}

// applyRoleAttributes sets SUPERUSER, CREATEDB, CREATEROLE, REPLICATION,
// CONNECTION LIMIT and VALID UNTIL on d (PG-S-001). The caller is an
// admin: only an admin reaches here with any of them spelled.
//
// SUPERUSER is datax's admin: the attribute is membership in the
// built-in admin role, as GRANT admin TO r makes it, and NOSUPERUSER
// takes that membership away. root stays a superuser whatever is said.
func applyRoleAttributes(d *catalog.RoleDescriptor, t *parser.CreateRole) error {
	if t.Superuser != nil {
		if *t.Superuser {
			d.AddMembership(catalog.AdminRole, false)
		} else {
			if d.Name == catalog.RootRole {
				return newErrf(CodeFeatureNotSupported, "root is always a superuser")
			}
			d.RemoveMembership(catalog.AdminRole, false)
		}
	}
	if t.CreateDB != nil {
		d.CreateDB = *t.CreateDB
	}
	if t.CreateRole != nil {
		d.CreateRole = *t.CreateRole
	}
	if t.Replication != nil {
		d.Replication = *t.Replication
	}
	if t.ConnLimit != nil {
		n := *t.ConnLimit
		if n < -1 {
			return newErrf(CodeInvalidParameterValue, "invalid connection limit: %d", n)
		}
		if n == -1 {
			d.ConnLimit = nil
		} else {
			d.ConnLimit = &n
		}
	}
	if t.ValidUntil != nil {
		v, err := parseValidUntil(*t.ValidUntil)
		if err != nil {
			return err
		}
		if d.Name == catalog.RootRole && v != "infinity" {
			// As with NOLOGIN and PASSWORD NULL: the bootstrap
			// credential cannot be made to lapse.
			return newErrf(CodeFeatureNotSupported, "root's password cannot expire (VALID UNTIL 'infinity' only)")
		}
		d.ValidUntil = v
	}
	return nil
}

// parseValidUntil reads a VALID UNTIL operand: a timestamp with time
// zone (UTC when the text has none), infinity or -infinity.
func parseValidUntil(text string) (string, error) {
	switch v := strings.ToLower(strings.TrimSpace(text)); v {
	case "infinity", "-infinity":
		return v, nil
	}
	nanos, err := types.ParseTimestamp(strings.TrimSpace(text))
	if err != nil {
		return "", newErrf("22007", `invalid input syntax for type timestamp with time zone: "%s"`, text)
	}
	return time.Unix(0, nanos).UTC().Format(time.RFC3339Nano), nil
}

// execCreateRoleLegacy is the pre-v11 form: a login user with a
// password, written to the old layout so every node can authenticate it.
func (s *Session) execCreateRoleLegacy(ctx context.Context, txn *kvclient.Txn, t *parser.CreateRole, name, tag string) (*Result, error) {
	if t.Login != nil && !*t.Login || t.Inherit != nil || t.BypassRLS != nil && *t.BypassRLS || len(t.InRoles) > 0 || !t.IsUser && t.Login == nil || t.Password != nil && *t.Password == "" ||
		t.Superuser != nil || t.CreateDB != nil || t.CreateRole != nil || t.Replication != nil || t.ConnLimit != nil || t.ValidUntil != nil {
		return nil, s.requireV11("roles (NOLOGIN, INHERIT, IN ROLE, PASSWORD NULL, role attributes)")
	}
	key := keys.UserKey(name)
	existing, err := txn.Get(ctx, key)
	if err != nil {
		return nil, err
	}
	if t.Alter && existing == nil {
		return nil, newErrf(CodeUndefinedObject, "role %q does not exist", name)
	}
	if !t.Alter && existing != nil {
		if t.IfNotExists {
			return &Result{Tag: tag}, nil
		}
		return nil, newErrf(CodeDuplicateObject, "role %q already exists", name)
	}
	if t.Password == nil {
		if t.Alter {
			return &Result{Tag: tag}, nil
		}
		return nil, newErrf(CodeSyntaxError, "a password is required until cluster version v11 (CREATE USER name PASSWORD '...')")
	}
	raw, err := makeVerifier(*t.Password)
	if err != nil {
		return nil, err
	}
	if err := txn.Put(ctx, key, raw); err != nil {
		return nil, err
	}
	log.Audit("role-ddl", "stmt", tag, "target", name, "login", true, "principal", s.sessionUser, "role", s.user)
	return &Result{Tag: tag}, nil
}

// ownedObjects lists what a role owns: the reasons DROP ROLE refuses.
func (s *Session) ownedObjects(ctx context.Context, txn *kvclient.Txn, name string) ([]string, error) {
	var out []string
	tables, err := s.cat.List(ctx, txn)
	if err != nil {
		return nil, err
	}
	names, err := s.databaseNames(ctx, txn)
	if err != nil {
		return nil, err
	}
	qualify := func(dbID uint64, n string) string {
		if db := names[dbID]; db != "" && db != s.database {
			return db + "." + n
		}
		return n
	}
	for _, d := range tables {
		kind := "table"
		if d.IsView() {
			kind = "view"
		}
		if catalog.OwnerOf(d.Owner) == name {
			out = append(out, "owner of "+kind+" "+qualify(d.DatabaseID, d.Name))
		}
	}
	seqs, err := catalog.ListSequences(ctx, txn, 0)
	if err != nil {
		return nil, err
	}
	for _, d := range seqs {
		if catalog.OwnerOf(d.Owner) == name && d.OwnerTable == 0 {
			out = append(out, "owner of sequence "+qualify(d.DatabaseID, d.Name))
		}
	}
	typs, err := catalog.ListTypes(ctx, txn, 0)
	if err != nil {
		return nil, err
	}
	for _, d := range typs {
		if catalog.OwnerOf(d.Owner) == name {
			out = append(out, "owner of type "+qualify(d.DatabaseID, d.Name))
		}
	}
	schemas, err := catalog.ListAllSchemas(ctx, txn)
	if err != nil {
		return nil, err
	}
	for _, sd := range schemas {
		if catalog.OwnerOf(sd.Owner) == name {
			out = append(out, "owner of schema "+qualify(sd.DatabaseID, sd.Name))
		}
	}
	dbs, err := catalog.ListDatabases(ctx, txn)
	if err != nil {
		return nil, err
	}
	for _, db := range dbs {
		if db.ID == 0 {
			continue
		}
		if catalog.OwnerOf(db.Owner) == name {
			out = append(out, "owner of database "+db.Name)
		}
	}
	keys, err := s.ownedKeys(ctx, txn, map[string]bool{name: true})
	if err != nil {
		return nil, err
	}
	for _, k := range keys {
		out = append(out, "owner of encryption key "+k)
	}
	return out, nil
}

// revokeAllFrom strips every privilege granted to the roles — on tables,
// views, sequences, databases, the schema, and the default-privilege
// rules naming them — and reports how many objects changed.
func (s *Session) revokeAllFrom(ctx context.Context, txn *kvclient.Txn, owned map[string]bool) (int, error) {
	revoked := 0
	schemas, err := catalog.ListAllSchemas(ctx, txn)
	if err != nil {
		return 0, err
	}
	for _, sd := range schemas {
		c := sd.Clone()
		changed := false
		for r := range owned {
			if _, ok := c.Privileges[r]; ok {
				delete(c.Privileges, r)
				changed = true
			}
			delete(c.GrantOptions, r)
		}
		if changed {
			if err := s.cat.UpdateSchema(ctx, txn, c); err != nil {
				return 0, err
			}
			revoked++
		}
	}
	names, err := s.databaseNames(ctx, txn)
	if err != nil {
		return 0, err
	}
	tables, err := s.cat.List(ctx, txn)
	if err != nil {
		return 0, err
	}
	for _, d := range tables {
		changed := false
		c := d.Clone()
		for r := range owned {
			if _, ok := c.Privileges[r]; ok {
				delete(c.Privileges, r)
				changed = true
			}
			delete(c.GrantOptions, r)
		}
		if changed {
			if err := s.cat.Update(ctx, txn, c); err != nil {
				return 0, err
			}
			s.extraDDL = append(s.extraDDL, drainName(names, d))
			revoked++
		}
	}
	seqs, err := catalog.ListSequences(ctx, txn, 0)
	if err != nil {
		return 0, err
	}
	for _, d := range seqs {
		changed := false
		for r := range owned {
			if _, ok := d.Privileges[r]; ok {
				delete(d.Privileges, r)
				changed = true
			}
			delete(d.GrantOptions, r)
		}
		if changed {
			if err := catalog.UpdateSequence(ctx, txn, d); err != nil {
				return 0, err
			}
			revoked++
		}
	}
	dbs, err := catalog.ListDatabases(ctx, txn)
	if err != nil {
		return 0, err
	}
	for _, db := range dbs {
		if db.ID == 0 {
			continue
		}
		c := db.Clone()
		changed := false
		for r := range owned {
			for _, m := range []map[string][]string{c.Privileges, c.GrantOptions, c.SchemaPrivileges, c.SchemaGrantOptions} {
				if _, ok := m[r]; ok {
					delete(m, r)
					changed = true
				}
			}
		}
		var keep []catalog.DefaultPrivilege
		for _, dp := range c.DefaultPrivileges {
			if owned[dp.ForRole] || owned[dp.Grantee] {
				changed = true
				continue
			}
			keep = append(keep, dp)
		}
		c.DefaultPrivileges = keep
		if changed {
			if err := s.cat.UpdateDatabase(ctx, txn, c); err != nil {
				return 0, err
			}
			revoked++
		}
	}
	// And on the key service's keys (issue #310).
	n, err := s.revokeKeysFrom(ctx, txn, owned)
	if err != nil {
		return 0, err
	}
	return revoked + n, nil
}

// execDropRole drops roles (admins only). A role that owns objects is
// refused (2BP01): REASSIGN OWNED / DROP OWNED first. Its grants and
// its memberships go with it.
func (s *Session) execDropRole(ctx context.Context, txn *kvclient.Txn, t *parser.DropRole) (*Result, error) {
	if err := s.checkAdmin(ctx, txn); err != nil {
		return nil, err
	}
	tag := "DROP ROLE"
	for _, raw := range t.Names {
		name := strings.ToLower(raw)
		if name == catalog.RootRole {
			return nil, newErrf(CodeFeatureNotSupported, "cannot drop role %q", name)
		}
		if catalog.IsBuiltinRole(name) || name == catalog.PublicRole {
			return nil, newErrf(CodeFeatureNotSupported, "role %q is built in and cannot be dropped", name)
		}
		if name == s.sessionUser {
			return nil, newErrf(CodeObjectInUse, "current user cannot be dropped")
		}
		existing, err := catalog.LookupRole(ctx, txn, name)
		if err != nil {
			return nil, ToSQLError(err)
		}
		if existing == nil {
			if t.IfExists {
				continue
			}
			return nil, newErrf(CodeUndefinedObject, "role %q does not exist", name)
		}
		deps, err := s.ownedObjects(ctx, txn, name)
		if err != nil {
			return nil, err
		}
		if len(deps) > 0 {
			return nil, newErrf(CodeDependentObjectsExist, "role %q cannot be dropped because some objects depend on it: %s (REASSIGN OWNED BY %s TO ... or DROP OWNED BY %s first)",
				name, strings.Join(deps, "; "), name, name)
		}
		// A policy's TO list names the role: dropping it would leave the
		// policy applying to nobody, or to whoever is next given the name.
		if uses, err := s.rolePolicyUses(ctx, txn, name); err != nil {
			return nil, err
		} else if len(uses) > 0 {
			return nil, newErrf(CodeDependentObjectsExist, "role %q cannot be dropped because some objects depend on it: %s (ALTER POLICY ... TO, or DROP POLICY, first)",
				name, strings.Join(uses, "; "))
		}
		if _, err := s.revokeAllFrom(ctx, txn, map[string]bool{name: true}); err != nil {
			return nil, err
		}
		if s.rolesFinalized() {
			roles, err := catalog.ListRoles(ctx, txn)
			if err != nil {
				return nil, err
			}
			for _, r := range roles {
				if r.Builtin || r.Name == name {
					continue
				}
				if member, _ := r.IsMemberOf(name); member {
					rr := r.Clone()
					rr.RemoveMembership(name, false)
					if err := catalog.PutRole(ctx, txn, rr); err != nil {
						return nil, err
					}
				}
			}
		}
		if err := catalog.DeleteRole(ctx, txn, name); err != nil {
			return nil, err
		}
		log.Audit("role-ddl", "stmt", tag, "target", name, "principal", s.sessionUser, "role", s.user)
	}
	s.invalidateRoles()
	return &Result{Tag: tag}, nil
}

// execGrantRole applies GRANT role TO role / REVOKE role FROM role.
// Admins may grant any role; a member holding the admin option on a role
// may grant that role.
func (s *Session) execGrantRole(ctx context.Context, txn *kvclient.Txn, t *parser.GrantRevoke, tag string) (*Result, error) {
	roles := make([]string, 0, len(t.Roles))
	for _, r := range t.Roles {
		roles = append(roles, strings.ToLower(r))
	}
	grantees := make([]string, 0, len(t.Grantees))
	for _, g := range t.Grantees {
		grantees = append(grantees, strings.ToLower(g))
	}
	if !s.rolesFinalized() {
		if len(roles) != 1 || roles[0] != catalog.AdminRole || len(grantees) != 1 || t.AdminOption {
			return nil, s.requireV11("role memberships other than GRANT ADMIN TO user")
		}
		if err := s.checkAdmin(ctx, txn); err != nil {
			return nil, err
		}
		user := grantees[0]
		if user == catalog.RootRole {
			return nil, newErrf(CodeFeatureNotSupported, "root's admin membership is implicit and cannot be changed")
		}
		key := keys.AdminUserKey(user)
		if t.Revoke {
			if err := txn.Delete(ctx, key); err != nil {
				return nil, err
			}
		} else if err := txn.Put(ctx, key, []byte("1")); err != nil {
			return nil, err
		}
		s.invalidateRoles()
		log.Audit("privilege-ddl", "stmt", tag, "roles", catalog.AdminRole, "grantees", user, "principal", s.sessionUser, "role", s.user)
		return &Result{Tag: tag}, nil
	}

	admin, err := s.isAdmin(ctx, txn)
	if err != nil {
		return nil, err
	}
	var self *catalog.RoleDescriptor
	if !admin {
		if self, err = catalog.LookupRole(ctx, txn, s.actor()); err != nil {
			return nil, ToSQLError(err)
		}
	}
	graph := catalog.LazyRoleGraph(ctx, txn)
	for _, r := range roles {
		if _, err := s.lookupRoleOrErr(ctx, txn, r); err != nil {
			return nil, err
		}
		if r == catalog.PublicRole {
			return nil, newErrf(CodeInvalidGrantOperation, "public is not a role that can be granted")
		}
		if !admin {
			adminOpt := false
			if self != nil {
				_, adminOpt = self.IsMemberOf(r)
			}
			if !adminOpt {
				return nil, newErrf(CodeInsufficientPriv, "permission denied to %s role %q (%q holds no admin option on it)", strings.ToLower(tag), r, s.actor())
			}
		}
		for _, g := range grantees {
			if g == catalog.PublicRole || catalog.IsBuiltinRole(g) {
				return nil, newErrf(CodeInvalidGrantOperation, "role %q cannot be made a member of another role", g)
			}
			if g == catalog.RootRole {
				if r == catalog.AdminRole {
					return nil, newErrf(CodeFeatureNotSupported, "root's admin membership is implicit and cannot be changed")
				}
			}
			if g == r {
				return nil, newErrf(CodeInvalidGrantOperation, "role %q cannot be a member of itself", g)
			}
			gd, err := s.lookupRoleOrErr(ctx, txn, g)
			if err != nil {
				return nil, err
			}
			if !t.Revoke {
				// No cycles: r must not already be a member of g.
				reach, err := graph.Reachable(r)
				if err != nil {
					return nil, ToSQLError(err)
				}
				if reach.Has(g) {
					return nil, newErrf(CodeInvalidGrantOperation, "role %q is a member of role %q", r, g)
				}
			}
			gd = gd.Clone()
			changed := false
			if t.Revoke {
				changed = gd.RemoveMembership(r, t.AdminOption)
			} else {
				changed = gd.AddMembership(r, t.AdminOption)
			}
			if changed || gd.Legacy {
				if err := catalog.PutRole(ctx, txn, gd); err != nil {
					return nil, err
				}
			}
		}
	}
	s.invalidateRoles()
	log.Audit("privilege-ddl", "stmt", tag, "roles", strings.Join(roles, ","), "grantees", strings.Join(grantees, ","),
		"admin_option", t.AdminOption, "principal", s.sessionUser, "role", s.user)
	return &Result{Tag: tag}, nil
}

// checkNewOwner validates the target of an ownership transfer: an
// existing role that is not built in, which the actor holds (admins may
// give an object to anyone).
func (s *Session) checkNewOwner(ctx context.Context, txn *kvclient.Txn, owner string) error {
	if owner == catalog.PublicRole || catalog.IsBuiltinRole(owner) {
		return newErrf(CodeInvalidGrantOperation, "role %q cannot own objects", owner)
	}
	if _, err := s.lookupRoleOrErr(ctx, txn, owner); err != nil {
		return err
	}
	set, err := s.roleSet(ctx, txn)
	if err != nil {
		return err
	}
	if set.IsAdmin() || set.Has(owner) {
		return nil
	}
	return newErrf(CodeInsufficientPriv, "must be a member of role %q to give it objects", owner)
}

// execAlterOwner runs ALTER <kind> name OWNER TO role: the owner (or an
// admin) hands the object to a role it belongs to.
func (s *Session) execAlterOwner(ctx context.Context, txn *kvclient.Txn, t *parser.AlterOwner) (*Result, error) {
	if err := s.requireV11("ownership changes"); err != nil {
		return nil, err
	}
	owner := strings.ToLower(t.Owner)
	tag := "ALTER " + strings.ToUpper(t.Kind)
	switch t.Kind {
	case "table", "view":
		shared, err := s.lookup(ctx, txn, t.Name)
		if err != nil {
			return nil, err
		}
		if shared.Virtual != "" {
			// A view's owner changes too: by ALTER VIEW, or by ALTER
			// TABLE, which PostgreSQL accepts for a view.
			if err := mustBeReal(shared); err != nil {
				return nil, err
			}
		}
		if t.Kind == "view" && !shared.IsView() {
			return nil, newErrf(CodeWrongObjectType, "%q is not a view", t.Name)
		}
		if catalog.IsSystemTable(shared.Name) && !s.system {
			return nil, newErrf(CodeInsufficientPriv, "table %q belongs to the cluster", shared.Name)
		}
		if err := s.checkTableOwner(ctx, txn, shared); err != nil {
			return nil, err
		}
		if err := s.checkNewOwner(ctx, txn, owner); err != nil {
			return nil, err
		}
		desc := shared.Clone()
		desc.Owner = owner
		if err := s.cat.Update(ctx, txn, desc); err != nil {
			return nil, err
		}
	case "sequence":
		d, err := s.lookupSequence(ctx, txn, t.Name)
		if err != nil {
			return nil, err
		}
		if err := s.checkOwner(ctx, txn, "sequence", d.Name, d.Owner); err != nil {
			return nil, err
		}
		if err := s.checkNewOwner(ctx, txn, owner); err != nil {
			return nil, err
		}
		d.Owner = owner
		if err := catalog.UpdateSequence(ctx, txn, d); err != nil {
			return nil, err
		}
	case "type":
		d, err := s.lookupType(ctx, txn, t.Name)
		if err != nil {
			return nil, err
		}
		if err := s.checkOwner(ctx, txn, "type", d.Name, d.Owner); err != nil {
			return nil, err
		}
		if err := s.checkNewOwner(ctx, txn, owner); err != nil {
			return nil, err
		}
		d.Owner = owner
		if err := catalog.UpdateType(ctx, txn, d); err != nil {
			return nil, err
		}
	case "database":
		if reservedDatabase(t.Name) {
			return nil, newErrf(CodeInsufficientPriv, "database %q is reserved", t.Name)
		}
		db, err := catalog.LookupDatabase(ctx, txn, t.Name)
		if err != nil {
			return nil, ToSQLError(err)
		}
		if db.ID == 0 {
			return nil, errBeforeV6()
		}
		if err := s.checkOwner(ctx, txn, "database", db.Name, db.Owner); err != nil {
			return nil, err
		}
		if err := s.checkNewOwner(ctx, txn, owner); err != nil {
			return nil, err
		}
		db = db.Clone()
		db.Owner = owner
		if err := s.cat.UpdateDatabase(ctx, txn, db); err != nil {
			return nil, err
		}
	case "large object":
		loid, perr := strconv.ParseInt(t.Name, 10, 64)
		if perr != nil {
			return nil, newErrf(CodeSyntaxError, "invalid large object id %q", t.Name)
		}
		if err := s.checkNewOwner(ctx, txn, owner); err != nil {
			return nil, err
		}
		if err := s.loSetOwner(ctx, txn, loid, owner); err != nil {
			return nil, err
		}
	case "schema":
		// Another schema changes hands like any object. The public
		// schema is the database's owner's (PostgreSQL 15's
		// pg_database_owner): naming that owner, or pg_database_owner
		// itself, changes nothing — what pg_dump writes for it — and any
		// other role is refused rather than ignored.
		if t.Name != catalog.PublicSchema {
			if err := s.alterSchemaOwner(ctx, txn, t.Name, owner); err != nil {
				return nil, err
			}
			break
		}
		db, err := catalog.LookupDatabase(ctx, txn, s.database)
		if err != nil {
			return nil, ToSQLError(err)
		}
		dbOwner := catalog.OwnerOf(db.Owner)
		if owner != dbOwner && owner != "pg_database_owner" {
			return nil, newErrf(CodeFeatureNotSupported, "the public schema is owned by the database's owner, %q: change that with ALTER DATABASE %s OWNER TO", dbOwner, s.database)
		}
		return &Result{Tag: tag}, nil
	default:
		return nil, newErrf(CodeSyntaxError, "cannot change the owner of a %s", t.Kind)
	}
	log.Audit("ownership-ddl", "stmt", tag+" OWNER TO", "kind", t.Kind, "target", t.Name, "owner", owner, "principal", s.sessionUser, "role", s.user)
	return &Result{Tag: tag}, nil
}

// checkOwnedRoles validates the roles REASSIGN OWNED / DROP OWNED name:
// each exists, none is root, and the actor holds every one (admins
// hold all).
func (s *Session) checkOwnedRoles(ctx context.Context, txn *kvclient.Txn, names []string) ([]string, error) {
	set, err := s.roleSet(ctx, txn)
	if err != nil {
		return nil, err
	}
	out := make([]string, 0, len(names))
	for _, raw := range names {
		name := strings.ToLower(raw)
		if name == catalog.RootRole {
			return nil, newErrf(CodeFeatureNotSupported, "cannot reassign or drop objects owned by root")
		}
		if _, err := s.lookupRoleOrErr(ctx, txn, name); err != nil {
			return nil, err
		}
		if !set.IsAdmin() && !set.Has(name) {
			return nil, newErrf(CodeInsufficientPriv, "permission denied to reassign objects owned by role %q (%q is not a member)", name, s.actor())
		}
		out = append(out, name)
	}
	return out, nil
}

// execReassignOwned hands every object the roles own to another role.
func (s *Session) execReassignOwned(ctx context.Context, txn *kvclient.Txn, t *parser.ReassignOwned) (*Result, error) {
	if err := s.requireV11("ownership changes"); err != nil {
		return nil, err
	}
	from, err := s.checkOwnedRoles(ctx, txn, t.From)
	if err != nil {
		return nil, err
	}
	to := strings.ToLower(t.To)
	if err := s.checkNewOwner(ctx, txn, to); err != nil {
		return nil, err
	}
	owned := map[string]bool{}
	for _, f := range from {
		owned[f] = true
	}
	moved := 0
	tables, err := s.cat.List(ctx, txn)
	if err != nil {
		return nil, err
	}
	names, err := s.databaseNames(ctx, txn)
	if err != nil {
		return nil, err
	}
	for _, d := range tables {
		if owned[catalog.OwnerOf(d.Owner)] {
			c := d.Clone()
			c.Owner = to
			if err := s.cat.Update(ctx, txn, c); err != nil {
				return nil, err
			}
			s.extraDDL = append(s.extraDDL, drainName(names, d))
			moved++
		}
	}
	seqs, err := catalog.ListSequences(ctx, txn, 0)
	if err != nil {
		return nil, err
	}
	for _, d := range seqs {
		if owned[catalog.OwnerOf(d.Owner)] {
			d.Owner = to
			if err := catalog.UpdateSequence(ctx, txn, d); err != nil {
				return nil, err
			}
			moved++
		}
	}
	typs, err := catalog.ListTypes(ctx, txn, 0)
	if err != nil {
		return nil, err
	}
	for _, d := range typs {
		if owned[catalog.OwnerOf(d.Owner)] {
			d.Owner = to
			if err := catalog.UpdateType(ctx, txn, d); err != nil {
				return nil, err
			}
			moved++
		}
	}
	schemas, err := catalog.ListAllSchemas(ctx, txn)
	if err != nil {
		return nil, err
	}
	for _, sd := range schemas {
		if owned[catalog.OwnerOf(sd.Owner)] {
			c := sd.Clone()
			c.Owner = to
			if err := s.cat.UpdateSchema(ctx, txn, c); err != nil {
				return nil, err
			}
			moved++
		}
	}
	dbs, err := catalog.ListDatabases(ctx, txn)
	if err != nil {
		return nil, err
	}
	for _, db := range dbs {
		if db.ID != 0 && owned[catalog.OwnerOf(db.Owner)] {
			c := db.Clone()
			c.Owner = to
			if err := s.cat.UpdateDatabase(ctx, txn, c); err != nil {
				return nil, err
			}
			moved++
		}
	}
	n, err := s.reassignKeys(ctx, txn, owned, to)
	if err != nil {
		return nil, err
	}
	moved += n
	log.Audit("ownership-ddl", "stmt", "REASSIGN OWNED", "from", strings.Join(from, ","), "to", to, "objects", moved, "principal", s.sessionUser, "role", s.user)
	return &Result{Tag: "REASSIGN OWNED"}, nil
}

// execDropOwned drops every table, view, sequence and type the roles own
// (views first, then tables — CASCADE takes dependent views of other
// owners too) and revokes every privilege granted to them. A database
// or an encryption key the role owns is refused: ALTER DATABASE ...
// OWNER TO, or DROP ENCRYPTION KEY, first.
func (s *Session) execDropOwned(ctx context.Context, txn *kvclient.Txn, t *parser.DropOwned) (*Result, error) {
	if err := s.requireV11("ownership changes"); err != nil {
		return nil, err
	}
	roles, err := s.checkOwnedRoles(ctx, txn, t.Roles)
	if err != nil {
		return nil, err
	}
	owned := map[string]bool{}
	for _, r := range roles {
		owned[r] = true
	}
	names, err := s.databaseNames(ctx, txn)
	if err != nil {
		return nil, err
	}
	dbs, err := catalog.ListDatabases(ctx, txn)
	if err != nil {
		return nil, err
	}
	for _, db := range dbs {
		if db.ID != 0 && owned[catalog.OwnerOf(db.Owner)] {
			return nil, newErrf(CodeDependentObjectsExist, "cannot drop objects owned by role %q because database %q is owned by it (ALTER DATABASE %s OWNER TO ... first)", catalog.OwnerOf(db.Owner), db.Name, db.Name)
		}
	}
	// An encryption key is not dropped with its owner's other objects:
	// dropping one destroys it, and everything wrapped under it with it.
	// That is a DROP ENCRYPTION KEY of its own, or a REASSIGN OWNED.
	if keys, err := s.ownedKeys(ctx, txn, owned); err != nil {
		return nil, err
	} else if len(keys) > 0 {
		return nil, newErrf(CodeDependentObjectsExist, "cannot drop objects owned by %s because it owns encryption keys %s: dropping a key destroys everything wrapped under it, so it is never done by DROP OWNED (DROP ENCRYPTION KEY each one, or REASSIGN OWNED BY %s TO ..., first)",
			strings.Join(roles, ", "), strings.Join(keys, ", "), strings.Join(roles, ", "))
	}
	dropped := 0
	tables, err := s.cat.List(ctx, txn)
	if err != nil {
		return nil, err
	}
	sort.Slice(tables, func(i, j int) bool { return tables[i].IsView() && !tables[j].IsView() })
	for _, d := range tables {
		if !owned[catalog.OwnerOf(d.Owner)] || catalog.IsSystemTable(d.Name) {
			continue
		}
		rel := catalog.RelationName(d)
		if cur, err := s.cat.LookupIn(ctx, txn, names[d.DatabaseID], rel); err != nil || cur == nil || cur.ID != d.ID {
			continue // already gone with an earlier cascade
		}
		if d.IsView() {
			if _, err := s.execDropView(ctx, txn, &parser.DropView{Names: []string{rel}, Cascade: t.Cascade, IfExists: true}); err != nil {
				return nil, err
			}
		} else if _, err := s.execDropTable(ctx, txn, &parser.DropTable{Name: rel, Cascade: t.Cascade, IfExists: true}); err != nil {
			return nil, err
		}
		dropped++
	}
	seqs, err := catalog.ListSequences(ctx, txn, 0)
	if err != nil {
		return nil, err
	}
	for _, d := range seqs {
		if !owned[catalog.OwnerOf(d.Owner)] {
			continue
		}
		if cur, err := catalog.ReadSequence(ctx, txn, d.ID); err != nil || cur == nil {
			continue // dropped with its table
		}
		if _, err := s.execDropSequence(ctx, txn, &parser.DropSequence{Name: catalog.SequenceRelationName(d), IfExists: true, Cascade: t.Cascade}); err != nil {
			return nil, err
		}
		dropped++
	}
	typs, err := catalog.ListTypes(ctx, txn, 0)
	if err != nil {
		return nil, err
	}
	for _, d := range typs {
		if !owned[catalog.OwnerOf(d.Owner)] {
			continue
		}
		if _, err := s.execDropType(ctx, txn, &parser.DropType{Name: catalog.TypeRelationName(d), IfExists: true}); err != nil {
			return nil, err
		}
		dropped++
	}
	// The schemas the roles own, once their own objects are gone: one
	// still holding another role's objects goes only under CASCADE.
	schemas, err := catalog.ListAllSchemas(ctx, txn)
	if err != nil {
		return nil, err
	}
	for _, sd := range schemas {
		if !owned[catalog.OwnerOf(sd.Owner)] {
			continue
		}
		db, err := catalog.ReadDatabase(ctx, txn, sd.DatabaseID)
		if err != nil {
			return nil, err
		}
		if db == nil {
			continue
		}
		left, err := s.cat.TablesInSchema(ctx, txn, db, sd.ID)
		if err != nil {
			return nil, err
		}
		if len(left) > 0 {
			if !t.Cascade || db.Name != s.database {
				return nil, newErrf(CodeDependentObjectsExist, "cannot drop schema %s because other objects depend on it (use DROP OWNED ... CASCADE in database %s)", sd.Name, db.Name)
			}
			if _, err := s.execDropSchema(ctx, txn, &parser.DropSchema{Names: []string{sd.Name}, Cascade: true, IfExists: true}); err != nil {
				return nil, err
			}
		} else if err := s.cat.DropSchema(ctx, txn, sd); err != nil {
			return nil, err
		}
		dropped++
	}
	// Take the roles out of the policies that name them, as
	// PostgreSQL's DROP OWNED does: a policy naming only them goes.
	if err := s.stripPolicyRoles(ctx, txn, owned); err != nil {
		return nil, err
	}
	// Revoke what the roles were granted.
	revoked, err := s.revokeAllFrom(ctx, txn, owned)
	if err != nil {
		return nil, err
	}
	log.Audit("ownership-ddl", "stmt", "DROP OWNED", "roles", strings.Join(roles, ","), "dropped", dropped, "revoked", revoked, "principal", s.sessionUser, "role", s.user)
	return &Result{Tag: "DROP OWNED"}, nil
}

// stripPolicyRoles removes roles from every row-security policy's TO
// list (issue #312). A policy whose list would be left empty is
// dropped rather than left to apply to PUBLIC, which is what an empty
// list means; PostgreSQL's DROP OWNED does the same.
func (s *Session) stripPolicyRoles(ctx context.Context, txn *kvclient.Txn, roles map[string]bool) error {
	names, err := s.databaseNames(ctx, txn)
	if err != nil {
		return err
	}
	tables, err := s.cat.List(ctx, txn)
	if err != nil {
		return err
	}
	for _, d := range tables {
		changed := false
		var kept []catalog.Policy
		for _, p := range d.Policies {
			var rs []string
			for _, r := range p.Roles {
				if roles[r] {
					changed = true
					continue
				}
				rs = append(rs, r)
			}
			if len(p.Roles) > 0 && len(rs) == 0 {
				log.Audit("policy-ddl", "stmt", "DROP POLICY", "target", p.Name+" ON "+d.Name, "reason", "DROP OWNED", "principal", s.sessionUser, "role", s.user)
				continue
			}
			p.Roles = rs
			kept = append(kept, p)
		}
		if !changed {
			continue
		}
		c := d.Clone()
		c.Policies = kept
		if err := s.cat.Update(ctx, txn, c); err != nil {
			return err
		}
		s.extraDDL = append(s.extraDDL, drainName(names, d))
	}
	return nil
}

// drainName names a table for the post-commit lease drain: db.name, or
// the bare name for a table that predates databases.
func drainName(_ map[uint64]string, d *catalog.TableDescriptor) string {
	return catalog.RelationName(d)
}

// checkSetRole validates SET ROLE name for the session user: the role
// exists and the session user is a member of it (transitively,
// regardless of INHERIT), or an admin.
func (s *Session) checkSetRole(ctx context.Context, name string) *Error {
	if !s.rolesFinalized() {
		return ToSQLError(s.requireV11("SET ROLE"))
	}
	if name == catalog.PublicRole {
		return newErrf(CodeInvalidParameterValue, "role %q does not exist", name)
	}
	var serr *Error
	err := s.db.RunTxn(ctx, "set-role", func(ctx context.Context, txn *kvclient.Txn) error {
		serr = nil
		d, err := catalog.LookupRole(ctx, txn, name)
		if err != nil {
			return err
		}
		if d == nil {
			serr = newErrf(CodeInvalidParameterValue, "role %q does not exist", name)
			return nil
		}
		if s.sessionUser == catalog.RootRole || name == s.sessionUser {
			return nil
		}
		graph := catalog.LazyRoleGraph(ctx, txn)
		reach, err := graph.Reachable(s.sessionUser)
		if err != nil {
			return err
		}
		if reach.Has(name) {
			return nil
		}
		eff, err := graph.Effective(s.sessionUser)
		if err != nil {
			return err
		}
		if eff.IsAdmin() {
			return nil
		}
		serr = newErrf(CodeInsufficientPriv, "permission denied to set role %q (%q is not a member)", name, s.sessionUser)
		return nil
	})
	if err != nil {
		return ToSQLError(err)
	}
	return serr
}

// applyRole derives the current role from the session user and the
// role variable.
func (s *Session) applyRole() {
	s.user = s.sessionUser
	if s.vars.role != "" {
		s.user = s.vars.role
	}
	s.invalidateRoles()
}

var _ = fmt.Sprintf
