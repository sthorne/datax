package sql

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/kvclient"
	"github.com/sthorne/datax/pkg/security"
	"github.com/sthorne/datax/pkg/sql/catalog"
	"github.com/sthorne/datax/pkg/sql/parser"
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
	selfPassword := t.Alter && name == s.user && t.Login == nil && t.Inherit == nil && len(t.InRoles) == 0 && t.Password != nil && *t.Password != ""
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
	log.Audit("role-ddl", "stmt", tag, "target", name, "login", d.Login, "principal", s.sessionUser, "role", s.user)
	return &Result{Tag: tag}, nil
}

// execCreateRoleLegacy is the pre-v11 form: a login user with a
// password, written to the old layout so every node can authenticate it.
func (s *Session) execCreateRoleLegacy(ctx context.Context, txn *kvclient.Txn, t *parser.CreateRole, name, tag string) (*Result, error) {
	if t.Login != nil && !*t.Login || t.Inherit != nil || len(t.InRoles) > 0 || !t.IsUser && t.Login == nil || t.Password != nil && *t.Password == "" {
		return nil, s.requireV11("roles (NOLOGIN, INHERIT, IN ROLE, PASSWORD NULL)")
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
	return out, nil
}

// revokeAllFrom strips every privilege granted to the roles — on tables,
// views, sequences, databases, the schema, and the default-privilege
// rules naming them — and reports how many objects changed.
func (s *Session) revokeAllFrom(ctx context.Context, txn *kvclient.Txn, owned map[string]bool) (int, error) {
	revoked := 0
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
	return revoked, nil
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
		if err := mustBeReal(shared); err != nil {
			return nil, err
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
	log.Audit("ownership-ddl", "stmt", "REASSIGN OWNED", "from", strings.Join(from, ","), "to", to, "objects", moved, "principal", s.sessionUser, "role", s.user)
	return &Result{Tag: "REASSIGN OWNED"}, nil
}

// execDropOwned drops every table, view, sequence and type the roles own
// (views first, then tables — CASCADE takes dependent views of other
// owners too) and revokes every privilege granted to them. A database
// the role owns is refused: ALTER DATABASE ... OWNER TO first.
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
	qualify := func(dbID uint64, n string) string {
		if db := names[dbID]; db != "" {
			return db + "." + n
		}
		return n
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
		if cur, err := s.cat.LookupIn(ctx, txn, names[d.DatabaseID], d.Name); err != nil || cur == nil || cur.ID != d.ID {
			continue // already gone with an earlier cascade
		}
		if d.IsView() {
			if _, err := s.execDropView(ctx, txn, &parser.DropView{Names: []string{qualify(d.DatabaseID, d.Name)}, Cascade: t.Cascade, IfExists: true}); err != nil {
				return nil, err
			}
		} else if _, err := s.execDropTable(ctx, txn, &parser.DropTable{Name: qualify(d.DatabaseID, d.Name), Cascade: t.Cascade, IfExists: true}); err != nil {
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
		if _, err := s.execDropSequence(ctx, txn, &parser.DropSequence{Name: qualify(d.DatabaseID, d.Name), IfExists: true}); err != nil {
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
		if _, err := s.execDropType(ctx, txn, &parser.DropType{Name: qualify(d.DatabaseID, d.Name), IfExists: true}); err != nil {
			return nil, err
		}
		dropped++
	}
	// Revoke what the roles were granted.
	revoked, err := s.revokeAllFrom(ctx, txn, owned)
	if err != nil {
		return nil, err
	}
	log.Audit("ownership-ddl", "stmt", "DROP OWNED", "roles", strings.Join(roles, ","), "dropped", dropped, "revoked", revoked, "principal", s.sessionUser, "role", s.user)
	return &Result{Tag: "DROP OWNED"}, nil
}

// drainName names a table for the post-commit lease drain: db.name, or
// the bare name for a table that predates databases.
func drainName(names map[uint64]string, d *catalog.TableDescriptor) string {
	if db := names[d.DatabaseID]; db != "" {
		return db + "." + d.Name
	}
	return d.Name
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
