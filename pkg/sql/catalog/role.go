package catalog

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"

	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/kvclient"
	"github.com/sthorne/datax/pkg/kvpb"
	"github.com/sthorne/datax/pkg/util/encoding"
)

// Roles (issue #98, cluster version v11). A role is the unit of
// authentication and authorization: a login role is a user, a role
// without LOGIN is a group, and a role may be a member of other roles
// (GRANT role TO role). Privileges flow along membership: a role holds
// what it was granted directly plus, unless it is NOINHERIT, what the
// roles it belongs to hold. Objects record their owner, who holds every
// privilege on them.
//
// Role descriptors live at /system/roles/<name>. They supersede the
// pre-v11 layout — a SCRAM verifier at /system/users/<name> and an admin
// marker at /system/admins/<name> — which LookupRole and ListRoles keep
// reading so a cluster serves both layouts until the v11 migration
// (MigrateRoles) rewrites the old entries at finalize.

// The built-in roles. They have no descriptor: they always exist,
// cannot log in, be altered or dropped, and carry their meaning in the
// authorization code.
const (
	// AdminRole: every privilege on everything, user and role management,
	// the admin HTTP and RPC surfaces. root is an implicit member.
	AdminRole = "admin"
	// ReadAllRole: SELECT on every table, view and sequence
	// (PostgreSQL's pg_read_all_data).
	ReadAllRole = "read_all"
	// WriteAllRole: INSERT, UPDATE, DELETE and TRUNCATE on every table
	// (pg_write_all_data).
	WriteAllRole = "write_all"
	// MetricsRole: the HTTP /metrics endpoint, and nothing else — a
	// Prometheus scrape account needs no table grants and cannot read data.
	MetricsRole = "metrics"
	// PublicRole is the pseudo-role every role belongs to: a grantee, never
	// a member or an owner.
	PublicRole = "public"
	// RootRole is the bootstrap superuser: an implicit, irrevocable member
	// of admin that cannot be dropped.
	RootRole = "root"
)

// BuiltinRoles lists the built-in roles in display order.
var BuiltinRoles = []string{AdminRole, ReadAllRole, WriteAllRole, MetricsRole}

// IsBuiltinRole reports whether name is one of the built-in roles.
func IsBuiltinRole(name string) bool {
	for _, r := range BuiltinRoles {
		if r == name {
			return true
		}
	}
	return false
}

// RoleMembership is one edge of the membership graph: the role this
// role is a member of, and whether it may in turn grant that membership
// (WITH ADMIN OPTION).
type RoleMembership struct {
	Role  string `json:"role"`
	Admin bool   `json:"admin,omitempty"`
}

// RoleDescriptor is a role's persisted record.
type RoleDescriptor struct {
	Name string `json:"name"`
	// Login: the role may open a session (CREATE USER's default).
	Login bool `json:"login,omitempty"`
	// NoInherit: the role does not automatically hold the privileges of
	// the roles it belongs to (SET ROLE still reaches them).
	NoInherit bool `json:"noinherit,omitempty"`
	// Verifier is the SCRAM verifier (security.MarshalVerifier's JSON;
	// never a plaintext password); empty for a role without a password.
	Verifier json.RawMessage `json:"verifier,omitempty"`
	// MemberOf lists the roles this role is a direct member of.
	MemberOf []RoleMembership `json:"member_of,omitempty"`
	// Builtin marks a synthesized descriptor for a built-in role; never
	// persisted.
	Builtin bool `json:"-"`
	// Legacy marks a descriptor read from the pre-v11 layout; never
	// persisted.
	Legacy bool `json:"-"`
}

// Clone returns a deep copy.
func (r *RoleDescriptor) Clone() *RoleDescriptor {
	out := *r
	out.Verifier = append(json.RawMessage(nil), r.Verifier...)
	out.MemberOf = append([]RoleMembership(nil), r.MemberOf...)
	return &out
}

// IsMemberOf reports whether the role is a direct member of role, and
// whether with the admin option.
func (r *RoleDescriptor) IsMemberOf(role string) (member, admin bool) {
	for _, m := range r.MemberOf {
		if m.Role == role {
			return true, m.Admin
		}
	}
	return false, false
}

// AddMembership adds (or upgrades) a direct membership; reports whether
// anything changed.
func (r *RoleDescriptor) AddMembership(role string, admin bool) bool {
	for i, m := range r.MemberOf {
		if m.Role == role {
			if admin && !m.Admin {
				r.MemberOf[i].Admin = true
				return true
			}
			return false
		}
	}
	r.MemberOf = append(r.MemberOf, RoleMembership{Role: role, Admin: admin})
	sort.Slice(r.MemberOf, func(i, j int) bool { return r.MemberOf[i].Role < r.MemberOf[j].Role })
	return true
}

// RemoveMembership drops a direct membership (adminOnly: only the admin
// option); reports whether anything changed.
func (r *RoleDescriptor) RemoveMembership(role string, adminOnly bool) bool {
	for i, m := range r.MemberOf {
		if m.Role != role {
			continue
		}
		if adminOnly {
			if !m.Admin {
				return false
			}
			r.MemberOf[i].Admin = false
			return true
		}
		r.MemberOf = append(r.MemberOf[:i], r.MemberOf[i+1:]...)
		return true
	}
	return false
}

// ErrRoleNotFound is returned for an unknown role (SQLSTATE 42704).
type ErrRoleNotFound struct{ Name string }

func (e *ErrRoleNotFound) Error() string { return fmt.Sprintf("role %q does not exist", e.Name) }

// ErrRoleExists is returned by CreateRole for a taken name (42710).
type ErrRoleExists struct{ Name string }

func (e *ErrRoleExists) Error() string { return fmt.Sprintf("role %q already exists", e.Name) }

// RoleReader is what reading roles needs: a transaction or the DB.
type RoleReader interface {
	Get(ctx context.Context, key keys.Key) ([]byte, error)
	Scan(ctx context.Context, start, end keys.Key, max int64) ([]kvpb.KeyValue, error)
}

func builtinRole(name string) *RoleDescriptor {
	return &RoleDescriptor{Name: name, Builtin: true}
}

// LookupRole reads a role: a built-in one, its v11 descriptor, or the
// pre-v11 layout's records. nil, nil when no such role exists.
func LookupRole(ctx context.Context, r RoleReader, name string) (*RoleDescriptor, error) {
	if IsBuiltinRole(name) {
		return builtinRole(name), nil
	}
	raw, err := r.Get(ctx, keys.RoleKey(name))
	if err != nil {
		return nil, err
	}
	if raw != nil {
		var d RoleDescriptor
		if err := json.Unmarshal(raw, &d); err != nil {
			return nil, fmt.Errorf("corrupt role descriptor for %q: %v", name, err)
		}
		return &d, nil
	}
	d, err := lookupLegacyRole(ctx, r, name)
	if err == nil && d == nil && name == RootRole {
		// root exists on every cluster; an insecure one never stores a
		// credential for it.
		d = &RoleDescriptor{Name: RootRole, Login: true}
	}
	return d, err
}

// lookupLegacyRole assembles a role from the pre-v11 records.
func lookupLegacyRole(ctx context.Context, r RoleReader, name string) (*RoleDescriptor, error) {
	verifier, err := r.Get(ctx, keys.UserKey(name))
	if err != nil {
		return nil, err
	}
	if verifier == nil {
		return nil, nil
	}
	d := &RoleDescriptor{Name: name, Login: true, Verifier: append(json.RawMessage(nil), verifier...), Legacy: true}
	admin, err := r.Get(ctx, keys.AdminUserKey(name))
	if err != nil {
		return nil, err
	}
	if admin != nil {
		d.MemberOf = []RoleMembership{{Role: AdminRole}}
	}
	return d, nil
}

// ListRoles returns every role — the built-in ones, every descriptor and
// every pre-v11 record — root first, then by name.
func ListRoles(ctx context.Context, r RoleReader) ([]*RoleDescriptor, error) {
	byName := map[string]*RoleDescriptor{}
	lo, hi := keys.RoleSpan()
	rows, err := r.Scan(ctx, lo, hi, 0)
	if err != nil {
		return nil, err
	}
	for _, kv := range rows {
		var d RoleDescriptor
		if json.Unmarshal(kv.Value, &d) == nil && d.Name != "" {
			dd := d
			byName[d.Name] = &dd
		}
	}
	ulo, uhi := keys.UserSpan()
	users, err := r.Scan(ctx, ulo, uhi, 0)
	if err != nil {
		return nil, err
	}
	for _, kv := range users {
		_, name, derr := encoding.DecodeString(kv.Key[len(ulo):])
		if derr != nil || byName[name] != nil {
			continue
		}
		byName[name] = &RoleDescriptor{Name: name, Login: true, Verifier: append(json.RawMessage(nil), kv.Value...), Legacy: true}
	}
	alo, ahi := keys.AdminUserSpan()
	admins, err := r.Scan(ctx, alo, ahi, 0)
	if err != nil {
		return nil, err
	}
	for _, kv := range admins {
		_, name, derr := encoding.DecodeString(kv.Key[len(alo):])
		if derr != nil {
			continue
		}
		d := byName[name]
		if d == nil {
			continue // a marker without a credential: nothing can log in as it
		}
		if d.Legacy {
			d.AddMembership(AdminRole, false)
		}
	}
	for _, b := range BuiltinRoles {
		byName[b] = builtinRole(b)
	}
	if byName[RootRole] == nil {
		byName[RootRole] = &RoleDescriptor{Name: RootRole, Login: true}
	}
	out := make([]*RoleDescriptor, 0, len(byName))
	for _, d := range byName {
		out = append(out, d)
	}
	sort.Slice(out, func(i, j int) bool {
		if (out[i].Name == RootRole) != (out[j].Name == RootRole) {
			return out[i].Name == RootRole
		}
		return out[i].Name < out[j].Name
	})
	return out, nil
}

// PutRole writes a role descriptor (v11 layout) and removes the pre-v11
// records of the same name, so a later read finds one source of truth.
func PutRole(ctx context.Context, txn *kvclient.Txn, d *RoleDescriptor) error {
	if d.Builtin {
		return fmt.Errorf("role %q is built in", d.Name)
	}
	d.Legacy = false
	raw, err := json.Marshal(d)
	if err != nil {
		return err
	}
	if err := txn.Put(ctx, keys.RoleKey(d.Name), raw); err != nil {
		return err
	}
	if err := txn.Delete(ctx, keys.UserKey(d.Name)); err != nil {
		return err
	}
	return txn.Delete(ctx, keys.AdminUserKey(d.Name))
}

// DeleteRole removes a role's descriptor and any pre-v11 records.
func DeleteRole(ctx context.Context, txn *kvclient.Txn, name string) error {
	if err := txn.Delete(ctx, keys.RoleKey(name)); err != nil {
		return err
	}
	if err := txn.Delete(ctx, keys.UserKey(name)); err != nil {
		return err
	}
	return txn.Delete(ctx, keys.AdminUserKey(name))
}

// MigrateRoles is the v11 catalog migration, idempotent and run in one
// transaction: every pre-v11 credential record becomes a role
// descriptor (LOGIN, the verifier unchanged) and every admin marker a
// membership in the admin role; the old records are deleted. A second
// run finds nothing to move.
func MigrateRoles(ctx context.Context, db *kvclient.DB) (moved int, err error) {
	err = db.RunTxn(ctx, "catalog-migrate-v11", func(ctx context.Context, txn *kvclient.Txn) error {
		moved = 0
		ulo, uhi := keys.UserSpan()
		users, err := txn.Scan(ctx, ulo, uhi, 0)
		if err != nil {
			return err
		}
		alo, ahi := keys.AdminUserSpan()
		admins, err := txn.Scan(ctx, alo, ahi, 0)
		if err != nil {
			return err
		}
		adminSet := map[string]bool{}
		for _, kv := range admins {
			if _, name, derr := encoding.DecodeString(kv.Key[len(alo):]); derr == nil {
				adminSet[name] = true
			}
		}
		for _, kv := range users {
			_, name, derr := encoding.DecodeString(kv.Key[len(ulo):])
			if derr != nil {
				continue
			}
			d := &RoleDescriptor{Name: name, Login: true, Verifier: append(json.RawMessage(nil), kv.Value...)}
			if existing, err := txn.Get(ctx, keys.RoleKey(name)); err != nil {
				return err
			} else if existing != nil {
				// A descriptor written after finalize wins; the stale
				// record just goes.
				if json.Unmarshal(existing, d) != nil {
					return fmt.Errorf("corrupt role descriptor for %q", name)
				}
			}
			if adminSet[name] && name != RootRole {
				d.AddMembership(AdminRole, false)
			}
			if err := PutRole(ctx, txn, d); err != nil {
				return err
			}
			delete(adminSet, name)
			moved++
		}
		for name := range adminSet {
			// A marker without a credential (or root's, which is
			// implicit): drop it; nothing could log in as it.
			if err := txn.Delete(ctx, keys.AdminUserKey(name)); err != nil {
				return err
			}
			moved++
		}
		return nil
	})
	return moved, err
}

// RoleSet is a set of role names: the roles whose privileges an
// identity holds.
type RoleSet map[string]bool

// Has reports membership in the set.
func (s RoleSet) Has(name string) bool { return s[name] }

// IsAdmin reports whether the set carries admin authority.
func (s RoleSet) IsAdmin() bool { return s[RootRole] || s[AdminRole] }

// Names lists the set sorted.
func (s RoleSet) Names() []string {
	out := make([]string, 0, len(s))
	for n := range s {
		out = append(out, n)
	}
	sort.Strings(out)
	return out
}

// RoleGraph resolves membership in memory, from a snapshot of every
// role (ListRoles) or on demand from a reader (Lookup).
type RoleGraph struct {
	roles  map[string]*RoleDescriptor
	reader RoleReader
	ctx    context.Context
}

// NewRoleGraph builds a graph over a snapshot.
func NewRoleGraph(roles []*RoleDescriptor) *RoleGraph {
	g := &RoleGraph{roles: map[string]*RoleDescriptor{}}
	for _, r := range roles {
		g.roles[r.Name] = r
	}
	return g
}

// LazyRoleGraph builds a graph that reads roles from r as it needs them
// (caching each).
func LazyRoleGraph(ctx context.Context, r RoleReader) *RoleGraph {
	return &RoleGraph{roles: map[string]*RoleDescriptor{}, reader: r, ctx: ctx}
}

// Role returns a role's descriptor (nil when unknown).
func (g *RoleGraph) Role(name string) (*RoleDescriptor, error) {
	if d, ok := g.roles[name]; ok {
		return d, nil
	}
	if g.reader == nil {
		return nil, nil
	}
	d, err := LookupRole(g.ctx, g.reader, name)
	if err != nil {
		return nil, err
	}
	g.roles[name] = d
	return d, nil
}

// Effective returns the roles whose privileges name holds: itself,
// public, root's implicit admin membership, and — following INHERIT —
// every role reachable through membership. An unknown role holds only
// itself and public.
func (g *RoleGraph) Effective(name string) (RoleSet, error) {
	set := RoleSet{name: true, PublicRole: true}
	if name == RootRole {
		set[AdminRole] = true
	}
	var walk func(n string) error
	walk = func(n string) error {
		d, err := g.Role(n)
		if err != nil {
			return err
		}
		if d == nil || d.NoInherit {
			return nil
		}
		for _, m := range d.MemberOf {
			if set[m.Role] {
				continue
			}
			set[m.Role] = true
			if err := walk(m.Role); err != nil {
				return err
			}
		}
		return nil
	}
	if err := walk(name); err != nil {
		return nil, err
	}
	return set, nil
}

// Reachable returns every role name may SET ROLE to: the roles it is a
// member of transitively, regardless of INHERIT (plus itself).
func (g *RoleGraph) Reachable(name string) (RoleSet, error) {
	set := RoleSet{name: true}
	var walk func(n string) error
	walk = func(n string) error {
		d, err := g.Role(n)
		if err != nil {
			return err
		}
		if d == nil {
			return nil
		}
		for _, m := range d.MemberOf {
			if set[m.Role] {
				continue
			}
			set[m.Role] = true
			if err := walk(m.Role); err != nil {
				return err
			}
		}
		return nil
	}
	if err := walk(name); err != nil {
		return nil, err
	}
	return set, nil
}

// IsMember reports whether name holds role's privileges (transitively,
// honoring INHERIT); root holds admin's.
func IsMember(ctx context.Context, r RoleReader, name, role string) (bool, error) {
	if name == role {
		return true, nil
	}
	set, err := LazyRoleGraph(ctx, r).Effective(name)
	if err != nil {
		return false, err
	}
	return set[role], nil
}

// OwnerOf returns an object's owner, root for objects that predate
// ownership (an empty owner).
func OwnerOf(owner string) string {
	if owner == "" {
		return RootRole
	}
	return owner
}

// Privilege sets, as descriptors store them: grantee → sorted privilege
// names. Grants with the grant option live in a parallel map.

// AddPrivileges records privs for grantee in m (creating it), keeping
// each grantee's list in order's sequence; reports whether m changed.
func AddPrivileges(m map[string][]string, grantee string, privs []string, order []string) (map[string][]string, bool) {
	if m == nil {
		m = map[string][]string{}
	}
	cur := map[string]bool{}
	for _, p := range m[grantee] {
		cur[p] = true
	}
	changed := false
	for _, p := range privs {
		if !cur[p] {
			cur[p] = true
			changed = true
		}
	}
	if changed {
		m[grantee] = orderedPrivs(cur, order)
	}
	return m, changed
}

// RemovePrivileges drops privs for grantee from m; a grantee left with
// nothing disappears. Reports whether m changed.
func RemovePrivileges(m map[string][]string, grantee string, privs []string, order []string) bool {
	if m == nil {
		return false
	}
	cur := map[string]bool{}
	for _, p := range m[grantee] {
		cur[p] = true
	}
	changed := false
	for _, p := range privs {
		if cur[p] {
			delete(cur, p)
			changed = true
		}
	}
	if !changed {
		return false
	}
	if len(cur) == 0 {
		delete(m, grantee)
	} else {
		m[grantee] = orderedPrivs(cur, order)
	}
	return true
}

func orderedPrivs(set map[string]bool, order []string) []string {
	var out []string
	for _, p := range order {
		if set[p] {
			out = append(out, p)
		}
	}
	for p := range set {
		found := false
		for _, o := range order {
			if o == p {
				found = true
				break
			}
		}
		if !found {
			out = append(out, p)
		}
	}
	return out
}

// ClonePrivileges deep-copies a privilege map (nil stays nil).
func ClonePrivileges(m map[string][]string) map[string][]string {
	if m == nil {
		return nil
	}
	out := make(map[string][]string, len(m))
	for u, ps := range m {
		out[u] = append([]string(nil), ps...)
	}
	return out
}

// HasPrivilege reports whether any role of set holds priv in m.
func HasPrivilege(m map[string][]string, set RoleSet, priv string) bool {
	for grantee, ps := range m {
		if !set[grantee] {
			continue
		}
		for _, p := range ps {
			if p == priv {
				return true
			}
		}
	}
	return false
}
