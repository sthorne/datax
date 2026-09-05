package catalog

import (
	"reflect"
	"testing"
)

// TestRoleGraph: membership resolution honors INHERIT, root's implicit
// admin membership, public, and cycles do not loop.
func TestRoleGraph(t *testing.T) {
	roles := []*RoleDescriptor{
		{Name: "alice", Login: true, MemberOf: []RoleMembership{{Role: "readers"}}},
		{Name: "bob", Login: true, NoInherit: true, MemberOf: []RoleMembership{{Role: "readers", Admin: true}}},
		{Name: "readers", MemberOf: []RoleMembership{{Role: "base"}}},
		{Name: "base"},
		{Name: "loop1", MemberOf: []RoleMembership{{Role: "loop2"}}},
		{Name: "loop2", MemberOf: []RoleMembership{{Role: "loop1"}}},
	}
	g := NewRoleGraph(roles)
	eff := func(name string) []string {
		set, err := g.Effective(name)
		if err != nil {
			t.Fatal(err)
		}
		return set.Names()
	}
	if got := eff("alice"); !reflect.DeepEqual(got, []string{"alice", "base", "public", "readers"}) {
		t.Fatalf("alice: %v", got)
	}
	if got := eff("bob"); !reflect.DeepEqual(got, []string{"bob", "public"}) {
		t.Fatalf("bob (NOINHERIT): %v", got)
	}
	reach, _ := g.Reachable("bob")
	if !reach.Has("readers") || !reach.Has("base") {
		t.Fatalf("bob reachable: %v", reach.Names())
	}
	if got := eff(RootRole); !reflect.DeepEqual(got, []string{"admin", "public", "root"}) {
		t.Fatalf("root: %v", got)
	}
	if got := eff("loop1"); !reflect.DeepEqual(got, []string{"loop1", "loop2", "public"}) {
		t.Fatalf("cycle: %v", got)
	}
	if got := eff("nobody"); !reflect.DeepEqual(got, []string{"nobody", "public"}) {
		t.Fatalf("unknown: %v", got)
	}
	set, _ := g.Effective("alice")
	if set.IsAdmin() {
		t.Fatal("alice is not an admin")
	}
	d := roles[1].Clone()
	if member, admin := d.IsMemberOf("readers"); !member || !admin {
		t.Fatal("bob's admin option lost")
	}
	if !d.RemoveMembership("readers", true) || d.MemberOf[0].Admin {
		t.Fatal("admin option not removed")
	}
	if !d.RemoveMembership("readers", false) || len(d.MemberOf) != 0 {
		t.Fatal("membership not removed")
	}
	if !d.AddMembership("x", false) || d.AddMembership("x", false) || !d.AddMembership("x", true) {
		t.Fatal("AddMembership change reporting")
	}
}

// TestPrivilegeMaps: grant and revoke bookkeeping keeps storage order and
// drops empty grantees.
func TestPrivilegeMaps(t *testing.T) {
	order := []string{"SELECT", "INSERT", "UPDATE", "DELETE", "TRUNCATE"}
	var m map[string][]string
	m, changed := AddPrivileges(m, "a", []string{"DELETE", "SELECT"}, order)
	if !changed || !reflect.DeepEqual(m["a"], []string{"SELECT", "DELETE"}) {
		t.Fatalf("add: %v", m)
	}
	if _, changed := AddPrivileges(m, "a", []string{"SELECT"}, order); changed {
		t.Fatal("re-adding reported a change")
	}
	if !HasPrivilege(m, RoleSet{"a": true}, "DELETE") || HasPrivilege(m, RoleSet{"b": true}, "DELETE") || HasPrivilege(m, RoleSet{"a": true}, "INSERT") {
		t.Fatal("HasPrivilege")
	}
	if !RemovePrivileges(m, "a", []string{"SELECT", "INSERT"}, order) || !reflect.DeepEqual(m["a"], []string{"DELETE"}) {
		t.Fatalf("remove: %v", m)
	}
	if !RemovePrivileges(m, "a", []string{"DELETE"}, order) || len(m) != 0 {
		t.Fatalf("remove last: %v", m)
	}
	if RemovePrivileges(m, "zed", []string{"DELETE"}, order) {
		t.Fatal("removing from nobody reported a change")
	}
	if c := ClonePrivileges(map[string][]string{"a": {"SELECT"}}); c["a"][0] != "SELECT" || ClonePrivileges(nil) != nil {
		t.Fatal("clone")
	}
}
