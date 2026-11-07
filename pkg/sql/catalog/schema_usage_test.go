package catalog

import "testing"

// TestSchemaUsageDenied (issue #259): a database denies its schema to a
// role set only when usage is restricted and the set is neither admin,
// nor the owner, nor a holder of USAGE directly or through a role.
func TestSchemaUsageDenied(t *testing.T) {
	open := &DatabaseDescriptor{ID: 1, Name: "open", Owner: "alice"}
	restricted := &DatabaseDescriptor{ID: 2, Name: "sealed", Owner: "alice", UsageRestricted: true,
		SchemaPrivileges: map[string][]string{"auditors": {PrivUsage}, "carol": {PrivCreate}}}
	dbs := []*DatabaseDescriptor{open, restricted, nil}
	cases := []struct {
		name string
		set  RoleSet
		want map[uint64]bool
	}{
		{"admin", RoleSet{AdminRole: true, "bob": true}, nil},
		{"the owner", RoleSet{"alice": true}, nil},
		{"a USAGE holder through a role", RoleSet{"bob": true, "auditors": true}, nil},
		{"a CREATE holder without USAGE", RoleSet{"carol": true}, map[uint64]bool{2: true}},
		{"no grant at all", RoleSet{"bob": true}, map[uint64]bool{2: true}},
	}
	for _, c := range cases {
		got := SchemaUsageDenied(dbs, c.set)
		if len(got) != len(c.want) {
			t.Errorf("%s: denied %v, want %v", c.name, got, c.want)
			continue
		}
		for id := range c.want {
			if !got[id] {
				t.Errorf("%s: database %d not denied", c.name, id)
			}
		}
	}
	if SchemaUsageDenied(nil, RoleSet{"bob": true}) != nil {
		t.Error("no databases: something denied")
	}
}
