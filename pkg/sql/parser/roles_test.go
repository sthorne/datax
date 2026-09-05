package parser

import (
	"reflect"
	"testing"
)

// TestParseRoles: the role, membership, ownership, scoped-grant and
// default-privilege grammar (issue #98).
func TestParseRoles(t *testing.T) {
	cr := parseOne(t, `CREATE ROLE app WITH LOGIN PASSWORD 'pw' NOINHERIT IN ROLE read_all, metrics`).(*CreateRole)
	if cr.Name != "app" || cr.IsUser || cr.Login == nil || !*cr.Login || cr.Password == nil || *cr.Password != "pw" ||
		cr.Inherit == nil || *cr.Inherit || !reflect.DeepEqual(cr.InRoles, []string{"read_all", "metrics"}) {
		t.Fatalf("create role: %+v", cr)
	}
	cr = parseOne(t, `CREATE ROLE grp`).(*CreateRole)
	if cr.Login != nil || cr.Password != nil || cr.Inherit != nil {
		t.Fatalf("bare create role: %+v", cr)
	}
	cr = parseOne(t, `CREATE USER bob PASSWORD 'x'`).(*CreateRole)
	if !cr.IsUser || cr.Login != nil {
		t.Fatalf("create user: %+v", cr)
	}
	cr = parseOne(t, `ALTER ROLE bob NOLOGIN PASSWORD NULL`).(*CreateRole)
	if !cr.Alter || cr.Login == nil || *cr.Login || cr.Password == nil || *cr.Password != "" {
		t.Fatalf("alter role: %+v", cr)
	}
	cr = parseOne(t, `ALTER USER bob WITH ENCRYPTED PASSWORD 'y' NOSUPERUSER NOCREATEDB`).(*CreateRole)
	if !cr.Alter || !cr.IsUser || *cr.Password != "y" {
		t.Fatalf("alter user: %+v", cr)
	}
	if _, err := Parse(`CREATE ROLE r SUPERUSER`); err == nil {
		t.Fatal("SUPERUSER accepted")
	}
	dr := parseOne(t, `DROP ROLE IF EXISTS a, b`).(*DropRole)
	if !dr.IfExists || !reflect.DeepEqual(dr.Names, []string{"a", "b"}) {
		t.Fatalf("drop role: %+v", dr)
	}

	// Membership.
	gr := parseOne(t, `GRANT admin TO alice`).(*GrantRevoke)
	if gr.Revoke || !reflect.DeepEqual(gr.Roles, []string{"admin"}) || !reflect.DeepEqual(gr.Grantees, []string{"alice"}) || gr.AdminOption {
		t.Fatalf("grant admin: %+v", gr)
	}
	gr = parseOne(t, `GRANT r1, r2 TO a, b WITH ADMIN OPTION`).(*GrantRevoke)
	if len(gr.Roles) != 2 || len(gr.Grantees) != 2 || !gr.AdminOption {
		t.Fatalf("grant roles with admin option: %+v", gr)
	}
	gr = parseOne(t, `REVOKE ADMIN OPTION FOR r1 FROM a`).(*GrantRevoke)
	if !gr.Revoke || !gr.AdminOption || gr.Roles[0] != "r1" || gr.Grantees[0] != "a" {
		t.Fatalf("revoke admin option: %+v", gr)
	}
	gr = parseOne(t, `REVOKE admin FROM alice CASCADE`).(*GrantRevoke)
	if !gr.Revoke || gr.Roles[0] != "admin" {
		t.Fatalf("revoke admin: %+v", gr)
	}

	// Object privileges.
	gr = parseOne(t, `GRANT SELECT, INSERT ON TABLE t1, app.t2 TO alice, PUBLIC WITH GRANT OPTION`).(*GrantRevoke)
	if gr.ObjectKind != "table" || !reflect.DeepEqual(gr.Objects, []string{"t1", "app.t2"}) ||
		!reflect.DeepEqual(gr.Grantees, []string{"alice", "public"}) || !gr.GrantOption ||
		!reflect.DeepEqual(gr.Privileges, []string{"SELECT", "INSERT"}) {
		t.Fatalf("grant on tables: %+v", gr)
	}
	gr = parseOne(t, `GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO app`).(*GrantRevoke)
	if !gr.AllInSchema || gr.ObjectKind != "table" || gr.Privileges[0] != "ALL" || gr.Grantees[0] != "app" {
		t.Fatalf("grant on all tables: %+v", gr)
	}
	gr = parseOne(t, `REVOKE GRANT OPTION FOR SELECT ON ALL SEQUENCES IN SCHEMA public FROM app RESTRICT`).(*GrantRevoke)
	if !gr.Revoke || !gr.GrantOption || !gr.AllInSchema || gr.ObjectKind != "sequence" {
		t.Fatalf("revoke on all sequences: %+v", gr)
	}
	gr = parseOne(t, `GRANT USAGE, SELECT ON SEQUENCE s TO bob`).(*GrantRevoke)
	if gr.ObjectKind != "sequence" || gr.Objects[0] != "s" || !reflect.DeepEqual(gr.Privileges, []string{"USAGE", "SELECT"}) {
		t.Fatalf("grant on sequence: %+v", gr)
	}
	gr = parseOne(t, `GRANT USAGE, CREATE ON SCHEMA public TO bob`).(*GrantRevoke)
	if gr.ObjectKind != "schema" || gr.Objects[0] != "public" {
		t.Fatalf("grant on schema: %+v", gr)
	}
	if _, err := Parse(`GRANT USAGE ON SCHEMA other TO bob`); err == nil {
		t.Fatal("unknown schema accepted")
	}
	gr = parseOne(t, `GRANT CONNECT ON DATABASE app, shop TO GROUP ops`).(*GrantRevoke)
	if gr.ObjectKind != "database" || len(gr.Objects) != 2 || gr.Grantees[0] != "ops" {
		t.Fatalf("grant on databases: %+v", gr)
	}

	// Ownership.
	ao := parseOne(t, `ALTER TABLE t OWNER TO bob`).(*AlterOwner)
	if ao.Kind != "table" || ao.Name != "t" || ao.Owner != "bob" {
		t.Fatalf("alter table owner: %+v", ao)
	}
	for _, q := range []string{`ALTER VIEW v OWNER TO bob`, `ALTER SEQUENCE s OWNER TO bob`, `ALTER TYPE mood OWNER TO bob`, `ALTER DATABASE app OWNER TO bob`} {
		ao := parseOne(t, q).(*AlterOwner)
		if ao.Owner != "bob" {
			t.Fatalf("%s: %+v", q, ao)
		}
	}
	ro := parseOne(t, `REASSIGN OWNED BY a, b TO c`).(*ReassignOwned)
	if !reflect.DeepEqual(ro.From, []string{"a", "b"}) || ro.To != "c" {
		t.Fatalf("reassign owned: %+v", ro)
	}
	do := parseOne(t, `DROP OWNED BY a CASCADE`).(*DropOwned)
	if do.Roles[0] != "a" || !do.Cascade {
		t.Fatalf("drop owned: %+v", do)
	}

	// Default privileges.
	adp := parseOne(t, `ALTER DEFAULT PRIVILEGES FOR ROLE owner_r IN SCHEMA public GRANT SELECT ON TABLES TO reader WITH GRANT OPTION`).(*AlterDefaultPrivileges)
	if adp.Revoke || adp.ForRoles[0] != "owner_r" || adp.ObjectKind != "tables" || adp.Grantees[0] != "reader" || !adp.GrantOption || adp.Privileges[0] != "SELECT" {
		t.Fatalf("alter default privileges: %+v", adp)
	}
	adp = parseOne(t, `ALTER DEFAULT PRIVILEGES REVOKE ALL ON SEQUENCES FROM reader`).(*AlterDefaultPrivileges)
	if !adp.Revoke || len(adp.ForRoles) != 0 || adp.ObjectKind != "sequences" {
		t.Fatalf("alter default privileges revoke: %+v", adp)
	}

	// SET ROLE and SHOW GRANTS.
	sv := parseOne(t, `SET ROLE app`).(*SetVar)
	if sv.Name != "role" || sv.Value != "app" || sv.Reset {
		t.Fatalf("set role: %+v", sv)
	}
	sv = parseOne(t, `SET LOCAL ROLE NONE`).(*SetVar)
	if sv.Name != "role" || !sv.Reset || !sv.Local {
		t.Fatalf("set role none: %+v", sv)
	}
	sv = parseOne(t, `RESET ROLE`).(*SetVar)
	if sv.Name != "role" || !sv.Reset {
		t.Fatalf("reset role: %+v", sv)
	}
	sh := parseOne(t, `SHOW GRANTS ON ROLE admin FOR alice`).(*Show)
	if !sh.OnRole || sh.Role != "admin" || sh.User != "alice" {
		t.Fatalf("show grants on role: %+v", sh)
	}
	sh = parseOne(t, `SHOW GRANTS ON DATABASE app`).(*Show)
	if sh.Database != "app" {
		t.Fatalf("show grants on database: %+v", sh)
	}
	sh = parseOne(t, `SHOW GRANTS ON ROLE`).(*Show)
	if !sh.OnRole || sh.Role != "" {
		t.Fatalf("show grants on role (all): %+v", sh)
	}
}
