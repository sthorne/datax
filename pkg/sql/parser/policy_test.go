package parser

import (
	"reflect"
	"strings"
	"testing"
)

// TestParseRowSecurity: ALTER TABLE's four row-security forms, and that
// a near miss is an error rather than some other ALTER TABLE.
func TestParseRowSecurity(t *testing.T) {
	for src, want := range map[string]string{
		`ALTER TABLE docs ENABLE ROW LEVEL SECURITY`:            "enable",
		`alter table docs disable row level security`:           "disable",
		`ALTER TABLE shop.docs FORCE ROW LEVEL SECURITY`:        "force",
		`ALTER TABLE docs NO FORCE ROW LEVEL SECURITY`:          "no force",
		`ALTER TABLE IF EXISTS docs ENABLE ROW LEVEL SECURITY;`: "enable",
	} {
		at := parseOne(t, src).(*AlterTable)
		if at.RowSecurity != want || !strings.HasSuffix(at.Table, "docs") {
			t.Errorf("%s: %+v", src, at)
		}
	}
	for _, src := range []string{
		`ALTER TABLE docs ENABLE ROW SECURITY`,
		`ALTER TABLE docs ENABLE`,
		`ALTER TABLE docs FORCE ROW LEVEL`,
		`ALTER TABLE docs NO ROW LEVEL SECURITY`,
	} {
		if _, err := Parse(src); err == nil {
			t.Errorf("%s: parsed", src)
		}
	}
}

func TestParseCreatePolicy(t *testing.T) {
	cp := parseOne(t, `CREATE POLICY tenant ON docs USING (tenant_id = current_setting('app.tenant')::uuid)`).(*CreatePolicy)
	if !reflect.DeepEqual(cp, &CreatePolicy{Name: "tenant", Table: "docs", Command: "all", Using: `tenant_id = current_setting('app.tenant')::uuid`}) {
		t.Fatalf("%+v", cp)
	}
	cp = parseOne(t, `create policy p on shop.docs as restrictive for update to alice, "Bob", public
		using (owner = current_user and not archived) with check (owner = current_user)`).(*CreatePolicy)
	want := &CreatePolicy{Name: "p", Table: "shop.docs", Restrictive: true, Command: "update", Roles: []string{"alice", "bob", "public"},
		Using: "owner = current_user and not archived", Check: "owner = current_user"}
	if !reflect.DeepEqual(cp, want) {
		t.Fatalf("%+v", cp)
	}
	for src, cmd := range map[string]string{
		`CREATE POLICY p ON t AS PERMISSIVE FOR ALL USING (true)`:     "all",
		`CREATE POLICY p ON t FOR SELECT USING (a > 1 OR b IS NULL)`:  "select",
		`CREATE POLICY p ON t FOR INSERT WITH CHECK (a = 1)`:          "insert",
		`CREATE POLICY p ON t FOR DELETE TO admin USING (a IN (1,2))`: "delete",
		`CREATE POLICY p ON t`: "all",
	} {
		if got := parseOne(t, src).(*CreatePolicy); got.Command != cmd || got.Restrictive {
			t.Errorf("%s: %+v", src, got)
		}
	}
	// What PostgreSQL refuses, and what cannot be a policy at all.
	for src, msg := range map[string]string{
		`CREATE POLICY p ON t FOR SELECT WITH CHECK (a = 1)`:      "WITH CHECK cannot be applied",
		`CREATE POLICY p ON t FOR DELETE WITH CHECK (a = 1)`:      "WITH CHECK cannot be applied",
		`CREATE POLICY p ON t FOR INSERT USING (a = 1)`:           "only a WITH CHECK",
		`CREATE POLICY p ON t FOR TRUNCATE USING (a = 1)`:         "expected ALL, SELECT",
		`CREATE POLICY p ON t AS LENIENT USING (a = 1)`:           "PERMISSIVE or RESTRICTIVE",
		`CREATE POLICY p ON t TO current_user USING (a = 1)`:      "name the role",
		`CREATE POLICY p ON t USING a = 1`:                        "",
		`CREATE POLICY p ON t USING (a = 1) FOR SELECT`:           "unexpected",
		`CREATE POLICY p ON t WITH CHECK (a = 1) USING (a = 1)`:   "unexpected",
		`CREATE POLICY p USING (a = 1)`:                           "expected ON",
		`CREATE POLICY p ON t USING (a = 1) WITH (a = 1)`:         "expected CHECK",
		`CREATE POLICY p ON t USING ()`:                           "",
		`CREATE POLICY p ON t USING (a = 1`:                       "",
		`CREATE POLICY p ON t TO USING (a = 1)`:                   "",
		`CREATE POLICY p ON t AS RESTRICTIVE AS PERMISSIVE`:       "unexpected",
		`CREATE POLICY ON t USING (a = 1)`:                        "",
		`CREATE POLICY p ON t FOR SELECT, UPDATE USING (a = 1)`:   "unexpected",
		`CREATE POLICY p ON t USING (a = 1); DROP TABLE t USING`:  "",
		`CREATE POLICY p ON t USING (a = 1) garbage`:              "unexpected",
		`CREATE POLICY p ON t USING (a = 1) WITH CHECK (a = 1) x`: "unexpected",
	} {
		_, err := Parse(src)
		if err == nil || !strings.Contains(err.Error(), msg) {
			t.Errorf("%s: %v, want an error containing %q", src, err, msg)
		}
	}
}

func TestParseAlterDropPolicy(t *testing.T) {
	ap := parseOne(t, `ALTER POLICY p ON docs RENAME TO q`).(*AlterPolicy)
	if ap.Name != "p" || ap.Table != "docs" || ap.RenameTo != "q" || ap.SetRoles || ap.Using != nil || ap.Check != nil {
		t.Fatalf("%+v", ap)
	}
	ap = parseOne(t, `ALTER POLICY p ON docs TO PUBLIC USING (a = 1) WITH CHECK (a = 2)`).(*AlterPolicy)
	if !ap.SetRoles || len(ap.Roles) != 1 || ap.Roles[0] != "public" || ap.Using == nil || *ap.Using != "a = 1" || ap.Check == nil || *ap.Check != "a = 2" {
		t.Fatalf("%+v", ap)
	}
	ap = parseOne(t, `ALTER POLICY p ON docs WITH CHECK (true)`).(*AlterPolicy)
	if ap.SetRoles || ap.Using != nil || ap.Check == nil || *ap.Check != "true" {
		t.Fatalf("%+v", ap)
	}
	for _, src := range []string{
		`ALTER POLICY p ON docs`,
		`ALTER POLICY p ON docs FOR SELECT`,
		`ALTER POLICY p ON docs AS RESTRICTIVE`,
		`ALTER POLICY p ON docs RENAME TO q USING (a = 1)`,
		`ALTER POLICY p RENAME TO q`,
	} {
		if _, err := Parse(src); err == nil {
			t.Errorf("%s: parsed", src)
		}
	}
	dp := parseOne(t, `DROP POLICY IF EXISTS p ON shop.docs CASCADE`).(*DropPolicy)
	if dp.Name != "p" || dp.Table != "shop.docs" || !dp.IfExists {
		t.Fatalf("%+v", dp)
	}
	if dp := parseOne(t, `DROP POLICY p ON docs`).(*DropPolicy); dp.IfExists || dp.Name != "p" {
		t.Fatalf("%+v", dp)
	}
	if _, err := Parse(`DROP POLICY p`); err == nil {
		t.Error("DROP POLICY without ON parsed")
	}
}

// TestParsePolicyExpr: a stored expression lowers to the conjuncts that
// hold for the rows it admits, OR groups and negation included.
func TestParsePolicyExpr(t *testing.T) {
	conds, err := ParsePolicyExpr(`tenant_id = current_setting('app.tenant') AND (owner = current_user OR shared)`)
	if err != nil || len(conds) != 2 || conds[0].Column != "tenant_id" || conds[0].Op != "=" || conds[0].Value.Func != "current_setting" ||
		conds[1].Op != "OR" || len(conds[1].Or) != 2 {
		t.Fatalf("%+v, %v", conds, err)
	}
	if conds, err = ParsePolicyExpr(`true`); err != nil || len(conds) != 1 {
		t.Fatalf("true: %+v, %v", conds, err)
	}
	if conds, err = ParsePolicyExpr(`NOT (a = 1)`); err != nil || len(conds) != 1 || conds[0].Op != "!=" {
		t.Fatalf("NOT: %+v, %v", conds, err)
	}
	for _, src := range []string{`a = `, `a = 1) OR (true`, ``} {
		if _, err := ParsePolicyExpr(src); err == nil {
			t.Errorf("%q: parsed", src)
		}
	}
}

// TestParseRoleBypassRLS: BYPASSRLS and NOBYPASSRLS are recorded; a nil
// option was not spelled.
func TestParseRoleBypassRLS(t *testing.T) {
	cr := parseOne(t, `CREATE ROLE auditor LOGIN BYPASSRLS`).(*CreateRole)
	if cr.BypassRLS == nil || !*cr.BypassRLS || cr.Login == nil || !*cr.Login {
		t.Fatalf("%+v", cr)
	}
	cr = parseOne(t, `ALTER ROLE auditor NOBYPASSRLS`).(*CreateRole)
	if cr.BypassRLS == nil || *cr.BypassRLS || !cr.Alter {
		t.Fatalf("%+v", cr)
	}
	if cr = parseOne(t, `CREATE USER u PASSWORD 'x'`).(*CreateRole); cr.BypassRLS != nil {
		t.Fatalf("%+v", cr)
	}
}

// TestParseCustomSettings: SET, RESET and SHOW take a custom setting's
// two-part name, and set_config parses with its three arguments.
func TestParseCustomSettings(t *testing.T) {
	sv := parseOne(t, `SET app.tenant = 'a1'`).(*SetVar)
	if sv.Name != "app.tenant" || sv.Value != "a1" || sv.Local || sv.Reset {
		t.Fatalf("%+v", sv)
	}
	if sv = parseOne(t, `SET LOCAL App.User TO 42`).(*SetVar); sv.Name != "app.user" || sv.Value != "42" || !sv.Local {
		t.Fatalf("%+v", sv)
	}
	if sv = parseOne(t, `RESET app.tenant`).(*SetVar); sv.Name != "app.tenant" || !sv.Reset {
		t.Fatalf("%+v", sv)
	}
	if sv = parseOne(t, `SHOW app.tenant`).(*SetVar); sv.Name != "show:app.tenant" {
		t.Fatalf("%+v", sv)
	}
	if sv = parseOne(t, `SHOW TIME ZONE`).(*SetVar); sv.Name != "show:time_zone" {
		t.Fatalf("%+v", sv)
	}
	sel := parseOne(t, `SELECT set_config('app.tenant', $1, false)`).(*Select)
	if e := sel.Exprs[0].Expr; e.Func != "set_config" || len(e.Args) != 3 {
		t.Fatalf("%+v", e)
	}
	if _, err := Parse(`SELECT set_config('app.tenant', 'x')`); err == nil {
		t.Error("set_config with two arguments parsed")
	}
	if _, err := Parse(`SET app. = 1`); err == nil {
		t.Error("SET with an empty name part parsed")
	}
}

// TestPolicyStatementKinds: the fingerprint names the three statements
// by their words and their table, and renders no expression (a policy's
// text may carry literals).
func TestPolicyStatementKinds(t *testing.T) {
	for src, want := range map[string]string{
		`CREATE POLICY p ON docs USING (tenant = 'acme')`: "CREATE POLICY",
		`ALTER POLICY p ON docs USING (tenant = 'acme')`:  "ALTER POLICY",
		`DROP POLICY p ON docs`:                           "DROP POLICY",
	} {
		stmt := parseOne(t, src)
		if got := statementKindOf(stmt); got != want {
			t.Errorf("%s: kind %q, want %q", src, got, want)
		}
		if got := statementSubject(stmt); got != "docs" {
			t.Errorf("%s: subject %q", src, got)
		}
	}
}
