package sql

import (
	"strings"
	"testing"

	"github.com/sthorne/datax/pkg/sql/catalog"
	"github.com/sthorne/datax/pkg/sql/parser"
	"github.com/sthorne/datax/pkg/sql/types"
)

// rlsDesc: PRIMARY KEY (tenant, id); index by_owner on (owner).
func rlsDesc() *catalog.TableDescriptor {
	return &catalog.TableDescriptor{
		ID: 77, Name: "docs", Owner: "app_owner", RowSecurity: true,
		Columns: []catalog.Column{
			{ID: 1, Name: "tenant", Type: types.String, NotNull: true},
			{ID: 2, Name: "id", Type: types.Int, NotNull: true},
			{ID: 3, Name: "owner", Type: types.String},
			{ID: 4, Name: "shared", Type: types.Bool},
			{ID: 5, Name: "rowid", Type: types.Int, Hidden: true},
		},
		PrimaryKey: []catalog.ColumnID{1, 2},
		Indexes:    []catalog.IndexDescriptor{{ID: 2, Name: "by_owner", ColumnIDs: []catalog.ColumnID{3}}},
	}
}

// TestPolicyApplies: a policy governs its own command or ALL, for the
// roles in its list — held directly or through membership — or for
// every role when the list is empty or names public.
func TestPolicyApplies(t *testing.T) {
	alice := catalog.RoleSet{"alice": true, "staff": true, catalog.PublicRole: true}
	for name, c := range map[string]struct {
		p    catalog.Policy
		cmd  string
		want bool
	}{
		"all covers select":       {catalog.Policy{Command: catalog.PolicyAll}, catalog.PolicySelect, true},
		"all covers delete":       {catalog.Policy{Command: catalog.PolicyAll}, catalog.PolicyDelete, true},
		"select is not update":    {catalog.Policy{Command: catalog.PolicySelect}, catalog.PolicyUpdate, false},
		"insert is insert":        {catalog.Policy{Command: catalog.PolicyInsert}, catalog.PolicyInsert, true},
		"named role":              {catalog.Policy{Command: catalog.PolicyAll, Roles: []string{"alice"}}, catalog.PolicySelect, true},
		"role through membership": {catalog.Policy{Command: catalog.PolicyAll, Roles: []string{"staff"}}, catalog.PolicySelect, true},
		"someone else's":          {catalog.Policy{Command: catalog.PolicyAll, Roles: []string{"bob"}}, catalog.PolicySelect, false},
		"one of several":          {catalog.Policy{Command: catalog.PolicyAll, Roles: []string{"bob", "staff"}}, catalog.PolicySelect, true},
		"public in the list":      {catalog.Policy{Command: catalog.PolicyAll, Roles: []string{"bob", catalog.PublicRole}}, catalog.PolicySelect, true},
	} {
		if got := policyApplies(&c.p, c.cmd, alice); got != c.want {
			t.Errorf("%s: %v, want %v", name, got, c.want)
		}
	}
}

// TestValidatePolicyExpr: what a policy expression may hold. It names
// the table's own visible columns, calls the session functions that
// have no effect, and carries no subquery, parameter or volatile call.
func TestValidatePolicyExpr(t *testing.T) {
	desc := rlsDesc()
	for _, ok := range []string{
		`tenant = current_setting('app.tenant')`,
		`tenant = current_setting('app.tenant', true) AND (owner = current_user OR shared)`,
		`owner = session_user`,
		`lower(owner) = 'alice' AND id > 10`,
		`true`,
		`id IN (1, 2, 3)`,
		`owner IS NOT NULL`,
		`tenant = current_database()`,
		`owner = current_role OR owner = user OR tenant = current_catalog`,
	} {
		if err := validatePolicyExpr(desc, "USING", ok); err != nil {
			t.Errorf("%s: %v", ok, err)
		}
	}
	for bad, want := range map[string]string{
		`nosuch = 1`:                             CodeUndefinedColumn,
		`docs.tenant = 'a'`:                      CodeUndefinedColumn,
		`rowid = 1`:                              CodeUndefinedColumn, // hidden
		`id IN (SELECT id FROM docs)`:            CodeFeatureNotSupported,
		`id = (SELECT max(id) FROM docs)`:        CodeFeatureNotSupported,
		`EXISTS (SELECT 1 FROM docs)`:            CodeFeatureNotSupported,
		`tenant = $1`:                            CodeSyntaxError,
		`id = nextval('s')`:                      CodeFeatureNotSupported,
		`owner = pg_notify('c', 'x')`:            CodeFeatureNotSupported,
		`owner = set_config('app.t', 'x', true)`: CodeFeatureNotSupported,
		`owner = pg_sleep(1)`:                    CodeFeatureNotSupported,
		`tenant = `:                              CodeSyntaxError,
		`shared OR id = (SELECT 1)`:              CodeFeatureNotSupported,
		`id = 1 OR owner = gen_random_uuid()`:    CodeFeatureNotSupported,
	} {
		err := validatePolicyExpr(desc, "USING", bad)
		serr, _ := err.(*Error)
		if serr == nil || serr.Code != want {
			t.Errorf("%s: %v, want %s", bad, err, want)
		}
	}
}

// TestPolicyColumnUses: the policies that read a column, for DROP
// COLUMN's refusal; and RENAME COLUMN's rewrite of both expressions.
func TestPolicyColumnUses(t *testing.T) {
	desc := rlsDesc()
	desc.Policies = []catalog.Policy{
		{Name: "tenant", Command: catalog.PolicyAll, Using: `tenant = current_setting('app.tenant')`},
		{Name: "mine", Command: catalog.PolicyUpdate, Using: `owner = current_user OR shared`, Check: `owner = current_user`},
		{Name: "open", Command: catalog.PolicySelect},
	}
	for col, want := range map[string]string{"tenant": "tenant", "owner": "mine", "shared": "mine", "id": ""} {
		if got := strings.Join(policyColumnUses(desc, col), ","); got != want {
			t.Errorf("uses of %s: %q, want %q", col, got, want)
		}
	}
	if err := renamePolicyColumn(desc, "owner", "author"); err != nil {
		t.Fatal(err)
	}
	if p := desc.Policies[1]; !strings.Contains(p.Using, "author = current_user") || p.Check != "author = current_user" || strings.Contains(p.Using, "owner") {
		t.Fatalf("after rename: %+v", p)
	}
	if desc.Policies[0].Using != `tenant = current_setting('app.tenant')` || desc.Policies[2].Using != "" {
		t.Fatalf("rename touched other policies: %+v", desc.Policies)
	}
}

// TestUpdateReadsTable: which UPDATEs also answer to the SELECT policies
// — the ones that read the rows they change.
func TestUpdateReadsTable(t *testing.T) {
	for src, want := range map[string]bool{
		`UPDATE docs SET shared = true`:                       false,
		`UPDATE docs SET owner = 'bob', shared = false`:       false,
		`UPDATE docs SET shared = true WHERE id = 1`:          true,
		`UPDATE docs SET id = id + 1`:                         true,
		`UPDATE docs SET owner = lower(owner)`:                true,
		`UPDATE docs SET shared = true RETURNING id`:          true,
		`UPDATE docs SET owner = (SELECT max(owner) FROM t2)`: true,
	} {
		stmts, err := parser.Parse(src)
		if err != nil {
			t.Fatalf("%s: %v", src, err)
		}
		if got := updateReadsTable(stmts[0].(*parser.Update), nil); got != want {
			t.Errorf("%s: %v, want %v", src, got, want)
		}
	}
}

// TestPolicyConjunctsBoundScan: a tenant policy's conjunct, placed at
// the head of a statement's WHERE as securedWhere places it, bounds the
// scan to the tenant's primary-key prefix; with the statement's own
// key predicate it is a point lookup; and a policy on an indexed column
// is an index scan. Nothing reads the other tenants' span.
func TestPolicyConjunctsBoundScan(t *testing.T) {
	desc := rlsDesc()
	policy, err := parser.ParsePolicyExpr(`tenant = 'acme'`)
	if err != nil {
		t.Fatal(err)
	}
	plan := mustPlan(t, desc, policy)
	if plan.kind != planPKScan || len(plan.idxVals) != 1 || plan.idxVals[0].S != "acme" || len(plan.residual) != 0 {
		t.Fatalf("policy alone: %s (%+v)", plan.String(), plan)
	}
	plan = mustPlan(t, desc, append(append([]parser.Comparison{}, policy...), cmpInt("id", "=", 7)))
	if plan.kind != planPKPoint {
		t.Fatalf("policy + key: %s", plan.String())
	}
	byOwner, err := parser.ParsePolicyExpr(`owner = 'alice'`)
	if err != nil {
		t.Fatal(err)
	}
	if plan = mustPlan(t, desc, byOwner); plan.kind != planIndexScan || plan.idx.Name != "by_owner" {
		t.Fatalf("policy on an indexed column: %s", plan.String())
	}
	// Default deny plans, and matches nothing.
	deny := []parser.Comparison{{Op: "FALSE"}}
	if _, err := pickPlan(desc, append(deny, cmpInt("id", "=", 7)), nil); err != nil {
		t.Fatalf("default deny does not plan: %v", err)
	}
	row := map[catalog.ColumnID]types.Datum{1: types.NewString("acme"), 2: types.NewInt(7)}
	if ok, err := matchesWhere(deny, desc, row, nil); err != nil || ok {
		t.Fatalf("default deny matched a row: %v %v", ok, err)
	}
}

// TestPolicyFilterRunsFirst: the policy's conjuncts lead the WHERE
// clause, so a predicate of the statement that would fail on a hidden
// row — and so reveal something about it — is never evaluated there.
func TestPolicyFilterRunsFirst(t *testing.T) {
	desc := rlsDesc()
	policy, _ := parser.ParsePolicyExpr(`tenant = 'acme'`)
	leaky, err := parser.ParsePolicyExpr(`1 / (id - 7) > 0`)
	if err != nil {
		t.Fatal(err)
	}
	where := append(append([]parser.Comparison{}, policy...), leaky...)
	hidden := map[catalog.ColumnID]types.Datum{1: types.NewString("other"), 2: types.NewInt(7)}
	if ok, err := matchesWhere(where, desc, hidden, nil); err != nil || ok {
		t.Fatalf("hidden row: matched=%v err=%v, want it filtered before the division", ok, err)
	}
	// The same predicate does fail on a row the policy admits: the error
	// is the statement's own to see.
	own := map[catalog.ColumnID]types.Datum{1: types.NewString("acme"), 2: types.NewInt(7)}
	if _, err := matchesWhere(where, desc, own, nil); err == nil {
		t.Fatal("division by zero on a visible row raised nothing: the test proves nothing")
	}
}

// TestRowCheck: a written row must be TRUE under the bound check; NULL
// refuses it (unlike a CHECK constraint), and the error names the table
// and nothing of the row.
func TestRowCheck(t *testing.T) {
	desc := rlsDesc()
	check, _ := parser.ParsePolicyExpr(`owner = 'alice'`)
	rc := &rowCheck{desc: desc, insert: check, update: check}
	row := func(owner types.Datum) map[catalog.ColumnID]types.Datum {
		return map[catalog.ColumnID]types.Datum{1: types.NewString("secret-tenant"), 2: types.NewInt(1), 3: owner}
	}
	if err := rc.checkInsert(row(types.NewString("alice"))); err != nil {
		t.Fatalf("admitted row refused: %v", err)
	}
	for _, d := range []types.Datum{types.NewString("bob"), types.DNull} {
		for _, err := range []error{rc.checkInsert(row(d)), rc.checkUpdate(row(d))} {
			serr, _ := err.(*Error)
			if serr == nil || serr.Code != CodeInsufficientPriv || serr.Msg != `new row violates row-level security policy for table "docs"` {
				t.Fatalf("owner %v: %v", d, err)
			}
			if strings.Contains(serr.Msg, "secret-tenant") || strings.Contains(serr.Msg, "bob") {
				t.Fatalf("the refusal carries row data: %s", serr.Msg)
			}
		}
	}
	// A nil check (row security does not apply) admits everything.
	var none *rowCheck
	if err := none.checkInsert(row(types.DNull)); err != nil {
		t.Fatal(err)
	}
	if err := none.checkUpdate(row(types.DNull)); err != nil {
		t.Fatal(err)
	}
	// An empty, non-nil check list is "no rows" only for FALSE.
	rc = &rowCheck{desc: desc, insert: []parser.Comparison{{Op: "FALSE"}}}
	if err := rc.checkInsert(row(types.NewString("alice"))); err == nil {
		t.Fatal("default deny admitted an insert")
	}
}

// TestCustomSettings: a custom setting is unknown until set, keeps any
// text, and a saved copy of the variables (SET LOCAL's) is not changed
// by a later SET.
func TestCustomSettings(t *testing.T) {
	if isCustomVar("statement_timeout") || isCustomVar(".x") || isCustomVar("x.") || !isCustomVar("app.tenant") || !isCustomVar("a.b.c") {
		t.Fatal("isCustomVar")
	}
	s := &Session{vars: defaultVars()}
	if _, _, ok := s.setting("app.tenant"); ok {
		t.Fatal("an unset custom setting exists")
	}
	setCustomVar(&s.vars, "app.tenant", "a")
	saved := s.vars
	setCustomVar(&s.vars, "app.tenant", "b")
	setCustomVar(&s.vars, "app.other", "")
	if name, v, ok := s.setting("App.Tenant"); !ok || v != "b" || name != "app.tenant" {
		t.Fatalf("setting: %q %q %v", name, v, ok)
	}
	if _, v, ok := s.setting("app.other"); !ok || v != "" {
		t.Fatalf("an empty custom setting: %q %v", v, ok)
	}
	if saved.custom["app.tenant"] != "a" || len(saved.custom) != 1 {
		t.Fatalf("the saved variables changed with the live ones: %+v", saved.custom)
	}
}
