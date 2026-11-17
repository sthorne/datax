package parser

import "testing"

// TestRoleKeywords (issue #357): user, current_role and current_catalog
// are values without parentheses, like current_user and session_user;
// written in double quotes each is a column; and any word, reserved or
// not, may follow AS, and user may stand as a bare label, as in
// PostgreSQL.
func TestRoleKeywords(t *testing.T) {
	sel := parseOne(t, `SELECT user, current_role, current_catalog, session_user, current_user, "current_role", "user" FROM t`).(*Select)
	want := []string{"user", "current_role", "current_catalog", "session_user", "current_user"}
	for i, f := range want {
		if e := sel.Exprs[i].Expr; e.Func != f || e.Column != "" {
			t.Fatalf("item %d: %+v, want the %s keyword", i, e, f)
		}
	}
	if e := sel.Exprs[5].Expr; e.Func != "" || e.Column != "current_role" {
		t.Fatalf(`"current_role": %+v, want the column`, e)
	}
	if e := sel.Exprs[6].Expr; e.Func != "" || e.Column != "user" {
		t.Fatalf(`"user": %+v, want the column`, e)
	}

	sel = parseOne(t, `SELECT current_user AS user, 1 AS select, 2 user, 3 current_role, user AS who, current_role::text`).(*Select)
	for i, alias := range []string{"user", "select", "user", "current_role", "who", ""} {
		if sel.Exprs[i].Alias != alias {
			t.Fatalf("item %d: alias %q, want %q", i, sel.Exprs[i].Alias, alias)
		}
	}
	if e := sel.Exprs[5].Expr; e.Func != "current_role" || e.Cast != "text" {
		t.Fatalf("current_role::text: %+v", e)
	}

	sel = parseOne(t, `SELECT * FROM t WHERE owner = user OR owner = current_role`).(*Select)
	if len(sel.Where) != 1 {
		t.Fatalf("WHERE: %+v", sel.Where)
	}
	if _, err := Parse(`SELECT user FROM`); err == nil {
		t.Fatal("SELECT user FROM accepted")
	}
}
