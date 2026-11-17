package sql

import (
	"reflect"
	"testing"
)

// TestParseQualifiedName: names read as PostgreSQL's regclass and
// to_reg* input reads them — dotted parts, unquoted ones lower-cased,
// quoted ones kept, "" an embedded quote.
func TestParseQualifiedName(t *testing.T) {
	for in, want := range map[string][]string{
		`t`:                    {"t"},
		`T`:                    {"t"},
		` public . T `:         {"public", "t"},
		`"Mixed"`:              {"Mixed"},
		`public."Mi.xed"`:      {"public", "Mi.xed"},
		`"a""b"`:               {`a"b`},
		`db.public.t`:          {"db", "public", "t"},
		`pg_catalog.pg_class`:  {"pg_catalog", "pg_class"},
		`"pg_catalog"."pg_am"`: {"pg_catalog", "pg_am"},
	} {
		got, ok := parseQualifiedName(in)
		if !ok || !reflect.DeepEqual(got, want) {
			t.Errorf("%q: %q %v, want %q", in, got, ok, want)
		}
	}
	for _, bad := range []string{``, ` `, `.t`, `t.`, `a b`, `"unterminated`, `""`, `t."`} {
		if got, ok := parseQualifiedName(bad); ok {
			t.Errorf("%q: %q, want not a name", bad, got)
		}
	}
}

// TestParsePrivileges: a privilege list is comma-separated and
// case-insensitive, each entry optionally WITH GRANT OPTION (for roles
// also WITH ADMIN OPTION); anything else is 22023.
func TestParsePrivileges(t *testing.T) {
	table := []string{"SELECT", "INSERT"}
	got, err := parsePrivileges(` select , Insert WITH GRANT OPTION`, table, false)
	if err != nil || !reflect.DeepEqual(got, []privSpec{{priv: "SELECT"}, {priv: "INSERT", option: true}}) {
		t.Fatalf("%+v %v", got, err)
	}
	roles := []string{"MEMBER", "USAGE", "SET"}
	if got, err := parsePrivileges(`usage with admin option`, roles, true); err != nil || !reflect.DeepEqual(got, []privSpec{{priv: "USAGE", option: true}}) {
		t.Fatalf("role admin option: %+v %v", got, err)
	}
	for _, bad := range []string{`UPDATE`, `SELECT,`, `SELECT WITH ADMIN OPTION`, ``, `SELECT  WITH GRANT OPTION`} {
		_, err := parsePrivileges(bad, table, false)
		if e, ok := err.(*Error); !ok || e.Code != CodeInvalidParameterValue {
			t.Errorf("%q: %v, want 22023", bad, err)
		}
	}
}

// TestQuoteIdentifier: names come back as quote_ident renders them.
func TestQuoteIdentifier(t *testing.T) {
	for in, want := range map[string]string{
		"t_id_seq":        "t_id_seq",
		"T_id_seq":        `"T_id_seq"`,
		"1abc":            `"1abc"`,
		"user":            `"user"`,
		"a b":             `"a b"`,
		`q"t`:             `"q""t"`,
		"current_role_id": "current_role_id",
	} {
		if got := quoteIdentifier(in); got != want {
			t.Errorf("%q: %s, want %s", in, got, want)
		}
	}
}
