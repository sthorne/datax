package parser

import (
	"reflect"
	"testing"
)

// Schemas (gap PG-D-001): the statements, and qualified names kept as
// written for the catalog to resolve.
func TestParseSchemas(t *testing.T) {
	for src, want := range map[string]*CreateSchema{
		`CREATE SCHEMA s`:                                  {Name: "s"},
		`CREATE SCHEMA IF NOT EXISTS s`:                    {Name: "s", IfNotExists: true},
		`CREATE SCHEMA s AUTHORIZATION bob`:                {Name: "s", Authorization: "bob"},
		`CREATE SCHEMA AUTHORIZATION bob`:                  {Authorization: "bob"},
		`CREATE SCHEMA IF NOT EXISTS AUTHORIZATION "Bob"`:  {IfNotExists: true, Authorization: "Bob"},
		`create schema "Mixed" authorization current_user`: {Name: "Mixed", Authorization: "current_user"},
	} {
		got, ok := parseOne(t, src).(*CreateSchema)
		if !ok || !reflect.DeepEqual(got, want) {
			t.Errorf("%s: %+v, want %+v", src, got, want)
		}
	}
	parseCode(t, `CREATE SCHEMA s CREATE TABLE t (a INT)`, "0A000", "CREATE SCHEMA with schema elements is not supported: create the schema, then its objects")

	if ds := parseOne(t, `DROP SCHEMA IF EXISTS a, b CASCADE`).(*DropSchema); !ds.IfExists || !ds.Cascade || !reflect.DeepEqual(ds.Names, []string{"a", "b"}) {
		t.Fatalf("DROP SCHEMA: %+v", ds)
	}
	if ds := parseOne(t, `DROP SCHEMA a RESTRICT`).(*DropSchema); ds.Cascade || ds.Names[0] != "a" {
		t.Fatalf("DROP SCHEMA RESTRICT: %+v", ds)
	}
	if ar := parseOne(t, `ALTER SCHEMA a RENAME TO b`).(*AlterSchemaRename); ar.Name != "a" || ar.NewName != "b" {
		t.Fatalf("ALTER SCHEMA RENAME: %+v", ar)
	}
	if ao := parseOne(t, `ALTER SCHEMA a OWNER TO bob`).(*AlterOwner); ao.Kind != "schema" || ao.Name != "a" || ao.Owner != "bob" {
		t.Fatalf("ALTER SCHEMA OWNER: %+v", ao)
	}
	for src, want := range map[string]SetSchema{
		`ALTER TABLE t SET SCHEMA s`:                  {Kind: "table", Name: "t", Schema: "s"},
		`ALTER TABLE IF EXISTS public.t SET SCHEMA s`: {Kind: "table", Name: "public.t", Schema: "s", IfExists: true},
		`ALTER VIEW s.v SET SCHEMA public`:            {Kind: "view", Name: "s.v", Schema: "public"},
		`ALTER VIEW IF EXISTS v SET SCHEMA s`:         {Kind: "view", Name: "v", Schema: "s", IfExists: true},
		`ALTER SEQUENCE q SET SCHEMA s`:               {Kind: "sequence", Name: "q", Schema: "s"},
	} {
		got, ok := parseOne(t, src).(*SetSchema)
		if !ok || *got != want {
			t.Errorf("%s: %+v, want %+v", src, got, want)
		}
	}
	// Qualified names keep every part; a schema-qualified column binds
	// by its table's name.
	sel := parseOne(t, `SELECT s.t.a, t.b FROM s.t JOIN db.s.u ON true`).(*Select)
	if sel.Table != "s.t" || sel.Joins[0].Table != "db.s.u" || sel.Exprs[0].Expr.Column != "t.a" {
		t.Fatalf("qualified names: table %q, join %q, column %q", sel.Table, sel.Joins[0].Table, sel.Exprs[0].Expr.Column)
	}
	if co := parseOne(t, `COMMENT ON COLUMN s.t.a IS 'x'`).(*CommentOn); co.Name != "s.t" || co.Column != "a" {
		t.Fatalf("COMMENT ON COLUMN s.t.a: %+v", co)
	}
	if gr := parseOne(t, `GRANT SELECT ON ALL TABLES IN SCHEMA s, public TO bob`).(*GrantRevoke); !gr.AllInSchema || !reflect.DeepEqual(gr.Schemas, []string{"s", "public"}) {
		t.Fatalf("ALL TABLES IN SCHEMA: %+v", gr)
	}
	parseCode(t, `ALTER DEFAULT PRIVILEGES IN SCHEMA s GRANT SELECT ON TABLES TO bob`, "0A000", "ALTER DEFAULT PRIVILEGES IN SCHEMA s is not supported: only the public schema takes default privileges")
	if sv := parseOne(t, `SET search_path = "$user", "Mixed", s, 'a, b'`).(*SetVar); sv.Value != `"$user", "Mixed", s, a, b` {
		t.Fatalf("SET search_path: %q", sv.Value)
	}
}
