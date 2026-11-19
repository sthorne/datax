package parser

import (
	"errors"
	"testing"

	"github.com/sthorne/datax/pkg/sql/types"
)

// TestQuotedTypeNames (issue #367): a quoted type name keeps its case
// wherever a type name appears — a column definition, ALTER COLUMN ...
// TYPE, ::"T" and CAST(x AS "T"), "T"[] — and an unquoted one is
// folded to lower case.
func TestQuotedTypeNames(t *testing.T) {
	ct := parseOne(t, `CREATE TABLE t (a "Role", b Role, c "public"."Role", d db."Role", e "a""b", f "int4", g "text")`).(*CreateTable)
	want := []struct {
		fam  types.Family
		name string
	}{{types.Enum, "Role"}, {types.Enum, "role"}, {types.Enum, "Role"}, {types.Enum, "db.Role"}, {types.Enum, `a"b`}, {types.Int, ""}, {types.String, ""}}
	for i, w := range want {
		if c := ct.Columns[i]; c.Type != w.fam || c.TypeName != w.name {
			t.Errorf("column %s: %v %q, want %v %q", c.Name, c.Type, c.TypeName, w.fam, w.name)
		}
	}

	at := parseOne(t, `ALTER TABLE t ALTER COLUMN k TYPE "Role"`).(*AlterTable)
	if at.SetType == nil || at.SetType.Type != types.Enum || at.SetType.TypeName != "Role" {
		t.Errorf("ALTER COLUMN TYPE: %+v", at.SetType)
	}

	for src, cast := range map[string]string{
		`SELECT 'a'::"Role"`:                       "Role",
		`SELECT 'a'::Role`:                         "role",
		`SELECT 'a'::"public"."Role"`:              "public.Role",
		`SELECT 'a'::s.mood`:                       "s.mood",
		`SELECT 'a'::pg_catalog.text`:              "text",
		`SELECT 'a'::public.text`:                  "text",
		`SELECT CAST('a' AS "Role")`:               "Role",
		`SELECT CAST($1::text AS "public"."Role")`: "public.Role",
		`SELECT 'a'::"Role"[]`:                     "Role[]",
		`SELECT 'a'::"a""b"`:                       `a"b`,
		`SELECT 'a'::"text"`:                       "text",
		`SELECT 'a'::TEXT`:                         "text",
	} {
		sel := parseOneSelect(t, src)
		if got := sel.Exprs[0].Expr.Cast; got != cast {
			t.Errorf("%s: cast %q, want %q", src, got, cast)
		}
	}

	if _, err := Parse(`CREATE TABLE t (a "Role"[])`); err == nil {
		t.Errorf(`"Role"[]: no error (arrays of enums are not supported)`)
	} else if se := (*SyntaxError)(nil); !errors.As(err, &se) || se.Msg != `arrays of enum type "Role" are not supported` {
		t.Errorf(`"Role"[]: %v`, err)
	}

	cty := parseOne(t, `CREATE TYPE "Role" AS ENUM ('A')`).(*CreateType)
	aty := parseOne(t, `ALTER TYPE "Role" ADD VALUE 'B'`).(*AlterType)
	dty := parseOne(t, `DROP TYPE "a""b"`).(*DropType)
	if cty.Name != "Role" || aty.Name != "Role" || dty.Name != `a"b` {
		t.Errorf("CREATE / ALTER / DROP TYPE names: %q %q %q", cty.Name, aty.Name, dty.Name)
	}
}

// TestQuotedIdentifierEscapes: "" inside a quoted identifier is one ",
// in every position an identifier takes; an empty or unterminated
// quoted identifier is refused as PostgreSQL refuses it.
func TestQuotedIdentifierEscapes(t *testing.T) {
	ct := parseOne(t, `CREATE TABLE "a""b" ("c""d" INT8 PRIMARY KEY, CONSTRAINT "k""1" CHECK ("c""d" > 0))`).(*CreateTable)
	if ct.Name != `a"b` || ct.Columns[0].Name != `c"d` || ct.Constraints[0].Name != `k"1` {
		t.Errorf("CREATE TABLE: %q %q %q", ct.Name, ct.Columns[0].Name, ct.Constraints[0].Name)
	}
	ci := parseOne(t, `CREATE INDEX "i""x" ON "a""b" ("c""d")`).(*CreateIndex)
	if ci.Name != `i"x` || ci.Table != `a"b` || len(ci.Columns) != 1 || ci.Columns[0] != `c"d` {
		t.Errorf("CREATE INDEX: %+v", ci)
	}
	sel := parseOneSelect(t, `SELECT "x""y"."c""d" AS """" FROM "a""b" "x""y"`)
	if sel.Table != `a"b` || sel.Alias != `x"y` || sel.Exprs[0].Expr.Column != `x"y.c"d` || sel.Exprs[0].Alias != `"` {
		t.Errorf("SELECT: table %q alias %q column %q label %q", sel.Table, sel.Alias, sel.Exprs[0].Expr.Column, sel.Exprs[0].Alias)
	}
	for src, msg := range map[string]string{
		`SELECT ""`:      `zero-length delimited identifier at or near """"`,
		`SELECT 1 AS ""`: `zero-length delimited identifier at or near """"`,
		`SELECT "abc`:    `unterminated quoted identifier at or near ""abc"`,
		`SELECT "a""`:    `unterminated quoted identifier at or near ""a"""`,
	} {
		_, err := Parse(src)
		var se *SyntaxError
		if !errors.As(err, &se) || se.Code != "42601" || se.Msg != msg {
			t.Errorf("%s: %v, want 42601 %s", src, err, msg)
		}
	}
	// The rename rewrite of a stored CHECK finds the whole quoted name.
	got, err := RenameColumnRefs(`("c""d" > 0) AND (cd > 1)`, `c"d`, `e"f`)
	if err != nil || got != `("e""f" > 0) AND (cd > 1)` {
		t.Errorf("RenameColumnRefs: %q %v", got, err)
	}
}

// TestQuotedKeywordsAsIdentifiers: a quoted identifier is never a
// keyword, however it is spelled — "concurrently" names an index rather
// than asking for a concurrent build, "select", "order", "user" name
// tables and columns, "join" is an alias.
func TestQuotedKeywordsAsIdentifiers(t *testing.T) {
	ci := parseOne(t, `CREATE INDEX "concurrently" ON t (a)`).(*CreateIndex)
	if ci.Concurrently || ci.Name != "concurrently" {
		t.Errorf(`CREATE INDEX "concurrently": %+v`, ci)
	}
	ci = parseOne(t, `CREATE INDEX CONCURRENTLY "concurrently" ON t (a)`).(*CreateIndex)
	if !ci.Concurrently || ci.Name != "concurrently" {
		t.Errorf(`CREATE INDEX CONCURRENTLY "concurrently": %+v`, ci)
	}
	if ci := parseOne(t, `CREATE INDEX concurrently ON t (a)`).(*CreateIndex); !ci.Concurrently || ci.Name != "" {
		t.Errorf(`CREATE INDEX concurrently ON: %+v`, ci)
	}
	di := parseOne(t, `DROP INDEX "concurrently"`).(*DropIndex)
	if di.Concurrently || di.Name != "concurrently" {
		t.Errorf(`DROP INDEX "concurrently": %+v`, di)
	}

	ct := parseOne(t, `CREATE TABLE "user" ("select" TEXT, "order" INT8, "user" TEXT, "concurrently" INT8, "from" INT8, "with" INT8)`).(*CreateTable)
	if ct.Name != "user" || len(ct.Columns) != 6 || ct.Columns[0].Name != "select" || ct.Columns[1].Name != "order" || ct.Columns[5].Name != "with" {
		t.Errorf("CREATE TABLE: %+v", ct)
	}
	sel := parseOneSelect(t, `SELECT "select", "order", "user", "concurrently", "case", "default", "array" FROM "user" "join" ORDER BY "order"`)
	if sel.Table != "user" || sel.Alias != "join" || len(sel.Exprs) != 7 {
		t.Fatalf("SELECT: table %q alias %q, %d items", sel.Table, sel.Alias, len(sel.Exprs))
	}
	for i, want := range []string{"select", "order", "user", "concurrently", "case", "default", "array"} {
		if e := sel.Exprs[i].Expr; e.Column != want || e.Func != "" || e.Case != nil || e.IsDefault {
			t.Errorf("item %d: %+v, want the column %q", i, e, want)
		}
	}
	if len(sel.OrderBy) != 1 || sel.OrderBy[0].Column != "order" {
		t.Errorf("ORDER BY: %+v", sel.OrderBy)
	}
	// "with" begins no WITH clause; "like" is no operator; "for" no
	// locking clause.
	sel = parseOneSelect(t, `SELECT "like" FROM t "for" WHERE "with" = 1`)
	if sel.Alias != "for" || sel.Exprs[0].Expr.Column != "like" || len(sel.Where) != 1 || sel.Where[0].Column != "with" {
		t.Errorf(`SELECT "like" ...: alias %q, %+v`, sel.Alias, sel.Where)
	}
	ins := parseOne(t, `INSERT INTO "order" ("select", "user") VALUES (1, 2)`).(*Insert)
	if ins.Table != "order" || len(ins.Columns) != 2 || ins.Columns[0] != "select" || ins.Columns[1] != "user" {
		t.Errorf("INSERT: %+v", ins)
	}
	// A quoted word never begins a statement.
	if _, err := Parse(`"select" 1`); err == nil {
		t.Errorf(`"select" 1: no error`)
	}
}

// TestEscapeStrings: the backslash escapes of an E-string (E'...'), as
// PostgreSQL's scanner reads them, and its errors (codes and wording).
func TestEscapeStrings(t *testing.T) {
	for src, want := range map[string]string{
		`E'\b\f\n\r\t'`:   "\b\f\n\r\t",
		`e'a\\b\'c'`:      `a\b'c`,
		`E'it''s'`:        "it's",
		`E'\x41\x4a\x7'`:  "AJ\x07",
		`E'\x414'`:        "A4", // two hex digits at most
		`E'\xZ'`:          "xZ", // \x without a hex digit is x
		`E'\101\60\0101'`: "A0\b1",
		`E'é\U0001F600'`:  "é😀",
		`E'😀'`:            "😀",
		`E'\xC3\xA9'`:     "é",
		`E'\303\251'`:     "é",
		`E'\q\%\é'`:       "q%é",
		`'\x41\n'`:        `\x41\n`, // a standard string has no escapes
	} {
		toks, err := lex(src)
		if err != nil || len(toks) != 2 || toks[0].kind != tkString || toks[0].text != want {
			t.Errorf("%s: %+v %v, want %q", src, toks, err, want)
		}
	}
	for src, w := range map[string]struct{ code, msg string }{
		`E'\0'`:         {"22021", `invalid byte sequence for encoding "UTF8": 0x00`},
		`E'a\000b'`:     {"22021", `invalid byte sequence for encoding "UTF8": 0x00`},
		`E'\x00'`:       {"22021", `invalid byte sequence for encoding "UTF8": 0x00`},
		`E'\x80'`:       {"22021", `invalid byte sequence for encoding "UTF8": 0x80`},
		`E'\777'`:       {"22021", `invalid byte sequence for encoding "UTF8": 0xff`},
		`E'\xC3\x28'`:   {"22021", `invalid byte sequence for encoding "UTF8": 0xc3 0x28`},
		`E'\u12'`:       {"22025", "invalid Unicode escape"},
		`E'\U0001F60'`:  {"22025", "invalid Unicode escape"},
		`E'\u0000'`:     {"42601", "invalid Unicode escape value"},
		`E'\U00110000'`: {"42601", "invalid Unicode escape value"},
		`E'\uD83D'`:     {"42601", "invalid Unicode surrogate pair"},
		`E'\uD83Dx'`:    {"42601", "invalid Unicode surrogate pair"},
		`E'\uD83DA'`:    {"42601", "invalid Unicode surrogate pair"},
		`E'\uDE00'`:     {"42601", "invalid Unicode surrogate pair"},
	} {
		_, err := Parse("SELECT " + src)
		var se *SyntaxError
		if !errors.As(err, &se) || se.Code != w.code || se.Msg != w.msg {
			t.Errorf("%s: %v, want %s %s", src, err, w.code, w.msg)
		}
	}
	_, err := Parse(`SELECT E'\u12'`)
	if se := (*SyntaxError)(nil); errors.As(err, &se) && se.Hint != `Unicode escapes must be \uXXXX or \UXXXXXXXX.` {
		t.Errorf("hint: %q", se.Hint)
	}
}
