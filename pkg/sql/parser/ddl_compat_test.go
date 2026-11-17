package parser

import (
	"reflect"
	"strings"
	"testing"
)

// TestParseIndexCompat: CONCURRENTLY, a left-out name, COLLATE and
// qualified operator classes in CREATE INDEX; DROP INDEX CONCURRENTLY;
// DROP EXTENSION (issues #347, #348, #349).
func TestParseIndexCompat(t *testing.T) {
	ci := parseOne(t, `CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS i ON t (a)`).(*CreateIndex)
	if !ci.Unique || !ci.Concurrently || !ci.IfNotExists || ci.Name != "i" || ci.Table != "t" {
		t.Fatalf("parsed as %+v", ci)
	}
	ci = parseOne(t, `CREATE INDEX CONCURRENTLY ON t (a, b)`).(*CreateIndex)
	if !ci.Concurrently || ci.Name != "" || !reflect.DeepEqual(ci.Columns, []string{"a", "b"}) {
		t.Fatalf("unnamed parsed as %+v", ci)
	}
	if ci = parseOne(t, `CREATE INDEX ON t (a)`).(*CreateIndex); ci.Name != "" || ci.Concurrently {
		t.Fatalf("unnamed parsed as %+v", ci)
	}
	ci = parseOne(t, `CREATE INDEX "x_like" ON "t" ("col" varchar_pattern_ops);`).(*CreateIndex)
	if ci.Name != "x_like" || !reflect.DeepEqual(ci.OpClasses, []string{"varchar_pattern_ops"}) || ci.Collations != nil {
		t.Fatalf("Django's index parsed as %+v", ci)
	}
	ci = parseOne(t, `CREATE INDEX i ON t (a COLLATE "C" pg_catalog.text_pattern_ops, b, c COLLATE pg_catalog."POSIX")`).(*CreateIndex)
	if !reflect.DeepEqual(ci.Collations, []string{"C", "", "POSIX"}) || !reflect.DeepEqual(ci.OpClasses, []string{"text_pattern_ops", "", ""}) {
		t.Fatalf("collations parsed as %+v", ci)
	}
	for _, bad := range []string{
		`CREATE INDEX IF NOT EXISTS ON t (a)`,
		`CREATE INDEX i ON t (a COLLATE)`,
	} {
		if _, err := Parse(bad); err == nil {
			t.Errorf("%s parsed", bad)
		}
	}

	di := parseOne(t, `DROP INDEX CONCURRENTLY IF EXISTS i CASCADE`).(*DropIndex)
	if !di.Concurrently || !di.IfExists || !di.Cascade || di.Name != "i" {
		t.Fatalf("DROP INDEX parsed as %+v", di)
	}
	if di = parseOne(t, `DROP INDEX i RESTRICT`).(*DropIndex); di.Concurrently || di.Cascade {
		t.Fatalf("DROP INDEX parsed as %+v", di)
	}

	de := parseOne(t, `DROP EXTENSION IF EXISTS "uuid-ossp", pgcrypto CASCADE`).(*DropExtension)
	if !de.IfExists || !reflect.DeepEqual(de.Names, []string{"uuid-ossp", "pgcrypto"}) {
		t.Fatalf("DROP EXTENSION parsed as %+v", de)
	}
	ce := parseOne(t, `CREATE EXTENSION IF NOT EXISTS "uuid-ossp" WITH SCHEMA public VERSION '1.1' CASCADE`).(*CreateExtension)
	if !ce.IfNotExists || ce.Name != "uuid-ossp" {
		t.Fatalf("CREATE EXTENSION parsed as %+v", ce)
	}
}

// TestParseIndexForms: expression keys, ASC/DESC and NULLS FIRST/LAST
// per key, and a partial index's WHERE (issues #358-#360).
func TestParseIndexForms(t *testing.T) {
	ci := parseOne(t, `CREATE UNIQUE INDEX ON users ((lower(email)) DESC NULLS LAST, a, (b + 1) text_pattern_ops, lower(c) NULLS FIRST) WHERE deleted_at IS NULL AND status = 'x'`).(*CreateIndex)
	if len(ci.Columns) != 4 || ci.Columns[0] != "" || ci.Columns[1] != "a" || ci.Columns[2] != "" || ci.Columns[3] != "" {
		t.Fatalf("columns: %q", ci.Columns)
	}
	if len(ci.Exprs) != 4 || FormatExpr(*ci.Exprs[0]) != "lower(email)" || ci.Exprs[1] != nil || FormatExpr(*ci.Exprs[2]) != "b + 1" || FormatExpr(*ci.Exprs[3]) != "lower(c)" {
		t.Fatalf("exprs: %+v", ci.Exprs)
	}
	if want := []string{"DESC NULLS LAST", "", "", "NULLS FIRST"}; strings.Join(ci.Directions, "|") != strings.Join(want, "|") {
		t.Fatalf("directions: %q", ci.Directions)
	}
	if ci.OpClasses[2] != "text_pattern_ops" {
		t.Fatalf("opclasses: %q", ci.OpClasses)
	}
	if ci.Where != "deleted_at IS NULL AND status = 'x'" || len(ci.WhereConds) != 2 {
		t.Fatalf("where: %q %+v", ci.Where, ci.WhereConds)
	}
	for src, want := range map[string]string{
		`CREATE INDEX i ON t (a ASC)`:              "",
		`CREATE INDEX i ON t (a DESC)`:             "DESC",
		`CREATE INDEX i ON t (a DESC NULLS FIRST)`: "DESC",
		`CREATE INDEX i ON t (a ASC NULLS LAST)`:   "",
		`CREATE INDEX i ON t (a NULLS FIRST)`:      "NULLS FIRST",
	} {
		got := parseOne(t, src).(*CreateIndex)
		if d := strings.Join(got.Directions, ""); d != want || got.Exprs != nil {
			t.Errorf("%s: directions %q exprs %v", src, got.Directions, got.Exprs)
		}
	}
	// A column in parentheses is the column; a statement after the
	// predicate is not part of it.
	if ci := parseOne(t, `CREATE INDEX i ON t ((a)) WHERE b > 1;`).(*CreateIndex); ci.Columns[0] != "a" || ci.Exprs != nil || ci.Where != "b > 1" {
		t.Fatalf("(a): %+v", ci)
	}
	for _, bad := range []string{
		`CREATE INDEX i ON t (a) WHERE`,
		`CREATE INDEX i ON t (a DESC DESC)`,
	} {
		if _, err := Parse(bad); err == nil {
			t.Errorf("%s parsed", bad)
		}
	}
}
