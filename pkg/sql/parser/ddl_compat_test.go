package parser

import (
	"reflect"
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
