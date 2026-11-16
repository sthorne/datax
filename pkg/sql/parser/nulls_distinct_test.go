package parser

import "testing"

// TestParseUniqueNullsDistinct (issue #322): NULLS [NOT] DISTINCT after
// UNIQUE in a column or table constraint, and after the column list of
// CREATE UNIQUE INDEX.
func TestParseUniqueNullsDistinct(t *testing.T) {
	ct := parseOne(t, `CREATE TABLE t (a INT PRIMARY KEY, b TEXT UNIQUE NULLS NOT DISTINCT, c TEXT UNIQUE NULLS DISTINCT, d TEXT UNIQUE,
		CONSTRAINT cd UNIQUE NULLS NOT DISTINCT (c, d), UNIQUE NULLS DISTINCT (b, d))`).(*CreateTable)
	if got := ct.Columns[1].Constraints; len(got) != 1 || !got[0].NullsNotDistinct {
		t.Fatalf("b: %+v", got)
	}
	for _, i := range []int{2, 3} {
		if got := ct.Columns[i].Constraints; len(got) != 1 || got[0].NullsNotDistinct {
			t.Fatalf("column %d: %+v", i, got)
		}
	}
	if len(ct.Constraints) != 2 || !ct.Constraints[0].NullsNotDistinct || ct.Constraints[0].Name != "cd" || ct.Constraints[1].NullsNotDistinct {
		t.Fatalf("table constraints: %+v", ct.Constraints)
	}
	ci := parseOne(t, `CREATE UNIQUE INDEX i ON t (b) NULLS NOT DISTINCT`).(*CreateIndex)
	if !ci.Unique || !ci.NullsNotDistinct || !ci.NullsClause {
		t.Fatalf("create index: %+v", ci)
	}
	ci = parseOne(t, `CREATE UNIQUE INDEX i ON t USING btree (b, c) NULLS DISTINCT`).(*CreateIndex)
	if ci.NullsNotDistinct || !ci.NullsClause || len(ci.Columns) != 2 {
		t.Fatalf("create index: %+v", ci)
	}
	at := parseOne(t, `ALTER TABLE t ADD CONSTRAINT k UNIQUE NULLS NOT DISTINCT (b)`).(*AlterTable)
	if at.AddConstraint == nil || !at.AddConstraint.NullsNotDistinct {
		t.Fatalf("add constraint: %+v", at)
	}
	for _, bad := range []string{
		`CREATE UNIQUE INDEX i ON t (b) NULLS NOT`,
		`CREATE TABLE t (a INT PRIMARY KEY, b TEXT UNIQUE NULLS FIRST)`,
	} {
		if _, err := Parse(bad); err == nil {
			t.Fatalf("%q parsed", bad)
		}
	}
}

// TestParseAddConstraintIfNotExists (issue #322): the datax extension
// ADD CONSTRAINT IF NOT EXISTS name.
func TestParseAddConstraintIfNotExists(t *testing.T) {
	at := parseOne(t, `ALTER TABLE t ADD CONSTRAINT IF NOT EXISTS k CHECK (a > 0)`).(*AlterTable)
	if !at.AddConstraintIfNotExists || at.AddConstraint == nil || at.AddConstraint.Name != "k" || at.AddConstraint.Kind != "check" {
		t.Fatalf("%+v %+v", at, at.AddConstraint)
	}
	at = parseOne(t, `ALTER TABLE t ADD CONSTRAINT k UNIQUE (a)`).(*AlterTable)
	if at.AddConstraintIfNotExists {
		t.Fatalf("%+v", at)
	}
	for _, bad := range []string{
		`ALTER TABLE t ADD CONSTRAINT IF EXISTS k CHECK (a > 0)`,
		`ALTER TABLE t ADD CONSTRAINT IF NOT EXISTS CHECK (a > 0)`,
	} {
		if _, err := Parse(bad); err == nil {
			t.Fatalf("%q parsed", bad)
		}
	}
}
