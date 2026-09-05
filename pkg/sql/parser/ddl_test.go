package parser

import (
	"strings"
	"testing"
)

// TestParseDDLCompleteness: DROP INDEX, ALTER INDEX RENAME, the ALTER
// TABLE RENAME forms, ALTER COLUMN SET / DROP DEFAULT, TRUNCATE, and IF
// [NOT] EXISTS on every CREATE / DROP / ALTER (#95).
func TestParseDDLCompleteness(t *testing.T) {
	di := parseOne(t, `DROP INDEX IF EXISTS by_city CASCADE`).(*DropIndex)
	if di.Name != "by_city" || !di.IfExists {
		t.Fatalf("drop index: %+v", di)
	}
	if di := parseOne(t, `DROP INDEX app.by_city`).(*DropIndex); di.Name != "app.by_city" || di.IfExists {
		t.Fatalf("qualified drop index: %+v", di)
	}
	ai := parseOne(t, `ALTER INDEX IF EXISTS by_city RENAME TO by_town`).(*AlterIndex)
	if ai.Name != "by_city" || ai.NewName != "by_town" || !ai.IfExists {
		t.Fatalf("alter index: %+v", ai)
	}
	if _, err := Parse(`ALTER INDEX by_city SET (fillfactor = 70)`); err == nil || !strings.Contains(err.Error(), "RENAME TO") {
		t.Fatalf("alter index set: %v", err)
	}

	at := parseOne(t, `ALTER TABLE IF EXISTS users RENAME TO people`).(*AlterTable)
	if at.Table != "users" || !at.IfExists || at.RenameTo != "people" {
		t.Fatalf("rename table: %+v", at)
	}
	at = parseOne(t, `ALTER TABLE users RENAME COLUMN email TO mail`).(*AlterTable)
	if at.RenameCol == nil || at.RenameCol.From != "email" || at.RenameCol.To != "mail" {
		t.Fatalf("rename column: %+v", at)
	}
	at = parseOne(t, `ALTER TABLE users RENAME email TO mail`).(*AlterTable)
	if at.RenameCol == nil || at.RenameCol.From != "email" {
		t.Fatalf("rename bare column: %+v", at)
	}
	at = parseOne(t, `ALTER TABLE users RENAME CONSTRAINT users_age_check TO age_ok`).(*AlterTable)
	if at.RenameConstraint == nil || at.RenameConstraint.From != "users_age_check" || at.RenameConstraint.To != "age_ok" {
		t.Fatalf("rename constraint: %+v", at)
	}

	at = parseOne(t, `ALTER TABLE users ALTER COLUMN city SET DEFAULT 'oslo'`).(*AlterTable)
	if at.SetDefault == nil || at.SetDefault.Column != "city" || at.SetDefault.Default == nil || at.SetDefault.Default.S != "oslo" || at.SetDefault.Expr != nil {
		t.Fatalf("set constant default: %+v", at.SetDefault)
	}
	at = parseOne(t, `ALTER TABLE users ALTER city SET DEFAULT lower('X') || now()::text`).(*AlterTable)
	if at.SetDefault == nil || at.SetDefault.Expr == nil || at.SetDefault.Default != nil {
		t.Fatalf("set expression default: %+v", at.SetDefault)
	}
	if _, err := Parse(`ALTER TABLE users ALTER city SET DEFAULT age + 1`); err == nil || !strings.Contains(err.Error(), "reference columns") {
		t.Fatalf("column in default: %v", err)
	}
	at = parseOne(t, `ALTER TABLE users ALTER COLUMN city DROP DEFAULT`).(*AlterTable)
	if at.DropDefault != "city" || at.SetDefault != nil {
		t.Fatalf("drop default: %+v", at)
	}
	at = parseOne(t, `ALTER TABLE users ALTER COLUMN city SET NOT NULL`).(*AlterTable)
	if at.SetNotNull != "city" {
		t.Fatalf("set not null still parses: %+v", at)
	}
	if _, err := Parse(`ALTER TABLE users ALTER COLUMN city SET TYPE TEXT`); err == nil || !strings.Contains(err.Error(), "SET DEFAULT or SET NOT NULL") {
		t.Fatalf("unknown ALTER COLUMN SET: %v", err)
	}

	at = parseOne(t, `ALTER TABLE users ADD COLUMN IF NOT EXISTS note TEXT`).(*AlterTable)
	if at.AddCol == nil || at.AddCol.Name != "note" || !at.AddColIfNotExists {
		t.Fatalf("add column if not exists: %+v", at)
	}
	at = parseOne(t, `ALTER TABLE users DROP COLUMN IF EXISTS note RESTRICT`).(*AlterTable)
	if at.DropCol != "note" || !at.DropColIfExists {
		t.Fatalf("drop column if exists: %+v", at)
	}
	ci := parseOne(t, `CREATE UNIQUE INDEX IF NOT EXISTS by_email ON users (email)`).(*CreateIndex)
	if !ci.Unique || !ci.IfNotExists || ci.Name != "by_email" || ci.Table != "users" {
		t.Fatalf("create index if not exists: %+v", ci)
	}
	cu := parseOne(t, `CREATE USER IF NOT EXISTS ann PASSWORD 'pw'`).(*CreateRole)
	if cu.Name != "ann" || !cu.IfNotExists || cu.Alter || !cu.IsUser || cu.Password == nil || *cu.Password != "pw" {
		t.Fatalf("create user if not exists: %+v", cu)
	}
	du := parseOne(t, `DROP USER IF EXISTS ann`).(*DropRole)
	if len(du.Names) != 1 || du.Names[0] != "ann" || !du.IfExists {
		t.Fatalf("drop user if exists: %+v", du)
	}
	as := parseOne(t, `ALTER SEQUENCE IF EXISTS s RESTART WITH 10`).(*AlterSequence)
	if as.Name != "s" || !as.IfExists || !as.Options.RestartSet || *as.Options.Restart != 10 {
		t.Fatalf("alter sequence if exists: %+v", as)
	}

	tr := parseOne(t, `TRUNCATE TABLE a, app.b RESTART IDENTITY CASCADE`).(*Truncate)
	if len(tr.Tables) != 2 || tr.Tables[0] != "a" || tr.Tables[1] != "app.b" || !tr.RestartIdentity || !tr.Cascade {
		t.Fatalf("truncate: %+v", tr)
	}
	tr = parseOne(t, `TRUNCATE a CONTINUE IDENTITY RESTRICT`).(*Truncate)
	if len(tr.Tables) != 1 || tr.RestartIdentity || tr.Cascade {
		t.Fatalf("truncate defaults: %+v", tr)
	}
	if _, err := Parse(`TRUNCATE a RESTART`); err == nil || !strings.Contains(err.Error(), "IDENTITY") {
		t.Fatalf("truncate restart: %v", err)
	}
}

// TestRenameColumnRefs: the stored CHECK text follows a renamed column,
// leaving calls, strings, other identifiers and quoting alone.
func TestRenameColumnRefs(t *testing.T) {
	cases := []struct{ in, old, new, want string }{
		{`qty > 0 AND qty < 100`, "qty", "amount", `amount > 0 AND amount < 100`},
		{`qty > 0`, "q", "amount", `qty > 0`},
		{`lower(qty) <> 'qty' AND t.qty IS NOT NULL`, "qty", "n", `lower(n) <> 'qty' AND t.n IS NOT NULL`},
		{`"Qty" > 0`, "Qty", "count", `count > 0`},
		{`qty > 0`, "qty", "Big Name", `"Big Name" > 0`},
		{`qty(1) > qty`, "qty", "x", `qty(1) > x`},
	}
	for _, c := range cases {
		got, err := RenameColumnRefs(c.in, c.old, c.new)
		if err != nil {
			t.Fatalf("%q: %v", c.in, err)
		}
		if got != c.want {
			t.Fatalf("%q: got %q, want %q", c.in, got, c.want)
		}
	}
}
