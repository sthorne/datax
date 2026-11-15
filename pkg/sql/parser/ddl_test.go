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
	// USING names the access method; an operator class may follow a
	// column.
	ci = parseOne(t, `CREATE INDEX docs_gin ON docs USING GIN (doc)`).(*CreateIndex)
	if ci.Using != "gin" || len(ci.Columns) != 1 || ci.Columns[0] != "doc" || ci.OpClasses != nil || ci.Unique {
		t.Fatalf("create index using gin: %+v", ci)
	}
	ci = parseOne(t, `CREATE INDEX IF NOT EXISTS docs_gin ON app.docs USING gin (doc jsonb_path_ops)`).(*CreateIndex)
	if ci.Using != "gin" || ci.Table != "app.docs" || len(ci.OpClasses) != 1 || ci.OpClasses[0] != "jsonb_path_ops" {
		t.Fatalf("create index with an operator class: %+v", ci)
	}
	ci = parseOne(t, `CREATE INDEX by_ab ON t USING btree (a, b)`).(*CreateIndex)
	if ci.Using != "btree" || len(ci.Columns) != 2 || ci.OpClasses != nil {
		t.Fatalf("create index using btree: %+v", ci)
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

// TestParseSplitAt: ALTER TABLE ... SPLIT AT VALUES (k, ...), ... (issue
// #102's pre-split for benchmarks and bulk loads).
func TestParseSplitAt(t *testing.T) {
	at := parseOne(t, `ALTER TABLE t SPLIT AT VALUES (10), (20, 'x'), (30 + 1)`).(*AlterTable)
	if at.Table != "t" || len(at.SplitAt) != 3 || len(at.SplitAt[0]) != 1 || len(at.SplitAt[1]) != 2 || at.SplitAt[1][1].Lit == nil {
		t.Fatalf("split at: %+v", at)
	}
	if at := parseOne(t, `ALTER TABLE IF EXISTS t SPLIT AT VALUES (1)`).(*AlterTable); !at.IfExists || len(at.SplitAt) != 1 {
		t.Fatalf("split at if exists: %+v", at)
	}
	for _, bad := range []string{`ALTER TABLE t SPLIT VALUES (1)`, `ALTER TABLE t SPLIT AT (1)`, `ALTER TABLE t SPLIT AT VALUES 1`} {
		if _, err := Parse(bad); err == nil {
			t.Fatalf("%s: parsed", bad)
		}
	}
}

// TestParseSplitExpirationAndUnsplit: SPLIT AT ... WITH EXPIRATION e, and
// the way back, UNSPLIT AT VALUES (...) and UNSPLIT ALL (issue #265).
func TestParseSplitExpirationAndUnsplit(t *testing.T) {
	at := parseOne(t, `ALTER TABLE t SPLIT AT VALUES (10), (20) WITH EXPIRATION INTERVAL '1 hour'`).(*AlterTable)
	if len(at.SplitAt) != 2 || at.SplitExpiration == nil || at.UnsplitAt != nil || at.UnsplitAll {
		t.Fatalf("split at with expiration: %+v", at)
	}
	if at := parseOne(t, `ALTER TABLE t SPLIT AT VALUES (10) WITH EXPIRATION $1`).(*AlterTable); at.SplitExpiration == nil {
		t.Fatalf("split at with a parameter for the expiration: %+v", at)
	}
	if at := parseOne(t, `ALTER TABLE t SPLIT AT VALUES (10)`).(*AlterTable); at.SplitExpiration != nil {
		t.Fatalf("split at without an expiration: %+v", at)
	}
	at = parseOne(t, `ALTER TABLE IF EXISTS t UNSPLIT AT VALUES (10), (20, 'x')`).(*AlterTable)
	if !at.IfExists || len(at.UnsplitAt) != 2 || len(at.UnsplitAt[1]) != 2 || at.SplitAt != nil || at.UnsplitAll {
		t.Fatalf("unsplit at: %+v", at)
	}
	if at := parseOne(t, `ALTER TABLE t UNSPLIT ALL`).(*AlterTable); !at.UnsplitAll || at.UnsplitAt != nil || at.SplitAt != nil {
		t.Fatalf("unsplit all: %+v", at)
	}
	for _, bad := range []string{
		`ALTER TABLE t SPLIT AT VALUES (1) WITH EXPIRATION`,
		`ALTER TABLE t SPLIT AT VALUES (1) WITH (expiration = 1)`,
		`ALTER TABLE t UNSPLIT`,
		`ALTER TABLE t UNSPLIT VALUES (1)`,
		`ALTER TABLE t UNSPLIT AT (1)`,
		`ALTER TABLE t UNSPLIT ALL VALUES (1)`,
		`ALTER TABLE t UNSPLIT AT VALUES (1) WITH EXPIRATION '1 hour'`,
	} {
		if _, err := Parse(bad); err == nil {
			t.Fatalf("%s: parsed", bad)
		}
	}
}

// TestParseResetAndPurge: ALTER TABLE ... RESET (name, ...) and ALTER
// TABLE ... PURGE (issues #295, #298), and the option values a row expiry
// takes, which are column names rather than literals.
func TestParseResetAndPurge(t *testing.T) {
	at := parseOne(t, `ALTER TABLE t RESET (gc_ttl, ttl)`).(*AlterTable)
	if len(at.ResetOptions) != 2 || at.ResetOptions[0] != "gc_ttl" || at.ResetOptions[1] != "ttl" || at.Purge {
		t.Fatalf("RESET: %+v", at)
	}
	if at := parseOne(t, `ALTER TABLE IF EXISTS db.t PURGE`).(*AlterTable); !at.Purge || !at.IfExists || at.Table != "db.t" {
		t.Fatalf("PURGE: %+v", at)
	}
	at = parseOne(t, `ALTER TABLE t SET (ttl_column = created_at, ttl = '7 days', gc_ttl = '10m')`).(*AlterTable)
	if at.SetOptions["ttl_column"] != "created_at" || at.SetOptions["ttl"] != "7 days" || at.SetOptions["gc_ttl"] != "10m" {
		t.Fatalf("SET: %+v", at.SetOptions)
	}
	ct := parseOne(t, `CREATE TABLE t (a INT8 PRIMARY KEY, e TIMESTAMPTZ) WITH (ttl_expiration = e)`).(*CreateTable)
	if ct.Options["ttl_expiration"] != "e" {
		t.Fatalf("CREATE ... WITH: %+v", ct.Options)
	}
	for _, bad := range []string{`ALTER TABLE t RESET gc_ttl`, `ALTER TABLE t RESET ()`, `ALTER TABLE t RESET (gc_ttl = '1h')`, `ALTER TABLE t PURGE NOW`} {
		if _, err := Parse(bad); err == nil {
			t.Errorf("%s parsed", bad)
		}
	}
}

// TestParseTextSearch: the @@ operator, and the one expression CREATE
// INDEX accepts as a key.
func TestParseTextSearch(t *testing.T) {
	sel := parseOne(t, `SELECT id FROM t WHERE v @@ to_tsquery('english', 'a & b') AND NOT (v @@ 'c')`).(*Select)
	if len(sel.Where) != 2 || sel.Where[0].Column != "v" || sel.Where[0].Op != "@@" || sel.Where[0].Value.Func != "to_tsquery" || sel.Where[1].Op != "NOT @@" {
		t.Fatalf("WHERE: %+v", sel.Where)
	}
	sel = parseOne(t, `SELECT id FROM t WHERE to_tsvector('english', body) @@ $1`).(*Select)
	if c := sel.Where[0]; c.Op != "@@" || c.Expr == nil || c.Expr.Func != "to_tsvector" || len(c.Expr.Args) != 2 || c.Expr.Args[1].Column != "body" || c.Value.Param != 1 {
		t.Fatalf("WHERE: %+v", sel.Where[0])
	}
	for src, want := range map[string]CreateIndex{
		`CREATE INDEX i ON t USING GIN (to_tsvector('english', body))`:             {Name: "i", Table: "t", Using: "gin", Columns: []string{"body"}, ExprFunc: "to_tsvector", ExprArg: "english"},
		`CREATE INDEX i ON t USING gin ((TO_TSVECTOR('simple'::regconfig, body)))`: {Name: "i", Table: "t", Using: "gin", Columns: []string{"body"}, ExprFunc: "to_tsvector", ExprArg: "simple"},
		`CREATE INDEX i ON t (lower('x', body))`:                                   {Name: "i", Table: "t", Columns: []string{"body"}, ExprFunc: "lower", ExprArg: "x"},
		`CREATE INDEX i ON t USING GIN (v tsvector_ops)`:                           {Name: "i", Table: "t", Using: "gin", Columns: []string{"v"}, OpClasses: []string{"tsvector_ops"}},
	} {
		got := parseOne(t, src).(*CreateIndex)
		if got.Name != want.Name || got.Table != want.Table || got.Using != want.Using || got.ExprFunc != want.ExprFunc || got.ExprArg != want.ExprArg ||
			len(got.Columns) != 1 || got.Columns[0] != want.Columns[0] || len(got.OpClasses) != len(want.OpClasses) {
			t.Errorf("%s: %+v", src, got)
		}
	}
	for _, bad := range []string{
		`CREATE INDEX i ON t USING GIN (to_tsvector(body))`,
		`CREATE INDEX i ON t USING GIN (to_tsvector('english', body), other)`,
		`CREATE INDEX i ON t USING GIN (other, to_tsvector('english', body))`,
		`CREATE INDEX i ON t USING GIN (to_tsvector('english', lower(body)))`,
		`CREATE INDEX i ON t USING GIN (to_tsvector('english', body)`,
		`CREATE INDEX i ON t USING GIN ((body))`,
		`CREATE INDEX i ON t USING GIN (to_tsvector('english', body) jsonb_ops)`,
		`CREATE TABLE t (id INT8 PRIMARY KEY, v TSVECTOR[])`,
		`CREATE TABLE t (id INT8 PRIMARY KEY, q TSQUERY ARRAY)`,
	} {
		if _, err := Parse(bad); err == nil {
			t.Errorf("%s parsed", bad)
		}
	}
}
