package parser

import (
	"fmt"
	"strings"
	"testing"

	"github.com/sthorne/datax/pkg/sql/types"
)

func parseOne(t *testing.T, src string) Statement {
	t.Helper()
	stmts, err := Parse(src)
	if err != nil {
		t.Fatalf("parse %q: %v", src, err)
	}
	if len(stmts) != 1 {
		t.Fatalf("parse %q: %d statements", src, len(stmts))
	}
	return stmts[0]
}

func TestParseCreateTable(t *testing.T) {
	ct := parseOne(t, `CREATE TABLE accounts (
		id INT8 PRIMARY KEY,
		balance INT8 NOT NULL,
		name TEXT,
		rate DOUBLE PRECISION,
		active BOOL
	)`).(*CreateTable)
	if ct.Name != "accounts" || len(ct.Columns) != 5 {
		t.Fatalf("%+v", ct)
	}
	if !ct.Columns[0].PrimaryKey || !ct.Columns[0].NotNull {
		t.Fatal("column-level PRIMARY KEY not parsed")
	}
	if ct.Columns[1].Type != types.Int || !ct.Columns[1].NotNull {
		t.Fatal("balance INT8 NOT NULL not parsed")
	}
	if ct.Columns[3].Type != types.Float {
		t.Fatal("DOUBLE PRECISION not parsed")
	}

	ct2 := parseOne(t, `CREATE TABLE IF NOT EXISTS t (a INT, b TEXT, PRIMARY KEY (a, b))`).(*CreateTable)
	if !ct2.IfNotExists || len(ct2.PrimaryKey) != 2 {
		t.Fatalf("%+v", ct2)
	}
}

func TestParseInsertSelect(t *testing.T) {
	ins := parseOne(t, `INSERT INTO t (a, b) VALUES (1, 'x''y'), (-2, NULL)`).(*Insert)
	if len(ins.Rows) != 2 || len(ins.Columns) != 2 {
		t.Fatalf("%+v", ins)
	}
	if ins.Rows[0][1].Lit.S != "x'y" {
		t.Fatalf("string escape: %+v", ins.Rows[0][1].Lit)
	}
	if ins.Rows[1][0].Lit.I != -2 {
		t.Fatalf("negative literal: %+v", ins.Rows[1][0].Lit)
	}
	if !ins.Rows[1][1].Lit.Null {
		t.Fatal("NULL literal")
	}

	sel := parseOne(t, `SELECT id, balance FROM accounts WHERE id = $1 AND balance >= 10 LIMIT 5`).(*Select)
	if sel.Table != "accounts" || len(sel.Exprs) != 2 || len(sel.Where) != 2 || sel.Limit != 5 {
		t.Fatalf("%+v", sel)
	}
	if sel.Where[0].Value.Param != 1 || sel.Where[1].Op != ">=" {
		t.Fatalf("%+v", sel.Where)
	}

	star := parseOne(t, `SELECT * FROM t`).(*Select)
	if !star.Exprs[0].Star {
		t.Fatal("star")
	}

	bare := parseOne(t, `SELECT 1`).(*Select)
	if bare.Table != "" || bare.Exprs[0].Expr.Lit.I != 1 {
		t.Fatalf("%+v", bare)
	}
}

func TestParseUpdateDelete(t *testing.T) {
	up := parseOne(t, `UPDATE accounts SET balance = balance - 10, name = 'x' WHERE id = 1`).(*Update)
	if len(up.Set) != 2 || up.Set[0].Value.BinOp != "-" || up.Set[0].Value.Column != "balance" {
		t.Fatalf("%+v", up.Set)
	}
	del := parseOne(t, `DELETE FROM t WHERE a != TRUE`).(*Delete)
	if del.Table != "t" || del.Where[0].Op != "!=" || del.Where[0].Value.Lit.B != true {
		t.Fatalf("%+v", del)
	}
}

func TestParseTxnAndMulti(t *testing.T) {
	stmts, err := Parse(`BEGIN; UPDATE t SET a = 1; COMMIT;`)
	if err != nil {
		t.Fatal(err)
	}
	if len(stmts) != 3 {
		t.Fatalf("%d statements", len(stmts))
	}
	if _, ok := stmts[0].(*Begin); !ok {
		t.Fatal("BEGIN")
	}
	if _, ok := stmts[2].(*Commit); !ok {
		t.Fatal("COMMIT")
	}
	if _, ok := parseOne(t, "START TRANSACTION").(*Begin); !ok {
		t.Fatal("START TRANSACTION")
	}
	if _, ok := parseOne(t, "ABORT").(*Rollback); !ok {
		t.Fatal("ABORT")
	}
	if _, ok := parseOne(t, "SHOW TABLES").(*ShowTables); !ok {
		t.Fatal("SHOW TABLES")
	}
	if _, ok := parseOne(t, "SET client_encoding = 'UTF8'").(*SetVar); !ok {
		t.Fatal("SET")
	}
}

func TestParseErrors(t *testing.T) {
	for _, bad := range []string{
		"CREATE TABLE",
		"SELECT FROM t",
		"INSERT INTO t VALUES",
		"UPDATE t SET",
		"SELECT * FROM t WHERE a LIKE",
		"SELECT * FROM t; garbage",
	} {
		if _, err := Parse(bad); err == nil {
			t.Errorf("no error for %q", bad)
		}
	}
}

// TestJoinChains: JOIN clauses chain left-deep into Joins, qualified
// GROUP BY and aggregate arguments parse, and the table cap holds.
func TestJoinChains(t *testing.T) {
	sel := parseOne(t, `SELECT r.name, SUM(o.total) FROM regions r
		JOIN customers c ON c.region_id = r.id
		LEFT JOIN orders o ON o.customer_id = c.id AND o.id = r.id
		GROUP BY r.name`).(*Select)
	if len(sel.Joins) != 2 || sel.Joins[0].Left || !sel.Joins[1].Left {
		t.Fatalf("joins: %+v", sel.Joins)
	}
	if len(sel.Joins[1].On) != 2 {
		t.Fatalf("second join ON: %+v", sel.Joins[1].On)
	}
	if len(sel.GroupBy) != 1 || sel.GroupBy[0] != "r.name" {
		t.Fatalf("qualified GROUP BY: %+v", sel.GroupBy)
	}
	if sel.Exprs[1].AggCol != "o.total" {
		t.Fatalf("qualified aggregate arg: %+v", sel.Exprs[1])
	}

	long := `SELECT 1 FROM t0`
	for i := 1; i < 9; i++ {
		long += fmt.Sprintf(" JOIN t%d ON t0.a = t%d.a", i, i)
	}
	if _, err := Parse(long); err == nil || !strings.Contains(err.Error(), "too many joined tables") {
		t.Fatalf("cap: %v", err)
	}
}

// TestJSONBPathParsing: ->/->> chains attach to column references in the
// SELECT list and WHERE conjuncts; ->> is terminal (it yields text).
func TestJSONBPathParsing(t *testing.T) {
	sel := parseOne(t, `SELECT j -> 'a', j -> 'a' ->> 'b' AS x FROM t WHERE j ->> 'k' = 'v' AND j -> 'n' IS NULL`).(*Select)
	if len(sel.Exprs) != 2 {
		t.Fatalf("exprs: %+v", sel.Exprs)
	}
	p0 := sel.Exprs[0].Expr.Path
	if sel.Exprs[0].Expr.Column != "j" || len(p0) != 1 || !stepIs(p0[0], "a", false) {
		t.Fatalf("expr 0: %+v", sel.Exprs[0].Expr)
	}
	p1 := sel.Exprs[1].Expr.Path
	if len(p1) != 2 || !stepIs(p1[0], "a", false) || !stepIs(p1[1], "b", true) {
		t.Fatalf("expr 1: %+v", sel.Exprs[1].Expr)
	}
	if sel.Exprs[1].Alias != "x" {
		t.Fatalf("alias: %+v", sel.Exprs[1])
	}
	if len(sel.Where) != 2 {
		t.Fatalf("where: %+v", sel.Where)
	}
	w0 := sel.Where[0]
	if w0.Column != "j" || len(w0.Path) != 1 || !w0.Path[0].Text || w0.Path[0].Key != "k" || w0.Op != "=" {
		t.Fatalf("where 0: %+v", w0)
	}
	w1 := sel.Where[1]
	if w1.Op != "IS NULL" || len(w1.Path) != 1 || !stepIs(w1.Path[0], "n", false) {
		t.Fatalf("where 1: %+v", w1)
	}

	for _, bad := range []string{
		`SELECT j ->> 'a' -> 'b' FROM t`, // ->> yields text: not chainable
		`SELECT j -> col FROM t`,         // keys are string literals or array positions
	} {
		if _, err := Parse(bad); err == nil {
			t.Fatalf("accepted %q", bad)
		}
	}
}

// TestDecimalLiterals: non-integer numeric literals parse as exact DECIMAL
// datums (PostgreSQL semantics), not binary floats.
func TestDecimalLiterals(t *testing.T) {
	sel := parseOne(t, `SELECT 0.1, -1.5e2, 2.50 FROM t`).(*Select)
	want := []string{"0.1", "-150", "2.5"}
	for i, w := range want {
		lit := sel.Exprs[i].Expr.Lit
		if lit == nil || lit.Fam != types.Decimal || lit.S != w {
			t.Fatalf("expr %d: %+v, want canonical %q", i, lit, w)
		}
	}
	ins := parseOne(t, `INSERT INTO t VALUES (0.30000000000000004)`).(*Insert)
	if lit := ins.Rows[0][0].Lit; lit.S != "0.30000000000000004" {
		t.Fatalf("precision lost: %+v", lit)
	}
}

// TestBooleanWhere: OR/AND/NOT with parentheses lower to the executor's
// conjunction form — De Morgan eliminates NOT, single-column equality ORs
// rewrite to IN, and subqueries inside OR are rejected.
func TestBooleanWhere(t *testing.T) {
	// AND of a leaf and an OR group.
	sel := parseOne(t, `SELECT id FROM t WHERE a = 1 AND (b = 2 OR c > 3)`).(*Select)
	if len(sel.Where) != 2 || sel.Where[0].Column != "a" {
		t.Fatalf("where: %+v", sel.Where)
	}
	or := sel.Where[1]
	if or.Op != "OR" || len(or.Or) != 2 || or.Or[0][0].Column != "b" || or.Or[1][0].Op != ">" {
		t.Fatalf("or: %+v", or)
	}

	// Precedence: AND binds tighter than OR.
	sel = parseOne(t, `SELECT id FROM t WHERE a = 1 OR b = 2 AND c = 3`).(*Select)
	if len(sel.Where) != 1 || sel.Where[0].Op != "OR" {
		t.Fatalf("where: %+v", sel.Where)
	}
	if d := sel.Where[0].Or; len(d) != 2 || len(d[0]) != 1 || len(d[1]) != 2 {
		t.Fatalf("or shape: %+v", d)
	}

	// NOT elimination: De Morgan + operator negation.
	sel = parseOne(t, `SELECT id FROM t WHERE NOT (a = 1 AND b IS NULL)`).(*Select)
	or = sel.Where[0]
	if or.Op != "OR" || or.Or[0][0].Op != "!=" || or.Or[1][0].Op != "IS NOT NULL" {
		t.Fatalf("negated: %+v", or)
	}
	sel = parseOne(t, `SELECT id FROM t WHERE NOT a < 5`).(*Select)
	if sel.Where[0].Op != ">=" {
		t.Fatalf("negated leaf: %+v", sel.Where[0])
	}

	// Single-column equality OR rewrites to IN.
	sel = parseOne(t, `SELECT id FROM t WHERE a = 1 OR a = 2 OR a = $1`).(*Select)
	w := sel.Where[0]
	if w.Op != "IN" || w.Column != "a" || len(w.Values) != 3 || w.Values[2].Param != 1 {
		t.Fatalf("IN rewrite: %+v", w)
	}
	if CountParams(sel) != 1 {
		t.Fatalf("params through IN rewrite: %d", CountParams(sel))
	}

	// Mixed columns stay an OR; subqueries inside OR parse (the executor
	// refuses them, unless the query never runs) and are reported.
	sel = parseOne(t, `SELECT id FROM t WHERE a = 1 OR b = 2`).(*Select)
	if sel.Where[0].Op != "OR" {
		t.Fatalf("mixed: %+v", sel.Where[0])
	}
	if HasSubInOr(sel.Where) {
		t.Fatal("plain OR reported as holding a subquery")
	}
	sel = parseOne(t, `SELECT id FROM t WHERE a = 1 OR b IN (SELECT x FROM u)`).(*Select)
	if !HasSubInOr(sel.Where) {
		t.Fatal("subquery inside OR not reported")
	}
	// NOT IN and NOT EXISTS still parse as leaves.
	sel = parseOne(t, `SELECT id FROM t WHERE a NOT IN (1, 2) AND NOT EXISTS (SELECT 1 FROM u)`).(*Select)
	if sel.Where[0].Op != "NOT IN" || sel.Where[1].Op != "NOT EXISTS" {
		t.Fatalf("legacy NOTs: %+v", sel.Where)
	}
}

// TestArithmeticAndFunctions: precedence-correct * and /, parenthesized
// grouping, and builtin calls.
func TestArithmeticAndFunctions(t *testing.T) {
	sel := parseOne(t, `SELECT a + b * 2, (a + b) * 2, lower(name), coalesce(x, y, 0), now() FROM t`).(*Select)
	e := sel.Exprs[0].Expr // a + (b * 2): flat node a, +, Right = {b * 2}
	if e.Column != "a" || e.BinOp != "+" || e.Right.Column != "b" || e.Right.BinOp != "*" {
		t.Fatalf("precedence: %+v", e)
	}
	e = sel.Exprs[1].Expr // (a + b) * 2: Left = {a + b}, *, 2
	if e.Left == nil || e.Left.BinOp != "+" || e.BinOp != "*" || e.Right.Lit == nil {
		t.Fatalf("grouping: %+v", e)
	}
	if f := sel.Exprs[2].Expr; f.Func != "lower" || len(f.Args) != 1 || f.Args[0].Column != "name" {
		t.Fatalf("lower: %+v", f)
	}
	if f := sel.Exprs[3].Expr; f.Func != "coalesce" || len(f.Args) != 3 {
		t.Fatalf("coalesce: %+v", f)
	}
	if f := sel.Exprs[4].Expr; f.Func != "now" || len(f.Args) != 0 {
		t.Fatalf("now: %+v", f)
	}
	// Left associativity: a - b - c = (a - b) - c.
	sel = parseOne(t, `SELECT a - b - c FROM t`).(*Select)
	e = sel.Exprs[0].Expr
	if e.Left == nil || e.Left.Column != "a" || e.Left.BinOp != "-" || e.Right.Column != "c" {
		t.Fatalf("associativity: %+v", e)
	}
	for _, bad := range []string{
		`SELECT length() FROM t`,
		`SELECT now(1) FROM t`,
		`SELECT coalesce() FROM t`,
	} {
		if _, err := Parse(bad); err == nil {
			t.Fatalf("accepted %q", bad)
		}
	}
}

func TestCopyFrom(t *testing.T) {
	parse1 := func(t *testing.T, src string) *CopyFrom {
		t.Helper()
		stmts, err := Parse(src)
		if err != nil {
			t.Fatalf("%s: %v", src, err)
		}
		if len(stmts) != 1 {
			t.Fatalf("%s: %d statements", src, len(stmts))
		}
		cf, ok := stmts[0].(*CopyFrom)
		if !ok {
			t.Fatalf("%s: got %T", src, stmts[0])
		}
		return cf
	}

	cf := parse1(t, "COPY t FROM STDIN")
	if cf.Table != "t" || len(cf.Columns) != 0 || cf.Format != CopyFormatText {
		t.Fatalf("plain: %+v", cf)
	}
	cf = parse1(t, "COPY t (a, b) FROM STDIN WITH (FORMAT csv)")
	if cf.Table != "t" || len(cf.Columns) != 2 || cf.Columns[1] != "b" || cf.Format != CopyFormatCSV {
		t.Fatalf("with csv: %+v", cf)
	}
	cf = parse1(t, "copy t from stdin (format text)")
	if cf.Format != CopyFormatText {
		t.Fatalf("bare option list: %+v", cf)
	}
	cf = parse1(t, "COPY t FROM STDIN WITH (FORMAT 'binary')")
	if cf.Format != CopyFormatBinary {
		t.Fatalf("quoted format: %+v", cf)
	}
	// pgx's exact spelling.
	cf = parse1(t, `copy "t" ( "a", "b" ) from stdin binary;`)
	if cf.Table != "t" || len(cf.Columns) != 2 || cf.Format != CopyFormatBinary {
		t.Fatalf("pgx spelling: %+v", cf)
	}

	for _, bad := range []string{
		"COPY t TO STDOUT",
		"COPY t FROM '/tmp/f'",
		"COPY t FROM STDIN WITH (DELIMITER ',')",
		"COPY t FROM STDIN WITH (FORMAT nope)",
		"COPY t FROM STDIN WITH",
	} {
		if _, err := Parse(bad); err == nil {
			t.Fatalf("%q parsed", bad)
		}
	}
}

func TestAsOfMaxStaleness(t *testing.T) {
	stmts, err := Parse("SELECT * FROM t AS OF SYSTEM TIME with_max_staleness('10s')")
	if err != nil {
		t.Fatal(err)
	}
	sel := stmts[0].(*Select)
	if sel.AsOfMaxStaleness != "10s" || sel.AsOf != "" {
		t.Fatalf("parsed %+v", sel)
	}
	// The exact form still works.
	stmts, err = Parse("SELECT * FROM t AS OF SYSTEM TIME '-5s'")
	if err != nil {
		t.Fatal(err)
	}
	if sel := stmts[0].(*Select); sel.AsOf != "-5s" || sel.AsOfMaxStaleness != "" {
		t.Fatalf("exact form: %+v", sel)
	}
	for _, bad := range []string{
		"SELECT * FROM t AS OF SYSTEM TIME with_max_staleness('10s'",
		"SELECT * FROM t AS OF SYSTEM TIME with_max_staleness(10)",
		"SELECT * FROM t AS OF SYSTEM TIME with_max_staleness()",
		"SELECT * FROM t AS OF SYSTEM TIME with_max_staleness",
	} {
		if _, err := Parse(bad); err == nil {
			t.Fatalf("%q parsed", bad)
		}
	}
}

func TestDecimalTypmod(t *testing.T) {
	// Captured for DECIMAL and its aliases.
	for _, src := range []string{
		"CREATE TABLE t (a DECIMAL(10,2))",
		"CREATE TABLE t (a NUMERIC(10,2))",
	} {
		stmts, err := Parse(src)
		if err != nil {
			t.Fatalf("%s: %v", src, err)
		}
		ct := stmts[0].(*CreateTable)
		if ct.Columns[0].Precision != 10 || ct.Columns[0].Scale != 2 {
			t.Fatalf("%s: got (%d,%d)", src, ct.Columns[0].Precision, ct.Columns[0].Scale)
		}
	}
	// Precision without scale: scale 0.
	stmts, err := Parse("CREATE TABLE t (a DECIMAL(5))")
	if err != nil {
		t.Fatal(err)
	}
	if c := stmts[0].(*CreateTable).Columns[0]; c.Precision != 5 || c.Scale != 0 {
		t.Fatalf("DECIMAL(5): got (%d,%d)", c.Precision, c.Scale)
	}
	// Bare DECIMAL: unconstrained.
	stmts, err = Parse("CREATE TABLE t (a DECIMAL)")
	if err != nil {
		t.Fatal(err)
	}
	if c := stmts[0].(*CreateTable).Columns[0]; c.Precision != 0 || c.Scale != 0 {
		t.Fatalf("bare DECIMAL: got (%d,%d)", c.Precision, c.Scale)
	}
	// ALTER TABLE ADD COLUMN carries it too.
	stmts, err = Parse("ALTER TABLE t ADD COLUMN d DECIMAL(6,3)")
	if err != nil {
		t.Fatal(err)
	}
	if c := stmts[0].(*AlterTable).AddCol; c.Precision != 6 || c.Scale != 3 {
		t.Fatalf("ADD COLUMN: got (%d,%d)", c.Precision, c.Scale)
	}
	// VARCHAR(n) still absorbed and ignored.
	stmts, err = Parse("CREATE TABLE t (a VARCHAR(20))")
	if err != nil {
		t.Fatal(err)
	}
	if c := stmts[0].(*CreateTable).Columns[0]; c.Precision != 0 {
		t.Fatalf("VARCHAR: got precision %d", c.Precision)
	}
	// Invalid bounds.
	for _, src := range []string{
		"CREATE TABLE t (a DECIMAL(0))",
		"CREATE TABLE t (a DECIMAL(1001))",
		"CREATE TABLE t (a DECIMAL(5,6))",
		"CREATE TABLE t (a DECIMAL(1.5))",
	} {
		if _, err := Parse(src); err == nil {
			t.Fatalf("%s: accepted", src)
		}
	}
}

func TestJSONBContainmentParsing(t *testing.T) {
	// Plain form, with and without a leading path.
	stmts, err := Parse(`SELECT * FROM t WHERE j @> '{"a":1}'`)
	if err != nil {
		t.Fatal(err)
	}
	w := stmts[0].(*Select).Where
	if len(w) != 1 || w[0].Op != "@>" || w[0].Column != "j" {
		t.Fatalf("got %+v", w)
	}
	stmts, err = Parse(`SELECT * FROM t WHERE j -> 'a' @> '[1]'`)
	if err != nil {
		t.Fatal(err)
	}
	w = stmts[0].(*Select).Where
	if len(w) != 1 || w[0].Op != "@>" || len(w[0].Path) != 1 {
		t.Fatalf("pathed: %+v", w)
	}
	// NOT lowers to the negated op (and back under double negation).
	stmts, err = Parse(`SELECT * FROM t WHERE NOT (j @> '{"a":1}')`)
	if err != nil {
		t.Fatal(err)
	}
	w = stmts[0].(*Select).Where
	if len(w) != 1 || w[0].Op != "NOT @>" {
		t.Fatalf("negated: %+v", w)
	}
	// Inside OR with De Morgan round-trip.
	stmts, err = Parse(`SELECT * FROM t WHERE NOT (j @> '{"a":1}' OR x = 1)`)
	if err != nil {
		t.Fatal(err)
	}
	w = stmts[0].(*Select).Where
	if len(w) != 2 || w[0].Op != "NOT @>" || w[1].Op != "!=" {
		t.Fatalf("de morgan: %+v", w)
	}
	// Rejected surfaces: a bare @.
	for _, src := range []string{
		`SELECT * FROM t WHERE j @ '1'`,
	} {
		if _, err := Parse(src); err == nil {
			t.Fatalf("%s: accepted", src)
		}
	}
}

func TestAnalyzeAndShowStats(t *testing.T) {
	stmts, err := Parse("ANALYZE orders")
	if err != nil {
		t.Fatal(err)
	}
	if a := stmts[0].(*Analyze); a.Table != "orders" {
		t.Fatalf("got %+v", a)
	}
	stmts, err = Parse("analyze")
	if err != nil {
		t.Fatal(err)
	}
	if a := stmts[0].(*Analyze); a.Table != "" {
		t.Fatalf("bare: %+v", a)
	}
	stmts, err = Parse(`SHOW STATS FOR "orders"`)
	if err != nil {
		t.Fatal(err)
	}
	if s := stmts[0].(*ShowStats); s.Table != "orders" {
		t.Fatalf("show stats: %+v", s)
	}
	// SHOW <other> keeps its SetVar compatibility fallback.
	stmts, err = Parse("SHOW server_version")
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := stmts[0].(*SetVar); !ok {
		t.Fatalf("show fallback: %T", stmts[0])
	}
	if _, err := Parse("SHOW STATS orders"); err == nil {
		t.Fatal("missing FOR accepted")
	}
}

// Qualified table names and the database statements (issue #88).
func TestParseDatabases(t *testing.T) {
	for src, want := range map[string]string{
		`SELECT id FROM t`:                         "t",
		`SELECT id FROM public.t`:                  "t",
		`SELECT id FROM app.t`:                     "app.t",
		`SELECT id FROM app.public.t`:              "app.t",
		`INSERT INTO app.t VALUES (1)`:             "app.t",
		`DELETE FROM app.public.t`:                 "app.t",
		`UPDATE app.t SET x = 1`:                   "app.t",
		`DROP TABLE app.t`:                         "app.t",
		`ALTER TABLE app.t ADD COLUMN y INT8`:      "app.t",
		`CREATE TABLE app.t (id INT8 PRIMARY KEY)`: "app.t",
	} {
		stmts, err := Parse(src)
		if err != nil {
			t.Fatalf("%s: %v", src, err)
		}
		var got string
		switch s := stmts[0].(type) {
		case *Select:
			got = s.Table
		case *Insert:
			got = s.Table
		case *Delete:
			got = s.Table
		case *Update:
			got = s.Table
		case *DropTable:
			got = s.Name
		case *AlterTable:
			got = s.Table
		case *CreateTable:
			got = s.Name
		}
		if got != want {
			t.Errorf("%s: table %q, want %q", src, got, want)
		}
	}
	if _, err := Parse(`SELECT 1 FROM app.other.t`); err == nil {
		t.Fatal("a schema other than public parsed")
	}
	stmts, err := Parse(`SELECT a.id FROM app.t AS a JOIN app.u AS b ON a.id = b.id`)
	if err != nil {
		t.Fatal(err)
	}
	if sel := stmts[0].(*Select); sel.Table != "app.t" || sel.Alias != "a" || sel.Joins[0].Table != "app.u" || sel.Joins[0].Alias != "b" {
		t.Fatalf("joined qualified names: %+v", sel)
	}

	stmts, err = Parse(`CREATE DATABASE IF NOT EXISTS app; DROP DATABASE IF EXISTS app CASCADE; ALTER DATABASE app RENAME TO app2; SHOW DATABASES; USE app; SET database = app; SET search_path TO public; GRANT CONNECT, CREATE ON DATABASE app TO bob; REVOKE CONNECT ON DATABASE app FROM public; GRANT SELECT ON app.t TO bob`)
	if err != nil {
		t.Fatal(err)
	}
	if cd := stmts[0].(*CreateDatabase); cd.Name != "app" || !cd.IfNotExists {
		t.Fatalf("create: %+v", cd)
	}
	if dd := stmts[1].(*DropDatabase); dd.Name != "app" || !dd.IfExists || !dd.Cascade {
		t.Fatalf("drop: %+v", dd)
	}
	if ad := stmts[2].(*AlterDatabase); ad.Name != "app" || ad.NewName != "app2" {
		t.Fatalf("alter: %+v", ad)
	}
	if _, ok := stmts[3].(*ShowDatabases); !ok {
		t.Fatalf("show databases: %T", stmts[3])
	}
	if u := stmts[4].(*Use); u.Name != "app" {
		t.Fatalf("use: %+v", u)
	}
	if sv := stmts[5].(*SetVar); sv.Name != "database" || sv.Value != "app" {
		t.Fatalf("set database: %+v", sv)
	}
	if sv := stmts[6].(*SetVar); sv.Name != "search_path" || sv.Value != "public" {
		t.Fatalf("set search_path: %+v", sv)
	}
	if gr := stmts[7].(*GrantRevoke); gr.Database != "app" || gr.User != "bob" || len(gr.Privileges) != 2 || gr.Privileges[1] != "CREATE" {
		t.Fatalf("grant on database: %+v", gr)
	}
	if gr := stmts[8].(*GrantRevoke); !gr.Revoke || gr.Database != "app" || gr.User != "public" {
		t.Fatalf("revoke from public: %+v", gr)
	}
	if gr := stmts[9].(*GrantRevoke); gr.Table != "app.t" || gr.Database != "" {
		t.Fatalf("grant on qualified table: %+v", gr)
	}
	if e, err := Parse(`SELECT current_database(), current_schema()`); err != nil || e[0].(*Select).Exprs[0].Expr.Func != "current_database" {
		t.Fatalf("current_database(): %v", err)
	}
}

// TestParseReturningAndOnConflict: RETURNING on every write, the ON
// CONFLICT forms, and UPSERT INTO.
func TestParseReturningAndOnConflict(t *testing.T) {
	ins := parseOne(t, `INSERT INTO t (id, v) VALUES (1, 'a') ON CONFLICT (id) DO UPDATE SET v = excluded.v, n = t.n + 1 WHERE t.n < 5 RETURNING id, v AS val, *`).(*Insert)
	oc := ins.OnConflict
	if oc == nil || len(oc.Columns) != 1 || oc.Columns[0] != "id" || oc.DoNothing || len(oc.Set) != 2 ||
		oc.Set[0].Column != "v" || oc.Set[0].Value.Column != "excluded.v" || oc.Set[1].Value.BinOp != "+" || len(oc.Where) != 1 {
		t.Fatalf("on conflict: %+v", oc)
	}
	if len(ins.Returning) != 3 || ins.Returning[0].Expr.Column != "id" || ins.Returning[1].Alias != "val" || !ins.Returning[2].Star {
		t.Fatalf("returning: %+v", ins.Returning)
	}
	ins = parseOne(t, `INSERT INTO t VALUES (1) ON CONFLICT DO NOTHING`).(*Insert)
	if ins.OnConflict == nil || !ins.OnConflict.DoNothing || ins.OnConflict.Columns != nil || ins.Returning != nil {
		t.Fatalf("do nothing: %+v", ins.OnConflict)
	}
	ins = parseOne(t, `INSERT INTO t VALUES (1) ON CONFLICT ON CONSTRAINT t_pkey DO UPDATE SET v = 2`).(*Insert)
	if ins.OnConflict == nil || ins.OnConflict.Constraint != "t_pkey" || len(ins.OnConflict.Set) != 1 {
		t.Fatalf("on constraint: %+v", ins.OnConflict)
	}
	ins = parseOne(t, `UPSERT INTO t (id, v) VALUES (1, 'a') RETURNING id`).(*Insert)
	if !ins.Upsert || len(ins.Returning) != 1 {
		t.Fatalf("upsert: %+v", ins)
	}
	up := parseOne(t, `UPDATE t SET v = 'b' WHERE id = 1 RETURNING v, id * 2 doubled`).(*Update)
	if len(up.Returning) != 2 || up.Returning[1].Alias != "doubled" {
		t.Fatalf("update returning: %+v", up.Returning)
	}
	del := parseOne(t, `DELETE FROM t WHERE id = 1 RETURNING *`).(*Delete)
	if len(del.Returning) != 1 || !del.Returning[0].Star {
		t.Fatalf("delete returning: %+v", del.Returning)
	}
	if n := CountParams(parseOne(t, `INSERT INTO t VALUES ($1) ON CONFLICT (id) DO UPDATE SET v = $2 WHERE t.n > $3 RETURNING v || $4`)); n != 4 {
		t.Fatalf("params: %d", n)
	}
	for _, bad := range []string{
		`INSERT INTO t VALUES (1) ON CONFLICT DO UPDATE SET v = 2`, // DO UPDATE needs a target
		`INSERT INTO t VALUES (1) ON CONFLICT (id) DO`,
		`UPSERT INTO t VALUES (1) ON CONFLICT DO NOTHING`,
	} {
		if _, err := Parse(bad); err == nil {
			t.Errorf("no error for %q", bad)
		}
	}
}

// TestParseSequencesAndDefaults: expression defaults, SERIAL, identity
// columns, DEFAULT as a value, DEFAULT VALUES, OVERRIDING, and the
// sequence statements.
func TestParseSequencesAndDefaults(t *testing.T) {
	ct := parseOne(t, `CREATE TABLE t (id SERIAL PRIMARY KEY, u UUID DEFAULT gen_random_uuid(), at TIMESTAMPTZ DEFAULT now(), n INT8 DEFAULT 1 + 2, k INT8 GENERATED ALWAYS AS IDENTITY (START WITH 10 INCREMENT BY 5), c TEXT DEFAULT 'x')`).(*CreateTable)
	cols := ct.Columns
	if !cols[0].Serial || !cols[0].NotNull || !cols[0].PrimaryKey || cols[0].Type != types.Int {
		t.Fatalf("serial: %+v", cols[0])
	}
	if cols[1].DefaultExpr == nil || cols[1].DefaultExpr.Func != "gen_random_uuid" || cols[2].DefaultExpr.Func != "now" || cols[3].DefaultExpr.BinOp != "+" {
		t.Fatalf("expression defaults: %+v %+v %+v", cols[1].DefaultExpr, cols[2].DefaultExpr, cols[3].DefaultExpr)
	}
	if cols[4].Identity != "always" || cols[4].IdentitySeq == nil || *cols[4].IdentitySeq.Start != 10 || *cols[4].IdentitySeq.Increment != 5 || !cols[4].NotNull {
		t.Fatalf("identity: %+v", cols[4])
	}
	if cols[5].Default == nil || cols[5].Default.S != "x" || cols[5].DefaultExpr != nil {
		t.Fatalf("constant default: %+v", cols[5])
	}
	if _, err := Parse(`CREATE TABLE t (a INT8, b INT8 DEFAULT a + 1)`); err == nil {
		t.Fatal("a DEFAULT referencing a column parsed")
	}
	ins := parseOne(t, `INSERT INTO t (id, c) VALUES (DEFAULT, 'y'), (3, DEFAULT)`).(*Insert)
	if !ins.Rows[0][0].IsDefault || ins.Rows[0][1].IsDefault || !ins.Rows[1][1].IsDefault {
		t.Fatalf("DEFAULT values: %+v", ins.Rows)
	}
	ins = parseOne(t, `INSERT INTO t DEFAULT VALUES RETURNING id`).(*Insert)
	if !ins.DefaultValues || len(ins.Rows) != 0 || len(ins.Returning) != 1 {
		t.Fatalf("DEFAULT VALUES: %+v", ins)
	}
	ins = parseOne(t, `INSERT INTO t (k) OVERRIDING SYSTEM VALUE VALUES (7)`).(*Insert)
	if ins.Overriding != "system" || len(ins.Rows) != 1 {
		t.Fatalf("overriding: %+v", ins)
	}
	up := parseOne(t, `UPDATE t SET c = DEFAULT, n = 5 WHERE id = 1`).(*Update)
	if !up.Set[0].Value.IsDefault || up.Set[1].Value.IsDefault {
		t.Fatalf("SET DEFAULT: %+v", up.Set)
	}
	cs := parseOne(t, `CREATE SEQUENCE IF NOT EXISTS s AS int8 INCREMENT BY 2 MINVALUE 0 MAXVALUE 100 START WITH 4 CACHE 8 CYCLE OWNED BY t.id`).(*CreateSequence)
	o := cs.Options
	if cs.Name != "s" || !cs.IfNotExists || *o.Increment != 2 || *o.MinValue != 0 || *o.MaxValue != 100 || *o.Start != 4 || *o.Cache != 8 || o.Cycle == nil || !*o.Cycle || o.OwnedBy != "t.id" {
		t.Fatalf("create sequence: %+v", o)
	}
	cs = parseOne(t, `CREATE SEQUENCE s2 INCREMENT -1 NO MINVALUE NO MAXVALUE NO CYCLE`).(*CreateSequence)
	if *cs.Options.Increment != -1 || !cs.Options.NoMin || !cs.Options.NoMax || cs.Options.Cycle == nil || *cs.Options.Cycle {
		t.Fatalf("create sequence 2: %+v", cs.Options)
	}
	as := parseOne(t, `ALTER SEQUENCE s RESTART WITH 50 MAXVALUE 200`).(*AlterSequence)
	if as.Name != "s" || !as.Options.RestartSet || *as.Options.Restart != 50 || *as.Options.MaxValue != 200 {
		t.Fatalf("alter sequence: %+v", as.Options)
	}
	as = parseOne(t, `ALTER SEQUENCE s RESTART`).(*AlterSequence)
	if !as.Options.RestartSet || as.Options.Restart != nil {
		t.Fatalf("bare restart: %+v", as.Options)
	}
	ds := parseOne(t, `DROP SEQUENCE IF EXISTS s`).(*DropSequence)
	if ds.Name != "s" || !ds.IfExists {
		t.Fatalf("drop sequence: %+v", ds)
	}
	if _, ok := parseOne(t, `SHOW SEQUENCES`).(*ShowSequences); !ok {
		t.Fatal("SHOW SEQUENCES")
	}
	sel := parseOne(t, `SELECT nextval('s'), currval('s'), lastval(), setval('s', 10, false)`).(*Select)
	if len(sel.Exprs) != 4 || sel.Exprs[0].Expr.Func != "nextval" || sel.Exprs[3].Expr.Func != "setval" {
		t.Fatalf("sequence functions: %+v", sel.Exprs)
	}
}

// TestParseConstraints: column and table constraints in CREATE TABLE,
// every ALTER TABLE constraint form, and DROP TABLE CASCADE.
func TestParseConstraints(t *testing.T) {
	ct := parseOne(t, `CREATE TABLE c (
		id INT8 PRIMARY KEY,
		pid INT8 REFERENCES p ON DELETE CASCADE,
		code TEXT CONSTRAINT c_code_fkey REFERENCES p (code) MATCH SIMPLE ON DELETE SET NULL ON UPDATE CASCADE,
		qty INT8 CHECK (qty > 0),
		a INT8 UNIQUE,
		b TEXT,
		CONSTRAINT c_ab UNIQUE (a, b),
		CHECK (qty < 100 OR b IS NULL),
		CONSTRAINT c_pair_fkey FOREIGN KEY (a, b) REFERENCES q (x, y) ON UPDATE NO ACTION
	)`).(*CreateTable)
	if len(ct.Columns) != 6 || len(ct.Constraints) != 3 {
		t.Fatalf("shape: %d columns, %d constraints", len(ct.Columns), len(ct.Constraints))
	}
	pid := ct.Columns[1].Constraints
	if len(pid) != 1 || pid[0].Kind != "foreign" || pid[0].RefTable != "p" || len(pid[0].RefColumns) != 0 || pid[0].OnDelete != "cascade" || pid[0].OnUpdate != "" {
		t.Fatalf("pid references: %+v", pid)
	}
	code := ct.Columns[2].Constraints
	if len(code) != 1 || code[0].Name != "c_code_fkey" || code[0].RefColumns[0] != "code" || code[0].OnDelete != "set null" || code[0].OnUpdate != "cascade" {
		t.Fatalf("code references: %+v", code)
	}
	qty := ct.Columns[3].Constraints
	if len(qty) != 1 || qty[0].Kind != "check" || qty[0].Check != "qty > 0" || len(qty[0].CheckFails) != 1 || qty[0].CheckFails[0].Op != "<=" {
		t.Fatalf("qty check: %+v", qty)
	}
	if a := ct.Columns[4].Constraints; len(a) != 1 || a[0].Kind != "unique" || a[0].Columns[0] != "a" {
		t.Fatalf("a unique: %+v", a)
	}
	if c := ct.Constraints[0]; c.Name != "c_ab" || c.Kind != "unique" || len(c.Columns) != 2 {
		t.Fatalf("table unique: %+v", c)
	}
	if c := ct.Constraints[1]; c.Kind != "check" || c.Check != "qty < 100 OR b IS NULL" || len(c.CheckFails) != 2 {
		t.Fatalf("table check: %+v", c)
	}
	if c := ct.Constraints[2]; c.Name != "c_pair_fkey" || c.Kind != "foreign" || c.RefTable != "q" || len(c.RefColumns) != 2 || c.OnUpdate != "restrict" {
		t.Fatalf("table foreign key: %+v", c)
	}
	ct = parseOne(t, `CREATE TABLE p (a INT8, b INT8, CONSTRAINT p_pk PRIMARY KEY (a, b))`).(*CreateTable)
	if len(ct.PrimaryKey) != 2 || ct.PrimaryKeyName != "p_pk" {
		t.Fatalf("named primary key: %+v", ct)
	}
	for _, bad := range []string{
		`CREATE TABLE t (a INT8 PRIMARY KEY, CONSTRAINT x)`,
		`CREATE TABLE t (a INT8 PRIMARY KEY, b INT8 REFERENCES p ON DELETE SET DEFAULT)`,
		`CREATE TABLE t (a INT8 PRIMARY KEY, b INT8 REFERENCES p MATCH FULL)`,
		`ALTER TABLE t ADD PRIMARY KEY (a)`,
		`ALTER TABLE t ALTER COLUMN a SET TYPE INT8`,
	} {
		if _, err := Parse(bad); err == nil {
			t.Fatalf("%q parsed", bad)
		}
	}
	at := parseOne(t, `ALTER TABLE t ADD CONSTRAINT t_qty_check CHECK (qty > 0) NOT VALID`).(*AlterTable)
	if at.AddConstraint == nil || at.AddConstraint.Name != "t_qty_check" || at.AddConstraint.Kind != "check" || !at.AddConstraint.NotValid {
		t.Fatalf("add check: %+v", at.AddConstraint)
	}
	at = parseOne(t, `ALTER TABLE t ADD FOREIGN KEY (pid) REFERENCES p (id) ON DELETE RESTRICT`).(*AlterTable)
	if at.AddConstraint == nil || at.AddConstraint.Kind != "foreign" || at.AddConstraint.Name != "" || at.AddConstraint.OnDelete != "restrict" {
		t.Fatalf("add foreign key: %+v", at.AddConstraint)
	}
	at = parseOne(t, `ALTER TABLE t ADD UNIQUE (a, b)`).(*AlterTable)
	if at.AddConstraint == nil || at.AddConstraint.Kind != "unique" || len(at.AddConstraint.Columns) != 2 {
		t.Fatalf("add unique: %+v", at.AddConstraint)
	}
	at = parseOne(t, `ALTER TABLE t DROP CONSTRAINT IF EXISTS t_qty_check`).(*AlterTable)
	if at.DropConstraint != "t_qty_check" || !at.DropConstraintIfExists {
		t.Fatalf("drop constraint: %+v", at)
	}
	at = parseOne(t, `ALTER TABLE t VALIDATE CONSTRAINT t_qty_check`).(*AlterTable)
	if at.ValidateConstraint != "t_qty_check" {
		t.Fatalf("validate: %+v", at)
	}
	at = parseOne(t, `ALTER TABLE t ALTER COLUMN a SET NOT NULL`).(*AlterTable)
	if at.SetNotNull != "a" {
		t.Fatalf("set not null: %+v", at)
	}
	at = parseOne(t, `ALTER TABLE t ALTER a DROP NOT NULL`).(*AlterTable)
	if at.DropNotNull != "a" {
		t.Fatalf("drop not null: %+v", at)
	}
	// ADD COLUMN still parses, with a column constraint attached.
	at = parseOne(t, `ALTER TABLE t ADD COLUMN c INT8 DEFAULT 0`).(*AlterTable)
	if at.AddCol == nil || at.AddCol.Name != "c" {
		t.Fatalf("add column: %+v", at)
	}
	dt := parseOne(t, `DROP TABLE IF EXISTS p CASCADE`).(*DropTable)
	if !dt.IfExists || !dt.Cascade {
		t.Fatalf("drop cascade: %+v", dt)
	}
	// The stored CHECK text round-trips through ParseCheck.
	fails, err := ParseCheck("qty < 100 OR b IS NULL")
	if err != nil || len(fails) != 2 || fails[0].Op != ">=" || fails[1].Op != "IS NOT NULL" {
		t.Fatalf("ParseCheck: %+v %v", fails, err)
	}
	// ON CONFLICT ON CONSTRAINT still parses with CONSTRAINT a keyword.
	ins := parseOne(t, `INSERT INTO t VALUES (1) ON CONFLICT ON CONSTRAINT t_a_key DO NOTHING`).(*Insert)
	if ins.OnConflict == nil || ins.OnConflict.Constraint != "t_a_key" {
		t.Fatalf("on conflict on constraint: %+v", ins.OnConflict)
	}
}

// stepIs reports whether a path step is the given key with the given
// text flag.
func stepIs(s PathStep, key string, text bool) bool {
	return s.Key == key && s.Text == text && !s.IsIndex && len(s.Keys) == 0
}
