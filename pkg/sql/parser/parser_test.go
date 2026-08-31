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
		"SELECT * FROM t WHERE a LIKE 'x'",
		"CREATE TABLE t (a FANCYTYPE)",
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
	if sel.Exprs[0].Expr.Column != "j" || len(p0) != 1 || p0[0] != (PathStep{Key: "a"}) {
		t.Fatalf("expr 0: %+v", sel.Exprs[0].Expr)
	}
	p1 := sel.Exprs[1].Expr.Path
	if len(p1) != 2 || p1[0] != (PathStep{Key: "a"}) || p1[1] != (PathStep{Key: "b", Text: true}) {
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
	if w1.Op != "IS NULL" || len(w1.Path) != 1 || w1.Path[0] != (PathStep{Key: "n"}) {
		t.Fatalf("where 1: %+v", w1)
	}

	for _, bad := range []string{
		`SELECT j ->> 'a' -> 'b' FROM t`, // ->> yields text: not chainable
		`SELECT j -> 5 FROM t`,           // keys are string literals
		`SELECT j -> col FROM t`,
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

	// Mixed columns stay an OR; subqueries inside OR are rejected.
	sel = parseOne(t, `SELECT id FROM t WHERE a = 1 OR b = 2`).(*Select)
	if sel.Where[0].Op != "OR" {
		t.Fatalf("mixed: %+v", sel.Where[0])
	}
	if _, err := Parse(`SELECT id FROM t WHERE a = 1 OR b IN (SELECT x FROM u)`); err == nil {
		t.Fatal("subquery inside OR accepted")
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
