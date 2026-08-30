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
