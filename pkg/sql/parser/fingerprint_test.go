package parser

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

func shapeOf(t *testing.T, src string) Shape {
	t.Helper()
	stmts, err := Parse(src)
	if err != nil {
		t.Fatalf("parse %q: %v", src, err)
	}
	if len(stmts) != 1 {
		t.Fatalf("%q parsed to %d statements", src, len(stmts))
	}
	return Fingerprint(stmts[0])
}

// TestFingerprintEquality (issue #157): statements differing only in
// their values are one shape. This is the whole premise — a statement
// that runs forty thousand times an hour is only visible if its
// executions group.
func TestFingerprintEquality(t *testing.T) {
	for _, group := range [][]string{
		{
			`SELECT * FROM accounts WHERE id = 1`,
			`SELECT * FROM accounts WHERE id = 2`,
			`SELECT * FROM accounts WHERE id = $1`,
			// Whitespace, case and comments are not shape. A lexical
			// normaliser gets the first two and can be fooled by the
			// third; the AST cannot.
			"select  *\n  from Accounts\twhere ID = 99",
		},
		{
			`UPDATE accounts SET balance = balance - 7 WHERE id = 1`,
			`UPDATE accounts SET balance = balance - 99 WHERE id = 4`,
			`UPDATE accounts SET balance = balance - $1 WHERE id = $2`,
		},
		{
			// An IN list of three and of one are one shape: the length
			// is a value.
			`SELECT name FROM items WHERE id IN (1, 2, 3)`,
			`SELECT name FROM items WHERE id IN (9)`,
			`SELECT name FROM items WHERE id IN ($1, $2, $3, $4, $5)`,
		},
		{
			// One row of values and many rows are one shape.
			`INSERT INTO items (id, name) VALUES (1, 'anvil')`,
			`INSERT INTO items (id, name) VALUES (2, 'rope'), (3, 'tent')`,
			`INSERT INTO items (id, name) VALUES ($1, $2)`,
		},
		{
			`DELETE FROM items WHERE id = 1`,
			`DELETE FROM items WHERE id = 77`,
		},
		{
			// LIMIT's presence is shape; its value is not.
			`SELECT id FROM items ORDER BY id LIMIT 10`,
			`SELECT id FROM items ORDER BY id LIMIT 500`,
		},
	} {
		first := shapeOf(t, group[0])
		for _, src := range group[1:] {
			got := shapeOf(t, src)
			if got.Hash != first.Hash {
				t.Errorf("different shapes for the same query:\n  %q → %q\n  %q → %q",
					group[0], first.Text, src, got.Text)
			}
		}
		if strings.ContainsAny(first.Text, "0123456789") && !strings.Contains(first.Text, "?") {
			t.Errorf("a literal survived into the shape of %q: %q", group[0], first.Text)
		}
	}
}

// Genuinely different shapes must not collide: the list is only useful
// if a row names one thing.
func TestFingerprintInequality(t *testing.T) {
	distinct := []string{
		`SELECT * FROM accounts WHERE id = 1`,
		`SELECT * FROM items WHERE id = 1`,                     // another table
		`SELECT balance FROM accounts WHERE id = 1`,            // another projection
		`SELECT * FROM accounts WHERE balance = 1`,             // another column
		`SELECT * FROM accounts WHERE id > 1`,                  // another operator
		`SELECT * FROM accounts WHERE id = 1 ORDER BY balance`, // an ordering
		`SELECT * FROM accounts WHERE id = 1 LIMIT 1`,          // a limit
		`SELECT * FROM accounts WHERE id IN (1)`,               // IN, not =
		`UPDATE accounts SET balance = 1 WHERE id = 1`,         // another verb
		`DELETE FROM accounts WHERE id = 1`,                    // another verb
		`SELECT count(*) FROM accounts WHERE id = 1`,           // an aggregate
		`SELECT * FROM accounts WHERE id = 1 FOR UPDATE`,       // a lock
	}
	seen := map[string]string{}
	for _, src := range distinct {
		s := shapeOf(t, src)
		if prev, dup := seen[s.Hash]; dup {
			t.Errorf("distinct queries collided on one shape %q:\n  %q\n  %q", s.Text, prev, src)
		}
		seen[s.Hash] = src
	}
}

// The shape names the tables it touches, so the console can group by
// them without re-parsing.
func TestFingerprintTables(t *testing.T) {
	s := shapeOf(t, `SELECT a.id FROM accounts a JOIN items i ON a.id = i.owner WHERE a.balance > 5`)
	if len(s.Tables) != 2 || s.Tables[0] != "accounts" || s.Tables[1] != "items" {
		t.Fatalf("tables %v, want [accounts items]: %q", s.Tables, s.Text)
	}
	// Sorted and deduplicated, so the same set from a different written
	// order is the same list.
	u := shapeOf(t, `UPDATE accounts SET balance = 1 WHERE id = 2`)
	if len(u.Tables) != 1 || u.Tables[0] != "accounts" {
		t.Fatalf("tables %v: %q", u.Tables, u.Text)
	}
}

// The shape is readable: it is what the console lists, so it has to look
// like the statement it stands for.
func TestFingerprintReadable(t *testing.T) {
	for _, tc := range []struct{ src, want string }{
		{`UPDATE accounts SET balance = balance - 7 WHERE id = 1`,
			"UPDATE accounts SET balance = (balance - ?) WHERE id = ?"},
		{`SELECT * FROM items WHERE id IN (1, 2, 3)`,
			"SELECT * FROM items WHERE id IN (?)"},
		{`DELETE FROM items WHERE id = 4`, "DELETE FROM items WHERE id = ?"},
	} {
		if got := shapeOf(t, tc.src).Text; got != tc.want {
			t.Errorf("shape of %q\n got %q\nwant %q", tc.src, got, tc.want)
		}
	}
}

// A hash is stable across runs and is not the empty digest for an empty
// shape — the accounting keys on it.
func TestFingerprintHashStable(t *testing.T) {
	a := shapeOf(t, `SELECT 1`)
	b := shapeOf(t, `SELECT 1`)
	if a.Hash != b.Hash || a.Hash == "" {
		t.Fatalf("hash %q vs %q", a.Hash, b.Hash)
	}
	if len(a.Hash) != 16 {
		t.Errorf("hash %q is not 16 hex characters", a.Hash)
	}
}

// Statements nobody optimises still fingerprint, to a kind and their
// subject: two CREATE TABLEs on different tables are different rows, and
// a COMMIT never needs more than its name.
func TestFingerprintNonDML(t *testing.T) {
	a := shapeOf(t, `CREATE TABLE t1 (id INT8 PRIMARY KEY)`)
	b := shapeOf(t, `CREATE TABLE t2 (id INT8 PRIMARY KEY)`)
	if a.Hash == b.Hash {
		t.Errorf("two CREATE TABLEs collided: %q", a.Text)
	}
	c := shapeOf(t, `COMMIT`)
	if c.Text == "" || c.Hash == "" {
		t.Errorf("COMMIT produced no shape: %+v", c)
	}
}

// TestFingerprintKindsReadAsSQL (issue #275): a statement that is named
// by kind rather than rendered in full is named in the SQL words that
// open it. The kind used to be the Go type's name upper-cased whole, so
// the console listed "CREATETABLE bench_kv" and "ROLLBACKTOSAVEPOINT".
func TestFingerprintKindsReadAsSQL(t *testing.T) {
	for _, c := range []struct{ sql, want string }{
		{`CREATE TABLE IF NOT EXISTS t1 (id INT8 PRIMARY KEY)`, "CREATE TABLE t1"},
		{`DROP TABLE t1`, "DROP TABLE t1"},
		{`ALTER TABLE t1 ADD COLUMN v TEXT`, "ALTER TABLE t1"},
		// The subject of CREATE INDEX is the table: "CREATE INDEX t1"
		// would read as an index named t1.
		{`CREATE INDEX IF NOT EXISTS t1_v ON t1 (v)`, "CREATE INDEX ON t1"},
		{`DROP INDEX t1_v`, "DROP INDEX t1_v"},
		{`TRUNCATE t1`, "TRUNCATE t1"},
		{`BEGIN`, "BEGIN"},
		{`COMMIT`, "COMMIT"},
		{`ROLLBACK`, "ROLLBACK"},
		{`SAVEPOINT s`, "SAVEPOINT"},
		{`RELEASE SAVEPOINT s`, "RELEASE SAVEPOINT"},
		{`ROLLBACK TO SAVEPOINT s`, "ROLLBACK TO SAVEPOINT"},
		{`GRANT SELECT ON t1 TO alice`, "GRANT"},
		{`REVOKE SELECT ON t1 FROM alice`, "REVOKE"},
		{`SET statement_timeout = '5s'`, "SET"},
		{`SHOW TABLES`, "SHOW TABLES"},
		{`ANALYZE t1`, "ANALYZE"},
		// CREATE and ALTER ROLE share a type; a password change is not a
		// role being created (from InfoSec's review of #284). The role's
		// name and the password are not part of the shape.
		{`CREATE ROLE alice LOGIN PASSWORD 'hunter2'`, "CREATE ROLE"},
		{`CREATE USER alice PASSWORD 'hunter2'`, "CREATE ROLE"},
		{`ALTER ROLE alice PASSWORD 'hunter2'`, "ALTER ROLE"},
		{`ALTER USER alice PASSWORD 'hunter2'`, "ALTER ROLE"},
		// "ALTER OWNER" is not a statement.
		{`ALTER TABLE t1 OWNER TO alice`, "ALTER TABLE t1 OWNER TO"},
		{`ALTER DATABASE d OWNER TO alice`, "ALTER DATABASE d OWNER TO"},
		// SET, RESET and SHOW name share a type too (from QA's review of
		// #284): a read of a setting is not a write of one.
		{`SHOW statement_timeout`, "SHOW"},
		{`SHOW TRANSACTION ISOLATION LEVEL`, "SHOW"},
		{`RESET statement_timeout`, "RESET"},
		{`RESET ALL`, "RESET"},
		{`SET LOCAL statement_timeout = '30s'`, "SET"},
	} {
		got := shapeOf(t, c.sql).Text
		if got != c.want {
			t.Errorf("%s\n    shape %q, want %q", c.sql, got, c.want)
		}
	}
	// Whatever the kind, it is words: no shape opens with two SQL words
	// run together. A new statement type whose name is not its keywords
	// shows up here as soon as it is added to the list above.
	for _, sql := range []string{`CREATE VIEW v AS SELECT 1`, `DROP VIEW v`, `CREATE SEQUENCE s`, `CREATE ROLE r`, `DROP ROLE r`, `SHOW DATABASES`} {
		got := shapeOf(t, sql).Text
		first := strings.Fields(sql)[0] + " " + strings.Fields(sql)[1]
		if !strings.HasPrefix(got, first) {
			t.Errorf("%s\n    shape %q does not open with %q", sql, got, first)
		}
	}
}

// TestEveryStatementKindIsWords (issue #275, from the UI review of
// #284): the kind of a statement is its type's name split into words,
// and that is only right while every type is named in whole SQL words.
// This walks every Statement type in the package, so one added later
// under a name the splitter cannot read — an initialism, a stray
// letter — fails here instead of appearing in the console's list.
func TestEveryStatementKindIsWords(t *testing.T) {
	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	stmtMethod := regexp.MustCompile(`(?m)^func \((?:\w+ )?\*?(\w+)\) stmt\(\)`)
	seen := 0
	for _, f := range files {
		if strings.HasSuffix(f, "_test.go") {
			continue
		}
		src, err := os.ReadFile(f)
		if err != nil {
			t.Fatal(err)
		}
		for _, m := range stmtMethod.FindAllStringSubmatch(string(src), -1) {
			seen++
			kind := kindWords(m[1])
			for _, w := range strings.Fields(kind) {
				if len(w) < 2 {
					t.Errorf("%s reads as %q: the one-letter word %q means the type's name is not made of whole SQL words; name its kind outright in statementKindOf", m[1], kind, w)
				}
			}
			if kind != strings.ToUpper(kind) || strings.Contains(kind, "  ") {
				t.Errorf("%s reads as %q", m[1], kind)
			}
		}
	}
	if seen < 40 {
		t.Fatalf("found %d Statement types, want the forty-odd the parser has: the stmt() marker is no longer where this looks", seen)
	}
	for name, want := range map[string]string{
		"CreateTable": "CREATE TABLE", "RollbackToSavepoint": "ROLLBACK TO SAVEPOINT", "Use": "USE",
		// A run of capitals is one word.
		"ShowSQL": "SHOW SQL", "SQLShow": "SQL SHOW", "ShowDDLFor": "SHOW DDL FOR",
	} {
		if got := kindWords(name); got != want {
			t.Errorf("kindWords(%q) = %q, want %q", name, got, want)
		}
	}
}
