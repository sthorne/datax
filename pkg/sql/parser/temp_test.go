package parser

import (
	"reflect"
	"testing"
)

// TestParseTempTables: CREATE [GLOBAL | LOCAL] TEMP[ORARY] TABLE, CREATE
// UNLOGGED TABLE, the ON COMMIT clause (also before AS), and ALTER TABLE
// ... SET LOGGED / UNLOGGED (gap PG-D-009).
func TestParseTempTables(t *testing.T) {
	for _, c := range []struct {
		sql                  string
		temp, unlogged       bool
		onCommit, name, asOf string
	}{
		{sql: `CREATE TEMP TABLE t (a INT)`, temp: true, name: "t"},
		{sql: `CREATE TEMPORARY TABLE IF NOT EXISTS t (a INT)`, temp: true, name: "t"},
		{sql: `CREATE LOCAL TEMP TABLE t (a INT)`, temp: true, name: "t"},
		{sql: `CREATE GLOBAL TEMPORARY TABLE t (a INT)`, temp: true, name: "t"},
		{sql: `CREATE UNLOGGED TABLE t (a INT)`, unlogged: true, name: "t"},
		{sql: `CREATE TABLE pg_temp.t (a INT)`, name: "pg_temp.t"},
		{sql: `CREATE TEMP TABLE t (a INT) ON COMMIT PRESERVE ROWS`, temp: true, onCommit: "preserve", name: "t"},
		{sql: `CREATE TEMP TABLE t (a INT) ON COMMIT DELETE ROWS`, temp: true, onCommit: "delete", name: "t"},
		{sql: `CREATE TEMP TABLE t (a INT) ON COMMIT DROP`, temp: true, onCommit: "drop", name: "t"},
		{sql: `CREATE TEMP TABLE t ON COMMIT DROP AS SELECT 1`, temp: true, onCommit: "drop", name: "t", asOf: "SELECT 1"},
		{sql: `CREATE TEMP TABLE t (a) ON COMMIT DELETE ROWS AS SELECT 1`, temp: true, onCommit: "delete", name: "t", asOf: "SELECT 1"},
		{sql: `CREATE TEMP TABLE t AS SELECT 1`, temp: true, name: "t", asOf: "SELECT 1"},
		{sql: `CREATE TABLE t (a INT) ON COMMIT DROP`, onCommit: "drop", name: "t"},
	} {
		ct, ok := parseOne(t, c.sql).(*CreateTable)
		if !ok {
			t.Fatalf("%s: not a CREATE TABLE", c.sql)
		}
		if ct.Temp != c.temp || ct.Unlogged != c.unlogged || ct.OnCommit != c.onCommit || ct.Name != c.name || ct.AsText != c.asOf {
			t.Errorf("%s: parsed as temp=%v unlogged=%v on commit %q name %q as %q", c.sql, ct.Temp, ct.Unlogged, ct.OnCommit, ct.Name, ct.AsText)
		}
	}
	if ct := parseOne(t, `CREATE TEMP TABLE t (a, b) AS SELECT 1, 2`).(*CreateTable); !reflect.DeepEqual(ct.AsColumns, []string{"a", "b"}) {
		t.Errorf("column names before AS: %v", ct.AsColumns)
	}
	for _, bad := range []string{
		`CREATE TEMP TABLE t (a INT) ON COMMIT`,
		`CREATE TEMP TABLE t (a INT) ON COMMIT DELETE`,
		`CREATE TEMP TABLE t (a INT) ON COMMIT KEEP ROWS`,
		`CREATE TEMP TABLE t ON COMMIT DROP (a INT)`,
		`CREATE TEMP UNLOGGED TABLE t (a INT)`,
	} {
		if _, err := Parse(bad); err == nil {
			t.Errorf("%s parsed", bad)
		}
	}
	for sql, want := range map[string]string{
		`ALTER TABLE t SET LOGGED`:   "logged",
		`ALTER TABLE t SET UNLOGGED`: "unlogged",
	} {
		if at := parseOne(t, sql).(*AlterTable); at.SetLogged != want {
			t.Errorf("%s: SetLogged %q", sql, at.SetLogged)
		}
	}
	if at := parseOne(t, `ALTER TABLE t SET (gc_ttl = '1h')`).(*AlterTable); at.SetLogged != "" || at.SetOptions["gc_ttl"] == "" {
		t.Errorf("SET (...) parsed as %+v", at)
	}
}
