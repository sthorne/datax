package testcluster

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/sthorne/datax/pkg/sql"
)

// TestVirtualRowsOncePerStatement: a correlated subquery over the
// catalogs gathers the catalog once per statement, not once per outer
// row. pg_dump's catalog queries are full of such subqueries; gathering
// per row made a dump of a small database take seconds, and a test of
// pg_dump under the race detector overran its budget.
func TestVirtualRowsOncePerStatement(t *testing.T) {
	tc := Start(t, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	conn := pgConn(t, ctx, tc, 0)
	const tables = 30
	for i := range tables {
		mustExec(t, ctx, conn, fmt.Sprintf(`CREATE TABLE memo_%d (a INT PRIMARY KEY, b TEXT)`, i))
	}
	q := `SELECT c.relname,
	        (SELECT count(*) FROM pg_attribute a WHERE a.attrelid = c.oid AND a.attnum > 0)
	      FROM pg_class c WHERE c.relkind = 'r' AND c.relname LIKE 'memo%' ORDER BY c.relname`
	before := sql.TestingVirtualEnvCalls.Load()
	rows, err := conn.Query(ctx, q)
	if err != nil {
		t.Fatal(err)
	}
	n := 0
	for rows.Next() {
		var name string
		var cols string
		if err := rows.Scan(&name, &cols); err != nil {
			t.Fatal(err)
		}
		if cols != "2" {
			t.Fatalf("%s has %s columns, want 2", name, cols)
		}
		n++
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if n != tables {
		t.Fatalf("listed %d tables, want %d", n, tables)
	}
	// Two virtual tables, each gathered once; a little room for a
	// background reader of the catalogs.
	if calls := sql.TestingVirtualEnvCalls.Load() - before; calls > 6 {
		t.Fatalf("the statement gathered the catalog %d times for %d outer rows", calls, tables)
	}
}
