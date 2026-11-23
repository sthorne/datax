package testcluster

import (
	"context"
	"testing"
	"time"

	"github.com/sthorne/datax/pkg/sql"
	"github.com/sthorne/datax/pkg/sql/catalog"
)

// TestPrepareUntypedParams: SQL-level PREPARE without a type list (or
// with a shorter one) takes as many arguments as the highest $N the
// statement references, as PostgreSQL does, and types the undeclared
// ones from where they stand, as the protocol-level Parse does.
func TestPrepareUntypedParams(t *testing.T) {
	tc := Start(t, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	s := sql.NewSession(tc.Nodes[0].DB(), catalog.NewAccessor())
	waitForDatabases(t, ctx, s)
	h := nopkHelpers{t, ctx, s}

	h.exec(`PREPARE s AS SELECT $1::int8 + 1`)
	h.expect(`EXECUTE s(1)`, "2")
	h.code(`EXECUTE s`, sql.CodeSyntaxError)
	h.code(`EXECUTE s(1, 2)`, sql.CodeSyntaxError)

	// Declared types first, the rest inferred.
	h.exec(`PREPARE p(int8) AS SELECT $1 + $2::int8`)
	h.expect(`EXECUTE p(1, 2)`, "3")

	// A parameter typed by the column it is compared with or assigned
	// to: a text argument is coerced to it.
	h.exec(`CREATE TABLE pt (id INT8 PRIMARY KEY, d DECIMAL(10,2))`)
	h.exec(`PREPARE ins AS INSERT INTO pt (id, d) VALUES ($1, $2)`)
	h.exec(`EXECUTE ins('7', '1.25')`)
	h.exec(`PREPARE sel AS SELECT d FROM pt WHERE id = $1`)
	h.expect(`EXECUTE sel('7')`, "1.25")

	// A statement that cannot be planned fails at PREPARE, not later.
	h.code(`PREPARE bad AS SELECT * FROM nope WHERE x = $1`, sql.CodeUndefinedTable)
}
