package testcluster

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/sthorne/datax/pkg/sql"
	"github.com/sthorne/datax/pkg/sql/catalog"
	"github.com/sthorne/datax/pkg/sql/rowseal"
)

// TestUpdatePrimaryKey (issue #322): UPDATE may change primary-key
// columns. The row moves to its new key with its index entries; the
// statement's keys are checked together (a row may take a key another
// row of the statement gives up, never one a row it leaves in place
// holds); foreign keys see the change; RETURNING shows the new row.
func TestUpdatePrimaryKey(t *testing.T) {
	tc := Start(t, 3)
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()
	conn := pgConn(t, ctx, tc, 0)

	mustExec(t, ctx, conn, `CREATE TABLE t (id INT PRIMARY KEY, name TEXT, code TEXT UNIQUE, n INT)`)
	mustExec(t, ctx, conn, `CREATE INDEX t_n ON t (n)`)
	mustExec(t, ctx, conn, `INSERT INTO t VALUES (1, 'a', 'ca', 10), (2, 'b', 'cb', 20), (3, 'c', NULL, 30)`)

	// One row, with RETURNING.
	wantRows(t, ctx, conn, `UPDATE t SET id = 10 WHERE id = 1 RETURNING id, name, code`, "10|a|ca")
	wantRows(t, ctx, conn, `SELECT id, name FROM t ORDER BY id`, "2|b", "3|c", "10|a")
	// Every index follows the row: the unique one (whose entry's value is
	// the key), the non-unique one (whose entry's key ends in it).
	wantRows(t, ctx, conn, `SELECT id FROM t WHERE code = 'ca'`, "10")
	wantRows(t, ctx, conn, `SELECT id FROM t WHERE n = 10`, "10")
	wantPlan(t, ctx, conn, `SELECT id FROM t WHERE n = 10`, `scan of index "t_n"`)
	wantRows(t, ctx, conn, `SELECT id FROM t WHERE id = 1`)

	// A key another row holds: 23505, and nothing changed.
	wantSQLState(t, ctx, conn, "23505", `UPDATE t SET id = 2 WHERE id = 10`)
	wantSQLState(t, ctx, conn, "23505", `UPDATE t SET id = 5`)
	wantRows(t, ctx, conn, `SELECT id FROM t ORDER BY id`, "2", "3", "10")

	// Keys shifting over each other: checked for the statement as a
	// whole, in either direction (PostgreSQL, checking a non-deferrable
	// key row by row, refuses the first).
	mustExec(t, ctx, conn, `UPDATE t SET id = id + 1`)
	wantRows(t, ctx, conn, `SELECT id, name, n FROM t ORDER BY id`, "3|b|20", "4|c|30", "11|a|10")
	mustExec(t, ctx, conn, `UPDATE t SET id = id - 1`)
	wantRows(t, ctx, conn, `SELECT id, name, n FROM t ORDER BY id`, "2|b|20", "3|c|30", "10|a|10")
	// A swap.
	mustExec(t, ctx, conn, `UPDATE t SET id = CASE id WHEN 2 THEN 3 WHEN 3 THEN 2 ELSE id END`)
	wantRows(t, ctx, conn, `SELECT id, name FROM t ORDER BY id`, "2|c", "3|b", "10|a")
	wantRows(t, ctx, conn, `SELECT id FROM t WHERE code = 'cb'`, "3")
	wantRows(t, ctx, conn, `SELECT id FROM t WHERE n = 20`, "3")
	// Two rows onto one key, and one onto a key a row keeps.
	wantSQLState(t, ctx, conn, "23505", `UPDATE t SET id = 7 WHERE id IN (2, 3)`)
	wantSQLState(t, ctx, conn, "23505", `UPDATE t SET id = 10 WHERE id IN (2, 10)`)
	// The unique column shifts the same way.
	mustExec(t, ctx, conn, `UPDATE t SET code = CASE code WHEN 'ca' THEN 'cb' WHEN 'cb' THEN 'ca' END WHERE code IS NOT NULL`)
	wantRows(t, ctx, conn, `SELECT id, code FROM t ORDER BY id`, "2|<nil>", "3|ca", "10|cb")
	wantSQLState(t, ctx, conn, "23505", `UPDATE t SET code = 'cb' WHERE id = 2`)
	// A NULL key column.
	wantSQLState(t, ctx, conn, "23502", `UPDATE t SET id = NULL WHERE id = 2`)
	// The key and other columns at once.
	wantRows(t, ctx, conn, `UPDATE t SET id = id * 100, n = n + 1, code = 'z' WHERE id = 2 RETURNING *`, "200|c|z|31")
	wantRows(t, ctx, conn, `SELECT id FROM t WHERE n = 31`, "200")
	wantRows(t, ctx, conn, `SELECT id FROM t WHERE n = 30`)

	// In a transaction: the moved row reads at its new key, and a
	// rollback restores it.
	tx, err := conn.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, `UPDATE t SET id = 300 WHERE id = 200`); err != nil {
		t.Fatal(err)
	}
	var name string
	if err := tx.QueryRow(ctx, `SELECT name FROM t WHERE id = 300`).Scan(&name); err != nil || name != "c" {
		t.Fatalf("in the transaction: %q %v", name, err)
	}
	if err := tx.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	wantRows(t, ctx, conn, `SELECT id FROM t ORDER BY id`, "3", "10", "200")

	// A composite key, one column of it changed.
	mustExec(t, ctx, conn, `CREATE TABLE c (a TEXT, b INT, v TEXT, PRIMARY KEY (a, b))`)
	mustExec(t, ctx, conn, `INSERT INTO c VALUES ('x', 1, 'one'), ('x', 2, 'two'), ('y', 1, 'three')`)
	mustExec(t, ctx, conn, `UPDATE c SET a = 'z' WHERE a = 'x'`)
	wantRows(t, ctx, conn, `SELECT a, b, v FROM c ORDER BY a, b`, "y|1|three", "z|1|one", "z|2|two")
	wantSQLState(t, ctx, conn, "23505", `UPDATE c SET a = 'y' WHERE b = 1`)

	// A hash-sharded table: the moved row is filed under its new key's
	// bucket.
	mustExec(t, ctx, conn, `CREATE TABLE sh (s INT, at TIMESTAMPTZ, v TEXT, PRIMARY KEY (s, at)) WITH (timeseries = true, shards = 4)`)
	mustExec(t, ctx, conn, `INSERT INTO sh VALUES (1, '2026-01-01 00:00:00Z', 'a'), (2, '2026-01-01 00:00:00Z', 'b'), (3, '2026-01-01 00:00:00Z', 'c')`)
	mustExec(t, ctx, conn, `UPDATE sh SET s = s + 10, at = at + INTERVAL '1 hour'`)
	wantRows(t, ctx, conn, `SELECT s, v FROM sh ORDER BY s`, "11|a", "12|b", "13|c")
	wantRows(t, ctx, conn, `SELECT v FROM sh WHERE s = 12 AND at = '2026-01-01 01:00:00Z'`, "b")
	wantRows(t, ctx, conn, `SELECT count(*) FROM sh WHERE s < 10`, "0")

	// Foreign keys: ON UPDATE CASCADE re-keys the children, SET NULL
	// nulls them, RESTRICT refuses; the child's own key into the parent
	// is checked when it changes.
	mustExec(t, ctx, conn, `CREATE TABLE p (id INT PRIMARY KEY, v TEXT)`)
	mustExec(t, ctx, conn, `CREATE TABLE kc (id INT PRIMARY KEY, pid INT REFERENCES p (id) ON UPDATE CASCADE)`)
	mustExec(t, ctx, conn, `CREATE TABLE kn (id INT PRIMARY KEY, pid INT REFERENCES p (id) ON UPDATE SET NULL)`)
	mustExec(t, ctx, conn, `CREATE TABLE kr (id INT PRIMARY KEY, pid INT REFERENCES p (id))`)
	mustExec(t, ctx, conn, `INSERT INTO p VALUES (1, 'a'), (2, 'b'), (3, 'c')`)
	mustExec(t, ctx, conn, `INSERT INTO kc VALUES (10, 1), (11, 1), (12, 2)`)
	mustExec(t, ctx, conn, `INSERT INTO kn VALUES (20, 2)`)
	mustExec(t, ctx, conn, `INSERT INTO kr VALUES (30, 3)`)
	mustExec(t, ctx, conn, `UPDATE p SET id = 100 WHERE id = 1`)
	wantRows(t, ctx, conn, `SELECT id, pid FROM kc ORDER BY id`, "10|100", "11|100", "12|2")
	mustExec(t, ctx, conn, `UPDATE p SET id = 200 WHERE id = 2`)
	wantRows(t, ctx, conn, `SELECT id, pid FROM kc ORDER BY id`, "10|100", "11|100", "12|200")
	wantRows(t, ctx, conn, `SELECT id, pid FROM kn`, "20|<nil>")
	wantSQLState(t, ctx, conn, "23503", `UPDATE p SET id = 300 WHERE id = 3`)
	wantRows(t, ctx, conn, `SELECT id FROM p ORDER BY id`, "3", "100", "200")
	// The child side: a key column that is a foreign key too.
	mustExec(t, ctx, conn, `CREATE TABLE kk (pid INT REFERENCES p (id), seq INT, PRIMARY KEY (pid, seq))`)
	mustExec(t, ctx, conn, `INSERT INTO kk VALUES (3, 1)`)
	wantSQLState(t, ctx, conn, "23503", `UPDATE kk SET pid = 4`)
	mustExec(t, ctx, conn, `UPDATE kk SET pid = 100`)
	wantRows(t, ctx, conn, `SELECT pid, seq FROM kk`, "100|1")
	// A table referencing itself with ON UPDATE CASCADE: a key change
	// cascades to the rows referencing it — but a statement that also
	// changes one of those rows itself is refused (one of the two
	// changes would be lost).
	mustExec(t, ctx, conn, `CREATE TABLE emp (id INT PRIMARY KEY, boss INT REFERENCES emp (id) ON UPDATE CASCADE, code TEXT UNIQUE, mentor TEXT REFERENCES emp (code) ON UPDATE CASCADE)`)
	mustExec(t, ctx, conn, `INSERT INTO emp VALUES (1, NULL, 'a', NULL)`)
	mustExec(t, ctx, conn, `INSERT INTO emp VALUES (2, 1, 'b', 'a')`)
	mustExec(t, ctx, conn, `INSERT INTO emp VALUES (3, 2, 'c', 'b')`)
	mustExec(t, ctx, conn, `UPDATE emp SET id = 10 WHERE id = 1`)
	wantRows(t, ctx, conn, `SELECT id, boss FROM emp ORDER BY id`, "2|10", "3|2", "10|<nil>")
	wantSQLState(t, ctx, conn, "0A000", `UPDATE emp SET id = id + 100`)
	wantSQLState(t, ctx, conn, "0A000", `UPDATE emp SET code = code || 'x'`)
	mustExec(t, ctx, conn, `UPDATE emp SET code = 'bb' WHERE id = 2`)
	wantRows(t, ctx, conn, `SELECT id, mentor FROM emp ORDER BY id`, "2|a", "3|bb", "10|<nil>")
	wantRows(t, ctx, conn, `SELECT id FROM emp WHERE mentor = 'bb'`, "3")

	// A vector index: its entries end in the primary key, and move too.
	mustExec(t, ctx, conn, `CREATE TABLE vec (id INT8 PRIMARY KEY, v vector(2))`)
	mustExec(t, ctx, conn, `INSERT INTO vec VALUES (1, '[0,0]'), (2, '[0,1]'), (3, '[10,10]'), (4, '[10,11]')`)
	mustExec(t, ctx, conn, `CREATE INDEX vec_l2 ON vec USING ivfflat (v vector_l2_ops) WITH (lists = 2)`)
	mustExec(t, ctx, conn, `UPDATE vec SET id = id + 100`)
	if got := checkVectorIndex(t, ctx, tc.Nodes[0].DB(), "vec", "vec_l2"); got != 4 {
		t.Fatalf("the vector index holds %d entries after the move, want 4", got)
	}
	wantRows(t, ctx, conn, `SELECT id FROM vec ORDER BY v <-> '[10,10]' LIMIT 2`, "103", "104")

	// ON CONFLICT DO UPDATE still may not change the key.
	wantSQLState(t, ctx, conn, "0A000", `INSERT INTO p VALUES (3, 'x') ON CONFLICT (id) DO UPDATE SET id = 4`)

	// The same through a session (its batched reads, no streaming).
	s := sql.NewSession(tc.Nodes[1].DB(), catalog.NewAccessor())
	execSQL(t, ctx, s, `UPDATE t SET id = id + 1000 WHERE id < 1000`)
	requireSameRows(t, "after a session's move", rowsText(t, ctx, s, `SELECT id, code FROM t ORDER BY id`), []string{"1003|ca", "1010|cb", "1200|z"})
	requireSameRows(t, "index after a session's move", rowsText(t, ctx, s, `SELECT id FROM t WHERE code = 'cb'`), []string{"1010"})
}

// TestUpdatePrimaryKeyRowSecurity (issue #322): an UPDATE that moves a
// row is held to the UPDATE policy's WITH CHECK on the new row, and a
// refusal is the policy's, whether or not the new key is taken.
func TestUpdatePrimaryKeyRowSecurity(t *testing.T) {
	tc := Start(t, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	root := rlsConn(t, ctx, tc, 0, "root")
	for _, q := range []string{
		`CREATE USER app PASSWORD 'app-pw-123'`,
		`CREATE TABLE docs (id INT PRIMARY KEY, owner TEXT, body TEXT)`,
		`GRANT SELECT, INSERT, UPDATE, DELETE ON docs TO app`,
		`ALTER TABLE docs ENABLE ROW LEVEL SECURITY`,
		`CREATE POLICY mine ON docs USING (owner = current_user) WITH CHECK (owner = current_user AND id < 100)`,
		`INSERT INTO docs VALUES (1, 'app', 'one'), (2, 'app', 'two'), (150, 'other', 'theirs')`,
	} {
		mustExec(t, ctx, root, q)
	}
	app := rlsConn(t, ctx, tc, 0, "app")
	mustExec(t, ctx, app, `UPDATE docs SET id = 5 WHERE id = 1`)
	wantSQLState(t, ctx, app, "42501", `UPDATE docs SET id = 120 WHERE id = 2`)
	wantSQLState(t, ctx, app, "42501", `UPDATE docs SET id = 150 WHERE id = 2`) // taken, but refused by policy first
	wantSQLState(t, ctx, app, "23505", `UPDATE docs SET id = 5 WHERE id = 2`)
	wantRows(t, ctx, app, `SELECT id, body FROM docs ORDER BY id`, "2|two", "5|one")
	wantRows(t, ctx, root, `SELECT id FROM docs ORDER BY id`, "2", "5", "150")
	if got := strings.Join(queryStrings(t, ctx, root, `SELECT id FROM docs WHERE id = 1`), ","); got != "" {
		t.Fatalf("the old key still reads: %q", got)
	}
}

// TestUpdatePrimaryKeySealed (issue #322): a sealed value's associated
// data holds the row's key, so a moved row is sealed afresh for its new
// key — and, on a table sealed per tenant, under the new tenant's key
// when the tenant column changes.
func TestUpdatePrimaryKeySealed(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Second)
	defer cancel()
	c := startTenantCluster(t, ctx)
	conn := sealedConn(t, ctx, c, 0, nil)
	other := sealedConn(t, ctx, c, 1, nil)
	if errText, _ := c.op(0, "init"); errText != "" {
		t.Fatalf("init: %s", errText)
	}

	sealExec(t, ctx, conn, `CREATE TABLE notes (id INT8 PRIMARY KEY, body TEXT, tag TEXT UNIQUE) WITH (encryption_key = 'auto')`)
	sealExec(t, ctx, conn, `INSERT INTO notes VALUES (1, 'secret-one', 't1'), (2, 'secret-two', 't2'), (3, 'secret-three', NULL)`)
	sealExec(t, ctx, conn, `UPDATE notes SET id = id + 1`)
	sameStrings(t, "moved sealed rows", queryStrings(t, ctx, other, `SELECT id, body FROM notes ORDER BY id`),
		"2|secret-one", "3|secret-two", "4|secret-three")
	sameStrings(t, "through the unique index", queryStrings(t, ctx, other, `SELECT body FROM notes WHERE tag = 't2'`), "secret-two")
	primary, _ := tableKVs(t, ctx, c, tableDesc(t, ctx, c, "notes").ID)
	if len(primary) != 3 {
		t.Fatalf("%d primary rows, want 3", len(primary))
	}
	for _, kv := range primary {
		if strings.Contains(string(kv[1]), "secret") {
			t.Fatalf("a moved row's value is plaintext: %q", kv[1])
		}
	}

	sealExec(t, ctx, conn, `CREATE TABLE items (tenant_id UUID, id INT8, title TEXT, PRIMARY KEY (tenant_id, id)) WITH (encryption_key_by = 'tenant_id')`)
	a, b := tenantUUID(1), tenantUUID(2)
	sealExec(t, ctx, conn, `INSERT INTO items VALUES ('`+a+`', 1, 'title-a1'), ('`+a+`', 2, 'title-a2'), ('`+b+`', 1, 'title-b1')`)
	sealExec(t, ctx, conn, `UPDATE items SET id = id + 100 WHERE tenant_id = '`+a+`'`)
	// The tenant column changes: the row is sealed under b's key now.
	sealExec(t, ctx, conn, `UPDATE items SET tenant_id = '`+b+`' WHERE tenant_id = '`+a+`' AND id = 102`)
	sameStrings(t, "per-tenant rows after the moves", queryStrings(t, ctx, other, `SELECT tenant_id::TEXT, id, title FROM items ORDER BY tenant_id, id`),
		a+"|101|title-a1", b+"|1|title-b1", b+"|102|title-a2")
	d := tableDesc(t, ctx, c, "items")
	ids := sealedKeyIDs(t, ctx, c, d, "title-")
	ka, kb := tenantKeyOf(t, ids, d, a), tenantKeyOf(t, ids, d, b)
	if ka == kb {
		t.Fatalf("the two tenants' rows share key %d", ka)
	}
	encB, err := rowseal.TenantValue(d, stringDatum(b))
	if err != nil {
		t.Fatal(err)
	}
	if n := len(ids[string(encB)]); n != 2 {
		t.Fatalf("tenant b has %d rows, want 2", n)
	}
	c.checkConsistent("after moving sealed rows")
}
