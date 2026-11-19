package testcluster

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/kvclient"
	"github.com/sthorne/datax/pkg/server"
	"github.com/sthorne/datax/pkg/sql"
	"github.com/sthorne/datax/pkg/sql/catalog"
)

// Temporary and unlogged tables (gap PG-D-009 of
// docs/compatibility-gaps.md).

func tempSQLState(err error) string {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return pgErr.Code
	}
	return ""
}

func tempWantCode(t *testing.T, ctx context.Context, conn *pgx.Conn, q, code string) {
	t.Helper()
	_, err := conn.Exec(ctx, q)
	if got := tempSQLState(err); got != code {
		t.Fatalf("%s: got %v (SQLSTATE %q), want SQLSTATE %s", q, err, got, code)
	}
}

func tempInt(t *testing.T, ctx context.Context, conn *pgx.Conn, q string, args ...any) int64 {
	t.Helper()
	var n int64
	if err := conn.QueryRow(ctx, q, args...).Scan(&n); err != nil {
		t.Fatalf("%s: %v", q, err)
	}
	return n
}

func tempString(t *testing.T, ctx context.Context, conn *pgx.Conn, q string, args ...any) string {
	t.Helper()
	var s string
	if err := conn.QueryRow(ctx, q, args...).Scan(&s); err != nil {
		t.Fatalf("%s: %v", q, err)
	}
	return s
}

// tempDescriptors counts the temporary table descriptors named name in
// the catalog, whatever session owns them.
func tempDescriptors(t *testing.T, ctx context.Context, db *kvclient.DB, name string) int {
	t.Helper()
	n := 0
	if err := db.RunTxn(ctx, "test-temp-descs", func(ctx context.Context, txn *kvclient.Txn) error {
		n = 0
		start, end := keys.TableDescSpan()
		rows, err := txn.Scan(ctx, start, end, 0)
		if err != nil {
			return err
		}
		for _, kv := range rows {
			var d catalog.TableDescriptor
			if json.Unmarshal(kv.Value, &d) == nil && d.Temp != nil && d.Name == name {
				n++
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	return n
}

// TestTempTables: two sessions, on two nodes, each with a temporary
// table of the same name; shadowing of an ordinary table; isolation
// (42P01 for the other session, by any name, and no catalog rows);
// cleanup at the end of the session and by DISCARD TEMP.
func TestTempTables(t *testing.T) {
	tc := Start(t, 2)
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()
	a, b := pgConn(t, ctx, tc, 0), pgConn(t, ctx, tc, 1)
	db := tempString(t, ctx, a, `SELECT current_database()`)

	// Each session's own table of one name.
	mustExec(t, ctx, a, `CREATE TEMP TABLE t (id INT PRIMARY KEY, v TEXT)`)
	mustExec(t, ctx, b, `CREATE TEMPORARY TABLE t (id INT PRIMARY KEY, v TEXT, w INT)`)
	mustExec(t, ctx, a, `INSERT INTO t VALUES (1, 'a1'), (2, 'a2')`)
	mustExec(t, ctx, b, `INSERT INTO t VALUES (1, 'b1', 10)`)
	if n := tempInt(t, ctx, a, `SELECT count(*) FROM t`); n != 2 {
		t.Fatalf("session a sees %d rows in its t, want 2", n)
	}
	if v := tempString(t, ctx, b, `SELECT v FROM pg_temp.t WHERE id = 1`); v != "b1" {
		t.Fatalf("session b reads %q from pg_temp.t, want its own b1", v)
	}
	if v := tempString(t, ctx, a, `SELECT v FROM t WHERE id = 1`); v != "a1" {
		t.Fatalf("session a reads %q, want a1", v)
	}
	// Updates and deletes stay in the session's own table.
	mustExec(t, ctx, b, `UPDATE t SET v = 'b1x' WHERE id = 1`)
	mustExec(t, ctx, a, `DELETE FROM t WHERE id = 2`)
	if v := tempString(t, ctx, a, `SELECT string_agg(v, ',') FROM t`); v != "a1" {
		t.Fatalf("session a's t holds %q after the other session's update", v)
	}

	// pg_temp qualification in DML, joins with ordinary tables, COPY.
	mustExec(t, ctx, a, `INSERT INTO pg_temp.t VALUES (3, 'a3')`)
	mustExec(t, ctx, a, `UPDATE pg_temp.t SET v = 'a3x' WHERE id = 3`)
	mustExec(t, ctx, a, `CREATE TABLE names (id INT PRIMARY KEY, label TEXT)`)
	mustExec(t, ctx, a, `INSERT INTO names VALUES (1, 'one'), (3, 'three')`)
	if s := tempString(t, ctx, a, `SELECT string_agg(n.label || '=' || t.v, ',' ORDER BY t.id) FROM t JOIN names n ON n.id = t.id`); s != "one=a1,three=a3x" {
		t.Fatalf("join of a temporary and an ordinary table: %q", s)
	}
	if _, err := a.CopyFrom(ctx, pgx.Identifier{"t"}, []string{"id", "v"}, pgx.CopyFromRows([][]any{{int32(4), "a4"}})); err != nil {
		t.Fatalf("COPY into a temporary table: %v", err)
	}
	if n := tempInt(t, ctx, a, `SELECT count(*) FROM t`); n != 3 {
		t.Fatalf("session a's t holds %d rows after COPY, want 3", n)
	}
	mustExec(t, ctx, a, `DELETE FROM pg_temp.t WHERE id IN (3, 4)`)

	// A table only one session has: every other session gets 42P01,
	// however it names it, and sees no catalog row for it.
	mustExec(t, ctx, a, `CREATE TEMP TABLE only_a (k INT PRIMARY KEY)`)
	mustExec(t, ctx, a, `INSERT INTO only_a VALUES (7)`)
	for _, q := range []string{
		`SELECT * FROM only_a`,
		`SELECT * FROM pg_temp.only_a`,
		`INSERT INTO only_a VALUES (8)`,
		`DELETE FROM only_a`,
		`DROP TABLE only_a`,
		`SELECT 'only_a'::regclass`,
		`SELECT * FROM ` + db + `.only_a`,
	} {
		tempWantCode(t, ctx, b, q, "42P01")
	}
	if n := tempInt(t, ctx, b, `SELECT count(*) FROM pg_class WHERE relname = 'only_a'`); n != 0 {
		t.Fatalf("another session sees %d pg_class rows for a temporary table", n)
	}
	if n := tempInt(t, ctx, b, `SELECT count(*) FROM information_schema.tables WHERE table_name = 'only_a'`); n != 0 {
		t.Fatalf("another session sees %d information_schema rows for a temporary table", n)
	}
	if p := tempString(t, ctx, a, `SELECT relpersistence FROM pg_class WHERE relname = 'only_a'`); p != "t" {
		t.Fatalf("relpersistence of a temporary table = %q, want t", p)
	}
	if k := tempString(t, ctx, a, `SELECT table_type FROM information_schema.tables WHERE table_name = 'only_a'`); k != "LOCAL TEMPORARY" {
		t.Fatalf("table_type of a temporary table = %q, want LOCAL TEMPORARY", k)
	}
	if n := tempInt(t, ctx, a, `SELECT count(*) FROM pg_class WHERE relname = 't' AND relkind = 'r'`); n != 1 {
		t.Fatalf("session a sees %d pg_class rows named t, want its own only", n)
	}

	// Shadowing: a temporary table hides an ordinary one of its name from
	// its session only; the database-qualified name still reaches the
	// ordinary table, and dropping the temporary one uncovers it.
	mustExec(t, ctx, a, `CREATE TABLE shadow (id INT PRIMARY KEY, src TEXT)`)
	mustExec(t, ctx, a, `INSERT INTO shadow VALUES (1, 'permanent')`)
	mustExec(t, ctx, a, `CREATE TEMP TABLE shadow (id INT PRIMARY KEY, src TEXT)`)
	mustExec(t, ctx, a, `INSERT INTO shadow VALUES (1, 'temporary')`)
	if s := tempString(t, ctx, a, `SELECT src FROM shadow`); s != "temporary" {
		t.Fatalf("the owning session reads %q, want the temporary table", s)
	}
	if s := tempString(t, ctx, a, `SELECT src FROM `+db+`.shadow`); s != "permanent" {
		t.Fatalf("%s.shadow reads %q, want the ordinary table", db, s)
	}
	if s := tempString(t, ctx, b, `SELECT src FROM shadow`); s != "permanent" {
		t.Fatalf("another session reads %q, want the ordinary table", s)
	}
	mustExec(t, ctx, a, `DROP TABLE shadow`)
	if s := tempString(t, ctx, a, `SELECT src FROM shadow`); s != "permanent" {
		t.Fatalf("after dropping the temporary table, %q, want the ordinary table", s)
	}
	mustExec(t, ctx, a, `DROP TABLE shadow`)
	tempWantCode(t, ctx, a, `SELECT * FROM shadow`, "42P01")

	// IF NOT EXISTS, pg_temp creation and DROP TABLE IF EXISTS.
	mustExec(t, ctx, a, `CREATE TEMP TABLE IF NOT EXISTS t (x INT)`)
	tempWantCode(t, ctx, a, `CREATE TEMP TABLE t (x INT)`, "42P07")
	mustExec(t, ctx, a, `CREATE TABLE pg_temp.pt (x INT)`)
	if p := tempString(t, ctx, a, `SELECT relpersistence FROM pg_class WHERE relname = 'pt'`); p != "t" {
		t.Fatalf("CREATE TABLE pg_temp.pt made relpersistence %q, want t", p)
	}
	tempWantCode(t, ctx, b, `SELECT * FROM pt`, "42P01")
	mustExec(t, ctx, a, `DROP TABLE pg_temp.pt`)
	mustExec(t, ctx, a, `DROP TABLE IF EXISTS pg_temp.pt`)
	tempWantCode(t, ctx, a, `DROP TABLE pg_temp.pt`, "42P01")

	// The end of the session drops its temporary tables; the other
	// session's table of the same name is untouched.
	if n := tempDescriptors(t, ctx, tc.Node(0).DB(), "only_a"); n != 1 {
		t.Fatalf("%d descriptors for only_a before the session ends", n)
	}
	if err := a.Close(ctx); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(30 * time.Second)
	for tempDescriptors(t, ctx, tc.Node(0).DB(), "only_a") != 0 {
		if time.Now().After(deadline) {
			t.Fatal("the ended session's temporary table was not dropped")
		}
		time.Sleep(50 * time.Millisecond)
	}
	if n := tempDescriptors(t, ctx, tc.Node(0).DB(), "t"); n != 1 {
		t.Fatalf("%d temporary tables named t after one session ended, want the other session's", n)
	}
	if v := tempString(t, ctx, b, `SELECT v FROM t WHERE id = 1`); v != "b1x" {
		t.Fatalf("the remaining session reads %q from its t", v)
	}
	c := pgConn(t, ctx, tc, 0)
	tempWantCode(t, ctx, c, `SELECT * FROM only_a`, "42P01")

	// DISCARD TEMP drops the session's temporary tables.
	mustExec(t, ctx, b, `DISCARD TEMP`)
	tempWantCode(t, ctx, b, `SELECT * FROM t`, "42P01")
	if n := tempDescriptors(t, ctx, tc.Node(0).DB(), "t"); n != 0 {
		t.Fatalf("%d temporary tables named t after DISCARD TEMP", n)
	}
	mustExec(t, ctx, b, `CREATE TEMP TABLE again (x INT)`)
	mustExec(t, ctx, b, `DISCARD ALL`)
	tempWantCode(t, ctx, b, `SELECT * FROM again`, "42P01")
}

// TestTempTableOnCommit: ON COMMIT PRESERVE ROWS, DELETE ROWS and DROP,
// in and out of transaction blocks; ON COMMIT on an ordinary table.
func TestTempTableOnCommit(t *testing.T) {
	tc := Start(t, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	a := pgConn(t, ctx, tc, 0)

	tempWantCode(t, ctx, a, `CREATE TABLE oc (x INT) ON COMMIT DELETE ROWS`, "42P16")
	tempWantCode(t, ctx, a, `CREATE TABLE oc (x INT) ON COMMIT PRESERVE ROWS`, "42P16")
	tempWantCode(t, ctx, a, `CREATE TABLE oc ON COMMIT DROP AS SELECT 1 AS x`, "42P16")
	tempWantCode(t, ctx, a, `CREATE TEMP TABLE oc (x INT) ON COMMIT EXPLODE`, "42601")

	mustExec(t, ctx, a, `CREATE TEMP TABLE keep (x INT) ON COMMIT PRESERVE ROWS`)
	mustExec(t, ctx, a, `INSERT INTO keep VALUES (1), (2)`)
	if n := tempInt(t, ctx, a, `SELECT count(*) FROM keep`); n != 2 {
		t.Fatalf("ON COMMIT PRESERVE ROWS kept %d rows, want 2", n)
	}

	// DELETE ROWS: empty after every commit, full within the block.
	mustExec(t, ctx, a, `CREATE TEMP TABLE scratch (x INT PRIMARY KEY, y TEXT) ON COMMIT DELETE ROWS`)
	mustExec(t, ctx, a, `CREATE INDEX scratch_y ON scratch (y)`)
	mustExec(t, ctx, a, `INSERT INTO scratch VALUES (1, 'one')`)
	if n := tempInt(t, ctx, a, `SELECT count(*) FROM scratch`); n != 0 {
		t.Fatalf("ON COMMIT DELETE ROWS: %d rows after an implicit commit, want 0", n)
	}
	tx, err := a.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, `INSERT INTO scratch VALUES (1, 'one'), (2, 'two')`); err != nil {
		t.Fatal(err)
	}
	var n int64
	if err := tx.QueryRow(ctx, `SELECT count(*) FROM scratch WHERE y = 'two'`).Scan(&n); err != nil || n != 1 {
		t.Fatalf("within the block: %d rows (err %v), want 1", n, err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if n := tempInt(t, ctx, a, `SELECT count(*) FROM scratch`); n != 0 {
		t.Fatalf("ON COMMIT DELETE ROWS: %d rows after COMMIT, want 0", n)
	}
	// The index went with the rows: the same keys insert again.
	mustExec(t, ctx, a, `BEGIN`)
	mustExec(t, ctx, a, `INSERT INTO scratch VALUES (1, 'one'), (2, 'two')`)
	mustExec(t, ctx, a, `COMMIT`)

	// DROP: at the end of the block, or of the statement's own
	// transaction outside one; a rolled-back block leaves nothing.
	mustExec(t, ctx, a, `BEGIN`)
	mustExec(t, ctx, a, `CREATE TEMP TABLE gone (x INT) ON COMMIT DROP`)
	mustExec(t, ctx, a, `INSERT INTO gone VALUES (1)`)
	if n := tempInt(t, ctx, a, `SELECT count(*) FROM gone`); n != 1 {
		t.Fatalf("ON COMMIT DROP table within its block: %d rows", n)
	}
	mustExec(t, ctx, a, `COMMIT`)
	tempWantCode(t, ctx, a, `SELECT * FROM gone`, "42P01")
	mustExec(t, ctx, a, `CREATE TEMP TABLE gone (x INT) ON COMMIT DROP`)
	tempWantCode(t, ctx, a, `SELECT * FROM gone`, "42P01")
	mustExec(t, ctx, a, `BEGIN`)
	mustExec(t, ctx, a, `CREATE TEMP TABLE gone (x INT) ON COMMIT DROP`)
	mustExec(t, ctx, a, `ROLLBACK`)
	tempWantCode(t, ctx, a, `SELECT * FROM gone`, "42P01")
	if n := tempDescriptors(t, ctx, tc.Node(0).DB(), "gone"); n != 0 {
		t.Fatalf("%d ON COMMIT DROP descriptors left behind", n)
	}

	// CREATE TEMP TABLE ... AS with the ON COMMIT actions.
	mustExec(t, ctx, a, `CREATE TEMP TABLE ctas_del ON COMMIT DELETE ROWS AS SELECT 1 AS x`)
	if n := tempInt(t, ctx, a, `SELECT count(*) FROM ctas_del`); n != 0 {
		t.Fatalf("CREATE TEMP TABLE ... ON COMMIT DELETE ROWS AS kept %d rows", n)
	}
	mustExec(t, ctx, a, `CREATE TEMP TABLE ctas_drop ON COMMIT DROP AS SELECT 1 AS x`)
	tempWantCode(t, ctx, a, `SELECT * FROM ctas_drop`, "42P01")
	if n := tempInt(t, ctx, a, `SELECT count(*) FROM keep`); n != 2 {
		t.Fatalf("an ON COMMIT action of another table emptied the PRESERVE ROWS table: %d rows", n)
	}
}

// TestTempTableDDL: CREATE TEMP TABLE ... AS, indexes, renames, foreign
// keys between temporary tables, and what a temporary table refuses.
func TestTempTableDDL(t *testing.T) {
	tc := Start(t, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	a, b := pgConn(t, ctx, tc, 0), pgConn(t, ctx, tc, 0)

	mustExec(t, ctx, a, `CREATE TABLE src (id INT PRIMARY KEY, name TEXT)`)
	mustExec(t, ctx, a, `INSERT INTO src VALUES (1, 'x'), (2, 'y'), (3, 'z')`)
	mustExec(t, ctx, a, `CREATE TEMP TABLE copy AS SELECT * FROM src WHERE id > 1`)
	if n := tempInt(t, ctx, a, `SELECT count(*) FROM copy`); n != 2 {
		t.Fatalf("CREATE TEMP TABLE AS copied %d rows, want 2", n)
	}
	mustExec(t, ctx, a, `CREATE TEMP TABLE IF NOT EXISTS copy AS SELECT * FROM src`)
	if n := tempInt(t, ctx, a, `SELECT count(*) FROM copy`); n != 2 {
		t.Fatalf("IF NOT EXISTS over an existing temporary table changed it: %d rows", n)
	}
	tempWantCode(t, ctx, b, `SELECT * FROM copy`, "42P01")
	// A temporary table of an ordinary table's name, by CREATE TEMP
	// TABLE IF NOT EXISTS: only a temporary table is in its way.
	mustExec(t, ctx, a, `CREATE TEMP TABLE IF NOT EXISTS src (id INT PRIMARY KEY, name TEXT)`)
	if n := tempInt(t, ctx, a, `SELECT count(*) FROM src`); n != 0 {
		t.Fatalf("CREATE TEMP TABLE IF NOT EXISTS src did not shadow the ordinary table (%d rows)", n)
	}
	mustExec(t, ctx, a, `DROP TABLE src`)

	// Indexes, unique ones included, and DROP INDEX.
	mustExec(t, ctx, a, `CREATE UNIQUE INDEX copy_name ON copy (name)`)
	tempWantCode(t, ctx, a, `INSERT INTO copy VALUES (9, 'y')`, "23505")
	if n := tempInt(t, ctx, a, `SELECT id FROM copy WHERE name = 'z'`); n != 3 {
		t.Fatalf("index lookup on a temporary table: id %d", n)
	}
	if p := tempString(t, ctx, a, `SELECT relpersistence FROM pg_class WHERE relname = 'copy_name'`); p != "t" {
		t.Fatalf("relpersistence of a temporary table's index = %q, want t", p)
	}
	mustExec(t, ctx, a, `DROP INDEX copy_name`)
	mustExec(t, ctx, a, `INSERT INTO copy VALUES (9, 'y')`)
	mustExec(t, ctx, a, `BEGIN`)
	mustExec(t, ctx, a, `CREATE TEMP TABLE blk (x INT PRIMARY KEY, y INT)`)
	mustExec(t, ctx, a, `CREATE INDEX blk_y ON blk (y)`)
	mustExec(t, ctx, a, `COMMIT`)

	// Renames keep the table the session's.
	mustExec(t, ctx, a, `ALTER TABLE copy RENAME TO copy2`)
	if n := tempInt(t, ctx, a, `SELECT count(*) FROM copy2`); n != 3 {
		t.Fatalf("renamed temporary table: %d rows", n)
	}
	tempWantCode(t, ctx, b, `SELECT * FROM copy2`, "42P01")
	mustExec(t, ctx, a, `ALTER TABLE copy2 ADD COLUMN extra INT`)
	mustExec(t, ctx, a, `TRUNCATE copy2`)
	if n := tempInt(t, ctx, a, `SELECT count(*) FROM copy2`); n != 0 {
		t.Fatalf("TRUNCATE of a temporary table left %d rows", n)
	}

	// Foreign keys: between temporary tables only.
	mustExec(t, ctx, a, `CREATE TABLE perm (id INT PRIMARY KEY)`)
	mustExec(t, ctx, a, `CREATE TEMP TABLE tparent (id INT PRIMARY KEY)`)
	mustExec(t, ctx, a, `CREATE TEMP TABLE tchild (id INT PRIMARY KEY, p INT REFERENCES tparent (id))`)
	tempWantCode(t, ctx, a, `INSERT INTO tchild VALUES (1, 5)`, "23503")
	tempWantCode(t, ctx, a, `CREATE TEMP TABLE bad (p INT REFERENCES perm (id))`, "42P16")
	tempWantCode(t, ctx, a, `CREATE TABLE bad (p INT REFERENCES tparent (id))`, "42P16")
	tempWantCode(t, ctx, a, `DROP TABLE tparent`, "2BP01")
	mustExec(t, ctx, a, `DROP TABLE tparent CASCADE`)
	mustExec(t, ctx, a, `INSERT INTO tchild VALUES (1, 5)`)

	// What a temporary table refuses.
	tempWantCode(t, ctx, a, `CREATE VIEW v AS SELECT * FROM tchild`, "0A000")
	tempWantCode(t, ctx, a, `CREATE TEMP TABLE ser (id SERIAL PRIMARY KEY)`, "0A000")
	tempWantCode(t, ctx, a, `CREATE TEMP TABLE opt (x INT) WITH (gc_ttl = '1h')`, "0A000")
	tempWantCode(t, ctx, a, `CREATE TEMP TABLE `+tempString(t, ctx, a, `SELECT current_database()`)+`.q (x INT)`, "42P16")
	tempWantCode(t, ctx, a, `ALTER TABLE tchild SET LOGGED`, "42P16")
	tempWantCode(t, ctx, a, `ALTER TABLE tchild SET UNLOGGED`, "42P16")

	// UNLOGGED: an ordinary, durable table that pg_class reports as
	// unlogged; SET LOGGED / UNLOGGED flip only that.
	mustExec(t, ctx, a, `CREATE UNLOGGED TABLE u (x INT PRIMARY KEY)`)
	mustExec(t, ctx, a, `INSERT INTO u VALUES (1)`)
	if n := tempInt(t, ctx, b, `SELECT count(*) FROM u`); n != 1 {
		t.Fatalf("an unlogged table is shared: another session sees %d rows", n)
	}
	persistence := func() string {
		return tempString(t, ctx, b, `SELECT relpersistence FROM pg_class WHERE relname = 'u'`)
	}
	if p := persistence(); p != "u" {
		t.Fatalf("relpersistence of an unlogged table = %q, want u", p)
	}
	mustExec(t, ctx, a, `ALTER TABLE u SET LOGGED`)
	if p := persistence(); p != "p" {
		t.Fatalf("after SET LOGGED relpersistence = %q, want p", p)
	}
	mustExec(t, ctx, a, `ALTER TABLE u SET UNLOGGED`)
	if p := persistence(); p != "u" {
		t.Fatalf("after SET UNLOGGED relpersistence = %q, want u", p)
	}
	tempWantCode(t, ctx, a, `CREATE UNLOGGED TABLE pg_temp.uu (x INT)`, "42P16")
	if p := tempString(t, ctx, b, `SELECT relpersistence FROM pg_class WHERE relname = 'perm'`); p != "p" {
		t.Fatalf("relpersistence of an ordinary table = %q, want p", p)
	}
}

// TestTempTableOrphanSweep: temporary tables a session could not drop —
// its drop failed, or its node stopped — are dropped by the sweep, once
// the session has ended or its node's liveness has lapsed.
func TestTempTableOrphanSweep(t *testing.T) {
	tc := StartWithOptions(t, 3, func(c *server.Config) {
		c.TempTableSweepInterval = -1 // the test runs the passes
		c.DeadNodeThreshold = 3 * time.Second
	})
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()
	sql.TestingKeepTempTablesOnClose.Store(true)
	defer sql.TestingKeepTempTablesOnClose.Store(false)

	// A session of node 0 that ended without dropping its table, and a
	// live session with a table of its own.
	live := pgConn(t, ctx, tc, 0)
	mustExec(t, ctx, live, `CREATE TEMP TABLE kept (x INT)`)
	mustExec(t, ctx, live, `INSERT INTO kept VALUES (1)`)
	ended := pgConn(t, ctx, tc, 0)
	mustExec(t, ctx, ended, `CREATE TEMP TABLE orphan (x INT)`)
	mustExec(t, ctx, ended, `INSERT INTO orphan VALUES (1)`)
	if err := ended.Close(ctx); err != nil {
		t.Fatal(err)
	}
	// A session of node 2, whose node then stops.
	remote := pgConn(t, ctx, tc, 2)
	mustExec(t, ctx, remote, `CREATE TEMP TABLE stranded (x INT)`)
	mustExec(t, ctx, remote, `INSERT INTO stranded VALUES (1)`)

	db := tc.Node(0).DB()
	// The ended session's table is left behind until node 0 sweeps.
	deadline := time.Now().Add(30 * time.Second)
	for tempDescriptors(t, ctx, db, "orphan") != 0 {
		if time.Now().After(deadline) {
			t.Fatal("the sweep did not drop the ended session's temporary table")
		}
		if _, err := tc.Node(0).SweepTempTablesOnce(ctx); err != nil {
			t.Fatal(err)
		}
		time.Sleep(50 * time.Millisecond)
	}
	if n := tempDescriptors(t, ctx, db, "kept"); n != 1 {
		t.Fatalf("the sweep dropped a live session's temporary table (%d left)", n)
	}
	if n := tempInt(t, ctx, live, `SELECT count(*) FROM kept`); n != 1 {
		t.Fatalf("the live session's table holds %d rows after the sweep", n)
	}
	// Node 2's table survives a sweep while node 2 is up.
	if _, err := tc.Node(1).SweepTempTablesOnce(ctx); err != nil {
		t.Fatal(err)
	}
	if n := tempDescriptors(t, ctx, db, "stranded"); n != 1 {
		t.Fatalf("a live node's session lost its temporary table (%d left)", n)
	}
	tc.StopNode(2)
	deadline = time.Now().Add(60 * time.Second)
	for tempDescriptors(t, ctx, db, "stranded") != 0 {
		if time.Now().After(deadline) {
			t.Fatal("the sweep did not drop the stopped node's temporary table")
		}
		if _, err := tc.Node(1).SweepTempTablesOnce(ctx); err != nil {
			t.Logf("sweep: %v", err)
		}
		time.Sleep(200 * time.Millisecond)
	}
	if n := tempDescriptors(t, ctx, db, "kept"); n != 1 {
		t.Fatalf("the sweep dropped a live session's temporary table (%d left)", n)
	}
}

// TestTempTableSweepSparesLiveNodes: a dead-node threshold shorter than
// a node's liveness beat cannot make the sweep drop the temporary tables
// of a live node's session. A live node's registry row is up to a beat
// old, so another node's sweep judges on fresh rows and never on less
// than two beats; the drop could not be undone.
func TestTempTableSweepSparesLiveNodes(t *testing.T) {
	tc := StartWithOptions(t, 3, func(c *server.Config) {
		c.TempTableSweepInterval = -1 // the test runs the passes
		c.DeadNodeThreshold = time.Millisecond
	})
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	live := pgConn(t, ctx, tc, 0)
	mustExec(t, ctx, live, `CREATE TEMP TABLE kept (x INT)`)
	mustExec(t, ctx, live, `INSERT INTO kept VALUES (1)`)
	db := tc.Node(0).DB()
	// Sweep from the other nodes for longer than two beats, so that every
	// phase of node 0's beat is seen.
	deadline := time.Now().Add(7 * time.Second)
	for time.Now().Before(deadline) {
		for _, i := range []int{1, 2} {
			if n, err := tc.Node(i).SweepTempTablesOnce(ctx); err != nil || n != 0 {
				t.Fatalf("node %d swept %d tables: %v", i, n, err)
			}
		}
		time.Sleep(250 * time.Millisecond)
	}
	if n := tempDescriptors(t, ctx, db, "kept"); n != 1 {
		t.Fatalf("the sweep dropped a live node's temporary table (%d left)", n)
	}
	if n := tempInt(t, ctx, live, `SELECT count(*) FROM kept`); n != 1 {
		t.Fatalf("the live session's table holds %d rows after the sweeps", n)
	}
}
