package testcluster

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/sthorne/datax/pkg/server"
	"github.com/sthorne/datax/pkg/sql"
	"github.com/sthorne/datax/pkg/sql/catalog"
	"github.com/sthorne/datax/pkg/version"
)

// wantState runs q and fails unless it fails with SQLSTATE code.
func wantSQLState(t *testing.T, ctx context.Context, conn *pgx.Conn, code, q string, args ...any) {
	t.Helper()
	_, err := conn.Exec(ctx, q, args...)
	if got := sqlState(err); got != code {
		t.Fatalf("%s: got %v (%q), want SQLSTATE %s", q, err, got, code)
	}
}

// wantRows runs q and compares its rows, rendered by queryStrings.
func wantRows(t *testing.T, ctx context.Context, conn *pgx.Conn, q string, want ...string) {
	t.Helper()
	got := queryStrings(t, ctx, conn, q)
	if strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Fatalf("%s:\n got %q\nwant %q", q, got, want)
	}
}

func wantPlan(t *testing.T, ctx context.Context, conn *pgx.Conn, q, want string) {
	t.Helper()
	plan := strings.Join(queryStrings(t, ctx, conn, "EXPLAIN "+q), "\n")
	if !strings.Contains(plan, want) {
		t.Fatalf("EXPLAIN %s:\n%s\nwant it to contain %q", q, plan, want)
	}
}

// TestUniqueIndexNullsDistinct (issue #322): a unique index takes any
// number of rows with a NULL in an indexed column, as PostgreSQL's
// default NULLS DISTINCT does, on every path that writes or reads one.
func TestUniqueIndexNullsDistinct(t *testing.T) {
	tc := Start(t, 3)
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()
	conn := pgConn(t, ctx, tc, 0)

	// The report.
	mustExec(t, ctx, conn, `CREATE TABLE e (t TEXT NOT NULL, i TEXT NOT NULL, link TEXT, PRIMARY KEY (t, i))`)
	mustExec(t, ctx, conn, `CREATE UNIQUE INDEX e_link ON e (link)`)
	mustExec(t, ctx, conn, `INSERT INTO e (t, i) VALUES ('a', '1')`)
	mustExec(t, ctx, conn, `INSERT INTO e (t, i) VALUES ('a', '2'), ('a', '3')`)
	mustExec(t, ctx, conn, `INSERT INTO e VALUES ('b', '1', 'x'), ('b', '2', NULL)`)
	wantSQLState(t, ctx, conn, "23505", `INSERT INTO e VALUES ('b', '3', 'x')`)
	wantRows(t, ctx, conn, `SELECT t, i FROM e WHERE link IS NULL ORDER BY t, i`, "a|1", "a|2", "a|3", "b|2")
	wantRows(t, ctx, conn, `SELECT t, i FROM e WHERE link = 'x'`, "b|1")
	wantPlan(t, ctx, conn, `SELECT t, i FROM e WHERE link = 'x'`, `point lookup via unique index "e_link"`)
	// A range over the index sees the values only; the planner does not
	// take a unique index for every row when NULLs may match.
	wantRows(t, ctx, conn, `SELECT count(*) FROM e WHERE link >= 'a'`, "1")
	wantRows(t, ctx, conn, `SELECT count(*) FROM e WHERE link IS NULL OR link = 'x'`, "5")

	// UPDATE to and from NULL.
	mustExec(t, ctx, conn, `UPDATE e SET link = NULL WHERE t = 'b' AND i = '1'`)
	mustExec(t, ctx, conn, `INSERT INTO e VALUES ('c', '1', 'x')`) // 'x' is free again
	wantSQLState(t, ctx, conn, "23505", `UPDATE e SET link = 'x' WHERE t = 'a' AND i = '1'`)
	mustExec(t, ctx, conn, `UPDATE e SET link = 'y' WHERE t = 'a' AND i = '1'`)
	wantRows(t, ctx, conn, `SELECT t, i FROM e WHERE link = 'y'`, "a|1")
	wantSQLState(t, ctx, conn, "23505", `UPDATE e SET link = 'z' WHERE link IS NULL`)
	mustExec(t, ctx, conn, `UPDATE e SET link = NULL`)
	wantRows(t, ctx, conn, `SELECT count(*) FROM e WHERE link IS NULL`, "6")
	wantRows(t, ctx, conn, `SELECT count(*) FROM e WHERE link = 'x'`, "0")
	mustExec(t, ctx, conn, `DELETE FROM e WHERE t = 'a'`)
	wantRows(t, ctx, conn, `SELECT t, i FROM e ORDER BY t, i`, "b|1", "b|2", "c|1")

	// ON CONFLICT on the unique column: a NULL conflicts with nothing,
	// so every such row is inserted; a value still arbitrates.
	mustExec(t, ctx, conn, `INSERT INTO e VALUES ('d', '1', NULL) ON CONFLICT (link) DO NOTHING`)
	mustExec(t, ctx, conn, `INSERT INTO e VALUES ('d', '2', NULL) ON CONFLICT (link) DO UPDATE SET link = 'never'`)
	mustExec(t, ctx, conn, `INSERT INTO e VALUES ('d', '3', 'v') ON CONFLICT (link) DO NOTHING`)
	mustExec(t, ctx, conn, `INSERT INTO e VALUES ('d', '4', 'v') ON CONFLICT (link) DO NOTHING`)
	mustExec(t, ctx, conn, `INSERT INTO e VALUES ('d', '5', 'v') ON CONFLICT (link) DO UPDATE SET link = 'w'`)
	wantRows(t, ctx, conn, `SELECT t, i, link FROM e WHERE t = 'd' ORDER BY i`, "d|1|<nil>", "d|2|<nil>", "d|3|w")
	mustExec(t, ctx, conn, `UPSERT INTO e VALUES ('d', '1', NULL), ('d', '9', NULL)`)
	wantRows(t, ctx, conn, `SELECT count(*) FROM e WHERE link IS NULL`, "6")

	// A unique index over two columns: a NULL in either is distinct, and
	// a prefix lookup still finds the rows the index does not hold.
	mustExec(t, ctx, conn, `CREATE TABLE m (id INT PRIMARY KEY, a INT, b INT, UNIQUE (a, b))`)
	mustExec(t, ctx, conn, `INSERT INTO m VALUES (1, 1, NULL), (2, 1, NULL), (3, 1, 1), (4, NULL, 1), (5, NULL, 1)`)
	wantSQLState(t, ctx, conn, "23505", `INSERT INTO m VALUES (6, 1, 1)`)
	wantRows(t, ctx, conn, `SELECT id FROM m WHERE a = 1 ORDER BY id`, "1", "2", "3")
	wantRows(t, ctx, conn, `SELECT id FROM m WHERE b = 1 ORDER BY id`, "3", "4", "5")
	wantRows(t, ctx, conn, `SELECT id FROM m WHERE a = 1 AND b = 1`, "3")

	// CREATE UNIQUE INDEX over existing NULLs: the backfill skips them,
	// and still finds a duplicate value.
	mustExec(t, ctx, conn, `CREATE TABLE f (id INT PRIMARY KEY, code TEXT)`)
	mustExec(t, ctx, conn, `INSERT INTO f VALUES (1, NULL), (2, NULL), (3, 'a'), (4, 'b')`)
	mustExec(t, ctx, conn, `CREATE UNIQUE INDEX f_code ON f (code)`)
	mustExec(t, ctx, conn, `INSERT INTO f VALUES (5, NULL)`)
	wantSQLState(t, ctx, conn, "23505", `INSERT INTO f VALUES (6, 'a')`)
	wantRows(t, ctx, conn, `SELECT id FROM f WHERE code = 'b'`, "4")
	mustExec(t, ctx, conn, `CREATE TABLE g (id INT PRIMARY KEY, code TEXT)`)
	mustExec(t, ctx, conn, `INSERT INTO g VALUES (1, NULL), (2, 'a'), (3, 'a')`)
	wantSQLState(t, ctx, conn, "23505", `CREATE UNIQUE INDEX g_code ON g (code)`)
	// ADD CONSTRAINT UNIQUE: the same backfill.
	mustExec(t, ctx, conn, `DELETE FROM g WHERE id = 3`)
	mustExec(t, ctx, conn, `INSERT INTO g VALUES (4, NULL)`)
	mustExec(t, ctx, conn, `ALTER TABLE g ADD CONSTRAINT g_code_key UNIQUE (code)`)
	mustExec(t, ctx, conn, `INSERT INTO g VALUES (5, NULL)`)
	wantSQLState(t, ctx, conn, "23505", `INSERT INTO g VALUES (6, 'a')`)

	// A foreign key referencing the unique column: a parent row whose
	// key is NULL is referenced by nothing, so it is deleted or updated
	// freely; a NULL in the child is not checked.
	mustExec(t, ctx, conn, `CREATE TABLE parent (id INT PRIMARY KEY, code TEXT UNIQUE)`)
	mustExec(t, ctx, conn, `CREATE TABLE child (id INT PRIMARY KEY, code TEXT REFERENCES parent (code) ON DELETE CASCADE ON UPDATE CASCADE)`)
	mustExec(t, ctx, conn, `INSERT INTO parent VALUES (1, 'p'), (2, NULL), (3, NULL)`)
	mustExec(t, ctx, conn, `INSERT INTO child VALUES (10, 'p'), (11, NULL), (12, NULL)`)
	wantSQLState(t, ctx, conn, "23503", `INSERT INTO child VALUES (13, 'q')`)
	mustExec(t, ctx, conn, `DELETE FROM parent WHERE id = 2`)
	mustExec(t, ctx, conn, `UPDATE parent SET code = 'r' WHERE id = 3`)
	mustExec(t, ctx, conn, `UPDATE parent SET code = 'p2' WHERE id = 1`)
	wantRows(t, ctx, conn, `SELECT id, code FROM child ORDER BY id`, "10|p2", "11|<nil>", "12|<nil>")
	mustExec(t, ctx, conn, `UPDATE parent SET code = NULL WHERE id = 3`)
	wantRows(t, ctx, conn, `SELECT count(*) FROM parent WHERE code IS NULL`, "1")

	// COPY goes through the insert path.
	if _, err := conn.CopyFrom(ctx, pgx.Identifier{"f"}, []string{"id", "code"}, pgx.CopyFromRows([][]any{{20, nil}, {21, nil}})); err != nil {
		t.Fatalf("COPY: %v", err)
	}
	wantRows(t, ctx, conn, `SELECT count(*) FROM f WHERE code IS NULL`, "5")

	// A backup and a table restore carry the rows, and the restored
	// index goes on taking NULLs and refusing duplicate values.
	dir := t.TempDir()
	if _, err := tc.Nodes[0].RunBackup(ctx, dir, "", false, false); err != nil {
		t.Fatalf("backup: %v", err)
	}
	if _, err := tc.Nodes[1].RunRestoreOpts(ctx, server.RestoreOptions{
		Srcs: []string{dir}, Tables: []server.RestoreTable{{Name: "f", Into: "f_back"}},
	}); err != nil {
		t.Fatalf("table restore: %v", err)
	}
	wantRows(t, ctx, conn, `SELECT id FROM f_back WHERE code IS NULL ORDER BY id`, "1", "2", "5", "20", "21")
	mustExec(t, ctx, conn, `INSERT INTO f_back VALUES (30, NULL)`)
	wantSQLState(t, ctx, conn, "23505", `INSERT INTO f_back VALUES (31, 'a')`)
	wantRows(t, ctx, conn, `SELECT id FROM f_back WHERE code = 'b'`, "4")
}

// TestUniqueNullsNotDistinct (issue #322): UNIQUE NULLS NOT DISTINCT
// (PostgreSQL 15) makes NULLs conflict with each other, in every place
// a unique key can be declared; NULLS DISTINCT may be written too.
func TestUniqueNullsNotDistinct(t *testing.T) {
	tc := Start(t, 3)
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()
	conn := pgConn(t, ctx, tc, 0)

	mustExec(t, ctx, conn, `CREATE TABLE n (id INT PRIMARY KEY, a TEXT UNIQUE NULLS NOT DISTINCT, b TEXT UNIQUE NULLS DISTINCT, c INT, d INT,
		CONSTRAINT n_cd UNIQUE NULLS NOT DISTINCT (c, d))`)
	mustExec(t, ctx, conn, `INSERT INTO n VALUES (1, NULL, NULL, 1, NULL)`)
	wantSQLState(t, ctx, conn, "23505", `INSERT INTO n VALUES (2, NULL, 'x', 2, 2)`)
	wantSQLState(t, ctx, conn, "23505", `INSERT INTO n VALUES (2, 'x', 'x', 1, NULL)`)
	mustExec(t, ctx, conn, `INSERT INTO n VALUES (2, 'y', NULL, 1, 1), (3, 'z', NULL, NULL, 1), (4, '', NULL, 1, 2)`)
	wantSQLState(t, ctx, conn, "23505", `INSERT INTO n VALUES (5, 'w', NULL, NULL, 1)`)
	wantSQLState(t, ctx, conn, "23505", `UPDATE n SET a = NULL WHERE id = 2`)
	wantRows(t, ctx, conn, `SELECT id FROM n WHERE a = 'y'`, "2")
	wantPlan(t, ctx, conn, `SELECT id FROM n WHERE a = 'y'`, "point lookup via unique index")
	wantRows(t, ctx, conn, `SELECT id FROM n WHERE a = ''`, "4")
	wantRows(t, ctx, conn, `SELECT id FROM n WHERE a IS NULL`, "1")
	// A scan of the index's prefix with no lower bound starts past the
	// NULLs: with LIMIT pushed into the scan they would take its rows.
	wantRows(t, ctx, conn, `SELECT d FROM n WHERE c = 1 AND d < 5 ORDER BY d LIMIT 1`, "1")
	wantRows(t, ctx, conn, `SELECT d FROM n WHERE c = 1 AND d <= 5 ORDER BY d DESC LIMIT 1`, "2")
	wantRows(t, ctx, conn, `SELECT id FROM n WHERE c = 1 ORDER BY id`, "1", "2", "4")
	// (The same through a session, whose scan takes the pushed limit.)
	s := sql.NewSession(tc.Nodes[1].DB(), catalog.NewAccessor())
	requireSameRows(t, "the first d below 5", rowsText(t, ctx, s, `SELECT d FROM n WHERE c = 1 AND d < 5 ORDER BY d LIMIT 1`), []string{"1"})
	// ON CONFLICT: the NULL arbitrates.
	mustExec(t, ctx, conn, `INSERT INTO n VALUES (6, NULL, NULL, 6, 6) ON CONFLICT (a) DO NOTHING`)
	mustExec(t, ctx, conn, `INSERT INTO n VALUES (7, NULL, NULL, 7, 7) ON CONFLICT (a) DO UPDATE SET b = 'hit'`)
	wantRows(t, ctx, conn, `SELECT id, b FROM n WHERE a IS NULL`, "1|hit")
	mustExec(t, ctx, conn, `DELETE FROM n WHERE id = 1`)
	mustExec(t, ctx, conn, `INSERT INTO n VALUES (8, NULL, NULL, 1, NULL)`)

	// CREATE UNIQUE INDEX ... NULLS NOT DISTINCT and its backfill.
	mustExec(t, ctx, conn, `CREATE TABLE o (id INT PRIMARY KEY, k INT)`)
	mustExec(t, ctx, conn, `INSERT INTO o VALUES (1, NULL), (2, 1), (3, NULL)`)
	wantSQLState(t, ctx, conn, "23505", `CREATE UNIQUE INDEX o_k ON o (k) NULLS NOT DISTINCT`)
	mustExec(t, ctx, conn, `DELETE FROM o WHERE id = 3`)
	mustExec(t, ctx, conn, `CREATE UNIQUE INDEX o_k ON o (k) NULLS NOT DISTINCT`)
	wantSQLState(t, ctx, conn, "23505", `INSERT INTO o VALUES (4, NULL)`)
	mustExec(t, ctx, conn, `CREATE UNIQUE INDEX o_k2 ON o (k) NULLS DISTINCT`)
	mustExec(t, ctx, conn, `ALTER TABLE o ADD CONSTRAINT o_k3 UNIQUE NULLS NOT DISTINCT (k)`)
	wantRows(t, ctx, conn, `SELECT indexname, indexdef FROM pg_indexes WHERE tablename = 'o' ORDER BY indexname`,
		"o_k|CREATE UNIQUE INDEX o_k ON public.o USING btree (k) NULLS NOT DISTINCT",
		"o_k2|CREATE UNIQUE INDEX o_k2 ON public.o USING btree (k)",
		"o_k3|CREATE UNIQUE INDEX o_k3 ON public.o USING btree (k) NULLS NOT DISTINCT",
		"o_pkey|CREATE UNIQUE INDEX o_pkey ON public.o USING btree (id)")
	wantRows(t, ctx, conn, `SELECT pg_get_constraintdef(oid) FROM pg_constraint WHERE conname = 'o_k3'`, "UNIQUE NULLS NOT DISTINCT (k)")
	wantRows(t, ctx, conn, `SELECT i.indnullsnotdistinct FROM pg_index i JOIN pg_class c ON c.oid = i.indexrelid WHERE c.relname = 'o_k'`, "true")
	// CREATE TABLE ... LIKE carries the clause.
	mustExec(t, ctx, conn, `CREATE TABLE o2 (LIKE o INCLUDING ALL)`)
	mustExec(t, ctx, conn, `INSERT INTO o2 VALUES (1, NULL)`)
	wantSQLState(t, ctx, conn, "23505", `INSERT INTO o2 VALUES (2, NULL)`)
}

// TestUniqueIndexNullsBeforeV22 (issue #322): until v22 is finalized a
// unique index refuses NULLs as every earlier binary does (a v21
// planner takes a unique index for every row of the table), and NULLS
// NOT DISTINCT is refused.
func TestUniqueIndexNullsBeforeV22(t *testing.T) {
	tc := StartWithOptions(t, 1, func(c *server.Config) { c.BinaryVersionOverride = version.V21 })
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	s := sql.NewSession(tc.Nodes[0].DB(), catalog.NewAccessor())
	waitForDatabases(t, ctx, s)
	execSQL(t, ctx, s, `CREATE TABLE e (id INT PRIMARY KEY, link TEXT UNIQUE)`)
	if _, serr := trySQL(ctx, s, `INSERT INTO e VALUES (1, NULL)`); serr == nil || serr.Code != sql.CodeNotNullViolation {
		t.Fatalf("a NULL under a unique index at v21: %v", serr)
	}
	execSQL(t, ctx, s, `INSERT INTO e VALUES (1, 'a')`)
	if _, serr := trySQL(ctx, s, `UPDATE e SET link = NULL`); serr == nil || serr.Code != sql.CodeNotNullViolation {
		t.Fatalf("an update to NULL under a unique index at v21: %v", serr)
	}
	execSQL(t, ctx, s, `CREATE TABLE f (id INT PRIMARY KEY, code TEXT)`)
	execSQL(t, ctx, s, `INSERT INTO f VALUES (1, NULL)`)
	if _, serr := trySQL(ctx, s, `CREATE UNIQUE INDEX f_code ON f (code)`); serr == nil || serr.Code != sql.CodeNotNullViolation {
		t.Fatalf("a unique backfill over a NULL at v21: %v", serr)
	}
	if _, serr := trySQL(ctx, s, `CREATE TABLE g (id INT PRIMARY KEY, code TEXT UNIQUE NULLS NOT DISTINCT)`); serr == nil || serr.Code != sql.CodeFeatureNotSupported {
		t.Fatalf("NULLS NOT DISTINCT at v21: %v", serr)
	}
}
