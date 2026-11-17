package testcluster

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/sthorne/datax/pkg/sql"
	"github.com/sthorne/datax/pkg/sql/catalog"
)

// nopkHelpers are the statement helpers the issue #354 tests share.
type nopkHelpers struct {
	t   *testing.T
	ctx context.Context
	s   *sql.Session
}

func (h nopkHelpers) exec(q string) *sql.Result {
	h.t.Helper()
	return execSQL(h.t, h.ctx, h.s, q)
}

func (h nopkHelpers) expect(q, want string) {
	h.t.Helper()
	r := execSQL(h.t, h.ctx, h.s, q)
	var out []string
	for _, row := range r.Rows {
		var cells []string
		for _, d := range row {
			if d.Null {
				cells = append(cells, "NULL")
			} else {
				cells = append(cells, d.Text())
			}
		}
		out = append(out, strings.Join(cells, "|"))
	}
	if got := strings.Join(out, ";"); got != want {
		h.t.Fatalf("%s: got %q, want %q", q, got, want)
	}
}

func (h nopkHelpers) code(q, code string) *sql.Error {
	h.t.Helper()
	_, serr := trySQL(h.ctx, h.s, q)
	if serr == nil || serr.Code != code {
		h.t.Fatalf("%s: want %s, got %v", q, code, serr)
	}
	return serr
}

func (h nopkHelpers) columns(q string) string {
	h.t.Helper()
	r := execSQL(h.t, h.ctx, h.s, q)
	names := make([]string, 0, len(r.Columns))
	for _, c := range r.Columns {
		names = append(names, c.Name)
	}
	return strings.Join(names, ",")
}

// TestNoPrimaryKeyTables covers issue #354: a table created without a
// PRIMARY KEY is keyed by a hidden, generated rowid (CREATE TABLE AS's
// mechanism) — invisible to SELECT *, RETURNING *, COPY and the
// catalogs, selectable by name, duplicate rows allowed — and every
// statement works on it: INSERT (multi-row, INSERT ... SELECT), UPDATE,
// DELETE, COPY FROM, TRUNCATE, indexes, CHECK / NOT NULL, foreign keys
// from it (an ORM join table) and to it (through a unique constraint
// only), ADD / DROP COLUMN, LIKE ... INCLUDING ALL, and the refusal of
// ADD PRIMARY KEY.
func TestNoPrimaryKeyTables(t *testing.T) {
	tc := Start(t, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	s := sql.NewSession(tc.Nodes[0].DB(), catalog.NewAccessor())
	waitForDatabases(t, ctx, s)
	h := nopkHelpers{t, ctx, s}

	// ---- the hidden key ----
	if r := h.exec(`CREATE TABLE t (a INT, b TEXT)`); r.Tag != "CREATE TABLE" {
		t.Fatalf("tag %q", r.Tag)
	}
	if c := h.columns(`SELECT * FROM t`); c != "a,b" {
		t.Fatalf("SELECT * columns: %s", c)
	}
	h.expect(`SELECT column_name FROM information_schema.columns WHERE table_name = 't' ORDER BY ordinal_position`, "a;b")
	// psql's \d reads pg_attribute with attnum > 0.
	h.expect(`SELECT attname FROM pg_attribute WHERE attrelid = 't'::regclass AND attnum > 0 AND NOT attisdropped ORDER BY attnum`, "a;b")
	// No primary key in the catalogs, as in PostgreSQL (psql's \d
	// lists none; ORMs introspecting the key find none).
	h.expect(`SELECT count(*) FROM pg_indexes WHERE tablename = 't'`, "0")
	h.expect(`SELECT count(*) FROM pg_index WHERE indrelid = 't'::regclass`, "0")
	h.expect(`SELECT count(*) FROM pg_constraint WHERE conrelid = 't'::regclass`, "0")
	h.expect(`SELECT count(*) FROM pg_class WHERE relname = 't_pkey'`, "0")
	h.expect(`SELECT count(*) FROM information_schema.table_constraints WHERE table_name = 't'`, "0")
	h.expect(`SELECT count(*) FROM information_schema.key_column_usage WHERE table_name = 't'`, "0")
	if def := h.exec(`SHOW CREATE TABLE t`).Rows[0][1].S; strings.Contains(def, "rowid") || strings.Contains(def, "PRIMARY KEY") {
		t.Fatalf("SHOW CREATE TABLE shows the hidden key:\n%s", def)
	}

	// ---- INSERT: duplicates, multi-row, INSERT ... SELECT, RETURNING * ----
	h.exec(`INSERT INTO t VALUES (1, 'x'), (1, 'x'), (2, 'y')`)
	h.exec(`INSERT INTO t (a, b) VALUES (1, 'x')`)
	h.exec(`INSERT INTO t SELECT a + 10, b FROM t WHERE a = 2`)
	h.expect(`SELECT a, b, count(*) FROM t GROUP BY a, b ORDER BY a`, "1|x|3;2|y|1;12|y|1")
	if c := h.columns(`INSERT INTO t VALUES (3, 'z') RETURNING *`); c != "a,b" {
		t.Fatalf("RETURNING * columns: %s", c)
	}
	// Selectable by name, unique, NOT NULL.
	h.expect(`SELECT count(DISTINCT rowid), count(*), count(rowid) FROM t`, "6|6|6")
	h.expect(`SELECT count(*) FROM t WHERE rowid > 0`, "6")
	h.code(`INSERT INTO t (a) VALUES (1) ON CONFLICT DO NOTHING RETURNING rowid, nope`, sql.CodeUndefinedColumn)

	// ---- UPDATE and DELETE on duplicate rows ----
	if r := h.exec(`UPDATE t SET b = 'w' WHERE a = 1`); r.Tag != "UPDATE 3" {
		t.Fatalf("update tag %q", r.Tag)
	}
	h.expect(`SELECT DISTINCT b FROM t WHERE a = 1`, "w")
	if r := h.exec(`DELETE FROM t WHERE a = 1`); r.Tag != "DELETE 3" {
		t.Fatalf("delete tag %q", r.Tag)
	}
	h.expect(`SELECT a FROM t ORDER BY a`, "2;3;12")

	// ---- TRUNCATE ----
	h.exec(`TRUNCATE t`)
	h.expect(`SELECT count(*) FROM t`, "0")
	h.exec(`INSERT INTO t VALUES (5, 'v'), (5, 'v')`)
	h.expect(`SELECT count(*) FROM t WHERE a = 5`, "2")

	// ---- indexes, CHECK, NOT NULL ----
	h.exec(`CREATE TABLE c (k INT NOT NULL, v INT CHECK (v > 0), email TEXT, UNIQUE (email))`)
	h.exec(`CREATE INDEX c_by_v ON c (v)`)
	h.exec(`INSERT INTO c VALUES (1, 1, 'a@x'), (1, 1, 'b@x'), (1, 1, NULL), (1, 1, NULL)`)
	h.code(`INSERT INTO c VALUES (1, 1, 'a@x')`, sql.CodeUniqueViolation)
	h.code(`INSERT INTO c VALUES (NULL, 1, 'c@x')`, sql.CodeNotNullViolation)
	h.code(`INSERT INTO c VALUES (1, 0, 'c@x')`, sql.CodeCheckViolation)
	if p := explainPlan(t, ctx, s, `SELECT k FROM c WHERE v = 1`); !strings.Contains(p, `index "c_by_v"`) {
		t.Fatalf("secondary index unused: %s", p)
	}
	h.expect(`SELECT count(*) FROM c WHERE v = 1`, "4")
	h.exec(`CREATE UNIQUE INDEX c_k_v ON c (v, email)`)
	h.code(`INSERT INTO c VALUES (2, 1, 'b@x')`, sql.CodeUniqueViolation)

	// ---- an ORM-style join table: foreign keys from a keyless table ----
	h.exec(`CREATE TABLE users (id INT PRIMARY KEY, name TEXT)`)
	h.exec(`CREATE TABLE roles (id INT PRIMARY KEY, name TEXT)`)
	h.exec(`CREATE TABLE users_roles (user_id INT REFERENCES users, role_id INT REFERENCES roles)`)
	h.exec(`INSERT INTO users VALUES (1, 'ann'), (2, 'bob')`)
	h.exec(`INSERT INTO roles VALUES (10, 'admin'), (20, 'dev')`)
	h.exec(`INSERT INTO users_roles VALUES (1, 10), (1, 20), (2, 20), (2, 20)`)
	h.code(`INSERT INTO users_roles VALUES (3, 10)`, sql.CodeForeignKeyViolation)
	h.code(`DELETE FROM users WHERE id = 2`, sql.CodeForeignKeyViolation)
	h.expect(`SELECT u.name, r.name FROM users u JOIN users_roles ur ON ur.user_id = u.id JOIN roles r ON r.id = ur.role_id WHERE u.id = 1 ORDER BY r.name`, "ann|admin;ann|dev")
	if r := h.exec(`DELETE FROM users_roles WHERE user_id = 2 AND role_id = 20`); r.Tag != "DELETE 2" {
		t.Fatalf("join table delete: %q", r.Tag)
	}
	h.exec(`DELETE FROM users WHERE id = 2`)
	h.exec(`CREATE TABLE tags (user_id INT REFERENCES users ON DELETE CASCADE, tag TEXT)`)
	h.exec(`INSERT INTO tags VALUES (1, 'a'), (1, 'a')`)
	h.exec(`DELETE FROM users_roles WHERE user_id = 1`)
	h.exec(`DELETE FROM users WHERE id = 1`)
	h.expect(`SELECT count(*) FROM tags`, "0")

	// ---- foreign keys to a keyless table need a unique constraint ----
	h.code(`CREATE TABLE child (p INT REFERENCES t)`, sql.CodeInvalidForeignKey)
	h.code(`CREATE TABLE child (p INT REFERENCES t (a))`, sql.CodeInvalidForeignKey)
	h.code(`CREATE TABLE child (p INT REFERENCES t (rowid))`, sql.CodeUndefinedColumn)
	h.exec(`CREATE TABLE child (p TEXT REFERENCES c (email))`)
	h.exec(`INSERT INTO child VALUES ('a@x')`)
	h.code(`INSERT INTO child VALUES ('zz')`, sql.CodeForeignKeyViolation)

	// ---- ALTER TABLE ADD / DROP COLUMN ----
	h.exec(`ALTER TABLE t ADD COLUMN n INT DEFAULT 7`)
	h.exec(`INSERT INTO t (a, b) VALUES (6, 'u')`)
	if c := h.columns(`SELECT * FROM t`); c != "a,b,n" {
		t.Fatalf("after ADD COLUMN: %s", c)
	}
	h.expect(`SELECT a, b, n FROM t ORDER BY a, b`, "5|v|7;5|v|7;6|u|7")
	h.exec(`ALTER TABLE t DROP COLUMN b`)
	h.expect(`SELECT * FROM t ORDER BY a`, "5|7;5|7;6|7")
	h.expect(`SELECT attname FROM pg_attribute WHERE attrelid = 't'::regclass AND attnum > 0 AND NOT attisdropped ORDER BY attnum`, "a;n")
	h.code(`ALTER TABLE t DROP COLUMN rowid`, sql.CodeUndefinedColumn)
	h.code(`ALTER TABLE t ADD COLUMN rowid INT`, sql.CodeDuplicateObject)

	// ---- ADD PRIMARY KEY: refused ----
	serr := h.code(`ALTER TABLE t ADD PRIMARY KEY (a)`, sql.CodeFeatureNotSupported)
	if !strings.Contains(serr.Msg, "hidden rowid") {
		t.Fatalf("ADD PRIMARY KEY message: %s", serr.Msg)
	}
	h.code(`ALTER TABLE users ADD CONSTRAINT users_pk PRIMARY KEY (id)`, sql.CodeInvalidTableDefinition)

	// ---- LIKE ... INCLUDING ALL ----
	h.exec(`CREATE TABLE c2 (LIKE c INCLUDING ALL)`)
	if c := h.columns(`SELECT * FROM c2`); c != "k,v,email" {
		t.Fatalf("LIKE columns: %s", c)
	}
	h.exec(`INSERT INTO c2 VALUES (1, 1, NULL), (1, 1, NULL)`)
	h.code(`INSERT INTO c2 VALUES (1, 0, NULL)`, sql.CodeCheckViolation)
	h.exec(`INSERT INTO c2 VALUES (1, 2, 'a@x')`)
	h.code(`INSERT INTO c2 VALUES (1, 3, 'a@x')`, sql.CodeUniqueViolation)
	h.expect(`SELECT count(DISTINCT rowid) FROM c2`, "3")

	// ---- a column named rowid: the hidden key takes rowid_1 ----
	h.exec(`CREATE TABLE r (rowid INT, x INT)`)
	h.exec(`INSERT INTO r VALUES (1, 1), (1, 1)`)
	if c := h.columns(`SELECT * FROM r`); c != "rowid,x" {
		t.Fatalf("visible rowid column: %s", c)
	}
	h.expect(`SELECT count(DISTINCT rowid_1), sum(rowid) FROM r`, "2|2")

	// ---- a time-series table still needs its declared key ----
	h.code(`CREATE TABLE ts (a INT, at TIMESTAMPTZ) WITH (timeseries = true)`, sql.CodeFeatureNotSupported)

	// ---- CREATE TABLE AS keeps its rowid ----
	h.exec(`CREATE TABLE ctas AS SELECT a FROM t`)
	h.expect(`SELECT count(DISTINCT rowid) FROM ctas`, "3")

	// ---- COPY FROM over the wire: no rowid in the default column list ----
	conn, err := pgx.Connect(ctx, pgURL(tc, 0))
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer func() { _ = conn.Close(ctx) }()
	h.exec(`CREATE TABLE cp (a INT, b TEXT)`)
	tag, err := conn.PgConn().CopyFrom(ctx, strings.NewReader("1\tone\n1\tone\n2\ttwo\n"), `COPY cp FROM STDIN`)
	if err != nil || tag.RowsAffected() != 3 {
		t.Fatalf("COPY FROM: %v %v", tag, err)
	}
	n, err := conn.CopyFrom(ctx, pgx.Identifier{"cp"}, []string{"a", "b"}, pgx.CopyFromRows([][]any{{int32(3), "three"}, {int32(3), "three"}}))
	if err != nil || n != 2 {
		t.Fatalf("pgx CopyFrom: %d %v", n, err)
	}
	// A row carrying a third field is refused: the default column list
	// has no room for the rowid.
	if _, err := conn.PgConn().CopyFrom(ctx, strings.NewReader("4\tfour\t99\n"), `COPY cp FROM STDIN`); err == nil {
		t.Fatalf("COPY FROM accepted a value for the hidden key")
	}
	h.expect(`SELECT a, count(*) FROM cp GROUP BY a ORDER BY a`, "1|2;2|1;3|2")
	h.expect(`SELECT count(DISTINCT rowid) FROM cp`, "5")
}

// TestNoPrimaryKeyConcurrentInserts: sessions on every node of a
// three-node cluster insert into one keyless table at once; every row
// lands with its own rowid (a collision would be a unique violation on
// the hidden key, or a lost row).
func TestNoPrimaryKeyConcurrentInserts(t *testing.T) {
	tc := Start(t, 3)
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()
	s := sql.NewSession(tc.Nodes[0].DB(), catalog.NewAccessor())
	waitForDatabases(t, ctx, s)
	h := nopkHelpers{t, ctx, s}
	h.exec(`CREATE TABLE events (who INT, what TEXT)`)

	const perNode, stmts, rowsPerStmt = 3, 15, 20
	var wg sync.WaitGroup
	errs := make(chan error, len(tc.Nodes)*perNode)
	for n := range tc.Nodes {
		for w := 0; w < perNode; w++ {
			wg.Add(1)
			go func(n, w int) {
				defer wg.Done()
				ws := sql.NewSession(tc.Nodes[n].DB(), catalog.NewAccessor())
				who := n*perNode + w
				for i := 0; i < stmts; i++ {
					vals := make([]string, rowsPerStmt)
					for j := range vals {
						vals[j] = fmt.Sprintf("(%d, 'same')", who) // identical rows on purpose
					}
					if _, serr := trySQL(ctx, ws, `INSERT INTO events VALUES `+strings.Join(vals, ", ")); serr != nil {
						errs <- fmt.Errorf("node %d worker %d: %v", n, w, serr)
						return
					}
				}
			}(n, w)
		}
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	total := len(tc.Nodes) * perNode * stmts * rowsPerStmt
	h.expect(`SELECT count(*), count(DISTINCT rowid) FROM events`, fmt.Sprintf("%d|%d", total, total))
	h.expect(`SELECT count(DISTINCT who) FROM events`, fmt.Sprint(len(tc.Nodes)*perNode))
	// The low 15 bits carry the node: every node contributed its own.
	h.expect(`SELECT count(DISTINCT rowid % 32768) FROM events`, fmt.Sprint(len(tc.Nodes)))
}

// TestNoPrimaryKeyBackupRestore: a keyless table survives a backup and
// restore with its hidden key, duplicates and the key's default.
func TestNoPrimaryKeyBackupRestore(t *testing.T) {
	tc := Start(t, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	s := sql.NewSession(tc.Nodes[0].DB(), catalog.NewAccessor())
	waitForDatabases(t, ctx, s)
	h := nopkHelpers{t, ctx, s}
	h.exec(`CREATE TABLE users (id INT PRIMARY KEY)`)
	h.exec(`CREATE TABLE users_roles (user_id INT REFERENCES users, role TEXT)`)
	h.exec(`CREATE INDEX ur_by_role ON users_roles (role)`)
	h.exec(`INSERT INTO users VALUES (1), (2)`)
	h.exec(`INSERT INTO users_roles VALUES (1, 'a'), (1, 'a'), (2, 'b')`)
	src := h.exec(`SELECT rowid FROM users_roles ORDER BY rowid`)

	dir := t.TempDir()
	if _, err := tc.Nodes[0].RunBackup(ctx, dir, "", false, false); err != nil {
		t.Fatalf("backup: %v", err)
	}
	tc2 := Start(t, 1)
	if _, err := tc2.Nodes[0].RunRestore(ctx, []string{dir}); err != nil {
		t.Fatalf("restore: %v", err)
	}
	s2 := sql.NewSession(tc2.Nodes[0].DB(), catalog.NewAccessor())
	h2 := nopkHelpers{t, ctx, s2}
	got := h2.exec(`SELECT rowid FROM users_roles ORDER BY rowid`)
	if len(got.Rows) != len(src.Rows) {
		t.Fatalf("restored rows: %d, want %d", len(got.Rows), len(src.Rows))
	}
	for i := range got.Rows {
		if got.Rows[i][0].I != src.Rows[i][0].I {
			t.Fatalf("restored rowid %d: %d, want %d", i, got.Rows[i][0].I, src.Rows[i][0].I)
		}
	}
	if c := h2.columns(`SELECT * FROM users_roles`); c != "user_id,role" {
		t.Fatalf("restored columns: %s", c)
	}
	h2.exec(`INSERT INTO users_roles VALUES (2, 'b')`)
	h2.expect(`SELECT role, count(*) FROM users_roles GROUP BY role ORDER BY role`, "a|2;b|2")
	h2.expect(`SELECT count(DISTINCT rowid) FROM users_roles`, "4")
	h2.code(`INSERT INTO users_roles VALUES (9, 'c')`, sql.CodeForeignKeyViolation)
}
