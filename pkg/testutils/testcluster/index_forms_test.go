package testcluster

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/sthorne/datax/pkg/server"
	"github.com/sthorne/datax/pkg/sql"
	"github.com/sthorne/datax/pkg/sql/catalog"
	"github.com/sthorne/datax/pkg/version"
)

// planUses checks that EXPLAIN q names index idx ("" = names no
// secondary index).
func (d *ddlCompat) planUses(q, idx string) {
	d.t.Helper()
	p := explainPlan(d.t, d.ctx, d.s, q)
	if idx == "" {
		if strings.Contains(p, "index \"") {
			d.t.Fatalf("%s: plan %q uses an index", q, p)
		}
		return
	}
	if !strings.Contains(p, `"`+idx+`"`) {
		d.t.Fatalf("%s: plan %q does not use %q", q, p, idx)
	}
}

// TestPartialIndex (issue #358): CREATE [UNIQUE] INDEX ... WHERE indexes
// only the rows the predicate holds for — uniqueness among them only —
// on the online build and in the block that created the table; writes
// keep it exact as rows move in and out of the predicate; the planner
// uses it only when the WHERE clause repeats the predicate.
func TestPartialIndex(t *testing.T) {
	d := newDDLCompat(t)
	d.exec(`CREATE TABLE users (id INT8 PRIMARY KEY, email TEXT, deleted_at TIMESTAMPTZ, status TEXT)`)
	d.exec(`INSERT INTO users VALUES (1, 'a@x', NULL, 'active'), (2, 'a@x', now(), 'gone'), (3, 'b@x', now(), 'gone'), (4, 'b@x', now(), 'gone')`)

	// The backfill honours the predicate: the deleted duplicates are
	// not in the index, so the unique build succeeds.
	d.exec(`CREATE UNIQUE INDEX ON users (email) WHERE deleted_at IS NULL`)
	d.expect(`SELECT indexdef FROM pg_indexes WHERE indexname = 'users_email_idx'`,
		"CREATE UNIQUE INDEX users_email_idx ON public.users USING btree (email) WHERE (deleted_at IS NULL)")
	d.expect(`SELECT pg_get_expr(i.indpred, i.indrelid) FROM pg_index i JOIN pg_class c ON c.oid = i.indexrelid WHERE c.relname = 'users_email_idx'`, "deleted_at IS NULL")
	// Not a constraint: pg_constraint does not list it.
	d.expect(`SELECT count(*) FROM pg_constraint WHERE conname = 'users_email_idx'`, "0")
	// psql's \d users lists it with its predicate.
	oid := d.exec(`SELECT oid FROM pg_class WHERE relname = 'users'`).Rows[0][0].Text()
	d.expect(`SELECT c2.relname, i.indisunique, pg_catalog.pg_get_indexdef(i.indexrelid, 0, true), contype
FROM pg_catalog.pg_class c, pg_catalog.pg_class c2, pg_catalog.pg_index i
  LEFT JOIN pg_catalog.pg_constraint con ON (conrelid = i.indrelid AND conindid = i.indexrelid AND contype IN ('p','u','x'))
WHERE c.oid = '`+oid+`' AND c.oid = i.indrelid AND i.indexrelid = c2.oid AND NOT i.indisprimary
ORDER BY i.indisprimary DESC, c2.relname`,
		"users_email_idx|t|CREATE UNIQUE INDEX users_email_idx ON public.users USING btree (email) WHERE (deleted_at IS NULL)|NULL")

	// Soft-delete uniqueness.
	d.refused(`INSERT INTO users VALUES (5, 'a@x', NULL, 'active')`, sql.CodeUniqueViolation, "users_email_idx")
	d.exec(`INSERT INTO users VALUES (5, 'a@x', now(), 'gone')`) // deleted: no entry, no conflict
	d.exec(`INSERT INTO users VALUES (6, 'b@x', NULL, 'active')`)
	// Moving a row into the predicate: the entry appears and is checked.
	d.refused(`UPDATE users SET deleted_at = NULL WHERE id = 3`, sql.CodeUniqueViolation, "users_email_idx")
	// Moving one out: its entry goes, and the value is free again.
	d.exec(`UPDATE users SET deleted_at = now() WHERE id = 6`)
	d.exec(`UPDATE users SET deleted_at = NULL WHERE id = 3`)
	d.expect(`SELECT id FROM users WHERE email = 'b@x' AND deleted_at IS NULL`, "3")
	d.planUses(`SELECT id FROM users WHERE email = 'b@x' AND deleted_at IS NULL`, "users_email_idx")
	// A key update inside the predicate moves the entry.
	d.exec(`UPDATE users SET email = 'c@x' WHERE id = 3`)
	d.expect(`SELECT id FROM users WHERE email = 'c@x' AND deleted_at IS NULL`, "3")
	d.expect(`SELECT id FROM users WHERE email = 'b@x' AND deleted_at IS NULL`, "")
	d.exec(`DELETE FROM users WHERE id = 3`)
	d.expect(`SELECT id FROM users WHERE email = 'c@x' AND deleted_at IS NULL`, "")
	d.exec(`INSERT INTO users VALUES (3, 'c@x', NULL, 'active')`)

	// Without the predicate in the WHERE clause the index would lose the
	// deleted rows: never used.
	d.planUses(`SELECT id FROM users WHERE email = 'a@x'`, "")
	d.expect(`SELECT id FROM users WHERE email = 'a@x' ORDER BY id`, "1;2;5")
	d.planUses(`SELECT id FROM users WHERE email = 'a@x' AND deleted_at IS NOT NULL`, "")
	d.planUses(`SELECT id FROM users WHERE email = 'a@x' OR deleted_at IS NULL`, "")
	d.expect(`SELECT id FROM users WHERE email = 'a@x' AND deleted_at IS NOT NULL ORDER BY id`, "2;5")

	// A non-unique partial index with a value predicate, range scans.
	d.exec(`CREATE INDEX active_by_email ON users (email) WHERE status = 'active'`)
	d.planUses(`SELECT id FROM users WHERE email > 'a' AND status = 'active'`, "active_by_email")
	d.expect(`SELECT id FROM users WHERE email > 'a' AND status = 'active' ORDER BY id`, "1;3;6")
	d.planUses(`SELECT id FROM users WHERE email > 'a' AND status = 'gone'`, "")
	d.exec(`UPDATE users SET status = 'gone' WHERE id = 1`)
	d.expect(`SELECT id FROM users WHERE email > 'a' AND status = 'active' ORDER BY id`, "3;6")
	d.expect(`SELECT id FROM users WHERE email > 'a' AND status = 'gone' ORDER BY id`, "1;2;4;5")

	// Refused like PostgreSQL refuses them.
	d.refused(`CREATE INDEX bad ON users (email) WHERE id IN (SELECT 1)`, sql.CodeFeatureNotSupported, "cannot use subquery in index predicate")
	d.refused(`CREATE INDEX bad ON users (email) WHERE EXISTS (SELECT 1)`, sql.CodeFeatureNotSupported, "cannot use subquery in index predicate")
	d.refused(`CREATE INDEX bad ON users (email) WHERE deleted_at < now()`, sql.CodeInvalidObjectDefinition, "functions in index predicate must be marked IMMUTABLE")
	d.refused(`CREATE INDEX bad ON users (email) WHERE random() > 0.5`, sql.CodeInvalidObjectDefinition, "functions in index predicate must be marked IMMUTABLE")
	d.refused(`CREATE INDEX bad ON users (email) WHERE nope IS NULL`, sql.CodeUndefinedColumn, `column "nope" does not exist`)
	d.refused(`INSERT INTO users VALUES (7, 'a@x', NULL, 'active') ON CONFLICT (email) DO NOTHING`, sql.CodeInvalidColumnReference, "no unique or exclusion constraint")

	// A partial GIN index.
	d.exec(`CREATE TABLE docs (id INT8 PRIMARY KEY, doc JSONB, live BOOL)`)
	d.exec(`INSERT INTO docs VALUES (1, '{"a": 1}', true), (2, '{"a": 1}', false), (3, '{"a": 2}', true)`)
	d.exec(`CREATE INDEX docs_live ON docs USING gin (doc) WHERE live = true`)
	d.planUses(`SELECT id FROM docs WHERE doc @> '{"a": 1}' AND live = true`, "docs_live")
	d.expect(`SELECT id FROM docs WHERE doc @> '{"a": 1}' AND live = true`, "1")
	d.planUses(`SELECT id FROM docs WHERE doc @> '{"a": 1}'`, "")
	d.expect(`SELECT id FROM docs WHERE doc @> '{"a": 1}' ORDER BY id`, "1;2")
	d.exec(`UPDATE docs SET live = true WHERE id = 2`)
	d.expect(`SELECT id FROM docs WHERE doc @> '{"a": 1}' AND live = true ORDER BY id`, "1;2")

	// In the block that created the table.
	d.exec(`BEGIN`)
	d.exec(`CREATE TABLE accounts (id INT8 PRIMARY KEY, name TEXT, archived BOOL)`)
	d.exec(`INSERT INTO accounts VALUES (1, 'n', true), (2, 'n', true), (3, 'n', false)`)
	d.exec(`CREATE UNIQUE INDEX accounts_live_name ON accounts (name) WHERE archived = false`)
	d.refused(`INSERT INTO accounts VALUES (4, 'n', false)`, sql.CodeUniqueViolation, "accounts_live_name")
	d.exec(`ROLLBACK`)
	d.exec(`BEGIN`)
	d.exec(`CREATE TABLE accounts (id INT8 PRIMARY KEY, name TEXT, archived BOOL)`)
	d.exec(`INSERT INTO accounts VALUES (1, 'n', true), (2, 'n', true), (3, 'n', false)`)
	d.exec(`CREATE UNIQUE INDEX accounts_live_name ON accounts (name) WHERE archived = false`)
	d.exec(`COMMIT`)
	d.expect(`SELECT id FROM accounts WHERE name = 'n' AND archived = false`, "3")
	d.planUses(`SELECT id FROM accounts WHERE name = 'n' AND archived = false`, "accounts_live_name")
	d.refused(`INSERT INTO accounts VALUES (4, 'n', false)`, sql.CodeUniqueViolation, "accounts_live_name")
	d.exec(`INSERT INTO accounts VALUES (4, 'n', true)`)
}

// TestExpressionIndex (issue #359): an index key may be an immutable
// expression over the row's columns; a unique one compares the values;
// the planner uses it for equality and ranges on the same expression.
func TestExpressionIndex(t *testing.T) {
	d := newDDLCompat(t)
	d.exec(`CREATE TABLE users (id INT8 PRIMARY KEY, email TEXT, a INT8, b INT8, doc JSONB, created_at TIMESTAMPTZ)`)
	d.exec(`INSERT INTO users VALUES (1, 'Ann@X.org', 1, 10, '{"k": "one"}', now()), (2, 'bob@x.org', 2, 20, '{"k": "two"}', now())`)

	d.exec(`CREATE UNIQUE INDEX ON users ((lower(email)))`)
	d.expect(`SELECT indexdef FROM pg_indexes WHERE indexname = 'users_lower_idx'`,
		"CREATE UNIQUE INDEX users_lower_idx ON public.users USING btree (lower(email))")
	d.refused(`INSERT INTO users VALUES (3, 'ANN@x.org', 3, 30, NULL, now())`, sql.CodeUniqueViolation, "users_lower_idx")
	d.exec(`INSERT INTO users VALUES (3, 'carl@x.org', 3, 30, NULL, now())`)
	d.refused(`UPDATE users SET email = 'BOB@X.ORG' WHERE id = 3`, sql.CodeUniqueViolation, "users_lower_idx")
	d.exec(`UPDATE users SET email = 'Carl@X.org' WHERE id = 3`) // same value: its own entry
	d.planUses(`SELECT id FROM users WHERE lower(email) = 'carl@x.org'`, "users_lower_idx")
	d.expect(`SELECT id FROM users WHERE lower(email) = 'carl@x.org'`, "3")
	d.expect(`SELECT id FROM users WHERE lower(email) = 'Carl@X.org'`, "")
	d.planUses(`SELECT id FROM users WHERE email = 'Carl@X.org'`, "")
	d.planUses(`SELECT id FROM users WHERE upper(email) = 'CARL@X.ORG'`, "")
	d.exec(`DELETE FROM users WHERE id = 3`)
	d.expect(`SELECT id FROM users WHERE lower(email) = 'carl@x.org'`, "")
	d.exec(`INSERT INTO users VALUES (3, 'CARL@x.org', 3, 30, NULL, now())`)

	// Mixed keys, ranges on the expression, and the normalized form of
	// the expression in the WHERE clause.
	d.exec(`CREATE INDEX ab ON users (a, (b + 1))`)
	d.expect(`SELECT indexdef FROM pg_indexes WHERE indexname = 'ab'`, "CREATE INDEX ab ON public.users USING btree (a, ((b + 1)))")
	d.planUses(`SELECT id FROM users WHERE a = 2 AND (b + 1) > 15`, "ab")
	d.planUses(`SELECT id FROM users WHERE a = 2 AND b+1 = 21`, "ab")
	d.expect(`SELECT id FROM users WHERE a = 2 AND b + 1 = 21`, "2")
	d.expect(`SELECT id FROM users WHERE a = 2 AND (b + 1) > 21`, "")
	d.exec(`UPDATE users SET b = 30 WHERE id = 2`)
	d.expect(`SELECT id FROM users WHERE a = 2 AND b + 1 = 31`, "2")
	d.expect(`SELECT id FROM users WHERE a = 2 AND b + 1 = 21`, "")

	// A JSON path, chosen as the index's name part "expr".
	d.exec(`CREATE INDEX ON users ((doc ->> 'k'))`)
	d.expect(`SELECT indexdef FROM pg_indexes WHERE indexname = 'users_expr_idx'`, "CREATE INDEX users_expr_idx ON public.users USING btree (((doc ->> 'k')))")
	d.planUses(`SELECT id FROM users WHERE doc->>'k' = 'two'`, "users_expr_idx")
	d.expect(`SELECT id FROM users WHERE doc->>'k' = 'two'`, "2")

	// Only immutable expressions over the row's columns.
	d.refused(`CREATE INDEX bad ON users ((random()))`, sql.CodeInvalidObjectDefinition, "functions in index expression must be marked IMMUTABLE")
	d.refused(`CREATE INDEX bad ON users ((now()))`, sql.CodeInvalidObjectDefinition, "functions in index expression must be marked IMMUTABLE")
	d.refused(`CREATE INDEX bad ON users ((created_at::date))`, sql.CodeInvalidObjectDefinition, "functions in index expression must be marked IMMUTABLE")
	d.refused(`CREATE INDEX bad ON users ((concat(email, 'x')))`, sql.CodeInvalidObjectDefinition, "functions in index expression must be marked IMMUTABLE")
	d.refused(`CREATE INDEX bad ON users (((SELECT 1)))`, sql.CodeFeatureNotSupported, "cannot use subquery in index expression")
	d.refused(`CREATE INDEX bad ON users ((lower(nope)))`, sql.CodeUndefinedColumn, `column "nope" does not exist`)
	d.refused(`CREATE INDEX bad ON users USING gin ((lower(email)))`, sql.CodeFeatureNotSupported, "does not support expression keys")
	// The error for lower(b) no longer asks for a text-search config.
	d.exec(`CREATE INDEX ON users (lower(email))`)
	d.expect(`SELECT indexname FROM pg_indexes WHERE indexname = 'users_lower_idx1'`, "users_lower_idx1")

	// What pg_dump (and Rails' structure.sql) writes for a VARCHAR
	// column: the casts to text change nothing, and queries without
	// them use the index.
	d.exec(`CREATE TABLE dumped (id INT8 PRIMARY KEY, email VARCHAR(80), status VARCHAR(10))`)
	d.exec(`CREATE UNIQUE INDEX dumped_lower ON public.dumped USING btree (lower((email)::text)) WHERE ((status)::text = 'active'::text)`)
	d.expect(`SELECT indexdef FROM pg_indexes WHERE indexname = 'dumped_lower'`,
		"CREATE UNIQUE INDEX dumped_lower ON public.dumped USING btree (lower(email)) WHERE ((status)::text = 'active'::text)")
	d.exec(`INSERT INTO dumped VALUES (1, 'A@x', 'active'), (2, 'a@X', 'gone')`)
	d.refused(`INSERT INTO dumped VALUES (3, 'a@x', 'active')`, sql.CodeUniqueViolation, "dumped_lower")
	d.planUses(`SELECT id FROM dumped WHERE lower(email) = 'a@x' AND status = 'active'`, "dumped_lower")
	d.expect(`SELECT id FROM dumped WHERE lower(email) = 'a@x' AND status = 'active'`, "1")

	// A failing expression fails the write with its own error.
	d.exec(`CREATE TABLE ratio (id INT8 PRIMARY KEY, n INT8, m INT8)`)
	d.exec(`CREATE INDEX ON ratio ((n / m))`)
	d.refused(`INSERT INTO ratio VALUES (1, 1, 0)`, sql.CodeDivisionByZero, "")
	d.exec(`INSERT INTO ratio VALUES (1, 6, 3)`)
	d.expect(`SELECT id FROM ratio WHERE n / m = 2`, "1")
}

// TestIndexKeyOrder (issue #360): ASC/DESC and NULLS FIRST/LAST per key
// are recorded and shown; keys stay ascending and ORDER BY ... DESC is
// served by a reverse scan.
func TestIndexKeyOrder(t *testing.T) {
	d := newDDLCompat(t)
	d.exec(`CREATE TABLE posts (id INT8 PRIMARY KEY, user_id INT8 NOT NULL, created_at TIMESTAMPTZ NOT NULL, title TEXT)`)
	for i := 1; i <= 6; i++ {
		d.exec(`INSERT INTO posts VALUES (` + itoa(i) + `, ` + itoa(i%2) + `, '2024-01-0` + itoa(i) + `', 't` + itoa(i) + `')`)
	}
	// Rails: add_index :posts, [:user_id, :created_at], order: { created_at: :desc }
	d.exec(`CREATE INDEX "index_posts_on_user_id_and_created_at" ON "posts" ("user_id", "created_at" DESC)`)
	d.expect(`SELECT indexdef FROM pg_indexes WHERE indexname = 'index_posts_on_user_id_and_created_at'`,
		"CREATE INDEX index_posts_on_user_id_and_created_at ON public.posts USING btree (user_id, created_at DESC)")
	d.exec(`CREATE INDEX t_order ON posts (title ASC NULLS FIRST, created_at DESC NULLS LAST, id DESC NULLS FIRST)`)
	d.expect(`SELECT indexdef FROM pg_indexes WHERE indexname = 't_order'`,
		"CREATE INDEX t_order ON public.posts USING btree (title NULLS FIRST, created_at DESC NULLS LAST, id DESC)")

	q := `SELECT id FROM posts WHERE user_id = 1 ORDER BY created_at DESC`
	d.planUses(q, "index_posts_on_user_id_and_created_at")
	d.expect(q, "5;3;1")
	d.expect(`SELECT id FROM posts WHERE user_id = 1 ORDER BY created_at DESC LIMIT 2`, "5;3")
	d.expect(`SELECT id FROM posts WHERE user_id = 0 ORDER BY created_at ASC`, "2;4;6")
	d.expect(`SELECT id FROM posts WHERE user_id = 0 AND created_at > '2024-01-02' ORDER BY created_at DESC`, "6;4")
	d.refused(`CREATE INDEX bad ON posts USING gin (title DESC)`, sql.CodeFeatureNotSupported, "does not support ASC/DESC options")

	// DESC in an expression index, unique and partial together.
	d.exec(`CREATE UNIQUE INDEX live_title ON posts ((lower(title)) DESC) WHERE user_id = 1`)
	d.expect(`SELECT indexdef FROM pg_indexes WHERE indexname = 'live_title'`,
		"CREATE UNIQUE INDEX live_title ON public.posts USING btree (lower(title) DESC) WHERE (user_id = 1)")
	d.refused(`INSERT INTO posts VALUES (7, 1, now(), 'T1')`, sql.CodeUniqueViolation, "live_title")
	d.exec(`INSERT INTO posts VALUES (7, 0, now(), 'T1')`)
	d.planUses(`SELECT id FROM posts WHERE lower(title) = 't3' AND user_id = 1`, "live_title")
	d.expect(`SELECT id FROM posts WHERE lower(title) = 't3' AND user_id = 1`, "3")
	d.expect(`SELECT id FROM posts WHERE lower(title) = 't1' ORDER BY id`, "1;7")
}

// TestIndexFormsAndDDL: expression keys, predicates and orders survive
// what other DDL does to their table — RENAME COLUMN rewrites them,
// DROP COLUMN drops the indexes that read the column (as PostgreSQL
// does), ALTER COLUMN TYPE refuses a column they read, LIKE ...
// INCLUDING INDEXES copies them — and a backup, a restore and a table
// restore bring them back.
func TestIndexFormsAndDDL(t *testing.T) {
	d := newDDLCompat(t)
	d.exec(`CREATE TABLE users (id INT8 PRIMARY KEY, email TEXT, deleted_at TIMESTAMPTZ, n INT8, note TEXT)`)
	d.exec(`INSERT INTO users VALUES (1, 'A@x', NULL, 1, 'a'), (2, 'a@X', now(), 2, 'b'), (3, 'b@x', NULL, 3, 'c')`)
	d.exec(`CREATE UNIQUE INDEX live_email ON users ((lower(email)) DESC) WHERE deleted_at IS NULL`)
	d.exec(`CREATE INDEX by_n ON users (n DESC)`)
	d.exec(`CREATE INDEX note_live ON users (note) WHERE n > 0`)

	// RENAME COLUMN: the stored texts follow the column.
	d.exec(`ALTER TABLE users RENAME COLUMN email TO mail`)
	d.exec(`ALTER TABLE users RENAME COLUMN deleted_at TO gone_at`)
	d.expect(`SELECT indexdef FROM pg_indexes WHERE indexname = 'live_email'`,
		"CREATE UNIQUE INDEX live_email ON public.users USING btree (lower(mail) DESC) WHERE (gone_at IS NULL)")
	d.refused(`INSERT INTO users VALUES (4, 'B@X', NULL, 4, 'd')`, sql.CodeUniqueViolation, "live_email")
	d.planUses(`SELECT id FROM users WHERE lower(mail) = 'b@x' AND gone_at IS NULL`, "live_email")
	d.expect(`SELECT id FROM users WHERE lower(mail) = 'b@x' AND gone_at IS NULL`, "3")

	// ALTER COLUMN TYPE of a column an expression or a predicate reads.
	d.refused(`ALTER TABLE users ALTER COLUMN n TYPE TEXT`, sql.CodeFeatureNotSupported, `used by index "by_n"`)
	d.refused(`ALTER TABLE users ALTER COLUMN gone_at TYPE TEXT`, sql.CodeFeatureNotSupported, `used by index "live_email"`)

	// LIKE ... INCLUDING INDEXES copies every form.
	d.exec(`CREATE TABLE users2 (LIKE users INCLUDING INDEXES)`)
	d.expect(`SELECT indexdef FROM pg_indexes WHERE tablename = 'users2' ORDER BY indexname`,
		"CREATE INDEX by_n ON public.users2 USING btree (n DESC);"+
			"CREATE UNIQUE INDEX live_email ON public.users2 USING btree (lower(mail) DESC) WHERE (gone_at IS NULL);"+
			"CREATE INDEX note_live ON public.users2 USING btree (note) WHERE (n > 0);"+
			"CREATE UNIQUE INDEX users2_pkey ON public.users2 USING btree (id)")
	d.exec(`INSERT INTO users2 VALUES (1, 'Q@x', NULL, 1, 'a'), (2, 'q@X', now(), 1, 'a')`)
	d.refused(`INSERT INTO users2 VALUES (3, 'q@x', NULL, 1, 'a')`, sql.CodeUniqueViolation, "live_email")

	// DROP COLUMN drops the indexes that read it: as a key, in an
	// expression, in a predicate.
	d.exec(`ALTER TABLE users2 DROP COLUMN n`)
	d.expect(`SELECT indexname FROM pg_indexes WHERE tablename = 'users2' ORDER BY indexname`, "live_email;users2_pkey")
	d.exec(`ALTER TABLE users2 DROP COLUMN gone_at`)
	d.expect(`SELECT indexname FROM pg_indexes WHERE tablename = 'users2' ORDER BY indexname`, "users2_pkey")
	d.exec(`INSERT INTO users2 VALUES (3, 'q@x', 'a')`)
	d.expect(`SELECT count(*) FROM users2 WHERE lower(mail) = 'q@x'`, "3")

	// A backup restored into a new cluster, and a table restore.
	dir := d.t.TempDir()
	if _, err := d.tc.Nodes[0].RunBackup(d.ctx, dir, "", false, false); err != nil {
		t.Fatalf("backup: %v", err)
	}
	if _, err := d.tc.Nodes[0].RunRestoreOpts(d.ctx, server.RestoreOptions{
		Srcs: []string{dir}, Tables: []server.RestoreTable{{Name: "users", Into: "users_r"}},
	}); err != nil {
		t.Fatalf("table restore: %v", err)
	}
	d.expect(`SELECT indexdef FROM pg_indexes WHERE indexname = 'live_email' AND tablename = 'users_r'`,
		"CREATE UNIQUE INDEX live_email ON public.users_r USING btree (lower(mail) DESC) WHERE (gone_at IS NULL)")
	d.planUses(`SELECT id FROM users_r WHERE lower(mail) = 'b@x' AND gone_at IS NULL`, "live_email")
	d.expect(`SELECT id FROM users_r WHERE lower(mail) = 'b@x' AND gone_at IS NULL`, "3")
	d.refused(`INSERT INTO users_r VALUES (4, 'B@X', NULL, 4, 'd')`, sql.CodeUniqueViolation, "live_email")

	tc2 := Start(t, 1)
	if _, err := tc2.Nodes[0].RunRestore(d.ctx, []string{dir}); err != nil {
		t.Fatalf("restore: %v", err)
	}
	d2 := &ddlCompat{t: t, ctx: d.ctx, s: sql.NewSession(tc2.Nodes[0].DB(), catalog.NewAccessor()), tc: tc2}
	d2.planUses(`SELECT id FROM users WHERE lower(mail) = 'a@x' AND gone_at IS NULL`, "live_email")
	d2.expect(`SELECT id FROM users WHERE lower(mail) = 'a@x' AND gone_at IS NULL`, "1")
	d2.refused(`INSERT INTO users VALUES (4, 'B@X', NULL, 4, 'd')`, sql.CodeUniqueViolation, "live_email")
	d2.exec(`INSERT INTO users VALUES (4, 'B@X', now(), 4, 'd')`)
	d2.expect(`SELECT id FROM users WHERE note = 'd' AND n > 0`, "4")
	d2.planUses(`SELECT id FROM users WHERE note = 'd' AND n > 0`, "note_live")
}

// TestIndexFormsNeedV22: until v22 is finalized a node may run a binary
// that would not compute expression keys or test a predicate, so none
// is created; ASC/DESC changes only how an index is shown and is taken.
func TestIndexFormsNeedV22(t *testing.T) {
	tc := StartWithOptions(t, 1, func(c *server.Config) { c.BinaryVersionOverride = version.V21 })
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	d := &ddlCompat{t: t, tc: tc, ctx: ctx, s: sql.NewSession(tc.Nodes[0].DB(), catalog.NewAccessor())}
	waitForDatabases(t, d.ctx, d.s)
	d.exec(`CREATE TABLE old (id INT8 PRIMARY KEY, email TEXT, gone BOOL)`)
	d.refused(`CREATE INDEX ON old ((lower(email)))`, sql.CodeFeatureNotSupported, "v22")
	d.refused(`CREATE INDEX ON old (email) WHERE gone = false`, sql.CodeFeatureNotSupported, "v22")
	d.exec(`CREATE INDEX ON old (email DESC)`)
}

// TestIndexFormsOnSealedTables: an index on an expression, or with a
// WHERE clause, would keep what it computes from sealed columns in
// plaintext, so a sealed table takes none and a table with one is not
// sealed.
func TestIndexFormsOnSealedTables(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Second)
	defer cancel()
	c := startSealedCluster(t, ctx)
	if errText, _ := c.op(0, "init"); errText != "" {
		t.Fatalf("init: %s", errText)
	}
	conn := sealedConn(t, ctx, c, 0, nil)
	sealExec(t, ctx, conn, `CREATE TABLE notes (id INT8 PRIMARY KEY, email TEXT, gone BOOL) WITH (encryption_key = 'auto')`)
	wantSQLErr(t, ctx, conn, `CREATE INDEX ON notes ((lower(email)))`, "0A000", "is sealed")
	wantSQLErr(t, ctx, conn, `CREATE INDEX ON notes (email) WHERE gone = false`, "0A000", "is sealed")
	sealExec(t, ctx, conn, `CREATE INDEX ON notes (email DESC)`)
	sealExec(t, ctx, conn, `CREATE TABLE plain (id INT8 PRIMARY KEY, email TEXT)`)
	sealExec(t, ctx, conn, `CREATE INDEX plain_lower ON plain ((lower(email)))`)
	wantSQLErr(t, ctx, conn, `ALTER TABLE plain SET (encryption_key = 'auto')`, "0A000", `index "plain_lower"`)
}
