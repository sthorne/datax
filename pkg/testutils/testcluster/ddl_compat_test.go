package testcluster

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/sthorne/datax/pkg/sql"
	"github.com/sthorne/datax/pkg/sql/catalog"
)

// ddlCompat is a session plus the checks the tests below share.
type ddlCompat struct {
	t   *testing.T
	ctx context.Context
	s   *sql.Session
	tc  *TestCluster
}

func newDDLCompat(t *testing.T) *ddlCompat {
	tc := Start(t, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	t.Cleanup(cancel)
	s := sql.NewSession(tc.Nodes[0].DB(), catalog.NewAccessor())
	waitForDatabases(t, ctx, s)
	return &ddlCompat{t: t, ctx: ctx, s: s, tc: tc}
}

func (d *ddlCompat) exec(q string) *sql.Result {
	d.t.Helper()
	return execSQL(d.t, d.ctx, d.s, q)
}

// expect checks the rows of q, each row's cells joined by "|" and the
// rows by ";".
func (d *ddlCompat) expect(q, want string) {
	d.t.Helper()
	var out []string
	for _, row := range d.exec(q).Rows {
		var cells []string
		for _, c := range row {
			if c.Null {
				cells = append(cells, "NULL")
			} else {
				cells = append(cells, c.Text())
			}
		}
		out = append(out, strings.Join(cells, "|"))
	}
	if got := strings.Join(out, ";"); got != want {
		d.t.Fatalf("%s:\n got %q\nwant %q", q, got, want)
	}
}

// refused checks that q fails with code and a message containing msg.
func (d *ddlCompat) refused(q, code, msg string) {
	d.t.Helper()
	_, serr := trySQL(d.ctx, d.s, q)
	if serr == nil || serr.Code != code || !strings.Contains(serr.Msg, msg) {
		d.t.Fatalf("%s: got %+v, want [%s] ...%s...", q, serr, code, msg)
	}
}

// TestCreateIndexConcurrently: CONCURRENTLY is accepted and changes
// nothing (every build is online), refused inside a transaction block as
// PostgreSQL refuses it; the name may be left out (issue #347).
func TestCreateIndexConcurrently(t *testing.T) {
	d := newDDLCompat(t)
	d.exec(`CREATE TABLE cc (id INT8 PRIMARY KEY, v TEXT, n INT8)`)
	d.exec(`INSERT INTO cc VALUES (1, 'a', 10), (2, 'b', 20), (3, 'c', 30)`)

	d.exec(`CREATE INDEX CONCURRENTLY cc_v ON cc (v)`)
	if p := explainPlan(t, d.ctx, d.s, `SELECT id FROM cc WHERE v = 'b'`); !strings.Contains(p, `"cc_v"`) {
		t.Fatalf("plan: %q", p)
	}
	d.expect(`SELECT id FROM cc WHERE v = 'b'`, "2")
	d.exec(`CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS cc_n ON cc (n)`)
	d.exec(`CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS cc_n ON cc (n)`)
	d.refused(`INSERT INTO cc VALUES (4, 'd', 10)`, sql.CodeUniqueViolation, "")

	// No name: <table>_<columns>_idx, numbered when taken.
	d.exec(`CREATE INDEX CONCURRENTLY ON cc (n)`)
	d.exec(`CREATE INDEX ON cc (n, v)`)
	d.exec(`CREATE INDEX ON cc (n)`)
	d.expect(`SELECT indexname FROM pg_indexes WHERE tablename = 'cc' ORDER BY indexname`, "cc_n;cc_n_idx;cc_n_idx1;cc_n_v_idx;cc_pkey;cc_v")
	d.expect(`SELECT indexdef FROM pg_indexes WHERE indexname = 'cc_n_v_idx'`, "CREATE INDEX cc_n_v_idx ON public.cc USING btree (n, v)")
	if _, serr := trySQL(d.ctx, d.s, `CREATE INDEX IF NOT EXISTS ON cc (n)`); serr == nil {
		t.Fatal("IF NOT EXISTS without a name accepted")
	}

	// Inside a transaction block: 25001, and the block is failed.
	d.exec(`BEGIN`)
	d.refused(`CREATE INDEX CONCURRENTLY cc_v2 ON cc (v)`, sql.CodeActiveTransaction, "CREATE INDEX CONCURRENTLY cannot run inside a transaction block")
	d.refused(`SELECT 1`, sql.CodeInFailedTransaction, "")
	d.exec(`ROLLBACK`)
	d.exec(`BEGIN`)
	d.exec(`CREATE TABLE cc_new (id INT8 PRIMARY KEY, v TEXT)`)
	d.refused(`CREATE INDEX CONCURRENTLY ON cc_new (v)`, sql.CodeActiveTransaction, "CREATE INDEX CONCURRENTLY cannot run inside a transaction block")
	d.exec(`ROLLBACK`)
	d.exec(`BEGIN`)
	d.refused(`DROP INDEX CONCURRENTLY cc_v`, sql.CodeActiveTransaction, "DROP INDEX CONCURRENTLY cannot run inside a transaction block")
	d.exec(`ROLLBACK`)

	d.refused(`DROP INDEX CONCURRENTLY cc_v CASCADE`, sql.CodeFeatureNotSupported, "DROP INDEX CONCURRENTLY does not support CASCADE")
	d.exec(`DROP INDEX CONCURRENTLY IF EXISTS cc_v`)
	d.exec(`DROP INDEX CONCURRENTLY IF EXISTS cc_v`)
	d.refused(`DROP INDEX CONCURRENTLY cc_v`, sql.CodeUndefinedObject, `index "cc_v" does not exist`)
	d.exec(`DROP INDEX CONCURRENTLY cc_n_idx1 RESTRICT`)
	d.expect(`SELECT count(*) FROM pg_indexes WHERE tablename = 'cc'`, "4")
}

// TestCreateIndexOnTableCreatedInBlock: CREATE INDEX runs inside a
// transaction block when the block created the table, as Rails and
// Django migrations do (create_table + add_index in one transaction);
// on any other table it is still refused (issue #347).
func TestCreateIndexOnTableCreatedInBlock(t *testing.T) {
	d := newDDLCompat(t)

	d.exec(`BEGIN`)
	d.exec(`CREATE TABLE users (id INT8 PRIMARY KEY, email VARCHAR(100), city TEXT)`)
	d.exec(`INSERT INTO users VALUES (1, 'ann@x', 'oslo'), (2, 'bob@x', 'bergen')`)
	d.exec(`CREATE UNIQUE INDEX index_users_on_email ON users (email)`)
	d.exec(`CREATE INDEX ON users (city)`)
	d.exec(`INSERT INTO users VALUES (3, 'cat@x', 'oslo')`)
	// The block reads through the index it built.
	d.expect(`SELECT id FROM users WHERE email = 'bob@x'`, "2")
	d.refused(`INSERT INTO users VALUES (4, 'ann@x', 'oslo')`, sql.CodeUniqueViolation, "")
	d.exec(`ROLLBACK`)
	d.expect(`SELECT count(*) FROM pg_class WHERE relname = 'users'`, "0")

	d.exec(`BEGIN`)
	d.exec(`CREATE TABLE users (id INT8 PRIMARY KEY, email VARCHAR(100), city TEXT)`)
	d.exec(`INSERT INTO users VALUES (1, 'ann@x', 'oslo'), (2, 'bob@x', 'bergen')`)
	d.exec(`CREATE UNIQUE INDEX index_users_on_email ON users (email)`)
	d.exec(`CREATE INDEX ON users (city)`)
	d.exec(`INSERT INTO users VALUES (3, 'cat@x', 'oslo')`)
	d.exec(`COMMIT`)

	// Committed: public, valid and complete, as another session sees it.
	other := sql.NewSession(d.tc.Nodes[0].DB(), catalog.NewAccessor())
	if p := explainPlan(t, d.ctx, other, `SELECT id FROM users WHERE email = 'cat@x'`); !strings.Contains(p, `"index_users_on_email"`) {
		t.Fatalf("plan: %q", p)
	}
	d.expect(`SELECT id FROM users WHERE email = 'ann@x'`, "1")
	d.expect(`SELECT id FROM users WHERE city = 'oslo' ORDER BY id`, "1;3")
	d.expect(`SELECT c.relname, i.indisvalid FROM pg_index i JOIN pg_class c ON c.oid = i.indexrelid WHERE c.relname LIKE 'index_users%' OR c.relname LIKE 'users_city%' ORDER BY 1`,
		"index_users_on_email|t;users_city_idx|t")
	if _, serr := trySQL(d.ctx, other, `INSERT INTO users VALUES (4, 'bob@x', 'x')`); serr == nil || serr.Code != sql.CodeUniqueViolation {
		t.Fatalf("duplicate after commit: %+v", serr)
	}

	// Duplicates already in the block's rows fail the unique index.
	d.exec(`BEGIN`)
	d.exec(`CREATE TABLE dup (id INT8 PRIMARY KEY, v TEXT)`)
	d.exec(`INSERT INTO dup VALUES (1, 'x'), (2, 'x')`)
	d.refused(`CREATE UNIQUE INDEX dup_v ON dup (v)`, sql.CodeUniqueViolation, "duplicate")
	d.exec(`ROLLBACK`)

	// A table the block did not create: still the online build, refused.
	d.exec(`BEGIN`)
	d.refused(`CREATE INDEX users_id_city ON users (id, city)`, sql.CodeActiveTransaction, "CREATE INDEX cannot run inside a transaction block")
	d.refused(`SELECT 1`, sql.CodeInFailedTransaction, "")
	d.exec(`ROLLBACK`)

	// After ROLLBACK TO SAVEPOINT the block forgets what it created
	// (the safe side).
	d.exec(`BEGIN`)
	d.exec(`CREATE TABLE sp (id INT8 PRIMARY KEY, v TEXT)`)
	d.exec(`SAVEPOINT a`)
	d.exec(`ROLLBACK TO SAVEPOINT a`)
	d.refused(`CREATE INDEX sp_v ON sp (v)`, sql.CodeActiveTransaction, "cannot run inside a transaction block")
	d.exec(`ROLLBACK`)

	// A table created by an earlier block is an ordinary table now.
	d.exec(`BEGIN`)
	d.exec(`CREATE TABLE early (id INT8 PRIMARY KEY, v TEXT)`)
	d.exec(`COMMIT`)
	d.exec(`BEGIN`)
	d.refused(`CREATE INDEX early_v ON early (v)`, sql.CodeActiveTransaction, "cannot run inside a transaction block")
	d.exec(`ROLLBACK`)
	d.exec(`CREATE INDEX early_v ON early (v)`)
}

// TestIndexOperatorClassesAndCollations: the string types' btree
// operator classes and the C, POSIX and default collations are accepted
// on an ordinary index and shown by pg_get_indexdef; other collations
// and classes are refused (issue #348).
func TestIndexOperatorClassesAndCollations(t *testing.T) {
	d := newDDLCompat(t)
	d.exec(`CREATE TABLE "t" (id INT8 PRIMARY KEY, "col" VARCHAR(50), body TEXT, code CHAR(4), n INT8)`)
	d.exec(`INSERT INTO t VALUES (1, 'abc', 'xyz', 'ab', 1), (2, 'abd', 'xya', 'cd', 2)`)

	// Django's statement for a CharField with db_index, exactly.
	d.exec(`CREATE INDEX "x_like" ON "t" ("col" varchar_pattern_ops);`)
	d.exec(`CREATE INDEX t_body_like ON t (body text_pattern_ops)`)
	d.exec(`CREATE INDEX t_code_like ON t (code bpchar_pattern_ops)`)
	d.exec(`CREATE INDEX t_body_c ON t (body COLLATE "C")`)
	d.exec(`CREATE INDEX t_body_posix ON t (body COLLATE "POSIX" text_pattern_ops, n)`)
	d.exec(`CREATE INDEX t_body_default ON t (body COLLATE "default" pg_catalog.text_ops)`)
	d.exec(`CREATE UNIQUE INDEX t_col_c ON t (col COLLATE pg_catalog."C" varchar_ops)`)
	d.expect(`SELECT indexname, indexdef FROM pg_indexes WHERE tablename = 't' AND indexname <> 't_pkey' ORDER BY indexname`, strings.Join([]string{
		"t_body_c|CREATE INDEX t_body_c ON public.t USING btree (body COLLATE \"C\")",
		"t_body_default|CREATE INDEX t_body_default ON public.t USING btree (body)",
		"t_body_like|CREATE INDEX t_body_like ON public.t USING btree (body text_pattern_ops)",
		"t_body_posix|CREATE INDEX t_body_posix ON public.t USING btree (body COLLATE \"POSIX\" text_pattern_ops, n)",
		"t_code_like|CREATE INDEX t_code_like ON public.t USING btree (code bpchar_pattern_ops)",
		"t_col_c|CREATE UNIQUE INDEX t_col_c ON public.t USING btree (col COLLATE \"C\")",
		"x_like|CREATE INDEX x_like ON public.t USING btree (col varchar_pattern_ops)",
	}, ";"))
	d.expect(`SELECT pg_get_indexdef(i.indexrelid) FROM pg_index i JOIN pg_class c ON c.oid = i.indexrelid WHERE c.relname = 'x_like'`,
		"CREATE INDEX x_like ON public.t USING btree (col varchar_pattern_ops)")
	// The indexes serve as ordinary ones.
	d.expect(`SELECT id FROM t WHERE col = 'abd'`, "2")
	d.expect(`SELECT id FROM t WHERE body = 'xyz'`, "1")
	d.expect(`SELECT id FROM t WHERE col LIKE 'ab%' ORDER BY id`, "1;2")

	d.refused(`CREATE INDEX bad ON t (body COLLATE "en_US")`, sql.CodeFeatureNotSupported, `collation "en_US" is not supported in an index`)
	d.refused(`CREATE INDEX bad ON t (body COLLATE "und-x-icu" text_pattern_ops)`, sql.CodeFeatureNotSupported, "collation")
	d.refused(`CREATE INDEX bad ON t (n text_pattern_ops)`, "42804", `operator class "text_pattern_ops" does not accept data type`)
	d.refused(`CREATE INDEX bad ON t (n COLLATE "C")`, "42804", "collations are not supported by type")
	d.refused(`CREATE INDEX bad ON t (body int4_ops)`, sql.CodeFeatureNotSupported, `operator class "int4_ops" is not supported`)
	d.refused(`CREATE INDEX bad ON t (n int8_ops)`, sql.CodeFeatureNotSupported, `operator class "int8_ops" is not supported`)

	// Django's migration: the table and its _like index in one block.
	d.exec(`BEGIN`)
	d.exec(`CREATE TABLE "app_item" ("id" bigint NOT NULL PRIMARY KEY, "slug" varchar(50) NOT NULL)`)
	d.exec(`CREATE INDEX "app_item_slug_1b0e3c4d" ON "app_item" ("slug")`)
	d.exec(`CREATE INDEX "app_item_slug_1b0e3c4d_like" ON "app_item" ("slug" varchar_pattern_ops)`)
	d.exec(`COMMIT`)
	d.expect(`SELECT indexdef FROM pg_indexes WHERE indexname = 'app_item_slug_1b0e3c4d_like'`,
		"CREATE INDEX app_item_slug_1b0e3c4d_like ON public.app_item USING btree (slug varchar_pattern_ops)")

	// CREATE TABLE ... LIKE ... INCLUDING INDEXES keeps them.
	d.exec(`CREATE TABLE t2 (LIKE t INCLUDING INDEXES)`)
	d.expect(`SELECT indexdef FROM pg_indexes WHERE tablename = 't2' AND indexname = 'x_like'`,
		"CREATE INDEX x_like ON public.t2 USING btree (col varchar_pattern_ops)")
}

// TestBuiltInExtensions: CREATE EXTENSION and DROP EXTENSION accept the
// extensions whose surface is built in and do nothing; the catalogs list
// them as installed; any other extension is refused (issue #349).
func TestBuiltInExtensions(t *testing.T) {
	d := newDDLCompat(t)
	for _, q := range []string{
		`CREATE EXTENSION IF NOT EXISTS "uuid-ossp"`,
		`CREATE EXTENSION "uuid-ossp"`,
		`CREATE EXTENSION IF NOT EXISTS pgcrypto`,
		`CREATE EXTENSION pgcrypto WITH SCHEMA public VERSION '1.3' CASCADE`,
		`CREATE EXTENSION IF NOT EXISTS plpgsql`,
		`CREATE EXTENSION IF NOT EXISTS "plpgsql" WITH SCHEMA pg_catalog`,
		`CREATE EXTENSION plpgsql`,
		`CREATE EXTENSION IF NOT EXISTS vector`,
	} {
		if r := d.exec(q); r.Tag != "CREATE EXTENSION" {
			t.Fatalf("%s: tag %q", q, r.Tag)
		}
	}
	// Inside a migration's transaction block too.
	d.exec(`BEGIN`)
	d.exec(`CREATE EXTENSION IF NOT EXISTS "uuid-ossp"`)
	d.exec(`CREATE TABLE things (id UUID PRIMARY KEY DEFAULT uuid_generate_v4(), other UUID DEFAULT gen_random_uuid(), name TEXT)`)
	d.exec(`COMMIT`)
	d.exec(`INSERT INTO things (name) VALUES ('a'), ('b')`)
	d.expect(`SELECT count(DISTINCT id), count(DISTINCT other) FROM things`, "2|2")

	d.refused(`CREATE EXTENSION pg_trgm`, sql.CodeFeatureNotSupported, `extension "pg_trgm" is not available`)
	d.refused(`CREATE EXTENSION IF NOT EXISTS hstore`, sql.CodeFeatureNotSupported, `extension "hstore" is not available`)

	// What is listed is always the same four, created or dropped.
	const listed = "pgcrypto|1.3;plpgsql|1.0;uuid-ossp|1.1;vector|0.8.0"
	d.expect(`SELECT extname, extversion FROM pg_extension ORDER BY extname`, listed)
	d.expect(`SELECT e.extname FROM pg_extension e JOIN pg_namespace n ON n.oid = e.extnamespace WHERE n.nspname = 'pg_catalog'`, "plpgsql")
	// Django's and Rails's checks.
	d.expect(`SELECT 1 FROM pg_extension WHERE extname = 'pgcrypto'`, "1")
	d.expect(`SELECT EXISTS(SELECT * FROM pg_available_extensions WHERE name = 'uuid-ossp' AND installed_version IS NOT NULL)`, "t")

	if r := d.exec(`DROP EXTENSION IF EXISTS "uuid-ossp", pgcrypto`); r.Tag != "DROP EXTENSION" || len(r.Notices) != 0 {
		t.Fatalf("drop: %+v", r)
	}
	d.exec(`DROP EXTENSION pgcrypto CASCADE`)
	d.exec(`DROP EXTENSION plpgsql RESTRICT`)
	d.refused(`DROP EXTENSION pg_trgm`, sql.CodeUndefinedObject, `extension "pg_trgm" does not exist`)
	if r := d.exec(`DROP EXTENSION IF EXISTS pg_trgm, pgcrypto`); len(r.Notices) != 1 || !strings.Contains(r.Notices[0], `extension "pg_trgm" does not exist, skipping`) {
		t.Fatalf("drop if exists: %+v", r.Notices)
	}
	d.expect(`SELECT extname, extversion FROM pg_extension ORDER BY extname`, listed)
	d.expect(`SELECT gen_random_uuid() IS NOT NULL, uuid_generate_v4() IS NOT NULL`, "t|t")
}
