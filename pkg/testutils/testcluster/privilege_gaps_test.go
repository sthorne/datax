package testcluster

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/sthorne/datax/pkg/server"
)

// Privilege checks that every table is owed, whatever its row security
// (issue #312). One single-node cluster serves every case: each needs
// a role, a table or two and a grant, and nothing here is about
// replication.

func privCluster(t *testing.T) *TestCluster {
	t.Helper()
	return StartWithOptions(t, 1, func(cfg *server.Config) { cfg.DescLeaseTTL = time.Second })
}

// privNotNaming asserts that a refusal's message mentions none of names.
func privNotNaming(t *testing.T, msg string, names ...string) {
	t.Helper()
	for _, n := range names {
		if strings.Contains(msg, n) {
			t.Fatalf("refusal names %q: %s", n, msg)
		}
	}
}

func TestPrivilegeGaps(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	tc := privCluster(t)
	root := rlsConn(t, ctx, tc, 0, "root")
	for _, stmt := range []string{
		`CREATE USER alice PASSWORD 'alice-pw-1'`,
		`CREATE TABLE secret (id INT8 PRIMARY KEY, v INT8)`,
		`INSERT INTO secret VALUES (1, 10), (2, 20), (3, 30)`,
		`CREATE TABLE open_t (id INT8 PRIMARY KEY, v INT8)`,
		`INSERT INTO open_t VALUES (1, 1)`,
		`GRANT SELECT ON open_t TO alice`,
		`ANALYZE secret`,
		`ANALYZE open_t`,
	} {
		rlsExec(t, ctx, root, stmt)
	}
	alice := rlsConn(t, ctx, tc, 0, "alice")

	t.Run("every role reads the catalogs", func(t *testing.T) {
		for _, stmt := range []string{
			`CREATE SEQUENCE secret_seq`,
			`SELECT nextval('secret_seq')`,
			`CREATE SEQUENCE open_seq`,
			`SELECT nextval('open_seq')`,
			`GRANT SELECT ON SEQUENCE open_seq TO alice`,
			`CREATE TABLE ins_only (id INT8 PRIMARY KEY)`,
			`GRANT INSERT ON ins_only TO alice`,
			`CREATE USER cat_bob PASSWORD 'bob-pw-123'`,
			`GRANT SELECT ON secret TO cat_bob`,
			`ALTER TABLE secret ENABLE ROW LEVEL SECURITY`,
			`CREATE POLICY bobs ON secret TO cat_bob USING (v > 10)`,
		} {
			rlsExec(t, ctx, root, stmt)
		}
		// pg_catalog lists every relation, as PostgreSQL's does.
		rlsWant(t, ctx, alice, []string{"ins_only|r", "open_t|r", "secret|r"},
			`SELECT relname, relkind FROM pg_class WHERE relname IN ('secret', 'open_t', 'ins_only') ORDER BY relname`)
		rlsWant(t, ctx, alice, []string{"id", "v"},
			`SELECT a.attname FROM pg_attribute a JOIN pg_class c ON a.attrelid = c.oid WHERE c.relname = 'secret' AND a.attnum > 0 ORDER BY a.attnum`)
		rlsWant(t, ctx, alice, []string{"secret|true"}, `SELECT tablename, rowsecurity FROM pg_tables WHERE tablename = 'secret'`)
		rlsWant(t, ctx, alice, []string{"secret_pkey"}, `SELECT indexname FROM pg_indexes WHERE tablename = 'secret'`)
		rlsWant(t, ctx, alice, []string{"bobs|{cat_bob}|v > 10"}, `SELECT policyname, roles, qual FROM pg_policies WHERE tablename = 'secret'`)
		rlsWant(t, ctx, alice, []string{"bobs"}, `SELECT polname FROM pg_policy`)
		// psql's \d, as an ordinary role.
		rlsRows(t, ctx, alice, `SELECT c.oid, n.nspname, c.relname FROM pg_catalog.pg_class c LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace WHERE c.relname OPERATOR(pg_catalog.~) '^(secret)$' AND pg_catalog.pg_table_is_visible(c.oid)`)
		// Statistics describe the rows: shown only where the role may read
		// them.
		rlsWant(t, ctx, alice, []string{"open_t|1", "secret|0"},
			`SELECT relname, reltuples FROM pg_class WHERE relname IN ('secret', 'open_t') ORDER BY relname`)
		rlsWant(t, ctx, alice, []string{"open_t|1", "secret|0"},
			`SELECT relname, n_live_tup FROM pg_stat_user_tables WHERE relname IN ('secret', 'open_t') ORDER BY relname`)
		// A sequence's counter likewise.
		rlsWant(t, ctx, alice, []string{"open_seq|true", "secret_seq|false"},
			`SELECT sequencename, last_value IS NOT NULL FROM pg_sequences WHERE sequencename IN ('open_seq', 'secret_seq') ORDER BY sequencename`)
		rlsWant(t, ctx, alice, []string{"true"}, `SELECT is_called FROM open_seq`)
		rlsRefused(t, ctx, alice, "42501", `SELECT last_value FROM secret_seq`)
		// information_schema shows what the role holds some privilege on.
		rlsWant(t, ctx, alice, []string{"ins_only", "open_t"},
			`SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' AND table_name IN ('secret', 'open_t', 'ins_only') ORDER BY table_name`)
		rlsWant(t, ctx, alice, []string{"id", "v"},
			`SELECT column_name FROM information_schema.columns WHERE table_name = 'open_t' ORDER BY ordinal_position`)
		rlsWant(t, ctx, alice, []string{}, `SELECT column_name FROM information_schema.columns WHERE table_name = 'secret'`)
		rlsWant(t, ctx, alice, []string{"alice|ins_only|INSERT", "alice|open_t|SELECT"},
			`SELECT grantee, table_name, privilege_type FROM information_schema.role_table_grants WHERE table_name IN ('secret', 'open_t', 'ins_only') ORDER BY table_name`)
		// Roles: every role is listed, no password with any of them.
		rlsWant(t, ctx, alice, []string{"alice|********", "cat_bob|********", "root|********"},
			`SELECT rolname, rolpassword FROM pg_roles WHERE rolname IN ('alice', 'cat_bob', 'root') ORDER BY rolname`)
		rlsWant(t, ctx, alice, []string{"cat_bob|********"}, `SELECT usename, passwd FROM pg_user WHERE usename = 'cat_bob'`)
		rlsRefused(t, ctx, alice, "42P01", `SELECT * FROM pg_authid`)
		rlsRefused(t, ctx, alice, "42P01", `SELECT * FROM pg_shadow`)
		// Another role's statement text stays hidden.
		for _, r := range rlsRows(t, ctx, alice, `SELECT usename, query FROM pg_stat_activity`) {
			if strings.HasPrefix(r, "root|") && r != "root|<insufficient privilege>" {
				t.Fatalf("pg_stat_activity shows root's statement to alice: %s", r)
			}
		}
		// The rows themselves stay behind their privileges.
		rlsRefused(t, ctx, alice, "42501", `SELECT * FROM secret`)
		rlsRefused(t, ctx, alice, "42501", `INSERT INTO pg_catalog.pg_class (relname) VALUES ('x')`)
		// Whoever reads the catalogs, joins and subqueries over them work.
		rlsWant(t, ctx, alice, []string{"secret"},
			`SELECT relname FROM pg_class WHERE oid IN (SELECT polrelid FROM pg_policy)`)
		rlsRows(t, ctx, alice, `EXPLAIN SELECT * FROM pg_class`)
	})

	t.Run("EXPLAIN takes SELECT", func(t *testing.T) {
		for _, q := range []string{
			`EXPLAIN SELECT * FROM secret`,
			`EXPLAIN SELECT v FROM secret WHERE id = 1`,
			`EXPLAIN SELECT * FROM open_t JOIN secret ON open_t.id = secret.id`,
			`EXPLAIN WITH s AS (SELECT * FROM secret) SELECT * FROM s`,
			`EXPLAIN SELECT * FROM open_t JOIN (SELECT id FROM secret) AS d ON d.id = open_t.id`,
			`EXPLAIN SELECT * FROM (SELECT id FROM secret) AS d`,
		} {
			msg := rlsRefused(t, ctx, alice, "42501", q)
			privNotNaming(t, msg, "rows]")
		}
		rlsRows(t, ctx, alice, `EXPLAIN SELECT * FROM open_t`)
		rlsRows(t, ctx, alice, `EXPLAIN WITH s AS (SELECT * FROM open_t) SELECT * FROM s`)
		// A view is the reader's to explain if it may read the view: the
		// view's query reads as its owner, as when it runs.
		rlsExec(t, ctx, root, `CREATE VIEW secret_ids AS SELECT id FROM secret`)
		rlsRefused(t, ctx, alice, "42501", `EXPLAIN SELECT * FROM secret_ids`)
		rlsExec(t, ctx, root, `GRANT SELECT ON secret_ids TO alice`)
		rlsRows(t, ctx, alice, `EXPLAIN SELECT * FROM secret_ids`)
		rlsRows(t, ctx, alice, `SELECT * FROM secret_ids`)
	})
}
