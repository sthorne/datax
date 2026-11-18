package testcluster

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

// sharedCatalogs are the catalogs that describe the cluster rather than
// a database (PostgreSQL's shared catalogs and the views over them, and
// the node's sessions): they list every database's.
var sharedCatalogs = map[string]bool{
	"pg_catalog.pg_database":      true,
	"pg_catalog.pg_roles":         true,
	"pg_catalog.pg_user":          true,
	"pg_catalog.pg_authid":        true,
	"pg_catalog.pg_auth_members":  true,
	"pg_catalog.pg_tablespace":    true,
	"pg_catalog.pg_shdescription": true,
	"pg_catalog.pg_stat_activity": true,
}

func scopeConn(t *testing.T, ctx context.Context, tc *TestCluster, user, db string) *pgx.Conn {
	t.Helper()
	conn, err := pgx.Connect(ctx, fmt.Sprintf("postgres://%s@%s/%s?sslmode=disable", user, tc.Nodes[0].SQLAddr(), db))
	if err != nil {
		t.Fatalf("connect as %s to %s: %v", user, db, err)
	}
	t.Cleanup(func() { _ = conn.Close(context.Background()) })
	return conn
}

// TestCatalogsScopedToCurrentDatabase (issue #322): information_schema
// and the per-database pg_catalog tables describe the session's current
// database only, as each PostgreSQL database has its own catalogs; the
// shared catalogs list every database's. Every virtual table is read
// from each of two databases holding different objects, as an admin and
// as an ordinary role, and none may name the other database's objects.
func TestCatalogsScopedToCurrentDatabase(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()
	tc := privCluster(t)
	root := rlsConn(t, ctx, tc, 0, "root")
	for _, stmt := range []string{
		`CREATE DATABASE scope_a`,
		`CREATE DATABASE scope_b`,
		`CREATE USER scope_reader`,
		`GRANT CONNECT ON DATABASE scope_a TO scope_reader`,
		`GRANT CONNECT ON DATABASE scope_b TO scope_reader`,
	} {
		rlsExec(t, ctx, root, stmt)
	}
	// The same shapes in both, every name carrying its database's
	// marker, so a row from the other database is recognisable in any
	// column of any catalog.
	objects := func(m string) []string {
		return []string{
			fmt.Sprintf(`CREATE TYPE %s_mood AS ENUM ('%s_happy', '%s_sad')`, m, m, m),
			fmt.Sprintf(`CREATE TABLE %s_parent (id INT8 PRIMARY KEY, code TEXT UNIQUE, mood %s_mood, %s_note TEXT, CONSTRAINT %s_chk CHECK (id > 0))`, m, m, m, m),
			fmt.Sprintf(`CREATE TABLE %s_child (id INT8 PRIMARY KEY, pid INT8 DEFAULT 1, CONSTRAINT %s_fk FOREIGN KEY (pid) REFERENCES %s_parent (id))`, m, m, m),
			fmt.Sprintf(`CREATE INDEX %s_idx ON %s_child (pid)`, m, m),
			fmt.Sprintf(`CREATE VIEW %s_view AS SELECT id FROM %s_parent`, m, m),
			fmt.Sprintf(`CREATE SEQUENCE %s_seq`, m),
			fmt.Sprintf(`COMMENT ON TABLE %s_parent IS '%s_comment'`, m, m),
			fmt.Sprintf(`ALTER TABLE %s_parent ENABLE ROW LEVEL SECURITY`, m),
			fmt.Sprintf(`CREATE POLICY %s_pol ON %s_parent USING (true)`, m, m),
			fmt.Sprintf(`GRANT SELECT ON %s_parent TO scope_reader`, m),
			fmt.Sprintf(`GRANT SELECT ON %s_view TO scope_reader`, m),
		}
	}
	dbs := map[string]string{"scope_a": "zqa", "scope_b": "zqb"}
	for db, m := range dbs {
		c := scopeConn(t, ctx, tc, "root", db)
		for _, stmt := range objects(m) {
			rlsExec(t, ctx, c, stmt)
		}
	}

	for db, mine := range dbs {
		other := dbs["scope_a"]
		if db == "scope_a" {
			other = dbs["scope_b"]
		}
		for _, user := range []string{"root", "scope_reader"} {
			t.Run(db+"/"+user, func(t *testing.T) {
				c := scopeConn(t, ctx, tc, user, db)
				// Every virtual table there is: information_schema.tables
				// lists them as views.
				var names []string
				for _, r := range rlsRows(t, ctx, c, `SELECT table_schema, table_name FROM information_schema.tables
					WHERE table_schema IN ('pg_catalog', 'information_schema')`) {
					names = append(names, strings.Replace(r, "|", ".", 1))
				}
				if len(names) < 50 {
					t.Fatalf("only %d virtual tables listed: %v", len(names), names)
				}
				mineSeen := map[string]bool{}
				for _, name := range names {
					rows, err := rlsTryRows(ctx, c, `SELECT * FROM `+name)
					if err != nil && user != "root" && (strings.HasPrefix(name, "pg_catalog.datax_") || name == "pg_catalog.pg_authid") && rlsCode(err) == "42501" {
						continue // the admin-only catalogs: datax's own, and pg_authid
					}
					if err != nil {
						t.Fatalf("%s: %v", name, err)
					}
					all := strings.Join(rows, "\n")
					if strings.Contains(all, other) && !sharedCatalogs[name] {
						t.Errorf("%s in %s lists %s's objects:\n%s", name, db, other, grepLines(all, other))
					}
					if strings.Contains(all, mine) {
						mineSeen[name] = true
					}
				}
				// What the database holds is there, in the catalogs a
				// client introspects with.
				wantMine := []string{
					"information_schema.tables", "information_schema.columns", "information_schema.views",
					"information_schema.role_table_grants", "pg_catalog.pg_class", "pg_catalog.pg_attribute",
					"pg_catalog.pg_type", "pg_catalog.pg_enum", "pg_catalog.pg_description", "pg_catalog.pg_policy",
					"pg_catalog.pg_tables", "pg_catalog.pg_views", "pg_catalog.pg_indexes", "pg_catalog.pg_constraint",
				}
				if user == "root" {
					wantMine = append(wantMine, "information_schema.table_constraints", "information_schema.key_column_usage",
						"information_schema.referential_constraints", "information_schema.check_constraints",
						"information_schema.statistics", "pg_catalog.pg_sequences")
				}
				for _, name := range wantMine {
					if !mineSeen[name] {
						t.Errorf("%s in %s lists none of its objects", name, db)
					}
				}

				// The report's query: only this database's public tables,
				// under its name.
				want := []string{db + "|public|" + mine + "_child|BASE TABLE", db + "|public|" + mine + "_parent|BASE TABLE", db + "|public|" + mine + "_view|VIEW"}
				if user != "root" {
					// information_schema lists what the role holds a
					// privilege on (issue #312).
					want = []string{db + "|public|" + mine + "_parent|BASE TABLE", db + "|public|" + mine + "_view|VIEW"}
				}
				rlsWant(t, ctx, c, want, `SELECT table_catalog, table_schema, table_name, table_type FROM information_schema.tables WHERE table_schema = 'public' ORDER BY table_name`)
				rlsWant(t, ctx, c, []string{fmt.Sprint(len(want))}, `SELECT count(*) FROM information_schema.tables WHERE table_schema = 'public'`)
				rlsWant(t, ctx, c, []string{db}, `SELECT DISTINCT table_catalog FROM information_schema.columns WHERE table_schema = 'public'`)
				rlsWant(t, ctx, c, []string{db + "|information_schema", db + "|pg_catalog", db + "|public"},
					`SELECT catalog_name, schema_name FROM information_schema.schemata ORDER BY schema_name`)
				// pg_catalog lists every relation of this database, whatever
				// the reader's privileges (#312), and no other database's.
				rlsWant(t, ctx, c, []string{mine + "_child", mine + "_parent", mine + "_seq", mine + "_view"},
					`SELECT relname FROM pg_class WHERE relkind IN ('r', 'v', 'S') AND relnamespace = 2200 ORDER BY relname`)
				rlsWant(t, ctx, c, []string{mine + "_mood"}, `SELECT typname FROM pg_type WHERE typtype = 'e'`)
				// The shared catalogs are the cluster's.
				rlsWant(t, ctx, c, []string{"scope_a", "scope_b"}, `SELECT datname FROM pg_database WHERE datname LIKE 'scope_%' ORDER BY 1`)
				rlsWant(t, ctx, c, []string{"scope_reader"}, `SELECT rolname FROM pg_roles WHERE rolname = 'scope_reader'`)
			})
		}
	}
}
