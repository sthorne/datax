package testcluster

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

// TestRoleDDLAfterDatabaseDescriptorWrite: DROP OWNED, DROP ROLE and
// REASSIGN OWNED write a database's descriptor when the role holds
// default privileges there or owns the database, and then ask the key
// service for the keys the role holds. The key service looked its table
// up in a transaction of its own, which waited on the statement's own
// write to the database descriptor, and the statement hung until its
// client gave up. Each statement here must answer, and do its work.
func TestRoleDDLAfterDatabaseDescriptorWrite(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()
	tc := privCluster(t)
	// Each case has a connection of its own: a hung statement takes its
	// connection with it.
	run := func(t *testing.T, c *pgx.Conn, stmt string) {
		t.Helper()
		sctx, scancel := context.WithTimeout(ctx, 20*time.Second)
		defer scancel()
		if _, err := c.Exec(sctx, stmt); err != nil {
			t.Fatalf("%s: %v", stmt, err)
		}
	}
	// defaults reports the grantees a new table is given.
	defaults := func(t *testing.T, c *pgx.Conn) string {
		t.Helper()
		run(t, c, `CREATE TABLE dflt_probe (id INT8 PRIMARY KEY)`)
		defer run(t, c, `DROP TABLE dflt_probe`)
		return strings.Join(rlsRows(t, ctx, c, `SELECT grantee FROM information_schema.role_table_grants WHERE table_name = 'dflt_probe'`), " ")
	}

	t.Run("drop owned with default privileges on tables", func(t *testing.T) {
		c := rlsConn(t, ctx, tc, 0, "root")
		run(t, c, `CREATE ROLE dp_a`)
		run(t, c, `ALTER DEFAULT PRIVILEGES GRANT SELECT ON TABLES TO dp_a`)
		if got := defaults(t, c); !strings.Contains(got, "dp_a") {
			t.Fatalf("default privilege not applied: %q", got)
		}
		run(t, c, `DROP OWNED BY dp_a`)
		if got := defaults(t, c); strings.Contains(got, "dp_a") {
			t.Fatalf("DROP OWNED left the default privilege: %q", got)
		}
		run(t, c, `DROP ROLE dp_a`)
	})
	t.Run("drop owned with default privileges on sequences", func(t *testing.T) {
		c := rlsConn(t, ctx, tc, 0, "root")
		run(t, c, `CREATE ROLE dp_b`)
		run(t, c, `ALTER DEFAULT PRIVILEGES GRANT USAGE ON SEQUENCES TO dp_b`)
		run(t, c, `DROP OWNED BY dp_b`)
		run(t, c, `DROP ROLE dp_b`)
	})
	t.Run("drop owned for the role whose defaults they are", func(t *testing.T) {
		c := rlsConn(t, ctx, tc, 0, "root")
		run(t, c, `CREATE ROLE dp_c`)
		run(t, c, `CREATE ROLE dp_c2`)
		run(t, c, `ALTER DEFAULT PRIVILEGES FOR ROLE dp_c GRANT SELECT ON TABLES TO dp_c2`)
		run(t, c, `DROP OWNED BY dp_c`)
		run(t, c, `DROP ROLE dp_c`)
	})
	t.Run("drop role with default privileges", func(t *testing.T) {
		c := rlsConn(t, ctx, tc, 0, "root")
		run(t, c, `CREATE ROLE dp_d`)
		run(t, c, `ALTER DEFAULT PRIVILEGES GRANT SELECT ON TABLES TO dp_d`)
		run(t, c, `DROP ROLE dp_d`)
		if got := defaults(t, c); strings.Contains(got, "dp_d") {
			t.Fatalf("DROP ROLE left the default privilege: %q", got)
		}
	})
	t.Run("reassign owned of a database", func(t *testing.T) {
		c := rlsConn(t, ctx, tc, 0, "root")
		run(t, c, `CREATE ROLE dp_e`)
		run(t, c, `CREATE ROLE dp_e2`)
		run(t, c, `CREATE DATABASE dp_e_db`)
		run(t, c, `ALTER DATABASE dp_e_db OWNER TO dp_e`)
		run(t, c, `REASSIGN OWNED BY dp_e TO dp_e2`)
		rlsWant(t, ctx, c, []string{"dp_e2"}, `SELECT r.rolname FROM pg_database d JOIN pg_roles r ON d.datdba = r.oid WHERE d.datname = 'dp_e_db'`)
	})
}
