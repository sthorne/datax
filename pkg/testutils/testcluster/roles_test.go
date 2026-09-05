package testcluster

import (
	"context"
	"errors"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/sthorne/datax/pkg/base"
	"github.com/sthorne/datax/pkg/cluster"
	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/server"
	"github.com/sthorne/datax/pkg/sql"
	"github.com/sthorne/datax/pkg/sql/catalog"
	"github.com/sthorne/datax/pkg/version"
)

// userSession opens an in-process session for user on node, with a
// leased catalog accessor of its own (grants ride descriptor leases).
func userSession(t *testing.T, tc *TestCluster, node int, user string) *sql.Session {
	t.Helper()
	_, cat := leasedSessionWithAccessor(t, tc, node, 2*time.Second)
	return sql.NewSessionForUser(tc.Nodes[node].DB(), cat, user)
}

// expectCodeSQL runs stmt and requires the SQLSTATE.
func expectCodeSQL(t *testing.T, ctx context.Context, s *sql.Session, stmt, code string) {
	t.Helper()
	_, serr := trySQL(ctx, s, stmt)
	if serr == nil || serr.Code != code {
		t.Fatalf("%s: expected %s, got %v", stmt, code, serr)
	}
}

// TestRoles (issue #98): roles as groups with inheritance and SET ROLE,
// ownership, the privilege scopes (database, schema, ALL TABLES,
// sequences, default privileges, grant options, PUBLIC), definer views,
// the built-in roles, and the catalogs that render them.
func TestRoles(t *testing.T) {
	tc := Start(t, 3)
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()
	root := leasedSession(t, tc, 0, 2*time.Second)
	waitForDatabases(t, ctx, root)

	// ---- roles, membership, inheritance ----
	execSQL(t, ctx, root, `CREATE ROLE app_readers`)
	execSQL(t, ctx, root, `CREATE USER alice PASSWORD 'pw12345' IN ROLE app_readers`)
	execSQL(t, ctx, root, `CREATE USER bob WITH LOGIN PASSWORD 'pw12345' NOINHERIT`)
	execSQL(t, ctx, root, `CREATE USER carol PASSWORD 'pw12345'`)
	expectCodeSQL(t, ctx, root, `CREATE ROLE app_readers`, sql.CodeDuplicateObject)
	execSQL(t, ctx, root, `CREATE ROLE IF NOT EXISTS app_readers`)
	expectCodeSQL(t, ctx, root, `CREATE ROLE admin`, sql.CodeInvalidParameterValue)
	expectCodeSQL(t, ctx, root, `CREATE ROLE node`, sql.CodeInvalidParameterValue)
	expectCodeSQL(t, ctx, root, `GRANT nosuch TO alice`, sql.CodeUndefinedObject)
	expectCodeSQL(t, ctx, root, `GRANT app_readers TO nosuch`, sql.CodeUndefinedObject)
	expectCodeSQL(t, ctx, root, `GRANT ADMIN TO root`, sql.CodeFeatureNotSupported)

	execSQL(t, ctx, root, `CREATE TABLE t (id INT8 PRIMARY KEY, v INT8)`)
	execSQL(t, ctx, root, `INSERT INTO t VALUES (1, 10), (2, 20)`)
	execSQL(t, ctx, root, `GRANT SELECT ON t TO app_readers`)

	alice := userSession(t, tc, 1, "alice")
	bob := userSession(t, tc, 2, "bob")
	carol := userSession(t, tc, 1, "carol")
	if r := execEventually(t, ctx, alice, `SELECT id FROM t ORDER BY id`); len(r.Rows) != 2 {
		t.Fatalf("inherited select: %+v", r.Rows)
	}
	denied(t, ctx, alice, `INSERT INTO t VALUES (3, 30)`)
	denied(t, ctx, carol, `SELECT id FROM t`)

	// NOINHERIT: bob holds app_readers only through SET ROLE.
	execSQL(t, ctx, root, `GRANT app_readers TO bob`)
	denied(t, ctx, bob, `SELECT id FROM t`)
	execSQL(t, ctx, bob, `SET ROLE app_readers`)
	if r := execEventually(t, ctx, bob, `SELECT id FROM t`); len(r.Rows) != 2 {
		t.Fatalf("select after SET ROLE: %+v", r.Rows)
	}
	if r := execSQL(t, ctx, bob, `SELECT current_user, session_user`); r.Rows[0][0].S != "app_readers" || r.Rows[0][1].S != "bob" {
		t.Fatalf("current_user / session_user: %+v", r.Rows)
	}
	if r := execSQL(t, ctx, bob, `SHOW role`); r.Rows[0][0].S != "app_readers" {
		t.Fatalf("SHOW role: %+v", r.Rows)
	}
	execSQL(t, ctx, bob, `RESET ROLE`)
	denied(t, ctx, bob, `SELECT id FROM t`)
	if r := execSQL(t, ctx, bob, `SHOW role`); r.Rows[0][0].S != "none" {
		t.Fatalf("SHOW role after RESET: %+v", r.Rows)
	}
	expectCodeSQL(t, ctx, bob, `SET ROLE admin`, sql.CodeInsufficientPriv)
	expectCodeSQL(t, ctx, bob, `SET ROLE nosuch`, sql.CodeInvalidParameterValue)
	// SET LOCAL ROLE lasts for the block.
	execSQL(t, ctx, bob, `BEGIN`)
	execSQL(t, ctx, bob, `SET LOCAL ROLE app_readers`)
	execSQL(t, ctx, bob, `SELECT id FROM t`)
	execSQL(t, ctx, bob, `COMMIT`)
	denied(t, ctx, bob, `SELECT id FROM t`)
	// A cycle is refused; a role cannot join itself.
	execSQL(t, ctx, root, `CREATE ROLE outer_r`)
	execSQL(t, ctx, root, `GRANT app_readers TO outer_r`)
	expectCodeSQL(t, ctx, root, `GRANT outer_r TO app_readers`, sql.CodeInvalidGrantOperation)
	expectCodeSQL(t, ctx, root, `GRANT outer_r TO outer_r`, sql.CodeInvalidGrantOperation)
	execSQL(t, ctx, root, `REVOKE app_readers FROM outer_r`)

	// The admin role, granted and revoked at runtime; WITH ADMIN OPTION
	// lets a member hand the role on.
	execSQL(t, ctx, root, `GRANT admin TO alice`)
	execSQL(t, ctx, alice, `CREATE TABLE hers (id INT8 PRIMARY KEY)`)
	execSQL(t, ctx, root, `REVOKE admin FROM alice`)
	denied(t, ctx, alice, `CREATE TABLE more (id INT8 PRIMARY KEY)`)
	denied(t, ctx, alice, `GRANT app_readers TO carol`)
	execSQL(t, ctx, root, `GRANT app_readers TO alice WITH ADMIN OPTION`)
	execSQL(t, ctx, alice, `GRANT app_readers TO carol`)
	if r := execEventually(t, ctx, carol, `SELECT id FROM t`); len(r.Rows) != 2 {
		t.Fatalf("carol via alice's admin option: %+v", r.Rows)
	}
	execSQL(t, ctx, alice, `REVOKE app_readers FROM carol`)
	awaitDenied(t, ctx, carol, `SELECT id FROM t`)
	execSQL(t, ctx, root, `REVOKE ADMIN OPTION FOR app_readers FROM alice`)
	denied(t, ctx, alice, `GRANT app_readers TO carol`)

	// ---- ownership ----
	execSQL(t, ctx, root, `GRANT CREATE ON DATABASE datax TO carol`)
	execEventually(t, ctx, carol, `CREATE TABLE ct (id INT8 PRIMARY KEY, v INT8)`)
	execSQL(t, ctx, carol, `INSERT INTO ct VALUES (1, 1)`)
	execSQL(t, ctx, carol, `ALTER TABLE ct ADD COLUMN w INT8`)
	execSQL(t, ctx, carol, `CREATE INDEX ct_v ON ct (v)`)
	execSQL(t, ctx, carol, `COMMENT ON TABLE ct IS 'carol''s'`)
	execSQL(t, ctx, carol, `GRANT SELECT ON ct TO alice`) // the owner grants
	if r := execEventually(t, ctx, alice, `SELECT id FROM ct`); len(r.Rows) != 1 {
		t.Fatalf("alice reads ct: %+v", r.Rows)
	}
	denied(t, ctx, alice, `ALTER TABLE ct ADD COLUMN x INT8`)
	denied(t, ctx, alice, `DROP TABLE ct`)
	denied(t, ctx, alice, `GRANT SELECT ON ct TO bob`)
	denied(t, ctx, alice, `TRUNCATE ct`)
	if r := execSQL(t, ctx, root, `SELECT tableowner FROM pg_tables WHERE tablename = 'ct'`); len(r.Rows) != 1 || r.Rows[0][0].S != "carol" {
		t.Fatalf("pg_tables.tableowner: %+v", r.Rows)
	}
	// DROP ROLE refuses while carol owns ct; REASSIGN moves it.
	expectCodeSQL(t, ctx, root, `DROP ROLE carol`, sql.CodeDependentObjectsExist)
	denied(t, ctx, alice, `ALTER TABLE ct OWNER TO alice`)
	expectCodeSQL(t, ctx, carol, `ALTER TABLE ct OWNER TO bob`, sql.CodeInsufficientPriv) // carol is no member of bob
	execSQL(t, ctx, root, `REASSIGN OWNED BY carol TO alice`)
	execEventually(t, ctx, alice, `ALTER TABLE ct ADD COLUMN x INT8`)
	awaitDenied(t, ctx, carol, `COMMENT ON TABLE ct IS 'nope'`)
	execSQL(t, ctx, root, `ALTER TABLE ct OWNER TO carol`)
	execEventually(t, ctx, carol, `COMMENT ON TABLE ct IS 'mine again'`)
	// DROP OWNED drops carol's objects and revokes her grants; then the
	// role goes.
	execSQL(t, ctx, carol, `CREATE TABLE ct2 (id INT8 PRIMARY KEY)`)
	execSQL(t, ctx, carol, `CREATE VIEW cv AS SELECT id FROM ct2`)
	execSQL(t, ctx, root, `CREATE SEQUENCE carol_seq`)
	execSQL(t, ctx, root, `ALTER SEQUENCE carol_seq OWNER TO carol`)
	execSQL(t, ctx, root, `DROP OWNED BY carol`)
	expectCodeSQL(t, ctx, root, `SELECT id FROM ct`, sql.CodeUndefinedTable)
	expectCodeSQL(t, ctx, root, `SELECT id FROM cv`, sql.CodeUndefinedTable)
	expectCodeSQL(t, ctx, root, `SELECT nextval('carol_seq')`, sql.CodeUndefinedTable)
	if r := execSQL(t, ctx, root, `SHOW GRANTS ON DATABASE datax FOR carol`); len(r.Rows) != 0 {
		t.Fatalf("carol's database grant survived DROP OWNED: %+v", r.Rows)
	}
	execSQL(t, ctx, root, `DROP ROLE carol`)
	expectCodeSQL(t, ctx, root, `DROP ROLE carol`, sql.CodeUndefinedObject)
	execSQL(t, ctx, root, `DROP ROLE IF EXISTS carol`)
	expectCodeSQL(t, ctx, root, `DROP ROLE root`, sql.CodeFeatureNotSupported)
	expectCodeSQL(t, ctx, root, `DROP ROLE admin`, sql.CodeFeatureNotSupported)
	// Dropping a role a table is granted to just drops the grant.
	execSQL(t, ctx, root, `CREATE USER dave PASSWORD 'pw12345'`)
	execSQL(t, ctx, root, `GRANT SELECT ON t TO dave`)
	execSQL(t, ctx, root, `DROP USER dave`)
	if r := execSQL(t, ctx, root, `SHOW GRANTS ON t FOR dave`); len(r.Rows) != 0 {
		t.Fatalf("dave's grant survived DROP USER: %+v", r.Rows)
	}

	// ---- scopes ----
	execSQL(t, ctx, root, `CREATE TABLE t2 (id INT8 PRIMARY KEY)`)
	execSQL(t, ctx, root, `INSERT INTO t2 VALUES (1)`)
	execSQL(t, ctx, root, `GRANT USAGE ON SCHEMA public TO bob`)
	execSQL(t, ctx, root, `GRANT ALL ON ALL TABLES IN SCHEMA public TO app_readers`)
	execEventually(t, ctx, alice, `INSERT INTO t2 VALUES (2)`)
	execSQL(t, ctx, alice, `TRUNCATE t2`)
	execSQL(t, ctx, root, `REVOKE INSERT, UPDATE, DELETE, TRUNCATE ON ALL TABLES IN SCHEMA public FROM app_readers`)
	awaitDenied(t, ctx, alice, `DELETE FROM t2 WHERE id = 999`)
	// PUBLIC.
	execSQL(t, ctx, root, `GRANT SELECT ON t2 TO PUBLIC`)
	execEventually(t, ctx, bob, `SELECT id FROM t2`)
	execSQL(t, ctx, root, `REVOKE SELECT ON t2 FROM PUBLIC`)
	awaitDenied(t, ctx, bob, `SELECT id FROM t2`)
	// WITH GRANT OPTION: bob may pass SELECT on, and nothing else.
	execSQL(t, ctx, root, `GRANT SELECT ON t TO bob WITH GRANT OPTION`)
	execSQL(t, ctx, root, `CREATE USER erin PASSWORD 'pw12345'`)
	erin := userSession(t, tc, 2, "erin")
	execEventually(t, ctx, bob, `GRANT SELECT ON t TO erin`)
	denied(t, ctx, bob, `GRANT INSERT ON t TO erin`)
	if r := execEventually(t, ctx, erin, `SELECT id FROM t`); len(r.Rows) != 2 {
		t.Fatalf("erin via bob's grant option: %+v", r.Rows)
	}
	if r := execSQL(t, ctx, root, `SHOW GRANTS ON t FOR bob`); len(r.Rows) != 1 || r.Rows[0][5].S != "YES" {
		t.Fatalf("SHOW GRANTS is_grantable: %+v", r.Rows)
	}
	if r := execSQL(t, ctx, root, `SELECT grantor, is_grantable FROM information_schema.role_table_grants WHERE grantee = 'bob' AND table_name = 't'`); len(r.Rows) != 1 || r.Rows[0][0].S != "root" || r.Rows[0][1].S != "YES" {
		t.Fatalf("role_table_grants: %+v", r.Rows)
	}
	execSQL(t, ctx, root, `REVOKE GRANT OPTION FOR SELECT ON t FROM bob`)
	awaitDenied(t, ctx, bob, `GRANT SELECT ON t TO erin`)
	execSQL(t, ctx, bob, `SELECT id FROM t`) // the privilege itself stays
	// Sequences: USAGE for nextval; a SERIAL column's sequence follows
	// INSERT on the table.
	execSQL(t, ctx, root, `CREATE SEQUENCE s1`)
	denied(t, ctx, alice, `SELECT nextval('s1')`)
	execSQL(t, ctx, root, `GRANT USAGE ON SEQUENCE s1 TO alice`)
	execSQL(t, ctx, alice, `SELECT nextval('s1')`)
	denied(t, ctx, alice, `SELECT setval('s1', 100)`)
	execSQL(t, ctx, root, `CREATE TABLE ser (id SERIAL PRIMARY KEY, v INT8)`)
	execSQL(t, ctx, root, `GRANT INSERT, SELECT ON ser TO alice`)
	execEventually(t, ctx, alice, `INSERT INTO ser (v) VALUES (1)`)
	execSQL(t, ctx, alice, `SELECT currval('ser_id_seq')`)
	denied(t, ctx, bob, `SELECT nextval('ser_id_seq')`)
	execSQL(t, ctx, root, `GRANT SELECT ON ALL SEQUENCES IN SCHEMA public TO bob`)
	denied(t, ctx, bob, `SELECT nextval('s1')`)
	// Default privileges apply to tables created afterwards.
	execSQL(t, ctx, root, `ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO app_readers`)
	execSQL(t, ctx, root, `CREATE TABLE later (id INT8 PRIMARY KEY)`)
	if r := execEventually(t, ctx, alice, `SELECT id FROM later`); len(r.Rows) != 0 {
		t.Fatalf("default privilege: %+v", r.Rows)
	}
	if r := execSQL(t, ctx, root, `SHOW GRANTS ON later`); len(r.Rows) != 1 || r.Rows[0][3].S != "app_readers" || r.Rows[0][4].S != "SELECT" {
		t.Fatalf("SHOW GRANTS on a table with default privileges: %+v", r.Rows)
	}
	execSQL(t, ctx, root, `ALTER DEFAULT PRIVILEGES IN SCHEMA public REVOKE SELECT ON TABLES FROM app_readers`)
	execSQL(t, ctx, root, `CREATE TABLE later2 (id INT8 PRIMARY KEY)`)
	denied(t, ctx, alice, `SELECT id FROM later2`)
	// Database scope: CREATE on the database, and CONNECT revoked from
	// PUBLIC.
	execSQL(t, ctx, root, `CREATE DATABASE app`)
	execSQL(t, ctx, root, `REVOKE CONNECT ON DATABASE app FROM PUBLIC`)
	if serr := alice.UseDatabase(ctx, "app"); serr == nil || serr.Code != sql.CodeInsufficientPriv {
		t.Fatalf("USE app without CONNECT: %v", serr)
	}
	execSQL(t, ctx, root, `GRANT CONNECT, CREATE ON DATABASE app TO app_readers`)
	if serr := alice.UseDatabase(ctx, "app"); serr != nil {
		t.Fatalf("USE app with inherited CONNECT: %v", serr)
	}
	execSQL(t, ctx, alice, `CREATE TABLE app.mine (id INT8 PRIMARY KEY)`)
	if r := execSQL(t, ctx, root, `SHOW GRANTS ON DATABASE app`); len(r.Rows) != 2 || r.Rows[0][3].S != "app_readers" {
		t.Fatalf("SHOW GRANTS ON DATABASE: %+v", r.Rows)
	}
	execSQL(t, ctx, root, `ALTER DATABASE app OWNER TO alice`)
	execSQL(t, ctx, alice, `ALTER DATABASE app RENAME TO app2`)
	denied(t, ctx, bob, `DROP DATABASE app2`)
	if serr := alice.UseDatabase(ctx, "datax"); serr != nil {
		t.Fatalf("back to datax: %v", serr)
	}
	// Schema USAGE revoked from PUBLIC closes the schema to non-holders.
	execSQL(t, ctx, root, `REVOKE USAGE ON SCHEMA public FROM PUBLIC`)
	awaitDenied(t, ctx, erin, `SELECT id FROM t`)
	execSQL(t, ctx, root, `GRANT USAGE ON SCHEMA public TO erin`)
	execEventually(t, ctx, erin, `SELECT id FROM t`)
	execSQL(t, ctx, root, `GRANT USAGE ON SCHEMA public TO PUBLIC`)

	// ---- views run as their owner ----
	execSQL(t, ctx, root, `CREATE TABLE secret (id INT8 PRIMARY KEY, s TEXT)`)
	execSQL(t, ctx, root, `INSERT INTO secret VALUES (1, 'x')`)
	execSQL(t, ctx, root, `CREATE VIEW v AS SELECT id FROM secret`)
	execSQL(t, ctx, root, `GRANT SELECT ON v TO erin`)
	if r := execEventually(t, ctx, erin, `SELECT id FROM v`); len(r.Rows) != 1 {
		t.Fatalf("definer view: %+v", r.Rows)
	}
	denied(t, ctx, erin, `SELECT id FROM secret`)
	denied(t, ctx, erin, `SELECT id FROM v JOIN secret USING (id)`)
	execSQL(t, ctx, root, `CREATE USER frank PASSWORD 'pw12345'`)
	execSQL(t, ctx, root, `GRANT CREATE ON DATABASE datax TO frank`)
	frank := userSession(t, tc, 1, "frank")
	denied(t, ctx, frank, `CREATE VIEW fv AS SELECT id FROM secret`) // frank cannot read secret
	execSQL(t, ctx, root, `GRANT SELECT ON v TO frank`)
	execEventually(t, ctx, frank, `CREATE VIEW fv AS SELECT id FROM v`)
	execSQL(t, ctx, root, `REVOKE SELECT ON v FROM frank`)
	awaitDenied(t, ctx, frank, `SELECT id FROM fv`) // fv runs as frank, who lost v

	// ---- built-in roles ----
	execSQL(t, ctx, root, `ALTER ROLE bob INHERIT`) // NOINHERIT until now
	execSQL(t, ctx, root, `GRANT read_all TO bob`)
	if r := execSQL(t, ctx, bob, `SELECT s FROM secret`); len(r.Rows) != 1 {
		t.Fatalf("read_all: %+v", r.Rows)
	}
	denied(t, ctx, bob, `INSERT INTO secret VALUES (2, 'y')`)
	execSQL(t, ctx, root, `GRANT write_all TO bob`)
	execSQL(t, ctx, bob, `INSERT INTO secret VALUES (2, 'y')`)
	denied(t, ctx, bob, `DROP TABLE secret`)
	execSQL(t, ctx, root, `REVOKE read_all, write_all FROM bob`)
	execSQL(t, ctx, root, `CREATE USER scrape PASSWORD 'pw12345' IN ROLE metrics`)
	scrape := userSession(t, tc, 2, "scrape")
	denied(t, ctx, scrape, `SELECT s FROM secret`)
	denied(t, ctx, scrape, `SELECT id FROM t`)

	// ---- catalogs ----
	if r := execSQL(t, ctx, root, `SHOW ROLES`); len(r.Rows) < 8 || r.Rows[0][0].S != "root" {
		t.Fatalf("SHOW ROLES: %+v", r.Rows)
	}
	users := execSQL(t, ctx, root, `SHOW USERS`)
	for _, row := range users.Rows {
		if row[0].S == "app_readers" || row[0].S == "admin" {
			t.Fatalf("SHOW USERS lists a group: %+v", users.Rows)
		}
	}
	if r := execSQL(t, ctx, root, `SHOW GRANTS ON ROLE app_readers`); len(r.Rows) != 2 || r.Rows[0][1].S != "alice" || r.Rows[1][1].S != "bob" {
		t.Fatalf("SHOW GRANTS ON ROLE: %+v", r.Rows)
	}
	if r := execSQL(t, ctx, root, `SHOW GRANTS ON ROLE FOR scrape`); len(r.Rows) != 1 || r.Rows[0][0].S != "metrics" {
		t.Fatalf("SHOW GRANTS ON ROLE FOR: %+v", r.Rows)
	}
	if r := execSQL(t, ctx, root, `SELECT rolname, rolcanlogin, rolinherit FROM pg_roles WHERE rolname IN ('bob', 'app_readers') ORDER BY rolname`); len(r.Rows) != 2 ||
		r.Rows[0][1].B || !r.Rows[0][2].B || !r.Rows[1][1].B || !r.Rows[1][2].B {
		t.Fatalf("pg_roles: %+v", r.Rows)
	}
	if r := execSQL(t, ctx, root, `SELECT b.rolname FROM pg_auth_members m JOIN pg_roles b ON m.roleid = b.oid JOIN pg_roles r ON m.member = r.oid WHERE r.rolname = 'scrape'`); len(r.Rows) != 1 || r.Rows[0][0].S != "metrics" {
		t.Fatalf("pg_auth_members: %+v", r.Rows)
	}
	// psql's \du query shape.
	if r := execSQL(t, ctx, root, `SELECT r.rolname, ARRAY(SELECT b.rolname FROM pg_catalog.pg_auth_members m JOIN pg_catalog.pg_roles b ON (m.roleid = b.oid) WHERE m.member = r.oid) AS memberof FROM pg_catalog.pg_roles r WHERE r.rolname = 'alice'`); len(r.Rows) != 1 || len(r.Rows[0][1].A) != 1 || r.Rows[0][1].A[0].S != "app_readers" {
		t.Fatalf("\\du memberof: %+v", r.Rows)
	}
	if r := execSQL(t, ctx, root, `SELECT usename FROM pg_user WHERE usename = 'app_readers'`); len(r.Rows) != 0 {
		t.Fatalf("pg_user lists a group: %+v", r.Rows)
	}
	// Dropping a group removes its members' memberships.
	execSQL(t, ctx, root, `DROP ROLE app_readers`)
	awaitDenied(t, ctx, alice, `SELECT id FROM t`)
	if r := execSQL(t, ctx, root, `SHOW GRANTS ON ROLE FOR alice`); len(r.Rows) != 0 {
		t.Fatalf("membership survived DROP ROLE: %+v", r.Rows)
	}
}

// TestRolePrivilegeMatrix (issue #98): a table of statement × role ×
// expected SQLSTATE, run through pgwire against a secure cluster; LOGIN
// and NOLOGIN at the door; SET ROLE over the wire.
func TestRolePrivilegeMatrix(t *testing.T) {
	tc, certsDir := startSecureCluster(t, "topsecret")
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()

	var root *pgx.Conn
	deadline := time.Now().Add(30 * time.Second)
	for {
		var err error
		root, err = connectSecure(ctx, secureURL(tc, certsDir, "root", "topsecret"))
		if err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("root could never authenticate: %v", err)
		}
		time.Sleep(200 * time.Millisecond)
	}
	defer func() { _ = root.Close(ctx) }()
	for _, q := range []string{
		`CREATE TABLE m (id INT8 PRIMARY KEY, v INT8)`,
		`INSERT INTO m VALUES (1, 1)`,
		`CREATE ROLE app LOGIN PASSWORD 'app-pw'`,
		`CREATE USER reader PASSWORD 'reader-pw' IN ROLE read_all`,
		`CREATE USER writer PASSWORD 'writer-pw' IN ROLE write_all`,
		`CREATE USER owner_u PASSWORD 'owner-pw'`,
		`CREATE ROLE grp PASSWORD 'grp-pw'`,
		`CREATE USER scrape PASSWORD 'scrape-pw' IN ROLE metrics`,
		`GRANT SELECT ON m TO app`,
		`GRANT CREATE ON DATABASE datax TO owner_u`,
		`GRANT app TO owner_u`,
	} {
		if _, err := root.Exec(ctx, q); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}

	conns := map[string]*pgx.Conn{}
	connect := func(user, pw string) *pgx.Conn {
		t.Helper()
		if c, ok := conns[user]; ok {
			return c
		}
		c, err := connectSecure(ctx, secureURL(tc, certsDir, user, pw))
		if err != nil {
			t.Fatalf("%s connect: %v", user, err)
		}
		conns[user] = c
		return c
	}
	defer func() {
		for _, c := range conns {
			_ = c.Close(ctx)
		}
	}()
	passwords := map[string]string{"app": "app-pw", "reader": "reader-pw", "writer": "writer-pw", "owner_u": "owner-pw", "scrape": "scrape-pw", "root": "topsecret"}

	// NOLOGIN roles cannot open a session, whatever the password.
	if _, err := connectSecure(ctx, secureURL(tc, certsDir, "grp", "grp-pw")); err == nil {
		t.Fatal("NOLOGIN role logged in")
	}
	if _, err := root.Exec(ctx, `ALTER ROLE app NOLOGIN`); err != nil {
		t.Fatal(err)
	}
	if _, err := connectSecure(ctx, secureURL(tc, certsDir, "app", "app-pw")); err == nil {
		t.Fatal("role altered to NOLOGIN logged in")
	}
	if _, err := root.Exec(ctx, `ALTER ROLE app LOGIN`); err != nil {
		t.Fatal(err)
	}

	type step struct{ user, stmt, code string }
	matrix := []step{
		{"app", `SELECT id FROM m`, ""},
		{"app", `INSERT INTO m VALUES (2, 2)`, "42501"},
		{"app", `UPDATE m SET v = 0 WHERE id = 1`, "42501"},
		{"app", `DELETE FROM m WHERE id = 1`, "42501"},
		{"app", `TRUNCATE m`, "42501"},
		{"app", `CREATE TABLE nope (id INT8 PRIMARY KEY)`, "42501"},
		{"app", `ALTER TABLE m ADD COLUMN w INT8`, "42501"},
		{"app", `DROP TABLE m`, "42501"},
		{"app", `CREATE INDEX m_v ON m (v)`, "42501"},
		{"app", `GRANT SELECT ON m TO reader`, "42501"},
		{"app", `CREATE ROLE x`, "42501"},
		{"app", `CREATE USER x PASSWORD 'x'`, "42501"},
		{"app", `DROP ROLE reader`, "42501"},
		{"app", `GRANT admin TO app`, "42501"},
		{"app", `GRANT read_all TO app`, "42501"},
		{"app", `SET ROLE admin`, "42501"},
		{"app", `SET ROLE owner_u`, "42501"},
		{"app", `ALTER TABLE m OWNER TO app`, "42501"},
		{"app", `REASSIGN OWNED BY root TO app`, "0A000"},
		{"app", `ALTER DEFAULT PRIVILEGES FOR ROLE root GRANT SELECT ON TABLES TO app`, "42501"},
		{"app", `ALTER ROLE app PASSWORD 'app-pw'`, ""}, // one's own password
		{"app", `ALTER ROLE app NOLOGIN`, "42501"},
		{"app", `ALTER ROLE reader PASSWORD 'x'`, "42501"},
		{"reader", `SELECT id FROM m`, ""},
		{"reader", `INSERT INTO m VALUES (3, 3)`, "42501"},
		{"reader", `CREATE TABLE nope (id INT8 PRIMARY KEY)`, "42501"},
		{"writer", `INSERT INTO m VALUES (3, 3)`, ""},
		{"writer", `DELETE FROM m WHERE id = 3`, ""},
		{"writer", `SELECT id FROM m`, "42501"},
		{"scrape", `SELECT id FROM m`, "42501"},
		{"scrape", `SELECT 1`, ""},
		{"owner_u", `CREATE TABLE mine (id INT8 PRIMARY KEY, v INT8)`, ""},
		{"owner_u", `INSERT INTO mine VALUES (1, 1)`, ""},
		{"owner_u", `CREATE INDEX mine_v ON mine (v)`, ""},
		{"owner_u", `GRANT SELECT ON mine TO reader`, ""},
		{"owner_u", `ALTER TABLE mine OWNER TO app`, ""},      // owner_u is a member of app
		{"owner_u", `ALTER TABLE mine ADD COLUMN w INT8`, ""}, // still a member of the owner
		{"owner_u", `SET ROLE app`, ""},
		{"owner_u", `SELECT current_user`, ""},
		{"owner_u", `DROP TABLE mine`, ""},
		{"owner_u", `RESET ROLE`, ""},
		{"owner_u", `DROP TABLE m`, "42501"},
		{"owner_u", `ALTER TABLE m ADD COLUMN w INT8`, "42501"},
		{"owner_u", `CREATE DATABASE other`, "42501"},
		{"root", `CREATE DATABASE other`, ""},
		{"root", `DROP DATABASE other`, ""},
	}
	for _, st := range matrix {
		c := connect(st.user, passwords[st.user])
		_, err := c.Exec(ctx, st.stmt)
		var pgErr *pgconn.PgError
		switch {
		case st.code == "" && err != nil:
			t.Fatalf("%s as %s: unexpected %v", st.stmt, st.user, err)
		case st.code != "" && err == nil:
			t.Fatalf("%s as %s: accepted, want %s", st.stmt, st.user, st.code)
		case st.code != "" && (!errors.As(err, &pgErr) || pgErr.Code != st.code):
			t.Fatalf("%s as %s: %v, want %s", st.stmt, st.user, err, st.code)
		}
	}
	// SET ROLE over the wire: current_user follows, session_user stays.
	ou := connect("owner_u", "owner-pw")
	if _, err := ou.Exec(ctx, `SET ROLE app`); err != nil {
		t.Fatal(err)
	}
	var cur, sess string
	if err := ou.QueryRow(ctx, `SELECT current_user, session_user`).Scan(&cur, &sess); err != nil || cur != "app" || sess != "owner_u" {
		t.Fatalf("current_user/session_user over the wire: %s/%s %v", cur, sess, err)
	}
	// The dropped role's session may go on; a new login is refused.
	if _, err := root.Exec(ctx, `DROP OWNED BY writer`); err != nil {
		t.Fatal(err)
	}
	if _, err := root.Exec(ctx, `DROP ROLE writer`); err != nil {
		t.Fatal(err)
	}
	if _, err := connectSecure(ctx, secureURL(tc, certsDir, "writer", "writer-pw")); err == nil {
		t.Fatal("dropped role logged in")
	}
}

// TestMetricsRoleHTTP (issue #98): the metrics role scrapes /metrics and
// nothing else needs a grant; a plain user cannot scrape; the role member
// cannot read data.
func TestMetricsRoleHTTP(t *testing.T) {
	httpLis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	tc, certsDir := startSecureCluster(t, "topsecret", func(i int, cfg *server.Config) {
		if i == 0 {
			cfg.HTTPListener = httpLis
		}
	})
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	client := httpsClient(t, certsDir, "")
	base := "https://" + tc.Nodes[0].HTTPAddr()
	var root *pgx.Conn
	deadline := time.Now().Add(30 * time.Second)
	for {
		var err error
		root, err = connectSecure(ctx, secureURL(tc, certsDir, "root", "topsecret"))
		if err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("root could never authenticate: %v", err)
		}
		time.Sleep(200 * time.Millisecond)
	}
	defer func() { _ = root.Close(ctx) }()
	for _, q := range []string{
		`CREATE TABLE secret (id INT8 PRIMARY KEY)`,
		`CREATE USER prom PASSWORD 'prom-pw' IN ROLE metrics`,
		`CREATE USER plain PASSWORD 'plain-pw'`,
	} {
		if _, err := root.Exec(ctx, q); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}
	if code, body, _ := authedGet(t, client, base+"/metrics", "prom", "prom-pw"); code != http.StatusOK || !strings.Contains(body, "datax_") {
		t.Fatalf("metrics role scrape: %d", code)
	}
	if code, _, _ := authedGet(t, client, base+"/metrics", "plain", "plain-pw"); code != http.StatusForbidden {
		t.Fatalf("plain user scrape: %d, want 403", code)
	}
	if code, _, _ := authedGet(t, client, base+"/api/range?id=1", "prom", "prom-pw"); code != http.StatusForbidden {
		t.Fatalf("metrics role on an admin endpoint: %d, want 403", code)
	}
	prom, err := connectSecure(ctx, secureURL(tc, certsDir, "prom", "prom-pw"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = prom.Close(ctx) }()
	var pgErr *pgconn.PgError
	if _, err := prom.Exec(ctx, `SELECT id FROM secret`); err == nil || !errors.As(err, &pgErr) || pgErr.Code != "42501" {
		t.Fatalf("metrics role reads data: %v", err)
	}
}

// waitForClusterVersion waits for a gateway's version mirror to reach v.
func waitForClusterVersion(t *testing.T, ctx context.Context, s *sql.Session, v version.Version) {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for s.ClusterVersion() < v {
		if time.Now().After(deadline) {
			t.Fatalf("cluster version mirror stuck at %s, want %s", s.ClusterVersion(), v)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// TestUpgradeV11Roles (issue #98): a cluster at v10 with an admin user
// and grants in the old layout keeps them through the rolling upgrade;
// role DDL is refused until finalize, which rewrites the records as
// role descriptors; the user is still an admin and the grants hold.
func TestUpgradeV11Roles(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()
	asV10 := func(c *server.Config) { c.BinaryVersionOverride = version.V10 }
	tc, engines := StartWithEngines(t, 3, asV10)
	tc.LeaderIndex(1)

	sess := func(i int) *sql.Session { return sql.NewSession(tc.Nodes[i].DB(), catalog.NewAccessor()) }
	as := func(i int, user string) *sql.Session {
		return sql.NewSessionForUser(tc.Nodes[i].DB(), catalog.NewAccessor(), user)
	}
	execSQL(t, ctx, sess(0), `CREATE TABLE t (id INT8 PRIMARY KEY)`)
	execSQL(t, ctx, sess(0), `INSERT INTO t VALUES (1)`)
	execSQL(t, ctx, sess(0), `CREATE USER legacy PASSWORD 'pw12345'`)
	execSQL(t, ctx, sess(0), `GRANT ADMIN TO legacy`)
	execSQL(t, ctx, sess(0), `CREATE USER reader PASSWORD 'pw12345'`)
	execSQL(t, ctx, sess(0), `GRANT SELECT ON t TO reader`)
	expectCodeSQL(t, ctx, sess(1), `CREATE ROLE grp`, sql.CodeFeatureNotSupported)
	expectCodeSQL(t, ctx, sess(1), `SET ROLE admin`, sql.CodeFeatureNotSupported)
	legacyKeys := func() (users, admins, roles int) {
		count := func(lo, hi keys.Key) int {
			rows, err := tc.Nodes[0].DB().Scan(ctx, lo, hi, 0)
			if err != nil {
				t.Fatal(err)
			}
			return len(rows)
		}
		ulo, uhi := keys.UserSpan()
		alo, ahi := keys.AdminUserSpan()
		rlo, rhi := keys.RoleSpan()
		return count(ulo, uhi), count(alo, ahi), count(rlo, rhi)
	}
	if u, a, r := legacyKeys(); u != 2 || a != 1 || r != 0 {
		t.Fatalf("old layout before the upgrade: users=%d admins=%d roles=%d", u, a, r)
	}

	// Rolling restart onto the v11 binary; the old layout stays
	// authoritative until finalize, and both users keep working.
	for i := 0; i < 3; i++ {
		tc.StopNode(i)
		tc.RestartNode(i, engines[i])
		tc.LeaderIndex(1)
	}
	waitForAdvertisedVersion(t, ctx, tc.Nodes[0].Addr(), []base.NodeID{1, 2, 3}, int(version.V11))
	expectCodeSQL(t, ctx, sess(1), `CREATE ROLE grp`, sql.CodeFeatureNotSupported)
	execSQL(t, ctx, as(2, "legacy"), `CREATE TABLE by_legacy (id INT8 PRIMARY KEY)`)
	if r := execSQL(t, ctx, as(1, "reader"), `SELECT id FROM t`); len(r.Rows) != 1 {
		t.Fatalf("reader before finalize: %+v", r.Rows)
	}
	execSQL(t, ctx, sess(0), `CREATE USER mid PASSWORD 'pw12345'`) // still the old layout
	if u, a, r := legacyKeys(); u != 3 || a != 1 || r != 0 {
		t.Fatalf("old layout before finalize: users=%d admins=%d roles=%d", u, a, r)
	}

	resp := adminCall(t, ctx, tc.Nodes[0].Addr(), cluster.AdminRequest{Op: "upgrade-cluster", Version: int(version.V11)})
	if resp.Error != "" || resp.ClusterVersion != int(version.V11) {
		t.Fatalf("finalize v11: %+v", resp)
	}
	if u, a, r := legacyKeys(); u != 0 || a != 0 || r != 3 {
		t.Fatalf("layout after finalize: users=%d admins=%d roles=%d", u, a, r)
	}
	for i := range tc.Nodes {
		waitForClusterVersion(t, ctx, sess(i), version.V11)
	}
	execSQL(t, ctx, as(2, "legacy"), `CREATE TABLE by_legacy2 (id INT8 PRIMARY KEY)`)
	if r := execSQL(t, ctx, as(1, "reader"), `SELECT id FROM t`); len(r.Rows) != 1 {
		t.Fatalf("reader after finalize: %+v", r.Rows)
	}
	if r := execSQL(t, ctx, sess(0), `SHOW GRANTS ON ROLE admin`); len(r.Rows) != 2 || r.Rows[0][1].S != "legacy" || r.Rows[1][1].S != "root" {
		t.Fatalf("admin memberships after finalize: %+v", r.Rows)
	}
	execSQL(t, ctx, sess(1), `CREATE ROLE grp`)
	execSQL(t, ctx, sess(1), `GRANT grp TO mid`)
	if r := execSQL(t, ctx, sess(2), `SHOW USERS`); len(r.Rows) != 4 {
		t.Fatalf("SHOW USERS after finalize: %+v", r.Rows)
	}
	// A node restarted after finalize finds nothing left to migrate.
	tc.StopNode(0)
	tc.RestartNode(0, engines[0])
	tc.LeaderIndex(1)
	if r := execSQL(t, ctx, as(0, "reader"), `SELECT id FROM t`); len(r.Rows) != 1 {
		t.Fatalf("reader after restart: %+v", r.Rows)
	}
	if u, a, r := legacyKeys(); u != 0 || a != 0 || r != 4 {
		t.Fatalf("layout after restart: users=%d admins=%d roles=%d", u, a, r)
	}
}
