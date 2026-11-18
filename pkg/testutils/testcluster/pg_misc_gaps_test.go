package testcluster

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/sthorne/datax/pkg/sql"
)

// userURL is pgURL for another user and database (trust mode: the
// insecure cluster takes the startup user as given).
func userURL(tc *TestCluster, node int, user, db string) string {
	return fmt.Sprintf("postgres://%s@%s/%s?sslmode=disable", user, tc.Nodes[node].SQLAddr(), db)
}

// connectEventually connects, retrying while a CONNECTION LIMIT still
// counts a session the client has just closed (the server notices the
// close a moment later).
func connectEventually(t *testing.T, ctx context.Context, url string) *pgx.Conn {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		c, err := pgx.Connect(ctx, url)
		if err == nil {
			return c
		}
		if pgErrCode(err) != "53300" || time.Now().After(deadline) {
			t.Fatalf("connect %s: %v", url, err)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// TestDollarQuotedLiterals (PG-P-005): $$...$$ and $tag$...$tag$ are
// string literals in every expression context, over the simple and the
// extended protocol, next to $n parameters.
func TestDollarQuotedLiterals(t *testing.T) {
	tc := Start(t, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	conn, err := pgx.Connect(ctx, pgURL(tc, 0))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close(context.Background()) }()

	for _, mode := range []pgx.QueryExecMode{pgx.QueryExecModeSimpleProtocol, pgx.QueryExecModeExec} {
		var a, b string
		var n int
		if err := conn.QueryRow(ctx, `SELECT $$it's a "test"\n$$, $fn$body with $$ and ; inside$fn$, length($q$abc$q$)`, mode).Scan(&a, &b, &n); err != nil {
			t.Fatalf("mode %v: %v", mode, err)
		}
		if a != `it's a "test"\n` || b != "body with $$ and ; inside" || n != 3 {
			t.Fatalf("mode %v: %q %q %d", mode, a, b, n)
		}
	}
	// Beside a parameter, in the extended protocol.
	var s string
	if err := conn.QueryRow(ctx, `SELECT $1::text || $$-x$$`, "a").Scan(&s); err != nil || s != "a-x" {
		t.Fatalf("with a parameter: %q %v", s, err)
	}
	for _, q := range []string{
		`CREATE TABLE dq (k INT8 PRIMARY KEY, v TEXT DEFAULT $$dflt$$ CHECK (v <> $$bad$$))`,
		`INSERT INTO dq (k) VALUES (1)`,
		`INSERT INTO dq VALUES (2, $t$two's$t$)`,
		`COMMENT ON TABLE dq IS $$a 'commented' table$$`,
	} {
		if _, err := conn.Exec(ctx, q); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}
	rows, err := conn.Query(ctx, `SELECT v FROM dq WHERE v IN ($$dflt$$, $$two's$$) ORDER BY k`)
	if err != nil {
		t.Fatal(err)
	}
	got, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil || strings.Join(got, ",") != "dflt,two's" {
		t.Fatalf("rows: %v %v", got, err)
	}
	if _, err := conn.Exec(ctx, `INSERT INTO dq VALUES (3, $$bad$$)`); pgErrCode(err) != "23514" {
		t.Fatalf("CHECK with a dollar-quoted constant: %v", err)
	}
	for q, code := range map[string]string{
		`SELECT $1$`:   "42601",
		`SELECT $$abc`: "42601",
	} {
		if _, err := conn.Exec(ctx, q, pgx.QueryExecModeSimpleProtocol); pgErrCode(err) != code {
			t.Fatalf("%s: %v, want %s", q, err, code)
		}
	}
}

// TestVacuum (PG-O-003): VACUUM in all its forms succeeds and changes
// nothing; with ANALYZE it refreshes the statistics; it cannot run in a
// transaction block (25001); what it names must exist.
func TestVacuum(t *testing.T) {
	tc := Start(t, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	root := leasedSession(t, tc, 0, 2*time.Second)
	waitForDatabases(t, ctx, root)
	execSQL(t, ctx, root, `CREATE TABLE vt (k INT8 PRIMARY KEY, v TEXT)`)
	execSQL(t, ctx, root, `CREATE VIEW vv AS SELECT k FROM vt`)
	execSQL(t, ctx, root, `INSERT INTO vt VALUES (1, 'a'), (2, 'b'), (3, NULL)`)

	for _, q := range []string{
		`VACUUM`, `VACUUM vt`, `VACUUM FULL`, `VACUUM FULL vt`, `VACUUM FREEZE VERBOSE vt`,
		`VACUUM (VERBOSE) vt`, `VACUUM (FULL false, INDEX_CLEANUP off, PARALLEL 2, SKIP_LOCKED) vt, vt`,
	} {
		if r := execSQL(t, ctx, root, q); r.Tag != "VACUUM" {
			t.Fatalf("%s: tag %q", q, r.Tag)
		}
	}
	if r := execSQL(t, ctx, root, `SHOW STATS FOR vt`); len(r.Rows) != 0 {
		t.Fatalf("a bare VACUUM analyzed: %+v", r.Rows)
	}
	execSQL(t, ctx, root, `VACUUM ANALYZE vt`)
	r := execSQL(t, ctx, root, `SHOW STATS FOR vt`)
	if len(r.Rows) == 0 || r.Rows[0][1].I != 3 {
		t.Fatalf("VACUUM ANALYZE vt: stats %+v", r.Rows)
	}
	execSQL(t, ctx, root, `INSERT INTO vt VALUES (4, 'd')`)
	execSQL(t, ctx, root, `VACUUM (VERBOSE, ANALYZE) vt (v)`)
	if r := execSQL(t, ctx, root, `SHOW STATS FOR vt`); len(r.Rows) == 0 || r.Rows[0][1].I != 4 {
		t.Fatalf("VACUUM (VERBOSE, ANALYZE) vt: stats %+v", r.Rows)
	}
	if r := execSQL(t, ctx, root, `VACUUM vv`); len(r.Warnings) != 1 || !strings.Contains(r.Warnings[0], "cannot vacuum non-tables") {
		t.Fatalf("VACUUM of a view: %+v", r.Warnings)
	}

	expectCodeSQL(t, ctx, root, `VACUUM nosuch`, "42P01")
	expectCodeSQL(t, ctx, root, `VACUUM (bogus) vt`, "42601")
	expectCodeSQL(t, ctx, root, `VACUUM (VERBOSE 'maybe') vt`, "42601")
	expectCodeSQL(t, ctx, root, `VACUUM vt (v)`, "0A000")
	expectCodeSQL(t, ctx, root, `VACUUM ANALYZE vt (nosuch)`, "42703")

	execSQL(t, ctx, root, `BEGIN`)
	if _, serr := trySQL(ctx, root, `VACUUM`); serr == nil || serr.Code != sql.CodeActiveTransaction || serr.Msg != "VACUUM cannot run inside a transaction block" {
		t.Fatalf("VACUUM in a block: %v", serr)
	}
	execSQL(t, ctx, root, `ROLLBACK`)

	// A role that does not own the table is warned and skipped; one that
	// owns it but is no admin may vacuum, not analyze.
	execSQL(t, ctx, root, `CREATE USER vu`)
	execSQL(t, ctx, root, `GRANT CREATE ON DATABASE datax TO vu`)
	vu := userSession(t, tc, 0, "vu")
	if r := execSQL(t, ctx, vu, `VACUUM ANALYZE vt`); len(r.Warnings) != 1 || r.Warnings[0] != `permission denied to vacuum "vt", skipping it` {
		t.Fatalf("non-owner VACUUM: %+v", r.Warnings)
	}
	execEventually(t, ctx, vu, `CREATE TABLE vown (k INT8 PRIMARY KEY)`)
	if r := execSQL(t, ctx, vu, `VACUUM vown`); len(r.Warnings) != 0 {
		t.Fatalf("owner VACUUM: %+v", r.Warnings)
	}
	if r := execSQL(t, ctx, vu, `VACUUM ANALYZE vown`); len(r.Warnings) != 1 || !strings.Contains(r.Warnings[0], "admin") {
		t.Fatalf("owner VACUUM ANALYZE: %+v", r.Warnings)
	}
}

// TestAsOfSystemTimeForms (CR-AOST-001): follower_read_timestamp(),
// with_min_timestamp, with_max_staleness's nearest_only argument, BEGIN
// AS OF SYSTEM TIME and SET TRANSACTION AS OF SYSTEM TIME.
func TestAsOfSystemTimeForms(t *testing.T) {
	tc := Start(t, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	root := leasedSession(t, tc, 0, 2*time.Second)
	waitForDatabases(t, ctx, root)
	execSQL(t, ctx, root, `CREATE TABLE ht (k INT8 PRIMARY KEY)`)
	execSQL(t, ctx, root, `INSERT INTO ht VALUES (1)`)
	execSQL(t, ctx, root, `CREATE USER hu`)
	execSQL(t, ctx, root, `GRANT SELECT ON ht TO hu`)
	time.Sleep(100 * time.Millisecond)
	t1 := time.Now().UTC().Format(time.RFC3339Nano)
	time.Sleep(100 * time.Millisecond)
	execSQL(t, ctx, root, `INSERT INTO ht VALUES (2)`)
	count := func(s *sql.Session, q string) int64 {
		t.Helper()
		r := execSQL(t, ctx, s, q)
		return r.Rows[0][0].I
	}

	// with_min_timestamp: never older than the bound.
	if n := count(root, `SELECT count(*) FROM ht AS OF SYSTEM TIME with_min_timestamp('`+t1+`')`); n < 1 {
		t.Fatalf("with_min_timestamp(t1): %d", n)
	}
	after := time.Now().UTC().Format(time.RFC3339Nano)
	if n := count(root, `SELECT count(*) FROM ht AS OF SYSTEM TIME with_min_timestamp('`+after+`', false)`); n != 2 {
		t.Fatalf("with_min_timestamp(after the second row): %d", n)
	}
	future := time.Now().Add(time.Hour).UTC().Format(time.RFC3339Nano)
	expectCodeSQL(t, ctx, root, `SELECT count(*) FROM ht AS OF SYSTEM TIME with_min_timestamp('`+future+`')`, "22023")
	expectCodeSQL(t, ctx, root, `SELECT count(*) FROM ht AS OF SYSTEM TIME with_max_staleness('10s', TRUE)`, "0A000")

	// BEGIN AS OF SYSTEM TIME: a read-only block at t1.
	execSQL(t, ctx, root, `BEGIN AS OF SYSTEM TIME '`+t1+`'`)
	if n := count(root, `SELECT count(*) FROM ht`); n != 1 {
		t.Fatalf("BEGIN AS OF SYSTEM TIME t1: %d rows", n)
	}
	expectCodeSQL(t, ctx, root, `INSERT INTO ht VALUES (3)`, "25006")
	execSQL(t, ctx, root, `ROLLBACK`)
	execSQL(t, ctx, root, `START TRANSACTION READ ONLY, AS OF SYSTEM TIME '`+t1+`'`)
	if n := count(root, `SELECT count(*) FROM ht`); n != 1 {
		t.Fatalf("START TRANSACTION ..., AS OF SYSTEM TIME t1: %d rows", n)
	}
	execSQL(t, ctx, root, `COMMIT`)
	expectCodeSQL(t, ctx, root, `BEGIN AS OF SYSTEM TIME with_max_staleness('10s')`, "0A000")

	// SET TRANSACTION AS OF SYSTEM TIME, before the block's first query.
	execSQL(t, ctx, root, `BEGIN`)
	execSQL(t, ctx, root, `SET TRANSACTION AS OF SYSTEM TIME '`+t1+`'`)
	if n := count(root, `SELECT count(*) FROM ht`); n != 1 {
		t.Fatalf("SET TRANSACTION AS OF SYSTEM TIME t1: %d rows", n)
	}
	execSQL(t, ctx, root, `COMMIT`)
	execSQL(t, ctx, root, `BEGIN`)
	execSQL(t, ctx, root, `SELECT 1 FROM ht LIMIT 1`)
	expectCodeSQL(t, ctx, root, `SET TRANSACTION AS OF SYSTEM TIME '`+t1+`'`, "25001")
	execSQL(t, ctx, root, `ROLLBACK`)
	expectCodeSQL(t, ctx, root, `SET TRANSACTION AS OF SYSTEM TIME '`+t1+`'`, "25P01")

	// A historical block decides privileges by today's grants: a REVOKE
	// stops a read of the rows as they stood before it.
	hu := userSession(t, tc, 0, "hu")
	execEventually(t, ctx, hu, `SELECT count(*) FROM ht`)
	execSQL(t, ctx, root, `REVOKE SELECT ON ht FROM hu`)
	deadline := time.Now().Add(20 * time.Second)
	for {
		execSQL(t, ctx, hu, `BEGIN AS OF SYSTEM TIME '`+t1+`'`)
		_, serr := trySQL(ctx, hu, `SELECT count(*) FROM ht`)
		execSQL(t, ctx, hu, `ROLLBACK`)
		if serr != nil && serr.Code == sql.CodeInsufficientPriv {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("a revoked role reads history in a BEGIN AS OF SYSTEM TIME block: %v", serr)
		}
		time.Sleep(200 * time.Millisecond)
	}

	// follower_read_timestamp(): far enough back for any replica. The
	// first row is older than that once the lag has passed.
	time.Sleep(5 * time.Second)
	execSQL(t, ctx, root, `INSERT INTO ht VALUES (3)`)
	if n := count(root, `SELECT count(*) FROM ht AS OF SYSTEM TIME follower_read_timestamp()`); n != 2 {
		t.Fatalf("follower_read_timestamp(): %d rows, want 2 (the third is newer)", n)
	}
	if n := count(root, `SELECT count(*) FROM ht AS OF SYSTEM TIME with_max_staleness('1h', FALSE)`); n < 2 {
		t.Fatalf("with_max_staleness('1h', FALSE): %d rows", n)
	}
	execSQL(t, ctx, root, `BEGIN AS OF SYSTEM TIME experimental_follower_read_timestamp()`)
	if n := count(root, `SELECT count(*) FROM ht`); n != 2 {
		t.Fatalf("BEGIN AS OF SYSTEM TIME follower_read_timestamp(): %d rows", n)
	}
	execSQL(t, ctx, root, `COMMIT`)
}

// TestRoleAttributes (PG-S-001): SUPERUSER (datax's admin), CREATEDB,
// CREATEROLE, REPLICATION, CONNECTION LIMIT and VALID UNTIL; only an
// admin sets any of them, a role can grant itself nothing, and the
// catalogs show them.
func TestRoleAttributes(t *testing.T) {
	tc := Start(t, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	root := leasedSession(t, tc, 0, 2*time.Second)
	waitForDatabases(t, ctx, root)

	execSQL(t, ctx, root, `CREATE ROLE boss WITH LOGIN SUPERUSER`)
	execSQL(t, ctx, root, `CREATE ROLE maker LOGIN CREATEDB CREATEROLE REPLICATION CONNECTION LIMIT 7 VALID UNTIL '2100-01-02 03:04:05+00'`)
	execSQL(t, ctx, root, `CREATE USER plain PASSWORD 'pw12345'`)
	expectCodeSQL(t, ctx, root, `CREATE ROLE bad CONNECTION LIMIT -2`, "22023")
	expectCodeSQL(t, ctx, root, `CREATE ROLE bad VALID UNTIL 'someday'`, "22007")
	expectCodeSQL(t, ctx, root, `ALTER ROLE root NOSUPERUSER`, "0A000")
	expectCodeSQL(t, ctx, root, `ALTER ROLE root VALID UNTIL '2000-01-01'`, "0A000")

	r := execSQL(t, ctx, root, `SELECT rolname, rolsuper, rolcreatedb, rolcreaterole, rolreplication, rolconnlimit, rolvaliduntil IS NOT NULL
		FROM pg_roles WHERE rolname IN ('boss', 'maker', 'plain') ORDER BY rolname`)
	want := []string{"boss true true true false -1 false", "maker false true true true 7 true", "plain false false false false -1 false"}
	for i, row := range r.Rows {
		got := fmt.Sprintf("%s %v %v %v %v %d %v", row[0].S, row[1].B, row[2].B, row[3].B, row[4].B, row[5].I, row[6].B)
		if i >= len(want) || got != want[i] {
			t.Fatalf("pg_roles row %d: %s, want %v", i, got, want)
		}
	}
	if r := execSQL(t, ctx, root, `SELECT rolname, rolsuper, rolconnlimit FROM pg_authid WHERE rolname = 'maker'`); len(r.Rows) != 1 || r.Rows[0][1].B || r.Rows[0][2].I != 7 {
		t.Fatalf("pg_authid: %+v", r.Rows)
	}
	if r := execSQL(t, ctx, root, `SELECT usecreatedb, userepl FROM pg_user WHERE usename = 'maker'`); len(r.Rows) != 1 || !r.Rows[0][0].B || !r.Rows[0][1].B {
		t.Fatalf("pg_user: %+v", r.Rows)
	}

	// SUPERUSER is admin.
	boss := userSession(t, tc, 0, "boss")
	execEventually(t, ctx, boss, `CREATE DATABASE boss_db`)
	execSQL(t, ctx, boss, `CREATE ROLE by_boss`)

	// CREATEDB creates databases, with itself or a role it belongs to as
	// owner, and nothing else.
	maker := userSession(t, tc, 0, "maker")
	execSQL(t, ctx, maker, `CREATE DATABASE maker_db`)
	if r := execSQL(t, ctx, root, `SELECT r.rolname FROM pg_database d JOIN pg_roles r ON r.oid = d.datdba WHERE d.datname = 'maker_db'`); len(r.Rows) != 1 || r.Rows[0][0].S != "maker" {
		t.Fatalf("maker_db owner: %+v", r.Rows)
	}
	denied(t, ctx, maker, `CREATE DATABASE other_db OWNER boss`)
	denied(t, ctx, maker, `CREATE ROLE by_maker`)
	denied(t, ctx, maker, `ALTER ROLE plain CREATEDB`)
	denied(t, ctx, maker, `GRANT admin TO maker`)
	execSQL(t, ctx, maker, `DROP DATABASE maker_db`)

	// No role grants itself anything: only its password is its own.
	plain := userSession(t, tc, 0, "plain")
	for _, q := range []string{
		`ALTER ROLE plain SUPERUSER`,
		`ALTER ROLE plain CREATEDB`,
		`ALTER ROLE plain CREATEROLE`,
		`ALTER ROLE plain REPLICATION`,
		`ALTER ROLE plain CONNECTION LIMIT -1`,
		`ALTER ROLE plain VALID UNTIL 'infinity'`,
		`ALTER ROLE plain PASSWORD 'newpw123' SUPERUSER`,
		`ALTER ROLE plain WITH PASSWORD 'newpw123' CREATEDB`,
		`ALTER USER plain PASSWORD 'newpw123' VALID UNTIL 'infinity'`,
		`CREATE ROLE sneaky SUPERUSER`,
		`GRANT admin TO plain`,
		`CREATE DATABASE plain_db`,
	} {
		denied(t, ctx, plain, q)
	}
	if _, serr := trySQL(ctx, plain, `CREATE DATABASE plain_db`); serr == nil || serr.Msg != "permission denied to create database" {
		t.Fatalf("CREATE DATABASE without CREATEDB: %v", serr)
	}
	execSQL(t, ctx, plain, `ALTER ROLE plain PASSWORD 'newpw123'`)
	if r := execSQL(t, ctx, plain, `SELECT rolsuper, rolcreatedb FROM pg_roles WHERE rolname = 'plain'`); r.Rows[0][0].B || r.Rows[0][1].B {
		t.Fatalf("plain gained an attribute: %+v", r.Rows)
	}
	expectCodeSQL(t, ctx, plain, `SELECT * FROM pg_authid`, "42501")

	// NOSUPERUSER takes the admin role away; NOCREATEDB the attribute.
	execSQL(t, ctx, root, `ALTER ROLE boss NOSUPERUSER`)
	execSQL(t, ctx, root, `ALTER ROLE maker NOCREATEDB CONNECTION LIMIT -1 VALID UNTIL 'infinity'`)
	denied(t, ctx, boss, `CREATE DATABASE boss_db2`)
	denied(t, ctx, maker, `CREATE DATABASE maker_db2`)
	if r := execSQL(t, ctx, root, `SELECT rolsuper, rolcreatedb, rolconnlimit, rolvaliduntil IS NULL FROM pg_roles WHERE rolname IN ('boss', 'maker') ORDER BY rolname`); r.Rows[0][0].B || r.Rows[1][1].B || r.Rows[1][2].I != -1 || !r.Rows[1][3].B {
		t.Fatalf("after NOSUPERUSER / NOCREATEDB: %+v", r.Rows)
	}

	// CONNECTION LIMIT: the role's sessions on this node, admins exempt.
	execSQL(t, ctx, root, `CREATE ROLE limited LOGIN CONNECTION LIMIT 1`)
	c1, err := pgx.Connect(ctx, userURL(tc, 0, "limited", "datax"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pgx.Connect(ctx, userURL(tc, 0, "limited", "datax")); pgErrCode(err) != "53300" || !strings.Contains(err.Error(), `too many connections for role "limited"`) {
		t.Fatalf("second session past CONNECTION LIMIT 1: %v", err)
	}
	_ = c1.Close(ctx)
	c2 := connectEventually(t, ctx, userURL(tc, 0, "limited", "datax"))
	_ = c2.Close(ctx)
	execSQL(t, ctx, root, `ALTER ROLE root CONNECTION LIMIT 0`)
	rc, err := pgx.Connect(ctx, pgURL(tc, 0))
	if err != nil {
		t.Fatalf("an admin is exempt from CONNECTION LIMIT: %v", err)
	}
	_ = rc.Close(ctx)
}

// TestRoleValidUntil (PG-S-001): a password past its VALID UNTIL no
// longer authenticates, uniformly with a wrong one.
func TestRoleValidUntil(t *testing.T) {
	tc, certsDir := startSecureCluster(t, "topsecret")
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	var conn *pgx.Conn
	deadline := time.Now().Add(30 * time.Second)
	for {
		var err error
		if conn, err = connectSecure(ctx, secureURL(tc, certsDir, "root", "topsecret")); err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("root could never authenticate: %v", err)
		}
		time.Sleep(200 * time.Millisecond)
	}
	defer func() { _ = conn.Close(ctx) }()
	mustExec := func(q string) {
		t.Helper()
		if _, err := conn.Exec(ctx, q); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}
	mustExec(`CREATE USER lapsed PASSWORD 'pw12345' VALID UNTIL '2001-01-01 00:00:00+00'`)
	if _, err := connectSecure(ctx, secureURL(tc, certsDir, "lapsed", "pw12345")); err == nil || !strings.Contains(err.Error(), "password authentication failed") {
		t.Fatalf("expired password: %v", err)
	}
	mustExec(`ALTER USER lapsed VALID UNTIL '2200-01-01'`)
	c, err := connectSecure(ctx, secureURL(tc, certsDir, "lapsed", "pw12345"))
	if err != nil {
		t.Fatalf("after VALID UNTIL moved on: %v", err)
	}
	_ = c.Close(ctx)
	mustExec(`ALTER USER lapsed VALID UNTIL 'infinity'`)
	c, err = connectSecure(ctx, secureURL(tc, certsDir, "lapsed", "pw12345"))
	if err != nil {
		t.Fatalf("VALID UNTIL infinity: %v", err)
	}
	_ = c.Close(ctx)
}

// TestDatabaseOptions (PG-D-022): CREATE DATABASE's PostgreSQL options as
// Rails, Django and pg_dump send them, and ALTER DATABASE's forms.
func TestDatabaseOptions(t *testing.T) {
	tc := Start(t, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	root := leasedSession(t, tc, 0, 2*time.Second)
	waitForDatabases(t, ctx, root)
	execSQL(t, ctx, root, `CREATE USER alice`)
	execSQL(t, ctx, root, `CREATE USER mallory`)

	for _, q := range []string{
		`CREATE DATABASE d1 WITH OWNER = alice ENCODING = 'UTF8' TEMPLATE = template0 LC_COLLATE = 'C' LC_CTYPE = 'en_US.UTF-8' CONNECTION LIMIT = 5`,
		`CREATE DATABASE "rails_dev" ENCODING = 'utf8'`,
		`CREATE DATABASE d3 ENCODING 'UNICODE' LOCALE 'en_US.utf8' TEMPLATE template1 STRATEGY wal_log TABLESPACE pg_default ALLOW_CONNECTIONS true IS_TEMPLATE false`,
		`CREATE DATABASE d4 WITH ENCODING 6 LOCALE_PROVIDER libc LOCALE 'C.UTF-8' OWNER DEFAULT CONNECTION LIMIT -1`,
	} {
		execSQL(t, ctx, root, q)
	}
	r := execSQL(t, ctx, root, `SELECT d.datname, r.rolname, pg_encoding_to_char(d.encoding), d.datcollate, d.datctype, d.datconnlimit
		FROM pg_database d JOIN pg_roles r ON r.oid = d.datdba WHERE d.datname IN ('d1', 'rails_dev', 'd3', 'd4') ORDER BY d.datname`)
	want := []string{"d1 alice UTF8 C en_US.UTF-8 5", "d3 root UTF8 en_US.UTF-8 en_US.UTF-8 -1", "d4 root UTF8 C.UTF-8 C.UTF-8 -1", "rails_dev root UTF8 C C -1"}
	if len(r.Rows) != len(want) {
		t.Fatalf("pg_database: %+v", r.Rows)
	}
	for i, row := range r.Rows {
		if got := fmt.Sprintf("%s %s %s %s %s %d", row[0].S, row[1].S, row[2].S, row[3].S, row[4].S, row[5].I); got != want[i] {
			t.Fatalf("pg_database row %d: %s, want %s", i, got, want[i])
		}
	}
	for q, code := range map[string]string{
		`CREATE DATABASE e ENCODING 'LATIN1'`:               "22023",
		`CREATE DATABASE e ENCODING 'klingon'`:              "42704",
		`CREATE DATABASE e TEMPLATE nosuch`:                 "3D000",
		`CREATE DATABASE e TEMPLATE d1`:                     "0A000",
		`CREATE DATABASE e LOCALE 'de_DE.UTF-8'`:            "42809",
		`CREATE DATABASE e LC_COLLATE 'xx'`:                 "42809",
		`CREATE DATABASE e LOCALE_PROVIDER icu`:             "0A000",
		`CREATE DATABASE e TABLESPACE fast`:                 "42704",
		`CREATE DATABASE e OWNER nosuch`:                    "42704",
		`CREATE DATABASE e CONNECTION LIMIT -5`:             "22023",
		`CREATE DATABASE e FROBNICATE = 1`:                  "42601",
		`CREATE DATABASE e ENCODING 'UTF8' ENCODING 'UTF8'`: "42601",
		`CREATE DATABASE e IS_TEMPLATE true`:                "0A000",
	} {
		expectCodeSQL(t, ctx, root, q, code)
	}
	if _, serr := trySQL(ctx, root, `CREATE DATABASE e LC_COLLATE 'xx'`); serr == nil || serr.Msg != `invalid LC_COLLATE locale name: "xx"` {
		t.Fatalf("LC_COLLATE xx: %v", serr)
	}

	// ALTER DATABASE ... SET: a default for the database's new sessions.
	execSQL(t, ctx, root, `ALTER DATABASE d1 SET statement_timeout = '5s'`)
	execSQL(t, ctx, root, `ALTER DATABASE d1 SET TimeZone TO 'America/New_York'`)
	execSQL(t, ctx, root, `ALTER DATABASE d1 SET app.tenant = 'acme'`)
	expectCodeSQL(t, ctx, root, `ALTER DATABASE d1 SET nosuch_param = 1`, "42704")
	// datax's own "database" variable switches a session's database: as a
	// default it would move every session that connects to d1, an admin's
	// included, into another database. It is refused as PostgreSQL refuses
	// a parameter it does not have.
	expectCodeSQL(t, ctx, root, `ALTER DATABASE d1 SET database = 'defaultdb'`, "42704")
	expectCodeSQL(t, ctx, root, `ALTER DATABASE d1 SET statement_timeout = 'soon'`, "22023")
	expectCodeSQL(t, ctx, root, `ALTER DATABASE nosuch SET statement_timeout = '5s'`, "3D000")
	conn, err := pgx.Connect(ctx, userURL(tc, 0, "alice", "d1"))
	if err != nil {
		t.Fatal(err)
	}
	var st, tz, tenant string
	if err := conn.QueryRow(ctx, `SELECT current_setting('statement_timeout'), current_setting('TimeZone'), current_setting('app.tenant')`).Scan(&st, &tz, &tenant); err != nil || st != "5s" && st != "5000" || tz != "America/New_York" || tenant != "acme" {
		t.Fatalf("database defaults: %q %q %q %v", st, tz, tenant, err)
	}
	if v := conn.PgConn().ParameterStatus("TimeZone"); v != "America/New_York" {
		t.Fatalf("reported TimeZone: %q", v)
	}
	_ = conn.Close(ctx)
	execSQL(t, ctx, root, `ALTER DATABASE d1 RESET statement_timeout`)
	conn, err = pgx.Connect(ctx, userURL(tc, 0, "alice", "d1"))
	if err != nil {
		t.Fatal(err)
	}
	if err := conn.QueryRow(ctx, `SELECT current_setting('statement_timeout'), current_setting('TimeZone')`).Scan(&st, &tz); err != nil || st != "0" || tz != "America/New_York" {
		t.Fatalf("after RESET statement_timeout: %q %q %v", st, tz, err)
	}
	_ = conn.Close(ctx)
	execSQL(t, ctx, root, `ALTER DATABASE d1 RESET ALL`)

	// The other forms; an owner's, not anyone's.
	for _, q := range []string{
		`ALTER DATABASE d1 CONNECTION LIMIT 2`,
		`ALTER DATABASE d1 WITH ALLOW_CONNECTIONS true IS_TEMPLATE false`,
		`ALTER DATABASE d1 SET TABLESPACE pg_default`,
		`ALTER DATABASE d1 REFRESH COLLATION VERSION`,
		`ALTER DATABASE d3 RENAME TO d3b`,
		`ALTER DATABASE d3b OWNER TO alice`,
	} {
		execSQL(t, ctx, root, q)
	}
	if r := execSQL(t, ctx, root, `SELECT datconnlimit FROM pg_database WHERE datname = 'd1'`); r.Rows[0][0].I != 2 {
		t.Fatalf("ALTER DATABASE CONNECTION LIMIT: %+v", r.Rows)
	}
	expectCodeSQL(t, ctx, root, `ALTER DATABASE d1 IS_TEMPLATE true`, "0A000")
	expectCodeSQL(t, ctx, root, `ALTER DATABASE d1 SET TABLESPACE fast`, "42704")
	expectCodeSQL(t, ctx, root, `ALTER DATABASE d1 CONNECTION LIMIT -3`, "22023")
	mallory := userSession(t, tc, 0, "mallory")
	denied(t, ctx, mallory, `ALTER DATABASE d1 SET statement_timeout = '1ms'`)
	denied(t, ctx, mallory, `ALTER DATABASE d1 CONNECTION LIMIT 0`)
	alice := userSession(t, tc, 0, "alice")
	execEventually(t, ctx, alice, `ALTER DATABASE d1 SET statement_timeout = '7s'`)

	// The database's CONNECTION LIMIT, counted on this node; admins are
	// exempt.
	c1 := connectEventually(t, ctx, userURL(tc, 0, "mallory", "d1"))
	c2 := connectEventually(t, ctx, userURL(tc, 0, "alice", "d1"))
	if _, err := pgx.Connect(ctx, userURL(tc, 0, "mallory", "d1")); pgErrCode(err) != "53300" || !strings.Contains(err.Error(), `too many connections for database "d1"`) {
		t.Fatalf("third session past the database's CONNECTION LIMIT 2: %v", err)
	}
	rc, err := pgx.Connect(ctx, userURL(tc, 0, "root", "d1"))
	if err != nil {
		t.Fatalf("an admin is exempt: %v", err)
	}
	_ = rc.Close(ctx)
	_ = c1.Close(ctx)
	_ = c2.Close(ctx)

	// Placement still parses beside the PostgreSQL options.
	execSQL(t, ctx, root, `CREATE DATABASE pl WITH ENCODING 'UTF8' (replicas = 1)`)
	execSQL(t, ctx, root, `ALTER DATABASE pl SET replicas = 1`)
}
