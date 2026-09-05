package testcluster

import (
	"context"
	"errors"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/sthorne/datax/pkg/server"
	"github.com/sthorne/datax/pkg/sql"
	"github.com/sthorne/datax/pkg/sql/catalog"
)

// waitForDatabases waits until the v6 catalog is ready and the gateway's
// cluster-version mirror has caught up (the version-gated DDL — expression
// defaults, constraints, views — reads the mirror, which starts at the
// floor until the first heartbeat).
func waitForDatabases(t *testing.T, ctx context.Context, s *sql.Session) {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for {
		if _, serr := trySQL(ctx, s, `CREATE DATABASE IF NOT EXISTS probe_db`); serr == nil {
			execSQL(t, ctx, s, `DROP DATABASE probe_db`)
			break
		} else if time.Now().After(deadline) {
			t.Fatalf("the v6 catalog never became ready: %v", serr)
		}
		time.Sleep(200 * time.Millisecond)
	}
	// The mirror converges to the finalized version (Current on a fresh
	// cluster; a test's BinaryVersionOverride otherwise).
	for {
		want, err := s.FinalizedClusterVersion(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if s.ClusterVersion() >= want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("the cluster-version mirror never reached %s (at %s)", want, s.ClusterVersion())
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// TestDatabases (issue #88): two databases hold same-named tables in
// isolation; sessions switch with USE and SET database, connections
// choose with the URL (an unknown one is 3D000); qualified names reach
// across; SHOW TABLES and SHOW DATABASES are per database; DROP refuses
// a non-empty database without CASCADE and the reserved ones always;
// rename keeps the tables; database grants gate CREATE and CONNECT; the
// schema browser and backups carry the database.
func TestDatabases(t *testing.T) {
	// SQL and HTTP listeners on every node.
	listeners := make([]net.Listener, 3)
	for i := range listeners {
		lis, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		listeners[i] = lis
	}
	li := 0
	tc := StartWithOptions(t, 3, func(c *server.Config) { c.HTTPListener = listeners[li]; li++ })
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()
	root := sql.NewSession(tc.Nodes[0].DB(), catalog.NewAccessor())
	waitForDatabases(t, ctx, root)

	// Two databases, one table name, different contents.
	execSQL(t, ctx, root, `CREATE DATABASE app`)
	execSQL(t, ctx, root, `CREATE DATABASE IF NOT EXISTS app`)
	if _, serr := trySQL(ctx, root, `CREATE DATABASE app`); serr == nil || serr.Code != sql.CodeDuplicateDatabase {
		t.Fatalf("duplicate database: %v", serr)
	}
	execSQL(t, ctx, root, `CREATE DATABASE analytics`)
	execSQL(t, ctx, root, `CREATE TABLE items (id INT8 PRIMARY KEY, name TEXT)`) // in datax
	execSQL(t, ctx, root, `INSERT INTO items VALUES (1, 'default')`)
	execSQL(t, ctx, root, `USE app`)
	if r := execSQL(t, ctx, root, `SELECT current_database()`); r.Rows[0][0].S != "app" {
		t.Fatalf("current_database after USE: %+v", r.Rows)
	}
	execSQL(t, ctx, root, `CREATE TABLE items (id INT8 PRIMARY KEY, name TEXT)`)
	execSQL(t, ctx, root, `INSERT INTO items VALUES (1, 'app'), (2, 'app')`)
	execSQL(t, ctx, root, `SET database = analytics`)
	execSQL(t, ctx, root, `CREATE TABLE analytics.items (id INT8 PRIMARY KEY, name TEXT)`)
	execSQL(t, ctx, root, `INSERT INTO items VALUES (1, 'analytics')`)
	if r := execSQL(t, ctx, root, `SELECT name FROM items`); len(r.Rows) != 1 || r.Rows[0][0].S != "analytics" {
		t.Fatalf("analytics.items: %+v", r.Rows)
	}
	if r := execSQL(t, ctx, root, `SELECT name FROM app.items ORDER BY id`); len(r.Rows) != 2 || r.Rows[0][0].S != "app" {
		t.Fatalf("app.items from analytics: %+v", r.Rows)
	}
	if r := execSQL(t, ctx, root, `SELECT name FROM datax.public.items`); len(r.Rows) != 1 || r.Rows[0][0].S != "default" {
		t.Fatalf("datax.public.items: %+v", r.Rows)
	}
	if r := execSQL(t, ctx, root, `SELECT a.name, d.name FROM app.items AS a JOIN datax.items AS d ON a.id = d.id`); len(r.Rows) != 1 {
		t.Fatalf("cross-database join: %+v", r.Rows)
	}
	if r := execSQL(t, ctx, root, `SHOW TABLES`); len(r.Rows) != 1 || r.Rows[0][0].S != "items" {
		t.Fatalf("SHOW TABLES in analytics: %+v", r.Rows)
	}
	execSQL(t, ctx, root, `USE datax`)
	if r := execSQL(t, ctx, root, `SHOW TABLES`); len(r.Rows) != 1 || r.Rows[0][0].S != "items" {
		t.Fatalf("SHOW TABLES in datax (the metrics table is off in tests): %+v", r.Rows)
	}
	r := execSQL(t, ctx, root, `SHOW DATABASES`)
	var names []string
	for _, row := range r.Rows {
		names = append(names, row[0].S)
	}
	if strings.Join(names, ",") != "analytics,app,datax,system" {
		t.Fatalf("SHOW DATABASES: %v", names)
	}
	if _, serr := trySQL(ctx, root, `USE nope`); serr == nil || serr.Code != sql.CodeInvalidCatalogName {
		t.Fatalf("USE nope: %v", serr)
	}
	if _, serr := trySQL(ctx, root, `SELECT id FROM nope.items`); serr == nil || serr.Code != sql.CodeInvalidCatalogName {
		t.Fatalf("nope.items: %v", serr)
	}
	if _, serr := trySQL(ctx, root, `SELECT id FROM app.other.items`); serr == nil || serr.Code != sql.CodeSyntaxError {
		t.Fatalf("other schema: %v", serr)
	}

	// The wire: the URL's database is the session's, an unknown one is
	// refused at startup with 3D000.
	base := "postgres://root@" + tc.Nodes[1].SQLAddr() + "/"
	conn, err := pgx.Connect(ctx, base+"app?sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}
	var n int
	if err := conn.QueryRow(ctx, `SELECT count(*) FROM items`).Scan(&n); err != nil || n != 2 {
		t.Fatalf("count in app over the wire: %d %v", n, err)
	}
	var cur string
	if err := conn.QueryRow(ctx, `SHOW database`).Scan(&cur); err != nil || cur != "app" {
		t.Fatalf("SHOW database: %q %v", cur, err)
	}
	_ = conn.Close(ctx)
	if _, err := pgx.Connect(ctx, base+"nope?sslmode=disable"); err == nil {
		t.Fatal("connected to an unknown database")
	} else {
		var pgErr *pgconn.PgError
		if !errors.As(err, &pgErr) || pgErr.Code != "3D000" {
			t.Fatalf("unknown database at startup: %v", err)
		}
	}

	// DROP: non-empty without CASCADE, reserved, the current one.
	if _, serr := trySQL(ctx, root, `DROP DATABASE app`); serr == nil || serr.Code != sql.CodeDependentObjectsExist {
		t.Fatalf("DROP non-empty: %v", serr)
	}
	for _, stmt := range []string{`DROP DATABASE datax`, `DROP DATABASE system`, `ALTER DATABASE system RENAME TO sys2`, `CREATE TABLE system.t (id INT8 PRIMARY KEY)`} {
		if _, serr := trySQL(ctx, root, stmt); serr == nil || serr.Code != sql.CodeInsufficientPriv {
			t.Fatalf("%s: %v", stmt, serr)
		}
	}
	execSQL(t, ctx, root, `USE analytics`)
	if _, serr := trySQL(ctx, root, `DROP DATABASE analytics`); serr == nil || serr.Code != sql.CodeObjectInUse {
		t.Fatalf("DROP current: %v", serr)
	}
	execSQL(t, ctx, root, `USE datax`)
	execSQL(t, ctx, root, `DROP DATABASE analytics CASCADE`)
	if _, serr := trySQL(ctx, root, `SELECT id FROM analytics.items`); serr == nil || serr.Code != sql.CodeInvalidCatalogName {
		t.Fatalf("after CASCADE: %v", serr)
	}
	execSQL(t, ctx, root, `DROP DATABASE IF EXISTS analytics`)

	// Rename keeps the tables.
	execSQL(t, ctx, root, `ALTER DATABASE app RENAME TO shop`)
	if r := execSQL(t, ctx, root, `SELECT name FROM shop.items ORDER BY id`); len(r.Rows) != 2 {
		t.Fatalf("shop.items after rename: %+v", r.Rows)
	}
	if _, serr := trySQL(ctx, root, `SELECT id FROM app.items`); serr == nil || serr.Code != sql.CodeInvalidCatalogName {
		t.Fatalf("old name after rename: %v", serr)
	}

	// Database privileges: CREATE lets a user make tables, CONNECT closes
	// a database to the public.
	execSQL(t, ctx, root, `CREATE USER bob PASSWORD 'pw'`)
	bob := sql.NewSessionForUser(tc.Nodes[2].DB(), catalog.NewAccessor(), "bob")
	execSQL(t, ctx, bob, `USE shop`)
	if _, serr := trySQL(ctx, bob, `CREATE TABLE carts (id INT8 PRIMARY KEY)`); serr == nil || serr.Code != sql.CodeInsufficientPriv {
		t.Fatalf("bob CREATE without grant: %v", serr)
	}
	execSQL(t, ctx, root, `GRANT CREATE ON DATABASE shop TO bob`)
	execSQL(t, ctx, bob, `CREATE TABLE carts (id INT8 PRIMARY KEY)`)
	if _, serr := trySQL(ctx, bob, `CREATE TABLE datax.carts (id INT8 PRIMARY KEY)`); serr == nil || serr.Code != sql.CodeInsufficientPriv {
		t.Fatalf("bob CREATE in datax: %v", serr)
	}
	execSQL(t, ctx, root, `REVOKE CONNECT ON DATABASE shop FROM public`)
	bob2 := sql.NewSessionForUser(tc.Nodes[2].DB(), catalog.NewAccessor(), "bob")
	if _, serr := trySQL(ctx, bob2, `USE shop`); serr == nil || serr.Code != sql.CodeInsufficientPriv {
		t.Fatalf("bob USE after REVOKE CONNECT: %v", serr)
	}
	if _, err := pgx.Connect(ctx, "postgres://bob@"+tc.Nodes[1].SQLAddr()+"/shop?sslmode=disable"); err == nil {
		t.Fatal("bob connected to a closed database")
	}
	execSQL(t, ctx, root, `GRANT CONNECT ON DATABASE shop TO bob`)
	execSQL(t, ctx, bob2, `USE shop`)
	if _, serr := trySQL(ctx, root, `GRANT SELECT ON DATABASE shop TO bob`); serr == nil || serr.Code != sql.CodeSyntaxError {
		t.Fatalf("table privilege on a database: %v", serr)
	}
	execSQL(t, ctx, root, `GRANT SELECT ON shop.items TO bob`)
	if r := execSQL(t, ctx, bob2, `SELECT name FROM items ORDER BY id`); len(r.Rows) != 2 {
		t.Fatalf("bob reads shop.items: %+v", r.Rows)
	}

	// The schema browser names each table's database.
	_, _, body := httpGet(t, "http://"+tc.Nodes[0].HTTPAddr()+"/api/schema")
	var sd server.SchemaStatus
	if err := jsonUnmarshal([]byte(body), &sd); err != nil {
		t.Fatal(err)
	}
	seen := map[string]string{}
	for _, tbl := range sd.Tables {
		seen[tbl.Database+"."+tbl.Name] = tbl.Database
	}
	if seen["shop.items"] != "shop" || seen["datax.items"] != "datax" || seen["shop.carts"] != "shop" {
		t.Fatalf("schema browser databases: %v", seen)
	}

	// Backups carry the database catalog: a restore into a fresh cluster
	// brings shop back with its tables.
	dir := t.TempDir()
	if _, err := tc.Nodes[0].RunBackup(ctx, dir, "", false, false); err != nil {
		t.Fatal(err)
	}
	tc2 := Start(t, 1)
	if _, err := tc2.Nodes[0].RunRestore(ctx, []string{dir}); err != nil {
		t.Fatal(err)
	}
	root2 := sql.NewSession(tc2.Nodes[0].DB(), catalog.NewAccessor())
	if r := execSQL(t, ctx, root2, `SELECT name FROM shop.items ORDER BY id`); len(r.Rows) != 2 || r.Rows[0][0].S != "app" {
		t.Fatalf("restored shop.items: %+v", r.Rows)
	}
	if r := execSQL(t, ctx, root2, `SELECT name FROM items`); len(r.Rows) != 1 || r.Rows[0][0].S != "default" {
		t.Fatalf("restored datax.items: %+v", r.Rows)
	}
}
