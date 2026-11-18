package testcluster

import (
	"bytes"
	"context"
	"net"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/sthorne/datax/pkg/cluster"
	"github.com/sthorne/datax/pkg/server"
	"github.com/sthorne/datax/pkg/sql"
	"github.com/sthorne/datax/pkg/sql/catalog"
	"github.com/sthorne/datax/pkg/storage/enc"
)

// The cluster's own tables are created by the nodes as root, in the
// datax database, and often only on first use. ALTER DEFAULT PRIVILEGES
// rules for root therefore reach the ones created after the rule, as
// they reach any table root creates; the sealed ones — the audit table,
// the large-object tables and the named keys — are the exception.

// openSystemTables are the system tables default privileges reach.
var openSystemTables = []string{
	catalog.MetricsTableName, catalog.ShapesMetricsTableName,
	catalog.StatementShapesTableName, catalog.PrefsTableName,
}

// sealedSystemTables are the system tables they do not reach (named
// keys apart: TestDefaultPrivilegesKeysTable has them).
var sealedSystemTables = []string{
	catalog.AuditTableName, catalog.LargeObjectTableName,
	catalog.LargeObjectMetadataTableName, catalog.LargeObjectPendingTableName,
}

// systemTableACLs returns pg_class.relacl of every system table that
// exists, by name. The named keys are left out: pg_class always lists
// their virtual table, whatever the real one's state.
func systemTableACLs(t *testing.T, ctx context.Context, s *sql.Session) map[string]string {
	t.Helper()
	res := execSQL(t, ctx, s, `SELECT relname, relacl FROM pg_class`)
	out := map[string]string{}
	for _, r := range res.Rows {
		if catalog.IsSystemTable(r[0].S) && !catalog.IsUnreadableSystemTable(r[0].S) {
			out[r[0].S] = r[1].S
		}
	}
	return out
}

// grantedTables returns the tables information_schema.role_table_grants
// lists for grantee, each with its privileges.
func grantedTables(t *testing.T, ctx context.Context, s *sql.Session, grantee string) map[string]string {
	t.Helper()
	res := execSQL(t, ctx, s, `SELECT table_name, privilege_type FROM information_schema.role_table_grants WHERE grantee = '`+grantee+`'`)
	privs := map[string][]string{}
	for _, r := range res.Rows {
		privs[r[0].S] = append(privs[r[0].S], r[1].S)
	}
	out := map[string]string{}
	for name, p := range privs {
		sort.Strings(p)
		out[name] = strings.Join(p, ",")
	}
	return out
}

// dpSystemConfig keeps every lazily created system table from being
// created until the test asks for it: no metrics ticks, no large-object
// pass, and an audit flush only when the test calls one.
func dpSystemConfig(c *server.Config) {
	c.AuditFlushInterval = time.Hour
	c.MetricsRecordInterval = -1
	c.LargeObjectSweepInterval = -1
	c.LargeObjectUploadGrace = loTestGrace
}

// recordMetrics runs one metrics tick on n, which also creates the shape
// tables, until it writes rows.
func recordMetrics(t *testing.T, ctx context.Context, n *server.Node) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for n.RecordMetricsAt(ctx, 10*time.Second, time.Now()) <= 0 {
		if time.Now().After(deadline) {
			t.Fatal("the metrics recorder wrote nothing")
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// flushAuditUntil flushes every node's audit buffer until root reads a
// record of event on target.
func flushAuditUntil(t *testing.T, ctx context.Context, tc *TestCluster, root *sql.Session, event, target string) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for {
		for _, n := range tc.Nodes {
			n.AuditFlush(ctx)
		}
		if findAudit(auditRecords(t, ctx, root), event, target) != nil {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("no %s record on %q in datax_audit", event, target)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// TestDefaultPrivilegesSystemTables: ALTER DEFAULT PRIVILEGES made by
// root, restrictive and permissive, before the cluster's own tables are
// created. The metrics, shape and console-preference tables take the
// grants, shown in pg_class.relacl and information_schema, and a SELECT
// grant reads them while no grant writes them. The audit and
// large-object tables take none: a blanket rule must not open the audit
// trail or every large object's bytes. The nodes' own work — metrics
// ticks, audit flushes, console preferences, the lo_* functions and
// the sweeper, advisory locks, backup and restore — goes on regardless.
func TestDefaultPrivilegesSystemTables(t *testing.T) {
	sql.TestingLargeObjectHeartbeat = loTestGrace / 10
	t.Cleanup(func() { sql.TestingLargeObjectHeartbeat = 0 })
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	first := true
	tc := StartWithOptions(t, 3, func(c *server.Config) {
		dpSystemConfig(c)
		if first {
			c.HTTPListener = lis
			first = false
		}
	})
	ctx, cancel := context.WithTimeout(context.Background(), 240*time.Second)
	defer cancel()
	root := leasedSession(t, tc, 0, 2*time.Second)
	waitForDatabases(t, ctx, root)
	if got := systemTableACLs(t, ctx, root); len(got) != 0 {
		t.Fatalf("system tables exist before the test created them: %v", got)
	}

	for _, q := range []string{
		`CREATE USER dp_reader`, `CREATE USER dp_all`, `CREATE USER dp_lo`,
		// Restrictive: PUBLIC and the owner itself.
		`ALTER DEFAULT PRIVILEGES REVOKE ALL ON TABLES FROM PUBLIC`,
		`ALTER DEFAULT PRIVILEGES FOR ROLE root REVOKE ALL ON TABLES FROM root`,
		// Permissive.
		`ALTER DEFAULT PRIVILEGES GRANT SELECT ON TABLES TO dp_reader`,
		`ALTER DEFAULT PRIVILEGES FOR ROLE root GRANT ALL ON TABLES TO dp_all WITH GRANT OPTION`,
		`ALTER DEFAULT PRIVILEGES GRANT SELECT ON TABLES TO PUBLIC`,
	} {
		execSQL(t, ctx, root, q)
	}

	// Each table's first use creates it.
	recordMetrics(t, ctx, tc.Nodes[0])
	if code, body := setPref(t, tc, 0, "theme", "dark"); code != 200 {
		t.Fatalf("POST /api/prefs: %d %s", code, body)
	}
	execSQL(t, ctx, root, `CREATE ROLE dp_audited`)
	flushAuditUntil(t, ctx, tc, root, "role-ddl", "dp_audited")
	loURL := func(user string) string { return strings.Replace(pgURL(tc, 1), "root@", user+"@", 1) }
	loConn, err := pgx.Connect(ctx, loURL("dp_lo"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = loConn.Close(context.Background()) })
	data := randomBytes(t, 3*sql.LargeObjectPageSize/2)
	oid := loStore(t, ctx, loConn, data, 100_000)
	if got := loFetch(t, ctx, loConn, oid, 1<<20); !bytes.Equal(got, data) {
		t.Fatal("the owner cannot read its large object back")
	}

	// The grants, as the catalogs show them.
	acls := systemTableACLs(t, ctx, root)
	for _, name := range openSystemTables {
		acl, ok := acls[name]
		if !ok {
			t.Fatalf("%s was not created (have %v)", name, acls)
		}
		for _, want := range []string{"dp_reader=r/root", "dp_all=a*r*w*d*D*/root", "=r/root"} {
			if !strings.Contains(acl, want) {
				t.Errorf("%s relacl %q: no %q", name, acl, want)
			}
		}
	}
	for _, name := range sealedSystemTables {
		acl, ok := acls[name]
		if !ok {
			t.Fatalf("%s was not created (have %v)", name, acls)
		}
		if acl != "" {
			t.Errorf("%s took default privileges: relacl %q", name, acl)
		}
	}
	got := grantedTables(t, ctx, root, "dp_reader")
	for _, name := range openSystemTables {
		if got[name] != "SELECT" {
			t.Errorf("role_table_grants for dp_reader on %s: %q, want SELECT", name, got[name])
		}
	}
	for _, name := range sealedSystemTables {
		if p, ok := got[name]; ok {
			t.Errorf("role_table_grants lists dp_reader on %s: %q", name, p)
		}
	}

	// What the grants open, and what they do not.
	reader := userSession(t, tc, 1, "dp_reader")
	all := userSession(t, tc, 2, "dp_all")
	nobody := userSession(t, tc, 2, "dp_lo")
	for _, name := range openSystemTables {
		execEventually(t, ctx, reader, `SELECT count(*) FROM `+name)
		execEventually(t, ctx, nobody, `SELECT count(*) FROM `+name) // PUBLIC's grant
		denied(t, ctx, all, `DELETE FROM `+name+` WHERE false`)
		denied(t, ctx, all, `TRUNCATE `+name)
	}
	for _, name := range sealedSystemTables {
		for _, s := range []*sql.Session{reader, all, nobody} {
			denied(t, ctx, s, `SELECT count(*) FROM `+name)
		}
		denied(t, ctx, all, `DELETE FROM `+name+` WHERE false`)
	}
	readerConn, err := pgx.Connect(ctx, loURL("dp_reader"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = readerConn.Close(context.Background()) })
	if _, err := readerConn.Exec(ctx, `SELECT lo_get($1)`, oid); sqlState(err) != "42501" {
		t.Fatalf("dp_reader reads dp_lo's large object: %v", err)
	}
	// An admin can still give the audit table to a reader on purpose.
	execSQL(t, ctx, root, `GRANT SELECT ON datax_audit TO dp_reader`)
	execEventually(t, ctx, reader, `SELECT count(*) FROM datax_audit`)
	execSQL(t, ctx, root, `REVOKE SELECT ON datax_audit FROM dp_reader`)
	awaitDenied(t, ctx, reader, `SELECT count(*) FROM datax_audit`)
	// A sealed table created before default privileges stopped reaching
	// it carries every privilege a rule stamped on it (the node's own
	// session stands in for that rule here). Only SELECT is granted on
	// it, but an admin may revoke the rest.
	sys := sql.NewSystemSession(tc.Nodes[0].DB(), catalog.NewAccessor())
	for _, name := range sealedSystemTables {
		execSQL(t, ctx, sys, `GRANT ALL ON `+name+` TO dp_all WITH GRANT OPTION`)
	}
	sealedACLs := func(want func(string) bool) {
		t.Helper()
		deadline := time.Now().Add(20 * time.Second) // root's leases catch up
		for {
			acls, ok := systemTableACLs(t, ctx, root), true
			for _, name := range sealedSystemTables {
				ok = ok && want(acls[name])
			}
			if ok {
				return
			}
			if time.Now().After(deadline) {
				t.Fatalf("sealed tables' relacl: %v", acls)
			}
			time.Sleep(50 * time.Millisecond)
		}
	}
	// The stand-in took no leases, so root's leased copies of the
	// descriptors may still lack the grants: the REVOKE reads them in
	// its own transaction.
	for _, name := range sealedSystemTables {
		expectCodeSQL(t, ctx, root, `GRANT INSERT ON `+name+` TO dp_all`, sql.CodeInsufficientPriv)
		execSQL(t, ctx, root, `REVOKE ALL ON `+name+` FROM dp_all`)
	}
	sealedACLs(func(acl string) bool { return acl == "" })

	// The nodes' own work is untouched.
	recordMetrics(t, ctx, tc.Nodes[1])
	if code, body := setPref(t, tc, 0, "theme", "light"); code != 200 {
		t.Fatalf("POST /api/prefs after the tables exist: %d %s", code, body)
	}
	execSQL(t, ctx, root, `CREATE ROLE dp_audited_later`)
	flushAuditUntil(t, ctx, tc, root, "role-ddl", "dp_audited_later")
	mustVerify(t, ctx, root, nil)
	patch := randomBytes(t, 1000)
	tx, err := loConn.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	obj, err := loOf(tx).Open(ctx, oid, pgx.LargeObjectModeWrite)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := obj.Write(patch); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	copy(data, patch)
	if got := loFetch(t, ctx, loConn, oid, 1<<20); !bytes.Equal(got, data) {
		t.Fatal("a write to an existing large object did not land")
	}
	gone := loStore(t, ctx, loConn, randomBytes(t, 1000), 1000)
	if _, err := loConn.Exec(ctx, `SELECT lo_unlink($1)`, gone); err != nil {
		t.Fatal(err)
	}
	if sw, err := tc.Nodes[0].RunLargeObjectSweepOnce(ctx); err != nil || sw.Contents == 0 {
		t.Fatalf("large-object sweep: %+v, %v", sw, err)
	}
	wantTry(t, ctx, loConn, `SELECT pg_try_advisory_lock(4242)`, true)
	wantTry(t, ctx, readerConn, `SELECT pg_try_advisory_lock(4242)`, false)
	wantTry(t, ctx, loConn, `SELECT pg_advisory_unlock(4242)`, true)
	wantTry(t, ctx, readerConn, `SELECT pg_try_advisory_lock(4242)`, true)
	wantTry(t, ctx, readerConn, `SELECT pg_advisory_unlock(4242)`, true)

	// Backup and restore carry the tables, their rows and their grants.
	dir := t.TempDir()
	if _, err := tc.Nodes[0].RunBackup(ctx, dir, "", true, true); err != nil {
		t.Fatalf("backup: %v", err)
	}
	tc.StopAll()
	tc2 := StartWithOptions(t, 1, dpSystemConfig)
	if _, err := tc2.Nodes[0].RunRestore(ctx, []string{dir}); err != nil {
		t.Fatalf("restore: %v", err)
	}
	root2 := leasedSession(t, tc2, 0, 2*time.Second)
	if findAudit(auditRecords(t, ctx, root2), "role-ddl", "dp_audited_later") == nil {
		t.Fatal("the audit records were not restored")
	}
	restored := systemTableACLs(t, ctx, root2)
	for name, acl := range acls {
		if restored[name] != acl {
			t.Errorf("%s relacl after restore: %q, was %q", name, restored[name], acl)
		}
	}
	conn2 := pgConn(t, ctx, tc2, 0)
	if got := loFetch(t, ctx, conn2, oid, 1<<20); !bytes.Equal(got, data) {
		t.Fatal("the large object did not survive backup and restore")
	}
	reader2 := userSession(t, tc2, 0, "dp_reader")
	execEventually(t, ctx, reader2, `SELECT count(*) FROM datax_metrics`)
	denied(t, ctx, reader2, `SELECT count(*) FROM datax_audit`)
	denied(t, ctx, reader2, `SELECT count(*) FROM datax_largeobject`)
	recordMetrics(t, ctx, tc2.Nodes[0])
	tc2.Nodes[0].AuditFlush(ctx)
	if st := tc2.Nodes[0].AuditStatus(); st.LastError != "" {
		t.Fatalf("audit after restore: %s", st.LastError)
	}
}

// keysTableDesc reads the real named-keys descriptor on node i (nil
// before it exists).
func keysTableDesc(t *testing.T, ctx context.Context, c *ckCluster, i int) *catalog.TableDescriptor {
	t.Helper()
	n := c.node(t, i)
	d, err := sql.LookupKeysTable(ctx, n.DB(), catalog.NewAccessor())
	if err != nil {
		t.Fatal(err)
	}
	return d
}

// TestDefaultPrivilegesKeysTable: default privileges made before the
// cluster key is enabled — which is when datax_keys is created — do not
// reach the named keys, and no grant shows their material: a session
// naming the table is given the metadata, to admins only, and there is
// no material column to ask for. Key creation, rotation, wrapping and
// unwrapping, a sealed table and backup and restore of the keys work
// as without the rules; an ordinary table created under them, a sealed
// one included, takes the grants as PostgreSQL's would.
func TestDefaultPrivilegesKeysTable(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Second)
	defer cancel()
	hook := func(i int, cfg *server.Config) {
		cfg.KeyCacheLease = keyLease
		cfg.AuditFlushInterval = time.Hour
		cfg.MetricsRecordInterval = -1
	}
	src := startCKCluster(t, ctx, hook)
	src.waitReported(3)
	root := ksConn(t, ctx, src, 0, "root", "topsecret")
	if d := keysTableDesc(t, ctx, src, 0); d != nil {
		t.Fatal("datax_keys exists before the cluster key")
	}
	for _, q := range []string{
		`CREATE USER dp_keys WITH PASSWORD 'pw-dp_keys'`,
		`CREATE USER dp_app WITH PASSWORD 'pw-dp_app'`,
		`ALTER DEFAULT PRIVILEGES REVOKE ALL ON TABLES FROM PUBLIC`,
		`ALTER DEFAULT PRIVILEGES GRANT SELECT ON TABLES TO dp_keys`,
		`ALTER DEFAULT PRIVILEGES GRANT SELECT ON TABLES TO PUBLIC`,
		`ALTER DEFAULT PRIVILEGES GRANT ALL ON TABLES TO dp_app WITH GRANT OPTION`,
	} {
		mustExec(t, ctx, root, q)
	}

	// Enabling the cluster key creates the table.
	if errText, _ := src.op(0, "init"); errText != "" {
		t.Fatalf("init: %s", errText)
	}
	src.waitFor("every node to hold the cluster key", 30*time.Second, func() bool {
		st := src.status(0)
		return holdsCurrent(st, 1) && holdsCurrent(st, 2) && holdsCurrent(st, 3)
	})
	d := keysTableDesc(t, ctx, src, 0)
	if d == nil {
		t.Fatal("datax_keys was not created by cluster-key init")
	}
	if len(d.Privileges) != 0 || len(d.GrantOptions) != 0 {
		t.Fatalf("datax_keys took default privileges: %v / %v", d.Privileges, d.GrantOptions)
	}

	// The key service works under the rules.
	mustExec(t, ctx, root, `CREATE ENCRYPTION KEY dpk`)
	mustExec(t, ctx, root, `GRANT USAGE ON ENCRYPTION KEY dpk TO dp_app`)
	app := ksConn(t, ctx, src, 1, "dp_app", "pw-dp_app")
	keysReader := ksConn(t, ctx, src, 2, "dp_keys", "pw-dp_keys")
	dataKey, kctx := randBytes(32), []byte("tenant:dp")
	blob, err := wrapWith(ctx, app, "dpk", dataKey, kctx)
	if err != nil {
		t.Fatal(err)
	}
	mustExec(t, ctx, root, `ALTER ENCRYPTION KEY dpk ROTATE`)
	time.Sleep(keyLease + 200*time.Millisecond)
	blob2, err := wrapWith(ctx, app, "dpk", dataKey, kctx)
	if err != nil || blob2[12] != 2 {
		t.Fatalf("a wrap after rotation: %x %v", blob2, err)
	}
	for _, b := range [][]byte{blob, blob2} {
		if got, err := unwrapWith(ctx, app, "dpk", b, kctx); err != nil || !bytes.Equal(got, dataKey) {
			t.Fatalf("unwrap: %v", err)
		}
	}
	_, err = wrapWith(ctx, keysReader, "dpk", dataKey, kctx)
	wantState(t, "wrap by a role the table-wide rules name", err, "42501", "permission denied")

	// No grant reaches the material, or the metadata: the name is the
	// metadata's virtual table, an admin's alone, with no material
	// column even for root.
	for _, q := range []string{`SELECT material FROM datax_keys`, `SELECT count(*) FROM datax_keys`, `SELECT name FROM datax_keys`} {
		_, err = keysReader.Exec(ctx, q)
		wantState(t, "dp_keys: "+q, err, "42501", "permission denied")
	}
	_, err = root.Exec(ctx, `SELECT material FROM datax_keys`)
	wantState(t, "root: the material column", err, "42703", "material")
	var listed int64
	if err := root.QueryRow(ctx, `SELECT count(*) FROM datax_keys WHERE name = 'dpk'`).Scan(&listed); err != nil || listed != 1 {
		t.Fatalf("root lists dpk %d times, %v", listed, err)
	}

	// An ordinary table, and a sealed one, take the grants.
	mustExec(t, ctx, root, `CREATE TABLE dp_sealed (id INT8 PRIMARY KEY, v TEXT) WITH (encryption_key = 'auto')`)
	mustExec(t, ctx, root, `INSERT INTO dp_sealed VALUES (1, 'one')`)
	var v string
	if err := keysReader.QueryRow(ctx, `SELECT v FROM dp_sealed WHERE id = 1`).Scan(&v); err != nil || v != "one" {
		t.Fatalf("dp_keys reads the sealed table its default grant covers: %q %v", v, err)
	}
	if _, err := keysReader.Exec(ctx, `INSERT INTO dp_sealed VALUES (2, 'two')`); sqlState(err) != "42501" {
		t.Fatalf("dp_keys writes the sealed table: %v", err)
	}

	// Backup and restore, into a cluster holding the source's key.
	backupDir := filepath.Join(t.TempDir(), "bk")
	if _, err := src.tc.Nodes[0].RunBackupWith(ctx, server.BackupOptions{Dest: backupDir, AllowPlaintext: true}); err != nil {
		t.Fatalf("backup: %v", err)
	}
	x, _ := enc.NewPeerExchange()
	resp := src.call(0, false, cluster.AdminRequest{Op: "cluster-key-export", ClusterKey: &cluster.ClusterKeyRequest{Exchange: x.Public()}})
	if resp.Error != "" {
		t.Fatalf("export: %s", resp.Error)
	}
	srcID := resp.ClusterKey.Status.ClusterID
	exported, err := x.OpenClusterKeys(resp.ClusterKey.Exchange, resp.ClusterKey.Sealed, []byte("datax cluster key export|"+srcID))
	if err != nil {
		t.Fatal(err)
	}
	exportRoot := writeRootKey(t, t.TempDir(), "export.key")
	rk, err := enc.ParseRoot(exportRoot)
	if err != nil {
		t.Fatal(err)
	}
	file, err := enc.ExportClusterKeys(ctx, rk, srcID, enc.ClusterKeyRef{ID: resp.ClusterKey.Status.ID, Version: resp.ClusterKey.Status.Version}, exported)
	if err != nil {
		t.Fatal(err)
	}
	enc.ZeroTransportKeys(exported)
	src.tc.StopAll()

	dst := startCKCluster(t, ctx, hook)
	dst.waitReported(3)
	if errText, _ := dst.op(0, "init"); errText != "" {
		t.Fatalf("init on the target: %s", errText)
	}
	opened, err := file.Open(ctx, []enc.Root{rk})
	if err != nil {
		t.Fatal(err)
	}
	if resp := dst.call(1, false, cluster.AdminRequest{Op: "cluster-key-import", ClusterKey: &cluster.ClusterKeyRequest{Keys: opened}}); resp.Error != "" {
		t.Fatalf("import: %s", resp.Error)
	}
	if _, err := dst.tc.Nodes[0].RunRestore(ctx, []string{backupDir}); err != nil {
		t.Fatalf("restore: %v", err)
	}
	if d := keysTableDesc(t, ctx, dst, 0); d == nil || len(d.Privileges) != 0 {
		t.Fatalf("datax_keys after restore: %+v", d)
	}
	app2 := ksConn(t, ctx, dst, 2, "dp_app", "pw-dp_app")
	for _, b := range [][]byte{blob, blob2} {
		if got, err := unwrapWith(ctx, app2, "dpk", b, kctx); err != nil || !bytes.Equal(got, dataKey) {
			t.Fatalf("unwrap after restore: %v", err)
		}
	}
	keysReader2 := ksConn(t, ctx, dst, 0, "dp_keys", "pw-dp_keys")
	if err := keysReader2.QueryRow(ctx, `SELECT v FROM dp_sealed WHERE id = 1`).Scan(&v); err != nil || v != "one" {
		t.Fatalf("the sealed table after restore: %q %v", v, err)
	}
	_, err = keysReader2.Exec(ctx, `SELECT material FROM datax_keys`)
	wantState(t, "the material column after restore", err, "42501", "permission denied")
}
