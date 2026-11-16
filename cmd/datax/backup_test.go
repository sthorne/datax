package main

import (
	"bytes"
	"context"
	"encoding/hex"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/sthorne/datax/pkg/cluster"
	"github.com/sthorne/datax/pkg/server"
	"github.com/sthorne/datax/pkg/sql"
	"github.com/sthorne/datax/pkg/sql/catalog"
	"github.com/sthorne/datax/pkg/sql/parser"
	"github.com/sthorne/datax/pkg/storage/enc"
)

func startBackupTestNode(t *testing.T) *server.Node {
	t.Helper()
	n, err := server.Start(server.Config{
		Listen: "127.0.0.1:0", Dir: t.TempDir(), BootstrapSelf: true,
		GCInterval: -1, MetricsRecordInterval: -1,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(n.Stop)
	return n
}

func backupTestSQL(t *testing.T, n *server.Node, src string) *sql.Result {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	stmts, err := parser.Parse(src)
	if err != nil {
		t.Fatal(err)
	}
	s := sql.NewSession(n.DB(), catalog.NewAccessor())
	var last *sql.Result
	for _, stmt := range stmts {
		res, serr := s.Execute(ctx, stmt, nil)
		if serr != nil {
			t.Fatalf("sql %q: %s", src, serr.Msg)
		}
		last = res
	}
	return last
}

// TestBackupKeyFlags (issue #291): `datax backup --backup-key` and
// `datax restore --backup-key` end to end against running nodes — key
// files raw and hex, a chain under two keys named in --src order, and
// the refusals the flags can produce on their own.
func TestBackupKeyFlags(t *testing.T) {
	src := startBackupTestNode(t)
	backupTestSQL(t, src, `CREATE TABLE cli_orders (id INT8 PRIMARY KEY, v TEXT)`)
	backupTestSQL(t, src, `INSERT INTO cli_orders VALUES (1, 'first-capybara')`)

	work := t.TempDir()
	keyA, keyB := filepath.Join(work, "a.key"), filepath.Join(work, "b.key")
	if err := os.WriteFile(keyA, bytes.Repeat([]byte{0xa1}, enc.KeyLen), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(keyB, []byte(hex.EncodeToString(bytes.Repeat([]byte{0xb2}, enc.KeyLen))+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	short := filepath.Join(work, "short.key")
	if err := os.WriteFile(short, []byte("too short"), 0o600); err != nil {
		t.Fatal(err)
	}
	full, inc := filepath.Join(work, "full"), filepath.Join(work, "inc")
	addr := []string{"--addr", src.Addr()}
	args := func(a ...string) []string { return append(append([]string(nil), addr...), a...) }

	for _, c := range []struct {
		what string
		args []string
		want string
	}{
		{"a key file of the wrong size", args("--dest", full, "--backup-key", short), "want 32 raw bytes or 64 hex characters"},
		{"a key file that is not there", args("--dest", full, "--backup-key", filepath.Join(work, "none.key")), "--backup-key"},
		{"a key and --allow-plaintext", args("--dest", full, "--backup-key", keyA, "--allow-plaintext"), "pass one of them"},
		{"--base-key without --base", args("--dest", full, "--backup-key", keyA, "--base-key", keyA), "no --base was given"},
	} {
		if err := runBackupCmd(c.args); err == nil || !strings.Contains(err.Error(), c.want) {
			t.Fatalf("backup with %s: %v", c.what, err)
		}
	}
	if _, err := os.Stat(full); err == nil {
		t.Fatal("a refused backup command reached the node")
	}

	if err := runBackupCmd(args("--dest", full, "--backup-key", keyA)); err != nil {
		t.Fatalf("sealed backup: %v", err)
	}
	backupTestSQL(t, src, `INSERT INTO cli_orders VALUES (2, 'second-capybara')`)
	if err := runBackupCmd(args("--dest", inc, "--base", full, "--backup-key", keyB)); err == nil || !strings.Contains(err.Error(), "does not open this backup") {
		t.Fatalf("incremental under another key without --base-key: %v", err)
	}
	if err := runBackupCmd(args("--dest", inc, "--base", full, "--backup-key", keyB, "--base-key", keyA)); err != nil {
		t.Fatalf("sealed incremental: %v", err)
	}
	for _, dir := range []string{full, inc} {
		entries, err := os.ReadDir(dir)
		if err != nil || len(entries) < 2 {
			t.Fatalf("%s: %v %v", dir, entries, err)
		}
		for _, e := range entries {
			raw, err := os.ReadFile(filepath.Join(dir, e.Name()))
			if err != nil {
				t.Fatal(err)
			}
			if bytes.Contains(raw, []byte("capybara")) || bytes.Contains(raw, []byte("cli_orders")) || strings.Contains(e.Name(), "cli_orders") {
				t.Fatalf("%s/%s holds the table's name or a row in the clear", dir, e.Name())
			}
		}
	}

	dst := startBackupTestNode(t)
	chain := full + "," + inc
	rargs := func(a ...string) []string { return append([]string{"--addr", dst.Addr(), "--src", chain}, a...) }
	for _, c := range []struct {
		what string
		args []string
		want string
	}{
		{"no key", rargs(), "is a sealed backup and no key was given"},
		{"one key for a chain under two", rargs("--backup-key", keyA), "does not open this backup"},
		{"the keys in the wrong order", rargs("--backup-key", keyB+","+keyA), "does not open this backup"},
		{"three keys for two directories", rargs("--backup-key", keyA+","+keyB+","+keyA), "names 3 keys for 2 backup directories"},
		{"an empty entry for a sealed directory", rargs("--backup-key", keyA+","), "is a sealed backup and no key was given"},
		{"a key file of the wrong size", rargs("--backup-key", keyA+","+short), "want 32 raw bytes or 64 hex characters"},
	} {
		if err := runRestoreCmd(c.args); err == nil || !strings.Contains(err.Error(), c.want) {
			t.Fatalf("restore with %s: %v", c.what, err)
		}
	}
	if err := runRestoreCmd(rargs("--backup-key", keyA+","+keyB)); err != nil {
		t.Fatalf("restore of the sealed chain: %v", err)
	}
	res := backupTestSQL(t, dst, `SELECT v FROM cli_orders ORDER BY id`)
	if len(res.Rows) != 2 || res.Rows[0][0].S != "first-capybara" || res.Rows[1][0].S != "second-capybara" {
		t.Fatalf("restored rows: %+v", res.Rows)
	}
}

// TestRestoreSelectionFlags (issues #301, #302): `datax backup
// --revision-history` and `datax restore --table / --into / --as-of`
// end to end against a running node — a table brought back beside the
// live one as it stood before a bad UPDATE — and the refusals the flags
// produce on their own.
func TestRestoreSelectionFlags(t *testing.T) {
	n := startBackupTestNode(t)
	backupTestSQL(t, n, `CREATE TABLE cli_prices (id INT8 PRIMARY KEY, cents INT8)`)
	backupTestSQL(t, n, `INSERT INTO cli_prices VALUES (1, 100), (2, 250)`)
	work := t.TempDir()
	full, inc := filepath.Join(work, "full"), filepath.Join(work, "inc")
	args := func(a ...string) []string { return append([]string{"--addr", n.Addr()}, a...) }

	// The node makes its audit table when it first writes an audit record,
	// in the background. Made between the full and the incremental backup
	// below, it would be a table created inside the chain, and a restore
	// of everything to a moment in between is refused for it before the
	// cluster-in-use check this test expects. A backup is an audited admin
	// operation: take one elsewhere, flush the audit buffer and wait for
	// the table, so it predates the chain.
	if err := runBackupCmd(args("--dest", filepath.Join(work, "warmup"))); err != nil {
		t.Fatal(err)
	}
	{
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		s := sql.NewSession(n.DB(), catalog.NewAccessor())
		stmt, err := parser.Parse(`SELECT count(*) FROM ` + catalog.AuditTableName)
		if err != nil {
			t.Fatal(err)
		}
		for {
			n.AuditFlush(ctx)
			_, serr := s.Execute(ctx, stmt[0], nil)
			if serr == nil {
				break
			}
			if ctx.Err() != nil {
				t.Fatalf("the audit table was never made: %v", serr)
			}
			time.Sleep(50 * time.Millisecond)
		}
		cancel()
	}
	if err := runBackupCmd(args("--dest", full, "--revision-history")); err == nil || !strings.Contains(err.Error(), "a full backup has no base") {
		t.Fatalf("a full backup with --revision-history: %v", err)
	}
	if _, err := os.Stat(full); err == nil {
		t.Fatal("a refused backup command reached the node")
	}
	if err := runBackupCmd(args("--dest", full)); err != nil {
		t.Fatal(err)
	}
	backupTestSQL(t, n, `UPDATE cli_prices SET cents = 300 WHERE id = 2`)
	time.Sleep(2 * time.Millisecond)
	before := strconv.FormatInt(n.Clock().Now().WallTime, 10)
	time.Sleep(2 * time.Millisecond)
	backupTestSQL(t, n, `UPDATE cli_prices SET cents = 0`) // the bad UPDATE
	if err := runBackupCmd(args("--dest", inc, "--base", full, "--revision-history")); err != nil {
		t.Fatalf("incremental with revision history: %v", err)
	}

	rargs := func(a ...string) []string { return args(append([]string{"--src", full + "," + inc}, a...)...) }
	for _, c := range []struct {
		what string
		args []string
		want string
	}{
		{"--table and --database", rargs("--table", "cli_prices", "--database", "datax"), "two restores"},
		{"--into without a table", rargs("--into", "x"), "belong to a restore of tables"},
		{"--drop-foreign-keys without a table", rargs("--drop-foreign-keys"), "belong to a restore of tables"},
		{"two names for one table", rargs("--table", "cli_prices", "--into", "a,b"), "names 2 tables for the 1 in --table"},
		{"a time that is not one", rargs("--table", "cli_prices", "--into", "x", "--as-of", "last tuesday"), "cannot interpret"},
		{"a time before the chain", rargs("--table", "cli_prices", "--into", "x", "--as-of", "1000"), "is earlier than the full backup"},
		{"a table the backup lacks", rargs("--table", "nope"), "the backup has no table datax.nope"},
		{"the live table's own name", rargs("--table", "cli_prices"), "already exists"},
		{"everything, into a cluster in use", rargs("--as-of", before), "restore requires an empty cluster"},
	} {
		if err := runRestoreCmd(c.args); err == nil || !strings.Contains(err.Error(), c.want) {
			t.Fatalf("restore with %s: %v", c.what, err)
		}
	}
	if err := runRestoreCmd(rargs("--table", "cli_prices", "--into", "cli_prices_before", "--as-of", before)); err != nil {
		t.Fatalf("table restore to a moment: %v", err)
	}
	res := backupTestSQL(t, n, `SELECT cents FROM cli_prices_before ORDER BY id`)
	if len(res.Rows) != 2 || res.Rows[0][0].I != 100 || res.Rows[1][0].I != 300 {
		t.Fatalf("the table before the bad UPDATE: %+v", res.Rows)
	}
	// Copying the rows back is SQL.
	backupTestSQL(t, n, `BEGIN; DELETE FROM cli_prices WHERE id IN (SELECT id FROM cli_prices_before); INSERT INTO cli_prices SELECT * FROM cli_prices_before; COMMIT`)
	res = backupTestSQL(t, n, `SELECT cents FROM cli_prices ORDER BY id`)
	if len(res.Rows) != 2 || res.Rows[0][0].I != 100 || res.Rows[1][0].I != 300 {
		t.Fatalf("the live table after copying back: %+v", res.Rows)
	}

	// What the flags send: a request under an op name of its own exactly
	// when a node from before them would do something else with it.
	for _, c := range []struct {
		asOf, tables, database, into string
		wantOp                       string
	}{
		{"", "", "", "", "restore"},
		{"12", "", "", "", "restore-select"},
		{"", "a,b", "", "", "restore-select"},
		{"", "", "d", "e", "restore-select"},
	} {
		req := cluster.AdminRequest{Op: "restore"}
		if err := restoreSelection(&req, c.asOf, c.tables, c.database, c.into, false); err != nil || req.Op != c.wantOp {
			t.Fatalf("%+v: op %q, err %v", c, req.Op, err)
		}
		if c.database != "" && (req.Database != "d" || req.IntoDatabase != "e" || len(req.Into) != 0) {
			t.Fatalf("database restore request: %+v", req)
		}
	}
}

// TestSealedOpNames (issue #313): a sealed backup travels under an admin
// op of its own. Under "backup", a node from before sealed backups
// ignores the backup_key field it does not know and writes the backup in
// plaintext, reporting success; under "backup-sealed" it refuses the
// unknown op. A restore given keys likewise travels as "restore-sealed":
// an older node would ignore the keys and restore a plaintext directory
// put in a sealed backup's place, which is what the keys refuse. The
// serving node takes both the new ops and the old ones with keys, which
// is what a command from before this change sends.
func TestSealedOpNames(t *testing.T) {
	key := bytes.Repeat([]byte{0xa1}, enc.KeyLen)
	for _, c := range []struct {
		what    string
		req     cluster.AdminRequest
		history bool
		want    string
	}{
		{"plaintext", cluster.AdminRequest{}, false, "backup"},
		{"sealed", cluster.AdminRequest{BackupKey: key}, false, "backup-sealed"},
		{"plaintext on a sealed base", cluster.AdminRequest{BaseBackupKey: key}, false, "backup"},
		{"with history", cluster.AdminRequest{}, true, "backup-history"},
		{"sealed, with history", cluster.AdminRequest{BackupKey: key}, true, "backup-history"},
	} {
		if got := backupOp(c.req, c.history); got != c.want {
			t.Errorf("backup %s: op %q, want %q", c.what, got, c.want)
		}
	}
	for _, c := range []struct {
		what string
		keys [][]byte
		want string
	}{
		{"no keys", nil, "restore"},
		{"empty entries only", [][]byte{nil, nil}, "restore"},
		{"one key", [][]byte{key}, "restore-sealed"},
		{"a key for the second directory", [][]byte{nil, key}, "restore-sealed"},
	} {
		req := cluster.AdminRequest{Op: "restore", BackupKeys: c.keys}
		if got := restoreOp(req); got != c.want {
			t.Errorf("restore with %s: op %q, want %q", c.what, got, c.want)
		}
	}
	if got := restoreOp(cluster.AdminRequest{Op: "restore-select", BackupKeys: [][]byte{key}}); got != "restore-select" {
		t.Errorf("a selective restore with a key: op %q", got)
	}

	n := startBackupTestNode(t)
	backupTestSQL(t, n, `CREATE TABLE op_names (id INT8 PRIMARY KEY)`)
	work := t.TempDir()
	call := func(req cluster.AdminRequest) string {
		t.Helper()
		resp, err := adminCall(n.Addr(), "", "root", 10*time.Second, req)
		if err != nil {
			return err.Error()
		}
		return resp.Error
	}
	if got := call(cluster.AdminRequest{Op: "backup-sealed", Path: filepath.Join(work, "nokey")}); !strings.Contains(got, "backup-sealed carries the backup key") {
		t.Fatalf("backup-sealed without a key: %q", got)
	}
	sealed := filepath.Join(work, "sealed")
	if got := call(cluster.AdminRequest{Op: "backup-sealed", Path: sealed, BackupKey: key}); got != "" {
		t.Fatalf("backup-sealed: %s", got)
	}
	if got := call(cluster.AdminRequest{Op: "backup", Path: filepath.Join(work, "old-style"), BackupKey: key}); got != "" {
		t.Fatalf("backup with a key, as an older command sends it: %s", got)
	}
	dst := startBackupTestNode(t)
	if resp, err := adminCall(dst.Addr(), "", "root", 10*time.Second, cluster.AdminRequest{Op: "restore-sealed", Paths: []string{sealed}}); err == nil && !strings.Contains(resp.Error, "restore-sealed carries the backup keys") {
		t.Fatalf("restore-sealed without keys: %q", resp.Error)
	}
	if resp, err := adminCall(dst.Addr(), "", "root", 10*time.Second, cluster.AdminRequest{Op: "restore-sealed", Paths: []string{sealed}, BackupKeys: [][]byte{key}}); err != nil || resp.Error != "" {
		t.Fatalf("restore-sealed: %v %s", err, resp.Error)
	}
	backupTestSQL(t, dst, `SELECT * FROM op_names`)
}
