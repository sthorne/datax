package main

import (
	"bytes"
	"context"
	"encoding/hex"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

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
