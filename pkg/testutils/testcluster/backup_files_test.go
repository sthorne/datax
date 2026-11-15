package testcluster

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/sthorne/datax/pkg/sql"
	"github.com/sthorne/datax/pkg/sql/catalog"
)

// TestBackupFilesAndFailure (issue #313): a backup's files are readable
// by the node's user alone — a plaintext backup holds every row — and a
// backup that fails leaves nothing in its destination: no data file of
// the tables it had already written, no manifest, nothing a later look
// could take for a backup, finished or not.
func TestBackupFilesAndFailure(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	tc := Start(t, 1)
	n := tc.Nodes[0]
	s := sql.NewSession(n.DB(), catalog.NewAccessor())
	waitForDatabases(t, ctx, s)
	execSQL(t, ctx, s, `CREATE TABLE a (id INT8 PRIMARY KEY)`)
	execSQL(t, ctx, s, `CREATE TABLE b (id INT8 PRIMARY KEY)`)
	execSQL(t, ctx, s, `INSERT INTO a VALUES (1)`)
	execSQL(t, ctx, s, `INSERT INTO b VALUES (1)`)

	dest := filepath.Join(t.TempDir(), "full")
	sum, err := n.RunBackup(ctx, dest, "", false, false)
	if err != nil {
		t.Fatal(err)
	}
	if fi, err := os.Stat(dest); err != nil || fi.Mode().Perm() != 0o700 {
		t.Fatalf("backup directory: %v %v, want 0700", fi.Mode(), err)
	}
	entries, err := os.ReadDir(dest)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		fi, err := e.Info()
		if err != nil {
			t.Fatal(err)
		}
		if !fi.Mode().IsRegular() || fi.Mode().Perm() != 0o600 {
			t.Errorf("%s: %v, want a 0600 file", e.Name(), fi.Mode())
		}
	}
	if len(entries) != len(sum.Tables)+1 {
		t.Errorf("the backup directory holds %d entries for %d tables and a manifest", len(entries), len(sum.Tables))
	}

	// A backup that fails part way: the destination already has a
	// directory where table b's file goes, so b's file cannot be written
	// — after a's has been.
	failed := filepath.Join(t.TempDir(), "failed")
	if err := os.MkdirAll(filepath.Join(failed, "table_"+itoa(int(tableSummary(t, sum, "b").ID))+".dxbk"), 0o700); err != nil {
		t.Fatal(err)
	}
	if _, err := n.RunBackup(ctx, failed, "", false, false); err == nil {
		t.Fatal("the backup succeeded over a directory at a data file's name")
	}
	entries, err = os.ReadDir(failed)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if !e.IsDir() || e.Name() != "table_"+itoa(int(tableSummary(t, sum, "b").ID))+".dxbk" {
			t.Errorf("a failed backup left %s behind", e.Name())
		}
	}
	// And the destination takes a backup once the obstacle is gone.
	if err := os.Remove(filepath.Join(failed, "table_"+itoa(int(tableSummary(t, sum, "b").ID))+".dxbk")); err != nil {
		t.Fatal(err)
	}
	if _, err := n.RunBackup(ctx, failed, "", false, false); err != nil {
		t.Fatalf("a backup into the failed one's directory: %v", err)
	}
}
