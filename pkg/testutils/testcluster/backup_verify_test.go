package testcluster

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/sthorne/datax/pkg/sql"
	"github.com/sthorne/datax/pkg/sql/catalog"
)

// TestRestoreVerifiesPlaintextBackup (issue #313): a restore of
// everything held a plaintext data file to its table's key span and its
// framing, and reported the manifest's checksums afterwards for the
// operator to compare — it did not enforce them, so a data file swapped
// for another backup's file of the same table restored silently. Every
// file is now held to its manifest's record count and checksum before
// anything is applied: the target is left empty and takes the right
// backup afterwards.
func TestRestoreVerifiesPlaintextBackup(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	tc := Start(t, 1)
	n := tc.Nodes[0]
	s := sql.NewSession(n.DB(), catalog.NewAccessor())
	waitForDatabases(t, ctx, s)
	execSQL(t, ctx, s, `CREATE TABLE t (id INT8 PRIMARY KEY, v TEXT)`)
	execSQL(t, ctx, s, `INSERT INTO t VALUES (1, 'one'), (2, 'two')`)
	older := t.TempDir()
	if _, err := n.RunBackup(ctx, older, "", false, false); err != nil {
		t.Fatal(err)
	}
	execSQL(t, ctx, s, `UPDATE t SET v = 'TWO' WHERE id = 2`)
	execSQL(t, ctx, s, `INSERT INTO t VALUES (3, 'three')`)
	dir := t.TempDir()
	sum, err := n.RunBackup(ctx, dir, "", false, false)
	if err != nil {
		t.Fatal(err)
	}
	file := "table_" + itoa(int(tableSummary(t, sum, "t").ID)) + ".dxbk"
	orig, err := os.ReadFile(filepath.Join(dir, file))
	if err != nil {
		t.Fatal(err)
	}
	swapped, err := os.ReadFile(filepath.Join(older, file))
	if err != nil {
		t.Fatal(err)
	}
	flipped := append([]byte(nil), orig...)
	flipped[len(flipped)-2] ^= 0x20 // a byte of the last value

	target := Start(t, 1)
	s2 := sql.NewSession(target.Nodes[0].DB(), catalog.NewAccessor())
	waitForDatabases(t, ctx, s2)
	for what, data := range map[string][]byte{"an older backup's file of the table": swapped, "a value changed": flipped} {
		if err := os.WriteFile(filepath.Join(dir, file), data, 0o600); err != nil {
			t.Fatal(err)
		}
		_, err := target.Nodes[0].RunRestore(ctx, []string{dir})
		if err == nil || !strings.Contains(err.Error(), "is not the file its backup's manifest describes") {
			t.Fatalf("%s: restore gave %v", what, err)
		}
		if _, serr := trySQL(ctx, s2, `SELECT 1 FROM t LIMIT 1`); serr == nil {
			t.Fatalf("%s: a refused restore left the table", what)
		}
	}
	if err := os.WriteFile(filepath.Join(dir, file), orig, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := target.Nodes[0].RunRestore(ctx, []string{dir}); err != nil {
		t.Fatalf("the repaired backup: %v", err)
	}
	requireSameRows(t, "restored", rowsText(t, ctx, s2, `SELECT id, v FROM t ORDER BY id`), []string{"1|one", "2|TWO", "3|three"})
}
