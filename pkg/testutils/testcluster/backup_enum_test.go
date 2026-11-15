package testcluster

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/sthorne/datax/pkg/server"
	"github.com/sthorne/datax/pkg/sql"
	"github.com/sthorne/datax/pkg/sql/catalog"
)

// TestBackupRestoreEnumTypes (issue #313): an enum type is part of a
// backup. A restore of everything brings the CREATE TYPE back with its
// id, so the restored columns are still of it (ALTER TYPE ... ADD VALUE
// reaches them) and a type created afterwards takes a fresh id; a table
// restore into a cluster without the type creates it, and one into a
// database where a type of that name has other labels is refused before
// anything is written.
func TestBackupRestoreEnumTypes(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()
	tc := Start(t, 1)
	s := sql.NewSession(tc.Nodes[0].DB(), catalog.NewAccessor())
	waitForDatabases(t, ctx, s)
	execSQL(t, ctx, s, `CREATE TYPE mood AS ENUM ('sad', 'ok', 'happy')`)
	execSQL(t, ctx, s, `CREATE TABLE feelings (id INT8 PRIMARY KEY, m mood NOT NULL)`)
	execSQL(t, ctx, s, `INSERT INTO feelings VALUES (1, 'happy'), (2, 'sad'), (3, 'ok')`)
	// The type is the newest descriptor id: a restore that did not move
	// the id generator past it would hand the id out again.
	execSQL(t, ctx, s, `CREATE TYPE size AS ENUM ('s', 'm', 'l')`)
	dir := t.TempDir()
	if _, err := tc.Nodes[0].RunBackup(ctx, dir, "", false, false); err != nil {
		t.Fatal(err)
	}
	want := []string{"2|sad", "3|ok", "1|happy"}

	// A restore of everything.
	tc2 := Start(t, 1)
	s2 := sql.NewSession(tc2.Nodes[0].DB(), catalog.NewAccessor())
	waitForDatabases(t, ctx, s2)
	if _, err := tc2.Nodes[0].RunRestore(ctx, []string{dir}); err != nil {
		t.Fatalf("restore: %v", err)
	}
	requireSameRows(t, "restored enum rows", rowsText(t, ctx, s2, `SELECT id, m FROM feelings ORDER BY m`), want)
	if got := rowsText(t, ctx, s2, `SELECT 'happy'::mood, 'l'::size`); len(got) != 1 || got[0] != "happy|l" {
		t.Fatalf("restored types: %q", got)
	}
	execSQL(t, ctx, s2, `CREATE TYPE later AS ENUM ('x')`)
	execSQL(t, ctx, s2, `ALTER TYPE mood ADD VALUE 'ecstatic'`)
	execSQL(t, ctx, s2, `INSERT INTO feelings VALUES (4, 'ecstatic')`)
	requireSameRows(t, "after ADD VALUE on the restored type",
		rowsText(t, ctx, s2, `SELECT id, m, 'l'::size, 'x'::later FROM feelings ORDER BY m DESC LIMIT 1`), []string{"4|ecstatic|l|x"})

	// A table restore into a running cluster that has no such type.
	tc3 := Start(t, 1)
	s3 := sql.NewSession(tc3.Nodes[0].DB(), catalog.NewAccessor())
	waitForDatabases(t, ctx, s3)
	if _, err := tc3.Nodes[0].RunRestoreOpts(ctx, server.RestoreOptions{Srcs: []string{dir}, Tables: []server.RestoreTable{{Name: "feelings"}}}); err != nil {
		t.Fatalf("table restore without the type: %v", err)
	}
	requireSameRows(t, "table restore created the type", rowsText(t, ctx, s3, `SELECT id, m FROM feelings ORDER BY m`), want)
	if got := rowsText(t, ctx, s3, `SELECT 'ok'::mood`); len(got) != 1 || got[0] != "ok" {
		t.Fatalf("type after table restore: %q", got)
	}
	execSQL(t, ctx, s3, `ALTER TYPE mood ADD VALUE 'ecstatic'`)
	execSQL(t, ctx, s3, `INSERT INTO feelings VALUES (4, 'ecstatic')`)
	// Again under another name: the type is there now, labels changed —
	// refused, by name, and nothing is left behind.
	_, err := tc3.Nodes[0].RunRestoreOpts(ctx, server.RestoreOptions{Srcs: []string{dir}, Tables: []server.RestoreTable{{Name: "feelings", Into: "feelings2"}}})
	if err == nil || !strings.Contains(err.Error(), `enum type "mood"`) || !strings.Contains(err.Error(), "labels") {
		t.Fatalf("table restore over a type with other labels: %v", err)
	}
	if _, serr := trySQL(ctx, s3, `SELECT 1 FROM feelings2`); serr == nil {
		t.Fatal("a refused restore left a table")
	}
	// Into the source cluster beside the original: the very type is used.
	if _, err := tc.Nodes[0].RunRestoreOpts(ctx, server.RestoreOptions{Srcs: []string{dir}, Tables: []server.RestoreTable{{Name: "feelings", Into: "feelings_r"}}}); err != nil {
		t.Fatalf("table restore beside the original: %v", err)
	}
	execSQL(t, ctx, s, `ALTER TYPE mood ADD VALUE 'ecstatic'`)
	execSQL(t, ctx, s, `INSERT INTO feelings_r VALUES (4, 'ecstatic')`)
	// And into a new database: the type is made there too.
	if _, err := tc.Nodes[0].RunRestoreOpts(ctx, server.RestoreOptions{Srcs: []string{dir}, Database: "datax", IntoDatabase: "copy"}); err != nil {
		t.Fatalf("database restore: %v", err)
	}
	requireSameRows(t, "database restore", rowsText(t, ctx, s, `SELECT id, m FROM copy.feelings ORDER BY m`), want)
}
