package testcluster

import (
	"context"
	"crypto/rand"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/sthorne/datax/pkg/cluster"
	"github.com/sthorne/datax/pkg/server"
	"github.com/sthorne/datax/pkg/sql"
	"github.com/sthorne/datax/pkg/sql/catalog"
	"github.com/sthorne/datax/pkg/util/hlc"
	"github.com/sthorne/datax/pkg/version"
)

// requireNote fails unless one of a restore summary's notes holds want.
func requireNote(t *testing.T, sum *cluster.BackupSummary, want string) {
	t.Helper()
	for _, n := range sum.Notes {
		if strings.Contains(n, want) {
			return
		}
	}
	t.Fatalf("no note saying %q in %q", want, sum.Notes)
}

// TestRetentionRowsStayExpiredAcrossAnIncrementalChain (issues #313,
// #316): retention expires rows beneath SQL and leaves no tombstone, so
// an incremental taken after rows expired has nothing to say about them.
// The backups keep each time-series row's commit time, and a restore of
// the full backup and the incremental leaves out the rows whose commit
// time the retention had passed by the incremental's time — and keeps a
// row backfilled with an old timestamp shortly before it, which the
// source held, because retention goes by commit time and not by the
// timestamp column. A table restore (#301) of the same chain into the
// source cluster does the same, and so does a restore to a point in time
// (#302) inside an incremental with revision history, whose versions
// carry their commit times already.
func TestRetentionRowsStayExpiredAcrossAnIncrementalChain(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	tc := Start(t, 1)
	n := tc.Nodes[0]
	s := sql.NewSession(n.DB(), catalog.NewAccessor())
	waitForDatabases(t, ctx, s)
	tc2 := Start(t, 1)
	s2 := sql.NewSession(tc2.Nodes[0].DB(), catalog.NewAccessor())
	waitForDatabases(t, ctx, s2)

	execSQL(t, ctx, s, `CREATE TABLE m (series TEXT, at TIMESTAMPTZ, v FLOAT8, PRIMARY KEY (series, at)) WITH (timeseries = true, retention = '3s')`)
	execSQL(t, ctx, s, `INSERT INTO m VALUES ('cpu', now(), 1), ('disk', now(), 2)`)
	// The full backup two seconds after the rows were written, the
	// expiry a second and a half after it: the rows are past the
	// retention, and the full backup is not, so an incremental on it is
	// still taken (one whose base the retention has passed is refused).
	time.Sleep(2 * time.Second)
	full := t.TempDir()
	if _, err := n.RunBackup(ctx, full, "", false, false); err != nil {
		t.Fatal(err)
	}
	time.Sleep(1500 * time.Millisecond)
	n.TestingRefreshRetention()
	n.Store().RunGCOnce(ctx, 24*time.Hour)
	if got := execSQL(t, ctx, s, `SELECT count(*) FROM m`).Rows[0][0].I; got != 0 {
		t.Fatalf("retention left %d rows in the source", got)
	}
	// A new row, and one backfilled with a day-old timestamp: both
	// committed now, both held by the source at the incremental.
	execSQL(t, ctx, s, `INSERT INTO m VALUES ('mem', now(), 3), ('old', now() - INTERVAL '1 day', 4)`)
	inc := t.TempDir()
	if _, err := n.RunBackup(ctx, inc, full, false, false); err != nil {
		t.Fatal(err)
	}
	hist := t.TempDir()
	hsum, err := n.RunBackupWith(ctx, server.BackupOptions{Dest: hist, BasePath: full, RevisionHistory: true})
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"mem|3", "old|4"}
	requireSameRows(t, "the source at the incremental", rowsText(t, ctx, s, `SELECT series, v FROM m`), want)

	sum, err := tc2.Nodes[0].RunRestore(ctx, []string{full, inc})
	if err != nil {
		t.Fatal(err)
	}
	requireSameRows(t, "restored from the chain", rowsText(t, ctx, s2, `SELECT series, v FROM m`), want)
	requireNote(t, sum, "2 row(s) committed at or before")

	sum, err = n.RunRestoreOpts(ctx, server.RestoreOptions{Srcs: []string{full, inc}, Tables: []server.RestoreTable{{Name: "m", Into: "m_restored"}}})
	if err != nil {
		t.Fatal(err)
	}
	requireSameRows(t, "table restore of the chain", rowsText(t, ctx, s, `SELECT series, v FROM m_restored`), want)
	requireNote(t, sum, "2 row(s) committed at or before")

	asOf := hlc.Timestamp{WallTime: hsum.EndTSNanos - 1}
	sum, err = n.RunRestoreOpts(ctx, server.RestoreOptions{Srcs: []string{full, hist}, AsOf: asOf, Tables: []server.RestoreTable{{Name: "m", Into: "m_pitr"}}})
	if err != nil {
		t.Fatal(err)
	}
	requireSameRows(t, "restore to a point in time", rowsText(t, ctx, s, `SELECT series, v FROM m_pitr`), want)
	requireNote(t, sum, "2 row(s) committed at or before")
}

// TestRetentionRestoredRowsExpireOnTheSourceSchedule (issue #316): a
// restore writes every row at its own time, and retention counts from a
// version's commit time; the restore records when its rows were first
// written, and retention expires each restored row when it would have
// expired at the source rather than a retention period after the
// restore — a row backfilled with an old timestamp included, which goes
// by its commit time like every other. The backup is sealed (#291): the
// commit times are inside its authenticated files, and a damaged file is
// refused before anything is restored.
func TestRetentionRestoredRowsExpireOnTheSourceSchedule(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	tc := Start(t, 1)
	n := tc.Nodes[0]
	s := sql.NewSession(n.DB(), catalog.NewAccessor())
	waitForDatabases(t, ctx, s)
	tc2 := Start(t, 1)
	n2 := tc2.Nodes[0]
	s2 := sql.NewSession(n2.DB(), catalog.NewAccessor())
	waitForDatabases(t, ctx, s2)
	key := make([]byte, 32)
	if _, err := rand.Read(key); err != nil {
		t.Fatal(err)
	}

	execSQL(t, ctx, s, `CREATE TABLE m (series TEXT, at TIMESTAMPTZ, v FLOAT8, PRIMARY KEY (series, at)) WITH (timeseries = true, retention = '5s')`)
	execSQL(t, ctx, s, `INSERT INTO m VALUES ('early', now(), 1)`)
	early := time.Now()
	time.Sleep(3 * time.Second)
	execSQL(t, ctx, s, `INSERT INTO m VALUES ('late', now(), 2), ('backfill', now() - INTERVAL '1 day', 3)`)
	dir := t.TempDir()
	bsum, err := n.RunBackupWith(ctx, server.BackupOptions{Dest: dir, Key: key})
	if err != nil {
		t.Fatal(err)
	}

	// A damaged copy of the time-series table's sealed file is refused.
	var file string
	for _, tb := range bsum.Tables {
		if tb.Name == "m" {
			file = fmt.Sprintf("table_%d.dxbk", tb.ID)
		}
	}
	if file == "" {
		t.Fatalf("no table m in the backup: %+v", bsum.Tables)
	}
	damaged := t.TempDir()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		raw, err := os.ReadFile(filepath.Join(dir, e.Name()))
		if err != nil {
			t.Fatal(err)
		}
		if e.Name() == file {
			raw[len(raw)/2] ^= 0x01
		}
		if err := os.WriteFile(filepath.Join(damaged, e.Name()), raw, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	// (Refused before anything is applied, or the restore below would
	// find the cluster no longer empty.)
	if _, err := n2.RunRestoreWith(ctx, []string{damaged}, [][]byte{key}); err == nil {
		t.Fatal("a sealed backup with a damaged data file restored")
	}

	sum, err := n2.RunRestoreWith(ctx, []string{dir}, [][]byte{key})
	if err != nil {
		t.Fatal(err)
	}
	requireNote(t, sum, "expire on the source's schedule")
	requireSameRows(t, "restored", rowsText(t, ctx, s2, `SELECT series, v FROM m`), []string{"backfill|3", "early|1", "late|2"})

	// Past the early row's retention, inside the others': the restored
	// early row goes on the source's schedule; restored at its own time
	// it would stay five seconds after the restore.
	if wait := time.Until(early.Add(5*time.Second + 300*time.Millisecond)); wait > 0 {
		time.Sleep(wait)
	}
	n2.TestingRefreshRetention()
	n2.Store().RunGCOnce(ctx, 24*time.Hour)
	requireSameRows(t, "after the early row's retention", rowsText(t, ctx, s2, `SELECT series, v FROM m`), []string{"backfill|3", "late|2"})
}

// TestRetentionOldFormatBackupRestoresAsBefore (issue #316): a backup
// taken before cluster version v22 holds no commit times; a restore of
// it brings the time-series rows back as it always has, and its summary
// says the table's retention was not applied.
func TestRetentionOldFormatBackupRestoresAsBefore(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	tc := StartWithOptions(t, 1, func(c *server.Config) { c.BinaryVersionOverride = version.V21 })
	n := tc.Nodes[0]
	s := sql.NewSession(n.DB(), catalog.NewAccessor())
	waitForDatabases(t, ctx, s)
	execSQL(t, ctx, s, `CREATE TABLE m (series TEXT, at TIMESTAMPTZ, v FLOAT8, PRIMARY KEY (series, at)) WITH (timeseries = true, retention = '1h')`)
	execSQL(t, ctx, s, `INSERT INTO m VALUES ('cpu', now(), 1)`)
	dir := t.TempDir()
	if _, err := n.RunBackup(ctx, dir, "", false, false); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(filepath.Join(dir, "BACKUP.json"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(raw), `"DXBK1"`) || strings.Contains(string(raw), "commit_times") {
		t.Fatal("a backup at v21 was written with commit times")
	}

	tc2 := Start(t, 1)
	s2 := sql.NewSession(tc2.Nodes[0].DB(), catalog.NewAccessor())
	waitForDatabases(t, ctx, s2)
	sum, err := tc2.Nodes[0].RunRestore(ctx, []string{dir})
	if err != nil {
		t.Fatal(err)
	}
	requireSameRows(t, "restored", rowsText(t, ctx, s2, `SELECT series, v FROM m`), []string{"cpu|1"})
	requireNote(t, sum, "holds no commit times")
}
