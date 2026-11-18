package testcluster

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"time"

	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/server"
	"github.com/sthorne/datax/pkg/sql"
	"github.com/sthorne/datax/pkg/sql/catalog"
	"github.com/sthorne/datax/pkg/storage"
	"github.com/sthorne/datax/pkg/util/hlc"
)

// Purge and backup (issue #376).

// blockedBackup starts a backup that stops once its timestamp is taken
// and protected, before it reads any table, until release is closed.
// The backup's result arrives on the returned channel.
func blockedBackup(t *testing.T, ctx context.Context, n *server.Node, opts server.BackupOptions) (release chan struct{}, result chan error) {
	t.Helper()
	reached := make(chan struct{})
	release = make(chan struct{})
	result = make(chan error, 1)
	opts.TestingBeforeExport = func() {
		close(reached)
		<-release
	}
	go func() {
		_, err := n.RunBackupWith(ctx, opts)
		result <- err
	}()
	select {
	case <-reached:
	case err := <-result:
		t.Fatalf("the backup ended before it read anything: %v", err)
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	return release, result
}

// waitEngineClear waits for no key or value of eng to hold marker.
func waitEngineClear(t *testing.T, eng *storage.Engine, marker string, within time.Duration) {
	t.Helper()
	for deadline := time.Now().Add(within); engineHolds(t, eng, []byte(marker)); {
		if time.Now().After(deadline) {
			t.Fatalf("the engine still holds %q", marker)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// TestPurgeDuringBackup: a purge that runs while a backup is reading
// does not cut the backup off. It removes every version deleted before
// the backup's timestamp at once, says that it was held back — a tag
// that is not an attestation, a notice, the backups in its report — and
// purges the rest by itself once the backup has ended. The backup
// succeeds and restores the table as it stood at its timestamp.
func TestPurgeDuringBackup(t *testing.T) {
	n, eng := startGCNodeCfg(t, func(c *server.Config) { c.PurgeBackupWait = -1 })
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	s := sql.NewSession(n.DB(), catalog.NewAccessor())
	waitForDatabases(t, ctx, s)
	const before, during, kept = "BEFORE-Hq3Lm8Zr", "DURING-Wc5Pn1Xs", "KEPT-Ty7Bv2Kd"
	// gc_ttl at creation gives the table its own ranges before any row
	// is written, so every write of the marker goes through their logs.
	execSQL(t, ctx, s, `CREATE TABLE acct (id INT8 PRIMARY KEY, secret TEXT) WITH (gc_ttl = '1h')`)
	for i := 1; i <= 15; i++ {
		marker := kept
		switch {
		case i <= 5:
			marker = before
		case i <= 10:
			marker = during
		}
		execSQL(t, ctx, s, `INSERT INTO acct VALUES ($1, $2)`, intD(i), strD(marker))
	}
	execSQL(t, ctx, s, `DELETE FROM acct WHERE id <= 5`)

	dir := t.TempDir()
	release, result := blockedBackup(t, ctx, n, server.BackupOptions{Dest: dir})
	execSQL(t, ctx, s, `DELETE FROM acct WHERE id <= 10`)
	res := execSQL(t, ctx, s, `ALTER TABLE acct PURGE`)
	if res.Tag != "PURGE INCOMPLETE" {
		t.Fatalf("a purge held back by a backup reads as %q", res.Tag)
	}
	if len(res.Notices) == 0 || !strings.Contains(res.Notices[0], "held back by 1 backup(s) in progress") ||
		!strings.Contains(res.Notices[0], "purged again automatically") {
		t.Fatalf("notices: %q", res.Notices)
	}
	// What was deleted before the backup began is gone; what was deleted
	// since is the backup's to read.
	if engineHolds(t, eng, []byte(before)) {
		t.Fatal("the rows deleted before the backup began survived the purge")
	}
	if !engineHolds(t, eng, []byte(during)) {
		t.Fatal("the purge collected the rows the backup reads")
	}
	start, end := keys.TableDataSpan(tableID(t, ctx, n, "acct"))
	rep, err := n.PurgeSpan(ctx, start, end)
	if err != nil {
		t.Fatal(err)
	}
	if rep.Complete || len(rep.HeldBy) != 1 || !strings.Contains(rep.HeldBy[0], dir) || !rep.Rerun || rep.HeldBelowNanos == 0 ||
		!strings.Contains(rep.Incomplete[0], "held back by 1 backup(s) in progress") {
		t.Fatalf("report of a held purge: %+v", rep)
	}

	close(release)
	if err := <-result; err != nil {
		t.Fatalf("the backup a purge ran during: %v", err)
	}
	// The purge finishes by itself once the backup has ended.
	waitEngineClear(t, eng, during, 30*time.Second)
	if !engineHolds(t, eng, []byte(kept)) {
		t.Fatal("the purge removed live rows")
	}

	target, _ := startGCNode(t)
	if _, err := target.RunRestore(ctx, []string{dir}); err != nil {
		t.Fatalf("restoring the backup a purge ran during: %v", err)
	}
	s2 := sql.NewSession(target.DB(), catalog.NewAccessor())
	requireSameRows(t, "restored as of the backup", rowsText(t, ctx, s2, `SELECT id, secret FROM acct ORDER BY id`), []string{
		"6|" + during, "7|" + during, "8|" + during, "9|" + during, "10|" + during,
		"11|" + kept, "12|" + kept, "13|" + kept, "14|" + kept, "15|" + kept,
	})
}

// TestPurgeWaitsForBackup: a purge first waits for the backups in
// progress, up to PurgeBackupWait; one that ends within it leaves the
// purge nothing to hold back for, and the purge is complete.
func TestPurgeWaitsForBackup(t *testing.T) {
	n, eng := startGCNodeCfg(t, func(c *server.Config) { c.PurgeBackupWait = 30 * time.Second })
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	s := sql.NewSession(n.DB(), catalog.NewAccessor())
	waitForDatabases(t, ctx, s)
	const gone = "GONE-Rk4Jd9Qa"
	// gc_ttl at creation gives the table its own ranges before any row
	// is written, so every write of the marker goes through their logs.
	execSQL(t, ctx, s, `CREATE TABLE acct (id INT8 PRIMARY KEY, secret TEXT) WITH (gc_ttl = '1h')`)
	execSQL(t, ctx, s, `INSERT INTO acct VALUES (1, $1), (2, 'stays')`, strD(gone))

	dir := t.TempDir()
	release, result := blockedBackup(t, ctx, n, server.BackupOptions{Dest: dir})
	execSQL(t, ctx, s, `DELETE FROM acct WHERE id = 1`)
	start, end := keys.TableDataSpan(tableID(t, ctx, n, "acct"))
	type purged struct {
		complete bool
		heldBy   []string
		err      error
	}
	done := make(chan purged, 1)
	go func() {
		rep, err := n.PurgeSpan(ctx, start, end)
		if err != nil {
			done <- purged{err: err}
			return
		}
		done <- purged{complete: rep.Complete, heldBy: rep.HeldBy}
	}()
	select {
	case p := <-done:
		t.Fatalf("the purge did not wait for the backup: %+v", p)
	case <-time.After(500 * time.Millisecond):
	}
	close(release)
	if err := <-result; err != nil {
		t.Fatalf("the backup: %v", err)
	}
	p := <-done
	if p.err != nil || !p.complete || len(p.heldBy) != 0 {
		t.Fatalf("the purge that waited for the backup: %+v", p)
	}
	waitEngineClear(t, eng, gone, time.Second)
	// The backup holds the row: it was deleted after the backup's time.
	target, _ := startGCNode(t)
	if _, err := target.RunRestore(ctx, []string{dir}); err != nil {
		t.Fatal(err)
	}
	requireSameRows(t, "restored", rowsText(t, ctx, sql.NewSession(target.DB(), catalog.NewAccessor()), `SELECT id, secret FROM acct ORDER BY id`),
		[]string{"1|" + gone, "2|stays"})
}

// TestIncrementalAfterPurge: an incremental whose base a purge of a table
// has since passed holds that table in full, as of its end, and the rest
// as changes; it no longer fails. The chain restores to the cluster's
// data, and the rows the purge removed — which the full backup still
// holds — do not come back. The same when the purge runs during the
// incremental, with or without revision history, sealed or not.
func TestIncrementalAfterPurge(t *testing.T) {
	n, _ := startGCNodeCfg(t, func(c *server.Config) { c.PurgeBackupWait = -1 })
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()
	s := sql.NewSession(n.DB(), catalog.NewAccessor())
	waitForDatabases(t, ctx, s)
	execSQL(t, ctx, s, `CREATE TABLE acct (id INT8 PRIMARY KEY, v TEXT)`)
	execSQL(t, ctx, s, `CREATE TABLE other (id INT8 PRIMARY KEY, v TEXT)`)
	for i := 1; i <= 10; i++ {
		execSQL(t, ctx, s, `INSERT INTO acct VALUES ($1, 'a')`, intD(i))
		execSQL(t, ctx, s, `INSERT INTO other VALUES ($1, 'o')`, intD(i))
	}
	key := bytes.Repeat([]byte{7}, 32)
	full, fullSealed := t.TempDir(), t.TempDir()
	if _, err := n.RunBackup(ctx, full, "", false, false); err != nil {
		t.Fatal(err)
	}
	if _, err := n.RunBackupWith(ctx, server.BackupOptions{Dest: fullSealed, Key: key}); err != nil {
		t.Fatal(err)
	}
	execSQL(t, ctx, s, `DELETE FROM acct WHERE id <= 5`)
	execSQL(t, ctx, s, `UPDATE acct SET v = 'b' WHERE id = 6`)
	execSQL(t, ctx, s, `INSERT INTO acct VALUES (11, 'a')`)
	execSQL(t, ctx, s, `DELETE FROM other WHERE id <= 2`)
	execSQL(t, ctx, s, `UPDATE other SET v = 'p' WHERE id = 3`)
	mid := pitrNow(n)
	execSQL(t, ctx, s, `UPDATE other SET v = 'q' WHERE id = 4`)
	if res := execSQL(t, ctx, s, `ALTER TABLE acct PURGE`); res.Tag == "PURGE INCOMPLETE" {
		t.Fatalf("purge with no backup in progress: %s", purgeRows(t, res))
	}
	wantAcct := rowsText(t, ctx, s, `SELECT id, v FROM acct ORDER BY id`)
	wantOther := rowsText(t, ctx, s, `SELECT id, v FROM other ORDER BY id`)

	restoreAndCheck := func(what string, srcs []string, keys [][]byte) {
		t.Helper()
		target, _ := startGCNode(t)
		if _, err := target.RunRestoreWith(ctx, srcs, keys); err != nil {
			t.Fatalf("%s: restoring the chain: %v", what, err)
		}
		s2 := sql.NewSession(target.DB(), catalog.NewAccessor())
		requireSameRows(t, what+": acct", rowsText(t, ctx, s2, `SELECT id, v FROM acct ORDER BY id`), wantAcct)
		requireSameRows(t, what+": other", rowsText(t, ctx, s2, `SELECT id, v FROM other ORDER BY id`), wantOther)
	}

	for _, c := range []struct {
		what    string
		base    string
		key     []byte
		history bool
		keys    [][]byte
	}{
		{"plain", full, nil, false, nil},
		{"sealed", fullSealed, key, false, [][]byte{key}},
		{"with revision history", full, nil, true, nil},
	} {
		dir := t.TempDir()
		sum, err := n.RunBackupWith(ctx, server.BackupOptions{Dest: dir, BasePath: c.base, Key: c.key, RevisionHistory: c.history})
		if err != nil {
			t.Fatalf("%s: an incremental after a purge: %v", c.what, err)
		}
		if !tableSummary(t, sum, "acct").Full || tableSummary(t, sum, "other").Full {
			t.Fatalf("%s: acct full %v, other full %v; want the purged table alone in full", c.what, tableSummary(t, sum, "acct").Full, tableSummary(t, sum, "other").Full)
		}
		if len(sum.Notes) != 1 || !strings.Contains(sum.Notes[0], "table acct is held in full") {
			t.Fatalf("%s: notes %q", c.what, sum.Notes)
		}
		restoreAndCheck(c.what, []string{c.base, dir}, c.keys)
		if c.history {
			// Inside the interval, the table held in full has no history
			// to restore from; the others do.
			target, _ := startGCNode(t)
			_, err := target.RunRestoreOpts(ctx, server.RestoreOptions{Srcs: []string{c.base, dir}, AsOf: mid})
			if err == nil || !strings.Contains(err.Error(), "cannot restore table acct") || !strings.Contains(err.Error(), "holds it only as it stood then") {
				t.Fatalf("restoring a table held in full to a moment inside the interval: %v", err)
			}
			if _, err := n.RunRestoreOpts(ctx, server.RestoreOptions{Srcs: []string{c.base, dir}, AsOf: mid,
				Tables: []server.RestoreTable{{Name: "other", Into: "other_mid"}}}); err != nil {
				t.Fatal(err)
			}
			requireSameRows(t, "other at the middle moment", rowsText(t, ctx, s, `SELECT id, v FROM other_mid ORDER BY id`),
				[]string{"3|p", "4|o", "5|o", "6|o", "7|o", "8|o", "9|o", "10|o"})
			execSQL(t, ctx, s, `DROP TABLE other_mid`)
		}
	}

	// A purge during an incremental: the backup protects its own
	// timestamp and not its base, so the purge collects past the base, and
	// the table the incremental has yet to read is held in full.
	inc1 := t.TempDir()
	if _, err := n.RunBackup(ctx, inc1, full, false, false); err != nil {
		t.Fatal(err)
	}
	execSQL(t, ctx, s, `DELETE FROM acct WHERE id = 6`)
	execSQL(t, ctx, s, `INSERT INTO other VALUES (20, 'o')`)
	inc2 := t.TempDir()
	var held []string
	sum, err := n.RunBackupWith(ctx, server.BackupOptions{Dest: inc2, BasePath: inc1, TestingBeforeExport: func() {
		start, end := keys.TableDataSpan(tableID(t, ctx, n, "acct"))
		rep, err := n.PurgeSpan(ctx, start, end)
		if err != nil {
			t.Error(err)
			return
		}
		held = rep.HeldBy
	}})
	if err != nil {
		t.Fatalf("an incremental a purge ran during: %v", err)
	}
	if len(held) != 1 || !tableSummary(t, sum, "acct").Full || tableSummary(t, sum, "other").Full {
		t.Fatalf("held by %q; acct full %v, other full %v", held, tableSummary(t, sum, "acct").Full, tableSummary(t, sum, "other").Full)
	}
	wantAcct = rowsText(t, ctx, s, `SELECT id, v FROM acct ORDER BY id`)
	wantOther = rowsText(t, ctx, s, `SELECT id, v FROM other ORDER BY id`)
	restoreAndCheck("a purge during the incremental", []string{full, inc1, inc2}, nil)
	// The chain before it still restores to its own end.
	target, _ := startGCNode(t)
	if _, err := target.RunRestore(ctx, []string{full}); err != nil {
		t.Fatal(err)
	}
	if got := rowsText(t, ctx, sql.NewSession(target.DB(), catalog.NewAccessor()), `SELECT count(*) FROM acct`); len(got) != 1 || got[0] != "10" {
		t.Fatalf("the full backup taken before the purge restores %v rows of acct, want 10", got)
	}
}

// TestBackupProtectionLapses: a backup whose node died leaves its
// protection record behind, unrenewed. It holds purges and garbage
// collection back only until its lease runs out; after that a purge is
// complete and collection goes past it.
func TestBackupProtectionLapses(t *testing.T) {
	const ttl = 5 * time.Second
	n, eng := startGCNodeCfg(t, func(c *server.Config) {
		c.PurgeBackupWait = -1
		c.BackupProtectionTTL = ttl
	})
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	s := sql.NewSession(n.DB(), catalog.NewAccessor())
	waitForDatabases(t, ctx, s)
	const gone = "GONE-Mx2Vb7Lp"
	// gc_ttl at creation gives the table its own ranges before any row
	// is written, so every write of the marker goes through their logs.
	execSQL(t, ctx, s, `CREATE TABLE acct (id INT8 PRIMARY KEY, secret TEXT) WITH (gc_ttl = '1h')`)
	execSQL(t, ctx, s, `CREATE TABLE gc (id INT8 PRIMARY KEY, v INT8)`)
	execSQL(t, ctx, s, `INSERT INTO acct VALUES (1, 'stays')`)
	execSQL(t, ctx, s, `INSERT INTO gc VALUES (1, 0)`)
	protected, err := n.TestingAbandonBackupProtection(ctx, "/backups/crashed")
	if err != nil {
		t.Fatal(err)
	}
	abandonedAt := time.Now()
	execSQL(t, ctx, s, `INSERT INTO acct VALUES (2, $1)`, strD(gone))
	execSQL(t, ctx, s, `DELETE FROM acct WHERE id = 2`)
	execSQL(t, ctx, s, `UPDATE gc SET v = 1 WHERE id = 1`)
	start, end := keys.TableDataSpan(tableID(t, ctx, n, "acct"))
	gstart, gend := keys.TableDataSpan(tableID(t, ctx, n, "gc"))

	// Held while the record is live: garbage collection does not pass
	// the protected timestamp, and a purge stops below it.
	time.Sleep(50 * time.Millisecond)
	n.Store().RunGCOnce(ctx, time.Millisecond)
	if _, err := n.DB().NewHistoricalTxn("protected", protected).Scan(ctx, gstart, gend, 0); err != nil {
		t.Fatalf("garbage collection passed a live backup protection: %v", err)
	}
	rep, err := n.PurgeSpan(ctx, start, end)
	if err != nil {
		t.Fatal(err)
	}
	if time.Since(abandonedAt) >= ttl {
		t.Skip("the machine is too slow to see the record live")
	}
	if rep.Complete || len(rep.HeldBy) != 1 || !strings.Contains(rep.HeldBy[0], "/backups/crashed") {
		t.Fatalf("a purge under a live protection: %+v", rep)
	}
	if !engineHolds(t, eng, []byte(gone)) {
		t.Fatal("the purge collected through a live protection")
	}

	// The lease runs out: the purge (the rerun, or one asked for now) is
	// complete, and collection is free to pass the old timestamp.
	time.Sleep(time.Until(abandonedAt.Add(ttl + 200*time.Millisecond)))
	rep, err = n.PurgeSpan(ctx, start, end)
	if err != nil {
		t.Fatal(err)
	}
	if !rep.Complete || len(rep.HeldBy) != 0 {
		t.Fatalf("a purge after the protection lapsed: %+v", rep)
	}
	waitEngineClear(t, eng, gone, time.Second)
	if _, err := n.DB().NewHistoricalTxn("lapsed", protected).Scan(ctx, start, end, 0); err == nil {
		t.Fatal("a read at the lapsed protection's timestamp was served after the purge")
	}
	n.Store().RunGCOnce(ctx, time.Millisecond)
	if _, err := n.DB().NewHistoricalTxn("lapsed", hlc.Timestamp{WallTime: protected.WallTime + 1}).Scan(ctx, gstart, gend, 0); err == nil {
		t.Fatal("garbage collection is still held by a lapsed protection")
	}
}
