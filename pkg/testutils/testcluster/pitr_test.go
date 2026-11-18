package testcluster

import (
	"context"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/sthorne/datax/pkg/server"
	"github.com/sthorne/datax/pkg/sql"
	"github.com/sthorne/datax/pkg/sql/catalog"
	"github.com/sthorne/datax/pkg/util/hlc"
)

// pitrNow returns a moment between the statements before and after it:
// the clock is read, and then allowed to move on, so no write shares the
// moment's nanosecond.
func pitrNow(n *server.Node) hlc.Timestamp {
	time.Sleep(2 * time.Millisecond)
	ts := hlc.Timestamp{WallTime: n.Clock().Now().WallTime}
	time.Sleep(2 * time.Millisecond)
	return ts
}

// asOfRows is a query's rows as the source saw them at ts, through AS OF
// SYSTEM TIME.
func asOfRows(t *testing.T, ctx context.Context, s *sql.Session, table, cols string, ts hlc.Timestamp) []string {
	t.Helper()
	return rowsText(t, ctx, s, `SELECT `+cols+` FROM `+table+` AS OF SYSTEM TIME '`+strconv.FormatInt(ts.WallTime, 10)+`' ORDER BY id`)
}

func mustParseTS(t *testing.T, s string) hlc.Timestamp {
	t.Helper()
	ts, err := server.ParseBackupTS(s)
	if err != nil {
		t.Fatal(err)
	}
	return ts
}

// TestPointInTimeRestore (issue #302): a known history — inserts,
// updates, deletes, a transaction across two ranges — written across a
// full backup and two incrementals with revision history on a 3-node
// cluster, then restored to moments in each interval and at each
// boundary. At every moment the restored rows are what AS OF SYSTEM TIME
// returned on the source; the two-range transaction is there whole or
// not at all; and moments the chain does not cover are refused with the
// ones it does.
func TestPointInTimeRestore(t *testing.T) {
	tc := Start(t, 3)
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Second)
	defer cancel()
	n := tc.Nodes[0]
	s := sql.NewSession(n.DB(), catalog.NewAccessor())
	waitForDatabases(t, ctx, s)

	// Two tables, so two ranges at least: a transfer between them is a
	// transaction no single range commits.
	execSQL(t, ctx, s, `CREATE TABLE ledger (id INT8 PRIMARY KEY, who TEXT, balance INT8 NOT NULL)`)
	execSQL(t, ctx, s, `CREATE INDEX by_who ON ledger (who)`)
	execSQL(t, ctx, s, `CREATE TABLE vault (id INT8 PRIMARY KEY, balance INT8 NOT NULL)`)
	execSQL(t, ctx, s, `INSERT INTO ledger VALUES (1, 'ann', 100), (2, 'bob', 100), (3, 'cy', 100)`)
	execSQL(t, ctx, s, `INSERT INTO vault VALUES (1, 1000)`)
	const cols = "id, who, balance"

	type moment struct {
		what string
		ts   hlc.Timestamp
	}
	var moments []moment
	mark := func(what string) hlc.Timestamp {
		ts := pitrNow(n)
		moments = append(moments, moment{what, ts})
		return ts
	}
	backup := func(base string, history bool) (string, hlc.Timestamp) {
		t.Helper()
		dir := t.TempDir()
		sum, err := n.RunBackupWith(ctx, server.BackupOptions{Dest: dir, BasePath: base, RevisionHistory: history})
		if err != nil {
			t.Fatalf("backup onto %q: %v", base, err)
		}
		if sum.RevisionHistory != history {
			t.Fatalf("backup summary says revision history %v, asked for %v", sum.RevisionHistory, history)
		}
		return dir, mustParseTS(t, sum.EndTS)
	}

	beforeFull := pitrNow(n)
	dirFull, tsFull := backup("", false)
	// A boundary's reference is the table as it stands right after the
	// backup returns, with nothing written in between.
	atFull := rowsText(t, ctx, s, `SELECT `+cols+` FROM ledger ORDER BY id`)

	// The first interval.
	mark("before any change")
	execSQL(t, ctx, s, `INSERT INTO ledger VALUES (4, 'dee', 100)`)
	mark("after an insert")
	execSQL(t, ctx, s, `UPDATE ledger SET balance = 150, who = 'ann2' WHERE id = 1`)
	mark("after an update of an indexed column")
	execSQL(t, ctx, s, `UPDATE ledger SET balance = 175 WHERE id = 1`)
	execSQL(t, ctx, s, `DELETE FROM ledger WHERE id = 2`)
	mark("after a second update and a delete")
	dirInc1, tsInc1 := backup(dirFull, true)
	atInc1 := rowsText(t, ctx, s, `SELECT `+cols+` FROM ledger ORDER BY id`)

	// The second interval: a row deleted and inserted again, and the
	// transfer between the two tables.
	mark("just inside the second interval")
	execSQL(t, ctx, s, `INSERT INTO ledger VALUES (2, 'bob-again', 5)`)
	execSQL(t, ctx, s, `DELETE FROM ledger WHERE id = 3`)
	beforeTransfer := mark("before the transfer")
	execSQL(t, ctx, s, `BEGIN;
		UPDATE ledger SET balance = balance - 60 WHERE id = 4;
		UPDATE vault SET balance = balance + 60 WHERE id = 1;
		COMMIT`)
	afterTransfer := mark("after the transfer")
	execSQL(t, ctx, s, `INSERT INTO ledger VALUES (5, 'eve', 1)`)
	mark("after the last write")
	dirInc2, tsInc2 := backup(dirInc1, true)
	atInc2 := rowsText(t, ctx, s, `SELECT `+cols+` FROM ledger ORDER BY id`)
	afterLast := pitrNow(n)

	// Later writes are in no backup and no restore.
	execSQL(t, ctx, s, `DELETE FROM ledger`)
	chain := []string{dirFull, dirInc1, dirInc2}

	// Each moment restored into the cluster under a name of its own, and
	// compared with the source's own account of that moment.
	next := 0
	restoreAs := func(ts hlc.Timestamp, tables ...string) (map[string]string, error) {
		next++
		into := map[string]string{}
		var targets []server.RestoreTable
		for _, name := range tables {
			into[name] = name + "_p" + itoa(next)
			targets = append(targets, server.RestoreTable{Name: name, Into: into[name]})
		}
		_, err := tc.Nodes[next%3].RunRestoreOpts(ctx, server.RestoreOptions{Srcs: chain, AsOf: ts, Tables: targets})
		return into, err
	}
	for _, m := range moments {
		into, err := restoreAs(m.ts, "ledger")
		if err != nil {
			t.Fatalf("restore to the moment %s: %v", m.what, err)
		}
		want := asOfRows(t, ctx, s, "ledger", cols, m.ts)
		requireSameRows(t, "ledger restored to the moment "+m.what, rowsText(t, ctx, s, `SELECT `+cols+` FROM `+into["ledger"]+` ORDER BY id`), want)
		// The index is the index of that moment too.
		for _, who := range []string{"ann", "ann2", "bob", "bob-again"} {
			requireSameRows(t, "lookup of "+who+" at the moment "+m.what,
				rowsText(t, ctx, s, `SELECT id FROM `+into["ledger"]+` WHERE who = '`+who+`' ORDER BY id`),
				rowsText(t, ctx, s, `SELECT id FROM ledger AS OF SYSTEM TIME '`+strconv.FormatInt(m.ts.WallTime, 10)+`' WHERE who = '`+who+`' ORDER BY id`))
		}
	}
	// The history really differs from moment to moment: the test is not
	// comparing one state with itself nine times.
	if a, b := asOfRows(t, ctx, s, "ledger", cols, moments[0].ts), asOfRows(t, ctx, s, "ledger", cols, moments[len(moments)-1].ts); strings.Join(a, ";") == strings.Join(b, ";") {
		t.Fatalf("the first and last moments hold the same rows: %q", a)
	}

	// The boundaries, exactly: the end of each backup.
	for _, b := range []struct {
		what string
		ts   hlc.Timestamp
		want []string
	}{
		{"the full backup", tsFull, atFull},
		{"the first incremental", tsInc1, atInc1},
		{"the second incremental", tsInc2, atInc2},
	} {
		into, err := restoreAs(b.ts, "ledger")
		if err != nil {
			t.Fatalf("restore to exactly the end of %s: %v", b.what, err)
		}
		requireSameRows(t, "ledger restored to exactly the end of "+b.what, rowsText(t, ctx, s, `SELECT `+cols+` FROM `+into["ledger"]+` ORDER BY id`), b.want)
	}
	// No --as-of is the end of the chain.
	into, err := restoreAs(hlc.Timestamp{}, "ledger")
	if err != nil {
		t.Fatal(err)
	}
	requireSameRows(t, "ledger restored to the chain's end", rowsText(t, ctx, s, `SELECT `+cols+` FROM `+into["ledger"]+` ORDER BY id`), atInc2)

	// The transfer: at every moment around its commit both tables show
	// it or neither does. The commit lies somewhere between the two
	// marks; walk the gap.
	sawBefore, sawAfter := false, false
	const steps = 8
	for i := 0; i <= steps; i++ {
		ts := hlc.Timestamp{WallTime: beforeTransfer.WallTime + (afterTransfer.WallTime-beforeTransfer.WallTime)*int64(i)/steps}
		into, err := restoreAs(ts, "ledger", "vault")
		if err != nil {
			t.Fatalf("restore around the transfer: %v", err)
		}
		dee := execSQL(t, ctx, s, `SELECT balance FROM `+into["ledger"]+` WHERE id = 4`).Rows[0][0].I
		vault := execSQL(t, ctx, s, `SELECT balance FROM `+into["vault"]+` WHERE id = 1`).Rows[0][0].I
		switch {
		case dee == 100 && vault == 1000:
			sawBefore = true
			if sawAfter {
				t.Fatalf("the transfer is undone again at a later moment (%s)", ts)
			}
		case dee == 40 && vault == 1060:
			sawAfter = true
		default:
			t.Fatalf("half a transaction at %s: ledger has %d, vault %d", ts, dee, vault)
		}
		if srcDee := execSQL(t, ctx, s, `SELECT balance FROM ledger AS OF SYSTEM TIME '`+strconv.FormatInt(ts.WallTime, 10)+`' WHERE id = 4`).Rows[0][0].I; srcDee != dee {
			t.Fatalf("at %s the restore has %d and the source had %d", ts, dee, srcDee)
		}
	}
	if !sawBefore || !sawAfter {
		t.Fatalf("the walk did not straddle the transfer (before %v, after %v)", sawBefore, sawAfter)
	}

	// Moments the chain does not hold.
	for _, c := range []struct {
		what string
		ts   hlc.Timestamp
		want string
	}{
		{"before the full backup", beforeFull, "is earlier than the full backup"},
		{"after the last backup", afterLast, "is later than the chain's last backup"},
	} {
		_, err := restoreAs(c.ts, "ledger")
		if err == nil || !strings.Contains(err.Error(), c.want) || !strings.Contains(err.Error(), "This chain restores to: any time from") {
			t.Fatalf("restore to a moment %s: got %v, want a refusal saying %q and what is restorable", c.what, err, c.want)
		}
	}
	// A chain whose second backup was taken without history restores to
	// its ends and to nothing between them.
	execSQL(t, ctx, s, `INSERT INTO ledger VALUES (9, 'zed', 9)`)
	dirPlainFull, tsPlainFull := backup("", false)
	between := pitrNow(n)
	execSQL(t, ctx, s, `UPDATE ledger SET balance = 10 WHERE id = 9`)
	dirPlainInc, _ := backup(dirPlainFull, false)
	_, err = n.RunRestoreOpts(ctx, server.RestoreOptions{Srcs: []string{dirPlainFull, dirPlainInc}, AsOf: between,
		Tables: []server.RestoreTable{{Name: "ledger", Into: "ledger_between"}}})
	if err == nil || !strings.Contains(err.Error(), "was taken without --revision-history") {
		t.Fatalf("restore between two backups without history: %v", err)
	}
	if _, err = n.RunRestoreOpts(ctx, server.RestoreOptions{Srcs: []string{dirPlainFull, dirPlainInc}, AsOf: tsPlainFull,
		Tables: []server.RestoreTable{{Name: "ledger", Into: "ledger_at_full"}}}); err != nil {
		t.Fatalf("restore to the end of the full backup of a chain without history: %v", err)
	}
	requireSameRows(t, "the chain without history, at its full backup",
		rowsText(t, ctx, s, `SELECT `+cols+` FROM ledger_at_full ORDER BY id`), []string{"9|zed|9"})
	// A full backup is not taken with history.
	if _, err := n.RunBackupWith(ctx, server.BackupOptions{Dest: t.TempDir(), RevisionHistory: true}); err == nil || !strings.Contains(err.Error(), "a full backup has no base") {
		t.Fatalf("a full backup with revision history: %v", err)
	}

	// A restore of everything, into an empty cluster, to a moment inside
	// the second interval.
	mid := moments[5].ts // before the transfer
	tc2 := Start(t, 1)
	sum, err := tc2.Nodes[0].RunRestoreOpts(ctx, server.RestoreOptions{Srcs: chain, AsOf: mid})
	if err != nil {
		t.Fatalf("restore of everything to a moment: %v", err)
	}
	if mustParseTS(t, sum.EndTS) != mid {
		t.Fatalf("the restore reports it is as of %s, asked for %s", sum.EndTS, mid)
	}
	s2 := sql.NewSession(tc2.Nodes[0].DB(), catalog.NewAccessor())
	requireSameRows(t, "the cluster restored to a moment", rowsText(t, ctx, s2, `SELECT `+cols+` FROM ledger ORDER BY id`), asOfRows(t, ctx, s, "ledger", cols, mid))
	requireSameRows(t, "its other table", rowsText(t, ctx, s2, `SELECT id, balance FROM vault ORDER BY id`), []string{"1|1000"})
	requireSameRows(t, "a lookup through its index", rowsText(t, ctx, s2, `SELECT id FROM ledger WHERE who = 'bob-again'`), []string{"2"})
	// And the refusals are the same there.
	tc3 := Start(t, 1)
	if _, err := tc3.Nodes[0].RunRestoreOpts(ctx, server.RestoreOptions{Srcs: chain, AsOf: beforeFull}); err == nil || !strings.Contains(err.Error(), "is earlier than the full backup") {
		t.Fatalf("restore of everything to before the full backup: %v", err)
	}
	if got := rowsText(t, ctx, sql.NewSession(tc3.Nodes[0].DB(), catalog.NewAccessor()), `SELECT table_name FROM information_schema.tables WHERE table_name = 'ledger'`); len(got) != 0 {
		t.Fatalf("a refused restore created tables: %q", got)
	}
}

// TestPointInTimeRestoreSchemaChange: a backup holds a table's
// definition at its end only, so a restore to a moment inside an
// interval refuses a table whose definition changed in that interval —
// and still restores it to either end, and restores the tables that did
// not change.
func TestPointInTimeRestoreSchemaChange(t *testing.T) {
	tc := Start(t, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	n := tc.Nodes[0]
	s := sql.NewSession(n.DB(), catalog.NewAccessor())
	waitForDatabases(t, ctx, s)
	execSQL(t, ctx, s, `CREATE TABLE steady (id INT8 PRIMARY KEY, v TEXT)`)
	execSQL(t, ctx, s, `CREATE TABLE altered (id INT8 PRIMARY KEY, v TEXT)`)
	execSQL(t, ctx, s, `INSERT INTO steady VALUES (1, 'a')`)
	execSQL(t, ctx, s, `INSERT INTO altered VALUES (1, 'a')`)
	dirFull, dirInc := t.TempDir(), t.TempDir()
	if _, err := n.RunBackup(ctx, dirFull, "", false, false); err != nil {
		t.Fatal(err)
	}
	execSQL(t, ctx, s, `UPDATE steady SET v = 'b' WHERE id = 1`)
	mid := pitrNow(n)
	execSQL(t, ctx, s, `ALTER TABLE altered ADD COLUMN extra INT8 DEFAULT 7`)
	execSQL(t, ctx, s, `CREATE TABLE born (id INT8 PRIMARY KEY)`)
	execSQL(t, ctx, s, `UPDATE steady SET v = 'c' WHERE id = 1`)
	sum, err := n.RunBackupWith(ctx, server.BackupOptions{Dest: dirInc, BasePath: dirFull, RevisionHistory: true})
	if err != nil {
		t.Fatal(err)
	}
	chain := []string{dirFull, dirInc}
	restore := func(table string, ts hlc.Timestamp) error {
		_, err := n.RunRestoreOpts(ctx, server.RestoreOptions{Srcs: chain, AsOf: ts, Tables: []server.RestoreTable{{Name: table, Into: table + "_r"}}})
		return err
	}
	if err := restore("altered", mid); err == nil || !strings.Contains(err.Error(), "its definition changed between") {
		t.Fatalf("a table altered in the interval, restored to a moment inside it: %v", err)
	}
	if err := restore("born", mid); err == nil || !strings.Contains(err.Error(), "it was created between") {
		t.Fatalf("a table created in the interval, restored to a moment inside it: %v", err)
	}
	if err := restore("steady", mid); err != nil {
		t.Fatalf("a table that did not change: %v", err)
	}
	requireSameRows(t, "the steady table at the moment", rowsText(t, ctx, s, `SELECT id, v FROM steady_r`), []string{"1|b"})
	if err := restore("altered", mustParseTS(t, sum.EndTS)); err != nil {
		t.Fatalf("the altered table, restored to the interval's end: %v", err)
	}
	requireSameRows(t, "the altered table at the interval's end", rowsText(t, ctx, s, `SELECT id, v, extra FROM altered_r`), []string{"1|a|7"})
	// A restore of everything to the moment refuses for the same reason,
	// before it has applied anything.
	tc2 := Start(t, 1)
	if _, err := tc2.Nodes[0].RunRestoreOpts(ctx, server.RestoreOptions{Srcs: chain, AsOf: mid}); err == nil || !strings.Contains(err.Error(), "definition changed between") && !strings.Contains(err.Error(), "was created between") {
		t.Fatalf("a restore of everything to a moment a table changed around: %v", err)
	}
	if got := rowsText(t, ctx, sql.NewSession(tc2.Nodes[0].DB(), catalog.NewAccessor()), `SELECT table_name FROM information_schema.tables WHERE table_name = 'steady'`); len(got) != 0 {
		t.Fatalf("the refused restore created tables: %q", got)
	}
}

// TestSealedPointInTimeRestore (issues #291, #302): a sealed chain with
// revision history restores to a moment under its keys, and not without
// them.
func TestSealedPointInTimeRestore(t *testing.T) {
	tc := Start(t, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	n := tc.Nodes[0]
	s := sql.NewSession(n.DB(), catalog.NewAccessor())
	waitForDatabases(t, ctx, s)
	execSQL(t, ctx, s, `CREATE TABLE notes (id INT8 PRIMARY KEY, body TEXT)`)
	execSQL(t, ctx, s, `INSERT INTO notes VALUES (1, 'sealed-history-one'), (2, 'sealed-history-two')`)
	key := newBackupKey(t)
	dirFull, dirInc := t.TempDir(), t.TempDir()
	if _, err := n.RunBackupWith(ctx, server.BackupOptions{Dest: dirFull, Key: key}); err != nil {
		t.Fatal(err)
	}
	execSQL(t, ctx, s, `UPDATE notes SET body = 'sealed-history-three' WHERE id = 1`)
	mid := pitrNow(n)
	execSQL(t, ctx, s, `DELETE FROM notes WHERE id = 1`)
	execSQL(t, ctx, s, `UPDATE notes SET body = 'sealed-history-four' WHERE id = 2`)
	sum, err := n.RunBackupWith(ctx, server.BackupOptions{Dest: dirInc, BasePath: dirFull, Key: key, RevisionHistory: true})
	if err != nil {
		t.Fatal(err)
	}
	if !sum.Sealed || !sum.RevisionHistory {
		t.Fatalf("summary: %+v", sum)
	}
	// The versions between the two backups are in the file, and not in
	// the clear: 'three' was overwritten before the incremental was taken.
	if tbl := tableSummary(t, sum, "notes"); tbl.Versions <= tbl.Records {
		t.Fatalf("the history incremental holds %d versions of %d keys", tbl.Versions, tbl.Records)
	}
	for _, dir := range []string{dirFull, dirInc} {
		if hits := dirContains(t, dir, []string{"sealed-history", "notes"}); len(hits) != 0 {
			t.Fatalf("%s holds plaintext: %v", dir, hits)
		}
	}
	chain := []string{dirFull, dirInc}
	target := []server.RestoreTable{{Name: "notes", Into: "notes_r"}}
	for what, keys := range map[string][][]byte{"no key": nil, "another key": {newBackupKey(t)}} {
		if _, err := n.RunRestoreOpts(ctx, server.RestoreOptions{Srcs: chain, Keys: keys, AsOf: mid, Tables: target}); err == nil {
			t.Fatalf("a sealed chain was restored to a moment with %s", what)
		}
	}
	if _, serr := trySQL(ctx, s, `SELECT 1 FROM notes_r LIMIT 1`); serr == nil {
		t.Fatal("a refused restore left a table")
	}
	if _, err := n.RunRestoreOpts(ctx, server.RestoreOptions{Srcs: chain, Keys: [][]byte{key}, AsOf: mid, Tables: target}); err != nil {
		t.Fatalf("sealed restore to a moment: %v", err)
	}
	requireSameRows(t, "the sealed chain at the moment", rowsText(t, ctx, s, `SELECT id, body FROM notes_r ORDER BY id`),
		[]string{"1|sealed-history-three", "2|sealed-history-two"})
	requireSameRows(t, "and the source agrees", rowsText(t, ctx, s, `SELECT id, body FROM notes_r ORDER BY id`), asOfRows(t, ctx, s, "notes", "id, body", mid))

	// A restore of everything from the sealed chain, to the same moment.
	tc2 := Start(t, 1)
	if _, err := tc2.Nodes[0].RunRestoreOpts(ctx, server.RestoreOptions{Srcs: chain, Keys: [][]byte{key}, AsOf: mid}); err != nil {
		t.Fatal(err)
	}
	requireSameRows(t, "the cluster restored from the sealed chain",
		rowsText(t, ctx, sql.NewSession(tc2.Nodes[0].DB(), catalog.NewAccessor()), `SELECT id, body FROM notes ORDER BY id`),
		[]string{"1|sealed-history-three", "2|sealed-history-two"})
}

// TestRevisionHistoryLostToGC (issue #302): a backup can hold only the
// history the cluster still has. Once garbage collection has passed the
// base backup's timestamp, versions written since may be gone, and the
// incremental — with or without history — holds the table in full, as of
// its end, rather than the changes since the base (issue #376; before it
// the incremental was refused). Such a chain restores to its end and not
// to a moment inside the interval. While collection stays behind the
// base, the incremental holds every version.
func TestRevisionHistoryLostToGC(t *testing.T) {
	n, _ := startGCNode(t)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	s := sql.NewSession(n.DB(), catalog.NewAccessor())
	waitForDatabases(t, ctx, s)
	execSQL(t, ctx, s, `CREATE TABLE hist (id INT8 PRIMARY KEY, v INT8)`)
	execSQL(t, ctx, s, `INSERT INTO hist VALUES (1, 0)`)
	dirFull := t.TempDir()
	if _, err := n.RunBackup(ctx, dirFull, "", false, false); err != nil {
		t.Fatal(err)
	}
	for v := 1; v <= 5; v++ {
		execSQL(t, ctx, s, `UPDATE hist SET v = `+itoa(v)+` WHERE id = 1`)
	}
	mid := pitrNow(n)
	execSQL(t, ctx, s, `UPDATE hist SET v = 6 WHERE id = 1`)

	// Collection with a long TTL leaves the window alone: the
	// incremental holds all six versions, and restores to the middle.
	n.Store().RunGCOnce(ctx, 24*time.Hour)
	dirInc := t.TempDir()
	sum, err := n.RunBackupWith(ctx, server.BackupOptions{Dest: dirInc, BasePath: dirFull, RevisionHistory: true})
	if err != nil {
		t.Fatalf("incremental with history inside the GC window: %v", err)
	}
	if tbl := tableSummary(t, sum, "hist"); tbl.Versions != 6 || tbl.Records != 1 {
		t.Fatalf("the incremental holds %d versions of %d keys, want 6 of 1", tbl.Versions, tbl.Records)
	}
	if _, err := n.RunRestoreOpts(ctx, server.RestoreOptions{Srcs: []string{dirFull, dirInc}, AsOf: mid,
		Tables: []server.RestoreTable{{Name: "hist", Into: "hist_mid"}}}); err != nil {
		t.Fatal(err)
	}
	requireSameRows(t, "the row at the middle moment", rowsText(t, ctx, s, `SELECT v FROM hist_mid`), []string{"5"})

	// Now collection passes the full backup's timestamp and takes the
	// overwritten versions with it.
	time.Sleep(400 * time.Millisecond)
	n.Store().RunGCOnce(ctx, 200*time.Millisecond)
	for _, history := range []bool{true, false} {
		dir := t.TempDir()
		sum, err := n.RunBackupWith(ctx, server.BackupOptions{Dest: dir, BasePath: dirFull, RevisionHistory: history})
		if err != nil {
			t.Fatalf("incremental (history %v) onto a base the GC has passed: %v", history, err)
		}
		if tbl := tableSummary(t, sum, "hist"); !tbl.Full || tbl.Records != 1 {
			t.Fatalf("incremental (history %v) onto a base the GC has passed holds hist as %+v, want it in full", history, tbl)
		}
		into := "hist_end_" + strconv.FormatBool(history)
		if _, err := n.RunRestoreOpts(ctx, server.RestoreOptions{Srcs: []string{dirFull, dir},
			Tables: []server.RestoreTable{{Name: "hist", Into: into}}}); err != nil {
			t.Fatal(err)
		}
		requireSameRows(t, "the row at the chain's end", rowsText(t, ctx, s, `SELECT v FROM `+into), []string{"6"})
		if history {
			_, err := n.RunRestoreOpts(ctx, server.RestoreOptions{Srcs: []string{dirFull, dir}, AsOf: mid,
				Tables: []server.RestoreTable{{Name: "hist", Into: "hist_lost"}}})
			if err == nil || !strings.Contains(err.Error(), "holds it only as it stood then") {
				t.Fatalf("a restore to a moment whose history was collected: %v", err)
			}
		}
	}
	// The chain taken while the history was there still restores to the
	// middle: what a backup holds no longer depends on the cluster.
	if _, err := n.RunRestoreOpts(ctx, server.RestoreOptions{Srcs: []string{dirFull, dirInc}, AsOf: mid,
		Tables: []server.RestoreTable{{Name: "hist", Into: "hist_mid_again"}}}); err != nil {
		t.Fatal(err)
	}
	requireSameRows(t, "the row at the middle moment, after collection", rowsText(t, ctx, s, `SELECT v FROM hist_mid_again`), []string{"5"})
	// A new full backup starts a new chain.
	dirFull2 := t.TempDir()
	if _, err := n.RunBackup(ctx, dirFull2, "", false, false); err != nil {
		t.Fatal(err)
	}
	execSQL(t, ctx, s, `UPDATE hist SET v = 7 WHERE id = 1`)
	if _, err := n.RunBackupWith(ctx, server.BackupOptions{Dest: t.TempDir(), BasePath: dirFull2, RevisionHistory: true}); err != nil {
		t.Fatalf("incremental with history onto the new full backup: %v", err)
	}
}
