package testcluster

import (
	"context"
	"testing"
	"time"

	"github.com/sthorne/datax/pkg/sql"
	"github.com/sthorne/datax/pkg/sql/catalog"
)

// TestRetentionRowsComeBackFromAnIncrementalChain (issue #313) pins
// down, as documented in docs/user/operations.md, what a time-series
// retention does to a backup chain. Retention expiry collects rows
// beneath SQL and leaves no tombstone, so an incremental taken after
// rows expired has nothing to say about them, and a restore of the full
// backup and the incremental brings them back. They are rewritten at
// the restore's time, and retention counts from a version's commit
// time, so they would stay a retention period longer; the documented
// remedy, a DELETE of the rows older than the retention, removes
// exactly them. If restore ever re-applies retention itself, this test
// fails and the documentation is to be changed with it.
func TestRetentionRowsComeBackFromAnIncrementalChain(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	tc := Start(t, 1)
	n := tc.Nodes[0]
	s := sql.NewSession(n.DB(), catalog.NewAccessor())
	waitForDatabases(t, ctx, s)
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
	execSQL(t, ctx, s, `INSERT INTO m VALUES ('mem', now(), 3)`)
	inc := t.TempDir()
	if _, err := n.RunBackup(ctx, inc, full, false, false); err != nil {
		t.Fatal(err)
	}

	tc2 := Start(t, 1)
	s2 := sql.NewSession(tc2.Nodes[0].DB(), catalog.NewAccessor())
	waitForDatabases(t, ctx, s2)
	if _, err := tc2.Nodes[0].RunRestore(ctx, []string{full, inc}); err != nil {
		t.Fatal(err)
	}
	if got := execSQL(t, ctx, s2, `SELECT count(*) FROM m`).Rows[0][0].I; got != 3 {
		t.Fatalf("restored %d rows; the two expired rows and the new one were expected (see the documentation)", got)
	}
	execSQL(t, ctx, s2, `DELETE FROM m WHERE at < now() - INTERVAL '3 seconds'`)
	requireSameRows(t, "after the documented DELETE", rowsText(t, ctx, s2, `SELECT series, v FROM m`), []string{"mem|3"})
}
