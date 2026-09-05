package testcluster

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/sthorne/datax/pkg/kvserver"
	"github.com/sthorne/datax/pkg/sql"
	"github.com/sthorne/datax/pkg/sql/types"
)

// countRanges is the number of replicas the node's store holds.
func countRanges(n interface {
	Store() *kvserver.Store
}) int {
	c := 0
	n.Store().VisitReplicas(func(*kvserver.Replica) bool { c++; return true })
	return c
}

// TestSplitAt: ALTER TABLE ... SPLIT AT VALUES carves ranges at primary
// key tuples (a prefix allowed), reports the boundaries, refuses bad
// tuples, sharded timeseries tables and transaction blocks, and the
// table keeps serving across the new boundaries (issue #102).
func TestSplitAt(t *testing.T) {
	tc := Start(t, 3)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	s := leasedSession(t, tc, 0, 3*time.Second)
	waitForDatabases(t, ctx, s)

	execSQL(t, ctx, s, `CREATE TABLE sp (a INT8, b TEXT, v INT8, PRIMARY KEY (a, b))`)
	for i := 0; i < 40; i++ {
		execSQL(t, ctx, s, `INSERT INTO sp VALUES ($1, 'b', $1)`, types.NewInt(int64(i)))
	}
	before := countRanges(tc.Nodes[0])
	res := execSQL(t, ctx, s, `ALTER TABLE sp SPLIT AT VALUES (10), (20, 'x'), (30)`)
	if len(res.Rows) != 3 || res.Tag != "ALTER TABLE 3" {
		t.Fatalf("split at: %+v", res)
	}
	for i, want := range []string{"/10", "/20/\"x\"", "/30"} {
		if got := res.Rows[i][0].Text(); !strings.Contains(got, "/table/") || !strings.HasSuffix(got, want) {
			t.Fatalf("boundary %d: %q", i, got)
		}
	}
	if after := countRanges(tc.Nodes[0]); after < before+3 {
		t.Fatalf("ranges: %d before, %d after three splits", before, after)
	}
	// Idempotent: a boundary that already exists is fine.
	execSQL(t, ctx, s, `ALTER TABLE sp SPLIT AT VALUES (10)`)
	if r := execSQL(t, ctx, s, `SELECT count(*), sum(v) FROM sp`); r.Rows[0][0].I != 40 || r.Rows[0][1].I != 780 {
		t.Fatalf("rows across the boundaries: %+v", r.Rows)
	}
	execSQL(t, ctx, s, `INSERT INTO sp VALUES (10, 'a', 1), (20, 'x', 2), (25, 'z', 3)`)
	if r := execSQL(t, ctx, s, `SELECT count(*) FROM sp WHERE a >= 10 AND a < 30`); r.Rows[0][0].I != 23 {
		t.Fatalf("range scan across the boundaries: %+v", r.Rows)
	}

	expectCode := func(q, code string) {
		t.Helper()
		if _, serr := trySQL(ctx, s, q); serr == nil || serr.Code != code {
			t.Fatalf("%s: want %s, got %v", q, code, serr)
		}
	}
	expectCode(`ALTER TABLE sp SPLIT AT VALUES (1, 'a', 3)`, sql.CodeSyntaxError)
	expectCode(`ALTER TABLE sp SPLIT AT VALUES (NULL)`, sql.CodeSyntaxError)
	expectCode(`ALTER TABLE sp SPLIT AT VALUES ('text')`, sql.CodeInvalidParameterValue)
	expectCode(`ALTER TABLE nope SPLIT AT VALUES (1)`, sql.CodeUndefinedTable)
	execSQL(t, ctx, s, `ALTER TABLE IF EXISTS nope SPLIT AT VALUES (1)`)
	execSQL(t, ctx, s, `CREATE TABLE sharded (series INT8, ts TIMESTAMPTZ, PRIMARY KEY (series, ts)) WITH (timeseries = true, shards = 4)`)
	expectCode(`ALTER TABLE sharded SPLIT AT VALUES (1)`, sql.CodeFeatureNotSupported)
	execSQL(t, ctx, s, `BEGIN`)
	expectCode(`ALTER TABLE sp SPLIT AT VALUES (35)`, sql.CodeActiveTransaction)
	execSQL(t, ctx, s, `ROLLBACK`)
}
