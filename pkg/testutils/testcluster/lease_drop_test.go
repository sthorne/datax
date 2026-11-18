package testcluster

import (
	"context"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/sql"
	"github.com/sthorne/datax/pkg/sql/catalog"
)

// TestDropDrainsEveryGateway (issue #377): gateway B plans against a
// table it caches under lease; gateway A drops it. Once A's statement
// returns, B must not plan against the table any more: its lookup fails
// with 42P01. One case per path that drops a table or a view.
func TestDropDrainsEveryGateway(t *testing.T) {
	tc := Start(t, 3)
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()

	const ttl = 3 * time.Second
	sA := leasedSession(t, tc, 0, ttl)
	sB := leasedSession(t, tc, 1, ttl)
	waitForDatabases(t, ctx, sA)

	cases := []struct {
		name  string
		setup []string
		probe string // B's statement, cached before the drop
		drop  string
	}{
		{
			name:  "drop table",
			setup: []string{`CREATE TABLE pt (id INT8 PRIMARY KEY)`, `INSERT INTO pt VALUES (1)`},
			probe: `SELECT id FROM pt`,
			drop:  `DROP TABLE pt`,
		},
		{
			name:  "drop view",
			setup: []string{`CREATE TABLE vbase (id INT8 PRIMARY KEY)`, `CREATE VIEW pv AS SELECT id FROM vbase`},
			probe: `SELECT id FROM pv`,
			drop:  `DROP VIEW pv`,
		},
		{
			name: "drop owned",
			setup: []string{
				`CREATE ROLE dropper`,
				`CREATE TABLE owned_t (id INT8 PRIMARY KEY)`,
				`ALTER TABLE owned_t OWNER TO dropper`,
			},
			probe: `SELECT id FROM owned_t`,
			drop:  `DROP OWNED BY dropper`,
		},
		{
			name:  "drop database cascade",
			setup: []string{`CREATE DATABASE dropdb`, `CREATE TABLE dropdb.dt (id INT8 PRIMARY KEY)`},
			probe: `SELECT id FROM dropdb.dt`,
			drop:  `DROP DATABASE dropdb CASCADE`,
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			for _, stmt := range c.setup {
				execSQL(t, ctx, sA, stmt)
			}
			execEventually(t, ctx, sB, c.probe) // B caches it under lease
			execSQL(t, ctx, sB, c.probe)
			execSQL(t, ctx, sA, c.drop)
			expectCodeSQL(t, ctx, sB, c.probe, sql.CodeUndefinedTable)
		})
	}

	// A drop rolled back to a savepoint drains nothing: the statement
	// returns without waiting out B's lease, and B keeps the table.
	execSQL(t, ctx, sA, `CREATE TABLE kept (id INT8 PRIMARY KEY)`)
	execEventually(t, ctx, sB, `SELECT id FROM kept`)
	execSQL(t, ctx, sA, `BEGIN`)
	execSQL(t, ctx, sA, `SAVEPOINT s`)
	execSQL(t, ctx, sA, `DROP TABLE kept`)
	execSQL(t, ctx, sA, `ROLLBACK TO SAVEPOINT s`)
	began := time.Now()
	execSQL(t, ctx, sA, `COMMIT`)
	if d := time.Since(began); d >= ttl {
		t.Fatalf("COMMIT of a rolled-back drop took %s: it drained a table that is still there", d)
	}
	execSQL(t, ctx, sB, `SELECT id FROM kept`)
	// DROP ... IF EXISTS of a table that is not there is still a no-op.
	execSQL(t, ctx, sA, `DROP TABLE IF EXISTS pt`)
	execSQL(t, ctx, sA, `DROP VIEW IF EXISTS pv`)
}

// TestDropRenewalRace (issue #377): a renewal on the dropping gateway
// reads the descriptor and commits its lease just before the drop
// commits, and is held there while the drop runs, uncaches the table
// and returns. Released, it must not install the entry it took: the
// gateway would serve the dropped table for the rest of a TTL.
func TestDropRenewalRace(t *testing.T) {
	tc := Start(t, 3)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	const ttl = 3 * time.Second
	sA, catA := leasedSessionWithAccessor(t, tc, 0, ttl)
	execSQL(t, ctx, sA, `CREATE TABLE racy (id INT8 PRIMARY KEY)`)
	execSQL(t, ctx, sA, `SELECT id FROM racy`) // A caches it
	catA.TestingPauseRenewal(true)             // the only renewal is the test's
	defer catA.TestingPauseRenewal(false)

	inWindow := make(chan struct{})
	release := make(chan struct{})
	var held atomic.Bool // the first renewal only: not the drop's own refresh
	catalog.SetTestingAfterLeaseCommit(func(a *catalog.Accessor, name string) {
		if a != catA || !strings.HasSuffix(name, "/racy") || !held.CompareAndSwap(false, true) {
			return
		}
		close(inWindow)
		<-release
	})
	defer catalog.SetTestingAfterLeaseCommit(nil)

	renewed := make(chan error, 1)
	go func() { renewed <- catA.TestingRenewNow(ctx) }()
	select {
	case <-inWindow:
	case <-ctx.Done():
		t.Fatal("the renewal never committed its lease")
	}
	execSQL(t, ctx, sA, `DROP TABLE racy`)
	close(release)
	if err := <-renewed; err != nil {
		t.Fatal(err)
	}
	expectCodeSQL(t, ctx, sA, `SELECT id FROM racy`, sql.CodeUndefinedTable)
}

// TestDropFailsWriteFromBefore (issue #377): a write that gateway B
// planned against its cached descriptor before A dropped the table may
// not commit after the drop. B's transaction is pushed above a read
// made once the drop has returned (it writes a key read there), so it
// could commit only after the drop; it must fail, leaving nothing in the
// dropped table's span.
func TestDropFailsWriteFromBefore(t *testing.T) {
	tc := Start(t, 3)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	const ttl = 3 * time.Second
	sA := leasedSession(t, tc, 0, ttl)
	sB := leasedSession(t, tc, 1, ttl)
	execSQL(t, ctx, sA, `CREATE TABLE wt (id INT8 PRIMARY KEY)`)
	execSQL(t, ctx, sA, `CREATE TABLE side (id INT8 PRIMARY KEY)`)
	execSQL(t, ctx, sA, `INSERT INTO wt VALUES (1)`)
	execSQL(t, ctx, sB, `SELECT id FROM wt`) // B caches both
	execSQL(t, ctx, sB, `SELECT id FROM side`)
	id := lookupDescriptor(t, ctx, tc.Nodes[0].DB(), "wt").ID
	lo, hi := keys.TableDataSpan(id)
	before, err := tc.Nodes[0].DB().Scan(ctx, lo, hi, 0)
	if err != nil {
		t.Fatal(err)
	}

	execSQL(t, ctx, sB, `BEGIN`)
	execSQL(t, ctx, sB, `INSERT INTO wt VALUES (2)`) // planned under B's lease
	execSQL(t, ctx, sA, `DROP TABLE wt`)
	// A read after the drop returned, of a key B is about to write: B's
	// write, and so its commit, is pushed above it.
	execSQL(t, ctx, sA, `SELECT id FROM side WHERE id = 7`)
	if _, serr := trySQL(ctx, sB, `INSERT INTO side VALUES (7)`); serr == nil {
		if _, serr := trySQL(ctx, sB, `COMMIT`); serr == nil {
			t.Fatal("a write planned before the drop committed after it")
		}
	} else {
		execSQL(t, ctx, sB, `ROLLBACK`)
	}
	after, err := tc.Nodes[0].DB().Scan(ctx, lo, hi, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(after) != len(before) {
		t.Fatalf("the dropped table's span holds %d key(s), %d before the drop", len(after), len(before))
	}
}
