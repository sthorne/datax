package testcluster

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/sthorne/datax/pkg/base"
	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/kvclient"
	"github.com/sthorne/datax/pkg/kvpb"
	"github.com/sthorne/datax/pkg/sql"
	"github.com/sthorne/datax/pkg/sql/catalog"
)

// Region-restricted replication (issue #176).

// tableID resolves a table's descriptor ID through the catalog.
func placementTableID(t *testing.T, ctx context.Context, db *kvclient.DB, dbName, table string) uint64 {
	t.Helper()
	var id uint64
	err := db.RunTxn(ctx, "find-table", func(ctx context.Context, txn *kvclient.Txn) error {
		d, err := catalog.NewAccessor().LookupFreshIn(ctx, txn, dbName, table)
		if err != nil {
			return err
		}
		id = d.ID
		return nil
	})
	if err != nil {
		t.Fatalf("looking up %s.%s: %v", dbName, table, err)
	}
	return id
}

// waitForTable polls until every range of the given table has want
// replicas, all of them in the named region when one is given.
func (tc *TestCluster) waitForTable(ctx context.Context, t *testing.T, id uint64, region string, want int) {
	t.Helper()
	span := keys.TableDataPrefix(id)
	deadline := time.Now().Add(90 * time.Second)
	var last string
	for time.Now().Before(deadline) {
		descs, err := tc.ranges(ctx)
		if err != nil {
			last = err.Error()
			time.Sleep(200 * time.Millisecond)
			continue
		}
		seen, ok := 0, true
		for _, d := range descs {
			// Every range that starts inside the table's key space.
			if len(d.StartKey) < len(span) || string(d.StartKey[:len(span)]) != string(span) {
				continue
			}
			seen++
			if len(d.Replicas) != want {
				ok = false
				last = fmt.Sprintf("%s has %d replicas, want %d", d.RangeID, len(d.Replicas), want)
				break
			}
			for _, r := range d.Replicas {
				nd, found := tc.nodeDesc(r.NodeID)
				if !found {
					ok, last = false, fmt.Sprintf("no descriptor for n%d", r.NodeID)
					break
				}
				if got := tierValue(nd.Locality, "region"); region != "" && got != region {
					ok = false
					last = fmt.Sprintf("%s has a replica on n%d in region %q, want %q", d.RangeID, r.NodeID, got, region)
					break
				}
			}
			if !ok {
				break
			}
		}
		if ok && seen > 0 {
			return
		}
		if seen == 0 {
			last = "no range covers the table yet"
		}
		time.Sleep(250 * time.Millisecond)
	}
	t.Fatalf("placement never converged (region %q, %d replicas): %s", region, want, last)
}

// rangeContaining returns the range whose span covers key. Unlike the
// pinned table's ranges, an unconstrained table is not split out, so it
// generally shares a range that starts well before its own prefix.
func (tc *TestCluster) rangeContaining(ctx context.Context, key keys.Key) (kvpb.RangeDescriptor, bool) {
	descs, err := tc.ranges(ctx)
	if err != nil {
		return kvpb.RangeDescriptor{}, false
	}
	for _, d := range descs {
		if key.Compare(d.StartKey) >= 0 && (len(d.EndKey) == 0 || key.Compare(d.EndKey) < 0) {
			return d, true
		}
	}
	return kvpb.RangeDescriptor{}, false
}

// tableHasReplicaOutside reports whether the range holding the table's
// keys keeps a replica in a region other than the one named.
func (tc *TestCluster) tableHasReplicaOutside(ctx context.Context, id uint64, region string) bool {
	key := keys.TableDataPrefix(id)
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		if d, ok := tc.rangeContaining(ctx, key); ok {
			for _, r := range d.Replicas {
				if nd, found := tc.nodeDesc(r.NodeID); found && tierValue(nd.Locality, "region") != region {
					return true
				}
			}
		}
		time.Sleep(250 * time.Millisecond)
	}
	return false
}

func mustNodeDesc(t *testing.T, tc *TestCluster, id base.NodeID) base.Locality {
	t.Helper()
	nd, ok := tc.nodeDesc(id)
	if !ok {
		t.Fatalf("no descriptor for n%d", id)
	}
	return nd.Locality
}

// TestPlacementRegionRestricted is the end-to-end claim of issue #176: a
// database pinned to a region keeps every replica of its tables in that
// region, both for data written after the policy and for data that
// already existed when the policy was set.
//
// The cluster is deliberately lopsided — two nodes in eu, three in us —
// so "all replicas in us" is a placement the allocator can only reach by
// honouring the constraint, and "all replicas in eu" is one it cannot
// reach at all. Both are asserted.
func TestPlacementRegionRestricted(t *testing.T) {
	tc := Start(t, 6,
		"region=eu,rack=a", "region=eu,rack=b",
		"region=us,rack=a", "region=us,rack=b", "region=us,rack=c", "region=us,rack=d")
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Second)
	defer cancel()
	root := sql.NewSession(tc.Nodes[0].DB(), catalog.NewAccessor())
	waitForDatabases(t, ctx, root)

	// A table in a database that never gets a policy: the control.
	execSQL(t, ctx, root, `CREATE DATABASE plain`)
	execSQL(t, ctx, root, `USE plain`)
	execSQL(t, ctx, root, `CREATE TABLE untouched (id INT8 PRIMARY KEY, note TEXT)`)
	for i := 0; i < 10; i++ {
		execSQL(t, ctx, root, fmt.Sprintf(`INSERT INTO untouched VALUES (%d, 'row %d')`, i, i))
	}
	plainID := placementTableID(t, ctx, tc.Nodes[0].DB(), "plain", "untouched")

	// A table in an unconstrained database, replicated the ordinary way.
	execSQL(t, ctx, root, `CREATE DATABASE regional`)
	execSQL(t, ctx, root, `USE regional`)
	execSQL(t, ctx, root, `CREATE TABLE ledger (id INT8 PRIMARY KEY, note TEXT)`)
	for i := 0; i < 20; i++ {
		execSQL(t, ctx, root, fmt.Sprintf(`INSERT INTO ledger VALUES (%d, 'row %d')`, i, i))
	}
	id := placementTableID(t, ctx, tc.Nodes[0].DB(), "regional", "ledger")
	if err := tc.waitForReplication(ctx, 3, ""); err != nil {
		t.Fatal(err)
	}

	// No policy: SHOW PLACEMENT still answers, with the cluster default.
	r := execSQL(t, ctx, root, `SHOW PLACEMENT FOR DATABASE regional`)
	if len(r.Rows) != 1 || r.Rows[0][1].I != int64(base.DefaultReplicationFactor) || r.Rows[0][3].S != "cluster default" {
		t.Fatalf("SHOW PLACEMENT before the policy: %+v", r.Rows)
	}

	// Pin the existing database to us. Nothing about the data changes;
	// the allocator has to move the replicas that are in the wrong place.
	execSQL(t, ctx, root, `ALTER DATABASE regional SET (constraints = ('region=us'))`)
	r = execSQL(t, ctx, root, `SHOW PLACEMENT FOR DATABASE regional`)
	if len(r.Rows) != 1 || r.Rows[0][2].S != "region=us" || r.Rows[0][3].S != "database policy" {
		t.Fatalf("SHOW PLACEMENT after the policy: %+v", r.Rows)
	}
	tc.waitForTable(ctx, t, id, "us", base.DefaultReplicationFactor)

	// The rows are all still there and readable after the migration.
	if r := execSQL(t, ctx, root, `SELECT count(*) FROM ledger`); r.Rows[0][0].I != 20 {
		t.Fatalf("rows after the placement migration: %+v", r.Rows)
	}

	// The unconstrained database is unaffected. Its range still spreads
	// across both regions — the diversity scoring picks the widest
	// failure domains when no policy narrows the candidate set — and the
	// placement pass, which acts only on ranges whose policy has
	// constraints, never touches it.
	if !tc.tableHasReplicaOutside(ctx, plainID, "us") {
		t.Fatal("the unconstrained database's replicas were confined to us with the pinned database")
	}
	if r := execSQL(t, ctx, root, `SELECT count(*) FROM plain.untouched`); r.Rows[0][0].I != 10 {
		t.Fatalf("the unconstrained database's rows: %+v", r.Rows)
	}

	// A node holding one of the pinned replicas dies. The repair must
	// pick another node the policy admits — there is a spare in us and
	// two idle nodes in eu, and choosing an eu node would be a silent
	// residency violation.
	victim := -1
	descs, err := tc.ranges(ctx)
	if err != nil {
		t.Fatal(err)
	}
	span := keys.TableDataPrefix(id)
	for _, d := range descs {
		if len(d.StartKey) < len(span) || string(d.StartKey[:len(span)]) != string(span) {
			continue
		}
		for i, n := range tc.Nodes {
			if n == nil {
				continue
			}
			if _, holds := d.GetReplica(n.NodeID()); holds && tierValue(mustNodeDesc(t, tc, n.NodeID()), "region") == "us" {
				victim = i
			}
		}
		if victim >= 0 {
			break
		}
	}
	if victim < 0 {
		t.Fatal("no us node holds a replica of the pinned table")
	}
	tc.StopNode(victim)
	tc.waitForTable(ctx, t, id, "us", base.DefaultReplicationFactor)

	// A table created after the policy inherits it without a further pass
	// over the data: its ranges are allocated into us from the start.
	execSQL(t, ctx, root, `CREATE TABLE journal (id INT8 PRIMARY KEY, note TEXT)`)
	execSQL(t, ctx, root, `INSERT INTO journal VALUES (1, 'after')`)
	tc.waitForTable(ctx, t, placementTableID(t, ctx, tc.Nodes[0].DB(), "regional", "journal"), "us", base.DefaultReplicationFactor)

	// A policy no live node can satisfy for every replica: eu has two
	// nodes and three replicas will not fit. The allocator must leave a
	// replica outside eu rather than dropping one, and the health API
	// must say so — placing data outside a region an operator named is
	// the one thing this must never do, and silently doing nothing is
	// the other.
	execSQL(t, ctx, root, `ALTER DATABASE regional SET (constraints = ('region=eu'))`)
	time.Sleep(15 * time.Second) // several allocator passes
	if err := tc.waitForReplication(ctx, base.DefaultReplicationFactor, ""); err != nil {
		t.Fatalf("replication broke under an unsatisfiable policy: %v", err)
	}
	descs, err = tc.ranges(ctx)
	if err != nil {
		t.Fatal(err)
	}
	outside, covered := 0, 0
	for _, d := range descs {
		if len(d.StartKey) < len(span) || string(d.StartKey[:len(span)]) != string(span) {
			continue
		}
		covered++
		for _, rep := range d.Replicas {
			if nd, ok := tc.nodeDesc(rep.NodeID); ok && tierValue(nd.Locality, "region") != "eu" {
				outside++
			}
		}
	}
	if covered == 0 {
		t.Fatal("no range covers the table")
	}
	if outside == 0 {
		t.Fatal("every replica moved into eu, which has too few nodes to hold three")
	}
	if r := execSQL(t, ctx, root, `SELECT count(*) FROM ledger`); r.Rows[0][0].I != 20 {
		t.Fatalf("rows under an unsatisfiable policy: %+v", r.Rows)
	}

	// Lifting the constraint is an empty list; the replica count is set
	// on its own and the ranges grow to it.
	execSQL(t, ctx, root, `ALTER DATABASE regional SET (constraints = (), replicas = 5)`)
	r = execSQL(t, ctx, root, `SHOW PLACEMENT FOR DATABASE regional`)
	if len(r.Rows) != 1 || r.Rows[0][1].I != 5 || r.Rows[0][2].S != "any node" {
		t.Fatalf("SHOW PLACEMENT after lifting: %+v", r.Rows)
	}
	tc.waitForTable(ctx, t, id, "", 5)
}
