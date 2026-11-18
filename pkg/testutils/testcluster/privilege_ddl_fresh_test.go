package testcluster

import (
	"context"
	"testing"
	"time"

	"github.com/sthorne/datax/pkg/sql"
	"github.com/sthorne/datax/pkg/sql/catalog"
)

// TestPrivilegeDDLReadsTheCurrentDescriptor: GRANT, REVOKE and DROP
// POLICY decide what to change from the table's descriptor as their own
// transaction reads it, not from the gateway's leased copy. A change
// that leaves the copy stale — here grants, revokes and a policy written
// by sessions whose accessor takes no lease, so nothing drains the
// leased copy — must not turn a REVOKE, a GRANT or a DROP POLICY IF
// EXISTS into a success that changed nothing.
func TestPrivilegeDDLReadsTheCurrentDescriptor(t *testing.T) {
	tc := Start(t, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	db := tc.Nodes[0].DB()
	// A lease long enough that the copy outlives the test.
	root := leasedSession(t, tc, 0, time.Minute)
	waitForDatabases(t, ctx, root)
	execSQL(t, ctx, root, `CREATE ROLE fresh_r`)
	execSQL(t, ctx, root, `CREATE TABLE fresh_t (id INT8 PRIMARY KEY, v INT8)`)
	execSQL(t, ctx, root, `SELECT id FROM fresh_t`) // root's gateway now holds a lease on it

	current := func() *catalog.TableDescriptor {
		t.Helper()
		return lookupDescriptor(t, ctx, db, "fresh_t")
	}
	for _, w := range []struct {
		name string
		s    *sql.Session
	}{
		{"an unleased session", sql.NewSession(db, catalog.NewAccessor())},
		{"the node's own session", sql.NewSystemSession(db, catalog.NewAccessor())},
	} {
		execSQL(t, ctx, w.s, `GRANT SELECT, INSERT ON fresh_t TO fresh_r`)
		if got := current().Privileges["fresh_r"]; len(got) != 2 {
			t.Fatalf("%s: the grant did not land: %v", w.name, got)
		}
		execSQL(t, ctx, root, `REVOKE SELECT ON fresh_t FROM fresh_r`)
		if got := current().Privileges["fresh_r"]; len(got) != 1 || got[0] != "INSERT" {
			t.Fatalf("after a grant by %s, REVOKE SELECT from a leased session left %v", w.name, got)
		}
		execSQL(t, ctx, root, `REVOKE ALL ON fresh_t FROM fresh_r`)
		if got, ok := current().Privileges["fresh_r"]; ok {
			t.Fatalf("after a grant by %s, REVOKE ALL from a leased session left %v", w.name, got)
		}

		// And the other way: a GRANT whose privilege the stale copy
		// still shows, revoked since.
		execSQL(t, ctx, root, `GRANT UPDATE ON fresh_t TO fresh_r`)
		execSQL(t, ctx, w.s, `REVOKE UPDATE ON fresh_t FROM fresh_r`)
		execSQL(t, ctx, root, `GRANT UPDATE ON fresh_t TO fresh_r`)
		if got := current().Privileges["fresh_r"]; len(got) != 1 || got[0] != "UPDATE" {
			t.Fatalf("after a revoke by %s, GRANT from a leased session left %v", w.name, got)
		}
		execSQL(t, ctx, w.s, `REVOKE ALL ON fresh_t FROM fresh_r`)

		execSQL(t, ctx, w.s, `CREATE POLICY fresh_p ON fresh_t USING (v > 0)`)
		if len(current().Policies) != 1 {
			t.Fatalf("%s: the policy did not land", w.name)
		}
		execSQL(t, ctx, root, `DROP POLICY IF EXISTS fresh_p ON fresh_t`)
		if p := current().Policies; len(p) != 0 {
			t.Fatalf("after a policy made by %s, DROP POLICY IF EXISTS from a leased session left %v", w.name, p)
		}
	}
}
