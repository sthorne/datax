package testcluster

import (
	"bytes"
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/kvclient"
	"github.com/sthorne/datax/pkg/kvpb"
	"github.com/sthorne/datax/pkg/server"
	"github.com/sthorne/datax/pkg/sql"
	"github.com/sthorne/datax/pkg/sql/catalog"
	"github.com/sthorne/datax/pkg/version"
)

// TestViewDependencyRecords (issue #313): the views reading a relation
// are recorded under it, so DROP TABLE, a rename and DROP COLUMN find
// them without reading every descriptor. Before v22 — while a node that
// does not write the records may be running — every descriptor is read
// as before; the first question at v22 records every view and marks the
// records complete, after which a DROP TABLE reads no descriptor scan.
func TestViewDependencyRecords(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	// At v21 the answer comes from the descriptors, and nothing is marked.
	old := StartWithOptions(t, 1, func(c *server.Config) { c.BinaryVersionOverride = version.V21 })
	so := sql.NewSession(old.Nodes[0].DB(), catalog.NewAccessor())
	waitForDatabases(t, ctx, so)
	execSQL(t, ctx, so, `CREATE TABLE t (id INT8 PRIMARY KEY, v TEXT)`)
	execSQL(t, ctx, so, `CREATE VIEW v AS SELECT id FROM t`)
	if _, serr := trySQL(ctx, so, `DROP TABLE t`); serr == nil || serr.Code != sql.CodeDependentObjectsExist {
		t.Fatalf("DROP TABLE under a view at v21: %v", serr)
	}
	if raw, err := old.Nodes[0].DB().Get(ctx, keys.ViewDependencyIndexKey()); err != nil || raw != nil {
		t.Fatalf("the records were marked complete at v21: %q %v", raw, err)
	}

	tc := Start(t, 1)
	n := tc.Nodes[0]
	s := sql.NewSession(n.DB(), catalog.NewAccessor())
	waitForDatabases(t, ctx, s)
	execSQL(t, ctx, s, `CREATE TABLE t (id INT8 PRIMARY KEY, v TEXT)`)
	execSQL(t, ctx, s, `CREATE TABLE u (id INT8 PRIMARY KEY, w TEXT)`)
	execSQL(t, ctx, s, `CREATE TABLE plain (id INT8 PRIMARY KEY)`)
	execSQL(t, ctx, s, `CREATE VIEW v AS SELECT id, v FROM t`)
	execSQL(t, ctx, s, `CREATE VIEW vv AS SELECT id FROM v`)
	// Until the records are complete the descriptors answer; the node
	// leading range 1 completes them once.
	if raw, err := n.DB().Get(ctx, keys.ViewDependencyIndexKey()); err != nil || raw != nil {
		t.Fatalf("the records were marked complete before the pass: %q %v", raw, err)
	}
	if _, serr := trySQL(ctx, s, `DROP TABLE t`); serr == nil || serr.Code != sql.CodeDependentObjectsExist {
		t.Fatalf("DROP TABLE under a view: %v", serr)
	}
	// A view a v21 node created has no record: remove v's, as if it had.
	tID, vID := tableID(t, ctx, n, "t"), tableID(t, ctx, n, "v")
	if err := n.DB().RunTxn(ctx, "forget", func(ctx context.Context, txn *kvclient.Txn) error {
		return txn.Delete(ctx, keys.ViewDependencyKey(tID, vID))
	}); err != nil {
		t.Fatal(err)
	}
	if !n.RecordViewDependenciesOnce(ctx) {
		t.Fatal("the records were not completed at v22")
	}
	if raw, err := n.DB().Get(ctx, keys.ViewDependencyKey(tID, vID)); err != nil || raw == nil {
		t.Fatalf("the pass did not record v's dependency on t: %v", err)
	}
	if old.Nodes[0].RecordViewDependenciesOnce(ctx) {
		t.Fatal("the records were completed at v21")
	}

	// From the records: no descriptor scan.
	var descRows atomic.Int64
	lo, hi := keys.TableDescSpan()
	n.DB().TestingSetSendObserver(func(_ context.Context, ba *kvpb.BatchRequest, br *kvpb.BatchResponse) {
		for i, ru := range ba.Requests {
			if ru.Scan == nil || br == nil || i >= len(br.Responses) || br.Responses[i].Scan == nil {
				continue
			}
			if k := ru.GetInner().Header().Key; bytes.Compare(k, lo) >= 0 && bytes.Compare(k, hi) < 0 {
				descRows.Add(int64(len(br.Responses[i].Scan.Rows)))
			}
		}
	})
	defer n.DB().TestingSetSendObserver(nil)
	for _, c := range []struct{ stmt, code string }{
		{`DROP TABLE t`, sql.CodeDependentObjectsExist},
		{`ALTER TABLE t RENAME TO t2`, sql.CodeDependentObjectsExist},
		{`ALTER TABLE t DROP COLUMN v`, sql.CodeDependentObjectsExist},
		{`DROP VIEW v`, sql.CodeDependentObjectsExist}, // vv reads it
	} {
		if _, serr := trySQL(ctx, s, c.stmt); serr == nil || serr.Code != c.code {
			t.Fatalf("%s: %v, want %s", c.stmt, serr, c.code)
		}
	}
	execSQL(t, ctx, s, `DROP TABLE plain`)
	if got := descRows.Load(); got != 0 {
		t.Fatalf("the statements read %d descriptors by scanning", got)
	}
	n.DB().TestingSetSendObserver(nil)

	// OR REPLACE moves the records; DROP VIEW removes them.
	ids := map[string]uint64{}
	for _, name := range []string{"t", "u", "v"} {
		ids[name] = tableID(t, ctx, n, name)
	}
	execSQL(t, ctx, s, `CREATE OR REPLACE VIEW v AS SELECT id, w AS v FROM u`)
	if _, serr := trySQL(ctx, s, `DROP TABLE u`); serr == nil || serr.Code != sql.CodeDependentObjectsExist {
		t.Fatalf("DROP TABLE under the replaced view: %v", serr)
	}
	execSQL(t, ctx, s, `ALTER TABLE t DROP COLUMN v`)
	execSQL(t, ctx, s, `DROP VIEW v CASCADE`)
	execSQL(t, ctx, s, `DROP TABLE u`)
	execSQL(t, ctx, s, `DROP TABLE t`)
	for name, id := range ids {
		lo, hi := keys.ViewDependencySpan(id)
		// In a transaction: the DROPs' intents on these keys may not be
		// resolved yet, and a plain scan refuses an intent where a
		// transactional one pushes it.
		var kvs []kvpb.KeyValue
		err := n.DB().RunTxn(ctx, "view-deps-check", func(ctx context.Context, txn *kvclient.Txn) error {
			var err error
			kvs, err = txn.Scan(ctx, lo, hi, 0)
			return err
		})
		if err != nil {
			t.Fatal(err)
		}
		if len(kvs) != 0 {
			t.Fatalf("%s: %d record(s) outlived the views", name, len(kvs))
		}
	}
}
