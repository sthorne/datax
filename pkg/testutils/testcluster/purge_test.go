package testcluster

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/sthorne/datax/pkg/cluster"
	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/kvpb"
	"github.com/sthorne/datax/pkg/kvserver"
	"github.com/sthorne/datax/pkg/server"
	"github.com/sthorne/datax/pkg/sql"
	"github.com/sthorne/datax/pkg/sql/catalog"
	"github.com/sthorne/datax/pkg/sql/types"
	"github.com/sthorne/datax/pkg/storage"
	"github.com/sthorne/datax/pkg/util/hlc"
)

func intD(i int) types.Datum    { return types.NewInt(int64(i)) }
func strD(s string) types.Datum { return types.NewString(s) }

// dirHolds reports the files under dir that contain marker, read as raw
// bytes: sstables, write-ahead logs, manifests, whatever is there.
func dirHolds(t *testing.T, dir string, marker []byte) []string {
	t.Helper()
	var found []string
	err := filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			if os.IsNotExist(err) {
				return nil // the engine deleted it while we walked
			}
			return err
		}
		if info.IsDir() {
			return nil
		}
		raw, err := os.ReadFile(path)
		if err != nil {
			if os.IsNotExist(err) {
				return nil
			}
			return err
		}
		if bytes.Contains(raw, marker) {
			found = append(found, strings.TrimPrefix(path, dir))
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walking %s: %v", dir, err)
	}
	return found
}

// engineHolds reports whether any key or value of the engine, read below
// the MVCC layer, contains marker.
func engineHolds(t *testing.T, eng *storage.Engine, marker []byte) bool {
	t.Helper()
	lower, upper := []byte{0}, bytes.Repeat([]byte{0xff}, 16)
	it := eng.NewIter(lower, upper)
	defer func() { _ = it.Close() }()
	for ok := it.SeekGE(lower); ok; ok = it.Next() {
		if bytes.Contains(it.Key(), marker) || bytes.Contains(it.Value(), marker) {
			return true
		}
	}
	return false
}

// versionsIn counts the engine entries of every kind (versions,
// tombstones, intent records) in a table's key span.
func versionsIn(t *testing.T, eng *storage.Engine, tableID uint64) int {
	t.Helper()
	start, end := keys.TableDataSpan(tableID)
	lower, upper := storage.EncodeMVCCKey(start, hlc.Timestamp{}), storage.EncodeMVCCKey(end, hlc.Timestamp{})
	it := eng.NewIter(lower, upper)
	defer func() { _ = it.Close() }()
	n := 0
	for ok := it.SeekGE(lower); ok; ok = it.Next() {
		n++
	}
	return n
}

func purgeRows(t *testing.T, res *sql.Result) string {
	t.Helper()
	var b strings.Builder
	b.WriteString(res.Tag)
	for _, r := range res.Rows {
		b.WriteString("\n ")
		for _, d := range r {
			fmt.Fprintf(&b, " %v", d)
		}
	}
	return b.String()
}

// TestPurgeRemovesDeletedRowsFromDisk is issue #295's first acceptance
// item on a three-node cluster with on-disk stores: after rows are
// deleted and the table purged, no replica's engine holds any version of
// them, read below the MVCC layer, and no file in any node's data
// directory contains the marker their values carried. The rows that were
// not deleted, and a second table that was not purged, are as they were;
// a reader at a timestamp from before the purge is refused, not answered
// short.
func TestPurgeRemovesDeletedRowsFromDisk(t *testing.T) {
	dirs := []string{t.TempDir(), t.TempDir(), t.TempDir()}
	tc := &TestCluster{T: t}
	n1 := startDiskNode(t, dirs[0], true, "")
	tc.Nodes = append(tc.Nodes, n1)
	tc.Nodes = append(tc.Nodes, startDiskNode(t, dirs[1], false, n1.Addr()))
	tc.Nodes = append(tc.Nodes, startDiskNode(t, dirs[2], false, n1.Addr()))
	defer tc.StopAll()
	ctx, cancel := context.WithTimeout(context.Background(), 240*time.Second)
	defer cancel()
	if err := tc.waitForReplication(ctx, 3, ""); err != nil {
		t.Fatal(err)
	}
	s := sql.NewSession(n1.DB(), catalog.NewAccessor())

	// The marker is what an auditor would search the disks for. gc_ttl
	// at creation gives the table its own ranges before any row is
	// written, so every write of the marker goes through those ranges'
	// logs.
	const gone, kept, bystander = "MARKER-Zq7Lw2Xk9RtPm4Vn8Bc3", "KEPT-Hd5Js1Yf6Gu0Ae7Io2Ny", "BYSTANDER-Tx4Mv9Qe3Wz8Ur1"
	execSQL(t, ctx, s, `CREATE TABLE acct (id INT8 PRIMARY KEY, owner INT8, secret TEXT) WITH (gc_ttl = '1h')`)
	execSQL(t, ctx, s, `CREATE INDEX acct_owner ON acct (owner)`)
	execSQL(t, ctx, s, `CREATE TABLE other (id INT8 PRIMARY KEY, v TEXT)`)
	if err := tc.waitForReplication(ctx, 3, ""); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 20; i++ {
		execSQL(t, ctx, s, `INSERT INTO acct VALUES ($1, 1, $2)`, intD(i), strD(gone))
		execSQL(t, ctx, s, `INSERT INTO acct VALUES ($1, 2, $2)`, intD(100+i), strD(kept))
	}
	execSQL(t, ctx, s, `UPDATE acct SET secret = $1 WHERE id = 100`, strD(kept+"-v2"))
	execSQL(t, ctx, s, `INSERT INTO other VALUES (1, $1)`, strD(bystander))
	acctID, otherID := tableID(t, ctx, n1, "acct"), tableID(t, ctx, n1, "other")

	// The search must be able to find the marker while the rows exist,
	// or finding nothing afterwards would prove nothing.
	for i, n := range tc.Nodes {
		if !engineHolds(t, n.Store().Engine(), []byte(gone)) {
			t.Fatalf("n%d: the marker is not in the engine before the delete", i+1)
		}
		if err := n.Store().Engine().Flush(); err != nil {
			t.Fatal(err)
		}
		if len(dirHolds(t, dirs[i], []byte(gone))) == 0 {
			t.Fatalf("n%d: the marker is not in any file before the delete; the byte search cannot see values", i+1)
		}
	}

	before := n1.Clock().Now()
	execSQL(t, ctx, s, `DELETE FROM acct WHERE owner = 1`)
	execSQL(t, ctx, s, `DELETE FROM other WHERE id = 1`)

	// A transaction that read the table before the purge.
	old := sql.NewSession(n1.DB(), catalog.NewAccessor())
	execSQL(t, ctx, old, `BEGIN`)
	execSQL(t, ctx, old, `SELECT count(*) FROM acct`)

	res := execSQL(t, ctx, s, `ALTER TABLE acct PURGE`)
	report := purgeRows(t, res)
	if !strings.HasPrefix(res.Tag, "PURGE ") || strings.Contains(res.Tag, "INCOMPLETE") {
		t.Fatalf("purge did not complete:\n%s", report)
	}
	if len(res.Rows) < 3 {
		t.Fatalf("purge reported %d replica(s), want at least 3:\n%s", len(res.Rows), report)
	}
	seen := map[int64]bool{}
	for _, r := range res.Rows {
		seen[r[1].I] = true
		if r[3].I != 0 || r[4].I != 0 || r[5].I != 0 || !r[6].B || r[7].S != "clean" {
			t.Fatalf("a replica is not clean:\n%s", report)
		}
	}
	if len(seen) != 3 {
		t.Fatalf("purge reported nodes %v, want all three:\n%s", seen, report)
	}

	// Every replica's engine, below the MVCC layer: 20 primary rows and
	// 20 index entries of the kept owner, one version each, and nothing
	// that carries the marker.
	for i, n := range tc.Nodes {
		if got := versionsIn(t, n.Store().Engine(), acctID); got != 40 {
			t.Errorf("n%d: %d engine entries in the table's span after the purge, want 40", i+1, got)
		}
		for _, eng := range []*storage.Engine{n.Store().Engine(), n.Store().RaftEngine()} {
			if engineHolds(t, eng, []byte(gone)) {
				t.Errorf("n%d: an engine still holds the marker after the purge", i+1)
			}
		}
		if !engineHolds(t, n.Store().Engine(), []byte(kept)) {
			t.Errorf("n%d: the rows that were not deleted are gone", i+1)
		}
		// The other table was not purged: its deleted row is still a
		// version and a tombstone.
		if got := versionsIn(t, n.Store().Engine(), otherID); got != 2 {
			t.Errorf("n%d: %d engine entries in the unpurged table's span, want 2 (a version and its tombstone)", i+1, got)
		}
	}
	// The files. The engine deletes the files a compaction replaced
	// shortly after it finishes, so give it a moment.
	deadline := time.Now().Add(20 * time.Second)
	for i := range tc.Nodes {
		for {
			found := dirHolds(t, dirs[i], []byte(gone))
			if len(found) == 0 {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("n%d: the marker is still on disk after the purge, in %v", i+1, found)
			}
			time.Sleep(200 * time.Millisecond)
		}
		if len(dirHolds(t, dirs[i], []byte(kept))) == 0 {
			t.Errorf("n%d: the kept rows' value is in no file; the byte search proves nothing", i+1)
		}
	}

	// What survived reads as before.
	if res := execSQL(t, ctx, s, `SELECT count(*) FROM acct`); res.Rows[0][0].I != 20 {
		t.Fatalf("kept rows: %v", res.Rows)
	}
	if res := execSQL(t, ctx, s, `SELECT count(*) FROM acct WHERE owner = 2`); res.Rows[0][0].I != 20 {
		t.Fatalf("kept rows by index: %v", res.Rows)
	}

	// Older readers are refused. A historical read of the purged table:
	lo, hi := keys.TableDataSpan(acctID)
	if _, err := n1.DB().NewHistoricalTxn("before-purge", before).Scan(ctx, lo, hi, 0); err == nil || !strings.Contains(err.Error(), "GC threshold") {
		t.Fatalf("a read from before the purge was not refused: %v", err)
	}
	// The transaction that was open across it:
	if _, serr := trySQL(ctx, old, `SELECT count(*) FROM acct`); serr == nil {
		t.Fatal("a transaction older than the purge read the table again without an error")
	}
	_, _ = trySQL(ctx, old, `ROLLBACK`)
	// And the table that was not purged still answers at that time,
	// with the row that has since been deleted.
	olo, ohi := keys.TableDataSpan(otherID)
	rows, err := n1.DB().NewHistoricalTxn("before-purge-other", before).Scan(ctx, olo, ohi, 0)
	if err != nil || len(rows) != 1 {
		t.Fatalf("the unpurged table's history: %d row(s), %v", len(rows), err)
	}

	// A second purge finds nothing to do and says so.
	res = execSQL(t, ctx, s, `ALTER TABLE acct PURGE`)
	if res.Tag != "PURGE 0" {
		t.Fatalf("second purge:\n%s", purgeRows(t, res))
	}
}

// staleScan asks one replica for the table's rows at ts as a follower
// read: served locally if the replica believes it may, redirected or
// refused otherwise.
func staleScan(rep *kvserver.Replica, tableID uint64, ts hlc.Timestamp) (int, *kvpb.Error) {
	lo, hi := keys.TableDataSpan(tableID)
	ba := &kvpb.BatchRequest{Header: kvpb.BatchHeader{RangeID: rep.Desc().RangeID, Timestamp: ts, StaleRead: true}}
	ba.Add(&kvpb.ScanRequest{RequestHeader: kvpb.RequestHeader{Key: lo, EndKey: hi}})
	br, kerr := rep.Execute(context.Background(), ba)
	if kerr != nil {
		return 0, kerr
	}
	return len(br.Responses[0].Scan.Rows), nil
}

func tableReplica(t *testing.T, n *server.Node, tableID uint64) *kvserver.Replica {
	t.Helper()
	lo, _ := keys.TableDataSpan(tableID)
	var found *kvserver.Replica
	n.Store().VisitReplicas(func(r *kvserver.Replica) bool {
		if r.Desc().StartKey.Equal(lo) {
			found = r
			return false
		}
		return true
	})
	if found == nil {
		t.Fatalf("n%d holds no replica starting at the table's first key", n.Store().NodeID())
	}
	return found
}

// TestPurgeWithAReplicaDown is the second acceptance item: a replica that
// was down during a purge is reported as unconfirmed, serves none of the
// purged versions when it returns — not even to a follower read at a
// timestamp its own records say it may answer — and is clean after the
// purge is run again.
func TestPurgeWithAReplicaDown(t *testing.T) {
	tc, engines := StartWithEngines(t, 3, fastClosedTS)
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()
	s := sql.NewSession(tc.Nodes[0].DB(), catalog.NewAccessor())

	const gone = "MARKER-Pd8Kq3Zs6Wn1Ty5Vb0Xc"
	execSQL(t, ctx, s, `CREATE TABLE acct (id INT8 PRIMARY KEY, secret TEXT) WITH (gc_ttl = '1h')`)
	for i := 0; i < 10; i++ {
		execSQL(t, ctx, s, `INSERT INTO acct VALUES ($1, $2)`, intD(i), strD(gone))
	}
	id := tableID(t, ctx, tc.Nodes[0], "acct")
	before := tc.Nodes[0].Clock().Now()

	// Node 3 can serve a follower read of the rows at `before`: its
	// closed timestamp covers it. That entitlement is persisted, and is
	// what it comes back from the restart with. (A promise is written to
	// the log only when the log has grown since the last one, so one
	// more write, a moment later, is what puts a promise above `before`
	// on disk.)
	time.Sleep(400 * time.Millisecond)
	execSQL(t, ctx, s, `INSERT INTO acct VALUES (1000, 'later')`)
	time.Sleep(400 * time.Millisecond)
	rep3 := tableReplica(t, tc.Nodes[2], id)
	waitClosed(t, rep3, before)
	if tc.LeaderIndex(rep3.Desc().RangeID) != 2 {
		if rows, kerr := staleScan(rep3, id, before); kerr != nil || rows != 10 {
			t.Fatalf("follower read before the purge: %d rows, %v", rows, kerr)
		}
	}
	tc.StopNode(2)

	execSQL(t, ctx, s, `DELETE FROM acct`)
	res := execSQL(t, ctx, s, `ALTER TABLE acct PURGE`)
	if res.Tag != "PURGE INCOMPLETE" {
		t.Fatalf("a purge with a replica down reported itself complete:\n%s", purgeRows(t, res))
	}
	clean, unconfirmed := 0, 0
	for _, r := range res.Rows {
		switch {
		case r[1].I == 3 && strings.HasPrefix(r[7].S, "unconfirmed"):
			unconfirmed++
		case r[1].I != 3 && r[7].S == "clean":
			clean++
		}
	}
	if clean != 2 || unconfirmed != 1 {
		t.Fatalf("want two clean replicas and n3 unconfirmed:\n%s", purgeRows(t, res))
	}
	for i := 0; i < 2; i++ {
		if engineHolds(t, engines[i], []byte(gone)) {
			t.Errorf("n%d's state engine still holds the marker", i+1)
		}
		if engineHolds(t, tc.RaftEngine(i), []byte(gone)) {
			t.Errorf("n%d's raft engine still holds the marker", i+1)
		}
	}
	if !engineHolds(t, engines[2], []byte(gone)) {
		t.Fatal("the stopped node's engine does not hold the marker; the rest of the test proves nothing")
	}

	// It returns — alone at first, with the rest of the cluster stopped,
	// so that it cannot learn of the purge and has only its own records
	// to go by. Those say it may answer a read at `before`. It must not.
	tc.StopNode(0)
	tc.StopNode(1)
	n3 := tc.RestartNode(2, engines[2], fastClosedTS)
	rep3 = tableReplica(t, n3, id)
	if rep3.ClosedTimestamp().Less(before) {
		t.Fatalf("the returned replica's closed timestamp %s does not cover the read at %s; the test proves nothing", rep3.ClosedTimestamp(), before)
	}
	for i := 0; i < 50; i++ {
		if rows, kerr := staleScan(rep3, id, before); kerr == nil {
			t.Fatalf("the returned replica, cut off from the cluster, served %d row(s) from before the purge", rows)
		}
		time.Sleep(10 * time.Millisecond)
	}
	// The others come back; once it has caught up, its own GC threshold
	// is what refuses the read.
	tc.RestartNode(0, engines[0], fastClosedTS)
	tc.RestartNode(1, engines[1], fastClosedTS)
	s = sql.NewSession(tc.Nodes[0].DB(), catalog.NewAccessor())
	deadline := time.Now().Add(60 * time.Second)
	for {
		rows, kerr := staleScan(rep3, id, before)
		if kerr == nil {
			t.Fatalf("the returned replica served %d row(s) from before the purge", rows)
		}
		if strings.Contains(kerr.Error(), "GC threshold") {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("the returned replica never applied the purge; last answer: %v", kerr)
		}
		time.Sleep(5 * time.Millisecond)
	}
	// Through SQL on that node, the same.
	s3 := sql.NewSession(n3.DB(), catalog.NewAccessor())
	if got := countSQL(t, ctx, s3, `SELECT count(*) FROM acct`); got != 0 {
		t.Fatalf("n3 reads %d row(s)", got)
	}

	// The collection reached it through the log; a second purge
	// compacts it and counts it.
	res = execSQL(t, ctx, s, `ALTER TABLE acct PURGE`)
	if res.Tag != "PURGE 0" || len(res.Rows) != 3 {
		t.Fatalf("second purge:\n%s", purgeRows(t, res))
	}
	if engineHolds(t, engines[2], []byte(gone)) || engineHolds(t, tc.RaftEngine(2), []byte(gone)) {
		t.Error("n3 still holds the marker after the second purge")
	}
}

// TestPurgeKeySpanAndRefusals: the admin op purges a key span that is
// part of a table, leaving the rest of the table's history alone, and
// refuses spans outside table data.
func TestPurgeKeySpanAndRefusals(t *testing.T) {
	tc := StartWithOptions(t, 1, noRowTTLLoop)
	n := tc.Nodes[0]
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	s := sql.NewSession(n.DB(), catalog.NewAccessor())
	execSQL(t, ctx, s, `CREATE TABLE t (id INT8 PRIMARY KEY, v TEXT)`)
	for i := 0; i < 20; i++ {
		execSQL(t, ctx, s, `INSERT INTO t VALUES ($1, 'x')`, intD(i))
	}
	before := n.Clock().Now()
	execSQL(t, ctx, s, `DELETE FROM t WHERE id < 5 OR id >= 15`)
	id := tableID(t, ctx, n, "t")

	// The primary index is the span; the purge covers all of it here.
	lo, hi := keys.TableIndexSpan(id, 1)
	resp := adminCall(t, ctx, n.Addr(), cluster.AdminRequest{Op: "purge", Key: lo, EndKey: hi})
	if resp.Error != "" || resp.Purge == nil || !resp.Purge.Complete {
		t.Fatalf("purge of a key span: %+v", resp)
	}
	if rep := resp.Purge; rep.VersionsRemoved != 20 || len(rep.Replicas) != 1 || rep.Replicas[0].LiveKeys != 10 || len(rep.NotCovered) == 0 {
		t.Fatalf("report: %+v", rep)
	}
	if _, err := n.DB().NewHistoricalTxn("old", before).Scan(ctx, lo, hi, 0); err == nil {
		t.Fatal("a read below the purge was served")
	}
	// By name, the whole table.
	if resp := adminCall(t, ctx, n.Addr(), cluster.AdminRequest{Op: "purge", Table: "t"}); resp.Error != "" || resp.Purge == nil || !resp.Purge.Complete {
		t.Fatalf("purge by table name: %+v", resp)
	}
	// Not the cluster's own key space, and not nothing.
	dlo, dhi := keys.TableDescSpan()
	for _, req := range []cluster.AdminRequest{
		{Op: "purge", Key: dlo, EndKey: dhi},
		{Op: "purge", Key: hi, EndKey: lo},
		{Op: "purge", Table: "no_such_table"},
	} {
		if resp := adminCall(t, ctx, n.Addr(), req); resp.Error == "" {
			t.Fatalf("purge %+v was accepted", req)
		}
	}
	// The purge is an operation and an audit record.
	found := false
	for _, ev := range n.Events().Recent(0, 0, true) {
		if ev.Kind == "purge" && ev.Op != "" {
			found = true
		}
	}
	if !found {
		t.Error("no purge operation in the node's events")
	}
}
