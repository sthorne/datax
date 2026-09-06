package testcluster

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/sthorne/datax/pkg/server"
	"github.com/sthorne/datax/pkg/sql"
	"github.com/sthorne/datax/pkg/sql/catalog"
)

// TestOperationsTimeline (issue #153): the event ring recorded instants,
// so an operation that started twenty minutes ago and one that finished
// last week read identically. A long-running operation now records both
// of its ends under one id, and the console's ops view is the server's
// pairing of them — which means the pairing is a JSON contract every
// node answers, not a reading the page invents.
//
// The backup is the operation under test because a test cluster can run
// a real one end to end in a second.
func TestOperationsTimeline(t *testing.T) {
	tc := startWithHTTP(t, 3)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	s := sql.NewSession(tc.Nodes[0].DB(), catalog.NewAccessor())
	execSQL(t, ctx, s, `CREATE TABLE t (id INT8 PRIMARY KEY, v TEXT)`)
	execSQL(t, ctx, s, `INSERT INTO t VALUES (1, 'a'), (2, 'b')`)

	before := time.Now()
	if _, err := tc.Nodes[0].RunBackup(ctx, t.TempDir(), "", false, false); err != nil {
		t.Fatalf("backup: %v", err)
	}

	// The overview document — the one poll the console makes — carries
	// the paired operations, not just the flat ring.
	addr := tc.Nodes[0].HTTPAddr()
	code, _, body := httpGet(t, "http://"+addr+"/api/overview")
	if code != 200 {
		t.Fatalf("/api/overview: %d %s", code, body)
	}
	var ov server.OverviewStatus
	if err := json.Unmarshal([]byte(body), &ov); err != nil {
		t.Fatal(err)
	}
	if ov.Events == nil {
		t.Fatalf("no events section: %s", body)
	}
	var backup *server.Operation
	for i, op := range ov.Events.Operations {
		if op.Kind == "backup" {
			backup = &ov.Events.Operations[i]
		}
	}
	if backup == nil {
		t.Fatalf("the backup that just ran is not among the operations: %+v", ov.Events.Operations)
	}
	// Both ends recorded: it is finished, it says how it went, and the
	// duration is measured from its own start rather than guessed.
	if backup.Running {
		t.Fatalf("the backup returned, so it is not running: %+v", backup)
	}
	if backup.Outcome != "ok" {
		t.Fatalf("outcome %q, want ok: %+v", backup.Outcome, backup)
	}
	if backup.Op == "" {
		t.Fatalf("no operation id, so nothing pairs: %+v", backup)
	}
	if backup.StartedMs == 0 || backup.EndedMs == 0 || backup.EndedMs < backup.StartedMs {
		t.Fatalf("both ends should be recorded and ordered: %+v", backup)
	}
	if backup.ElapsedMs < 0 || backup.EndedMs-backup.StartedMs != backup.ElapsedMs {
		t.Fatalf("elapsed is the distance between the two ends: %+v", backup)
	}
	if backup.StartedMs < before.Add(-time.Minute).UnixMilli() {
		t.Fatalf("start %d predates the test: %+v", backup.StartedMs, backup)
	}

	// The scoped node's own document carries its own pairing, so the ops
	// view scoped to n2 reports what n2 did rather than what n1 did.
	code, _, body = httpGet(t, "http://"+addr+"/api/node")
	if code != 200 {
		t.Fatalf("/api/node: %d %s", code, body)
	}
	var self server.NodeDetail
	if err := json.Unmarshal([]byte(body), &self); err != nil {
		t.Fatal(err)
	}
	if self.Operations == nil {
		t.Fatalf("the node document carries no operations: %s", body)
	}
	found := false
	for _, op := range self.Operations {
		if op.Kind == "backup" && op.Op == backup.Op {
			found = true
		}
	}
	if !found {
		t.Fatalf("n1 ran the backup but its own document does not pair it: %+v", self.Operations)
	}

	code, _, body = httpGet(t, "http://"+addr+"/api/node?id=2")
	if code != 200 {
		t.Fatalf("/api/node?id=2: %d %s", code, body)
	}
	var n2 server.NodeDetail
	if err := json.Unmarshal([]byte(body), &n2); err != nil {
		t.Fatal(err)
	}
	for _, op := range n2.Operations {
		if op.Op == backup.Op {
			t.Fatalf("n2 did not run n1's backup, but reports it: %+v", op)
		}
	}
}

// TestEventsWindow (issue #155): the metrics charts mark the events that
// explain a change in shape, which needs a time window over the ring and
// an honest answer about how far back the ring reaches. A chart drawing
// seven days over a ring covering two hours must say so rather than
// implying five quiet days.
func TestEventsWindow(t *testing.T) {
	tc := startWithHTTP(t, 3)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	s := sql.NewSession(tc.Nodes[0].DB(), catalog.NewAccessor())
	// Splits are the structural events the charts mark by default; a few
	// of them give the window something to include and exclude.
	for i := 1; i <= 3; i++ {
		execSQL(t, ctx, s, fmt.Sprintf(`CREATE TABLE w%d (id INT8 PRIMARY KEY)`, i))
	}
	addr := tc.Nodes[0].HTTPAddr()

	// A window that starts before the cluster did holds everything the
	// ring holds; one that starts in the future holds nothing. Both are
	// answered without an error, because "nothing happened in this
	// window" is an answer.
	all := eventsWindow(t, addr, time.Now().Add(-24*time.Hour))
	none := eventsWindow(t, addr, time.Now().Add(time.Hour))
	if len(all.Events) == 0 {
		t.Fatalf("a day-wide window over a running cluster is empty: %+v", all)
	}
	if len(none.Events) != 0 {
		t.Fatalf("a window starting in the future is not empty: %+v", none.Events)
	}

	// Every event in the window is inside it.
	from := time.Now().Add(-24 * time.Hour)
	for _, ev := range all.Events {
		if ev.At.Before(from) {
			t.Fatalf("event outside the window: %+v", ev)
		}
	}

	// The oldest record the ring still holds is reported, so the chart
	// can say where its knowledge stops. It cannot be older than the
	// cluster and cannot be in the future.
	if all.OldestMs == 0 {
		t.Fatalf("a non-empty ring reports no oldest record: %+v", all)
	}
	if all.OldestMs > time.Now().UnixMilli() {
		t.Fatalf("the oldest record is in the future: %d", all.OldestMs)
	}
	oldest := all.Events[0]
	for _, ev := range all.Events {
		if ev.At.Before(oldest.At) {
			oldest = ev
		}
	}
	// The reported oldest is at or before the oldest record served: the
	// window may have cut records the ring still holds.
	if all.OldestMs > oldest.At.UnixMilli() {
		t.Fatalf("oldest_unix_ms %d is newer than the oldest event served (%s)", all.OldestMs, oldest.At)
	}

	// A window narrower than the ring returns a subset, not the lot.
	narrow := eventsWindow(t, addr, time.Now().Add(-time.Millisecond))
	if len(narrow.Events) > len(all.Events) {
		t.Fatalf("a 1ms window returned more than a day: %d vs %d", len(narrow.Events), len(all.Events))
	}
	if narrow.OldestMs != all.OldestMs {
		t.Fatalf("the ring's reach does not depend on the window asked for: %d vs %d", narrow.OldestMs, all.OldestMs)
	}
}

func eventsWindow(t *testing.T, addr string, from time.Time) server.EventsStatus {
	t.Helper()
	code, _, body := httpGet(t, fmt.Sprintf("http://%s/api/events?from=%d", addr, from.UnixMilli()))
	if code != 200 {
		t.Fatalf("/api/events?from=: %d %s", code, body)
	}
	var doc server.EventsStatus
	if err := json.Unmarshal([]byte(body), &doc); err != nil {
		t.Fatal(err)
	}
	return doc
}
