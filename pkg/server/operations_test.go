package server

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/sthorne/datax/pkg/util/events"
)

// Pairing the ring's start/end records into operations (issue #153).
func TestOperationsFromPairsStartAndEnd(t *testing.T) {
	r := events.New()
	r.RecordStart("backup", "op1", "backup to /b started")
	r.Record("split", "r7 split at k") // an instant: not an operation
	r.RecordStart("decommission", "op2", "n3 draining")
	r.RecordEnd("backup", "op1", "ok", "backup written: 4 tables")

	evs := r.Recent(0, 0, true)
	now := time.Now().UnixMilli() + 1000
	ops := operationsFrom(evs, nil, now)

	if len(ops) != 2 {
		t.Fatalf("got %d operations, want 2 (the instant is not one): %+v", len(ops), ops)
	}
	// Running first.
	if !ops[0].Running || ops[0].Kind != "decommission" {
		t.Fatalf("running operation should sort first: %+v", ops)
	}
	if ops[0].ElapsedMs <= 0 {
		t.Fatalf("a running operation reports how long it has been running: %+v", ops[0])
	}
	done := ops[1]
	if done.Running || done.Kind != "backup" || done.Outcome != "ok" {
		t.Fatalf("completed backup: %+v", done)
	}
	// The end's summary replaces the start's, so the row reads as the
	// outcome rather than the intent.
	if done.Summary != "backup written: 4 tables" {
		t.Fatalf("completed summary %q", done.Summary)
	}
	if done.EndedMs < done.StartedMs || done.ElapsedMs < 0 {
		t.Fatalf("completed timing: %+v", done)
	}
}

// An end whose start has aged out of the ring is still reported, with no
// elapsed time invented for it.
func TestOperationsFromEndWithoutStart(t *testing.T) {
	r := events.New()
	r.RecordEnd("re-encryption", "gone", "ok", "re-encryption complete")
	ops := operationsFrom(r.Recent(0, 0, true), r.Open(), time.Now().UnixMilli())
	if len(ops) != 1 {
		t.Fatalf("got %+v", ops)
	}
	if ops[0].Running || ops[0].StartedMs != 0 || ops[0].ElapsedMs != 0 {
		t.Fatalf("an end without a start claims no duration: %+v", ops[0])
	}
	if ops[0].Outcome != "ok" {
		t.Fatalf("outcome %q", ops[0].Outcome)
	}
}

// Since returns a time window and how far back the ring reaches (#155).
func TestRingSinceWindow(t *testing.T) {
	r := events.New()
	r.Record("split", "old")
	time.Sleep(10 * time.Millisecond)
	cut := time.Now()
	time.Sleep(10 * time.Millisecond)
	r.Record("merge", "new")
	r.RecordAudit("auth-failure", "secret")

	evs, oldest := r.Since(cut, 0, false)
	if len(evs) != 1 || evs[0].Summary != "new" {
		t.Fatalf("window: %+v", evs)
	}
	if oldest.IsZero() {
		t.Fatal("Since reports the oldest record the ring holds, so a caller can tell a short ring from a quiet cluster")
	}
	// Audit records stay admin-only through the window form too.
	evs, _ = r.Since(cut, 0, true)
	if len(evs) != 2 {
		t.Fatalf("an admin sees the audit record in the window: %+v", evs)
	}
}

// TestOperationSurvivesRingEviction (issue #190): a running operation
// must not disappear when its start record is pushed out of the ring.
//
// The ring is 500 records and is shared with every split, merge,
// rebalance, lease shed and audit record on the node — minutes of a busy
// cluster, well short of a backup or a decommission. Pairing over the
// ring alone therefore reported a long operation for the first few
// minutes and then nothing, and the operator read an idle cluster that
// was in the middle of moving every replica off a node. That is the
// failure the flat event log had, and the one #153 existed to fix.
func TestOperationSurvivesRingEviction(t *testing.T) {
	r := events.New()
	r.RecordStart("decommission", "op1", "n3 draining: moving its replicas off")
	started := time.Now()

	// Push the start out of the ring with unrelated traffic.
	for i := 0; i < events.RingSize+10; i++ {
		r.Record("split", "r%d split", i)
	}
	evs := r.Recent(0, 0, true)
	for _, ev := range evs {
		if ev.Op == "op1" {
			t.Fatal("the start record is still in the ring; the test is not exercising eviction")
		}
	}

	now := started.Add(20 * time.Minute).UnixMilli()
	ops := operationsFrom(evs, r.Open(), now)
	var found *Operation
	for i := range ops {
		if ops[i].Op == "op1" {
			found = &ops[i]
		}
	}
	if found == nil {
		t.Fatal("a running operation vanished with its start record: the cluster reads idle while it decommissions a node")
	}
	if !found.Running {
		t.Fatalf("operation is not reported as running: %+v", found)
	}
	if found.Summary != "n3 draining: moving its replicas off" {
		t.Fatalf("summary %q", found.Summary)
	}
	// The true start time, so the true elapsed — not a counter that
	// restarts when the record ages out.
	if found.ElapsedMs < 19*time.Minute.Milliseconds() {
		t.Fatalf("elapsed %d ms, want about 20 minutes: the original start time must survive", found.ElapsedMs)
	}
	if found.EndUnrecorded {
		t.Fatalf("20 minutes is not overdue: %+v", found)
	}

	// The end closes it, with the elapsed time measured from the start
	// that was never in the ring.
	time.Sleep(2 * time.Millisecond) // so the duration is not sub-millisecond
	r.RecordEnd("decommission", "op1", "ok", "n3 decommissioned")
	ops = operationsFrom(r.Recent(0, 0, true), r.Open(), time.Now().UnixMilli())
	found = nil
	for i := range ops {
		if ops[i].Op == "op1" {
			found = &ops[i]
		}
	}
	if found == nil || found.Running || found.Outcome != "ok" {
		t.Fatalf("after the end: %+v", found)
	}
	if found.StartedMs == 0 || found.ElapsedMs <= 0 {
		t.Fatalf("a completed operation keeps its start and elapsed: %+v", found)
	}
}

// TestOperationOpenAcrossRestartIsNotRunning: what is open lives in
// memory, so a node killed mid-backup comes back with nothing open —
// which is the honest answer, because that operation is not running.
func TestOperationOpenAcrossRestartIsNotRunning(t *testing.T) {
	r := events.New()
	r.RecordStart("backup", "op1", "backup to /b started")
	if len(r.Open()) != 1 {
		t.Fatalf("open before restart: %+v", r.Open())
	}
	restarted := events.New() // the node came back
	if got := restarted.Open(); len(got) != 0 {
		t.Fatalf("a restart carried operations forward as running: %+v", got)
	}
	if ops := operationsFrom(restarted.Recent(0, 0, true), restarted.Open(), time.Now().UnixMilli()); len(ops) != 0 {
		t.Fatalf("operations after restart: %+v", ops)
	}
}

// TestOperationEndOverdue: an operation whose end never arrives must not
// count up forever as though work is happening.
func TestOperationEndOverdue(t *testing.T) {
	r := events.New()
	r.RecordStart("backup", "op1", "backup to /b started")
	now := time.Now().Add(operationEndOverdue + time.Minute).UnixMilli()
	ops := operationsFrom(r.Recent(0, 0, true), r.Open(), now)
	if len(ops) != 1 || !ops[0].Running {
		t.Fatalf("got %+v", ops)
	}
	if !ops[0].EndUnrecorded {
		t.Fatalf("past %s with no end, the view must say so rather than keep counting: %+v", operationEndOverdue, ops[0])
	}
}

// TestOpenOperationsAreBounded: a caller that records a start and never
// an end must not turn the open map into a leak.
func TestOpenOperationsAreBounded(t *testing.T) {
	r := events.New()
	for i := 0; i < 500; i++ {
		r.RecordStart("leaky", fmt.Sprintf("op%d", i), "never ends")
	}
	if got := len(r.Open()); got > 64 {
		t.Fatalf("%d open operations retained; the map is meant to be a handful, not a job store", got)
	}
}

// TestConsistencySweepIsOneOperation (issue #192): the sweep is paced —
// one range per tick, forever — so the probe is the wrong unit and the
// pass is the right one. The pass has a duration and an outcome, and the
// outcome must agree with what the consistency-failure health check
// reports, because both count the same mismatches.
func TestConsistencySweepPairsByPass(t *testing.T) {
	r := events.New()
	// Two passes over three ranges, the second finding a mismatch.
	r.RecordStart("consistency", "pass1", "consistency sweep started over 3 ranges this node leads")
	r.RecordEnd("consistency", "pass1", "ok", "consistency sweep finished: every range agreed")
	r.RecordStart("consistency", "pass2", "consistency sweep started over 3 ranges this node leads")
	r.RecordEnd("consistency", "pass2", "mismatch", "consistency sweep finished: 1 range(s) disagreed")

	ops := operationsFrom(r.Recent(0, 0, true), r.Open(), time.Now().UnixMilli())
	got := map[string]string{}
	for _, o := range ops {
		if o.Kind == "consistency" {
			got[o.Op] = o.Outcome
		}
	}
	if len(got) != 2 {
		t.Fatalf("got %v, want one operation per pass", got)
	}
	if got["pass1"] != "ok" {
		t.Errorf("a sweep that found nothing reports %q", got["pass1"])
	}
	if got["pass2"] != "mismatch" {
		t.Errorf("a sweep that found a disagreement reports %q; it must agree with the consistency-failure check", got["pass2"])
	}
	for _, o := range ops {
		if o.Kind == "consistency" && o.Running {
			t.Fatalf("a closed pass is still running: %+v", o)
		}
	}
}

// A sweep in progress shows as running with a growing elapsed time,
// which is the whole point of pairing it: it is the longest-running and
// least visible thing a node does.
func TestConsistencySweepInFlight(t *testing.T) {
	r := events.New()
	r.RecordStart("consistency", "pass1", "consistency sweep started over 40 ranges this node leads")
	ops := operationsFrom(r.Recent(0, 0, true), r.Open(), time.Now().Add(time.Minute).UnixMilli())
	if len(ops) != 1 || !ops[0].Running {
		t.Fatalf("got %+v", ops)
	}
	if ops[0].ElapsedMs < 59*1000 {
		t.Fatalf("elapsed %d ms, want about a minute", ops[0].ElapsedMs)
	}
}

// The cluster merge (issue #210): running first, newest start first,
// then completed, newest end first, the node id breaking ties; only the
// completed ones are bounded, and the count dropped is reported.
func TestMergeOperationsOrdersAndBounds(t *testing.T) {
	run := func(node int, started int64) ClusterOperation {
		return ClusterOperation{NodeID: node, Operation: Operation{Kind: "backup", Op: fmt.Sprintf("r%d-%d", node, started), Running: true, StartedMs: started}}
	}
	done := func(node int, ended int64) ClusterOperation {
		return ClusterOperation{NodeID: node, Operation: Operation{Kind: "backup", Op: fmt.Sprintf("d%d-%d", node, ended), EndedMs: ended, Outcome: "ok"}}
	}
	running := []ClusterOperation{run(3, 100), run(1, 300), run(2, 300)}
	completed := []ClusterOperation{done(2, 50), done(1, 90), done(3, 90), done(1, 10)}
	out, truncated := mergeOperations(running, completed, 3)
	var got []string
	for _, o := range out {
		got = append(got, o.Op)
	}
	want := []string{"r1-300", "r2-300", "r3-100", "d1-90", "d3-90", "d2-50"}
	if strings.Join(got, " ") != strings.Join(want, " ") {
		t.Fatalf("order %v, want %v", got, want)
	}
	if truncated != 1 {
		t.Fatalf("truncated %d, want 1 (the oldest completed operation)", truncated)
	}
	// Running operations are never dropped, however many there are.
	out, truncated = mergeOperations(running, nil, 0)
	if len(out) != 3 || truncated != 0 {
		t.Fatalf("a limit of 0 dropped running operations: %d kept, %d truncated", len(out), truncated)
	}
}
