package testcluster

import (
	"bytes"
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/kvpb"
	"github.com/sthorne/datax/pkg/kvserver"
	"github.com/sthorne/datax/pkg/server"
	"github.com/sthorne/datax/pkg/sql"
	"github.com/sthorne/datax/pkg/sql/catalog"
	"github.com/sthorne/datax/pkg/util/hlc"
)

// fastClosedTS makes closed timestamps publish quickly so tests need not
// wait out the production 3s lag.
func fastClosedTS(cfg *server.Config) {
	cfg.ClosedTimestampLag = 200 * time.Millisecond
	cfg.ClosedTimestampInterval = 50 * time.Millisecond
}

// waitClosed polls until the replica's closed timestamp covers ts.
func waitClosed(t *testing.T, rep *kvserver.Replica, ts hlc.Timestamp) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for rep.ClosedTimestamp().Less(ts) {
		if time.Now().After(deadline) {
			t.Fatalf("closed timestamp stuck at %s, want >= %s", rep.ClosedTimestamp(), ts)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func staleGet(rep *kvserver.Replica, key keys.Key, ts hlc.Timestamp) (*kvpb.BatchResponse, *kvpb.Error) {
	ba := &kvpb.BatchRequest{Header: kvpb.BatchHeader{RangeID: 1, Timestamp: ts, StaleRead: true}}
	ba.Add(&kvpb.GetRequest{RequestHeader: kvpb.RequestHeader{Key: key}})
	return rep.Execute(context.Background(), ba)
}

// TestFollowerReadServesLocally: a NON-leader replica serves a read pinned
// at a timestamp its closed timestamp covers, returning the historical
// value; reads it cannot prove closed still redirect. Core of issue #5.
func TestFollowerReadServesLocally(t *testing.T) {
	tc, _ := StartWithEngines(t, 3, fastClosedTS)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	prefix := keys.TableDataPrefix(890)
	key := append(prefix.Clone(), "k"...)
	if err := tc.Nodes[0].DB().Put(ctx, key, []byte("v1")); err != nil {
		t.Fatal(err)
	}
	ts1 := tc.Nodes[0].Clock().Now()
	if err := tc.Nodes[0].DB().Put(ctx, key, []byte("v2")); err != nil {
		t.Fatal(err)
	}

	leader := tc.LeaderIndex(1)
	follower := (leader + 1) % 3
	rep, ok := tc.Nodes[follower].Store().GetReplica(1)
	if !ok {
		t.Fatal("no follower replica of range 1")
	}
	if rep.IsLeader() {
		t.Fatal("picked the leader as follower")
	}
	waitClosed(t, rep, ts1)

	// The follower serves the read at ts1 locally — and returns the value
	// as of ts1, not the newer overwrite.
	br, kerr := staleGet(rep, key, ts1)
	if kerr != nil {
		t.Fatalf("follower refused a closed stale read: %v", kerr)
	}
	if got := br.Responses[0].Get.Value; !bytes.Equal(got, []byte("v1")) {
		t.Fatalf("stale read at ts1 = %q, want v1", got)
	}

	// Without the stale flag the follower redirects, as always.
	ba := &kvpb.BatchRequest{Header: kvpb.BatchHeader{RangeID: 1, Timestamp: ts1}}
	ba.Add(&kvpb.GetRequest{RequestHeader: kvpb.RequestHeader{Key: key}})
	if _, kerr := rep.Execute(ctx, ba); kerr == nil || kerr.NotLeader == nil {
		t.Fatalf("non-stale read on follower not redirected: %v", kerr)
	}

	// A stale read ABOVE the closed timestamp redirects too.
	future := tc.Nodes[follower].Clock().Now().AddNanos(int64(time.Second))
	if _, kerr := staleGet(rep, key, future); kerr == nil || kerr.NotLeader == nil {
		t.Fatalf("unclosed stale read not redirected: %v", kerr)
	}
}

// TestFollowerReadSurvivesLeaderLoss: the acceptance bar — reads below the
// closed timestamp keep working with the leader gone (no election needed:
// the closed timestamp is replicated state).
func TestFollowerReadSurvivesLeaderLoss(t *testing.T) {
	tc, _ := StartWithEngines(t, 3, fastClosedTS)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	prefix := keys.TableDataPrefix(891)
	key := append(prefix.Clone(), "k"...)
	if err := tc.Nodes[0].DB().Put(ctx, key, []byte("v")); err != nil {
		t.Fatal(err)
	}
	ts := tc.Nodes[0].Clock().Now()

	leader := tc.LeaderIndex(1)
	follower := (leader + 1) % 3
	rep, _ := tc.Nodes[follower].Store().GetReplica(1)
	waitClosed(t, rep, ts)

	tc.StopNode(leader)

	// Immediately — before any new election can settle — the follower
	// serves the historical read from its replicated closed timestamp.
	br, kerr := staleGet(rep, key, ts)
	if kerr != nil {
		t.Fatalf("follower read failed after leader loss: %v", kerr)
	}
	if got := br.Responses[0].Get.Value; !bytes.Equal(got, []byte("v")) {
		t.Fatalf("got %q, want v", got)
	}
}

// TestFollowerReadBailsOnIntent: an unresolved intent beneath the read
// redirects to the leader (whose conflict machinery can push); once the
// transaction commits and resolution applies, the follower serves the
// value.
func TestFollowerReadBailsOnIntent(t *testing.T) {
	tc, _ := StartWithEngines(t, 3, fastClosedTS)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	prefix := keys.TableDataPrefix(892)
	key := append(prefix.Clone(), "k"...)
	txn := tc.Nodes[0].DB().NewTxn("writer")
	if err := txn.Put(ctx, key, []byte("provisional")); err != nil {
		t.Fatal(err)
	}
	afterIntent := tc.Nodes[0].Clock().Now()

	leader := tc.LeaderIndex(1)
	follower := (leader + 1) % 3
	rep, _ := tc.Nodes[follower].Store().GetReplica(1)
	waitClosed(t, rep, afterIntent)

	readTS := rep.ClosedTimestamp()
	if _, kerr := staleGet(rep, key, readTS); kerr == nil || kerr.NotLeader == nil {
		t.Fatalf("intent-covered stale read not redirected: %v", kerr)
	}

	if err := txn.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	// Resolution replicates asynchronously; eventually the follower serves
	// the committed value at a covering timestamp.
	deadline := time.Now().Add(15 * time.Second)
	for {
		ts := rep.ClosedTimestamp()
		br, kerr := staleGet(rep, key, ts)
		if kerr == nil && bytes.Equal(br.Responses[0].Get.Value, []byte("provisional")) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("follower never served the resolved value (last: %v)", kerr)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// TestSQLAsOfSystemTime: the SQL surface — a SELECT pinned to a past
// timestamp sees the data as of that moment.
func TestSQLAsOfSystemTime(t *testing.T) {
	tc, _ := StartWithEngines(t, 3, fastClosedTS)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	s := sql.NewSession(tc.Nodes[0].DB(), catalog.NewAccessor())

	execSQL(t, ctx, s, `CREATE TABLE ledger (id INT8 PRIMARY KEY, v INT8)`)
	execSQL(t, ctx, s, `INSERT INTO ledger VALUES (1, 10)`)
	tsNanos := tc.Nodes[0].Clock().Now().WallTime
	execSQL(t, ctx, s, `UPDATE ledger SET v = 20 WHERE id = 1`)

	res := execSQL(t, ctx, s, fmt.Sprintf(`SELECT v FROM ledger AS OF SYSTEM TIME '%d' WHERE id = 1`, tsNanos))
	if len(res.Rows) != 1 || res.Rows[0][0].I != 10 {
		t.Fatalf("AS OF SYSTEM TIME read = %+v, want v=10", res.Rows)
	}
	res = execSQL(t, ctx, s, `SELECT v FROM ledger WHERE id = 1`)
	if len(res.Rows) != 1 || res.Rows[0][0].I != 20 {
		t.Fatalf("current read = %+v, want v=20", res.Rows)
	}

	// Inside an explicit transaction it is refused.
	execSQL(t, ctx, s, `BEGIN`)
	if _, serr := trySQL(ctx, s, fmt.Sprintf(`SELECT v FROM ledger AS OF SYSTEM TIME '%d'`, tsNanos)); serr == nil {
		t.Fatal("AS OF SYSTEM TIME allowed inside a transaction block")
	}
	execSQL(t, ctx, s, `ROLLBACK`)

	// A malformed operand errors cleanly.
	if _, serr := trySQL(ctx, s, `SELECT v FROM ledger AS OF SYSTEM TIME 'yesterday'`); serr == nil {
		t.Fatal("bad AS OF operand accepted")
	}
	// A non-negative duration errors (must name a past time).
	if _, serr := trySQL(ctx, s, `SELECT v FROM ledger AS OF SYSTEM TIME '5s'`); serr == nil {
		t.Fatal("future AS OF accepted")
	}
}
