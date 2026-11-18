package testcluster

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/kvclient"
	"github.com/sthorne/datax/pkg/kvpb"
	"github.com/sthorne/datax/pkg/sql"
	"github.com/sthorne/datax/pkg/sql/catalog"
	"github.com/sthorne/datax/pkg/storage/enginepb"
	"github.com/sthorne/datax/pkg/util/hlc"
)

// TestTxnRecordOpsBelowGCThreshold (issue #374): the requests that work
// on transaction records and a transaction's own intents read no MVCC
// history at their batch's timestamp, so a GC threshold that has passed
// it is nothing to them. Refusing them was how an ordinary transaction
// reading alongside writers and ALTER TABLE ... PURGE got a raw
// GC-threshold error (72000) at its first read: the push or the intent
// resolution its read ran into a writer's intent with had been stamped
// before the purge moved the threshold. It also left the record of a
// transaction that rolled back after a purge PENDING.
func TestTxnRecordOpsBelowGCThreshold(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	tc := Start(t, 1)
	n := tc.Nodes[0]
	db := n.DB()
	s := sql.NewSession(db, catalog.NewAccessor())
	waitForDatabases(t, ctx, s)
	execSQL(t, ctx, s, `CREATE TABLE g (id INT8 PRIMARY KEY, v TEXT)`)
	execSQL(t, ctx, s, `INSERT INTO g VALUES (1, 'a')`)
	execSQL(t, ctx, s, `UPDATE g SET v = 'b' WHERE id = 1`) // 'a' is now collectable
	lo, _ := keys.TableDataSpan(tableID(t, ctx, n, "g"))
	k := append(lo.Clone(), "issue374"...)

	// A transaction from before the purge, with a write (its record).
	old := db.NewTxn("old")
	if err := old.Put(ctx, k, []byte("x")); err != nil {
		t.Fatal(err)
	}
	before := old.ReadTimestamp()
	execSQL(t, ctx, s, `ALTER TABLE g PURGE`)

	send := func(ts hlc.Timestamp, req kvpb.Request) (*kvpb.BatchResponse, *kvpb.Error) {
		ba := &kvpb.BatchRequest{Header: kvpb.BatchHeader{Timestamp: ts}}
		ba.Add(req)
		return db.Send(ctx, ba)
	}
	// The threshold has passed 'before': a read there is refused.
	if _, kerr := send(before, &kvpb.GetRequest{RequestHeader: kvpb.RequestHeader{Key: lo}}); !kerr.IsBelowGCThreshold() {
		t.Fatalf("a read at the old transaction's timestamp after the purge: %v, want a GC-threshold refusal", kerr)
	}
	// The intent names the old transaction.
	_, kerr := send(db.Clock().Now(), &kvpb.GetRequest{RequestHeader: kvpb.RequestHeader{Key: k}})
	if kerr == nil || kerr.WriteIntent == nil || len(kerr.WriteIntent.Intents) == 0 {
		t.Fatalf("a read of the old transaction's write: %v, want its intent", kerr)
	}
	meta := kerr.WriteIntent.Intents[0].Txn
	status := func(ts hlc.Timestamp) (enginepb.TxnStatus, *kvpb.Error) {
		br, kerr := send(ts, &kvpb.PushTxnRequest{
			RequestHeader: kvpb.RequestHeader{Key: keys.Key(meta.Key).Clone()},
			PusheeTxn:     meta,
			QueryOnly:     true,
			Now:           ts,
		})
		if kerr != nil {
			return 0, kerr
		}
		return br.Responses[0].PushTxn.Status, nil
	}

	// Stamped below the threshold, as a push or a resolution built before
	// the purge is when it arrives after it.
	if st, kerr := status(before); kerr != nil || st != enginepb.PENDING {
		t.Errorf("a push stamped below the threshold: %v, %v; want PENDING", st, kerr)
	}
	if _, kerr := send(before, &kvpb.ResolveIntentRequest{RequestHeader: kvpb.RequestHeader{Key: lo}, TxnID: uuid.New(), Status: enginepb.ABORTED}); kerr != nil {
		t.Errorf("an intent resolution stamped below the threshold: %v", kerr)
	}
	if _, kerr := send(before, &kvpb.RollbackIntentRequest{RequestHeader: kvpb.RequestHeader{Key: lo}, TxnID: uuid.New()}); kerr != nil {
		t.Errorf("a savepoint's intent rewind stamped below the threshold: %v", kerr)
	}

	// The old transaction gives up: its abort carries its own timestamp.
	if err := old.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	if st, kerr := status(db.Clock().Now()); kerr != nil || st != enginepb.ABORTED {
		t.Errorf("the record of a transaction rolled back after the purge: %v, %v; want ABORTED", st, kerr)
	}

	// A commit is not exempt: the transaction restarts.
	old2 := db.NewTxn("old2")
	k2 := append(lo.Clone(), "issue374-2"...)
	if err := old2.Put(ctx, k2, []byte("x")); err != nil {
		t.Fatal(err)
	}
	execSQL(t, ctx, s, `UPDATE g SET v = 'c' WHERE id = 1`)
	execSQL(t, ctx, s, `ALTER TABLE g PURGE`)
	if err := old2.Commit(ctx); !kvclient.IsRetryable(err) {
		t.Fatalf("a commit below the threshold: %v, want a restart", err)
	}
}

// TestRefusedCommitReleasesIntents (issue #374): a commit refused because
// a purge passed the transaction's timestamp (40001) is aborted on the
// spot (issue #315), so a writer queued on its intents goes ahead at
// once. The abort carries the same timestamp the commit did; while the
// range refused it too, the intents stayed until the record expired
// (kvserver.TxnExpiration, 5s).
func TestRefusedCommitReleasesIntents(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	tc := Start(t, 1)
	n := tc.Nodes[0]
	admin := sql.NewSession(n.DB(), catalog.NewAccessor())
	waitForDatabases(t, ctx, admin)
	execSQL(t, ctx, admin, `CREATE TABLE r (id INT8 PRIMARY KEY, v TEXT)`)
	execSQL(t, ctx, admin, `INSERT INTO r VALUES (1, 'a'), (2, 'a')`)

	a := sql.NewSession(n.DB(), catalog.NewAccessor())
	execSQL(t, ctx, a, `BEGIN`)
	execSQL(t, ctx, a, `UPDATE r SET v = 'x' WHERE id = 1`)
	execSQL(t, ctx, admin, `UPDATE r SET v = 'b' WHERE id = 2`)
	execSQL(t, ctx, admin, `ALTER TABLE r PURGE`)
	if _, serr := trySQL(ctx, a, `COMMIT`); serr == nil || serr.Code != sql.CodeSerializationFailure {
		t.Fatalf("COMMIT of a transaction the purge passed: %v, want %s", serr, sql.CodeSerializationFailure)
	}

	start := time.Now()
	execSQL(t, ctx, admin, `UPDATE r SET v = 'y' WHERE id = 1`)
	if took := time.Since(start); took > 2*time.Second {
		t.Fatalf("a write of the refused transaction's row waited %s for its intent", took)
	}
	if res := execSQL(t, ctx, admin, `SELECT v FROM r WHERE id = 1`); res.Rows[0][0].S != "y" {
		t.Fatalf("row 1: %+v", res.Rows)
	}
}
