package testcluster

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/kvclient"
	"github.com/sthorne/datax/pkg/kvpb"
	"github.com/sthorne/datax/pkg/storage/enginepb"
)

// abortedReadOutcome is what one transaction of the same-key program
// below saw.
type abortedReadOutcome struct {
	wrote     bool  // the write statement reported a row
	readOwn   bool  // the read of the key returned the row it wrote
	stmtErr   error // a statement (or BEGIN) failed; COMMIT was not sent
	commitErr error
}

// sameKeyRound runs one round of issue #320's program: n transactions at
// once, each BEGIN; <write> on key; SELECT v FROM s4 WHERE k = key;
// COMMIT, and returns what each saw.
func sameKeyRound(ctx context.Context, pool *pgxpool.Pool, n int, write, key string) []abortedReadOutcome {
	out := make([]abortedReadOutcome, n)
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			o := &out[i]
			conn, err := pool.Acquire(ctx)
			if err != nil {
				o.stmtErr = err
				return
			}
			defer conn.Release()
			<-start
			tx, err := conn.Begin(ctx)
			if err != nil {
				o.stmtErr = err
				return
			}
			defer func() { _ = tx.Rollback(context.Background()) }()
			mine := fmt.Sprintf("w%d", i)
			tag, err := tx.Exec(ctx, write, key, mine)
			if err != nil {
				o.stmtErr = err // a write that fails is not the anomaly
				return
			}
			o.wrote = tag.RowsAffected() == 1
			var v *string
			err = tx.QueryRow(ctx, `SELECT v FROM s4 WHERE k = $1`, key).Scan(&v)
			switch {
			case errors.Is(err, pgx.ErrNoRows):
			case err != nil:
				o.stmtErr = err
				return
			default:
				o.readOwn = v != nil && *v == mine
			}
			o.commitErr = tx.Commit(ctx)
		}(i)
	}
	close(start)
	wg.Wait()
	return out
}

// TestTxnAbortedReadsOwnWrites (issue #320): four transactions at once
// write one new key (INSERT ... ON CONFLICT DO NOTHING, UPSERT, plain
// INSERT) and read it back before committing. A transaction aborted by a
// stronger pusher had its intent resolved — removed — by the pusher, and
// went on answering: its read of the key it had just written found
// nothing, and only its COMMIT failed (36 of 200 transactions in the
// report). From cluster version v22 the pusher's resolution leaves an
// abort-span entry on the range, and the aborted transaction's next
// request there fails with 40001 instead: no transaction may see its
// write reported and then miss it.
func TestTxnAbortedReadsOwnWrites(t *testing.T) {
	tc := Start(t, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	pool, err := pgxpool.New(ctx, pgURL(tc, 0)+"&pool_max_conns=4&pool_min_conns=4")
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	if _, err := pool.Exec(ctx, `CREATE TABLE s4 (k TEXT PRIMARY KEY, v TEXT)`); err != nil {
		t.Fatal(err)
	}
	rounds := 50
	if raceEnabled || testing.Short() {
		rounds = 15
	}
	for _, c := range []struct{ name, write string }{
		{"on conflict do nothing", `INSERT INTO s4 (k, v) VALUES ($1, $2) ON CONFLICT (k) DO NOTHING`},
		{"upsert", `UPSERT INTO s4 (k, v) VALUES ($1, $2)`},
		{"insert", `INSERT INTO s4 (k, v) VALUES ($1, $2)`},
	} {
		t.Run(c.name, func(t *testing.T) {
			var txns, missed, failed, committed int
			codes := map[string]int{}
			for r := 0; r < rounds; r++ {
				key := fmt.Sprintf("%s-%d", c.name, r)
				for _, o := range sameKeyRound(ctx, pool, 4, c.write, key) {
					txns++
					if o.wrote && o.stmtErr == nil && !o.readOwn {
						missed++
						t.Errorf("round %d: the write reported a row, the transaction's own read of %q did not return it (commit: %v)", r, key, o.commitErr)
					}
					if o.stmtErr != nil {
						if s := sqlState(o.stmtErr); s != "40001" && s != "23505" {
							t.Errorf("round %d: statement failed with %v (SQLSTATE %q), want 40001 or 23505", r, o.stmtErr, s)
						}
						failed++
						codes["stmt "+sqlState(o.stmtErr)]++
					}
					if o.stmtErr == nil {
						if o.commitErr == nil {
							committed++
						} else if s := sqlState(o.commitErr); s != "40001" {
							t.Errorf("round %d: commit failed with %v (SQLSTATE %q), want 40001", r, o.commitErr, s)
						} else {
							codes["commit "+s]++
						}
					}
				}
			}
			t.Logf("%s: %d transactions, %d committed, %d failed a statement, %d missed their own write; failures %v", c.name, txns, committed, failed, missed, codes)
			if committed == 0 {
				t.Errorf("no transaction committed in %d rounds", rounds)
			}
		})
	}
}

// TestTxnAbortedByPusherFailsNextRequest (issue #320), at the KV layer
// and deterministically: a stronger writer aborts a transaction and
// removes its intent, then rolls back itself, so nothing is left on the
// key. The aborted transaction's next read of the key it wrote must fail
// with a retryable error, not answer that the key is empty — a point
// read, a scan, and a locking read alike.
func TestTxnAbortedByPusherFailsNextRequest(t *testing.T) {
	tc := Start(t, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	db := tc.Nodes[0].DB()
	key := func(k string) keys.Key { return append(keys.TableDataPrefix(432), k...) }

	for _, c := range []struct {
		name string
		read func(ctx context.Context, txn *kvclient.Txn, k keys.Key) ([]byte, error)
	}{
		{"get", func(ctx context.Context, txn *kvclient.Txn, k keys.Key) ([]byte, error) {
			return txn.Get(ctx, k)
		}},
		{"scan", func(ctx context.Context, txn *kvclient.Txn, k keys.Key) ([]byte, error) {
			kvs, err := txn.Scan(ctx, k, k.PrefixEnd(), 0)
			if err != nil || len(kvs) == 0 {
				return nil, err
			}
			return kvs[0].Value, nil
		}},
		{"get for update", func(ctx context.Context, txn *kvclient.Txn, k keys.Key) ([]byte, error) {
			return txn.GetForUpdate(ctx, k)
		}},
	} {
		t.Run(c.name, func(t *testing.T) {
			k := key(c.name)
			victim := db.NewTxn("victim")
			victim.TestingSetPriority(1)
			defer func() { _ = victim.Rollback(ctx) }()
			if err := victim.Put(ctx, k, []byte("mine")); err != nil {
				t.Fatal(err)
			}
			// A stronger writer of the key aborts the victim and resolves
			// its intent away, then gives up itself.
			bully := db.NewTxn("bully")
			bully.TestingSetPriority(1 << 20)
			if err := bully.Put(ctx, k, []byte("theirs")); err != nil {
				t.Fatal(err)
			}
			if err := bully.Rollback(ctx); err != nil {
				t.Fatal(err)
			}
			v, err := c.read(ctx, victim, k)
			if err == nil {
				t.Fatalf("aborted transaction read %q of the key it wrote, want a retryable error", v)
			}
			if !kvclient.IsRetryable(err) {
				t.Fatalf("aborted transaction's read: %v, want a retryable error", err)
			}
		})
	}
}

// TestTxnAbortedLearnedFromHeartbeat (issue #320): once the coordinator
// learns from its heartbeat that its transaction was aborted, every
// further request fails at once — also on a range where nothing of the
// transaction was removed, which has no abort-span entry to refuse it.
func TestTxnAbortedLearnedFromHeartbeat(t *testing.T) {
	tc := Start(t, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	db := tc.Nodes[0].DB()
	k := append(keys.TableDataPrefix(434), "k"...)
	elsewhere := keys.TableDataPrefix(435)
	if _, err := db.AdminSplit(ctx, elsewhere); err != nil {
		t.Fatal(err)
	}
	victim := db.NewTxn("victim")
	victim.TestingSetPriority(1)
	defer func() { _ = victim.Rollback(ctx) }()
	if err := victim.Put(ctx, k, []byte("mine")); err != nil {
		t.Fatal(err)
	}
	bully := db.NewTxn("bully")
	bully.TestingSetPriority(1 << 20)
	if err := bully.Put(ctx, k, []byte("theirs")); err != nil {
		t.Fatal(err)
	}
	if err := bully.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	// The heartbeat runs every second; within a few the coordinator
	// knows.
	deadline := time.Now().Add(5 * time.Second)
	for {
		_, err := victim.Get(ctx, elsewhere)
		if kvclient.IsRetryable(err) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		if time.Now().After(deadline) {
			t.Fatal("the aborted transaction kept reading for 5s after its record was aborted")
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// TestTxnAbortSpanAcrossSplitAndMerge (issue #320): the abort-span entry
// a pusher leaves follows the keys through a real split of the range —
// the half that holds the removed write refuses the aborted transaction
// — and an entry the right-hand half took is carried into the range a
// merge folds it into. The requests are sent as the aborted
// transactions' raw batches: a coordinator would refuse them itself
// after the first refusal, and clean up its entries.
func TestTxnAbortSpanAcrossSplitAndMerge(t *testing.T) {
	tc := Start(t, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	db := tc.Nodes[0].DB()
	prefix := keys.TableDataPrefix(433)
	k := append(prefix.Clone(), "m-key"...)
	if _, err := db.AdminSplit(ctx, prefix); err != nil {
		t.Fatal(err)
	}
	send := func(ba *kvpb.BatchRequest) *kvpb.Error {
		t.Helper()
		_, kerr := db.Send(ctx, ba)
		return kerr
	}
	// abort writes key in a new transaction, then aborts it and resolves
	// its intent away as a pusher does.
	abort := func(key keys.Key) *kvpb.Transaction {
		t.Helper()
		victim := kvpb.NewTransaction("victim", 1, db.Clock().Now())
		victim.Key = key
		victim.BinaryMeta = true
		w := &kvpb.BatchRequest{Header: kvpb.BatchHeader{Txn: victim, CreateTxnRecord: true}}
		w.Add(&kvpb.PutRequest{RequestHeader: kvpb.RequestHeader{Key: key}, Value: []byte("mine")})
		if kerr := send(w); kerr != nil {
			t.Fatal(kerr)
		}
		p := &kvpb.BatchRequest{Header: kvpb.BatchHeader{Timestamp: db.Clock().Now()}}
		p.Add(&kvpb.PushTxnRequest{RequestHeader: kvpb.RequestHeader{Key: key}, PusheeTxn: victim.TxnMeta, ForceAbort: true, Now: db.Clock().Now()})
		if kerr := send(p); kerr != nil {
			t.Fatal(kerr)
		}
		r := &kvpb.BatchRequest{Header: kvpb.BatchHeader{Timestamp: db.Clock().Now()}}
		r.Add(&kvpb.ResolveIntentRequest{RequestHeader: kvpb.RequestHeader{Key: key}, TxnID: victim.ID, Status: enginepb.ABORTED, Poison: true})
		if kerr := send(r); kerr != nil {
			t.Fatal(kerr)
		}
		return victim
	}
	refused := func(when string, victim *kvpb.Transaction, key keys.Key) {
		t.Helper()
		g := &kvpb.BatchRequest{Header: kvpb.BatchHeader{Txn: victim}}
		g.Add(&kvpb.GetRequest{RequestHeader: kvpb.RequestHeader{Key: key}})
		if kerr := send(g); kerr == nil || kerr.TxnAborted == nil {
			t.Fatalf("%s: the aborted transaction's read: %v, want a TxnAbortedError", when, kerr)
		}
	}

	first := abort(k)
	refused("before the split", first, k)
	split, err := db.AdminSplit(ctx, k)
	if err != nil {
		t.Fatal(err)
	}
	if !split.Right.StartKey.Equal(k) {
		t.Fatalf("split produced %+v", split)
	}
	refused("on the right-hand half", first, k)

	// An abort the right-hand half alone records...
	k2 := append(k.Clone(), "-2"...)
	second := abort(k2)
	refused("on the right-hand half", second, k2)
	merged, err := db.AdminMerge(ctx, prefix)
	if err != nil {
		t.Fatal(err)
	}
	if merged.Desc.RangeID != split.Left.RangeID || !merged.Desc.ContainsKey(k2) {
		t.Fatalf("the merge produced %+v, want %s holding %s", merged.Desc, split.Left.RangeID, k2)
	}
	// ...is the merged range's.
	refused("after the merge", second, k2)
	refused("after the merge", first, k)
}
