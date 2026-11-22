package kvclient

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/sthorne/datax/pkg/base"
	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/kvpb"
	"github.com/sthorne/datax/pkg/util/hlc"
)

// shedStore is a LocalStore of two ranges: r5 applies every batch, r6
// sheds every write as overloaded. It records the transaction-ending and
// intent-removing requests it is sent.
type shedStore struct {
	mu        sync.Mutex
	ends      []*kvpb.EndTxnRequest
	rollbacks []string // keys of RollbackIntent requests
}

func (s *shedStore) NodeID() base.NodeID { return 1 }

func (s *shedStore) ExecuteBatch(_ context.Context, ba *kvpb.BatchRequest) (*kvpb.BatchResponse, *kvpb.Error) {
	if ba.Header.RangeID == 6 && ba.HasMVCCWrites() {
		e := kvpb.NewErrorf("r6: storage overloaded, write shed: test")
		e.StorageOverloaded = &kvpb.StorageOverloadedError{}
		e.TxnRetry = &kvpb.TxnRetryError{}
		return nil, e
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	br := &kvpb.BatchResponse{Txn: ba.Header.Txn}
	for _, u := range ba.Requests {
		var ru kvpb.ResponseUnion
		switch {
		case u.Put != nil:
			ru.Put = &kvpb.PutResponse{}
		case u.Get != nil:
			ru.Get = &kvpb.GetResponse{}
		case u.EndTxn != nil:
			s.ends = append(s.ends, u.EndTxn)
			ru.EndTxn = &kvpb.EndTxnResponse{}
		case u.HeartbeatTxn != nil:
			ru.HeartbeatTxn = &kvpb.HeartbeatTxnResponse{}
		case u.RollbackIntent != nil:
			s.rollbacks = append(s.rollbacks, string(u.RollbackIntent.Key))
			ru.RollbackIntent = &kvpb.RollbackIntentResponse{}
		case u.ResolveIntent != nil:
			ru.ResolveIntent = &kvpb.ResolveIntentResponse{}
		}
		br.Responses = append(br.Responses, ru)
	}
	return br, nil
}

func (s *shedStore) endTxns() []*kvpb.EndTxnRequest {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]*kvpb.EndTxnRequest(nil), s.ends...)
}

func (s *shedStore) rolledBack(k keys.Key) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, r := range s.rollbacks {
		if r == string(k) {
			return true
		}
	}
	return false
}

// newShedStoreDB returns a DB over a shedStore and a key on each range:
// ok on r5, shed on r6.
func newShedStoreDB() (*DB, *shedStore, keys.Key, keys.Key) {
	a, b := keys.TableDataPrefix(910), keys.TableDataPrefix(911)
	repl := []kvpb.ReplicaDescriptor{{NodeID: 1, StoreID: 1, ReplicaID: 1}}
	store := &shedStore{}
	db := NewDB(store, nil, hlc.NewClock(nil, 0))
	db.SetOverloadRetryBudget(-1) // return the first shed at once
	db.SeedDescriptor(kvpb.RangeDescriptor{RangeID: 5, Generation: 1, StartKey: a, EndKey: a.PrefixEnd(), Replicas: repl})
	db.SeedDescriptor(kvpb.RangeDescriptor{RangeID: 6, Generation: 1, StartKey: b, EndKey: b.PrefixEnd(), Replicas: repl})
	return db, store, append(a.Clone(), "ok"...), append(b.Clone(), "shed"...)
}

// A batch that was to create the record and failed for a reason other
// than a duplicate key — its record's part sent first, alone, and applied
// before the other range shed the write — leaves a record that may
// exist: COMMIT and ROLLBACK must end it rather than take the transaction
// for read-only, and the batch's keys are tracked as written.
func TestFailedBatchMayHaveCreatedTheRecord(t *testing.T) {
	for _, commit := range []bool{true, false} {
		db, store, ok, shed := newShedStoreDB()
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		txn := db.NewTxn("partial")
		var wb WriteBatch
		wb.Put(ok, []byte("a"))
		wb.Put(shed, []byte("b"))
		if err := txn.RunBatch(ctx, &wb); !IsStorageOverloaded(err) {
			t.Fatalf("batch over a shedding range: got %v, want the shed", err)
		}
		if !txn.HasWritten(ok) || !txn.HasWritten(shed) {
			t.Fatal("the failed batch's keys are not tracked as possibly written")
		}
		var err error
		if commit {
			err = txn.Commit(ctx)
		} else {
			err = txn.Rollback(ctx)
		}
		cancel()
		if err != nil {
			t.Fatalf("commit=%v: %v", commit, err)
		}
		ends := store.endTxns()
		if len(ends) != 1 || ends[0].Commit != commit || !ends[0].Key.Equal(ok) {
			t.Fatalf("commit=%v: EndTxn requests %+v, want one ending the record at the anchor %q", commit, ends, ok)
		}
	}
}

// In an anchored transaction, a range of a failed batch that applied is
// rolled back by ROLLBACK TO SAVEPOINT.
func TestFailedBatchRolledBackBySavepoint(t *testing.T) {
	db, store, ok, shed := newShedStoreDB()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	txn := db.NewTxn("partial")
	defer func() { _ = txn.Rollback(ctx) }()
	anchor := append(ok.Clone(), "-anchor"...)
	if err := txn.Put(ctx, anchor, []byte("kept")); err != nil {
		t.Fatal(err)
	}
	if err := txn.Savepoint("sp"); err != nil {
		t.Fatal(err)
	}
	var wb WriteBatch
	wb.Put(ok, []byte("a"))
	wb.Put(shed, []byte("b"))
	if err := txn.RunBatch(ctx, &wb); !IsStorageOverloaded(err) {
		t.Fatalf("batch over a shedding range: got %v, want the shed", err)
	}
	if err := txn.RollbackToSavepoint(ctx, "sp"); err != nil {
		t.Fatal(err)
	}
	if !store.rolledBack(ok) {
		t.Fatal("ROLLBACK TO SAVEPOINT left the intent of a range of the failed batch that applied")
	}
	if txn.HasWritten(ok) || !txn.HasWritten(anchor) {
		t.Fatal("the savepoint's write set was not restored")
	}
}
