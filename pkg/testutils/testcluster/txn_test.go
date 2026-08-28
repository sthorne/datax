package testcluster

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/kvclient"
	"github.com/sthorne/datax/pkg/kvserver"
)

const bankTable = 300

func acctKey(i int) keys.Key {
	return append(keys.TableDataPrefix(bankTable), fmt.Sprintf("acct%02d", i)...)
}

func readBalance(ctx context.Context, txn *kvclient.Txn, i int) (int64, error) {
	v, err := txn.Get(ctx, acctKey(i))
	if err != nil {
		return 0, err
	}
	if v == nil {
		return 0, fmt.Errorf("account %d missing", i)
	}
	return strconv.ParseInt(string(v), 10, 64)
}

func writeBalance(ctx context.Context, txn *kvclient.Txn, i int, bal int64) error {
	return txn.Put(ctx, acctKey(i), []byte(strconv.FormatInt(bal, 10)))
}

func TestTxnAtomicCommitAcrossRanges(t *testing.T) {
	tc := Start(t, 3)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	db := tc.Nodes[0].DB()

	// Split so the two keys live on different ranges (distributed commit).
	if _, err := db.AdminSplit(ctx, acctKey(1)); err != nil {
		t.Fatalf("split: %v", err)
	}

	txn := db.NewTxn("cross-range")
	if err := writeBalance(ctx, txn, 0, 100); err != nil {
		t.Fatalf("put 0: %v", err)
	}
	if err := writeBalance(ctx, txn, 1, 200); err != nil {
		t.Fatalf("put 1: %v", err)
	}
	// Before commit: another transaction pushing our intents finds us alive
	// and eventually gives up with a retryable error.
	other := tc.Nodes[1].DB().NewTxn("blocked-reader")
	other.SetPriorityForTesting(0) // cannot win pushes
	if _, err := readBalance(ctx, other, 0); err == nil || !kvclient.IsRetryable(err) {
		t.Fatalf("read of uncommitted intent: got %v, want retryable", err)
	}
	_ = other.Rollback(ctx)

	if err := txn.Commit(ctx); err != nil {
		t.Fatalf("commit: %v", err)
	}

	// After commit: both keys visible atomically from another node.
	check := tc.Nodes[2].DB().NewTxn("check")
	b0, err := readBalance(ctx, check, 0)
	if err != nil {
		t.Fatalf("read 0: %v", err)
	}
	b1, err := readBalance(ctx, check, 1)
	if err != nil {
		t.Fatalf("read 1: %v", err)
	}
	if b0 != 100 || b1 != 200 {
		t.Fatalf("balances: %d, %d", b0, b1)
	}
	if err := check.Commit(ctx); err != nil {
		t.Fatalf("read-only commit: %v", err)
	}
}

// TestBankTransfers is the Phase 4 checkpoint: concurrent transfers between
// accounts spanning a range split, from multiple gateway nodes, with the
// invariant that the total balance never changes. Run with -race.
func TestBankTransfers(t *testing.T) {
	tc := Start(t, 3)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	db := tc.Nodes[0].DB()

	const accounts = 8
	const initial = 100
	const workers = 4
	const transfersPerWorker = 8

	if err := db.RunTxn(ctx, "seed", func(ctx context.Context, txn *kvclient.Txn) error {
		for i := 0; i < accounts; i++ {
			if err := writeBalance(ctx, txn, i, initial); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}
	// Split the accounts across two ranges.
	if _, err := db.AdminSplit(ctx, acctKey(accounts/2)); err != nil {
		t.Fatalf("split: %v", err)
	}

	var wg sync.WaitGroup
	errCh := make(chan error, workers+1)
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			gw := tc.Nodes[w%3].DB()
			for i := 0; i < transfersPerWorker; i++ {
				from := (w + i) % accounts
				to := (w + i + 3) % accounts
				if from == to {
					continue
				}
				err := gw.RunTxn(ctx, "transfer", func(ctx context.Context, txn *kvclient.Txn) error {
					fb, err := readBalance(ctx, txn, from)
					if err != nil {
						return err
					}
					tb, err := readBalance(ctx, txn, to)
					if err != nil {
						return err
					}
					if fb < 10 {
						return nil // insufficient funds; skip
					}
					if err := writeBalance(ctx, txn, from, fb-10); err != nil {
						return err
					}
					return writeBalance(ctx, txn, to, tb+10)
				})
				if err != nil {
					errCh <- fmt.Errorf("worker %d transfer %d: %w", w, i, err)
					return
				}
			}
		}(w)
	}

	// A concurrent auditor: the total must be constant in every snapshot.
	auditDone := make(chan struct{})
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer close(auditDone)
		for a := 0; a < 5; a++ {
			err := tc.Nodes[2].DB().RunTxn(ctx, "audit", func(ctx context.Context, txn *kvclient.Txn) error {
				var sum int64
				for i := 0; i < accounts; i++ {
					b, err := readBalance(ctx, txn, i)
					if err != nil {
						return err
					}
					sum += b
				}
				if sum != accounts*initial {
					// A real invariant violation, not a conflict: fail hard.
					t.Errorf("audit %d: total = %d, want %d", a, sum, accounts*initial)
				}
				return nil
			})
			if err != nil {
				errCh <- fmt.Errorf("audit %d: %w", a, err)
				return
			}
			time.Sleep(100 * time.Millisecond)
		}
	}()

	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatal(err)
	}

	// Final total.
	err := db.RunTxn(ctx, "final", func(ctx context.Context, txn *kvclient.Txn) error {
		var sum int64
		for i := 0; i < accounts; i++ {
			b, err := readBalance(ctx, txn, i)
			if err != nil {
				return err
			}
			sum += b
		}
		if sum != accounts*initial {
			t.Fatalf("final total = %d, want %d", sum, accounts*initial)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("final audit: %v", err)
	}
}

// TestCrashedCoordinatorCleanup: a transaction whose coordinator dies leaves
// intents behind; its record expires, and the next writer aborts it and
// cleans up. The crashed transaction can never commit afterwards.
func TestCrashedCoordinatorCleanup(t *testing.T) {
	tc := Start(t, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	db := tc.Nodes[0].DB()

	key := append(keys.TableDataPrefix(400), "victim"...)
	if err := db.RunTxn(ctx, "seed", func(ctx context.Context, txn *kvclient.Txn) error {
		return txn.Put(ctx, key, []byte("original"))
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	crashed := db.NewTxn("doomed")
	if err := crashed.Put(ctx, key, []byte("uncommitted")); err != nil {
		t.Fatalf("intent write: %v", err)
	}
	crashed.AbandonForTesting()

	// Wait past the record expiry so pushers may abort it.
	time.Sleep(kvserver.TxnExpiration + time.Second)

	winner := db.NewTxn("winner")
	winner.SetPriorityForTesting(0) // wins only via expiry, not priority
	v, err := winner.Get(ctx, key)
	if err != nil {
		t.Fatalf("read over abandoned intent: %v", err)
	}
	if string(v) != "original" {
		t.Fatalf("read %q, want the committed value", v)
	}
	if err := winner.Put(ctx, key, []byte("recovered")); err != nil {
		t.Fatalf("write over abandoned intent: %v", err)
	}
	if err := winner.Commit(ctx); err != nil {
		t.Fatalf("commit: %v", err)
	}

	// The abandoned transaction was aborted; its commit must fail.
	if err := crashed.Commit(ctx); err == nil {
		t.Fatal("crashed coordinator's commit unexpectedly succeeded")
	}

	final, err := db.Get(ctx, key)
	if err != nil {
		t.Fatalf("final read: %v", err)
	}
	if string(final) != "recovered" {
		t.Fatalf("final value %q", final)
	}
}
