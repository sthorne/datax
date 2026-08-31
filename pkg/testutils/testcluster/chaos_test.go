package testcluster

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sthorne/datax/pkg/kvclient"
	"github.com/sthorne/datax/pkg/server"
)

// The chaos suite (issue #54): each scenario runs the bank workload —
// concurrent transfers whose total is invariant — through a fault, then
// proves integrity twice over: the SQL-visible invariant holds, and the
// consistency checker finds every replica byte-identical.

const chaosAccounts = 8
const chaosSeed = 1000

func chaosSeedAccounts(t *testing.T, ctx context.Context, db *kvclient.DB) {
	t.Helper()
	for i := 0; i < chaosAccounts; i++ {
		txn := db.NewTxn("seed")
		if err := writeBalance(ctx, txn, i, chaosSeed); err != nil {
			t.Fatal(err)
		}
		if err := txn.Commit(ctx); err != nil {
			t.Fatal(err)
		}
	}
}

// chaosWorkers runs transfer workers against the given gateways until stop
// flips; transfers that fail (partition, retry exhaustion) roll back —
// atomicity, not availability, is what the invariant tests.
func chaosWorkers(tc *TestCluster, stop *atomic.Bool, wg *sync.WaitGroup, gateways []int) {
	for w, g := range gateways {
		wg.Add(1)
		go func(w, g int) {
			defer wg.Done()
			for i := 0; !stop.Load(); i++ {
				n := tc.Nodes[g]
				if n == nil {
					continue // node is down in this scenario
				}
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				from, to := (w+i)%chaosAccounts, (w*3+i+1)%chaosAccounts
				if from == to {
					cancel()
					continue
				}
				_ = n.DB().RunTxn(ctx, "chaos-transfer", func(ctx context.Context, txn *kvclient.Txn) error {
					fb, err := readBalance(ctx, txn, from)
					if err != nil {
						return err
					}
					tb, err := readBalance(ctx, txn, to)
					if err != nil {
						return err
					}
					if err := writeBalance(ctx, txn, from, fb-3); err != nil {
						return err
					}
					return writeBalance(ctx, txn, to, tb+3)
				})
				cancel()
			}
		}(w, g)
	}
}

// chaosVerify asserts the bank total and replica consistency on range 1.
func chaosVerify(t *testing.T, ctx context.Context, tc *TestCluster) {
	t.Helper()
	var total int64
	deadline := time.Now().Add(30 * time.Second)
	for {
		txn := tc.Nodes[tc.LeaderIndex(1)].DB().NewTxn("verify")
		total = 0
		var err error
		for i := 0; i < chaosAccounts; i++ {
			var b int64
			if b, err = readBalance(ctx, txn, i); err != nil {
				break
			}
			total += b
		}
		if err == nil {
			if cerr := txn.Commit(ctx); cerr == nil {
				break
			}
		} else {
			_ = txn.Rollback(ctx)
		}
		if time.Now().After(deadline) {
			t.Fatalf("could not read balances: %v", err)
		}
	}
	if total != chaosAccounts*chaosSeed {
		t.Fatalf("bank invariant broken: total %d, want %d", total, chaosAccounts*chaosSeed)
	}
	leader := tc.LeaderIndex(1)
	mismatch, err := tc.Nodes[leader].CheckRangeConsistency(ctx, 1)
	if err != nil {
		t.Fatalf("consistency check: %v", err)
	}
	if mismatch {
		t.Fatal("replicas diverged")
	}
}

// TestChaosPartitionLeader: the range leader is partitioned away mid-load;
// the survivors elect a new leader and transfers continue; after healing,
// the invariant holds and all three replicas are byte-identical.
func TestChaosPartitionLeader(t *testing.T) {
	tc, _ := StartWithEngines(t, 3)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	chaosSeedAccounts(t, ctx, tc.Nodes[0].DB())

	var stop atomic.Bool
	var wg sync.WaitGroup
	chaosWorkers(tc, &stop, &wg, []int{0, 1, 2, 0})

	time.Sleep(500 * time.Millisecond)
	victim := tc.LeaderIndex(1)
	tc.Isolate(victim)
	// Long enough for an election (1s timeout at the 100ms tick) plus real
	// post-failover traffic on the new leader.
	time.Sleep(4 * time.Second)
	tc.Heal()
	time.Sleep(2 * time.Second) // let the healed node catch up
	stop.Store(true)
	wg.Wait()

	chaosVerify(t, ctx, tc)
}

// TestChaosCrashRestart: a node crashes mid-load (process gone, engine
// retained), transfers continue on the survivors, the node restarts and
// catches up; invariant + byte-identical replicas.
func TestChaosCrashRestart(t *testing.T) {
	tc, engines := StartWithEngines(t, 3)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	chaosSeedAccounts(t, ctx, tc.Nodes[0].DB())

	victim := tc.LeaderIndex(1)
	live := []int{(victim + 1) % 3, (victim + 2) % 3, (victim + 1) % 3}
	var stop atomic.Bool
	var wg sync.WaitGroup
	chaosWorkers(tc, &stop, &wg, live)

	time.Sleep(500 * time.Millisecond)
	tc.StopNode(victim)
	time.Sleep(3 * time.Second)
	tc.RestartNode(victim, engines[victim])
	time.Sleep(2 * time.Second)
	stop.Store(true)
	wg.Wait()

	chaosVerify(t, ctx, tc)
}

// TestChaosStorageOverload: one node's engine reports overloaded mid-load —
// its leaders shed table-data writes with retryable errors that the txn
// retry loop absorbs; when the pressure clears, everything reconciles.
func TestChaosStorageOverload(t *testing.T) {
	var overloaded atomic.Bool
	tc, _ := StartWithEngines(t, 3, func(c *server.Config) {
		id := c.StaticBootstrap.NodeID
		c.TestingKnobs.OverrideOverloaded = func() (bool, string) {
			return id == 2 && overloaded.Load(), "chaos: injected overload"
		}
	})
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	chaosSeedAccounts(t, ctx, tc.Nodes[0].DB())

	var stop atomic.Bool
	var wg sync.WaitGroup
	chaosWorkers(tc, &stop, &wg, []int{0, 1, 2})

	time.Sleep(500 * time.Millisecond)
	overloaded.Store(true)
	time.Sleep(3 * time.Second)
	overloaded.Store(false)
	time.Sleep(1 * time.Second)
	stop.Store(true)
	wg.Wait()

	chaosVerify(t, ctx, tc)
}
