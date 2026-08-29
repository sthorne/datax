package main

import (
	"context"
	"flag"
	"fmt"
	"math/rand"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"
)

// runBench drives load against a running cluster over the PostgreSQL wire
// protocol — the same path real applications use.
func runBench(args []string) error {
	fs := flag.NewFlagSet("bench", flag.ContinueOnError)
	url := fs.String("url", "postgres://root@127.0.0.1:26433/datax?sslmode=disable", "database URL")
	concurrency := fs.Int("concurrency", 16, "concurrent workers")
	duration := fs.Duration("duration", 10*time.Second, "how long to run")
	readPct := fs.Int("read-pct", 95, "kv workload: percentage of reads")
	preload := fs.Int("preload", 1000, "rows (kv) or accounts (bank) to preload")
	forUpdate := fs.Bool("for-update", false, "bank workload: read balances with SELECT ... FOR UPDATE")
	fs.Usage = func() {
		fmt.Fprintf(fs.Output(), "Usage: datax bench <kv|bank> [flags]\n\n")
		fs.PrintDefaults()
	}
	if len(args) == 0 || strings.HasPrefix(args[0], "-") {
		fs.Usage()
		return fmt.Errorf("bench requires a workload: kv or bank")
	}
	workload := args[0]
	if err := fs.Parse(args[1:]); err != nil {
		return err
	}
	if workload != "kv" && workload != "bank" {
		return fmt.Errorf("unknown workload %q (want kv or bank)", workload)
	}

	ctx := context.Background()
	connect := func() (*pgx.Conn, error) {
		cfg, err := pgx.ParseConfig(*url)
		if err != nil {
			return nil, err
		}
		cfg.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol
		return pgx.ConnectConfig(ctx, cfg)
	}

	setup, err := connect()
	if err != nil {
		return fmt.Errorf("connecting to %s: %w", *url, err)
	}
	switch workload {
	case "kv":
		if _, err := setup.Exec(ctx, "CREATE TABLE IF NOT EXISTS bench_kv (k INT PRIMARY KEY, v TEXT)"); err != nil {
			return err
		}
		fmt.Printf("preloading %d rows...\n", *preload)
		for i := 0; i < *preload; i += 100 {
			var vals []string
			for j := i; j < i+100 && j < *preload; j++ {
				vals = append(vals, fmt.Sprintf("(%d, 'v%d')", j, j))
			}
			if _, err := setup.Exec(ctx, "INSERT INTO bench_kv (k, v) VALUES "+strings.Join(vals, ", ")); err != nil && !strings.Contains(err.Error(), "duplicate") {
				return err
			}
		}
	case "bank":
		if _, err := setup.Exec(ctx, "CREATE TABLE IF NOT EXISTS bench_bank (id INT PRIMARY KEY, balance INT)"); err != nil {
			return err
		}
		fmt.Printf("preloading %d accounts...\n", *preload)
		for i := 0; i < *preload; i += 100 {
			var vals []string
			for j := i; j < i+100 && j < *preload; j++ {
				vals = append(vals, fmt.Sprintf("(%d, 1000)", j))
			}
			if _, err := setup.Exec(ctx, "INSERT INTO bench_bank (id, balance) VALUES "+strings.Join(vals, ", ")); err != nil && !strings.Contains(err.Error(), "duplicate") {
				return err
			}
		}
	}
	_ = setup.Close(ctx)

	var ops, errs, retries atomic.Int64
	var latMu sync.Mutex
	var lats []time.Duration

	recordLatency := func(d time.Duration) {
		latMu.Lock()
		if len(lats) < 200000 {
			lats = append(lats, d)
		}
		latMu.Unlock()
	}

	deadline := time.Now().Add(*duration)
	var wg sync.WaitGroup
	fmt.Printf("running %s: %d workers for %s...\n", workload, *concurrency, *duration)
	for w := 0; w < *concurrency; w++ {
		wg.Add(1)
		go func(seed int64) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(seed))
			conn, err := connect()
			if err != nil {
				errs.Add(1)
				return
			}
			defer func() { _ = conn.Close(ctx) }()
			for time.Now().Before(deadline) {
				start := time.Now()
				var err error
				switch workload {
				case "kv":
					if rng.Intn(100) < *readPct {
						var v string
						err = conn.QueryRow(ctx, fmt.Sprintf("SELECT v FROM bench_kv WHERE k = %d", rng.Intn(*preload))).Scan(&v)
						if err == pgx.ErrNoRows {
							err = nil
						}
					} else {
						_, err = conn.Exec(ctx, fmt.Sprintf("UPDATE bench_kv SET v = 'u%d' WHERE k = %d", rng.Int63(), rng.Intn(*preload)))
					}
				case "bank":
					err = bankTransfer(ctx, conn, rng, *preload, *forUpdate)
				}
				if err != nil {
					if strings.Contains(err.Error(), "40001") || strings.Contains(err.Error(), "restart transaction") {
						retries.Add(1)
					} else {
						errs.Add(1)
					}
					continue
				}
				ops.Add(1)
				recordLatency(time.Since(start))
			}
		}(int64(w) + time.Now().UnixNano())
	}
	wg.Wait()

	total := ops.Load()
	fmt.Printf("\n%s results:\n", workload)
	fmt.Printf("  ops:        %d (%.1f/s)\n", total, float64(total)/duration.Seconds())
	fmt.Printf("  40001s:     %d\n", retries.Load())
	fmt.Printf("  errors:     %d\n", errs.Load())
	latMu.Lock()
	defer latMu.Unlock()
	if len(lats) > 0 {
		sort.Slice(lats, func(i, j int) bool { return lats[i] < lats[j] })
		pct := func(p float64) time.Duration { return lats[int(float64(len(lats)-1)*p)] }
		fmt.Printf("  latency:    p50 %s  p95 %s  p99 %s\n",
			pct(0.50).Round(time.Microsecond), pct(0.95).Round(time.Microsecond), pct(0.99).Round(time.Microsecond))
	}
	return nil
}

// bankTransfer moves a random amount between two random accounts in one
// serializable transaction (the classic consistency workload).
func bankTransfer(ctx context.Context, conn *pgx.Conn, rng *rand.Rand, accounts int, forUpdate bool) error {
	a, b := rng.Intn(accounts), rng.Intn(accounts)
	if a == b {
		b = (b + 1) % accounts
	}
	// Locking the two rows in a fixed order avoids trivial 2-row deadlocks.
	if forUpdate && b < a {
		a, b = b, a
	}
	suffix := ""
	if forUpdate {
		suffix = " FOR UPDATE"
	}
	tx, err := conn.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	var balA, balB int64
	if err := tx.QueryRow(ctx, fmt.Sprintf("SELECT balance FROM bench_bank WHERE id = %d%s", a, suffix)).Scan(&balA); err != nil {
		return err
	}
	if err := tx.QueryRow(ctx, fmt.Sprintf("SELECT balance FROM bench_bank WHERE id = %d%s", b, suffix)).Scan(&balB); err != nil {
		return err
	}
	amount := int64(rng.Intn(10) + 1)
	if balA < amount {
		return tx.Commit(ctx) // nothing to move
	}
	if _, err := tx.Exec(ctx, fmt.Sprintf("UPDATE bench_bank SET balance = %d WHERE id = %d", balA-amount, a)); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, fmt.Sprintf("UPDATE bench_bank SET balance = %d WHERE id = %d", balB+amount, b)); err != nil {
		return err
	}
	return tx.Commit(ctx)
}
