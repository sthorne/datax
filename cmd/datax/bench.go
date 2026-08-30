package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"math/rand"
	"net/http"
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
	batch := fs.Int("batch", 100, "ingest workload: rows per INSERT batch")
	payloadBytes := fs.Int("payload-bytes", 256, "ingest workload: value size per row")
	rate := fs.Int("rate", 0, "ingest workload: target rows/s across all workers (0 = unthrottled)")
	reportInterval := fs.Duration("report-interval", 5*time.Second, "ingest/timeseries: throughput-over-time reporting cadence")
	metricsURL := fs.String("metrics-url", "", "ingest/timeseries: node /metrics URL; reports storage and rows-scanned deltas")
	series := fs.Int("series", 1000, "timeseries workload: number of distinct series")
	shards := fs.Int("shards", 0, "timeseries workload: shard buckets for the table (0 = unsharded; the table name embeds this, so A/B runs don't collide)")
	fs.Usage = func() {
		fmt.Fprintf(fs.Output(), "Usage: datax bench <kv|bank|ingest|timeseries> [flags]\n\n")
		fmt.Fprintf(fs.Output(), "ingest writes batches of RANDOM keys to stress the LSM write path;\n")
		fmt.Fprintf(fs.Output(), "timeseries appends per-series MONOTONE timestamps — the hot-tail shape\n")
		fmt.Fprintf(fs.Output(), "that shard buckets exist to spread — then times windowed reads.\n\n")
		fs.PrintDefaults()
	}
	if len(args) == 0 || strings.HasPrefix(args[0], "-") {
		fs.Usage()
		return fmt.Errorf("bench requires a workload: kv, bank, ingest, or timeseries")
	}
	workload := args[0]
	if err := fs.Parse(args[1:]); err != nil {
		return err
	}
	switch workload {
	case "kv", "bank", "ingest", "timeseries":
	default:
		return fmt.Errorf("unknown workload %q (want kv, bank, ingest, or timeseries)", workload)
	}
	tsTable := "bench_ts"
	if *shards > 0 {
		tsTable = fmt.Sprintf("bench_ts_s%d", *shards)
	}
	// The write phase appends whole seconds per series starting here; the
	// read phase then queries windows below this watermark.
	tsBase := time.Now().UTC().Truncate(time.Second)
	ivalReport := workload == "ingest" || workload == "timeseries"

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
	case "ingest":
		if _, err := setup.Exec(ctx, "CREATE TABLE IF NOT EXISTS bench_ingest (k INT PRIMARY KEY, pad TEXT)"); err != nil {
			return err
		}
	case "timeseries":
		opts := "timeseries = true"
		if *shards > 0 {
			opts += fmt.Sprintf(", shards = %d", *shards)
		}
		ddl := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (series INT, ts TIMESTAMPTZ, val FLOAT8, PRIMARY KEY (series, ts)) WITH (%s)", tsTable, opts)
		if _, err := setup.Exec(ctx, ddl); err != nil {
			return err
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

	// Ingest extras: per-interval throughput/latency reporting, a shared
	// pacer for --rate, and storage-health counter deltas from /metrics.
	var intervalOps atomic.Int64
	var intervalLatMu sync.Mutex
	var intervalLats []time.Duration
	stopReport := make(chan struct{})
	var pace func()
	if ivalReport && *rate > 0 {
		var paceMu sync.Mutex
		next := time.Now()
		per := time.Duration(float64(*batch) / float64(*rate) * float64(time.Second))
		pace = func() {
			paceMu.Lock()
			n := next
			next = next.Add(per)
			paceMu.Unlock()
			if d := time.Until(n); d > 0 {
				time.Sleep(d)
			}
		}
	}

	recordLatency := func(d time.Duration) {
		latMu.Lock()
		if len(lats) < 200000 {
			lats = append(lats, d)
		}
		latMu.Unlock()
		if ivalReport {
			intervalOps.Add(1)
			intervalLatMu.Lock()
			if len(intervalLats) < 100000 {
				intervalLats = append(intervalLats, d)
			}
			intervalLatMu.Unlock()
		}
	}

	var before map[string]float64
	if ivalReport && *metricsURL != "" {
		before = scrapeCounters(*metricsURL)
	}

	deadline := time.Now().Add(*duration)
	var wg sync.WaitGroup
	fmt.Printf("running %s: %d workers for %s...\n", workload, *concurrency, *duration)
	if ivalReport {
		started := time.Now()
		go func() {
			t := time.NewTicker(*reportInterval)
			defer t.Stop()
			for {
				select {
				case <-stopReport:
					return
				case <-t.C:
					n := intervalOps.Swap(0)
					intervalLatMu.Lock()
					sample := intervalLats
					intervalLats = nil
					intervalLatMu.Unlock()
					p99 := time.Duration(0)
					if len(sample) > 0 {
						sort.Slice(sample, func(i, j int) bool { return sample[i] < sample[j] })
						p99 = sample[int(float64(len(sample)-1)*0.99)]
					}
					fmt.Printf("  t=%4.0fs  %9.0f rows/s  batch p99 %s\n",
						time.Since(started).Seconds(),
						float64(n*int64(*batch))/reportInterval.Seconds(),
						p99.Round(time.Microsecond))
				}
			}
		}()
	}
	for w := 0; w < *concurrency; w++ {
		wg.Add(1)
		go func(worker int, seed int64) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(seed))
			// Timeseries: this worker owns the series ≡ worker (mod
			// concurrency), each appending strictly monotone timestamps —
			// the hot right-edge tail a naive PK would serialize onto one
			// range.
			tsNext := map[int]int64{}
			tsSeries := worker
			// Ingest payload: per-worker random hex, reused across rows.
			padRaw := make([]byte, (*payloadBytes+1)/2)
			rng.Read(padRaw)
			pad := fmt.Sprintf("%x", padRaw)[:*payloadBytes]
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
				case "ingest":
					if pace != nil {
						pace()
					}
					var sb strings.Builder
					sb.WriteString("INSERT INTO bench_ingest (k, pad) VALUES ")
					for j := 0; j < *batch; j++ {
						if j > 0 {
							sb.WriteByte(',')
						}
						fmt.Fprintf(&sb, "(%d, '%s')", rng.Int63(), pad)
					}
					_, err = conn.Exec(ctx, sb.String())
				case "timeseries":
					if pace != nil {
						pace()
					}
					var sb strings.Builder
					fmt.Fprintf(&sb, "INSERT INTO %s (series, ts, val) VALUES ", tsTable)
					for j := 0; j < *batch; j++ {
						if j > 0 {
							sb.WriteByte(',')
						}
						n := tsNext[tsSeries]
						tsNext[tsSeries] = n + 1
						at := tsBase.Add(time.Duration(n) * time.Second)
						fmt.Fprintf(&sb, "(%d, '%s', %g)", tsSeries, at.Format("2006-01-02 15:04:05Z"), rng.Float64())
						tsSeries += *concurrency
						if tsSeries >= *series {
							tsSeries = worker
						}
					}
					_, err = conn.Exec(ctx, sb.String())
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
		}(w, int64(w)+time.Now().UnixNano())
	}
	wg.Wait()
	close(stopReport)

	total := ops.Load()
	fmt.Printf("\n%s results:\n", workload)
	fmt.Printf("  ops:        %d (%.1f/s)\n", total, float64(total)/duration.Seconds())
	if ivalReport {
		fmt.Printf("  rows:       %d (%.0f/s)\n", total*int64(*batch), float64(total*int64(*batch))/duration.Seconds())
	}
	fmt.Printf("  40001s:     %d\n", retries.Load())
	fmt.Printf("  errors:     %d\n", errs.Load())
	if before != nil {
		after := scrapeCounters(*metricsURL)
		fmt.Printf("  hard write stalls: %+.0f (must be 0 at the target rate)\n",
			after["datax_storage_write_stalls_total"]-before["datax_storage_write_stalls_total"])
		fmt.Printf("  backpressured writes: %+.0f\n",
			after["datax_storage_backpressure_total"]-before["datax_storage_backpressure_total"])
	}
	latMu.Lock()
	if len(lats) > 0 {
		sort.Slice(lats, func(i, j int) bool { return lats[i] < lats[j] })
		pct := func(p float64) time.Duration { return lats[int(float64(len(lats)-1)*p)] }
		fmt.Printf("  latency:    p50 %s  p95 %s  p99 %s\n",
			pct(0.50).Round(time.Microsecond), pct(0.95).Round(time.Microsecond), pct(0.99).Round(time.Microsecond))
	}
	latMu.Unlock()

	// Timeseries read phase: windowed per-series queries — the query shape
	// the (series, ts) key layout serves — timed against the rows-scanned
	// counter (a narrow window must not scan the world).
	if workload == "timeseries" {
		conn, err := connect()
		if err != nil {
			return err
		}
		defer func() { _ = conn.Close(ctx) }()
		winLo := tsBase.Format("2006-01-02 15:04:05Z")
		winHi := tsBase.Add(60 * time.Second).Format("2006-01-02 15:04:05Z")
		var plan string
		if err := conn.QueryRow(ctx, fmt.Sprintf(
			"EXPLAIN SELECT val FROM %s WHERE series = 1 AND ts >= '%s' AND ts < '%s'", tsTable, winLo, winHi)).Scan(&plan); err != nil {
			return err
		}
		fmt.Printf("\nread phase (60s windows over single series):\n  plan: %s\n", plan)
		beforeReads := map[string]float64{}
		if *metricsURL != "" {
			beforeReads = scrapeCounters(*metricsURL)
		}
		const queries = 200
		var rlats []time.Duration
		totalRows := int64(0)
		rng := rand.New(rand.NewSource(time.Now().UnixNano()))
		for i := 0; i < queries; i++ {
			q := fmt.Sprintf("SELECT COUNT(*) AS n FROM %s WHERE series = %d AND ts >= '%s' AND ts < '%s'",
				tsTable, rng.Intn(*series), winLo, winHi)
			qs := time.Now()
			var n int64
			if err := conn.QueryRow(ctx, q).Scan(&n); err != nil {
				return err
			}
			rlats = append(rlats, time.Since(qs))
			totalRows += n
		}
		sort.Slice(rlats, func(i, j int) bool { return rlats[i] < rlats[j] })
		fmt.Printf("  %d queries, %d rows returned, p50 %s  p99 %s\n",
			queries, totalRows,
			rlats[len(rlats)/2].Round(time.Microsecond),
			rlats[int(float64(len(rlats)-1)*0.99)].Round(time.Microsecond))
		if *metricsURL != "" {
			afterReads := scrapeCounters(*metricsURL)
			fmt.Printf("  rows scanned (gateway): %+.0f\n",
				afterReads["datax_sql_rows_scanned_total"]-beforeReads["datax_sql_rows_scanned_total"])
		}
	}
	return nil
}

// scrapeCounters fetches a Prometheus /metrics page and returns the plain
// (unlabeled) series it can parse; failures return an empty map.
func scrapeCounters(url string) map[string]float64 {
	out := map[string]float64{}
	resp, err := http.Get(url)
	if err != nil {
		fmt.Printf("  (metrics scrape failed: %v)\n", err)
		return out
	}
	defer func() { _ = resp.Body.Close() }()
	sc := bufio.NewScanner(resp.Body)
	for sc.Scan() {
		line := sc.Text()
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		name, val, ok := strings.Cut(line, " ")
		if !ok {
			continue
		}
		var f float64
		if _, err := fmt.Sscanf(val, "%g", &f); err == nil {
			out[name] = f
		}
	}
	return out
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
