package main

import (
	"bufio"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"math/rand"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"runtime/trace"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"
)

// benchResult is one run's record, written by --json and read by
// `datax bench compare` (issue #100). Latencies are microseconds.
type benchResult struct {
	Name        string    `json:"name"`
	Workload    string    `json:"workload"`
	Args        []string  `json:"args"`
	Seed        int64     `json:"seed"`
	Concurrency int       `json:"concurrency"`
	DurationSec float64   `json:"duration_sec"`
	StartedAt   time.Time `json:"started_at"`
	Version     string    `json:"version"`
	GoVersion   string    `json:"go_version"`
	Host        string    `json:"host"`

	Ops        int64   `json:"ops"`
	OpsPerSec  float64 `json:"ops_per_sec"`
	Rows       int64   `json:"rows,omitempty"`
	RowsPerSec float64 `json:"rows_per_sec,omitempty"`
	Errors     int64   `json:"errors"`
	Retries    int64   `json:"retries"`
	P50us      int64   `json:"p50_us"`
	P95us      int64   `json:"p95_us"`
	P99us      int64   `json:"p99_us"`
	// Metrics are the server's counter deltas over the run (every plain
	// datax_* series that moved), from --server-url or --metrics-url.
	Metrics map[string]float64 `json:"metrics,omitempty"`
	// ReadPhase is the timeseries workload's windowed-read phase.
	ReadPhase *benchReadPhase `json:"read_phase,omitempty"`
}

type benchReadPhase struct {
	Queries     int     `json:"queries"`
	Rows        int64   `json:"rows"`
	P50us       int64   `json:"p50_us"`
	P99us       int64   `json:"p99_us"`
	RowsScanned float64 `json:"rows_scanned"`
	Plan        string  `json:"plan"`
}

// benchWorkloads are the workloads and what they exercise.
var benchWorkloads = []struct{ name, doc string }{
	{"kv", "point reads and updates by primary key (--read-pct)"},
	{"bank", "contended two-row transfers in explicit transactions"},
	{"ingest", "batched INSERTs (--keys random|sequential|uuid, --batch, --payload-bytes)"},
	{"ingest-copy", "the same rows through COPY FROM"},
	{"timeseries", "per-series monotone timestamps (--series, --shards), then windowed reads"},
	{"index-join", "secondary-index lookups fanning out to wide primary rows (--preload rows)"},
	{"scan", "large result sets streamed through pgwire (--preload rows per scan)"},
}

func benchUsage(fs *flag.FlagSet) {
	fmt.Fprintf(fs.Output(), "Usage: datax bench <workload> [flags]\n       datax bench run --set bench/workloads.json --out DIR [flags]\n       datax bench compare BEFORE AFTER [--threshold 5]\n\nWorkloads:\n")
	for _, w := range benchWorkloads {
		fmt.Fprintf(fs.Output(), "  %-12s %s\n", w.name, w.doc)
	}
	fmt.Fprintf(fs.Output(), "\nFlags:\n")
	fs.PrintDefaults()
}

// runBench drives load against a running cluster over the PostgreSQL wire
// protocol — the same path real applications use.
func runBench(args []string) error {
	if len(args) > 0 {
		switch args[0] {
		case "run":
			return runBenchSet(args[1:])
		case "compare":
			return runBenchCompare(args[1:])
		}
	}
	_, err := runBenchWorkload(args)
	return err
}

// runBenchWorkload runs one workload and returns its record.
func runBenchWorkload(args []string) (*benchResult, error) {
	fs := flag.NewFlagSet("bench", flag.ContinueOnError)
	url := fs.String("url", "postgres://root@127.0.0.1:26433/datax?sslmode=disable", "database URL")
	concurrency := fs.Int("concurrency", 16, "concurrent workers")
	duration := fs.Duration("duration", 10*time.Second, "how long to run")
	readPct := fs.Int("read-pct", 95, "kv workload: percentage of reads")
	preload := fs.Int("preload", 1000, "rows (kv, index-join, scan) or accounts (bank) to preload")
	forUpdate := fs.Bool("for-update", false, "bank workload: read balances with SELECT ... FOR UPDATE")
	batch := fs.Int("batch", 100, "ingest workload: rows per INSERT batch")
	payloadBytes := fs.Int("payload-bytes", 256, "ingest, index-join, scan: value size per row")
	keys := fs.String("keys", "random", "ingest workload: key order — random (LSM stress), sequential (per-worker monotone), uuid (text keys)")
	rate := fs.Int("rate", 0, "ingest workload: target rows/s across all workers (0 = unthrottled)")
	reportInterval := fs.Duration("report-interval", 5*time.Second, "ingest/timeseries: throughput-over-time reporting cadence")
	metricsURL := fs.String("metrics-url", "", "node /metrics URL for server counter deltas (or derive it from --server-url)")
	serverURL := fs.String("server-url", "", "node HTTP base URL (http://host:8080): counter deltas, and --server-profile")
	serverProfile := fs.String("server-profile", "", "pull a server profile for the run's duration: cpu (needs --server-url)")
	serverProfileOut := fs.String("server-profile-out", "server-cpu.pprof", "where --server-profile writes")
	certsDir := fs.String("certs-dir", "", "secure cluster: certificate directory for the HTTP port (presents client.<user>.crt)")
	certUser := fs.String("user", "root", "username whose client certificate authenticates HTTP calls (with --certs-dir)")
	insecureTLS := fs.Bool("insecure-skip-verify", false, "skip TLS certificate verification on the HTTP port")
	series := fs.Int("series", 1000, "timeseries workload: number of distinct series")
	shards := fs.Int("shards", 0, "timeseries workload: shard buckets for the table (0 = unsharded; the table name embeds this, so A/B runs don't collide)")
	groups := fs.Int("groups", 1000, "index-join workload: distinct indexed values (rows per lookup = preload / groups)")
	seed := fs.Int64("seed", 1, "random seed (0 = from the clock); a fixed seed makes runs comparable")
	name := fs.String("name", "", "label in the JSON record (default: the workload)")
	jsonOut := fs.String("json", "", "write the run's record to this file")
	cpuProfile := fs.String("cpuprofile", "", "write the client's CPU profile here")
	memProfile := fs.String("memprofile", "", "write the client's heap profile here at the end")
	traceOut := fs.String("trace", "", "write the client's execution trace here")
	fs.Usage = func() { benchUsage(fs) }
	if len(args) == 0 || strings.HasPrefix(args[0], "-") {
		fs.Usage()
		return nil, fmt.Errorf("bench requires a workload: kv, bank, ingest, ingest-copy, timeseries, index-join, or scan")
	}
	workload := args[0]
	if err := fs.Parse(args[1:]); err != nil {
		return nil, err
	}
	known := false
	for _, w := range benchWorkloads {
		known = known || w.name == workload
	}
	if !known {
		return nil, fmt.Errorf("unknown workload %q (want kv, bank, ingest, ingest-copy, timeseries, index-join, or scan)", workload)
	}
	switch *keys {
	case "random", "sequential", "uuid":
	default:
		return nil, fmt.Errorf("--keys must be random, sequential or uuid")
	}
	if *seed == 0 {
		*seed = time.Now().UnixNano()
	}
	if *serverURL != "" && *metricsURL == "" {
		*metricsURL = strings.TrimSuffix(*serverURL, "/") + "/metrics"
	}
	if *serverProfile != "" && *serverProfile != "cpu" {
		return nil, fmt.Errorf("--server-profile takes cpu")
	}
	if *serverProfile != "" && *serverURL == "" {
		return nil, fmt.Errorf("--server-profile needs --server-url")
	}
	httpClient, connKind, err := newHTTPClient(*certsDir, *certUser, *insecureTLS)
	if err != nil {
		return nil, err
	}

	// Client-side profiles.
	if *cpuProfile != "" {
		f, err := os.Create(*cpuProfile)
		if err != nil {
			return nil, err
		}
		if err := pprof.StartCPUProfile(f); err != nil {
			return nil, err
		}
		defer func() { pprof.StopCPUProfile(); _ = f.Close() }()
	}
	if *traceOut != "" {
		f, err := os.Create(*traceOut)
		if err != nil {
			return nil, err
		}
		if err := trace.Start(f); err != nil {
			return nil, err
		}
		defer func() { trace.Stop(); _ = f.Close() }()
	}
	if *memProfile != "" {
		defer func() {
			f, err := os.Create(*memProfile)
			if err == nil {
				_ = pprof.WriteHeapProfile(f)
				_ = f.Close()
			}
		}()
	}

	res := &benchResult{Workload: workload, Args: args, Seed: *seed, Concurrency: *concurrency, DurationSec: duration.Seconds(),
		StartedAt: time.Now().UTC(), Version: version, GoVersion: runtime.Version()}
	res.Name = *name
	if res.Name == "" {
		res.Name = workload
	}
	res.Host, _ = os.Hostname()

	tsTable := "bench_ts"
	if *shards > 0 {
		tsTable = fmt.Sprintf("bench_ts_s%d", *shards)
	}
	ingestTable := "bench_ingest"
	if *keys == "uuid" {
		ingestTable = "bench_ingest_uuid"
	}
	// The write phase appends whole seconds per series starting here; the
	// read phase then queries windows below this watermark.
	tsBase := time.Now().UTC().Truncate(time.Second)
	ivalReport := workload == "ingest" || workload == "ingest-copy" || workload == "timeseries"
	rowsPerOp := int64(1)
	if ivalReport {
		rowsPerOp = int64(*batch)
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
		return nil, fmt.Errorf("connecting to %s: %w", *url, err)
	}
	pad := strings.Repeat("x", *payloadBytes)
	preloadRows := func(table, cols string, row func(i int) string) error {
		fmt.Printf("preloading %d rows into %s...\n", *preload, table)
		for i := 0; i < *preload; i += 100 {
			var vals []string
			for j := i; j < i+100 && j < *preload; j++ {
				vals = append(vals, row(j))
			}
			if _, err := setup.Exec(ctx, fmt.Sprintf("INSERT INTO %s (%s) VALUES %s", table, cols, strings.Join(vals, ", "))); err != nil && !strings.Contains(err.Error(), "duplicate") {
				return err
			}
		}
		return nil
	}
	switch workload {
	case "kv":
		if _, err := setup.Exec(ctx, "CREATE TABLE IF NOT EXISTS bench_kv (k INT8 PRIMARY KEY, v TEXT)"); err != nil {
			return nil, err
		}
		if err := preloadRows("bench_kv", "k, v", func(j int) string { return fmt.Sprintf("(%d, 'v%d')", j, j) }); err != nil {
			return nil, err
		}
	case "ingest", "ingest-copy":
		ddl := "CREATE TABLE IF NOT EXISTS bench_ingest (k INT8 PRIMARY KEY, pad TEXT)"
		if *keys == "uuid" {
			ddl = "CREATE TABLE IF NOT EXISTS bench_ingest_uuid (k TEXT PRIMARY KEY, pad TEXT)"
		}
		if _, err := setup.Exec(ctx, ddl); err != nil {
			return nil, err
		}
	case "timeseries":
		opts := "timeseries = true"
		if *shards > 0 {
			opts += fmt.Sprintf(", shards = %d", *shards)
		}
		ddl := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (series INT8, ts TIMESTAMPTZ, val FLOAT8, PRIMARY KEY (series, ts)) WITH (%s)", tsTable, opts)
		if _, err := setup.Exec(ctx, ddl); err != nil {
			return nil, err
		}
	case "bank":
		if _, err := setup.Exec(ctx, "CREATE TABLE IF NOT EXISTS bench_bank (id INT8 PRIMARY KEY, balance INT8)"); err != nil {
			return nil, err
		}
		if err := preloadRows("bench_bank", "id, balance", func(j int) string { return fmt.Sprintf("(%d, 1000)", j) }); err != nil {
			return nil, err
		}
	case "index-join":
		// A wide primary row (pad) reached through a narrow secondary
		// index: each lookup fans out to preload/groups primary fetches.
		if _, err := setup.Exec(ctx, "CREATE TABLE IF NOT EXISTS bench_ij (k INT8 PRIMARY KEY, g INT8, pad TEXT)"); err != nil {
			return nil, err
		}
		if _, err := setup.Exec(ctx, "CREATE INDEX IF NOT EXISTS bench_ij_g ON bench_ij (g)"); err != nil {
			return nil, err
		}
		if err := preloadRows("bench_ij", "k, g, pad", func(j int) string { return fmt.Sprintf("(%d, %d, '%s')", j, j%*groups, pad) }); err != nil {
			return nil, err
		}
	case "scan":
		if _, err := setup.Exec(ctx, "CREATE TABLE IF NOT EXISTS bench_scan (k INT8 PRIMARY KEY, pad TEXT)"); err != nil {
			return nil, err
		}
		if err := preloadRows("bench_scan", "k, pad", func(j int) string { return fmt.Sprintf("(%d, '%s')", j, pad) }); err != nil {
			return nil, err
		}
	}
	_ = setup.Close(ctx)

	var ops, errs, retries, rowsOut atomic.Int64
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
	if *metricsURL != "" {
		before = scrapeCounters(httpClient, *metricsURL)
	}
	// The server CPU profile covers the run: fetched concurrently, its
	// sampling window is the run's duration.
	var profileDone chan error
	if *serverProfile == "cpu" {
		profileDone = make(chan error, 1)
		target := fmt.Sprintf("%s/debug/pprof/profile?seconds=%d", strings.TrimSuffix(*serverURL, "/"), int(duration.Seconds()))
		go func() {
			f, err := os.Create(*serverProfileOut)
			if err != nil {
				profileDone <- err
				return
			}
			err = httpFetch(httpClient, connKind, target, 10*time.Second, *duration+30*time.Second, f)
			_ = f.Close()
			profileDone <- err
		}()
	}

	deadline := time.Now().Add(*duration)
	var wg sync.WaitGroup
	fmt.Printf("running %s: %d workers for %s (seed %d)...\n", workload, *concurrency, *duration, *seed)
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
		go func(worker int, workerSeed int64) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(workerSeed))
			// Timeseries: this worker owns the series ≡ worker (mod
			// concurrency), each appending strictly monotone timestamps —
			// the hot right-edge tail a naive PK would serialize onto one
			// range.
			tsNext := map[int]int64{}
			tsSeries := worker
			// Sequential ingest keys: each worker owns a disjoint,
			// monotone band.
			seqNext := int64(worker) << 40
			// Ingest payload: per-worker random hex, reused across rows.
			padRaw := make([]byte, (*payloadBytes+1)/2)
			rng.Read(padRaw)
			pad := fmt.Sprintf("%x", padRaw)[:*payloadBytes]
			ingestKey := func() string {
				switch *keys {
				case "sequential":
					seqNext++
					return fmt.Sprintf("%d", seqNext)
				case "uuid":
					var u [16]byte
					rng.Read(u[:])
					u[6] = u[6]&0x0f | 0x40
					u[8] = u[8]&0x3f | 0x80
					return fmt.Sprintf("'%x-%x-%x-%x-%x'", u[0:4], u[4:6], u[6:8], u[8:10], u[10:16])
				}
				return fmt.Sprintf("%d", rng.Int63())
			}
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
					fmt.Fprintf(&sb, "INSERT INTO %s (k, pad) VALUES ", ingestTable)
					for j := 0; j < *batch; j++ {
						if j > 0 {
							sb.WriteByte(',')
						}
						fmt.Fprintf(&sb, "(%s, '%s')", ingestKey(), pad)
					}
					_, err = conn.Exec(ctx, sb.String())
				case "ingest-copy":
					// The same rows through COPY: pgx's CopyFrom takes the
					// binary copy path regardless of the connection's simple
					// query mode.
					if pace != nil {
						pace()
					}
					rows := make([][]any, *batch)
					for j := range rows {
						switch *keys {
						case "sequential":
							seqNext++
							rows[j] = []any{seqNext, pad}
						case "uuid":
							rows[j] = []any{strings.Trim(ingestKey(), "'"), pad}
						default:
							rows[j] = []any{rng.Int63(), pad}
						}
					}
					_, err = conn.CopyFrom(ctx, pgx.Identifier{ingestTable},
						[]string{"k", "pad"}, pgx.CopyFromRows(rows))
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
				case "index-join":
					var n int64
					n, err = countRows(ctx, conn, fmt.Sprintf("SELECT k, pad FROM bench_ij WHERE g = %d", rng.Intn(*groups)))
					rowsOut.Add(n)
				case "scan":
					var n int64
					n, err = countRows(ctx, conn, "SELECT k, pad FROM bench_scan")
					rowsOut.Add(n)
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
		}(w, *seed+int64(w)*7919)
	}
	wg.Wait()
	close(stopReport)

	total := ops.Load()
	res.Ops, res.OpsPerSec = total, float64(total)/duration.Seconds()
	res.Errors, res.Retries = errs.Load(), retries.Load()
	fmt.Printf("\n%s results:\n", workload)
	fmt.Printf("  ops:        %d (%.1f/s)\n", total, res.OpsPerSec)
	switch {
	case ivalReport:
		res.Rows = total * rowsPerOp
	case workload == "index-join" || workload == "scan":
		res.Rows = rowsOut.Load()
	}
	if res.Rows > 0 {
		res.RowsPerSec = float64(res.Rows) / duration.Seconds()
		fmt.Printf("  rows:       %d (%.0f/s)\n", res.Rows, res.RowsPerSec)
	}
	fmt.Printf("  40001s:     %d\n", res.Retries)
	fmt.Printf("  errors:     %d\n", res.Errors)
	if before != nil {
		after := scrapeCounters(httpClient, *metricsURL)
		res.Metrics = counterDeltas(before, after)
		fmt.Printf("  hard write stalls: %+.0f (must be 0 at the target rate)\n", res.Metrics["datax_storage_write_stalls_total"])
		fmt.Printf("  backpressured writes: %+.0f\n", res.Metrics["datax_storage_backpressure_total"])
		for _, cause := range []string{"leader", "debt", "follower"} {
			series := fmt.Sprintf(`datax_storage_backpressure_cause_total{cause="%s"}`, cause)
			if d := res.Metrics[series]; d != 0 {
				fmt.Printf("    cause=%s: %+.0f\n", cause, d)
			}
		}
	}
	latMu.Lock()
	if len(lats) > 0 {
		sort.Slice(lats, func(i, j int) bool { return lats[i] < lats[j] })
		pct := func(p float64) time.Duration { return lats[int(float64(len(lats)-1)*p)] }
		res.P50us, res.P95us, res.P99us = pct(0.50).Microseconds(), pct(0.95).Microseconds(), pct(0.99).Microseconds()
		fmt.Printf("  latency:    p50 %s  p95 %s  p99 %s\n",
			pct(0.50).Round(time.Microsecond), pct(0.95).Round(time.Microsecond), pct(0.99).Round(time.Microsecond))
	}
	latMu.Unlock()
	if profileDone != nil {
		if err := <-profileDone; err != nil {
			fmt.Printf("  (server profile failed: %v)\n", err)
		} else {
			fmt.Printf("  server CPU profile: %s\n", *serverProfileOut)
		}
	}

	// Timeseries read phase: windowed per-series queries — the query shape
	// the (series, ts) key layout serves — timed against the rows-scanned
	// counter (a narrow window must not scan the world).
	if workload == "timeseries" {
		conn, err := connect()
		if err != nil {
			return nil, err
		}
		defer func() { _ = conn.Close(ctx) }()
		winLo := tsBase.Format("2006-01-02 15:04:05Z")
		winHi := tsBase.Add(60 * time.Second).Format("2006-01-02 15:04:05Z")
		var plan string
		if err := conn.QueryRow(ctx, fmt.Sprintf(
			"EXPLAIN SELECT val FROM %s WHERE series = 1 AND ts >= '%s' AND ts < '%s'", tsTable, winLo, winHi)).Scan(&plan); err != nil {
			return nil, err
		}
		fmt.Printf("\nread phase (60s windows over single series):\n  plan: %s\n", plan)
		beforeReads := map[string]float64{}
		if *metricsURL != "" {
			beforeReads = scrapeCounters(httpClient, *metricsURL)
		}
		const queries = 200
		var rlats []time.Duration
		totalRows := int64(0)
		rng := rand.New(rand.NewSource(*seed))
		for i := 0; i < queries; i++ {
			q := fmt.Sprintf("SELECT COUNT(*) AS n FROM %s WHERE series = %d AND ts >= '%s' AND ts < '%s'",
				tsTable, rng.Intn(*series), winLo, winHi)
			qs := time.Now()
			var n int64
			if err := conn.QueryRow(ctx, q).Scan(&n); err != nil {
				return nil, err
			}
			rlats = append(rlats, time.Since(qs))
			totalRows += n
		}
		sort.Slice(rlats, func(i, j int) bool { return rlats[i] < rlats[j] })
		rp := &benchReadPhase{Queries: queries, Rows: totalRows, Plan: plan,
			P50us: rlats[len(rlats)/2].Microseconds(), P99us: rlats[int(float64(len(rlats)-1)*0.99)].Microseconds()}
		fmt.Printf("  %d queries, %d rows returned, p50 %s  p99 %s\n",
			queries, totalRows,
			rlats[len(rlats)/2].Round(time.Microsecond),
			rlats[int(float64(len(rlats)-1)*0.99)].Round(time.Microsecond))
		if *metricsURL != "" {
			afterReads := scrapeCounters(httpClient, *metricsURL)
			rp.RowsScanned = afterReads["datax_sql_rows_scanned_total"] - beforeReads["datax_sql_rows_scanned_total"]
			fmt.Printf("  rows scanned (gateway): %+.0f\n", rp.RowsScanned)
		}
		res.ReadPhase = rp
	}
	if *jsonOut != "" {
		if err := writeJSON(*jsonOut, res); err != nil {
			return nil, err
		}
		fmt.Printf("  record:     %s\n", *jsonOut)
	}
	return res, nil
}

// countRows runs a query and drains it, returning the row count (the
// scan and index-join workloads measure result-set delivery).
func countRows(ctx context.Context, conn *pgx.Conn, q string) (int64, error) {
	rows, err := conn.Query(ctx, q)
	if err != nil {
		return 0, err
	}
	defer rows.Close()
	var n int64
	for rows.Next() {
		n++
	}
	return n, rows.Err()
}

func writeJSON(path string, v any) error {
	if dir := filepath.Dir(path); dir != "." {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return err
		}
	}
	raw, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, append(raw, '\n'), 0o644)
}

// scrapeCounters fetches a Prometheus /metrics page and returns the
// series it can parse (labels kept in the name); failures return an
// empty map.
func scrapeCounters(client *http.Client, url string) map[string]float64 {
	out := map[string]float64{}
	resp, err := client.Get(url)
	if err != nil {
		fmt.Printf("  (metrics scrape failed: %v)\n", err)
		return out
	}
	defer func() { _ = resp.Body.Close() }()
	sc := bufio.NewScanner(resp.Body)
	sc.Buffer(make([]byte, 1<<20), 1<<20)
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

// counterDeltas returns after - before for every datax_* series that
// moved.
func counterDeltas(before, after map[string]float64) map[string]float64 {
	out := map[string]float64{}
	for name, a := range after {
		if !strings.HasPrefix(name, "datax_") {
			continue
		}
		if d := a - before[name]; d != 0 {
			out[name] = d
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

// benchSet is bench/workloads.json: the checked-in workload set.
type benchSet struct {
	Workloads []benchSetEntry `json:"workloads"`
}

type benchSetEntry struct {
	Name string   `json:"name"`
	Args []string `json:"args"`
}

// runBenchSet runs every workload of a set against one cluster, writing
// a JSON record per workload into --out.
func runBenchSet(args []string) error {
	fs := flag.NewFlagSet("bench run", flag.ContinueOnError)
	set := fs.String("set", "bench/workloads.json", "the workload set")
	url := fs.String("url", "postgres://root@127.0.0.1:26433/datax?sslmode=disable", "database URL")
	serverURL := fs.String("server-url", "", "node HTTP base URL (counter deltas per workload)")
	out := fs.String("out", "bench-results", "directory for the JSON records")
	only := fs.String("only", "", "run only the workloads whose name contains this")
	durationScale := fs.Float64("duration-scale", 1, "multiply every workload's --duration (a quick smoke run: 0.1)")
	fs.Usage = func() {
		fmt.Fprintf(fs.Output(), "Usage: datax bench run --set bench/workloads.json --url ... [--server-url ...] --out DIR\n\n")
		fs.PrintDefaults()
	}
	if err := fs.Parse(args); err != nil {
		return err
	}
	raw, err := os.ReadFile(*set)
	if err != nil {
		return err
	}
	var bs benchSet
	if err := json.Unmarshal(raw, &bs); err != nil {
		return fmt.Errorf("%s: %v", *set, err)
	}
	if err := os.MkdirAll(*out, 0o755); err != nil {
		return err
	}
	for _, w := range bs.Workloads {
		if *only != "" && !strings.Contains(w.Name, *only) {
			continue
		}
		if len(w.Args) == 0 {
			return fmt.Errorf("workload %q has no args", w.Name)
		}
		wargs := append([]string{w.Args[0]}, "--url", *url, "--name", w.Name, "--json", filepath.Join(*out, w.Name+".json"))
		if *serverURL != "" {
			wargs = append(wargs, "--server-url", *serverURL)
		}
		wargs = append(wargs, scaleDurations(w.Args[1:], *durationScale)...)
		fmt.Printf("\n=== %s: datax bench %s\n", w.Name, strings.Join(wargs, " "))
		if _, err := runBenchWorkload(wargs); err != nil {
			return fmt.Errorf("%s: %w", w.Name, err)
		}
	}
	return nil
}

// scaleDurations multiplies a --duration value in an argument list.
func scaleDurations(args []string, scale float64) []string {
	if scale == 1 {
		return args
	}
	out := append([]string(nil), args...)
	for i := 0; i+1 < len(out); i++ {
		if out[i] == "--duration" || out[i] == "-duration" {
			if d, err := time.ParseDuration(out[i+1]); err == nil {
				out[i+1] = time.Duration(float64(d) * scale).Round(time.Millisecond).String()
			}
		}
	}
	return out
}

// runBenchCompare prints a before/after table over two sets of records
// (files or directories of files), matched by name, flagging deltas
// beyond --threshold percent.
func runBenchCompare(args []string) error {
	fs := flag.NewFlagSet("bench compare", flag.ContinueOnError)
	threshold := fs.Float64("threshold", 5, "flag deltas beyond this many percent")
	failOn := fs.Bool("fail-on-regression", false, "exit 1 when a flagged delta is a regression")
	fs.Usage = func() {
		fmt.Fprintf(fs.Output(), "Usage: datax bench compare BEFORE AFTER [--threshold 5] [--fail-on-regression]\n\nBEFORE and AFTER are JSON records from --json, or directories of them.\n\n")
		fs.PrintDefaults()
	}
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 2 {
		fs.Usage()
		return fmt.Errorf("bench compare takes two records or directories")
	}
	before, err := loadBenchResults(fs.Arg(0))
	if err != nil {
		return err
	}
	after, err := loadBenchResults(fs.Arg(1))
	if err != nil {
		return err
	}
	names := make([]string, 0, len(before))
	for n := range before {
		if _, ok := after[n]; ok {
			names = append(names, n)
		}
	}
	sort.Strings(names)
	if len(names) == 0 {
		return fmt.Errorf("no workload appears in both %s and %s", fs.Arg(0), fs.Arg(1))
	}
	// Metrics: name, before, after, higher-is-better.
	type metric struct {
		name   string
		get    func(*benchResult) float64
		higher bool
		fmt    string
	}
	metrics := []metric{
		{"ops/s", func(r *benchResult) float64 { return r.OpsPerSec }, true, "%.1f"},
		{"rows/s", func(r *benchResult) float64 { return r.RowsPerSec }, true, "%.0f"},
		{"p50 µs", func(r *benchResult) float64 { return float64(r.P50us) }, false, "%.0f"},
		{"p95 µs", func(r *benchResult) float64 { return float64(r.P95us) }, false, "%.0f"},
		{"p99 µs", func(r *benchResult) float64 { return float64(r.P99us) }, false, "%.0f"},
		{"errors", func(r *benchResult) float64 { return float64(r.Errors) }, false, "%.0f"},
		{"retries", func(r *benchResult) float64 { return float64(r.Retries) }, false, "%.0f"},
	}
	regressions := 0
	fmt.Printf("%-24s %-9s %14s %14s %9s\n", "workload", "metric", "before", "after", "delta")
	for _, n := range names {
		b, a := before[n], after[n]
		for _, m := range metrics {
			bv, av := m.get(b), m.get(a)
			if bv == 0 && av == 0 {
				continue
			}
			delta := "n/a"
			flag := ""
			if bv != 0 {
				pct := (av - bv) / bv * 100
				delta = fmt.Sprintf("%+.1f%%", pct)
				if pct > *threshold || pct < -*threshold {
					flag = " !"
					if (pct > 0) != m.higher {
						flag = " !!"
						regressions++
					}
				}
			} else if av != 0 {
				flag = " !"
				if !m.higher {
					flag = " !!"
					regressions++
				}
			}
			fmt.Printf("%-24s %-9s %14s %14s %9s%s\n", n, m.name, fmt.Sprintf(m.fmt, bv), fmt.Sprintf(m.fmt, av), delta, flag)
		}
	}
	fmt.Printf("\n! beyond ±%.0f%%; !! a regression (lower throughput or higher latency, errors, retries)\n", *threshold)
	if *failOn && regressions > 0 {
		return fmt.Errorf("%d regression(s) beyond ±%.0f%%", regressions, *threshold)
	}
	return nil
}

// loadBenchResults reads one record or every *.json in a directory,
// keyed by name.
func loadBenchResults(path string) (map[string]*benchResult, error) {
	st, err := os.Stat(path)
	if err != nil {
		return nil, err
	}
	var files []string
	if st.IsDir() {
		entries, err := os.ReadDir(path)
		if err != nil {
			return nil, err
		}
		for _, e := range entries {
			if strings.HasSuffix(e.Name(), ".json") {
				files = append(files, filepath.Join(path, e.Name()))
			}
		}
	} else {
		files = []string{path}
	}
	out := map[string]*benchResult{}
	for _, f := range files {
		raw, err := os.ReadFile(f)
		if err != nil {
			return nil, err
		}
		var r benchResult
		if err := json.Unmarshal(raw, &r); err != nil {
			return nil, fmt.Errorf("%s: %v", f, err)
		}
		if r.Name == "" {
			r.Name = strings.TrimSuffix(filepath.Base(f), ".json")
		}
		out[r.Name] = &r
	}
	return out, nil
}
