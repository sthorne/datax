package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/sthorne/datax/pkg/base"
	"github.com/sthorne/datax/pkg/server"
	"github.com/sthorne/datax/pkg/storage"
	"github.com/sthorne/datax/pkg/util/log"
	pversion "github.com/sthorne/datax/pkg/version"
)

type serverFlags struct {
	fs            *flag.FlagSet
	dir           string
	listen        string
	pgListen      string
	join          string
	locality      string
	maxOffset     time.Duration
	advertise     string
	certsDir      string
	rootPw        string
	httpListen    string
	profile       string
	cacheSize     string
	encKeyPath    string
	loadSplit     float64
	mergeThr      int64
	raftWorkers   int
	quiescence    bool
	shedFactor    float64
	consistInt    time.Duration
	bytesThr      int64
	verbose       bool
	slowStmt      time.Duration
	metricsRecord time.Duration
	drainTimeout  time.Duration
}

func newServerFlags(name string) *serverFlags {
	f := &serverFlags{fs: flag.NewFlagSet(name, flag.ContinueOnError)}
	f.fs.StringVar(&f.dir, "dir", "", "data directory (empty = in-memory, data is lost on exit)")
	f.fs.StringVar(&f.listen, "listen", ":26257", "internode RPC address")
	f.fs.StringVar(&f.pgListen, "pg-listen", ":26433", "SQL (PostgreSQL protocol) address")
	f.fs.StringVar(&f.join, "join", "", "RPC address of any existing cluster node")
	f.fs.StringVar(&f.locality, "locality", "", "ordered failure-domain tiers, e.g. region=r1,rack=a")
	f.fs.DurationVar(&f.maxOffset, "max-offset", base.DefaultMaxClockOffset, "maximum tolerated clock offset between nodes")
	f.fs.StringVar(&f.advertise, "advertise", "", "address other nodes should use to reach this node (default: resolved listen address)")
	f.fs.StringVar(&f.certsDir, "certs-dir", "", "certificate directory; enables mutual internode TLS and SQL TLS+SCRAM (empty = insecure)")
	f.fs.StringVar(&f.rootPw, "root-password", "", "secure mode: seed the root SQL user's password at startup if unset")
	f.fs.StringVar(&f.httpListen, "http-listen", "", "observability address serving /metrics and /status (empty = disabled)")
	f.fs.StringVar(&f.profile, "storage-profile", "balanced", "storage engine tuning profile: balanced or ingest")
	f.fs.StringVar(&f.cacheSize, "cache-size", "", "block cache size, e.g. 2GiB or 512MB (default: the profile's share of memory — 25% capped at 8GiB for balanced, 10% capped at 2GiB for ingest)")
	f.fs.StringVar(&f.encKeyPath, "enc-key", "", "file holding the 32-byte store encryption key (raw or hex); enables encryption at rest (empty = plaintext). A comma-separated list is tried in order against the store, so a new key can be staged beside the current one before an online rotation")
	f.fs.Float64Var(&f.loadSplit, "load-split-threshold", 0, "sustained per-range QPS that triggers a load-based split (0 = default 500, negative = disabled)")
	f.fs.IntVar(&f.raftWorkers, "raft-workers", 0, "workers driving this node's raft groups (0 = one per CPU)")
	f.fs.BoolVar(&f.quiescence, "raft-quiescence", true, "let idle ranges stop ticking and heartbeating (cluster version v12)")
	f.fs.Int64Var(&f.mergeThr, "merge-size-threshold", 0, "size in bytes below which a range and its right neighbor are merged back together (0 = default 16 MiB, negative = disabled; disable to keep an empty pre-split, e.g. for a benchmark)")
	f.fs.Float64Var(&f.shedFactor, "lease-shed-factor", 0, "leader-QPS multiple of the cluster mean at which a node sheds hot leases (0 = default 1.5)")
	f.fs.Int64Var(&f.bytesThr, "rebalance-bytes-threshold", 0, "replica-byte spread that triggers byte-weighted replica moves (0 = default 64 MiB, negative = disabled)")
	f.fs.DurationVar(&f.consistInt, "consistency-interval", 0, "pace of the replica consistency sweep, one led range per interval (0 = disabled)")
	f.fs.DurationVar(&f.slowStmt, "slow-statement-threshold", 0, "SQL statements slower than this are kept for the dashboard's slow list (0 = default 500ms)")
	f.fs.DurationVar(&f.metricsRecord, "metrics-record-interval", 10*time.Second, "how often this node records its metrics into the datax_metrics table (0 disables recording)")
	f.fs.DurationVar(&f.drainTimeout, "drain-timeout", server.DefaultDrainTimeout, "on SIGTERM or Ctrl-C, how long to spend handing leases to peers and letting SQL connections finish before stopping (0 stops at once)")
	f.fs.BoolVar(&f.verbose, "v", false, "verbose (debug) logging")
	return f
}

func (f *serverFlags) config(bootstrap bool) (server.Config, error) {
	loc, err := base.ParseLocality(f.locality)
	if err != nil {
		return server.Config{}, err
	}
	prof, err := storage.ParseProfile(f.profile)
	if err != nil {
		return server.Config{}, err
	}
	cacheSize, err := parseBytes(f.cacheSize)
	if err != nil {
		return server.Config{}, fmt.Errorf("--cache-size: %v", err)
	}
	log.SetVerbose(f.verbose)
	return server.Config{
		StorageProfile:          prof,
		StorageCacheSize:        cacheSize,
		EncKeyPath:              f.encKeyPath,
		LoadSplitThreshold:      f.loadSplit,
		MergeSizeThreshold:      f.mergeThr,
		RaftWorkers:             f.raftWorkers,
		DisableQuiescence:       !f.quiescence,
		LeaseShedFactor:         f.shedFactor,
		ConsistencyInterval:     f.consistInt,
		SlowStatementThreshold:  f.slowStmt,
		MetricsRecordInterval:   metricsRecordInterval(f.metricsRecord),
		DrainTimeout:            f.drainTimeout,
		RebalanceBytesThreshold: f.bytesThr,
		Dir:                     f.dir,
		Listen:                  f.listen,
		PGListen:                f.pgListen,
		Join:                    f.join,
		BootstrapSelf:           bootstrap,
		Locality:                loc,
		MaxOffset:               f.maxOffset,
		AdvertiseAddr:           f.advertise,
		CertsDir:                f.certsDir,
		RootPassword:            f.rootPw,
		HTTPListen:              f.httpListen,
	}, nil
}

// parseBytes reads a byte count with an optional unit: 1073741824, 1GiB,
// 1GB, 512MiB, 64M ("" = 0).
func parseBytes(s string) (int64, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, nil
	}
	i := 0
	for i < len(s) && (s[i] >= '0' && s[i] <= '9' || s[i] == '.') {
		i++
	}
	if i == 0 {
		return 0, fmt.Errorf("%q is not a byte count", s)
	}
	n, err := strconv.ParseFloat(s[:i], 64)
	if err != nil {
		return 0, fmt.Errorf("%q is not a byte count", s)
	}
	unit := strings.ToLower(strings.TrimSpace(s[i:]))
	mult := map[string]float64{"": 1, "b": 1, "k": 1 << 10, "kb": 1 << 10, "kib": 1 << 10, "m": 1 << 20, "mb": 1 << 20, "mib": 1 << 20, "g": 1 << 30, "gb": 1 << 30, "gib": 1 << 30, "t": 1 << 40, "tb": 1 << 40, "tib": 1 << 40}
	m, ok := mult[unit]
	if !ok {
		return 0, fmt.Errorf("unknown unit %q in %q", unit, s)
	}
	return int64(n * m), nil
}

// metricsRecordInterval maps the flag (0 = off) onto the config's
// convention (negative = off, 0 = default).
func metricsRecordInterval(d time.Duration) time.Duration {
	if d <= 0 {
		return -1
	}
	return d
}

func runServer(cfg server.Config) error {
	n, err := server.Start(cfg)
	if err != nil {
		return err
	}
	fmt.Printf("datax node %s ready (protocol %s)\n", n.NodeID(), pversion.Current)
	fmt.Printf("  internode RPC: %s\n", n.Addr())
	if cfg.PGListen != "" {
		mode := "sslmode=disable"
		if cfg.CertsDir != "" {
			mode = "sslmode=verify-ca"
		}
		fmt.Printf("  SQL clients:   postgres://root@%s/datax?%s\n", n.SQLAddr(), mode)
	}

	ch := make(chan os.Signal, 3)
	signal.Notify(ch, os.Interrupt, syscall.SIGTERM)
	awaitShutdown(ch, shutdownHooks{
		timeout: cfg.DrainTimeout,
		drain:   n.Drain,
		stop:    n.Stop,
		exit:    os.Exit,
		out:     os.Stdout,
	})
	return nil
}

func runInit(args []string) error {
	f := newServerFlags("init")
	if err := f.fs.Parse(args); err != nil {
		return err
	}
	if f.join != "" {
		return fmt.Errorf("init starts a new cluster; use 'datax start --join' to join an existing one")
	}
	cfg, err := f.config(true)
	if err != nil {
		return err
	}
	return runServer(cfg)
}

func runStart(args []string) error {
	f := newServerFlags("start")
	if err := f.fs.Parse(args); err != nil {
		return err
	}
	cfg, err := f.config(false)
	if err != nil {
		return err
	}
	if cfg.Join == "" && cfg.Dir == "" {
		return fmt.Errorf("--join is required (or use 'datax init' to start a new cluster)")
	}
	return runServer(cfg)
}
