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
	"github.com/sthorne/datax/pkg/storage/enc"
	"github.com/sthorne/datax/pkg/util/log"
	pversion "github.com/sthorne/datax/pkg/version"
)

type serverFlags struct {
	fs            *flag.FlagSet
	dir           string
	inMemory      bool
	listen        string
	pgListen      string
	join          string
	locality      string
	localityAddrs string
	maxOffset     time.Duration
	advertise     string
	certsDir      string
	rootPw        string
	httpListen    string
	profile       string
	cacheSize     string
	encKeyPath    string
	encRoot       string
	allowDumps    bool
	requireLocked bool
	acceptEncRB   bool
	loadSplit     float64
	mergeThr      int64
	raftWorkers   int
	quiescence    bool
	shedFactor    float64
	consistInt    time.Duration
	bytesThr      int64
	verbose       bool
	slowStmt      time.Duration
	authTimeout   time.Duration
	maxPending    int
	httpMaxConns  int
	keyCacheKeys  int
	metricsRecord time.Duration
	auditTable    bool
	drainTimeout  time.Duration
}

func newServerFlags(name string) *serverFlags {
	f := &serverFlags{fs: flag.NewFlagSet(name, flag.ContinueOnError)}
	f.fs.StringVar(&f.dir, "dir", "", "data directory (empty = in-memory, data is lost on exit; a warning says so unless --in-memory is given)")
	f.fs.BoolVar(&f.inMemory, "in-memory", false, "keep all data in memory, lost when the node exits (the default without --dir), without the startup warning; refused together with --dir")
	f.fs.StringVar(&f.listen, "listen", ":26257", "internode RPC address")
	f.fs.StringVar(&f.pgListen, "pg-listen", ":26433", "SQL (PostgreSQL protocol) address")
	f.fs.StringVar(&f.join, "join", "", "RPC address of any existing cluster node")
	f.fs.StringVar(&f.locality, "locality", "", "ordered failure-domain tiers, e.g. region=r1,rack=a")
	f.fs.DurationVar(&f.maxOffset, "max-offset", base.DefaultMaxClockOffset, "maximum tolerated clock offset between nodes")
	f.fs.StringVar(&f.advertise, "advertise", "", "address other nodes should use to reach this node (default: resolved listen address)")
	const locAdvUsage = "addresses peers sharing a --locality tier with this node use to reach it instead of --advertise, e.g. a private intra-region network: a comma-separated list of key=value@host:port, each key=value one of this node's --locality tiers; a peer dials the first entry whose tier it has too, and the public address when that one does not answer"
	f.fs.StringVar(&f.localityAddrs, "locality-advertise", "", locAdvUsage)
	f.fs.StringVar(&f.localityAddrs, "locality-advertise-addr", "", "alias of --locality-advertise")
	f.fs.StringVar(&f.certsDir, "certs-dir", "", "certificate directory; enables mutual internode TLS and SQL TLS+SCRAM (empty = insecure)")
	f.fs.StringVar(&f.rootPw, "root-password", "", "secure mode: seed the root SQL user's password at startup if unset")
	f.fs.StringVar(&f.httpListen, "http-listen", "", "observability address serving /metrics and /status (empty = disabled)")
	f.fs.StringVar(&f.profile, "storage-profile", "balanced", "storage engine tuning profile: balanced or ingest")
	f.fs.StringVar(&f.cacheSize, "cache-size", "", "block cache size, e.g. 2GiB or 512MB (default: the profile's share of memory — 25% capped at 8GiB for balanced, 10% capped at 2GiB for ingest)")
	f.fs.BoolVar(&f.acceptEncRB, "accept-encryption-rollback", false, "start a store whose switch to authenticated encryption was rolled back (its key registry or the whole store restored from before the switch), lowering the records that caught the rollback to what the store has; for the one start after a rollback that was meant")
	f.fs.StringVar(&f.encKeyPath, "enc-key", "", "file holding the 32-byte store encryption key (raw or hex); enables encryption at rest (empty = plaintext). A comma-separated list is tried in order against the store, so a new key can be staged beside the current one before an online rotation")
	f.fs.BoolVar(&f.allowDumps, "allow-core-dumps", false, "encrypted node: leave the process dumpable. By default a node with --enc-key or --enc-root sets its core-file limit to zero and marks itself not dumpable, because a core dump holds its keys")
	f.fs.BoolVar(&f.requireLocked, "require-locked-keys", false, "encrypted node: refuse to start unless key memory could be locked against swap and left out of core dumps (by default that is a warning with the remedy)")
	f.fs.StringVar(&f.encRoot, "enc-root", "", "the alternative to --enc-key: what the store key is kept under, file:<path> (a 32-byte wrapping key), exec:<command> (run with wrap, unwrap or key-id), shares (a root key split among operators: the node starts sealed until enough run `datax unseal`), or a key management service spoken to directly: vault:<address>/<mount>/<key>, awskms:<key arn>, gcpkms:projects/.../cryptoKeys/<key> or azurekv:<vault>/<key>[/<version>], each taking options as ?name=value&name=value (credentials are never options: they are files the options name, or come from the environment or the instance's identity); see docs/encryption.md. A node whose KMS cannot be reached does not start. The store key is made on the node and kept in the data directory only wrapped. A comma-separated list names candidates; the one that wrapped the store's key is used")
	f.fs.Float64Var(&f.loadSplit, "load-split-threshold", 0, "sustained per-range QPS that triggers a load-based split (0 = default 500, negative = disabled)")
	f.fs.IntVar(&f.raftWorkers, "raft-workers", 0, "workers driving this node's raft groups (0 = one per CPU)")
	f.fs.BoolVar(&f.quiescence, "raft-quiescence", true, "let idle ranges stop ticking and heartbeating (cluster version v12)")
	f.fs.Int64Var(&f.mergeThr, "merge-size-threshold", 0, "size in bytes below which a range and its right neighbor are merged back together (0 = default 16 MiB, negative = disabled; disable to keep an empty pre-split, e.g. for a benchmark)")
	f.fs.Float64Var(&f.shedFactor, "lease-shed-factor", 0, "leader-QPS multiple of the cluster mean at which a node sheds hot leases (0 = default 1.5)")
	f.fs.Int64Var(&f.bytesThr, "rebalance-bytes-threshold", 0, "replica-byte spread that triggers byte-weighted replica moves (0 = default 64 MiB, negative = disabled)")
	f.fs.DurationVar(&f.consistInt, "consistency-interval", 0, "pace of the replica consistency sweep, one led range per interval (0 = disabled)")
	f.fs.DurationVar(&f.slowStmt, "slow-statement-threshold", 0, "SQL statements slower than this are kept for the dashboard's slow list (0 = default 500ms)")
	f.fs.DurationVar(&f.authTimeout, "auth-timeout", 0, "how long an unauthenticated connection has to complete its handshake (SQL startup and authentication; on the HTTP port, a request's headers, and separately its body) before it is closed (0 = default 60s, negative = never)")
	f.fs.IntVar(&f.httpMaxConns, "http-max-conns", 0, "maximum open connections to the HTTP port; past it new connections are closed as they are accepted rather than held (0 = default 512, negative = no cap). Idle keep-alive connections are closed after 2 minutes")
	f.fs.IntVar(&f.keyCacheKeys, "key-cache-keys", 0, "named keys (a sealed table's, a tenant's, the key service's) this node keeps unwrapped at once, the least recently used dropped beyond it (0 = default 10000, negative = no bound); size it above the tenants a node serves within ten seconds")
	f.fs.IntVar(&f.maxPending, "sql-max-pending-auth", 0, "maximum SQL connections in the pre-authentication state; past it new connections are refused rather than held (0 = default 512, negative = no cap). Also the widest burst of concurrent password guesses one address can land at once, every one of which is charged: raising it past 600 lengthens how long a guessing address is held afterwards, and removing it removes that bound")
	f.fs.DurationVar(&f.metricsRecord, "metrics-record-interval", 10*time.Second, "how often this node records its metrics into the datax_metrics table (0 disables recording)")
	f.fs.BoolVar(&f.auditTable, "audit-table", true, "also write this node's audit records to the datax_audit table (false: they are in its log only)")
	f.fs.DurationVar(&f.drainTimeout, "drain-timeout", server.DefaultDrainTimeout, "on SIGTERM or Ctrl-C, how long to spend handing leases to peers and letting SQL connections finish before stopping (0 stops at once)")
	f.fs.BoolVar(&f.verbose, "v", false, "verbose (debug) logging")
	return f
}

func (f *serverFlags) config(bootstrap bool) (server.Config, error) {
	loc, err := base.ParseLocality(f.locality)
	if err != nil {
		return server.Config{}, err
	}
	locAddrs, err := base.ParseLocalityAddresses(f.localityAddrs, loc)
	if err != nil {
		return server.Config{}, fmt.Errorf("--locality-advertise: %v", err)
	}
	prof, err := storage.ParseProfile(f.profile)
	if err != nil {
		return server.Config{}, err
	}
	cacheSize, err := parseBytes(f.cacheSize)
	if err != nil {
		return server.Config{}, fmt.Errorf("--cache-size: %v", err)
	}
	if f.inMemory && f.dir != "" {
		return server.Config{}, fmt.Errorf("--in-memory and --dir are mutually exclusive")
	}
	log.SetVerbose(f.verbose)
	return server.Config{
		StorageProfile:           prof,
		StorageCacheSize:         cacheSize,
		EncKeyPath:               f.encKeyPath,
		EncRoot:                  f.encRoot,
		RequireLockedKeys:        f.requireLocked,
		AllowCoreDumps:           f.allowDumps,
		AcceptEncryptionRollback: f.acceptEncRB,
		LoadSplitThreshold:       f.loadSplit,
		MergeSizeThreshold:       f.mergeThr,
		RaftWorkers:              f.raftWorkers,
		DisableQuiescence:        !f.quiescence,
		LeaseShedFactor:          f.shedFactor,
		ConsistencyInterval:      f.consistInt,
		SlowStatementThreshold:   f.slowStmt,
		AuthTimeout:              f.authTimeout,
		SQLMaxPendingAuth:        f.maxPending,
		HTTPMaxConns:             f.httpMaxConns,
		KeyCacheMaxKeys:          f.keyCacheKeys,
		MetricsRecordInterval:    metricsRecordInterval(f.metricsRecord),
		AuditFlushInterval:       auditFlushInterval(f.auditTable),
		DrainTimeout:             f.drainTimeout,
		RebalanceBytesThreshold:  f.bytesThr,
		Dir:                      f.dir,
		Listen:                   f.listen,
		PGListen:                 f.pgListen,
		Join:                     f.join,
		BootstrapSelf:            bootstrap,
		Locality:                 loc,
		LocalityAddresses:        locAddrs,
		MaxOffset:                f.maxOffset,
		AdvertiseAddr:            f.advertise,
		CertsDir:                 f.certsDir,
		RootPassword:             f.rootPw,
		HTTPListen:               f.httpListen,
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

// auditFlushInterval maps --audit-table onto the config's convention
// (0 = the default pace, negative = off).
func auditFlushInterval(on bool) time.Duration {
	if on {
		return 0
	}
	return -1
}

// metricsRecordInterval maps the flag (0 = off) onto the config's
// convention (negative = off, 0 = default).
func metricsRecordInterval(d time.Duration) time.Duration {
	if d <= 0 {
		return -1
	}
	return d
}

// inMemoryWarning is the warning a node with no data directory prints at
// startup, or "" when it has one or --in-memory says that is meant. An
// in-memory node is the default (for trying datax out), so a production
// start that forgot --dir, or whose --dir=$VAR was empty, would
// otherwise run normally until its first restart lost everything.
func (f *serverFlags) inMemoryWarning() string {
	if f.dir != "" || f.inMemory {
		return ""
	}
	return "WARNING: no --dir given: this node keeps ALL its data in memory, and everything it holds is lost when it exits or restarts.\n" +
		"  For data that must survive, stop it and start again with --dir <path>. To run in memory on purpose, pass --in-memory.\n"
}

// warnInMemory prints the in-memory warning, if any, to stderr as a
// banner and to the log.
func (f *serverFlags) warnInMemory() {
	if w := f.inMemoryWarning(); w != "" {
		fmt.Fprint(os.Stderr, "\n"+w+"\n")
		log.Warnf("running in memory (no --dir): all data is lost when the node exits")
	}
}

func runServer(cfg server.Config) error {
	if (cfg.EncKeyPath != "" || cfg.EncRoot != "") && !cfg.AllowCoreDumps {
		// Before any key is read: a dump of this process would hold them.
		if err := enc.DisableCoreDumps(); err != nil {
			if cfg.RequireLockedKeys {
				return fmt.Errorf("--require-locked-keys: core dumps could not be disabled: %w", err)
			}
			fmt.Fprintf(os.Stderr, "warning: core dumps could not be disabled (%v); a dump of this process would hold its keys\n", err)
		}
	}
	cfg.OnSealed = func(addr string) {
		fmt.Printf("datax node is SEALED: its store key is under a root key held as shares, and the store stays closed until enough are submitted\n  submit a share: datax unseal --addr %s\n  progress:       datax debug seal-status --addr %s\n", addr, addr)
	}
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
	f.warnInMemory()
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
	f.warnInMemory()
	return runServer(cfg)
}
