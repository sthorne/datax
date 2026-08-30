package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/sthorne/datax/pkg/base"
	"github.com/sthorne/datax/pkg/server"
	"github.com/sthorne/datax/pkg/storage"
	"github.com/sthorne/datax/pkg/util/log"
)

type serverFlags struct {
	fs         *flag.FlagSet
	dir        string
	listen     string
	pgListen   string
	join       string
	locality   string
	maxOffset  time.Duration
	advertise  string
	certsDir   string
	rootPw     string
	httpListen string
	profile    string
	encKeyPath string
	loadSplit  float64
	shedFactor float64
	bytesThr   int64
	verbose    bool
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
	f.fs.StringVar(&f.encKeyPath, "enc-key", "", "file holding the 32-byte store encryption key (raw or hex); enables encryption at rest (empty = plaintext)")
	f.fs.Float64Var(&f.loadSplit, "load-split-threshold", 0, "sustained per-range QPS that triggers a load-based split (0 = default 500, negative = disabled)")
	f.fs.Float64Var(&f.shedFactor, "lease-shed-factor", 0, "leader-QPS multiple of the cluster mean at which a node sheds hot leases (0 = default 1.5)")
	f.fs.Int64Var(&f.bytesThr, "rebalance-bytes-threshold", 0, "replica-byte spread that triggers byte-weighted replica moves (0 = default 64 MiB, negative = disabled)")
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
	log.SetVerbose(f.verbose)
	return server.Config{
		StorageProfile:          prof,
		EncKeyPath:              f.encKeyPath,
		LoadSplitThreshold:      f.loadSplit,
		LeaseShedFactor:         f.shedFactor,
		RebalanceBytesThreshold: f.bytesThr,
		Dir:                f.dir,
		Listen:             f.listen,
		PGListen:           f.pgListen,
		Join:               f.join,
		BootstrapSelf:      bootstrap,
		Locality:           loc,
		MaxOffset:          f.maxOffset,
		AdvertiseAddr:      f.advertise,
		CertsDir:           f.certsDir,
		RootPassword:       f.rootPw,
		HTTPListen:         f.httpListen,
	}, nil
}

func runServer(cfg server.Config) error {
	n, err := server.Start(cfg)
	if err != nil {
		return err
	}
	fmt.Printf("datax node %s ready\n", n.NodeID())
	fmt.Printf("  internode RPC: %s\n", n.Addr())
	if cfg.PGListen != "" {
		mode := "sslmode=disable"
		if cfg.CertsDir != "" {
			mode = "sslmode=verify-ca"
		}
		fmt.Printf("  SQL clients:   postgres://root@%s/datax?%s\n", n.SQLAddr(), mode)
	}

	ch := make(chan os.Signal, 1)
	signal.Notify(ch, os.Interrupt, syscall.SIGTERM)
	<-ch
	fmt.Println("shutting down...")
	n.Stop()
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
