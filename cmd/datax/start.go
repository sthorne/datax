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
	"github.com/sthorne/datax/pkg/util/log"
)

type serverFlags struct {
	fs        *flag.FlagSet
	dir       string
	listen    string
	pgListen  string
	join      string
	locality  string
	maxOffset time.Duration
	advertise string
	verbose   bool
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
	f.fs.BoolVar(&f.verbose, "v", false, "verbose (debug) logging")
	return f
}

func (f *serverFlags) config(bootstrap bool) (server.Config, error) {
	loc, err := base.ParseLocality(f.locality)
	if err != nil {
		return server.Config{}, err
	}
	log.SetVerbose(f.verbose)
	return server.Config{
		Dir:           f.dir,
		Listen:        f.listen,
		PGListen:      f.pgListen,
		Join:          f.join,
		BootstrapSelf: bootstrap,
		Locality:      loc,
		MaxOffset:     f.maxOffset,
		AdvertiseAddr: f.advertise,
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
		fmt.Printf("  SQL clients:   postgres://root@%s/datax?sslmode=disable\n", n.SQLAddr())
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
