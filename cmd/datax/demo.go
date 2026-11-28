package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/sthorne/datax/pkg/base"
	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/kvpb"
	"github.com/sthorne/datax/pkg/server"
	"github.com/sthorne/datax/pkg/util/log"
)

// runDemo starts an in-process multi-node cluster spread across simulated
// racks — the five-minute datax experience. Data is in-memory and lost on
// exit.
func runDemo(args []string) error {
	fs := flag.NewFlagSet("demo", flag.ContinueOnError)
	nodes := fs.Int("nodes", 3, "number of nodes (1-9)")
	basePG := fs.Int("pg-port", 26433, "first SQL port (node i listens on port+i-1)")
	baseRPC := fs.Int("rpc-port", 26257, "first internode RPC port")
	baseHTTP := fs.Int("http-port", 0, "first observability HTTP port (0 = disabled; node i listens on port+i-1)")
	verbose := fs.Bool("v", false, "verbose logging")
	mergeThr := fs.Int64("merge-size-threshold", 0, "size in bytes below which a range and its right neighbor are merged back together (0 = default 16 MiB, negative = disabled)")
	gcTTL := fs.Duration("gc-ttl", 0, "how long MVCC history, and the rows of dropped tables, are kept before they are reclaimed (0 = default 25h). A demo is in memory: a test suite that creates and drops databases over and over holds everything it dropped in memory for this long, so give such a cluster a short window, e.g. 5m")
	locSpec := fs.String("locality", "", "each node's locality, one per node separated by ';' (e.g. 'region=us-east-1;region=us-east-2;region=eu-west-1'), or one for every node; default region=demo,rack=<a…i>")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *gcTTL != 0 && *gcTTL < time.Second {
		return fmt.Errorf("--gc-ttl must be at least 1s (or 0 for the default)")
	}
	if *nodes < 1 || *nodes > 9 {
		return fmt.Errorf("--nodes must be between 1 and 9")
	}
	log.SetVerbose(*verbose)
	locs, err := demoLocalities(*locSpec, *nodes)
	if err != nil {
		return err
	}
	var started []*server.Node
	defer func() {
		for i := len(started) - 1; i >= 0; i-- {
			started[i].Stop()
		}
	}()

	fmt.Printf("Starting a %d-node in-memory datax cluster across racks...\n", *nodes)
	for i := 0; i < *nodes; i++ {
		loc := locs[i]
		cfg := server.Config{
			Listen:                fmt.Sprintf("127.0.0.1:%d", *baseRPC+i),
			PGListen:              fmt.Sprintf("127.0.0.1:%d", *basePG+i),
			Locality:              loc,
			UpreplicationInterval: time.Second,
			MergeSizeThreshold:    *mergeThr,
			GCTTL:                 *gcTTL,
		}
		if *baseHTTP > 0 {
			cfg.HTTPListen = fmt.Sprintf("127.0.0.1:%d", *baseHTTP+i)
		}
		if i == 0 {
			cfg.BootstrapSelf = true
		} else {
			cfg.Join = started[0].Addr()
		}
		n, err := server.Start(cfg)
		if err != nil {
			return fmt.Errorf("starting node %d: %w", i+1, err)
		}
		started = append(started, n)
		fmt.Printf("  %s  %s  sql=%s  rpc=%s\n", n.NodeID(), loc, n.SQLAddr(), n.Addr())
	}

	if *nodes >= 3 {
		fmt.Println("\nWaiting for ranges to replicate across racks...")
		waitForRF3(started[0])
	}

	fmt.Println()
	fmt.Println("Cluster ready. Connect with any PostgreSQL client:")
	fmt.Println()
	fmt.Printf("  psql \"postgres://root@%s/datax?sslmode=disable\"\n", started[0].SQLAddr())
	fmt.Printf("  datax sql --url \"postgres://root@%s/datax?sslmode=disable\"\n", started[0].SQLAddr())
	fmt.Println()
	if *baseHTTP > 0 {
		fmt.Printf("Dashboard: http://127.0.0.1:%d/\n", *baseHTTP)
		fmt.Println()
	}
	fmt.Println("Try:")
	fmt.Println("  CREATE TABLE accounts (id INT8 PRIMARY KEY, balance INT8);")
	fmt.Println("  INSERT INTO accounts VALUES (1, 100), (2, 100);")
	fmt.Println("  BEGIN; UPDATE accounts SET balance = balance - 10 WHERE id = 1;")
	fmt.Println("  UPDATE accounts SET balance = balance + 10 WHERE id = 2; COMMIT;")
	fmt.Println("  SELECT * FROM accounts;")
	fmt.Println()
	fmt.Println("Press Ctrl-C to shut down (in-memory data is discarded).")

	ch := make(chan os.Signal, 3)
	signal.Notify(ch, os.Interrupt, syscall.SIGTERM)
	fmt.Println()
	awaitShutdown(ch, shutdownHooks{
		timeout: 5 * time.Second,
		drain: func(ctx context.Context) server.DrainReport {
			// Every node is going: drain them together (their SQL
			// connections end cleanly; leases have nowhere to go).
			var mu sync.Mutex
			var wg sync.WaitGroup
			var total server.DrainReport
			for _, n := range started {
				wg.Add(1)
				go func(n *server.Node) {
					defer wg.Done()
					r := n.Drain(ctx)
					mu.Lock()
					total.LeasesTransferred += r.LeasesTransferred
					total.LeasesKept += r.LeasesKept
					total.ConnsClosed += r.ConnsClosed
					total.ConnsCut += r.ConnsCut
					if r.Incomplete != "" {
						total.Incomplete = r.Incomplete
					}
					mu.Unlock()
				}(n)
			}
			wg.Wait()
			return total
		},
		stop: func() {
			for i := len(started) - 1; i >= 0; i-- {
				started[i].Stop()
			}
		},
		exit: os.Exit,
		out:  os.Stdout,
	})
	return nil
}

// waitForRF3 polls until every range reports 3 replicas (best effort, 30s).
func waitForRF3(n *server.Node) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	for ctx.Err() == nil {
		descs, err := listRangesFor(ctx, n)
		if err == nil && len(descs) > 0 {
			done := true
			for _, d := range descs {
				if len(d.Replicas) < base.DefaultReplicationFactor {
					done = false
					break
				}
			}
			if done {
				for _, d := range descs {
					fmt.Printf("  %s [%s, %s): %d replicas\n", d.RangeID, d.StartKey, d.EndKey, len(d.Replicas))
				}
				return
			}
		}
		time.Sleep(300 * time.Millisecond)
	}
	fmt.Println("  (still replicating in the background)")
}

func listRangesFor(ctx context.Context, n *server.Node) ([]kvpb.RangeDescriptor, error) {
	start, end := keys.MetaSpan()
	rows, err := n.DB().Scan(ctx, start, end, 0)
	if err != nil {
		return nil, err
	}
	var out []kvpb.RangeDescriptor
	for _, kv := range rows {
		var d kvpb.RangeDescriptor
		if err := json.Unmarshal(kv.Value, &d); err == nil {
			out = append(out, d)
		}
	}
	return out, nil
}

// demoLocalities resolves --locality for n demo nodes (issue #405):
// empty gives the simulated racks, region=demo,rack=<a…i>; one locality
// applies to every node; otherwise there must be exactly one per node,
// separated by ';'.
func demoLocalities(spec string, n int) ([]base.Locality, error) {
	out := make([]base.Locality, n)
	if strings.TrimSpace(spec) == "" {
		racks := "abcdefghi"
		for i := range out {
			out[i], _ = base.ParseLocality(fmt.Sprintf("region=demo,rack=%c", racks[i]))
		}
		return out, nil
	}
	parts := strings.Split(spec, ";")
	if len(parts) != 1 && len(parts) != n {
		return nil, fmt.Errorf("--locality names %d localities for %d nodes: give one per node, separated by ';', or one for all", len(parts), n)
	}
	for i := range out {
		text := strings.TrimSpace(parts[0])
		if len(parts) > 1 {
			text = strings.TrimSpace(parts[i])
		}
		l, err := base.ParseLocality(text)
		if err != nil {
			return nil, fmt.Errorf("--locality for node %d: %w", i+1, err)
		}
		out[i] = l
	}
	return out, nil
}
