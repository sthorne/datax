package main

import (
	"context"
	"crypto/tls"
	"encoding/hex"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"github.com/sthorne/datax/pkg/base"
	"github.com/sthorne/datax/pkg/cluster"
	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/rpc"
	"github.com/sthorne/datax/pkg/server"
	"github.com/sthorne/datax/pkg/util/hlc"
)

// runDebugMetadata prints the node's periodic metadata export (written to
// the data directory every heartbeat; the recovery artifact for a cluster
// whose metadata range lost quorum).
func runDebugMetadata(args []string) error {
	fs := flag.NewFlagSet("debug metadata", flag.ContinueOnError)
	dir := fs.String("dir", "", "data directory of a (stopped or running) node")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *dir == "" {
		return fmt.Errorf("metadata requires --dir")
	}
	raw, err := os.ReadFile(filepath.Join(*dir, server.MetadataBackupFile))
	if err != nil {
		return fmt.Errorf("no metadata backup found: %w", err)
	}
	_, err = os.Stdout.Write(raw)
	return err
}

// runDebugUnsafeRecover rewrites a STOPPED store's range descriptors to
// single-replica membership so ranges that lost quorum serve again.
func runDebugUnsafeRecover(args []string) error {
	fs := flag.NewFlagSet("debug unsafe-recover", flag.ContinueOnError)
	dir := fs.String("dir", "", "data directory of the STOPPED surviving node")
	rangeID := fs.Int64("range", 0, "recover only this range (default: every range on the store)")
	yes := fs.Bool("yes", false, "confirm: I understand this discards the other replicas")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *dir == "" {
		return fmt.Errorf("unsafe-recover requires --dir")
	}
	if !*yes {
		return fmt.Errorf(`unsafe-recover rewrites range membership to THIS NODE ALONE.

It discards the other replicas' votes and any writes only they had
acknowledged. Run it on exactly ONE survivor per range, with the node
STOPPED — and never restart the removed peers with their old data: wipe
them and rejoin fresh. Replication is restored by upreplication once new
nodes join.

Re-run with --yes to proceed`)
	}
	descs, err := server.UnsafeRecover(*dir, base.RangeID(*rangeID))
	if err != nil {
		return err
	}
	if len(descs) == 0 {
		fmt.Println("nothing to recover (no multi-replica ranges on this store)")
		return nil
	}
	for _, d := range descs {
		fmt.Printf("recovered %s [%s, %s) to single-replica membership\n", d.RangeID, d.StartKey, d.EndKey)
	}
	fmt.Println("restart the node to serve; join fresh nodes to restore replication")
	return nil
}

// runDebugStatus fetches a node's /status document from its observability
// endpoint (--http-listen).
func runDebugStatus(args []string) error {
	fs := flag.NewFlagSet("debug status", flag.ContinueOnError)
	url := fs.String("url", "http://127.0.0.1:8080/status", "a node's /status URL")
	insecureTLS := fs.Bool("insecure-skip-verify", false, "skip TLS certificate verification")
	if err := fs.Parse(args); err != nil {
		return err
	}
	client := &http.Client{Timeout: 10 * time.Second}
	if *insecureTLS {
		client.Transport = &http.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: true}}
	}
	resp, err := client.Get(*url)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	_, err = io.Copy(os.Stdout, resp.Body)
	return err
}

func runDebug(args []string) error {
	if len(args) < 1 {
		return fmt.Errorf("usage: datax debug <split|ranges|nodes|rebalance|status|metadata|unsafe-recover> [flags]")
	}
	sub, rest := args[0], args[1:]
	switch sub {
	case "status":
		return runDebugStatus(rest)
	case "metadata":
		return runDebugMetadata(rest)
	case "unsafe-recover":
		return runDebugUnsafeRecover(rest)
	}
	fs := flag.NewFlagSet("debug "+sub, flag.ContinueOnError)
	addr := fs.String("addr", "127.0.0.1:26257", "RPC address of any cluster node")
	table := fs.Uint64("table", 0, "split: split at the boundary of this table ID")
	rawKey := fs.String("key", "", "split: raw split key (hex)")
	rangeID := fs.Int64("range", 0, "rebalance: range ID")
	toNode := fs.Int64("to", 0, "rebalance: destination node ID")
	fromNode := fs.Int64("from", 0, "rebalance: source node ID (default: chosen automatically)")
	if err := fs.Parse(rest); err != nil {
		return err
	}

	req := cluster.AdminRequest{Op: sub}
	switch sub {
	case "split":
		switch {
		case *table != 0:
			req.Key = keys.TableDataPrefix(*table)
		case *rawKey != "":
			k, err := hex.DecodeString(*rawKey)
			if err != nil {
				return fmt.Errorf("--key must be hex: %w", err)
			}
			req.Key = k
		default:
			return fmt.Errorf("split requires --table or --key")
		}
	case "rebalance":
		req.RangeID = base.RangeID(*rangeID)
		req.ToNode = base.NodeID(*toNode)
		req.FromNode = base.NodeID(*fromNode)
	case "ranges", "nodes":
	default:
		return fmt.Errorf("unknown debug subcommand %q", sub)
	}

	trans := rpc.NewTransport(hlc.NewClock(nil, base.DefaultMaxClockOffset), nil, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	var resp cluster.AdminResponse
	if err := trans.Call(ctx, *addr, "admin", req, &resp); err != nil {
		return err
	}
	if resp.Error != "" {
		return fmt.Errorf("%s", resp.Error)
	}
	switch sub {
	case "split":
		fmt.Printf("split done:\n  left:  %s\n  right: %s\n", resp.Left, resp.Right)
	case "ranges":
		for _, d := range resp.Ranges {
			fmt.Printf("%s\n", &d)
		}
	case "nodes":
		for _, nd := range resp.Nodes {
			age := time.Duration(time.Now().UnixNano()-nd.LivenessTime) * time.Nanosecond
			fmt.Printf("%s  %s  locality=%s  last-heartbeat=%s ago\n", nd.NodeID, nd.Address, nd.Locality, age.Round(time.Second))
		}
	case "rebalance":
		fmt.Println("rebalance done")
	}
	return nil
}
