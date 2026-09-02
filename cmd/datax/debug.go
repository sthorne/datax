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
	osuser "os/user"
	"path/filepath"
	"time"

	"github.com/cockroachdb/pebble/vfs"

	"github.com/sthorne/datax/pkg/base"
	"github.com/sthorne/datax/pkg/cluster"
	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/rpc"
	"github.com/sthorne/datax/pkg/security"
	"github.com/sthorne/datax/pkg/server"
	"github.com/sthorne/datax/pkg/storage/enc"
	"github.com/sthorne/datax/pkg/util/hlc"
	"github.com/sthorne/datax/pkg/util/log"
)

// osUsername names the operating-system user for offline-command audit
// records ("unknown" when unresolvable).
func osUsername() string {
	if u, err := osuser.Current(); err == nil && u.Username != "" {
		return u.Username
	}
	return "unknown"
}

// loadKeyCandidates resolves an --enc-key flag value: no key (empty), one
// key file, or a comma-separated list of candidates (see enc.MatchStoreKey).
func loadKeyCandidates(value string) ([][]byte, error) {
	paths := enc.SplitKeyPaths(value)
	if len(paths) == 0 {
		return nil, nil
	}
	return enc.LoadKeyFiles(paths)
}

// runDebugMetadata prints the node's periodic metadata export (written to
// the data directory every heartbeat; the recovery artifact for a cluster
// whose metadata range lost quorum).
func runDebugMetadata(args []string) error {
	fs := flag.NewFlagSet("debug metadata", flag.ContinueOnError)
	dir := fs.String("dir", "", "data directory of a (stopped or running) node")
	keyPath := fs.String("enc-key", "", "store encryption key file (for an encrypted store's sealed backup); a comma-separated list is tried in order")
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
	if len(raw) >= len(server.MetadataBackupMagic) && string(raw[:len(server.MetadataBackupMagic)]) == server.MetadataBackupMagic {
		keys, err := loadKeyCandidates(*keyPath)
		if err != nil {
			return err
		}
		if len(keys) == 0 {
			return fmt.Errorf("metadata backup is sealed (encrypted store); --enc-key is required")
		}
		var plain []byte
		for _, key := range keys {
			if plain, err = enc.Unseal(server.MetadataBackupMagic, key, raw); err == nil {
				break
			}
		}
		if err != nil {
			return err
		}
		raw = plain
	}
	_, err = os.Stdout.Write(raw)
	return err
}

// runDebugRotateEncKey reseals an encrypted store's key registry (and
// sealed metadata backup) under a new store key. Data keys — and so the
// file contents — are untouched; only the wrapping changes. Two modes:
// --addr rotates a LIVE node over the admin RPC (the node reseals
// atomically and swaps the key it seals artifacts with); --dir is the
// offline path for a stopped or damaged store.
func runDebugRotateEncKey(args []string) error {
	fs := flag.NewFlagSet("debug rotate-enc-key", flag.ContinueOnError)
	dir := fs.String("dir", "", "offline mode: data directory of the STOPPED node")
	addr := fs.String("addr", "", "online mode: RPC address of the RUNNING node to rotate")
	oldPath := fs.String("old-key", "", "current store encryption key file")
	newPath := fs.String("new-key", "", "new store encryption key file")
	certsDir := fs.String("certs-dir", "", "online mode, secure cluster: certificate directory (presents client.<user>.crt)")
	certUser := fs.String("user", "root", "online mode: username whose client certificate authenticates the call; must be an admin")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *oldPath == "" || *newPath == "" {
		return fmt.Errorf("rotate-enc-key requires --old-key and --new-key")
	}
	if (*dir == "") == (*addr == "") {
		return fmt.Errorf("rotate-enc-key requires exactly one of --dir (stopped node) or --addr (running node)")
	}
	if *addr != "" {
		if *certsDir == "" {
			return fmt.Errorf("online rotation sends the store keys to the node and requires --certs-dir (mutual TLS); use --dir on a stopped node otherwise")
		}
		oldKey, err := enc.LoadKeyFile(*oldPath)
		if err != nil {
			return err
		}
		newKey, err := enc.LoadKeyFile(*newPath)
		if err != nil {
			return err
		}
		if _, err := adminCall(*addr, *certsDir, *certUser, cluster.AdminRequest{
			Op: "rotate-store-key", OldStoreKey: oldKey, NewStoreKey: newKey,
		}); err != nil {
			return err
		}
		fmt.Println("store key rotated online; the node now seals with the new key (keep the new key file for restarts)")
		return nil
	}
	oldKey, err := enc.LoadKeyFile(*oldPath)
	if err != nil {
		return err
	}
	newKey, err := enc.LoadKeyFile(*newPath)
	if err != nil {
		return err
	}
	if err := enc.RotateStoreKey(vfs.Default, *dir, oldKey, newKey); err != nil {
		return err
	}
	// Reseal the metadata backup too, so one key opens everything.
	bakPath := filepath.Join(*dir, server.MetadataBackupFile)
	if raw, err := os.ReadFile(bakPath); err == nil &&
		len(raw) >= len(server.MetadataBackupMagic) && string(raw[:len(server.MetadataBackupMagic)]) == server.MetadataBackupMagic {
		plain, err := enc.Unseal(server.MetadataBackupMagic, oldKey, raw)
		if err != nil {
			return fmt.Errorf("resealing metadata backup: %w", err)
		}
		sealed, err := enc.Seal(server.MetadataBackupMagic, newKey, plain)
		if err != nil {
			return err
		}
		if err := os.WriteFile(bakPath+".tmp", sealed, 0o600); err != nil {
			return err
		}
		if err := os.Rename(bakPath+".tmp", bakPath); err != nil {
			return err
		}
	}
	fmt.Println("store key rotated; restart the node with the new key")
	return nil
}

// runDebugUnsafeRecover rewrites a STOPPED store's range descriptors to
// single-replica membership so ranges that lost quorum serve again.
func runDebugUnsafeRecover(args []string) error {
	fs := flag.NewFlagSet("debug unsafe-recover", flag.ContinueOnError)
	dir := fs.String("dir", "", "data directory of the STOPPED surviving node")
	rangeID := fs.Int64("range", 0, "recover only this range (default: every range on the store)")
	keyPath := fs.String("enc-key", "", "store encryption key file (required for an encrypted store); a comma-separated list is tried in order")
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
	keys, err := loadKeyCandidates(*keyPath)
	if err != nil {
		return err
	}
	var key []byte
	if len(keys) > 0 {
		idx, err := enc.MatchStoreKey(vfs.Default, *dir, keys)
		if err != nil {
			return err
		}
		key = keys[idx]
	}
	descs, err := server.UnsafeRecover(*dir, base.RangeID(*rangeID), key)
	if err != nil {
		return err
	}
	// Offline command: no cluster principal exists, so the audit record
	// carries the operating-system user who ran it.
	log.Audit("unsafe-recover", "dir", *dir, "range", *rangeID, "principal", "os:"+osUsername())
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
	certsDir := fs.String("certs-dir", "", "certificate directory for a secure cluster (presents client.<user>.crt)")
	certUser := fs.String("user", "root", "username whose client certificate authenticates this call (with --certs-dir)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	client := &http.Client{Timeout: 10 * time.Second}
	tlsCfg := &tls.Config{}
	if *certsDir != "" {
		var err error
		if tlsCfg, err = security.LoadClientTLS(*certsDir, *certUser); err != nil {
			return err
		}
	}
	tlsCfg.InsecureSkipVerify = *insecureTLS
	if *certsDir != "" || *insecureTLS {
		client.Transport = &http.Transport{TLSClientConfig: tlsCfg}
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
		return fmt.Errorf("usage: datax debug <split|merge|ranges|nodes|rebalance|transfer-lease|decommission|upgrade|status|metadata|unsafe-recover|rotate-enc-key|reencrypt|reencrypt-status> [flags]")
	}
	sub, rest := args[0], args[1:]
	switch sub {
	case "status":
		return runDebugStatus(rest)
	case "metadata":
		return runDebugMetadata(rest)
	case "unsafe-recover":
		return runDebugUnsafeRecover(rest)
	case "rotate-enc-key":
		return runDebugRotateEncKey(rest)
	}
	fs := flag.NewFlagSet("debug "+sub, flag.ContinueOnError)
	addr := fs.String("addr", "127.0.0.1:26257", "RPC address of any cluster node")
	certsDir := fs.String("certs-dir", "", "certificate directory for a secure cluster (presents client.<user>.crt)")
	certUser := fs.String("user", "root", "username whose client certificate authenticates this call (with --certs-dir); state-changing ops require an admin")
	table := fs.Uint64("table", 0, "split: split at the boundary of this table ID")
	rawKey := fs.String("key", "", "split: raw split key (hex)")
	rangeID := fs.Int64("range", 0, "rebalance, transfer-lease: range ID")
	toNode := fs.Int64("to", 0, "rebalance, transfer-lease: destination node ID; upgrade: target cluster version")
	fromNode := fs.Int64("from", 0, "rebalance: source node ID (default: chosen automatically)")
	nodeID := fs.Int64("node", 0, "decommission: node ID to drain")
	cancelDrain := fs.Bool("cancel", false, "decommission: stop draining the node instead")
	wait := fs.Bool("wait", false, "decommission: block until the node holds no replicas")
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
	case "rebalance", "transfer-lease":
		req.RangeID = base.RangeID(*rangeID)
		req.ToNode = base.NodeID(*toNode)
		req.FromNode = base.NodeID(*fromNode)
	case "merge":
		if *rangeID == 0 {
			return fmt.Errorf("merge requires --range (the left-hand range)")
		}
		req.RangeID = base.RangeID(*rangeID)
	case "decommission":
		if *nodeID == 0 {
			return fmt.Errorf("decommission requires --node")
		}
		req.NodeID = base.NodeID(*nodeID)
		req.Cancel = *cancelDrain
	case "upgrade":
		req.Op = "upgrade-cluster"
		req.Version = int(*toNode)
	case "reencrypt", "reencrypt-status":
	case "ranges", "nodes":
	default:
		return fmt.Errorf("unknown debug subcommand %q", sub)
	}

	trans := rpc.NewTransport(hlc.NewClock(nil, base.DefaultMaxClockOffset), nil, nil)
	if *certsDir != "" {
		tlsCfg, err := security.LoadClientTLS(*certsDir, *certUser)
		if err != nil {
			return err
		}
		trans.SetTLS(tlsCfg)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	var resp cluster.AdminResponse
	if err := trans.Call(ctx, *addr, "admin", req, &resp); err != nil {
		return err
	}
	if resp.Error != "" {
		return fmt.Errorf("%s", resp.Error)
	}
	if sub == "decommission" && *wait && !*cancelDrain {
		for resp.RemainingReplicas > 0 {
			fmt.Printf("node n%d draining: %d replicas remaining\n", *nodeID, resp.RemainingReplicas)
			time.Sleep(2 * time.Second)
			wctx, wcancel := context.WithTimeout(context.Background(), 30*time.Second)
			err := trans.Call(wctx, *addr, "admin", req, &resp)
			wcancel()
			if err != nil {
				return err
			}
			if resp.Error != "" {
				return fmt.Errorf("%s", resp.Error)
			}
		}
	}
	switch sub {
	case "split":
		fmt.Printf("split done:\n  left:  %s\n  right: %s\n", resp.Left, resp.Right)
	case "ranges":
		for _, d := range resp.Ranges {
			fmt.Printf("%s\n", &d)
		}
	case "nodes":
		if resp.ClusterVersion != 0 {
			fmt.Printf("cluster version: v%d\n", resp.ClusterVersion)
		}
		for _, nd := range resp.Nodes {
			age := time.Duration(time.Now().UnixNano()-nd.LivenessTime) * time.Nanosecond
			state := ""
			if nd.Draining {
				state = "  DRAINING"
			}
			bv := nd.BinaryVersion
			if bv == 0 {
				bv = 1
			}
			fmt.Printf("%s  %s  locality=%s  version=v%d  last-heartbeat=%s ago%s\n", nd.NodeID, nd.Address, nd.Locality, bv, age.Round(time.Second), state)
		}
	case "rebalance":
		fmt.Println("rebalance done")
	case "transfer-lease":
		fmt.Println("lease transferred")
	case "upgrade":
		fmt.Printf("cluster version finalized at v%d\n", resp.ClusterVersion)
	case "merge":
		if len(resp.Ranges) == 1 {
			fmt.Printf("merged: %s\n", &resp.Ranges[0])
		}
	case "decommission":
		switch {
		case *cancelDrain:
			fmt.Printf("node n%d no longer draining\n", *nodeID)
		case resp.RemainingReplicas == 0:
			fmt.Printf("node n%d drained; safe to stop\n", *nodeID)
		default:
			fmt.Printf("node n%d draining: %d replicas remaining (re-run or use --wait to follow)\n", *nodeID, resp.RemainingReplicas)
		}
	case "reencrypt", "reencrypt-status":
		printReencryption(resp.Reencryption)
		if sub == "reencrypt" && *wait {
			// Follow the worker until it exits; it stops on its own when
			// nothing more can be rewritten, so looping on the remaining
			// bytes would never return.
			for resp.Reencryption != nil && resp.Reencryption.Active {
				time.Sleep(2 * time.Second)
				wctx, wcancel := context.WithTimeout(context.Background(), 30*time.Second)
				err := trans.Call(wctx, *addr, "admin", cluster.AdminRequest{Op: "reencrypt-status"}, &resp)
				wcancel()
				if err != nil {
					return err
				}
				if resp.Error != "" {
					return fmt.Errorf("%s", resp.Error)
				}
				printReencryption(resp.Reencryption)
			}
			if st := resp.Reencryption; st != nil {
				if st.SweepError != "" {
					return fmt.Errorf("re-encryption status unknown: stale-file sweep failed: %s", st.SweepError)
				}
				if st.RemainingBytes > 0 {
					return fmt.Errorf("re-encryption stopped with %d bytes in %d files still under retired keys (see the node log); re-run later", st.RemainingBytes, st.RemainingFiles)
				}
			}
		}
	}
	return nil
}

func printReencryption(st *cluster.ReencryptionStatus) {
	if st == nil {
		fmt.Println("no re-encryption status reported")
		return
	}
	state := "idle"
	if st.Active {
		state = "running"
	}
	if !st.Active && st.RemainingBytes == 0 && st.SweepError == "" {
		state = "complete — no live sstable under a retired data key"
	}
	if st.SweepError != "" {
		state += " (stale-file sweep FAILED: " + st.SweepError + "; counts are the last good reading)"
	}
	fmt.Printf("re-encryption %s: %d bytes in %d files remain under retired keys (%d bytes rewritten)\n",
		state, st.RemainingBytes, st.RemainingFiles, st.RewrittenBytes)
}
