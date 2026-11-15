package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"strings"

	"github.com/sthorne/datax/pkg/cluster"
	"github.com/sthorne/datax/pkg/storage/enc"
	"github.com/sthorne/datax/pkg/util/log"
)

// runDebugClusterKey manages the cluster key (issue #310;
// docs/encryption.md, "Cluster key"):
//
//	status   whether it is enabled, its id and version, who holds it
//	init     enable it: one key made, handed to every node, recorded
//	rotate   a new version handed to every node, named keys rewrapped,
//	         the old version destroyed
//	sync     hand it to live nodes that lack it
//	export   write the cluster's keys to a file, wrapped under a root the
//	         operator names here, for another cluster to import
//	import   give this cluster another's keys from such a file, so that
//	         the other's backups restore here
//
// The roots of export and import are the operator's: they are used by
// this command, on this machine, and never named to a node.
func runDebugClusterKey(args []string) error {
	const usage = "usage: datax debug cluster-key <status|init|rotate|sync|export|import> --addr <node> --certs-dir <dir> [flags]"
	if len(args) < 1 {
		return errors.New(usage)
	}
	action, rest := args[0], args[1:]
	fs := flag.NewFlagSet("debug cluster-key "+action, flag.ContinueOnError)
	addr := fs.String("addr", "127.0.0.1:26257", "RPC address of a running node")
	certsDir := fs.String("certs-dir", "", "certificate directory (presents client.<user>.crt); the cluster key needs a secure cluster")
	certUser := fs.String("user", "root", "username whose client certificate authenticates the call; must be an admin")
	toRoot := fs.String("to-root", "", "export: the root to wrap the keys under — file:<path>, exec:<command>, or a KMS (vault:, awskms:, gcpkms:, azurekv:)")
	fromRoot := fs.String("from-root", "", "import: the root the export was wrapped under")
	out := fs.String("out", "", "export: the file to write (created, never overwritten)")
	in := fs.String("in", "", "import: the export file to read")
	timeout := connectTimeoutFlag(fs)
	if err := fs.Parse(rest); err != nil {
		return err
	}
	call := func(req cluster.AdminRequest) (*cluster.ClusterKeyAnswer, error) {
		resp, err := adminCall(*addr, *certsDir, *certUser, *timeout, req)
		if err != nil {
			return nil, err
		}
		if resp.ClusterKey == nil {
			return nil, errors.New("the node did not answer about the cluster key (does it predate it?)")
		}
		return resp.ClusterKey, nil
	}
	ctx := context.Background()
	switch action {
	case "status", "init", "rotate", "sync":
		ans, err := call(cluster.AdminRequest{Op: "cluster-key-" + action})
		if err != nil {
			return err
		}
		for _, note := range ans.Notes {
			fmt.Println(note)
		}
		fmt.Print(describeClusterKey(ans.Status))
		return nil

	case "export":
		if *toRoot == "" || *out == "" {
			return errors.New("cluster-key export requires --to-root and --out")
		}
		root, err := exportRoot(*toRoot)
		if err != nil {
			return err
		}
		f, err := os.OpenFile(*out, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
		if err != nil {
			return fmt.Errorf("--out: %w", err)
		}
		written := false
		defer func() {
			_ = f.Close()
			if !written {
				_ = os.Remove(*out)
			}
		}()
		x, err := enc.NewPeerExchange()
		if err != nil {
			return err
		}
		ans, err := call(cluster.AdminRequest{Op: "cluster-key-export", ClusterKey: &cluster.ClusterKeyRequest{Exchange: x.Public()}})
		if err != nil {
			return err
		}
		if ans.Status == nil {
			return errors.New("the node's answer carries no status")
		}
		keys, err := x.OpenClusterKeys(ans.Exchange, ans.Sealed, []byte("datax cluster key export|"+ans.Status.ClusterID))
		if err != nil {
			return err
		}
		defer enc.ZeroTransportKeys(keys)
		current := enc.ClusterKeyRef{ID: ans.Status.ID, Version: ans.Status.Version}
		xf, err := enc.ExportClusterKeys(ctx, root, ans.Status.ClusterID, current, keys)
		if err != nil {
			return err
		}
		raw, err := json.MarshalIndent(xf, "", "  ")
		if err != nil {
			return err
		}
		if _, err := f.Write(raw); err != nil {
			return err
		}
		if err := f.Sync(); err != nil {
			return err
		}
		written = true
		refs := make([]string, len(keys))
		for i, k := range keys {
			refs[i] = k.ClusterKeyRef.String()
		}
		log.Audit("cluster-key-export-file", "out", *out, "root", root.Kind(), "keys", strings.Join(refs, ","), "principal", "os:"+osUsername())
		fmt.Printf("exported %s of cluster %s to %s, wrapped under the %s root\n", strings.Join(refs, ", "), ans.Status.ClusterID, *out, root.Kind())
		return nil

	case "import":
		if *fromRoot == "" || *in == "" {
			return errors.New("cluster-key import requires --from-root and --in")
		}
		root, err := exportRoot(*fromRoot)
		if err != nil {
			return err
		}
		raw, err := os.ReadFile(*in)
		if err != nil {
			return err
		}
		xf, err := enc.ParseClusterKeyExport(raw)
		if err != nil {
			return err
		}
		keys, err := xf.Open(ctx, []enc.Root{root})
		if err != nil {
			return err
		}
		defer enc.ZeroTransportKeys(keys)
		ans, err := call(cluster.AdminRequest{Op: "cluster-key-import", ClusterKey: &cluster.ClusterKeyRequest{Keys: keys}})
		if err != nil {
			return err
		}
		for _, note := range ans.Notes {
			fmt.Println(note)
		}
		fmt.Print(describeClusterKey(ans.Status))
		return nil
	}
	return errors.New(usage)
}

// exportRoot parses the operator's root for an export or import. A root
// key held as shares is not one: it would need its holders here.
func exportRoot(spec string) (enc.Root, error) {
	if strings.TrimSpace(spec) == "shares" {
		return nil, errors.New("a root key held as shares cannot wrap an export; use file:, exec: or a KMS")
	}
	return enc.ParseRoot(spec)
}

func describeClusterKey(st *cluster.ClusterKeyStatus) string {
	if st == nil {
		return ""
	}
	var b strings.Builder
	if !st.Enabled {
		fmt.Fprintf(&b, "cluster %s: the cluster key is not enabled\n", st.ClusterID)
	} else {
		fmt.Fprintf(&b, "cluster %s: cluster key %s, version %d", st.ClusterID, st.ID, st.Version)
		if len(st.Previous) > 0 {
			fmt.Fprintf(&b, " (a rotation is still leaving version %v)", st.Previous)
		}
		b.WriteString("\n")
		if len(st.Imported) > 0 {
			fmt.Fprintf(&b, "imported: %s\n", strings.Join(st.Imported, ", "))
		}
	}
	if st.Pending != "" {
		fmt.Fprintf(&b, "in progress: %s (run the same command again to finish it)\n", st.Pending)
	}
	for _, nd := range st.Nodes {
		state := "live"
		if !nd.Live {
			state = "not live"
		}
		if nd.Decommissioning {
			state += ", decommissioning"
		}
		root := nd.Root
		if !nd.Reported {
			root = "not reported"
		} else if root == "" {
			root = "no root (key file or plaintext)"
		}
		holds := "does not hold the current key"
		if nd.HoldsCurrent {
			holds = "holds the current key"
		} else if !st.Enabled {
			holds = ""
		}
		fmt.Fprintf(&b, "  n%d: %s; root %s", nd.NodeID, state, root)
		if holds != "" {
			fmt.Fprintf(&b, "; %s", holds)
		}
		b.WriteString("\n")
	}
	return b.String()
}
