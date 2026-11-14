package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"

	"github.com/cockroachdb/pebble/v2/vfs"
	"github.com/sthorne/datax/pkg/cluster"
	"github.com/sthorne/datax/pkg/storage/enc"
	"github.com/sthorne/datax/pkg/util/log"
)

// runDebugEncRoot manages what a store's key is kept under (issue #305):
//
//	status   what wrapped the store key in --dir (no secret is printed)
//	adopt    put a key-file store's key under a root (stopped node)
//	rewrap   wrap the key again, under the same root or another
//	release  take a store back from a root to a key file (stopped node)
//
// The store key and everything under it are never changed by these: one
// small file in the data directory is.
func runDebugEncRoot(args []string) error {
	const usage = "usage: datax debug enc-root <status|adopt|rewrap|release> [flags]"
	if len(args) < 1 {
		return errors.New(usage)
	}
	action, rest := args[0], args[1:]
	fs := flag.NewFlagSet("debug enc-root "+action, flag.ContinueOnError)
	dir := fs.String("dir", "", "data directory of the STOPPED node")
	addr := fs.String("addr", "", "rewrap: RPC address of the RUNNING node")
	keyPath := fs.String("enc-key", "", "adopt: the store's current key file (a comma-separated list is tried in order)")
	rootSpec := fs.String("enc-root", "", "the root: file:<path> or exec:<command>; for an offline rewrap, a comma-separated list of the root the store is under and the one it moves to")
	toRoot := fs.Int("to-root", 0, "rewrap: wrap under this root instead of the one in use — its position (from 1) in the node's --enc-root list (online) or in --enc-root here (offline)")
	out := fs.String("out", "", "release: file to write the store key to (created, never overwritten)")
	certsDir := fs.String("certs-dir", "", "online mode, secure cluster: certificate directory (presents client.<user>.crt)")
	certUser := fs.String("user", "root", "online mode: username whose client certificate authenticates the call; must be an admin")
	timeout := connectTimeoutFlag(fs)
	if err := fs.Parse(rest); err != nil {
		return err
	}
	ctx := context.Background()
	audit := func(what string, r enc.Root) {
		log.Audit("store-key-"+what, "dir", *dir, "root", r.Kind(), "key_id", r.KeyID(), "principal", "os:"+osUsername())
	}
	// openStore unwraps the store key in --dir through --enc-root and
	// settles which entry the registry is under.
	openStore := func() ([]enc.Root, enc.Unwrapped, error) {
		roots, err := enc.ParseRoots(*rootSpec)
		if err != nil {
			return nil, enc.Unwrapped{}, err
		}
		got, err := enc.UnwrapStoreKeys(ctx, vfs.Default, *dir, roots)
		if err != nil {
			return nil, enc.Unwrapped{}, err
		}
		cands := make([][]byte, len(got))
		for i := range got {
			cands[i] = got[i].Key
		}
		idx, err := enc.MatchStoreKey(vfs.Default, *dir, cands)
		if err != nil {
			return nil, enc.Unwrapped{}, err
		}
		return roots, got[idx], nil
	}
	switch action {
	case "status":
		if *dir == "" {
			return fmt.Errorf("enc-root status requires --dir")
		}
		switch {
		case enc.WrappedKeyExists(vfs.Default, *dir):
			entries, err := enc.ReadWrappedKeys(vfs.Default, *dir)
			if err != nil {
				return err
			}
			for i, e := range entries {
				id := e.KeyID
				if id == "" {
					id = "(none)"
				}
				fmt.Printf("store key wrapped by a %s root, key id %s\n", e.Root, id)
				if i == 0 && len(entries) > 1 {
					fmt.Println("a store-key rotation did not finish; the next start settles it. Also held:")
				}
			}
		case enc.RegistryExists(vfs.Default, *dir):
			fmt.Println("store key is a key file (--enc-key); it is not kept under a root")
		default:
			fmt.Println("store is not encrypted")
		}
		return nil

	case "adopt":
		if *dir == "" || *keyPath == "" || *rootSpec == "" {
			return fmt.Errorf("enc-root adopt requires --dir, --enc-key and --enc-root")
		}
		if enc.WrappedKeyExists(vfs.Default, *dir) {
			return fmt.Errorf("the store in %s is already under a root (see `datax debug enc-root status`)", *dir)
		}
		if !enc.RegistryExists(vfs.Default, *dir) {
			return fmt.Errorf("the store in %s is not encrypted", *dir)
		}
		keys, err := loadKeyCandidates(*keyPath)
		if err != nil {
			return err
		}
		idx, err := enc.MatchStoreKey(vfs.Default, *dir, keys)
		if err != nil {
			return err
		}
		root, err := enc.ParseRoot(*rootSpec)
		if err != nil {
			return err
		}
		if err := enc.RewrapStoreKey(ctx, vfs.Default, *dir, root, keys[idx]); err != nil {
			return err
		}
		audit("adopt-root", root)
		fmt.Println("store key is now kept under the root; start the node with --enc-root in place of --enc-key, and rotate the store key (`datax debug rotate-enc-key`) so the key that was in the file opens nothing")
		return nil

	case "rewrap":
		if (*dir == "") == (*addr == "") {
			return fmt.Errorf("enc-root rewrap requires exactly one of --dir (stopped node) or --addr (running node)")
		}
		if *addr != "" {
			if *rootSpec != "" {
				return fmt.Errorf("online, the roots are the node's own: name the target with --to-root, not --enc-root")
			}
			if _, err := adminCall(*addr, *certsDir, *certUser, *timeout, cluster.AdminRequest{Op: "rewrap-store-key", Root: *toRoot}); err != nil {
				return err
			}
			fmt.Println("store key rewrapped; if it moved to another root, the node's next start must name that root in --enc-root")
			return nil
		}
		if *rootSpec == "" {
			return fmt.Errorf("enc-root rewrap --dir requires --enc-root")
		}
		roots, cur, err := openStore()
		if err != nil {
			return err
		}
		to := cur.Root
		if *toRoot != 0 {
			if *toRoot < 1 || *toRoot > len(roots) {
				return fmt.Errorf("--enc-root names %d root(s); there is no root %d", len(roots), *toRoot)
			}
			to = roots[*toRoot-1]
		}
		if err := enc.RewrapStoreKey(ctx, vfs.Default, *dir, to, cur.Key); err != nil {
			return err
		}
		audit("rewrap", to)
		fmt.Printf("store key rewrapped under the %s root; start the node with --enc-root naming it\n", to.Kind())
		return nil

	case "release":
		if *dir == "" || *rootSpec == "" || *out == "" {
			return fmt.Errorf("enc-root release requires --dir, --enc-root and --out")
		}
		_, cur, err := openStore()
		if err != nil {
			return err
		}
		// The key file first, and only a new one: the store must never be
		// left with neither a wrapped key nor a file.
		f, err := os.OpenFile(*out, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
		if err != nil {
			return err
		}
		if _, err := f.Write(cur.Key); err != nil {
			_ = f.Close()
			return err
		}
		if err := f.Sync(); err != nil {
			_ = f.Close()
			return err
		}
		if err := f.Close(); err != nil {
			return err
		}
		if err := enc.RemoveWrappedKeys(vfs.Default, *dir); err != nil {
			return err
		}
		audit("release-root", cur.Root)
		fmt.Printf("store key written to %s and no longer kept under the root; start the node with --enc-key %s\n", *out, *out)
		return nil
	}
	return errors.New(usage)
}
