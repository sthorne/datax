package main

import (
	"bufio"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/cockroachdb/pebble/v2/vfs"
	"github.com/sthorne/datax/pkg/cluster"
	"github.com/sthorne/datax/pkg/storage/enc"
	"golang.org/x/term"
)

// Shares of a root key at the command line (issue #306). A share is
// typed at a prompt that does not echo, or piped on standard input from
// whatever keeps it; it is never a flag or an environment variable,
// which is where process listings and shell histories would find it.

// shareInput is where shares are read from when standard input is not a
// terminal; a test points it at its own reader.
var (
	shareInput  io.Reader = os.Stdin
	shareReader *bufio.Reader
)

// readShare reads one share: without echo from a terminal, otherwise
// one line of standard input.
func readShare(prompt string) (string, error) {
	if f, ok := shareInput.(*os.File); ok && term.IsTerminal(int(f.Fd())) {
		fmt.Fprint(os.Stderr, prompt)
		b, err := term.ReadPassword(int(f.Fd()))
		fmt.Fprintln(os.Stderr)
		if err != nil {
			return "", err
		}
		return strings.TrimSpace(string(b)), nil
	}
	if shareReader == nil {
		shareReader = bufio.NewReader(shareInput)
	}
	for {
		line, err := shareReader.ReadString('\n')
		if s := strings.TrimSpace(line); s != "" {
			return s, nil
		}
		if err != nil {
			return "", errors.New("no share on standard input")
		}
	}
}

// unsealOffline rebuilds, from shares read here, the key of every share
// root among roots: an offline tool has no node to hold a sealed root,
// so the people with the shares give them to the tool itself. dir, when
// it holds a wrapped key under a share root, says which key to expect.
func unsealOffline(dir string, roots []enc.Root) error {
	shared := enc.SharedRoots(roots)
	if len(shared) == 0 {
		return nil
	}
	if dir != "" && enc.WrappedKeyExists(vfs.Default, dir) {
		entries, err := enc.ReadWrappedKeys(vfs.Default, dir)
		if err != nil {
			return err
		}
		for _, e := range entries {
			if e.Root == "shares" {
				shared[0].Expect(e.KeyID)
				break
			}
		}
	}
	for i, r := range shared {
		which := "the root key"
		if id := r.KeyID(); id != "" {
			which = "root key " + id
		} else if len(shared) > 1 {
			which = fmt.Sprintf("share root %d of %d", i+1, len(shared))
		}
		for r.Sealed() {
			st := r.Status()
			prompt := fmt.Sprintf("share of %s (%d of %d in): ", which, st.Have, st.Need)
			if st.Need == 0 {
				prompt = fmt.Sprintf("share of %s: ", which)
			}
			text, err := readShare(prompt)
			if err != nil {
				return fmt.Errorf("reading a share of %s: %w", which, err)
			}
			if _, err := r.Submit(text); err != nil {
				if strings.Contains(err.Error(), "too many refused") {
					return err
				}
				fmt.Fprintf(os.Stderr, "refused: %v\n", err)
			}
		}
	}
	return nil
}

// parseRootsOffline is enc.ParseRoots for a tool working on a stopped
// node's directory: share roots are unsealed at the prompt.
func parseRootsOffline(dir, spec string) ([]enc.Root, error) {
	roots, err := enc.ParseRoots(spec)
	if err != nil {
		return nil, err
	}
	if err := unsealOffline(dir, roots); err != nil {
		return nil, err
	}
	return roots, nil
}

func describeSeal(st *cluster.SealStatus) string {
	if st == nil {
		return "the node did not report a seal status (it predates sealed starts)"
	}
	progress := func(s *cluster.SealStatus) string {
		idx := make([]string, len(s.Indexes))
		for i, x := range s.Indexes {
			idx[i] = strconv.Itoa(x)
		}
		switch {
		case s.Need == 0:
			return "no shares submitted yet"
		case len(idx) == 0:
			return fmt.Sprintf("%d of %d shares", s.Have, s.Need)
		}
		return fmt.Sprintf("%d of %d shares in (share %s)", s.Have, s.Need, strings.Join(idx, ", "))
	}
	var b strings.Builder
	switch {
	case st.Sealed:
		fmt.Fprintf(&b, "sealed: %s", progress(st))
		if st.KeyID != "" {
			fmt.Fprintf(&b, "; waiting for shares of root key %s", st.KeyID)
		}
	case st.Root == "":
		b.WriteString("not sealed: the store key is not kept under a root")
	default:
		fmt.Fprintf(&b, "not sealed: the store key is under a %s root", st.Root)
		if st.KeyID != "" {
			fmt.Fprintf(&b, " (key id %s)", st.KeyID)
		}
	}
	if st.PeerUnseal != "" {
		fmt.Fprintf(&b, "\npeer unseal is %s", st.PeerUnseal)
		if st.PeerUnseal == "on" {
			b.WriteString(": this node hands the root key to a member of the cluster that restarts")
		} else {
			b.WriteString(": a node that restarts waits for shares")
		}
	}
	if s := st.Staged; s != nil {
		if s.Sealed {
			fmt.Fprintf(&b, "\na new root key is being assembled: %s", progress(s))
		} else {
			fmt.Fprintf(&b, "\na new root key (%s) is held as root %d: move the store key under it with `datax debug enc-root rewrap --addr <node> --to-root %d`", s.KeyID, s.Position, s.Position)
		}
	}
	return b.String()
}

// runUnseal submits one share to a node: a sealed node, to open it, or
// a running one, toward a new root key.
func runUnseal(args []string) error {
	fs := flag.NewFlagSet("unseal", flag.ContinueOnError)
	addr := fs.String("addr", "127.0.0.1:26257", "RPC address of the node")
	certsDir := fs.String("certs-dir", "", "secure cluster: certificate directory (presents client.<user>.crt)")
	certUser := fs.String("user", "root", "username whose client certificate authenticates the call; a sealed node accepts only root")
	timeout := connectTimeoutFlag(fs)
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 0 {
		return errors.New("unseal takes no arguments: the share is asked for, or read from standard input, and is never given on the command line")
	}
	share, err := readShare("share: ")
	if err != nil {
		return err
	}
	resp, err := adminCall(*addr, *certsDir, *certUser, *timeout, cluster.AdminRequest{Op: "unseal", Share: share})
	if err != nil {
		if resp.Seal != nil {
			return fmt.Errorf("%w\n%s", err, describeSeal(resp.Seal))
		}
		return err
	}
	if resp.Seal != nil && !resp.Seal.Sealed && resp.Seal.Staged == nil {
		fmt.Printf("share accepted: unsealed. The node is opening its store (root key %s)\n", resp.Seal.KeyID)
		return nil
	}
	fmt.Printf("share accepted\n%s\n", describeSeal(resp.Seal))
	return nil
}

func runDebugSealStatus(args []string) error {
	fs := flag.NewFlagSet("debug seal-status", flag.ContinueOnError)
	addr := fs.String("addr", "127.0.0.1:26257", "RPC address of the node")
	certsDir := fs.String("certs-dir", "", "secure cluster: certificate directory (presents client.<user>.crt)")
	certUser := fs.String("user", "root", "username whose client certificate authenticates the call")
	timeout := connectTimeoutFlag(fs)
	if err := fs.Parse(args); err != nil {
		return err
	}
	resp, err := adminCall(*addr, *certsDir, *certUser, *timeout, cluster.AdminRequest{Op: "seal-status"})
	if err != nil {
		return err
	}
	fmt.Println(describeSeal(resp.Seal))
	return nil
}

// parseSplit reads "<threshold>/<total>".
func parseSplit(s string) (threshold, total int, err error) {
	a, b, ok := strings.Cut(s, "/")
	if ok {
		threshold, err = strconv.Atoi(strings.TrimSpace(a))
		if err == nil {
			total, err = strconv.Atoi(strings.TrimSpace(b))
		}
	}
	if !ok || err != nil {
		return 0, 0, fmt.Errorf("--shares %q: want <threshold>/<total>, for example 2/3 (any 2 of 3 shares rebuild the key)", s)
	}
	return threshold, total, nil
}

// emitShares hands the shares over: one file each in outDir, never over
// a file that exists, or on standard output. This is the one time they
// are shown.
func emitShares(shares []enc.Share, outDir string) error {
	if outDir == "" {
		for _, s := range shares {
			fmt.Printf("share %d of %d: %s\n", s.Index, s.Total, s)
		}
	} else {
		if err := os.MkdirAll(outDir, 0o700); err != nil {
			return err
		}
		for _, s := range shares {
			p := filepath.Join(outDir, fmt.Sprintf("share-%d-of-%d.txt", s.Index, s.Total))
			f, err := os.OpenFile(p, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
			if err != nil {
				return err
			}
			if _, err := f.WriteString(s.String() + "\n"); err != nil {
				_ = f.Close()
				return err
			}
			if err := f.Close(); err != nil {
				return err
			}
			fmt.Printf("share %d of %d written to %s\n", s.Index, s.Total, p)
		}
	}
	fmt.Fprintf(os.Stderr, "\nroot key %s: any %d of these %d shares rebuild it; fewer reveal nothing.\nGive each to a different person and keep no copy together: this is the only time they are shown,\nand the root key itself has not been kept anywhere.\n", shares[0].KeyID, shares[0].Threshold, shares[0].Total)
	return nil
}

// runDebugPeerUnseal reports or changes the cluster's peer-unseal
// setting: whether running nodes hand the root key to a restarting
// member.
func runDebugPeerUnseal(args []string) error {
	fs := flag.NewFlagSet("debug peer-unseal", flag.ContinueOnError)
	addr := fs.String("addr", "127.0.0.1:26257", "RPC address of a running node")
	on := fs.Bool("on", false, "turn peer unseal on (the default for a cluster)")
	off := fs.Bool("off", false, "turn peer unseal off: every start of every node then needs a quorum of shares")
	certsDir := fs.String("certs-dir", "", "secure cluster: certificate directory (presents client.<user>.crt)")
	certUser := fs.String("user", "root", "username whose client certificate authenticates the call; must be an admin")
	timeout := connectTimeoutFlag(fs)
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *on && *off {
		return errors.New("--on and --off are alternatives")
	}
	req := cluster.AdminRequest{Op: "peer-unseal-setting"}
	if *on || *off {
		req.Enable = on
	}
	resp, err := adminCall(*addr, *certsDir, *certUser, *timeout, req)
	if err != nil {
		return err
	}
	fmt.Println(describeSeal(resp.Seal))
	return nil
}
