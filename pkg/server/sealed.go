package server

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"sync"

	"github.com/cockroachdb/pebble/v2/vfs"
	"github.com/sthorne/datax/pkg/cluster"
	"github.com/sthorne/datax/pkg/rpc"
	"github.com/sthorne/datax/pkg/security"
	"github.com/sthorne/datax/pkg/storage/enc"
	"github.com/sthorne/datax/pkg/util/log"
)

// A sealed start (issues #292, #306; the root is in
// pkg/storage/enc/shares.go). A node whose store key is under a root
// key held as operators' shares cannot open its store when it starts:
// the root key is in nobody's possession until enough share-holders
// have each handed the node a share. Until then the node is SEALED. It
// listens on its RPC address and serves two admin calls — seal-status
// and unseal — and nothing else: no SQL port, no console, no raft. To
// its peers it is a node that is down, which is what it is.
//
// The same two calls exist on a running node. There, seal-status says
// "not sealed", and unseal assembles a SECOND root key from shares: the
// one the store key is to be moved under when the root key itself is
// rotated (rewrap-store-key then moves it).

// handoffListener lets two servers take turns on one listener: the
// sealed server first, the node's own after it. A gRPC server closes
// its listener when it stops, and closing the real one would give up
// the port — so each server gets a view, and closing a view only ends
// that view's Accept. One goroutine accepts for all of them.
type handoffListener struct {
	net.Listener
	conns chan net.Conn
	done  chan struct{} // closed when the real listener fails
	err   error
}

func newHandoffListener(lis net.Listener) *handoffListener {
	h := &handoffListener{Listener: lis, conns: make(chan net.Conn), done: make(chan struct{})}
	go func() {
		for {
			c, err := lis.Accept()
			if err != nil {
				h.err = err
				close(h.done)
				return
			}
			select {
			case h.conns <- c:
			case <-h.done:
			}
		}
	}()
	return h
}

// view is one server's turn at the listener.
func (h *handoffListener) view() *listenerView {
	return &listenerView{h: h, closed: make(chan struct{})}
}

// final is the last turn: closing it closes the real listener.
func (h *handoffListener) final() net.Listener {
	return &listenerView{h: h, closed: make(chan struct{}), last: true}
}

type listenerView struct {
	h      *handoffListener
	closed chan struct{}
	once   sync.Once
	last   bool
}

func (v *listenerView) Accept() (net.Conn, error) {
	select {
	case <-v.closed:
		return nil, net.ErrClosed
	default:
	}
	select {
	case c := <-v.h.conns:
		return c, nil
	case <-v.closed:
		return nil, net.ErrClosed
	case <-v.h.done:
		return nil, v.h.err
	}
}

func (v *listenerView) Close() error {
	var err error
	v.once.Do(func() {
		close(v.closed)
		if v.last {
			err = v.h.Listener.Close()
		}
	})
	return err
}

func (v *listenerView) Addr() net.Addr { return v.h.Addr() }

// sealedRoot returns the share root this start cannot proceed without,
// or nil: the one the store's wrapped key is under, or, for a directory
// with no store yet, the root the new store key will be wrapped by.
func sealedRoot(dir string, roots []enc.Root) (*enc.ShareRoot, error) {
	if !enc.WrappedKeyExists(vfs.Default, dir) {
		if s, ok := roots[0].(*enc.ShareRoot); ok && s.Sealed() && !enc.RegistryExists(vfs.Default, dir) && !storeDirHasFiles(dir) {
			return s, nil
		}
		return nil, nil
	}
	entries, err := enc.ReadWrappedKeys(vfs.Default, dir)
	if err != nil {
		return nil, err
	}
	shared := enc.SharedRoots(roots)
	for _, e := range entries {
		if e.Root != "shares" {
			continue
		}
		if len(shared) == 0 {
			return nil, nil // UnwrapStoreKeys reports the missing root
		}
		if shared[0].Sealed() {
			shared[0].Expect(e.KeyID)
			return shared[0], nil
		}
	}
	return nil, nil
}

// awaitUnseal holds the start until root has its key: it serves the
// sealed admin calls on the node's RPC address and returns when a
// quorum of shares has arrived, or with an error when the start is
// abandoned.
func (n *Node) awaitUnseal(root *enc.ShareRoot) error {
	if n.cfg.CertsDir != "" {
		tlsCfgs, err := security.LoadNodeTLS(n.cfg.CertsDir)
		if err != nil {
			return fmt.Errorf("loading TLS certificates: %w", err)
		}
		n.tlsCfgs = tlsCfgs
	}
	lis := n.cfg.Listener
	if lis == nil {
		var err error
		if lis, err = net.Listen("tcp", n.cfg.Listen); err != nil {
			return err
		}
	}
	n.rpcListener = newHandoffListener(lis)
	var serverTLS *tls.Config
	if n.tlsCfgs != nil {
		serverTLS = n.tlsCfgs.Server
	}
	gs := rpc.NewSealedServer(n.clock, func(ctx context.Context, data []byte) ([]byte, error) {
		var req cluster.AdminRequest
		if err := json.Unmarshal(data, &req); err != nil {
			return nil, err
		}
		return json.Marshal(n.serveSealedAdmin(ctx, root, req))
	}, serverTLS)
	view := n.rpcListener.view()
	go func() { _ = gs.Serve(view) }()
	st := root.Status()
	which := "a new store: the first share says which root key"
	if st.KeyID != "" {
		which = "root key " + st.KeyID
	}
	log.Infof("SEALED: the store key is under a root key held as shares (%s). Listening on %s for `datax unseal`; the store stays closed until enough shares have been submitted", which, lis.Addr())
	log.Audit("sealed-start", "key_id", st.KeyID)
	if n.cfg.OnSealed != nil {
		n.cfg.OnSealed(lis.Addr().String())
	}
	// Before settling down to wait for people, ask the cluster: a peer
	// that is running holds this root key and may hand it over
	// (peerunseal.go). Shares are accepted all the while.
	askCtx, stopAsking := context.WithCancel(context.Background())
	asked := make(chan struct{})
	go func() {
		defer close(asked)
		n.askPeersToUnseal(askCtx, root)
	}()
	defer func() {
		stopAsking()
		<-asked
	}()
	var err error
	select {
	case <-root.Opened():
	case <-n.stopper.ShouldQuiesce():
		err = errors.New("stopped while sealed")
	case <-n.cfg.startAbandoned():
		err = errors.New("start abandoned while sealed")
	}
	// The reply to the share that completed the quorum is in flight:
	// let it finish, then give the listener to the node proper.
	gs.GracefulStop()
	if err != nil {
		_ = lis.Close()
		return err
	}
	log.Infof("UNSEALED: root key %s rebuilt from shares; opening the store", root.KeyID())
	log.Audit("unsealed", "key_id", root.KeyID())
	return nil
}

// sealAuthorized is the gate on submitting a share. A sealed node cannot
// read its role table, so membership of the admin role cannot be
// checked: the root user's certificate is what unseals. Without TLS
// there are no identities and a share would cross the network in the
// clear, so only this machine may submit.
func (n *Node) sealAuthorized(ctx context.Context, sealed bool) (principal string, err error) {
	if n.tlsCfgs == nil {
		if !rpc.PeerIsLoopback(ctx) {
			return "(insecure)", errors.New("this node runs without TLS: a share would cross the network in the clear, so it is accepted only from the node's own machine (run `datax unseal` there, against a loopback address)")
		}
		return "(insecure)", nil
	}
	cn := rpc.PeerCN(ctx)
	if cn == "root" || (!sealed && cn != security.NodePrincipal && n.isAdminPrincipal(ctx, cn)) {
		return cn, nil
	}
	if sealed {
		return cn, fmt.Errorf("permission denied: a sealed node cannot read its roles, so only the root user's certificate may submit a share (connected as %q)", cn)
	}
	return cn, fmt.Errorf("permission denied: submitting a share requires the admin role (connected as %q)", cn)
}

func sealStatusOf(st enc.SealStatus) *cluster.SealStatus {
	return &cluster.SealStatus{Sealed: st.Sealed, Root: "shares", KeyID: st.KeyID, Have: st.Have, Need: st.Need, Indexes: st.Indexes}
}

// submitShare gives root one share and writes the audit record: who,
// which share by its index, and what came of it — never the share.
func submitShare(root *enc.ShareRoot, principal, share, purpose string) (enc.SealStatus, error) {
	index := 0
	if s, perr := enc.ParseShare(share); perr == nil {
		index = s.Index
	}
	st, err := root.Submit(share)
	kv := []any{"principal", principal, "purpose", purpose, "share_index", int64(index), "key_id", st.KeyID, "have", int64(st.Have), "need", int64(st.Need)}
	if err != nil {
		kv = append(kv, "outcome", "refused", "error", err.Error())
	} else {
		kv = append(kv, "outcome", "accepted", "sealed", st.Sealed)
	}
	log.Audit("unseal-share", kv...)
	return st, err
}

// serveSealedAdmin is the whole of what a sealed node serves.
func (n *Node) serveSealedAdmin(ctx context.Context, root *enc.ShareRoot, req cluster.AdminRequest) cluster.AdminResponse {
	switch req.Op {
	case "seal-status":
		return cluster.AdminResponse{Seal: sealStatusOf(root.Status())}
	case "unseal":
		principal, err := n.sealAuthorized(ctx, true)
		if err != nil {
			log.Audit("admin-denied", "op", req.Op, "principal", principal)
			return cluster.AdminResponse{Error: err.Error()}
		}
		st, err := submitShare(root, principal, req.Share, "unseal")
		resp := cluster.AdminResponse{Seal: sealStatusOf(st)}
		if err != nil {
			resp.Error = err.Error()
		}
		return resp
	}
	return cluster.AdminResponse{Error: rpc.SealedMessage, Seal: sealStatusOf(root.Status())}
}

// serveSealStatus is seal-status on a running node.
func (n *Node) serveSealStatus() cluster.AdminResponse {
	n.encKeyMu.Lock()
	defer n.encKeyMu.Unlock()
	st := &cluster.SealStatus{}
	if n.encRoot != nil {
		st.Root, st.KeyID = n.encRoot.Kind(), n.encRoot.KeyID()
	}
	if st.Root == "shares" {
		st.PeerUnseal = n.peerUnsealSetting()
	}
	for i, r := range n.encRoots {
		s, ok := r.(*enc.ShareRoot)
		if !ok || r == n.encRoot {
			continue
		}
		staged := sealStatusOf(s.Status())
		staged.Position = i + 1
		st.Staged = staged
	}
	return cluster.AdminResponse{Seal: st}
}

// serveStageShare is unseal on a running node: the share goes toward a
// second root key, which the store key can then be moved under. The
// root in use already has its key and takes no more shares.
func (n *Node) serveStageShare(ctx context.Context, req cluster.AdminRequest) cluster.AdminResponse {
	principal, err := n.sealAuthorized(ctx, false)
	if err != nil {
		log.Audit("admin-denied", "op", req.Op, "principal", principal)
		return cluster.AdminResponse{Error: err.Error()}
	}
	if n.encRoot == nil {
		return cluster.AdminResponse{Error: "this node's store key is not kept under a root; there is nothing for a share to open or to move it under"}
	}
	s, perr := enc.ParseShare(req.Share)
	n.encKeyMu.Lock()
	if perr == nil && s.KeyID == n.encRoot.KeyID() && n.encRoot.Kind() == "shares" {
		n.encKeyMu.Unlock()
		return cluster.AdminResponse{Error: "this node is not sealed, and this is a share of the root key it is already running under; shares of a NEW root key are what a running node takes (to move its store key under it)"}
	}
	var staged *enc.ShareRoot
	for _, r := range n.encRoots {
		if sr, ok := r.(*enc.ShareRoot); ok && r != n.encRoot && sr.Sealed() {
			staged = sr
			break
		}
	}
	if staged == nil {
		staged = enc.NewShareRoot()
		n.encRoots = append(n.encRoots, staged)
	}
	n.encKeyMu.Unlock()
	st, err := submitShare(staged, principal, req.Share, "stage a new root key")
	resp := n.serveSealStatus()
	if err != nil {
		resp.Error = err.Error()
	} else if !st.Sealed {
		msg := fmt.Sprintf("a new root key (%s) has been rebuilt from shares and is held in memory as root %d; the store key is still under the root in use until it is rewrapped", st.KeyID, resp.Seal.Staged.Position)
		log.Infof("%s", msg)
		n.events.Record("key-rotation", "%s", msg)
	}
	return resp
}

// startAbandoned is closed when the caller gives up on a start that is
// waiting to be unsealed; never, when no StartContext was given.
func (c *Config) startAbandoned() <-chan struct{} {
	if c.StartContext == nil {
		return nil
	}
	return c.StartContext.Done()
}
