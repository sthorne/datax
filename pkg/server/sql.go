package server

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/sthorne/datax/pkg/cluster"
	"net"
	"time"

	"github.com/sthorne/datax/pkg/base"
	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/pgwire"
	"github.com/sthorne/datax/pkg/security"
	"github.com/sthorne/datax/pkg/sql/catalog"
	"github.com/sthorne/datax/pkg/util/log"
)

func kvNodeID(v int64) base.NodeID { return base.NodeID(v) }

// startSQL brings up the SQL (PostgreSQL wire protocol) listener.
func (n *Node) startSQL() error {
	lis := n.cfg.PGListener
	if lis == nil {
		if n.cfg.PGListen == "" {
			return nil
		}
		var err error
		lis, err = net.Listen("tcp", n.cfg.PGListen)
		if err != nil {
			return err
		}
	}
	cat, err := n.catalogAccessor()
	if err != nil {
		return err
	}
	opts := pgwire.ServerOptions{SlowStatementThreshold: n.cfg.SlowStatementThreshold, NodeID: int32(n.ident.NodeID)}
	opts.Forward = func(ctx context.Context, node, pid int32, secret uint32, terminate bool) (bool, error) {
		addr, err := n.registry.Resolve(base.NodeID(node))
		if err != nil {
			return false, nil // no such node: no such session
		}
		var resp cluster.AdminResponse
		if err := n.trans.Call(ctx, addr, "admin", cluster.AdminRequest{Op: "cancel-query", PID: pid, Secret: secret, Terminate: terminate}, &resp); err != nil {
			return false, err
		}
		if resp.Error != "" {
			return false, errors.New(resp.Error)
		}
		var found bool
		_ = json.Unmarshal(resp.Status, &found)
		return found, nil
	}
	if n.tlsCfgs != nil {
		opts.TLS = n.tlsCfgs.PGServer
		opts.Auth = n.lookupVerifier
		if n.cfg.RootPassword != "" {
			if err := n.stopper.RunWorker(n.seedRootUser); err != nil {
				return err
			}
		}
	}
	n.pgServer = pgwire.Serve(lis, n.db, cat, n.stopper, opts)
	log.Infof("node %s serving SQL at %s", n.ident.NodeID, n.pgServer.Addr())
	return nil
}

// catalogAccessor returns the node's catalog accessor, built on first use
// with statistics and descriptor leasing enabled (the SQL server and the
// metrics recorder share it, so one lease serves both).
func (n *Node) catalogAccessor() (*catalog.Accessor, error) {
	n.catOnce.Do(func() {
		cat := catalog.NewAccessor()
		cat.EnableStats(n.db)
		if n.cfg.DescLeaseTTL >= 0 {
			if err := cat.StartLeasing(n.db, n.clock, n.stopper, n.cfg.DescLeaseTTL); err != nil {
				n.catErr = err
				return
			}
		}
		n.cat = cat
	})
	return n.cat, n.catErr
}

// lookupVerifier reads a user's stored SCRAM verifier (nil, nil = no such
// user; the wire layer reports failures uniformly).
func (n *Node) lookupVerifier(ctx context.Context, user string) (*security.ScramVerifier, error) {
	raw, err := n.db.Get(ctx, keys.UserKey(user))
	if err != nil || raw == nil {
		return nil, err
	}
	return security.UnmarshalVerifier(raw)
}

// seedRootUser writes root's verifier at startup if none exists yet (the
// bootstrap path for a secure cluster's first credential). Retries until
// the cluster can serve writes.
func (n *Node) seedRootUser(ctx context.Context) {
	for {
		wctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		raw, err := n.db.Get(wctx, keys.UserKey("root"))
		if err == nil && raw != nil {
			cancel()
			return // already seeded
		}
		if err == nil {
			v, verr := security.MakeScramVerifier(n.cfg.RootPassword)
			if verr == nil {
				enc, merr := security.MarshalVerifier(v)
				if merr == nil {
					if perr := n.db.Put(wctx, keys.UserKey("root"), enc); perr == nil {
						cancel()
						log.Infof("seeded root user credential")
						return
					}
				}
			}
		}
		cancel()
		select {
		case <-ctx.Done():
			return
		case <-time.After(time.Second):
		}
	}
}
