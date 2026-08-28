package server

import (
	"context"
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
	var opts pgwire.ServerOptions
	if n.tlsCfgs != nil {
		opts.TLS = n.tlsCfgs.PGServer
		opts.Auth = n.lookupVerifier
		if n.cfg.RootPassword != "" {
			if err := n.stopper.RunWorker(n.seedRootUser); err != nil {
				return err
			}
		}
	}
	n.pgServer = pgwire.Serve(lis, n.db, catalog.NewAccessor(), n.stopper, opts)
	log.Infof("node %s serving SQL at %s", n.ident.NodeID, n.pgServer.Addr())
	return nil
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
