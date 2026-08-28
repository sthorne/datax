package server

import (
	"net"

	"github.com/sthorne/datax/pkg/base"
	"github.com/sthorne/datax/pkg/pgwire"
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
	n.pgServer = pgwire.Serve(lis, n.db, catalog.NewAccessor(), n.stopper)
	log.Infof("node %s serving SQL at %s", n.ident.NodeID, n.pgServer.Addr())
	return nil
}
