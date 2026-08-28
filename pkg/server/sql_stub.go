package server

import "github.com/sthorne/datax/pkg/base"

func kvNodeID(v int64) base.NodeID { return base.NodeID(v) }

// startSQL brings up the SQL/pgwire front end (Phase 6).
func (n *Node) startSQL() error { return nil }
