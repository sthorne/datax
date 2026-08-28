// Package pgwire serves the PostgreSQL wire protocol (v3) on top of the SQL
// layer, using pgproto3 for framing. Trust authentication only in v1 (no
// TLS, no SCRAM); the simple query protocol plus the minimal extended
// protocol pgx's default mode needs. See docs/sql.md.
package pgwire

import (
	"context"
	"net"
	"sync"

	"github.com/sthorne/datax/pkg/kvclient"
	"github.com/sthorne/datax/pkg/sql/catalog"
	"github.com/sthorne/datax/pkg/util/log"
	"github.com/sthorne/datax/pkg/util/stop"
)

// Server accepts SQL client connections.
type Server struct {
	db      *kvclient.DB
	cat     *catalog.Accessor
	stopper *stop.Stopper
	lis     net.Listener

	mu    sync.Mutex
	conns map[net.Conn]struct{}
}

// Serve starts accepting connections on lis (returns immediately).
func Serve(lis net.Listener, db *kvclient.DB, cat *catalog.Accessor, stopper *stop.Stopper) *Server {
	s := &Server{db: db, cat: cat, stopper: stopper, lis: lis, conns: make(map[net.Conn]struct{})}
	stopper.AddCloser(func() { _ = lis.Close() })
	go s.acceptLoop()
	return s
}

func (s *Server) Addr() string { return s.lis.Addr().String() }

func (s *Server) acceptLoop() {
	for {
		nc, err := s.lis.Accept()
		if err != nil {
			select {
			case <-s.stopper.ShouldQuiesce():
			default:
				log.Debugf("pgwire accept: %v", err)
			}
			return
		}
		s.mu.Lock()
		s.conns[nc] = struct{}{}
		s.mu.Unlock()
		go func() {
			defer func() {
				s.mu.Lock()
				delete(s.conns, nc)
				s.mu.Unlock()
				_ = nc.Close()
			}()
			c := newConn(nc, s.db, s.cat)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			go func() { // tear the connection down on server shutdown
				select {
				case <-s.stopper.ShouldQuiesce():
					_ = nc.Close()
				case <-ctx.Done():
				}
			}()
			if err := c.run(ctx); err != nil {
				log.Debugf("pgwire conn %s: %v", nc.RemoteAddr(), err)
			}
		}()
	}
}
