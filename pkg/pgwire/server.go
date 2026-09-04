// Package pgwire serves the PostgreSQL wire protocol (v3) on top of the SQL
// layer, using pgproto3 for framing: the simple query protocol plus the
// minimal extended protocol pgx's default mode needs, TLS (v2), and
// SCRAM-SHA-256 authentication in secure mode (trust in insecure mode).
// See docs/sql.md.
package pgwire

import (
	"context"
	"crypto/tls"
	"net"
	"sync"
	"time"

	"github.com/sthorne/datax/pkg/kvclient"
	"github.com/sthorne/datax/pkg/security"
	"github.com/sthorne/datax/pkg/sql/catalog"
	"github.com/sthorne/datax/pkg/util/log"
	"github.com/sthorne/datax/pkg/util/stop"
)

// Authenticator resolves a username to its stored SCRAM verifier. A nil
// verifier with nil error means "no such user" (the failure is reported
// uniformly). A nil Authenticator on the server means trust auth.
type Authenticator func(ctx context.Context, user string) (*security.ScramVerifier, error)

// ServerOptions configure the SQL listener beyond its dependencies.
type ServerOptions struct {
	// TLS, when set, answers SSLRequest with 'S' and wraps connections.
	TLS *tls.Config
	// Auth, when set, requires SCRAM-SHA-256; nil = trust.
	Auth Authenticator
	// SlowStatementThreshold is the duration past which a statement is
	// kept in the slow-statement ring (0 = the default, 500 ms).
	SlowStatementThreshold time.Duration
}

// Server accepts SQL client connections.
type Server struct {
	db      *kvclient.DB
	cat     *catalog.Accessor
	stopper *stop.Stopper
	lis     net.Listener
	opts    ServerOptions

	mu    sync.Mutex
	conns map[net.Conn]struct{}
	act   *Activity
}

// Activity exposes the server's client accounting.
func (s *Server) Activity() *Activity { return s.act }

// Serve starts accepting connections on lis (returns immediately).
func Serve(lis net.Listener, db *kvclient.DB, cat *catalog.Accessor, stopper *stop.Stopper, opts ServerOptions) *Server {
	s := &Server{db: db, cat: cat, stopper: stopper, lis: lis, opts: opts, conns: make(map[net.Conn]struct{}), act: newActivity(opts.SlowStatementThreshold)}
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
			c := newConn(nc, s.db, s.cat, s.opts)
			c.act = s.act
			s.act.connOpened(c, nc.RemoteAddr().String())
			defer s.act.connClosed(c)
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
