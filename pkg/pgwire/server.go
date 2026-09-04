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
	"sync/atomic"
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
	conns map[net.Conn]*conn
	// draining is set by Drain: the listener is closed and every
	// connection has been asked to finish.
	draining atomic.Bool
	act      *Activity
}

// Activity exposes the server's client accounting.
func (s *Server) Activity() *Activity { return s.act }

// Serve starts accepting connections on lis (returns immediately).
func Serve(lis net.Listener, db *kvclient.DB, cat *catalog.Accessor, stopper *stop.Stopper, opts ServerOptions) *Server {
	s := &Server{db: db, cat: cat, stopper: stopper, lis: lis, opts: opts, conns: make(map[net.Conn]*conn), act: newActivity(opts.SlowStatementThreshold)}
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
				if !s.draining.Load() {
					log.Debugf("pgwire accept: %v", err)
				}
			}
			return
		}
		c := newConn(nc, s.db, s.cat, s.opts)
		c.act = s.act
		s.mu.Lock()
		s.conns[nc] = c
		s.mu.Unlock()
		go func() {
			defer func() {
				s.mu.Lock()
				delete(s.conns, nc)
				s.mu.Unlock()
				_ = nc.Close()
			}()
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

// Drain stops accepting connections and asks every open one to finish:
// an idle connection outside a transaction is sent a FATAL 57P01
// (admin_shutdown) and closed at once; one running a statement or
// inside a transaction is left to finish, and told the same the moment
// it is idle again. At ctx's deadline the connections still open are
// closed: idle ones with the 57P01 first, busy ones under the
// statement. Returns how many ended cleanly and how many were cut.
func (s *Server) Drain(ctx context.Context) (closed, cut int) {
	s.draining.Store(true)
	_ = s.lis.Close()
	s.mu.Lock()
	initial := len(s.conns)
	for _, c := range s.conns {
		c.drain(false)
	}
	s.mu.Unlock()

	remaining := s.waitConns(ctx)
	for _, c := range remaining {
		c.drain(true)
	}
	if len(remaining) > 0 {
		// Give the cut connections a moment to send their 57P01.
		gctx, cancel := context.WithTimeout(context.Background(), time.Second)
		s.waitConns(gctx)
		cancel()
	}
	return initial - len(remaining), len(remaining)
}

// waitConns waits until no connection remains or ctx ends, returning
// the connections still open.
func (s *Server) waitConns(ctx context.Context) []*conn {
	for {
		s.mu.Lock()
		if len(s.conns) == 0 {
			s.mu.Unlock()
			return nil
		}
		if ctx.Err() != nil {
			out := make([]*conn, 0, len(s.conns))
			for _, c := range s.conns {
				out = append(out, c)
			}
			s.mu.Unlock()
			return out
		}
		s.mu.Unlock()
		select {
		case <-ctx.Done():
		case <-time.After(20 * time.Millisecond):
		}
	}
}
