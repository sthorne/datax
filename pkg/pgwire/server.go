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

	"github.com/jackc/pgx/v5/pgproto3"

	"github.com/sthorne/datax/pkg/kvclient"
	"github.com/sthorne/datax/pkg/metrics"
	"github.com/sthorne/datax/pkg/security"
	"github.com/sthorne/datax/pkg/sql/catalog"
	"github.com/sthorne/datax/pkg/util/log"
	"github.com/sthorne/datax/pkg/util/stop"
)

// Authenticator resolves a username to its stored SCRAM verifier. A nil
// verifier with nil error means "no such user" (the failure is reported
// uniformly). A nil Authenticator on the server means trust auth.
type Authenticator func(ctx context.Context, user string) (*security.ScramVerifier, error)

// AuthLimiter is the password-guessing budget the SQL listener consults
// before it runs a SCRAM exchange — the same one the HTTP doors use, so
// an attacker gains nothing by changing port (issue #212). It is asked
// about a budget before the exchange and told the outcome after it,
// rather than charged on the attempt: a connection pool opening twenty
// connections with the right password at once is not the caller being
// bounded, and charging on the attempt would refuse the eleventh of
// them before the first had succeeded and refunded.
//
// The cost of that order is that a burst of guesses that all begin
// before any has failed is bounded by MaxPendingAuth, not by the
// budget; from the first failures on, the budget holds.
type AuthLimiter interface {
	// Budget reports whether source may make another attempt at user.
	// It spends nothing.
	Budget(source, user string) bool
	// Failed charges one failed attempt against source and user.
	Failed(source, user string)
	// Succeeded refunds what a caller who proved it holds the
	// credential has spent.
	Succeeded(source, user string)
}

// Defaults for the pre-authentication bounds (issue #212).
const (
	// DefaultAuthTimeout is how long a connection has to complete its
	// startup and authentication before it is closed — PostgreSQL's
	// authentication_timeout.
	DefaultAuthTimeout = 60 * time.Second
	// DefaultMaxPendingAuth caps connections in the pre-authentication
	// state. Each holds a goroutine pair, a session, a cancel-registry
	// entry and a descriptor without having proved anything, so the cap
	// is what bounds what an unauthenticated peer can hold — generous
	// enough that a pool bringing every connection up at once over a
	// slow link is not refused, since a connection is pending only for
	// its handshake's round trips.
	DefaultMaxPendingAuth = 512
)

// ServerOptions configure the SQL listener beyond its dependencies.
type ServerOptions struct {
	// TLS, when set, answers SSLRequest with 'S' and wraps connections.
	TLS *tls.Config
	// Auth, when set, requires SCRAM-SHA-256; nil = trust.
	Auth Authenticator
	// CanLogin, when set, gates the certificate path too: a CA-verified
	// client certificate authenticates its CommonName only when that
	// role exists and may log in (false, nil = refused).
	CanLogin func(ctx context.Context, user string) (bool, error)
	// AuthLimiter, when set, bounds password guessing on this listener
	// (nil: unbounded, as in trust mode where there is nothing to guess).
	AuthLimiter AuthLimiter
	// AuthTimeout bounds startup and authentication: a connection that
	// has not reached ReadyForQuery within it is closed (0 = the
	// default, 60 s; negative = no deadline).
	AuthTimeout time.Duration
	// MaxPendingAuth caps connections in the pre-authentication state;
	// past it a new connection is refused with a FATAL 53300 and closed
	// rather than accepted and held (0 = the default, 512; negative =
	// no cap).
	MaxPendingAuth int
	// MockSecret, when set, returns the cluster-wide secret that keys
	// the stand-in verifier for users Auth does not know (the salt in
	// server-first is derived from the user name under it, so a name
	// shows the same salt on every node and a shared salt never marks
	// the names that do not exist; issue #137). nil, or an empty result,
	// falls back to a per-process secret.
	MockSecret func(ctx context.Context) []byte
	// SlowStatementThreshold is the duration past which a statement is
	// kept in the slow-statement ring (0 = the default, 500 ms).
	SlowStatementThreshold time.Duration
	// NodeID is encoded in every process ID this server hands out, so a
	// CancelRequest that lands elsewhere is forwarded here; Forward
	// carries one to the node a process ID names (nil: local only).
	NodeID  int32
	Forward func(ctx context.Context, node, pid int32, secret uint32, terminate bool) (bool, error)
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
	cancel   *cancelRegistry
	// pending counts connections that have not finished startup, against
	// opts.MaxPendingAuth. Incremented in acceptLoop before anything is
	// allocated for the connection, released when handleStartup returns
	// however it returns (conn.run).
	pending atomic.Int64
}

// Activity exposes the server's client accounting.
func (s *Server) Activity() *Activity { return s.act }

// Serve starts accepting connections on lis (returns immediately).
func Serve(lis net.Listener, db *kvclient.DB, cat *catalog.Accessor, stopper *stop.Stopper, opts ServerOptions) *Server {
	s := &Server{db: db, cat: cat, stopper: stopper, lis: lis, opts: opts, conns: make(map[net.Conn]*conn), act: newActivity(opts.SlowStatementThreshold), cancel: newCancelRegistry()}
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
		if !s.admitPending(nc) {
			continue
		}
		c := newConn(nc, s.db, s.cat, s.opts)
		c.act = s.act
		c.srv = s
		s.cancel.register(c, s.opts.NodeID)
		c.session.BackendPID = c.pid
		c.session.BackendControl = s.backendControl
		c.session.SessionsHook = s.act.Sessions
		s.mu.Lock()
		s.conns[nc] = c
		s.mu.Unlock()
		go func() {
			defer func() {
				s.mu.Lock()
				delete(s.conns, nc)
				s.mu.Unlock()
				s.cancel.unregister(c)
				_ = nc.Close()
			}()
			s.act.connOpened(c, nc.RemoteAddr().String(), c.pid)
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

// EffectiveMaxPendingAuth is the pre-authentication cap a configured
// MaxPendingAuth means: the default for 0, none (0) for a negative
// value. Exported because the guessing limiter's debt floor is derived
// from it (pkg/server): a wave of guesses can be as wide as this cap,
// and the floor has to be at least as deep or a wave is discounted.
func EffectiveMaxPendingAuth(configured int) int {
	switch {
	case configured < 0:
		return 0
	case configured == 0:
		return DefaultMaxPendingAuth
	}
	return configured
}

// pendingLimit is the pre-authentication cap in force (0 = none).
func (s *Server) pendingLimit() int64 { return int64(EffectiveMaxPendingAuth(s.opts.MaxPendingAuth)) }

// admitPending takes a pre-authentication slot for nc, or refuses it:
// a FATAL 53300 (too_many_connections) and a close, before any
// goroutine, session or registry entry exists for it (issue #212). The
// refusal is at the connection level, before a username has been read,
// so it cannot answer differently for a known and an unknown one.
//
// The error goes out in cleartext, ahead of any SSLRequest. A client
// mid-SSLRequest reads its first byte as the negotiation's answer —
// pgx reports "server refused TLS connection", libpq 14+ an invalid
// response to SSL negotiation — which is intended: the alternative is
// completing a TLS handshake for a connection this node has already
// decided not to hold, and the refusal has to be cheap to be a bound.
func (s *Server) admitPending(nc net.Conn) bool {
	limit := s.pendingLimit()
	if limit == 0 {
		s.pending.Add(1)
		return true
	}
	if n := s.pending.Add(1); n > limit {
		s.pending.Add(-1)
		metrics.SQLPreAuthClosed.WithLabelValues(metrics.PreAuthPendingFull).Inc()
		log.Audit("sql-preauth-closed", "remote", nc.RemoteAddr().String(), "cause", metrics.PreAuthPendingFull)
		// The write is a few dozen bytes into a fresh socket's empty send
		// buffer; the deadline is so the accept loop can never be held by
		// a peer that will not take even that.
		_ = nc.SetWriteDeadline(time.Now().Add(time.Second))
		be := pgproto3.NewBackend(nc, nc)
		be.Send(&pgproto3.ErrorResponse{Severity: "FATAL", SeverityUnlocalized: "FATAL", Code: "53300",
			Message: "too many unauthenticated connections on this node; try again"})
		_ = be.Flush()
		_ = nc.Close()
		return false
	}
	return true
}

// pendingDone releases the slot admitPending took, once startup has
// ended one way or another.
func (s *Server) pendingDone() { s.pending.Add(-1) }

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
