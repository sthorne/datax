package pgwire

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgproto3"

	"github.com/sthorne/datax/pkg/sql"
)

// Query cancellation (issue #97). Every connection gets a process ID
// and a random secret at startup (BackendKeyData); the node's registry
// maps the ID to the connection. A CancelRequest on a fresh connection
// — psql's Ctrl-C, pgx's context cancellation, every pool's cancel path
// — looks the pair up and cancels the statement in flight, whose
// transaction rolls back with 57014. The process ID carries the node in
// its high bits, so a cancel that lands on another node behind a load
// balancer is forwarded there (ServerOptions.Forward, over the
// internode admin RPC). pg_cancel_backend / pg_terminate_backend take
// the same path without a secret, on the admin role instead.
//
// Whichever way a cancel arrives, it is authorized on the node that
// acts, never on the node it arrived at (issue #211). There are exactly
// two authorities and they travel separately: a wire client holds the
// secret, which crosses the cluster with the request and is checked
// again at the far end; an administrator holds the admin role, which
// does not cross at all — the receiving node re-derives it from the
// forwarding node's certificate. Neither is inferred from the absence
// of the other.

// pidNodeShift is the bit the node ID starts at in a process ID: 20
// bits of per-node sequence below it (they wrap), 11 bits of node
// above (a positive int32).
const pidNodeShift = 20

// cancelRegistry maps this node's process IDs to their connections.
type cancelRegistry struct {
	mu    sync.Mutex
	seq   int32
	byPID map[int32]*conn
}

func newCancelRegistry() *cancelRegistry { return &cancelRegistry{byPID: map[int32]*conn{}} }

// newSecret draws a connection's cancellation secret. Zero is drawn
// again, and for the opposite reason to the obvious one: CancelBySecret
// refuses a zero outright, so a connection issued zero would be
// cancellable by nobody — its own client's Ctrl-C would silently do
// nothing — while being the one connection that a regression in that
// refusal would make cancellable by everybody. Redrawing costs a
// comparison and removes both.
func newSecret() uint32 {
	for {
		var buf [4]byte
		_, _ = rand.Read(buf[:])
		if secret := binary.BigEndian.Uint32(buf[:]); secret != 0 {
			return secret
		}
	}
}

// register assigns a connection its process ID and secret.
func (r *cancelRegistry) register(c *conn, nodeID int32) {
	secret := newSecret()
	r.mu.Lock()
	defer r.mu.Unlock()
	for {
		r.seq = (r.seq + 1) & (1<<pidNodeShift - 1)
		if r.seq == 0 {
			continue
		}
		pid := nodeID<<pidNodeShift | r.seq
		if _, taken := r.byPID[pid]; taken {
			continue
		}
		c.pid, c.secret = pid, secret
		r.byPID[pid] = c
		return
	}
}

func (r *cancelRegistry) unregister(c *conn) {
	r.mu.Lock()
	delete(r.byPID, c.pid)
	r.mu.Unlock()
}

func (r *cancelRegistry) lookup(pid int32) *conn {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.byPID[pid]
}

// pidNode is the node a process ID belongs to.
func pidNode(pid int32) int32 { return pid >> pidNodeShift }

// CancelBySecret cancels the statement in flight on this node's
// connection pid, on the authority of the secret issued to that
// connection in BackendKeyData. A wire CancelRequest carries no
// identity, so the secret is the whole authorization and must match
// exactly; zero is what a caller sends when it holds none, and never
// matches. Reports whether a connection was cancelled.
func (s *Server) CancelBySecret(pid int32, secret uint32) bool {
	if secret == 0 {
		return false
	}
	c := s.cancel.lookup(pid)
	if c == nil || c.secret != secret {
		return false
	}
	c.cancelStatement()
	return true
}

// ControlBackend cancels (or, with terminate, ends) this node's
// connection pid on the caller's admin authority, and takes no secret
// because an administrator is not expected to know one. That authority
// is established before the call and on this node: pkg/sql admin-gates
// pg_cancel_backend and pg_terminate_backend for a local session, and
// serveAdmin checks the principal's admin role for a cancel-query
// arriving over the internode RPC — whether a peer forwarded it (CN
// "node") or an operator sent it directly. Reports whether a connection
// was found.
func (s *Server) ControlBackend(pid int32, terminate bool) bool {
	c := s.cancel.lookup(pid)
	if c == nil {
		return false
	}
	if terminate {
		c.terminate()
		return true
	}
	c.cancelStatement()
	return true
}

// noSecret and secretCancel name the two constant arguments to the
// forwarding hook, whose bare 0 and false say nothing on their own: an
// admin cancel carries no secret, and a secret-authorized cancel never
// terminates.
const (
	noSecret     uint32 = 0
	secretCancel bool   = false
)

// cancelBySecret routes a wire cancel by process ID: locally, or to the
// node the ID names, where the secret is checked a second time.
func (s *Server) cancelBySecret(ctx context.Context, pid int32, secret uint32) (bool, error) {
	if node := pidNode(pid); node != s.opts.NodeID && s.opts.Forward != nil {
		return s.opts.Forward(ctx, node, pid, secret, secretCancel)
	}
	return s.CancelBySecret(pid, secret), nil
}

// controlBackend routes an admin cancel or terminate the same way.
func (s *Server) controlBackend(ctx context.Context, pid int32, terminate bool) (bool, error) {
	if node := pidNode(pid); node != s.opts.NodeID && s.opts.Forward != nil {
		return s.opts.Forward(ctx, node, pid, noSecret, terminate)
	}
	return s.ControlBackend(pid, terminate), nil
}

// handleCancelRequest serves the out-of-band CancelRequest that opened
// a connection of its own. A zero secret is refused at the door rather
// than at the far end, so that nothing forwards one to a node still
// running a binary that would honour it.
func (s *Server) handleCancelRequest(pid int32, secret uint32) {
	if secret == 0 {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_, _ = s.cancelBySecret(ctx, pid, secret)
}

// backendControl is the session hook behind pg_cancel_backend and
// pg_terminate_backend, which pkg/sql runs only for an admin.
func (s *Server) backendControl(pid int32, terminate bool) (bool, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	return s.controlBackend(ctx, pid, terminate)
}

// beginStatement derives the statement's context and registers its
// cancel; endStatement forgets it.
func (c *conn) beginStatement(ctx context.Context) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancel(ctx)
	c.mu.Lock()
	c.stmtCancel = cancel
	c.mu.Unlock()
	return ctx, func() {
		c.mu.Lock()
		c.stmtCancel = nil
		c.mu.Unlock()
		cancel()
	}
}

// cancelStatement cancels the statement in flight, if any (an idle
// connection ignores a cancel, as PostgreSQL does).
func (c *conn) cancelStatement() {
	c.mu.Lock()
	cancel := c.stmtCancel
	c.mu.Unlock()
	if cancel != nil {
		cancel()
	}
}

// terminate ends the connection: the statement in flight is cancelled,
// and the connection says goodbye (57P01) at once when idle or as soon
// as the statement returns.
func (c *conn) terminate() {
	c.cancelStatement()
	c.drain(true)
}

// idleInTxnTimeout is the FATAL a connection ends with when it sat idle
// inside a transaction past idle_in_transaction_session_timeout: the
// transaction is rolled back (its intents released) and the client
// reconnects.
var idleInTxnTimeout = &pgproto3.ErrorResponse{
	Severity: "FATAL", SeverityUnlocalized: "FATAL", Code: sql.CodeIdleInTransactionTimeout,
	Message: "terminating connection due to idle-in-transaction timeout",
}
