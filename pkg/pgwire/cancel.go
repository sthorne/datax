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
// the same path without the secret (admin only).

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

// register assigns a connection its process ID and secret.
func (r *cancelRegistry) register(c *conn, nodeID int32) {
	var buf [4]byte
	_, _ = rand.Read(buf[:])
	secret := binary.BigEndian.Uint32(buf[:])
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

// CancelLocal cancels (or, with terminate, ends) the statement of this
// node's connection pid when the secret matches (0 skips the check:
// the admin functions); reports whether a connection was found.
func (s *Server) CancelLocal(pid int32, secret uint32, terminate bool) bool {
	c := s.cancel.lookup(pid)
	if c == nil {
		return false
	}
	if secret != 0 && c.secret != secret {
		return false
	}
	if terminate {
		c.terminate()
		return true
	}
	c.cancelStatement()
	return true
}

// cancelPID routes a cancel (or terminate) by process ID: locally, or
// to the node the ID names.
func (s *Server) cancelPID(ctx context.Context, pid int32, secret uint32, terminate bool) (bool, error) {
	if node := pidNode(pid); node != s.opts.NodeID && s.opts.Forward != nil {
		return s.opts.Forward(ctx, node, pid, secret, terminate)
	}
	return s.CancelLocal(pid, secret, terminate), nil
}

// handleCancelRequest serves the out-of-band CancelRequest that opened
// a connection of its own.
func (s *Server) handleCancelRequest(pid int32, secret uint32) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_, _ = s.cancelPID(ctx, pid, secret, false)
}

// backendControl is the session hook behind pg_cancel_backend and
// pg_terminate_backend.
func (s *Server) backendControl(pid int32, terminate bool) (bool, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	return s.cancelPID(ctx, pid, 0, terminate)
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
