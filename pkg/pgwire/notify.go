package pgwire

import (
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgproto3"

	"github.com/sthorne/datax/pkg/sql"
	"github.com/sthorne/datax/pkg/sql/notify"
)

// Delivering notifications (issue #297). A listening session's
// notifications wait in its listener's queue (pkg/sql/notify) and reach
// the client at the two points PostgreSQL sends them:
//
//   - ahead of ReadyForQuery, whenever a command cycle ends outside a
//     transaction block (sendReady) — which is how a session receives
//     what its own COMMIT notified before the COMMIT is acknowledged;
//   - at any moment while the connection sits idle outside a
//     transaction, waiting for the client's next message. A client that
//     issues LISTEN and then only reads its socket — every driver's
//     notification loop — is sent each notification as it is committed,
//     without having to run a statement to collect it.
//
// The second is the one that needs care, because the connection's own
// goroutine is blocked reading the socket then and cannot write. A
// second goroutine per listening connection (pushLoop, started when the
// first notification is queued) does the writing, and the two never
// write at once: pushOK, under pushMu, is true exactly while the
// connection goroutine is parked in Receive at a cycle boundary with
// nothing buffered and no transaction open. The pusher writes only
// while holding pushMu with pushOK set; the connection goroutine takes
// pushMu to clear it as soon as Receive returns, before it writes
// anything, so it waits out a push in progress and the client never
// sees a NotificationResponse in the middle of another message's
// answer. Reading and writing a net.Conn (or a tls.Conn) from two
// goroutines at once is allowed; two writers are what must not overlap.
//
// A client that listens but does not read its socket fills the socket
// buffer and blocks the pusher, not the sessions notifying it: its
// notifications pile up in the listener's bounded queue, and when that
// overflows the session is ended (notifyEnd).

// pushGrace is how long a connection that is being closed waits for a
// push in progress before closing the socket under it.
const pushGrace = time.Second

// wakePush tells the pusher there is something to send. It is called
// from the notifying session's commit and must not block.
func (c *conn) wakePush() {
	c.pushStart.Do(func() { go c.pushLoop() })
	select {
	case c.pushWake <- struct{}{}:
	default:
	}
}

// pushLoop writes queued notifications to an idle client.
func (c *conn) pushLoop() {
	for {
		select {
		case <-c.pushWake:
		case <-c.done:
			return
		}
		c.pushMu.Lock()
		if c.pushOK {
			var buf []byte
			for _, n := range c.listener.Take() {
				buf, _ = notificationResponse(n).Encode(buf)
			}
			if len(buf) > 0 {
				if _, err := c.nc.Write(buf); err != nil {
					// The connection is gone; closing it is how the
					// goroutine reading it finds out.
					_ = c.nc.Close()
				}
			}
		}
		c.pushMu.Unlock()
	}
}

// allowPush opens the window in which the pusher may write: the
// connection goroutine is about to wait for a client message.
func (c *conn) allowPush() {
	c.pushMu.Lock()
	c.pushOK = true
	c.pushMu.Unlock()
	// What arrived since the last ReadyForQuery found the window shut.
	if c.listener.Pending() {
		c.wakePush()
	}
}

// endPush closes the window, waiting for a push in progress. With
// bounded set (the read ended in an error or a wake-up, so the
// connection is on its way out) the wait is limited to pushGrace: a
// client that has stopped reading would otherwise hold a closing
// connection open, and the socket is closed under the push instead.
// Reports false when it had to do that.
func (c *conn) endPush(bounded bool) bool {
	if !bounded {
		c.pushMu.Lock()
	} else {
		deadline := time.Now().Add(pushGrace)
		for !c.pushMu.TryLock() {
			if time.Now().After(deadline) {
				_ = c.nc.Close()
				return false
			}
			time.Sleep(time.Millisecond)
		}
	}
	c.pushOK = false
	c.pushMu.Unlock()
	return true
}

// sendNotifications queues the waiting notifications on the backend,
// ahead of the ReadyForQuery the caller is about to send.
func (c *conn) sendNotifications() {
	for _, n := range c.listener.Take() {
		c.backend.Send(notificationResponse(n))
	}
}

func notificationResponse(n notify.Notification) *pgproto3.NotificationResponse {
	return &pgproto3.NotificationResponse{PID: uint32(n.PID), Channel: n.Channel, Payload: n.Payload}
}

// notifyEnd ends a listening session that has missed notifications or
// is about to: the client is told why with a FATAL, so it reconnects,
// listens again and re-reads whatever the notifications stood for,
// instead of waiting on a channel that has silently skipped some. It is
// called from the notifying session's commit and must not block: an
// idle connection is woken to say goodbye, a busy one says it at the
// end of the command cycle it is in.
func (c *conn) notifyEnd(r notify.Reason) {
	bye := &pgproto3.ErrorResponse{Severity: "FATAL", SeverityUnlocalized: "FATAL"}
	switch r {
	case notify.ReasonQueueFull:
		bye.Code = sql.CodeProgramLimitExceeded
		bye.Message = fmt.Sprintf("terminating connection: its notification queue is full (%d bytes of notifications were waiting to be read)", notify.DefaultSessionQueueBytes)
	default:
		bye.Code = "57P01"
		bye.Message = "terminating connection: notifications from another node were lost before they reached this one"
	}
	bye.Hint = "Reconnect, LISTEN again, and re-read the state the notifications describe."
	c.mu.Lock()
	defer c.mu.Unlock()
	c.st.draining, c.st.cut = true, true
	if c.st.bye == nil {
		c.st.bye = bye
	}
	if c.st.idle {
		_ = c.nc.SetReadDeadline(time.Now()) // wakes Receive; see run
	}
}

// advisoryLost ends the connection because its node lost the lease its
// advisory locks were held under (pkg/sql/advisory): another session
// may have been granted them since. A statement in flight is cancelled;
// the connection ends with FATAL at once if idle, or after it.
func (c *conn) advisoryLost() {
	bye := &pgproto3.ErrorResponse{Severity: "FATAL", SeverityUnlocalized: "FATAL", Code: "57P01",
		Message: "terminating connection: this node could not renew its advisory-lock lease in time, so the advisory locks this session held may have been granted to other sessions",
		Hint:    "Reconnect and take the locks again."}
	c.mu.Lock()
	c.st.draining, c.st.cut = true, true
	if c.st.bye == nil {
		c.st.bye = bye
	}
	idle := c.st.idle
	if idle {
		_ = c.nc.SetReadDeadline(time.Now()) // wakes Receive; see run
	}
	c.mu.Unlock()
	if !idle {
		c.cancelStatement()
	}
}
