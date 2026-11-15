package sql

import (
	"github.com/sthorne/datax/pkg/sql/notify"
	"github.com/sthorne/datax/pkg/sql/parser"
)

// LISTEN / UNLISTEN / NOTIFY and pg_notify (issue #297), with
// PostgreSQL's transactional behaviour: nothing a transaction does to
// its subscriptions, and nothing it notifies, takes effect unless and
// until it commits. Inside a block both are therefore only recorded
// here — a savepoint remembers how much was recorded, and rolling back
// to it forgets the rest — and applied in order when COMMIT succeeds:
// subscriptions first, so a session that listens and notifies in one
// transaction hears itself. A statement outside a block is its own
// transaction and goes through the same steps at its end.

// notifyState is a session's share of LISTEN / NOTIFY.
type notifyState struct {
	hub *notify.Hub
	// listener is the session's subscriptions and its queue of
	// notifications to deliver; the wire layer owns the delivery.
	listener *notify.Listener
	// pending are the notifications of the open transaction (or of the
	// statement running outside one), in order, without duplicates; seen
	// is the set of them.
	pending []notify.Notification
	seen    map[notify.Notification]struct{}
	// actions are the open transaction's LISTEN and UNLISTEN statements.
	actions []listenAction
	// savepoints mirror the transaction's: how much of pending and
	// actions existed when each was established.
	savepoints []notifySavepoint
}

type listenAction struct {
	db, channel string
	// unlisten removes the subscription (every one, with all).
	unlisten, all bool
}

type notifySavepoint struct {
	name             string
	pending, actions int
}

// SetNotify gives the session its node's notification hub and its own
// listener (the wire layer's; nil for a session that can notify but has
// nowhere to receive).
func (s *Session) SetNotify(hub *notify.Hub, l *notify.Listener) {
	s.notify.hub, s.notify.listener = hub, l
}

// checkChannel validates a channel name the way PostgreSQL does.
func checkChannel(channel string) *Error {
	switch {
	case channel == "":
		return newErrf(CodeInvalidParameterValue, "channel name cannot be empty")
	case len(channel) > notify.MaxChannelLen:
		return newErrf(CodeInvalidParameterValue, "channel name too long")
	}
	return nil
}

// queueNotify records a notification for the transaction in progress.
// A notification identical to one the transaction already holds is
// dropped, as PostgreSQL drops it.
func (s *Session) queueNotify(channel, payload string) *Error {
	if s.notify.hub == nil {
		return newErrf(CodeFeatureNotSupported, "NOTIFY is not available on this session")
	}
	if serr := checkChannel(channel); serr != nil {
		return serr
	}
	if len(payload) > notify.MaxPayloadLen {
		return newErrf(CodeInvalidParameterValue, "payload string too long")
	}
	n := notify.Notification{Database: s.database, Channel: channel, Payload: payload, PID: s.BackendPID}
	if _, dup := s.notify.seen[n]; dup {
		return nil
	}
	if s.notify.seen == nil {
		s.notify.seen = make(map[notify.Notification]struct{})
	}
	s.notify.seen[n] = struct{}{}
	s.notify.pending = append(s.notify.pending, n)
	return nil
}

// execListenNotify runs LISTEN, UNLISTEN and NOTIFY: recorded in a
// transaction block, effective at once outside one.
func (s *Session) execListenNotify(stmt parser.Statement) (*Result, *Error) {
	var tag string
	switch t := stmt.(type) {
	case *parser.Notify:
		if s.state != StateOpen {
			s.discardNotify()
		}
		if serr := s.queueNotify(t.Channel, t.Payload); serr != nil {
			if s.state == StateOpen {
				s.state = StateFailed
			}
			return nil, serr
		}
		tag = "NOTIFY"
	case *parser.Listen, *parser.Unlisten:
		if s.notify.listener == nil {
			return nil, newErrf(CodeFeatureNotSupported, "LISTEN is not available on this session")
		}
		a := listenAction{db: s.database}
		if l, ok := t.(*parser.Listen); ok {
			a.channel, tag = l.Channel, "LISTEN"
		} else {
			u := t.(*parser.Unlisten)
			a.channel, a.unlisten, a.all, tag = u.Channel, true, u.All, "UNLISTEN"
		}
		if !a.all {
			if serr := checkChannel(a.channel); serr != nil {
				if s.state == StateOpen {
					s.state = StateFailed
				}
				return nil, serr
			}
		}
		if s.state != StateOpen {
			s.discardNotify()
		}
		s.notify.actions = append(s.notify.actions, a)
	}
	if s.state != StateOpen {
		s.commitNotify()
	}
	return &Result{Tag: tag}, nil
}

// commitNotify applies what a transaction that has just committed
// recorded: its subscription changes, then its notifications.
func (s *Session) commitNotify() {
	for _, a := range s.notify.actions {
		switch {
		case a.all:
			s.notify.listener.UnlistenAll()
		case a.unlisten:
			s.notify.listener.Unlisten(a.db, a.channel)
		default:
			s.notify.listener.Listen(a.db, a.channel)
		}
	}
	pending := s.notify.pending
	s.discardNotify()
	if s.notify.hub != nil {
		s.notify.hub.Publish(pending)
	}
}

// takeNotify hands the recorded notifications to a caller that will
// publish them itself once its transaction commits (a streamed result,
// whose implicit transaction outlives the statement's execution).
func (s *Session) takeNotify() []notify.Notification {
	pending := s.notify.pending
	s.notify.pending, s.notify.seen = nil, nil
	return pending
}

// discardNotify forgets what was recorded: the transaction rolled back,
// or a statement's attempt is starting over.
func (s *Session) discardNotify() {
	s.notify.pending, s.notify.seen, s.notify.actions, s.notify.savepoints = nil, nil, nil, nil
}

// notifySavepoint mirrors SAVEPOINT name. Establishing a name again
// moves it, which is how the transaction layer treats it.
func (s *Session) notifySavepoint(name string) {
	sps := s.notify.savepoints[:0]
	for _, sp := range s.notify.savepoints {
		if sp.name != name {
			sps = append(sps, sp)
		}
	}
	s.notify.savepoints = append(sps, notifySavepoint{name: name, pending: len(s.notify.pending), actions: len(s.notify.actions)})
}

// notifyReleaseSavepoint mirrors RELEASE: the savepoint and those after
// it are gone, and what was recorded since stays.
func (s *Session) notifyReleaseSavepoint(name string) {
	for i, sp := range s.notify.savepoints {
		if sp.name == name {
			s.notify.savepoints = s.notify.savepoints[:i]
			return
		}
	}
}

// notifyRollbackToSavepoint mirrors ROLLBACK TO: what was recorded
// after the savepoint is forgotten; the savepoint itself survives.
func (s *Session) notifyRollbackToSavepoint(name string) {
	for i, sp := range s.notify.savepoints {
		if sp.name != name {
			continue
		}
		s.notify.savepoints = s.notify.savepoints[:i+1]
		for _, n := range s.notify.pending[sp.pending:] {
			delete(s.notify.seen, n)
		}
		s.notify.pending = s.notify.pending[:sp.pending]
		s.notify.actions = s.notify.actions[:sp.actions]
		return
	}
}
