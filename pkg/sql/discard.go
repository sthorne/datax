package sql

import (
	"errors"

	"github.com/sthorne/datax/pkg/sql/advisory"
	"github.com/sthorne/datax/pkg/sql/parser"
)

// DISCARD (what connection poolers run to hand a connection on clean):
// DISCARD PLANS drops the session's cached plans, DISCARD SEQUENCES its
// currval / lastval state, DISCARD TEMP its temporary tables (datax has
// none), and DISCARD ALL all of that and the rest of the session's state,
// as PostgreSQL's does: it closes every cursor, resets every setting to
// its default (the role included, which is SET SESSION AUTHORIZATION
// DEFAULT), deallocates every prepared statement, stops listening on
// every channel and releases the session's advisory locks. The wire
// layer drops its named protocol-level statements with it. DISCARD ALL
// is refused inside a transaction block (25001), as in PostgreSQL.

func (s *Session) execDiscard(t *parser.Discard) (*Result, *Error) {
	switch t.Kind {
	case "plans":
		s.plans = nil
		return &Result{Tag: "DISCARD PLANS"}, nil
	case "sequences":
		s.seqs = nil
		return &Result{Tag: "DISCARD SEQUENCES"}, nil
	case "temp":
		return &Result{Tag: "DISCARD TEMP"}, nil
	}
	if s.state == StateOpen {
		return nil, newErrf(CodeActiveTransaction, "DISCARD ALL cannot run inside a transaction block")
	}
	s.cursors = nil
	s.vars = defaultVars()
	s.localVars = nil
	s.applyRole()
	s.preparedSQL = nil
	if s.notify.listener != nil {
		s.notify.listener.UnlistenAll()
	}
	if s.adv.h != nil {
		if err := s.adv.h.UnlockAll(); err != nil {
			if errors.Is(err, advisory.ErrLost) {
				return nil, newErrf(CodeAdminShutdown, "terminating connection: %v", err)
			}
			return nil, ToSQLError(err)
		}
	}
	s.plans = nil
	s.seqs = nil
	return &Result{Tag: "DISCARD ALL"}, nil
}
