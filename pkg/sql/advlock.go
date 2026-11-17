package sql

import (
	"context"
	"errors"
	"math"

	"github.com/sthorne/datax/pkg/sql/advisory"
	"github.com/sthorne/datax/pkg/sql/parser"
	"github.com/sthorne/datax/pkg/sql/types"
	"github.com/sthorne/datax/pkg/sql/vtable"
)

// Advisory locks (issue #353): pg_advisory_lock and its relatives, with
// PostgreSQL's semantics, held cluster-wide by pkg/sql/advisory (whose
// package comment has the design). The session keeps the bookkeeping
// that belongs to it — which holds are its transaction's, savepoints —
// and evaluates the functions once per call site and statement, as it
// does the other functions with effects (nextval, the large-object
// functions): their arguments must be constants or parameters.

// advisoryFunc describes one advisory-lock function.
type advisoryFunc struct {
	mode   advisory.Mode
	xact   bool // held by the transaction rather than the session
	try    bool // reports false instead of waiting
	unlock bool
	all    bool // pg_advisory_unlock_all
}

var advisoryFuncs = map[string]advisoryFunc{
	"pg_advisory_lock":                 {mode: advisory.Exclusive},
	"pg_advisory_lock_shared":          {mode: advisory.Share},
	"pg_try_advisory_lock":             {mode: advisory.Exclusive, try: true},
	"pg_try_advisory_lock_shared":      {mode: advisory.Share, try: true},
	"pg_advisory_xact_lock":            {mode: advisory.Exclusive, xact: true},
	"pg_advisory_xact_lock_shared":     {mode: advisory.Share, xact: true},
	"pg_try_advisory_xact_lock":        {mode: advisory.Exclusive, xact: true, try: true},
	"pg_try_advisory_xact_lock_shared": {mode: advisory.Share, xact: true, try: true},
	"pg_advisory_unlock":               {mode: advisory.Exclusive, unlock: true},
	"pg_advisory_unlock_shared":        {mode: advisory.Share, unlock: true},
	"pg_advisory_unlock_all":           {unlock: true, all: true},
}

func init() {
	for name := range advisoryFuncs {
		volatileFuncs[name] = true
		splicedFuncs[name] = true
	}
}

// advisoryState is a session's share of the advisory locks.
type advisoryState struct {
	h *advisory.Holder
	// savepoints mirror the transaction's: how many transaction-level
	// acquisitions there were when each was established.
	savepoints []advisorySavepoint
	// calls are the statement's calls so far, in order, and next the
	// position of the call the statement makes next. An attempt that
	// starts the statement over (an internal retry) rewinds next, and
	// the calls it makes again replay what was done instead of doing it
	// twice: a retried pg_advisory_lock must not count two holds.
	calls []advisoryCall
	next  int
}

type advisorySavepoint struct {
	name string
	mark int
}

type advisoryCall struct {
	name   string
	key    advisory.Key
	result types.Datum
}

// SetAdvisory gives the session its advisory-lock state (nil: the
// functions are refused).
func (s *Session) SetAdvisory(h *advisory.Holder) { s.adv.h = h }

// advisoryStatement starts a statement's bookkeeping; advisoryRestart
// starts an attempt at it over.
func (s *Session) advisoryStatement() { s.adv.calls, s.adv.next = nil, 0 }
func (s *Session) advisoryRestart()   { s.adv.next = 0 }

// advisoryEndTxn releases the transaction-level holds: the transaction
// (or the statement outside a block) has ended.
func (s *Session) advisoryEndTxn() {
	s.adv.savepoints = nil
	if s.adv.h != nil {
		s.adv.h.EndXact()
	}
}

func (s *Session) advisorySavepoint(name string) {
	if s.adv.h == nil {
		return
	}
	sps := s.adv.savepoints[:0]
	for _, sp := range s.adv.savepoints {
		if sp.name != name {
			sps = append(sps, sp)
		}
	}
	s.adv.savepoints = append(sps, advisorySavepoint{name: name, mark: s.adv.h.XactMark()})
}

func (s *Session) advisoryReleaseSavepoint(name string) {
	for i, sp := range s.adv.savepoints {
		if sp.name == name {
			s.adv.savepoints = s.adv.savepoints[:i]
			return
		}
	}
}

// advisoryRollbackToSavepoint gives back the transaction-level holds
// taken since the savepoint, as PostgreSQL's subtransaction abort does.
func (s *Session) advisoryRollbackToSavepoint(name string) {
	for i, sp := range s.adv.savepoints {
		if sp.name == name {
			s.adv.savepoints = s.adv.savepoints[:i+1]
			s.adv.h.RollbackXact(sp.mark)
			return
		}
	}
}

// advisoryClose releases everything (the session's end).
func (s *Session) advisoryClose() {
	s.adv.savepoints, s.adv.calls, s.adv.next = nil, nil, 0
	if s.adv.h != nil {
		s.adv.h.Close()
	}
}

// advisoryArgsConstant refuses a call whose arguments read a row: the
// session evaluates it once, before the rows are read.
func advisoryArgsConstant(e parser.Expr) *Error {
	for _, a := range e.Args {
		if exprHas(a, func(x parser.Expr) bool { return x.Column != "" || x.Sub != nil }) {
			return newErrf(CodeFeatureNotSupported, "%s() takes constants or parameters as its arguments, not columns or subqueries: it is evaluated once per statement (take one lock per statement, or read the keys first)", e.Func)
		}
	}
	return nil
}

// advisoryKey is the lock a call's arguments name.
func (s *Session) advisoryKey(name string, args []types.Datum) (advisory.Key, bool, error) {
	for _, a := range args {
		if a.Null {
			return advisory.Key{}, true, nil // strict
		}
	}
	ints := make([]int64, len(args))
	for i, a := range args {
		d, err := a.Coerce(types.Int)
		if err != nil {
			return advisory.Key{}, false, newErrf(CodeInvalidTextRepresentation, "%s: %v", name, err)
		}
		ints[i] = d.I
	}
	switch len(ints) {
	case 1:
		return advisory.Int64Key(s.database, ints[0]), false, nil
	case 2:
		for _, v := range ints {
			if v < math.MinInt32 || v > math.MaxInt32 {
				return advisory.Key{}, false, newErrf(CodeNumericValueOutOfRange, "integer out of range")
			}
		}
		return advisory.Int32Key(s.database, int32(ints[0]), int32(ints[1])), false, nil
	}
	return advisory.Key{}, false, newErrf(CodeUndefinedFunction, "function %s() with %d arguments does not exist", name, len(args))
}

// evalAdvisoryFunc runs one advisory-lock call.
func (s *Session) evalAdvisoryFunc(ctx context.Context, name string, args []types.Datum) (types.Datum, error) {
	f := advisoryFuncs[name]
	if s.adv.h == nil {
		return types.Datum{}, newErrf(CodeFeatureNotSupported, "advisory locks are not available on this session")
	}
	var key advisory.Key
	if f.all {
		if len(args) != 0 {
			return types.Datum{}, newErrf(CodeUndefinedFunction, "function %s() takes no arguments", name)
		}
	} else {
		k, null, err := s.advisoryKey(name, args)
		if err != nil {
			return types.Datum{}, err
		}
		if null {
			return types.DNull, nil
		}
		key = k
	}
	if s.adv.next < len(s.adv.calls) {
		if c := s.adv.calls[s.adv.next]; c.name == name && c.key == key {
			s.adv.next++
			return c.result, nil
		}
	}
	s.adv.calls = s.adv.calls[:s.adv.next]
	d, err := s.runAdvisory(ctx, name, f, key)
	if err != nil {
		if errors.Is(err, advisory.ErrLost) {
			return types.Datum{}, newErrf(CodeAdminShutdown, "terminating connection: %v", err)
		}
		return types.Datum{}, err
	}
	s.adv.calls = append(s.adv.calls, advisoryCall{name: name, key: key, result: d})
	s.adv.next++
	return d, nil
}

func (s *Session) runAdvisory(ctx context.Context, name string, f advisoryFunc, key advisory.Key) (types.Datum, error) {
	h := s.adv.h
	h.SetPID(s.BackendPID)
	switch {
	case f.all:
		if err := h.UnlockAll(); err != nil {
			return types.Datum{}, err
		}
		return types.NewString(""), nil
	case f.unlock:
		ok, err := h.Unlock(key, f.mode)
		if err != nil {
			return types.Datum{}, err
		}
		if !ok {
			s.warnings = append(s.warnings, "you don't own a lock of type "+f.mode.String())
		}
		return types.NewBool(ok), nil
	}
	ok, err := h.Lock(ctx, key, f.mode, f.xact, !f.try)
	if err != nil {
		return types.Datum{}, err
	}
	if f.try {
		return types.NewBool(ok), nil
	}
	return types.NewString(""), nil
}

// listAdvisoryLocks is pg_locks' source: the granted advisory locks of
// the whole cluster.
func (s *Session) listAdvisoryLocks(ctx context.Context) ([]vtable.AdvisoryLockInfo, error) {
	infos, err := s.adv.h.Manager().List(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]vtable.AdvisoryLockInfo, 0, len(infos))
	for _, in := range infos {
		out = append(out, vtable.AdvisoryLockInfo{Database: in.DB, ClassID: in.ClassID, ObjID: in.ObjID, ObjSubID: in.SubID, PID: in.PID, Mode: in.Mode.String()})
	}
	return out, nil
}
