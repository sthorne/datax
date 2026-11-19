package sql

import (
	"context"
	"fmt"
	"time"

	"github.com/sthorne/datax/pkg/sql/parser"
	"github.com/sthorne/datax/pkg/sql/types"
	"github.com/sthorne/datax/pkg/util/hlc"
)

// Historical and follower reads: the AS OF SYSTEM TIME forms
// CockroachDB has (CR-AOST-001). A statement's AS OF SYSTEM TIME runs
// in its own historical transaction (session.go); BEGIN AS OF SYSTEM
// TIME and SET TRANSACTION AS OF SYSTEM TIME pin a whole block there,
// read-only, as SET TRANSACTION SNAPSHOT does.

// FollowerReadLag is how far behind now follower_read_timestamp()
// reads: past the closed-timestamp lag (3s by default) and its
// publication interval (1s), with a margin, so that any replica of any
// range has closed the timestamp and serves the read itself.
const FollowerReadLag = 4800 * time.Millisecond

// resolveAsOfClause turns an AS OF SYSTEM TIME operand into the
// statement's timestamp.
func (s *Session) resolveAsOfClause(c parser.AsOfClause) (hlc.Timestamp, *Error) {
	now := s.db.Clock().Now()
	switch {
	case c.NearestOnly:
		fn := "with_max_staleness"
		if c.MinTimestamp != "" {
			fn = "with_min_timestamp"
		}
		return hlc.Timestamp{}, newErrf(CodeFeatureNotSupported, "%s with nearest_only = TRUE is not supported: a range this node cannot serve within the bound is read from its leader", fn)
	case c.MaxStaleness != "":
		ts, err := resolveMaxStaleness(c.MaxStaleness, now, s.db.LocalClosedTimestamp())
		if err != nil {
			return hlc.Timestamp{}, newErrf(CodeInvalidParameterValue, "with_max_staleness: %v", err)
		}
		return ts, nil
	case c.MinTimestamp != "":
		ts, err := resolveMinTimestamp(c.MinTimestamp, now, s.db.LocalClosedTimestamp())
		if err != nil {
			return hlc.Timestamp{}, newErrf(CodeInvalidParameterValue, "with_min_timestamp: %v", err)
		}
		return ts, nil
	}
	ts, err := resolveAsOf(c.Exact, now)
	if err != nil {
		return hlc.Timestamp{}, newErrf(CodeSyntaxError, "AS OF SYSTEM TIME: %v", err)
	}
	return ts, nil
}

// resolveTxnAsOf is resolveAsOfClause for a transaction block: only an
// exact timestamp pins one, as in CockroachDB, where bounded staleness
// is a single statement's.
func (s *Session) resolveTxnAsOf(c parser.AsOfClause) (hlc.Timestamp, *Error) {
	if c.MaxStaleness != "" || c.MinTimestamp != "" {
		return hlc.Timestamp{}, newErrf(CodeFeatureNotSupported, "with_max_staleness and with_min_timestamp are allowed only in a single SELECT, not for a transaction block")
	}
	return s.resolveAsOfClause(c)
}

// resolveMinTimestamp turns a with_min_timestamp operand into the
// statement timestamp: the freshest the local store can serve
// (localClosed), but never older than the bound. The bound must not be
// in the future.
func resolveMinTimestamp(operand string, now, localClosed hlc.Timestamp) (hlc.Timestamp, error) {
	var nanos int64
	if t, err := time.Parse(time.RFC3339Nano, operand); err == nil {
		nanos = t.UnixNano()
	} else if n, err := types.ParseTimestamp(operand); err == nil {
		nanos = n
	} else {
		return hlc.Timestamp{}, fmt.Errorf("cannot interpret %q as a timestamp", operand)
	}
	ts := hlc.Timestamp{WallTime: nanos}
	if now.Less(ts) {
		return hlc.Timestamp{}, fmt.Errorf("minimum timestamp %q is in the future", operand)
	}
	if ts.Less(localClosed) {
		ts = localClosed
	}
	return ts, nil
}

// execSetTransactionAsOf is SET TRANSACTION AS OF SYSTEM TIME: before
// the block's first query, it becomes a read-only transaction at that
// timestamp.
func (s *Session) execSetTransactionAsOf(t *parser.SetTransactionAsOf) (*Result, *Error) {
	if s.state != StateOpen {
		return nil, newErrf(CodeNoActiveTransaction, "SET TRANSACTION AS OF SYSTEM TIME can only be used in transaction blocks")
	}
	if s.txnQueried {
		return nil, newErrf(CodeActiveTransaction, "SET TRANSACTION AS OF SYSTEM TIME must be called before any query")
	}
	ts, serr := s.resolveTxnAsOf(t.AsOf)
	if serr != nil {
		return nil, serr
	}
	if s.txn != nil {
		_ = s.txn.Rollback(context.Background())
	}
	s.txn = s.db.NewHistoricalTxn("sql-asof", ts)
	return &Result{Tag: "SET"}, nil
}
