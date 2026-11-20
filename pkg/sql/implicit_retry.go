package sql

import (
	"context"
	"errors"
	"math/rand/v2"
	"time"

	"github.com/sthorne/datax/pkg/kvclient"
	"github.com/sthorne/datax/pkg/kvpb"
	"github.com/sthorne/datax/pkg/sql/parser"
)

// An implicit (autocommit) statement is its own transaction, so the
// server can re-run all of it when it loses a conflict, and the client
// never sees the 40001 — PostgreSQL never fails a CREATE TABLE with a
// serialization error, and the migration tools that run autocommit DDL
// do not retry one (issue #392). DDL contends on a few hot keys (the
// namespace, the descriptor-ID counter), so the retries are bounded by
// time rather than by a small count: they go on, with a jittered
// exponential backoff and a rising priority, until the statement
// succeeds, its deadline (statement_timeout, or the caller's) is too
// near for another attempt, or implicitRetryBudget is spent.
const (
	implicitRetryBudget  = 30 * time.Second
	implicitBackoffFirst = 5 * time.Millisecond
	implicitBackoffMax   = 250 * time.Millisecond
)

// implicitRetryable reports whether an implicit statement that failed
// with err may be re-run: a retryable transaction error, also once a
// statement has made it into its 40001 (*Error), but never a write the
// storage shed under overload — re-running the whole statement is the
// load the store asked to be spared.
func implicitRetryable(err error) bool {
	var kerr *kvpb.Error
	if errors.As(err, &kerr) && kerr.StorageOverloaded != nil {
		return false
	}
	var re *kvclient.RetryableError
	if errors.As(err, &re) && re.Cause != nil && re.Cause.StorageOverloaded != nil {
		return false
	}
	if kvclient.IsRetryable(err) {
		return true
	}
	var se *Error
	return errors.As(err, &se) && se.Code == CodeSerializationFailure
}

// implicitBackoff is the wait before re-run number attempt+1: an
// exponential step with jitter, so that the sessions that collided do
// not collide again in lockstep.
func implicitBackoff(attempt int) time.Duration {
	d := implicitBackoffFirst << min(attempt, 10)
	d = min(d, implicitBackoffMax)
	return d/2 + rand.N(d/2+1)
}

// runImplicit runs fn in an implicit transaction and commits it,
// re-running it in a fresh transaction on a retryable error (see
// above). Before each re-run the catalog cache forgets the tables the
// failed attempt changed: what it holds for them may be that attempt's
// own uncommitted view.
func (s *Session) runImplicit(ctx context.Context, stmt parser.Statement, fn func(ctx context.Context, txn *kvclient.Txn) error) error {
	start := time.Now()
	limit := start.Add(implicitRetryBudget)
	if d, ok := ctx.Deadline(); ok && d.Before(limit) {
		limit = d
	}
	txn := s.db.NewTxn("sql-implicit")
	txn.EnablePipelining()
	for attempt := 0; ; attempt++ {
		began := time.Now()
		err := fn(ctx, txn)
		if err == nil {
			err = txn.Commit(ctx)
		}
		if err == nil {
			return nil
		}
		_ = txn.Rollback(ctx)
		if !implicitRetryable(err) || ctx.Err() != nil {
			return err
		}
		if name := ddlTableName(stmt); name != "" {
			s.cat.Invalidate(name)
		}
		for _, name := range s.extraDDL {
			s.cat.Invalidate(name)
		}
		// Another attempt must fit before the deadline: the wait, and
		// about as long as this attempt took.
		wait := implicitBackoff(attempt)
		if time.Now().Add(wait + time.Since(began)).After(limit) {
			return err
		}
		next := s.db.NewTxn("sql-implicit")
		next.EnablePipelining()
		next.BumpPriority(txn) // push harder each retry
		txn = next
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(wait):
		}
	}
}
