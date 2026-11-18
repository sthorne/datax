package sql

import (
	"context"

	"github.com/sthorne/datax/pkg/kvclient"
	"github.com/sthorne/datax/pkg/sql/parser"
)

// execLockTable runs LOCK TABLE. datax's DDL is online — a schema
// change never waits for the statements reading or writing a table —
// so the weak modes, which conflict with nothing but DDL-strength
// locks, have nothing to wait for: ACCESS SHARE (pg_dump's), ROW SHARE
// and ROW EXCLUSIVE check the tables and the privilege their mode
// needs and succeed. The stronger modes, which exist to keep other
// writers out, are refused rather than granted without effect.
func (s *Session) execLockTable(ctx context.Context, txn *kvclient.Txn, t *parser.LockTable) (*Result, error) {
	if s.state != StateOpen {
		return nil, newErrf(CodeNoActiveTransaction, "LOCK TABLE can only be used in transaction blocks")
	}
	var privs []string
	switch t.Mode {
	case "ACCESS SHARE":
		privs = []string{"SELECT"}
	case "ROW SHARE":
		privs = []string{"SELECT", "UPDATE", "DELETE", "TRUNCATE"}
	case "ROW EXCLUSIVE":
		privs = []string{"INSERT", "UPDATE", "DELETE", "TRUNCATE"}
	default:
		return nil, newErrf(CodeFeatureNotSupported, "LOCK TABLE IN %s MODE is not supported", t.Mode)
	}
	for _, name := range t.Tables {
		desc, err := s.lookup(ctx, txn, name)
		if err != nil {
			return nil, err
		}
		var perr error
		for _, p := range privs {
			if perr = s.checkTablePriv(ctx, txn, desc, p); perr == nil {
				break
			}
		}
		if perr != nil {
			return nil, perr
		}
	}
	return &Result{Tag: "LOCK TABLE"}, nil
}
