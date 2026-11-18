package sql

import (
	"context"
	"fmt"

	"github.com/sthorne/datax/pkg/kvclient"
	"github.com/sthorne/datax/pkg/sql/parser"
	"github.com/sthorne/datax/pkg/util/hlc"
)

// Snapshot export and import (pg_export_snapshot(), SET TRANSACTION
// SNAPSHOT), over datax's consistent reads at a timestamp. A snapshot
// identifier names the exporting transaction's read timestamp; a
// transaction importing it becomes a read-only historical transaction
// pinned there — what AS OF SYSTEM TIME runs — and so reads exactly
// what the exporter read, needs no uncertainty restarts, and refuses
// writes. pg_dump uses the pair for its parallel workers (-j) and for
// --snapshot. Unlike PostgreSQL, the exporting transaction need not stay
// open: the identifier stays good while its timestamp is above the GC
// threshold.

// exportSnapshotID renders a read timestamp as a snapshot identifier.
func exportSnapshotID(ts hlc.Timestamp) string {
	return fmt.Sprintf("%016X-%08X-1", ts.WallTime, uint32(ts.Logical))
}

// parseSnapshotID reads a snapshot identifier back.
func parseSnapshotID(id string) (hlc.Timestamp, bool) {
	var wall int64
	var logical uint32
	var one int
	if n, err := fmt.Sscanf(id, "%016X-%08X-%d", &wall, &logical, &one); err != nil || n != 3 || one != 1 || wall <= 0 {
		return hlc.Timestamp{}, false
	}
	if exportSnapshotID(hlc.Timestamp{WallTime: wall, Logical: int32(logical)}) != id {
		return hlc.Timestamp{}, false
	}
	return hlc.Timestamp{WallTime: wall, Logical: int32(logical)}, true
}

// evalExportSnapshot is pg_export_snapshot(): the identifier of the
// statement's transaction's snapshot.
func (s *Session) evalExportSnapshot(txn *kvclient.Txn) (string, error) {
	if txn == nil {
		return "", newErrf(CodeNoActiveTransaction, "pg_export_snapshot() needs a transaction")
	}
	return exportSnapshotID(txn.ReadTimestamp()), nil
}

// execSetTransactionSnapshot is SET TRANSACTION SNAPSHOT 'id'.
func (s *Session) execSetTransactionSnapshot(t *parser.SetTransactionSnapshot) (*Result, *Error) {
	if s.state != StateOpen {
		return nil, newErrf(CodeNoActiveTransaction, "SET TRANSACTION SNAPSHOT can only be used in transaction blocks")
	}
	if s.txnQueried {
		return nil, newErrf(CodeActiveTransaction, "SET TRANSACTION SNAPSHOT must be called before any query")
	}
	ts, ok := parseSnapshotID(t.ID)
	if !ok || s.db.Clock().Now().Less(ts) {
		return nil, newErrf(CodeInvalidParameterValue, "invalid snapshot identifier: %q", t.ID)
	}
	if s.txn != nil {
		_ = s.txn.Rollback(context.Background())
	}
	s.txn = s.db.NewHistoricalTxn("sql-snapshot", ts)
	return &Result{Tag: "SET"}, nil
}
