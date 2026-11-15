package catalog

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/kvclient"
)

// DroppedTable is the record DROP TABLE leaves beside the rows it no
// longer names (keys.DroppedTableKey; issue #313). Dropping a table
// removes its descriptor and name in the DDL's transaction and leaves
// its rows: an AS OF SYSTEM TIME read below the drop still resolves the
// table and reads them, as it may for as long as the table's history is
// kept. Once that window has passed no admissible read can reach them,
// and the reclaim job on the node leading range 1 clears the table's
// span and removes the record. The record is replicated state like any
// other, so a reclaim that was cut short — by a restart, a lost
// leadership — is taken up again on the next pass.
type DroppedTable struct {
	ID         uint64 `json:"id"`
	Name       string `json:"name"`
	DatabaseID uint64 `json:"database_id,omitempty"`
	// DroppedAt is the wall time of the dropping transaction's read
	// timestamp, in nanoseconds: at or before the moment the table
	// stopped existing.
	DroppedAt int64 `json:"dropped_at"`
	// WindowSeconds is how long the table kept its history while it
	// existed: its gc_ttl, or the retention of a time-series table. Zero
	// means the node's GC TTL, resolved when the job runs.
	WindowSeconds int64 `json:"window_seconds,omitempty"`
	// Cleared is set once every row has been deleted; what is left is
	// collecting the deletions, after which the record goes.
	Cleared bool `json:"cleared,omitempty"`
	// Swept marks a record DROP TABLE did not write: the one-time sweep
	// (issue #316) found rows under an id with no table and no record —
	// a table dropped before DROP TABLE left records — and wrote it.
	// Name is empty and DroppedAt is when the sweep found the rows: the
	// drop was at or before it.
	Swept bool `json:"swept,omitempty"`
}

// Label names the dropped table for a log line or an event.
func (r *DroppedTable) Label() string {
	if r.Name == "" {
		return "(dropped before its record existed)"
	}
	return r.Name
}

// HistoryWindowSeconds is how long a table keeps its history: its own
// gc_ttl, or a time-series table's retention, or zero for the node's
// GC TTL.
func (d *TableDescriptor) HistoryWindowSeconds() int64 {
	if d.GCTTLSeconds > 0 {
		return d.GCTTLSeconds
	}
	if d.Timeseries && d.RetentionSeconds > 0 {
		return d.RetentionSeconds
	}
	return 0
}

// recordDroppedTable writes d's record in the dropping transaction. A
// view holds no rows and leaves none; a table of the cluster's own lives
// at a reserved id it is created at again, which a reclaim must never
// reach.
func recordDroppedTable(ctx context.Context, txn *kvclient.Txn, d *TableDescriptor) error {
	if d.IsView() || IsSystemTableID(d.ID) {
		return nil
	}
	rec := DroppedTable{
		ID: d.ID, Name: d.Name, DatabaseID: d.DatabaseID,
		DroppedAt:     txn.ReadTimestamp().WallTime,
		WindowSeconds: d.HistoryWindowSeconds(),
	}
	raw, err := json.Marshal(&rec)
	if err != nil {
		return err
	}
	return txn.Put(ctx, keys.DroppedTableKey(d.ID), raw)
}

// DecodeDroppedTable reads one record of keys.DroppedTableSpan.
func DecodeDroppedTable(raw []byte) (*DroppedTable, error) {
	var rec DroppedTable
	if err := json.Unmarshal(raw, &rec); err != nil {
		return nil, fmt.Errorf("corrupt dropped-table record: %w", err)
	}
	return &rec, nil
}
