package server

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/kvclient"
	"github.com/sthorne/datax/pkg/kvpb"
	"github.com/sthorne/datax/pkg/sql/catalog"
	"github.com/sthorne/datax/pkg/util/hlc"
	"github.com/sthorne/datax/pkg/version"
)

// Retention across a backup chain (issue #316).
//
// A time-series table's retention expires rows by commit time, beneath
// SQL, and leaves no tombstone. A plain incremental exports what changed
// since its base, so it says nothing about the rows that expired in
// between, and a restore of the chain used to bring them back; and a
// restore writes every row at its own time, so retention, counting from
// a version's commit time, kept every restored row a period longer than
// the source would have.
//
// Backups at cluster version v22 keep each time-series row's commit time
// (backupTable.CommitTimes; a history backup has every version's
// already). With them a restore applies the retention as the source did:
//
//   - A row whose commit time is at or below the chain's end minus the
//     table's retention (as of the chain's last backup) had expired at
//     the source by then, or was due to on its next GC pass, and is not
//     restored. The commit time is the one retention went by, not the
//     row's timestamp column: a row backfilled with an old timestamp
//     shortly before the backup was held by the source, and is restored.
//     The decision is the newest version's, per key, as of the chain's
//     end: that is the version the source's retention aged.
//   - The rows that are restored are written in bands of source commit
//     time, oldest first, each band after the one before has committed,
//     and the table's descriptor records each band
//     (catalog.TableDescriptor.RestoredRows). Retention then ages a
//     restored version by the latest source commit time in its band
//     (retention.go), so a restored row expires when it would have at the
//     source, late by at most one band — a sixteenth of the retention —
//     and never early.
//
// The bands cost reads, not writes: the table's files are read once per
// band, each pass writing only its band's rows. A chain in which some
// backup holding the table has no commit times (taken before v22) is
// restored as before, and the restore's summary says so.

// restoreExpiryBands is how many bands of commit time a restore writes a
// time-series table in: how late, as a share of the retention, a restored
// row can expire, against how many times its files are read.
const restoreExpiryBands = 16

// commitTimeExpiry is how a restore applies one time-series table's
// retention.
type commitTimeExpiry struct {
	// horizon: a row whose newest version committed at or below it had
	// expired at the source by the chain's end.
	horizon hlc.Timestamp
	// width is one band's span of commit time, in nanoseconds.
	width int64
	// bands says the restored rows are written in bands and the bands
	// recorded: cluster version v22, where every node's retention reads
	// them. Below it the expired rows are still left out.
	bands bool
}

// planCommitTimeExpiry works out how the restore of chain applies the
// retention of table tableID, whose descriptor in the chain's last
// backup is d: nil for a table without one, or one whose rows some
// backup of the chain holds without commit times, which is then restored
// as before — note says so.
func (n *Node) planCommitTimeExpiry(chain *restoreChain, tableID uint64, d *catalog.TableDescriptor, name string) (e *commitTimeExpiry, note string) {
	if !d.Timeseries || d.RetentionSeconds <= 0 {
		return nil, ""
	}
	for i, man := range chain.mans {
		for _, t := range man.Tables {
			if t.ID == tableID && !t.timed() {
				return nil, fmt.Sprintf("table %s: the backup in %s holds no commit times (taken before cluster version %s), so the table's retention is not applied: rows it expired between the chain's backups may be back, and every row is kept a retention period from now; delete the rows older than the retention to apply it",
					name, chain.srcs[i], version.V22)
			}
		}
	}
	r := time.Duration(d.RetentionSeconds) * time.Second
	e = &commitTimeExpiry{
		horizon: chain.asOf().AddNanos(-r.Nanoseconds()),
		width:   (r.Nanoseconds() + restoreExpiryBands - 1) / restoreExpiryBands,
		bands:   n.db.ClusterVersion() >= version.V22,
	}
	if !e.bands {
		note = fmt.Sprintf("table %s: the restored rows are kept a retention period from now, not from when they were written (that takes cluster version %s)", name, version.V22)
	}
	return e, note
}

// keep says a record of the chain's merge (the newest version of its key)
// is restored.
func (e *commitTimeExpiry) keep(rec kvpb.ExportRecord) bool {
	return e.horizon.Less(rec.Timestamp)
}

// band is the band a kept record is written in.
func (e *commitTimeExpiry) band(ts hlc.Timestamp) int {
	b := (ts.WallTime - e.horizon.WallTime - 1) / e.width
	switch {
	case b < 0:
		return 0
	case b >= restoreExpiryBands:
		return restoreExpiryBands - 1
	}
	return int(b)
}

// loadByCommitTime writes table tableID's rows as the chain leaves them,
// less the ones its retention had expired (e), in e's bands, under the
// keys rekey gives; it returns the bands to record and how many rows it
// left out.
func (n *Node) loadByCommitTime(ctx context.Context, chain *restoreChain, tableID uint64, e *commitTimeExpiry, txnName string, rekey func(keys.Key) keys.Key) ([]catalog.RestoredRowsBand, int64, error) {
	var bands []catalog.RestoredRowsBand
	var expired int64
	passes := restoreExpiryBands
	if !e.bands {
		passes = 1
	}
	for b := 0; b < passes; b++ {
		var wb kvclient.WriteBatch
		flush := func() error {
			if wb.Len() == 0 {
				return nil
			}
			batch := wb
			wb = kvclient.WriteBatch{}
			return n.db.RunTxn(ctx, txnName, func(ctx context.Context, txn *kvclient.Txn) error {
				b := batch
				return txn.RunBatch(ctx, &b)
			})
		}
		var writtenBy int64
		wrote := false
		err := mergeTable(chain, tableID, func(rec kvpb.ExportRecord) error {
			if !e.keep(rec) {
				if b == 0 {
					expired++
				}
				return nil
			}
			if e.bands && e.band(rec.Timestamp) != b {
				return nil
			}
			wb.Put(rekey(keys.Key(rec.Key)), rec.Value)
			wrote = true
			if rec.Timestamp.WallTime > writtenBy {
				writtenBy = rec.Timestamp.WallTime
			}
			if wb.Len() >= restoreChunk {
				return flush()
			}
			return nil
		})
		if err == nil {
			err = flush()
		}
		if err != nil {
			return nil, 0, err
		}
		if wrote && e.bands {
			// Every write of the band has committed, at or below this
			// node's clock (which each response has moved past the
			// timestamps it carried); the next band's transactions start
			// above it. A write that had committed above it would be
			// counted with a later band, whose rows are newer: kept
			// longer, never expired early.
			bands = append(bands, catalog.RestoredRowsBand{Through: n.clock.Now(), WrittenBy: writtenBy})
		}
	}
	return bands, expired, nil
}

// expiryNote is the summary's line for a table whose retention a restore
// applied.
func expiryNote(name string, e *commitTimeExpiry, expired int64) string {
	how := "the restored rows expire on the source's schedule"
	if !e.bands {
		how = "the restored rows are kept a retention period from now"
	}
	return fmt.Sprintf("table %s: %d row(s) committed at or before %s, which the table's retention had expired by the chain's end, left out; %s", name, expired, describeBackupTS(e.horizon), how)
}

// descriptorWithRestoredRows is a backed-up descriptor as a restore of
// everything writes it: verbatim, but for its RestoredRows, which are the
// bands this restore wrote (none, when it wrote the table's rows in one
// go). The bands a descriptor carried in the backup are the source
// cluster's timestamps, which mean nothing here.
func descriptorWithRestoredRows(raw json.RawMessage, bands []catalog.RestoredRowsBand) (json.RawMessage, error) {
	var top map[string]json.RawMessage
	if err := json.Unmarshal(raw, &top); err != nil {
		return nil, err
	}
	if _, had := top["restored_rows"]; !had && len(bands) == 0 {
		return raw, nil
	}
	delete(top, "restored_rows")
	if len(bands) > 0 {
		b, err := json.Marshal(bands)
		if err != nil {
			return nil, err
		}
		top["restored_rows"] = b
	}
	return json.Marshal(top)
}
