package sql

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/kvclient"
	"github.com/sthorne/datax/pkg/sql/catalog"
	"github.com/sthorne/datax/pkg/sql/parser"
	"github.com/sthorne/datax/pkg/sql/types"
	"github.com/sthorne/datax/pkg/util/log"
	"github.com/sthorne/datax/pkg/version"
)

// Table options that any table takes (issues #295, #298), beside the
// time-series ones applyTableOptions has always handled:
//
//	gc_ttl = '<interval>'            the table's own GC window
//	ttl_expiration = <column>        a row expires at the time the column names
//	ttl_column = <column>, ttl = '<interval>'
//	                                 a row expires that long after the column's time
//
// Both live in the table descriptor and both are read by the node, not by
// the statements that touch the table: the retention provider resolves a
// range to its table's GC window (pkg/server/retention.go), and the row
// expiry job deletes expired rows with ordinary DELETEs
// (pkg/server/rowttl.go). An expired row therefore stays visible until the
// job reaches it.

// requireV21 gates the statements that write v21 descriptor fields or ask
// for a purge.
func (s *Session) requireV21(what string) error {
	if s.db.ClusterVersion() < version.V21 {
		return newErrf(CodeFeatureNotSupported, "%s needs cluster version %s: finalize the upgrade with `datax debug upgrade` first", what, version.V21)
	}
	return nil
}

// generalTableOption reports whether name is an option any table takes.
func generalTableOption(name string) bool {
	switch name {
	case "gc_ttl", "ttl", "ttl_column", "ttl_expiration":
		return true
	}
	return false
}

// MinGCTTL is the shortest per-table GC window accepted. Below the
// window, a transaction or an AS OF SYSTEM TIME read is refused, so a
// window shorter than a statement takes would refuse ordinary work.
const MinGCTTL = time.Second

// applyGeneralOptions validates the general options in options and
// stamps them onto desc. alter says the table already exists, which is
// when one half of a ttl_column/ttl pair may be given alone to change
// that half. It reports whether the row expiry is new or names a
// different column, so the caller knows an index on it is wanted.
func applyGeneralOptions(desc *catalog.TableDescriptor, options map[string]string, alter bool) (ttlColumnChanged bool, err error) {
	if v, ok := options["gc_ttl"]; ok {
		secs, perr := parseRetention(v)
		if perr != nil {
			return false, newErrf(CodeSyntaxError, "gc_ttl: %v", perr)
		}
		if time.Duration(secs)*time.Second < MinGCTTL {
			return false, newErrf(CodeInvalidParameter, "gc_ttl must be at least %s", MinGCTTL)
		}
		if desc.Timeseries && desc.RetentionSeconds > 0 {
			// Retention already is this table's window: its ranges
			// collect everything older, the newest version included.
			return false, newErrf(CodeFeatureNotSupported, "table %q has a retention, which is its GC window; gc_ttl applies to tables without one", desc.Name)
		}
		desc.GCTTLSeconds = secs
	}
	_, hasTTL := options["ttl"]
	_, hasCol := options["ttl_column"]
	_, hasExp := options["ttl_expiration"]
	if !hasTTL && !hasCol && !hasExp {
		return false, nil
	}
	if desc.Timeseries {
		return false, newErrf(CodeFeatureNotSupported, "table %q is a time-series table: its rows expire by retention", desc.Name)
	}
	if hasExp && (hasTTL || hasCol) {
		return false, newErrf(CodeSyntaxError, "ttl_expiration names the expiry time itself; it cannot be combined with ttl or ttl_column")
	}
	next := catalog.RowTTL{}
	if desc.RowTTL != nil && !hasExp && desc.RowTTL.AfterSeconds > 0 && alter {
		next = *desc.RowTTL // changing one half of an existing pair
	}
	colName := options["ttl_expiration"]
	if hasCol {
		colName = options["ttl_column"]
	}
	if colName != "" {
		col, ok := desc.Col(colName)
		if !ok || col.Hidden {
			return false, newErrf(CodeUndefinedColumn, "column %q does not exist", colName)
		}
		if col.Type != types.Timestamp {
			return false, newErrf(CodeInvalidParameter, "row expiry needs a TIMESTAMPTZ or TIMESTAMP column; %q is %s", colName, col.Type)
		}
		next.ColumnID = col.ID
	}
	if hasTTL {
		secs, perr := parseRetention(options["ttl"])
		if perr != nil {
			return false, newErrf(CodeSyntaxError, "ttl: %v", perr)
		}
		next.AfterSeconds = secs
	}
	if hasExp {
		next.AfterSeconds = 0
	}
	if next.ColumnID == 0 {
		// A lifetime counted from the row's own write would need the
		// write time in a column the planner can index; there is none,
		// so the table names one.
		return false, newErrf(CodeFeatureNotSupported, "ttl needs the column the lifetime is counted from: WITH (ttl_column = <timestamp column>, ttl = '<interval>'), e.g. a created_at TIMESTAMPTZ DEFAULT now()")
	}
	if !hasExp && next.AfterSeconds <= 0 {
		return false, newErrf(CodeSyntaxError, "ttl_column needs ttl = '<interval>' (or use ttl_expiration for a column that names the expiry time itself)")
	}
	ttlColumnChanged = desc.RowTTL == nil || desc.RowTTL.ColumnID != next.ColumnID
	desc.RowTTL = &next
	return ttlColumnChanged, nil
}

// ttlIndexName is the index a row expiry creates on its column when the
// table has none leading with it.
func ttlIndexName(table string) string { return table + "_ttl_idx" }

// hasIndexLeadingWith reports whether desc has an index, public or still
// being built, whose first column is col.
func hasIndexLeadingWith(desc *catalog.TableDescriptor, col catalog.ColumnID) bool {
	if len(desc.PrimaryKey) > 0 && desc.PrimaryKey[0] == col {
		return true
	}
	for _, idx := range desc.Indexes {
		if len(idx.ColumnIDs) > 0 && idx.ColumnIDs[0] == col {
			return true
		}
	}
	return false
}

// addTTLIndex gives a table being created the index its row expiry
// scans: the job finds expired rows by a range scan of the expiry
// column, never by reading the table.
func addTTLIndex(desc *catalog.TableDescriptor) {
	if desc.RowTTL == nil || hasIndexLeadingWith(desc, desc.RowTTL.ColumnID) {
		return
	}
	if desc.NextIndexID < 2 {
		desc.NextIndexID = 2
	}
	desc.Indexes = append(desc.Indexes, catalog.IndexDescriptor{
		ID: desc.NextIndexID, Name: ttlIndexName(desc.Name),
		ColumnIDs: []catalog.ColumnID{desc.RowTTL.ColumnID}, State: catalog.IndexStatePublic,
	})
	desc.NextIndexID++
}

// execSetGeneralOptions is ALTER TABLE ... SET (gc_ttl | ttl | ttl_column
// | ttl_expiration = ...), on a table that may hold data. The index a row
// expiry wants is built first, online, so the option is never in force
// without it; then one descriptor write sets the options. A new GC
// window also asks for range boundaries at the table's edges: a window
// governs only the ranges wholly inside its table.
func (s *Session) execSetGeneralOptions(ctx context.Context, table string, options map[string]string) *Error {
	if err := s.requireV21("the gc_ttl and row expiry options"); err != nil {
		return ToSQLError(err)
	}
	if _, bare := catalog.SplitTableName(table); catalog.IsSystemTable(bare) && !s.system {
		return newErrf(CodeInsufficientPriv, "table %q belongs to the cluster: only ALTER TABLE ... SET (retention | shards) is allowed", table)
	}
	// Validate against the current descriptor before building anything.
	var wantIndexOn string
	if err := s.db.RunTxn(ctx, "set-table-options-check", func(ctx context.Context, txn *kvclient.Txn) error {
		wantIndexOn = ""
		shared, err := s.lookup(ctx, txn, table)
		if err != nil {
			return err
		}
		if err := mustBeReal(shared); err != nil {
			return err
		}
		desc := shared.Clone()
		if _, err := applyGeneralOptions(desc, options, true); err != nil {
			return err
		}
		if desc.RowTTL != nil && !hasIndexLeadingWith(desc, desc.RowTTL.ColumnID) {
			col, _ := desc.ColByID(desc.RowTTL.ColumnID)
			wantIndexOn = col.Name
		}
		return nil
	}); err != nil {
		return ToSQLError(err)
	}
	if wantIndexOn != "" {
		_, bare := catalog.SplitTableName(table)
		if _, serr := s.execCreateIndexOnline(ctx, &parser.CreateIndex{
			Name: ttlIndexName(bare), Table: table, Columns: []string{wantIndexOn}, IfNotExists: true,
		}); serr != nil {
			return serr
		}
	}
	var desc *catalog.TableDescriptor
	if err := s.db.RunTxn(ctx, "set-table-options", func(ctx context.Context, txn *kvclient.Txn) error {
		shared, err := s.lookup(ctx, txn, table)
		if err != nil {
			return err
		}
		desc = shared.Clone()
		if _, err := applyGeneralOptions(desc, options, true); err != nil {
			return err
		}
		return s.cat.Update(ctx, txn, desc)
	}); err != nil {
		return ToSQLError(err)
	}
	if _, ok := options["gc_ttl"]; ok {
		s.splitAtTableEdges(ctx, desc)
	}
	return nil
}

// splitAtTableEdges asks for range boundaries at both ends of a table's
// key span, as a time-series table gets at creation. A GC window (and a
// purge) acts on whole ranges, so a range the table shares with a
// neighbour keeps the longer window of the two; with the edges split,
// the table's ranges are its own, and the merge queue leaves them so
// while their window differs from the neighbour's. Best-effort and not
// sticky: the boundary is held by that rule, not by a mark nobody could
// name to release, and a split that fails here is asked for again by the
// node's GC pass when it finds the range straddling the edge.
func (s *Session) splitAtTableEdges(ctx context.Context, desc *catalog.TableDescriptor) {
	if desc == nil || desc.ID == 0 {
		return
	}
	start, end := keys.TableDataSpan(desc.ID)
	for _, k := range []keys.Key{start, end} {
		if _, err := s.db.AdminSplit(ctx, k); err != nil {
			log.Debugf("table-edge split at %s: %v", k, err)
		}
	}
}

// execResetOptions is ALTER TABLE ... RESET (name, ...): gc_ttl returns
// to the node's window, the row expiry options (any of the three names
// clears the whole expiry) stop the table expiring rows, and retention
// stops a time-series table expiring them. The index a row expiry
// created stays: it is an ordinary index and may be in use.
func (s *Session) execResetOptions(ctx context.Context, t *parser.AlterTable) (*Result, *Error) {
	for _, name := range t.ResetOptions {
		switch name {
		case "gc_ttl", "ttl", "ttl_column", "ttl_expiration", "retention":
		default:
			return nil, newErrf(CodeSyntaxError, "ALTER TABLE ... RESET takes gc_ttl, ttl, ttl_column, ttl_expiration or retention, not %q", name)
		}
	}
	if _, bare := catalog.SplitTableName(t.Table); catalog.IsSystemTable(bare) && !s.system {
		return nil, newErrf(CodeInsufficientPriv, "table %q belongs to the cluster: only ALTER TABLE ... SET (retention | shards) is allowed", t.Table)
	}
	err := s.db.RunTxn(ctx, "reset-table-options", func(ctx context.Context, txn *kvclient.Txn) error {
		shared, err := s.lookup(ctx, txn, t.Table)
		if err != nil {
			return err
		}
		if err := mustBeReal(shared); err != nil {
			return err
		}
		desc := shared.Clone()
		for _, name := range t.ResetOptions {
			switch name {
			case "gc_ttl":
				desc.GCTTLSeconds = 0
			case "retention":
				desc.RetentionSeconds = 0
			default:
				desc.RowTTL = nil
			}
		}
		return s.cat.Update(ctx, txn, desc)
	})
	if err != nil {
		return nil, ToSQLError(err)
	}
	return &Result{Tag: "ALTER TABLE"}, nil
}

// execPurge is ALTER TABLE t PURGE (issue #295): force garbage collection
// over the table's whole key span — primary rows and every index — at a
// threshold of now, compact the span on every replica, and return what
// each replica then holds. One row per replica; the tag says whether the
// purge is complete. The work is the node's (server.Node.PurgeSpan); the
// statement names the span and renders the report.
func (s *Session) execPurge(ctx context.Context, t *parser.AlterTable) (*Result, *Error) {
	if err := s.requireV21("ALTER TABLE ... PURGE"); err != nil {
		return nil, ToSQLError(err)
	}
	if _, bare := catalog.SplitTableName(t.Table); catalog.IsSystemTable(bare) && !s.system {
		return nil, newErrf(CodeInsufficientPriv, "table %q belongs to the cluster and cannot be purged", t.Table)
	}
	var desc *catalog.TableDescriptor
	if err := s.db.RunTxn(ctx, "purge-lookup", func(ctx context.Context, txn *kvclient.Txn) error {
		shared, err := s.lookup(ctx, txn, t.Table)
		if err != nil {
			return err
		}
		desc = shared
		return mustBeReal(shared)
	}); err != nil {
		return nil, ToSQLError(err)
	}
	start, end := keys.TableDataSpan(desc.ID)
	rep, err := s.db.Purge(ctx, start, end)
	if err != nil {
		return nil, ToSQLError(err)
	}
	res := &Result{Columns: []ResultColumn{
		{Name: "range_id", Type: types.Int}, {Name: "node_id", Type: types.Int},
		{Name: "live_keys", Type: types.Int}, {Name: "garbage_versions", Type: types.Int},
		{Name: "tombstones", Type: types.Int}, {Name: "intents", Type: types.Int},
		{Name: "compacted", Type: types.Bool}, {Name: "status", Type: types.String},
	}}
	sort.Slice(rep.Replicas, func(i, j int) bool {
		a, b := rep.Replicas[i], rep.Replicas[j]
		if a.RangeID != b.RangeID {
			return a.RangeID < b.RangeID
		}
		return a.NodeID < b.NodeID
	})
	for _, r := range rep.Replicas {
		status := "clean"
		switch {
		case r.Error != "":
			status = "unconfirmed: " + r.Error
		case r.Garbage+r.Tombstones+r.Intents > 0:
			status = "not clean"
		case !r.Compacted:
			status = "collected, not compacted"
		}
		res.Rows = append(res.Rows, []types.Datum{
			types.NewInt(int64(r.RangeID)), types.NewInt(int64(r.NodeID)),
			types.NewInt(r.LiveKeys), types.NewInt(r.Garbage), types.NewInt(r.Tombstones), types.NewInt(r.Intents),
			types.NewBool(r.Compacted), types.NewString(status),
		})
	}
	if rep.Complete {
		res.Tag = fmt.Sprintf("PURGE %d", rep.VersionsRemoved)
	} else {
		// The statement did what it could reach; the rows say what it
		// could not, and the tag must not read as an attestation.
		res.Tag = "PURGE INCOMPLETE"
	}
	return res, nil
}
