package catalog

// The cluster keeps some of its own state in system-owned tables. They
// are created by the nodes themselves, at reserved descriptor IDs far
// above anything the ID generator hands out, so a restore into a cluster
// that has already created them never collides with the backed-up user
// tables' IDs.
//
//   - datax_metrics (issue #115) is the metrics time series.
//   - datax_ui_prefs (issue #204) is the console's per-viewer display
//     choices. A console preference belongs to the person, not to the
//     browser they happen to be sitting at, so the cluster holds it and
//     it follows them to any browser and any node.
//   - datax_metrics_shapes (issue #209) is the statement-shape time
//     series: each node's busiest shapes by total time, recorded every
//     interval, with a shorter retention than datax_metrics because
//     the rows are numerous and go stale fast.
//   - datax_statement_shapes (issue #209) maps a fingerprint to the
//     normalised shape it stands for — literals replaced, so it carries
//     no data — which is what lets a chart of a fingerprint be read.

// MetricsTableName is the reserved name of the metrics table.
const MetricsTableName = "datax_metrics"

// PrefsTableName is the reserved name of the console preferences table.
const PrefsTableName = "datax_ui_prefs"

// MetricsTableID is the metrics table's fixed descriptor ID.
const MetricsTableID uint64 = 1 << 40

// PrefsTableID is the preferences table's fixed descriptor ID. Reserved
// IDs are assigned by hand and never reused; a new one takes the next
// value above the last, so no two system tables can collide.
const PrefsTableID uint64 = 1<<40 + 1

// ShapesMetricsTableName is the reserved name of the statement-shape
// time series; StatementShapesTableName that of the fingerprint table,
// which holds each shape's normalised text: no literal value, every
// identifier (issue #209). Both are regenerable history, left out of
// backups unless asked, like MetricsTableName.
const (
	ShapesMetricsTableName   = "datax_metrics_shapes"
	StatementShapesTableName = "datax_statement_shapes"
)

// ShapesMetricsTableID and StatementShapesTableID are the two shape
// tables' fixed descriptor IDs, the next above PrefsTableID.
const (
	ShapesMetricsTableID   uint64 = 1<<40 + 2
	StatementShapesTableID uint64 = 1<<40 + 3
)

// systemTableIDs is every reserved name and the ID it is created at.
// Adding an entry here is the whole registration: the DDL path assigns
// the ID from it, and the privilege, rename, drop and backup rules read
// the membership.
var systemTableIDs = map[string]uint64{
	MetricsTableName:         MetricsTableID,
	PrefsTableName:           PrefsTableID,
	ShapesMetricsTableName:   ShapesMetricsTableID,
	StatementShapesTableName: StatementShapesTableID,
}

// SystemTableName is the reserved name whose table is created at id, or
// "". A reserved table exists as a span before it exists as a
// descriptor — the preferences table is created on the first write to
// it — and a key in that span is named from here until then.
func SystemTableName(id uint64) string {
	for name, tid := range systemTableIDs {
		if tid == id {
			return name
		}
	}
	return ""
}

// IsSystemTable reports whether name is reserved for the cluster.
func IsSystemTable(name string) bool {
	_, ok := systemTableIDs[name]
	return ok
}

// SystemTableID returns the reserved descriptor ID for a system table.
// The second result reports whether name is reserved at all; callers
// that have already checked with IsSystemTable may ignore it, but they
// must not assume any particular table's ID — that was the bug when a
// second system table joined the first (issue #204).
func SystemTableID(name string) (uint64, bool) {
	id, ok := systemTableIDs[name]
	return id, ok
}

// IsSystemTableID reports whether id belongs to a system table. It reads
// the same map the names do — listing the IDs again here would be the
// registration silently forgetting a table, which is the bug the DDL
// path had until #204.
func IsSystemTableID(id uint64) bool {
	for _, known := range systemTableIDs {
		if id == known {
			return true
		}
	}
	return false
}
