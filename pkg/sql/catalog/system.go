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

// systemTableIDs is every reserved name and the ID it is created at.
// Adding an entry here is the whole registration: the DDL path assigns
// the ID from it, and the privilege, rename, drop and backup rules read
// the membership.
var systemTableIDs = map[string]uint64{
	MetricsTableName: MetricsTableID,
	PrefsTableName:   PrefsTableID,
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
