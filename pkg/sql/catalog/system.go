package catalog

// The cluster keeps its own metrics in a system-owned time-series table
// (issue #115). It is created by the nodes themselves, at a reserved
// descriptor ID far above anything the ID generator hands out, so a
// restore into a cluster that has already created it never collides with
// the backed-up user tables' IDs.

// MetricsTableName is the reserved name of the metrics table.
const MetricsTableName = "datax_metrics"

// MetricsTableID is the metrics table's fixed descriptor ID.
const MetricsTableID uint64 = 1 << 40

// IsSystemTable reports whether name is reserved for the cluster.
func IsSystemTable(name string) bool { return name == MetricsTableName }

// IsSystemTableID reports whether id belongs to a system table.
func IsSystemTableID(id uint64) bool { return id == MetricsTableID }
