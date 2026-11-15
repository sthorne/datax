// Package version defines the cluster protocol version and the
// compatibility rules that make rolling upgrades safe.
//
// The protocol version is a small integer, independent of the CLI's
// human-readable build string. A binary supports the window
// [MinSupported, Current]. A cluster persists one "cluster version"
// (keys.ClusterVersionKey), which only ever moves forward, and only via
// the operator-triggered "upgrade-cluster" admin op (finalize) once every
// live node runs a binary whose window contains the target. Nodes gate on
// it at join time, and a store remembers the last cluster version it
// observed (keys.StoreClusterVersionKey) so a binary downgrade past a
// finalized upgrade is refused at startup.
//
// Compatibility rules (enforced by golden decode tests across packages):
//
//  1. JSON payloads (persisted or on the wire) are additive-only: new
//     fields must be `omitempty` with safe zero-value semantics; fields
//     are never renamed, retyped, or reused.
//  2. Protobuf fields are add-only, with fresh field numbers; numbers are
//     never reused, and removed fields stay reserved.
//  3. Format bytes (raft command encoding, rowenc values) may only gain
//     new values behind a cluster-version gate; decoders accept every old
//     format forever.
//  4. A new RequestUnion member, admin op, or trigger may only be SENT
//     once the cluster version has reached the gate it was introduced
//     under. Receivers of unknown payloads must degrade to an error,
//     never crash or silently misapply.
//  5. Every wire/persisted type carries a golden decode test frozen at
//     its current encoding, so an accidental breaking change fails CI.
package version

import "fmt"

// Version is the cluster protocol version.
type Version int

const (
	// V1 is every cluster bootstrapped before versioning existed (a
	// missing cluster-version key reads as V1).
	V1 Version = 1
	// V2 introduces cluster versioning itself: the persisted cluster
	// version, join/restart gating, and version-advertising heartbeats.
	V2 Version = 2
	// V3 introduces reverse scans (ScanRequest.Reverse): a v2 node
	// ignores the field and runs a forward scan, so reverse scans are
	// sent only once the cluster has finalized v3 (rule 4).
	V3 Version = 3
	// V4 introduces ordered range-addressing repair
	// (kvpb.UpdateMetaRequest): a v3 leader of the meta range does not
	// know the request, so splits and merges keep repairing /meta with
	// blind writes until the cluster has finalized v4 (rule 4).
	V4 Version = 4
	// V5 introduces the datax_metrics system table (see
	// pkg/sql/catalog.MetricsTableName): once the cluster has finalized
	// v5 every node creates it if missing and records its metrics into it.
	// A v4 node knows nothing of the reservation and would treat the
	// table as an ordinary user table (droppable, backed up), so nothing
	// creates it before finalize (rule 4).
	V5 Version = 5
	// V6 introduces databases (pkg/sql/catalog/database.go): database
	// descriptors and a per-database table namespace. Before finalize
	// every node keeps creating tables in the flat namespace, which a v5
	// node reads; at finalize the upgrade migrates the flat entries under
	// the default database in one transaction (rule 4).
	V6 Version = 6
	// V7 introduces expression DEFAULTs, sequences, SERIAL and identity
	// columns (pkg/sql/catalog/sequence.go): a column descriptor may
	// carry a DefaultExpr that a v6 node cannot evaluate (it would insert
	// NULL), so the DDL that creates one is refused until finalize, when
	// every node runs a v7 binary (rule 4).
	V7 Version = 7
	// V8 introduces table constraints (pkg/sql/constraint.go): CHECK,
	// FOREIGN KEY and named UNIQUE constraints live in the table
	// descriptor, and a foreign key's parent records its referencing
	// tables. A v7 node would write rows without checking them and drop
	// a parent row without touching its children, so the DDL that
	// creates a constraint is refused until finalize (rule 4).
	V8 Version = 8
	// V9 introduces views (pkg/sql/view.go): a table descriptor may carry
	// a ViewQuery, and then describes no rows of its own. A v8 node would
	// read such a descriptor as an empty table and let DML target it, so
	// CREATE VIEW is refused until finalize (rule 4).
	V9 Version = 9
	// V10 introduces the INTERVAL and TIME column types (pkg/sql/types):
	// their row encodings (rowenc tags 12 and 13) are unknown to a v9
	// node, which would refuse every row of such a table, so a CREATE
	// TABLE, ADD COLUMN or ALTER COLUMN TYPE naming them is refused
	// until finalize (rule 4).
	V10 Version = 10
	// V11 introduces roles (pkg/sql/catalog/role.go): role descriptors
	// at /system/roles supersede the /system/users credential records
	// and the /system/admins markers, which the finalize migration
	// rewrites in one transaction. A v10 node authenticates from the
	// old layout only, so the statements that write role descriptors
	// (CREATE ROLE, GRANT role TO role, ownership, scoped grants) are
	// refused until finalize; CREATE USER and GRANT ADMIN keep writing
	// the old layout until then (rule 4).
	V11 Version = 11
	// V12 introduces coalesced raft heartbeats and range quiescence
	// (pkg/kvserver/scheduler.go, quiesce.go): a leader sends one
	// RaftEnvelope per peer node carrying every range's heartbeat, and a
	// quiescing leader's heartbeat carries a flag its followers act on. A
	// v11 node reads neither (it would drop the envelope and keep
	// expecting per-range heartbeats), so both stay off until finalize.
	V12 Version = 12
	// V13 introduces the split store (pkg/kvserver/raftengine.go): the
	// raft log and HardState move to their own engine and the
	// state-machine engine drops its write-ahead log, replaying the raft
	// log after a crash. A store migrates on its first start after the
	// cluster finalizes v13 and records that it did; a v12 binary does
	// not know the layout, so the store's own version gate refuses it
	// (rule 4: the migration is the one upgrade step that cannot roll
	// back).
	V13 Version = 13
	// V14 introduces two storage encodings. Pebble's columnar-block
	// sstable format (format major version 19, FormatColumnarBlocks;
	// issue #166): once the cluster finalizes v14, every node ratchets
	// both of its engines to that format online — no restart — and new
	// sstables (flushes, compactions) are written in it; a v13 binary
	// bundles a Pebble that does not know the format, so the store's own
	// version gate refuses it (rule 4: the ratchet cannot roll back;
	// Pebble never lowers a store's format). And the binary (protobuf)
	// encoding of intent metadata and transaction records (issue #141):
	// a coordinator at v14 flags its transactions (TxnMeta.BinaryMeta),
	// every replica encodes what that transaction writes in protobuf,
	// and readers tell the two encodings apart by the first byte, so the
	// JSON records of earlier transactions stay readable for as long as
	// they live.
	V14 Version = 14
	// V15 introduces prefix bloom filters (issue #161, pkg/storage
	// comparer.go): a store at v15 opens with the MVCC comparer, whose
	// Split lets Pebble's bloom filters answer point reads by user key,
	// writes its filters and columnar key schema under names that carry
	// the change, and reads the tables from before as they are. The
	// comparer is fixed at open, so a running node switches at its next
	// restart; a v14 binary does not know the new key schema, so the
	// store's version gate refuses it (rule 4).
	V15 Version = 15
	// V16 introduces per-database replica placement (issue #176): a
	// database descriptor may carry a PlacementPolicy — a replica count
	// and a set of locality constraints — and the allocator honours it
	// when it up-replicates, repairs, rebalances and decommissions the
	// ranges of that database's tables. A v15 node reads the descriptor
	// but not the policy field, so it would allocate replicas anywhere
	// and undo the placement a v16 node just made; the DDL that writes a
	// policy is therefore refused until finalize (rule 4). The field
	// itself is omitempty, so a descriptor written before v16 reads the
	// same at either version.
	V16 Version = 16
	// V17 introduces the datax_ui_prefs system table (see
	// pkg/sql/catalog.PrefsTableName): the console's per-viewer display
	// choices — theme, timestamp style — belong to the cluster rather
	// than to one browser, so they are stored in the cluster and follow
	// a user to any browser and any node. Like the metrics table, a v16
	// node knows nothing of the reservation and would treat it as an
	// ordinary user table (droppable, and writable by its owner), so
	// nothing creates it and /api/prefs persists nothing before
	// finalize (rule 4).
	V17 Version = 17
	// V18 introduces the authenticated encryption-at-rest file format
	// (issue #220, pkg/storage/enc "DXE2"): file content sealed in
	// chunks with AES-256-GCM, so an altered, moved or truncated byte is
	// refused at read rather than served. A store at v18 writes its new
	// files that way from its next open and, once none of the old format
	// remains, refuses the old format; a v17 binary cannot read the new
	// one, so a store that has written any is refused by the version
	// gate (rule 4). The cluster version is what guarantees every node
	// can read the format before any node writes it.
	V18 Version = 18
	// V19 introduces two statement-shape system tables (issue #209,
	// pkg/sql/catalog.ShapesMetricsTableName and
	// StatementShapesTableName): every node records its busiest
	// statement shapes into a time series with a day's retention, and
	// the fingerprint-to-shape map beside it, so "was this slow
	// yesterday" is a chart rather than a guess. Like the metrics and
	// preferences tables, a v18 node knows nothing of the reservations
	// and would treat them as ordinary user tables, so nothing creates
	// them before finalize (rule 4).
	V19 Version = 19
	// V20 introduces sticky range boundaries (issues #265, #290): a
	// range descriptor may record that its start boundary was set on
	// purpose (kvpb.RangeDescriptor.StickyUntil), which the merge queue
	// honours, and two things travel to carry it — a descriptor-update
	// trigger in the raft log and kvpb.AdminUnsplitRequest. A v19 replica
	// drops the field from a split trigger and ignores the new trigger
	// outright, so its copy of the descriptor would differ from a v20
	// replica's; a v19 leader would merge the range back. Until finalize,
	// therefore, a manual split is an ordinary one and UNSPLIT is refused
	// (rule 4). The field itself is omitempty, so a descriptor written
	// before v20 reads the same at either version.
	V20 Version = 20
	// V21 introduces the datax_audit system table (issue #293, see
	// pkg/sql/catalog.AuditTableName): every node's hash-chained audit
	// records, readable over SQL and writable by nobody. A v20 node
	// knows nothing of the reservation and would treat the table as an
	// ordinary one — its owner could delete from it or drop it, which
	// is exactly what the table exists to rule out — so nothing creates
	// it and audit records stay in the log alone before finalize
	// (rule 4).
	//
	// V21 also introduces three table options and a purge (issues #295, #298):
	// a per-table GC window (TableDescriptor.GCTTLSeconds), row expiry
	// (TableDescriptor.RowTTL) and the purge admin ops. A v20 node reads
	// the descriptor without the fields, so as a range's leader it would
	// collect a table's history at the cluster's window rather than the
	// table's, as the range-1 leader it would expire nothing, and as a
	// gateway it would drop both fields from the next descriptor it
	// rewrote; it answers a purge step with "unknown admin op". The DDL
	// that writes either field and the purge are therefore refused until
	// finalize (rule 4). The fields are omitempty, so a descriptor
	// written before v21 reads the same at either version.
	//
	// V21 also introduces inverted indexes (issue #300, pkg/sql/inverted): an
	// index descriptor may name an inverted kind
	// (catalog.IndexDescriptor.Inverted), and such an index holds one
	// entry per element of the indexed value rather than one per row. The
	// field is omitempty, so every descriptor written before reads the
	// same; but a v20 node drops the field it does not know and would
	// maintain and read the index as an ordinary one over a column type
	// it cannot key. CREATE INDEX ... USING GIN is therefore refused
	// until finalize (rule 4).
	//
	// V21 also introduces full-text search (issue #299, pkg/sql/tsearch):
	// the column types TSVECTOR and TSQUERY (two new types.Family values
	// and two new row value tags) and three more inverted kinds — one over
	// a tsvector column, and one per text-search configuration over the
	// lexemes of a text column. No descriptor field is new: the types are
	// new values of a column's type and the kinds new values of
	// IndexDescriptor.Inverted. A v20 node cannot decode a row holding
	// either type and knows none of the kinds, so a statement that would
	// give a table a column of either type is refused until finalize, as
	// CREATE INDEX ... USING GIN already is (rule 4). The functions and
	// the @@ operator write nothing and need no gate.
	//
	// V21 also introduces two things backups and restores rely on (issues
	// #301, #302). Exports of every revision
	// (kvpb.ExportRequest.AllRevisions): a v20 replica does not read the
	// flag and answers with the newest version of each key, which a
	// backup would then record as the whole history of its window, so a
	// backup with revision history is refused until finalize. And the
	// progress record of a table restore (keys.RestoreProgressKey), which
	// every node's janitor reads to reclaim the rows of a restore whose
	// node died: a v20 node knows nothing of the record and would leave
	// them, so restoring a table into a running cluster is refused until
	// finalize too (rule 4).
	//
	// V21 also introduces row-level security (issue #294): a table
	// descriptor may carry RowSecurity, ForceRowSecurity and Policies, and
	// a role descriptor BypassRLS. The fields are omitempty, so every
	// descriptor written before reads the same; but a v20 gateway drops
	// the fields it does not know and would serve every row of a
	// protected table to a role its policies confine, and would write the
	// descriptor back without them. ENABLE and FORCE ROW LEVEL SECURITY,
	// CREATE and ALTER POLICY and the BYPASSRLS role attribute are
	// therefore refused until finalize (rule 4).
	//
	// V21 also introduces large objects (issue #303, pkg/sql/largeobject.go):
	// three more system tables (pkg/sql/catalog.LargeObjectTableName and
	// its two companions) holding objects cut into pages, behind
	// PostgreSQL's lo_* functions. Nothing below SQL changes — a page is
	// an ordinary row — so the gate is the one every system table has: a
	// v20 node knows nothing of the reservations and would treat the
	// tables as ordinary ones, whose owner may delete rows, which here are
	// pages of somebody's object. Nothing creates the tables and the
	// functions are refused until finalize (rule 4).
	//
	// V21 also introduces the VECTOR column type and its index (issue #304,
	// pkg/sql/vector). A column of the new type family has a row-value
	// tag of its own, which a v20 node meets as a corrupt row; and an
	// index descriptor may describe a vector index
	// (catalog.IndexDescriptor.Vector), whose entries are keyed by the
	// list a row's vector is filed under and whose centres live beside
	// the table's indexes (keys.VectorCentroidPrefix). A v20 node drops
	// the field and would maintain the index as an ordinary one over a
	// type it cannot key. A VECTOR column and CREATE INDEX ... USING
	// ivfflat are therefore refused until finalize (rule 4). The field is
	// omitempty and the family is appended, so every descriptor written
	// before reads the same.
	V21 Version = 21
	// V22 introduces the cluster key and the datax_keys system table
	// (issue #310, docs/design-key-service.md; pkg/keystore and
	// pkg/server/clusterkey.go). The cluster key is one 32-byte key per
	// cluster, versioned, kept by every node wrapped under its own root
	// (--enc-root); its replicated record (keys.ClusterKeyMarkerKey)
	// says which version is current and how to check a copy. datax_keys
	// (pkg/sql/catalog.KeysTableName) holds named keys wrapped by it. A
	// v21 node knows nothing of either: it would hand nothing over to a
	// joining node, write no copy of its own, and treat datax_keys as an
	// ordinary table whose owner may read or delete its rows. So
	// `datax debug cluster-key init` is refused, nothing creates the
	// table, and no node writes the record or its holder report until
	// finalize (rule 4).
	//
	// V22 also introduces the reverse record of view dependencies (issue
	// #313, keys.ViewDependencyKey): under each relation, the views whose
	// query reads it, written in the transactions that create, replace
	// and drop a view, so DROP, RENAME and DROP COLUMN find a relation's
	// views without reading every descriptor. A v21 node creates views
	// without the records, so they are not trusted until finalize: at
	// v22 the node leading range 1 records every view once and marks the
	// records complete (keys.ViewDependencyIndexKey), and until then the
	// statements read every descriptor as before. The records are system
	// keys a v21 node never reads.
	//
	// V22 also introduces the key service's statements and functions
	// (CREATE / ALTER / DROP ENCRYPTION KEY, GRANT ... ON ENCRYPTION KEY,
	// datax_key_wrap and the rest; pkg/sql/keyservice.go) and the owner
	// and privileges columns of datax_keys, which a v21 node would not
	// enforce: they are refused until finalize as well.
	//
	// V22 also introduces sealed tables (issue #310, step 3;
	// pkg/sql/rowseal): the descriptor field encryption_key, omitempty,
	// and row values a v21 node could not decode, so CREATE TABLE ...
	// WITH (encryption_key) is refused until finalize too.
	//
	// V22 also introduces per-tenant keys (issue #310, step 4): the
	// descriptor field encryption_key.tenant_column, omitempty, keys of
	// kind tenant in datax_keys, and a sealed form naming its key; CREATE
	// TABLE ... WITH (encryption_key_by) and datax_destroy_tenant_key are
	// refused until finalize as well.
	//
	// V22 also introduces sealing an existing table and online key
	// rotation (issue #310, step 5): the descriptor field
	// encryption_key.sealing, omitempty; the seal job's records
	// (keys.SealJobKey), system keys a v21 node never reads; and
	// ALTER TABLE ... SET (encryption_key | encryption_key_by) on a table
	// with rows, ALTER TABLE ... ROTATE ENCRYPTION KEY,
	// datax_rotate_tenant_key, datax_destroy_old_key_versions and
	// datax_remove_tenant_rows, all refused until finalize, and the job
	// does not run before it.
	//
	// V22 also makes the metrics recorder write the per-table and
	// per-index series only when they change, with a keyframe every 60
	// ticks, a NULL row when a series ends and a recorder.tick marker
	// per tick (issue #316, pkg/server/metrics_sparse.go). A v21 node's
	// /api/metrics would chart the unwritten ticks as gaps, so the
	// recorder writes every series every tick until finalize.
	//
	// V22 also starts the one-time sweep for tables dropped before DROP
	// TABLE left a record (issue #316, pkg/server/dropped_sweep.go): an
	// id that holds rows and names nothing is recorded as dropped, for
	// the reclaim job. Only at v22 can no binary that drops tables
	// without records be running, so only then does "no record" mean
	// "dropped before records"; the sweep's own record
	// (keys.DroppedTableSweepKey) is a system key a v21 node never reads.
	V22 Version = 22

	// Current is the newest cluster version this binary can run.
	Current = V22
	// MinSupported is the oldest cluster version this binary can join.
	// The support window is adjacent versions only: operators upgrade
	// one major version at a time.
	MinSupported = V2
)

// Supported reports whether this binary can participate in a cluster at
// version cv.
func Supported(cv Version) bool {
	return cv >= MinSupported && cv <= Current
}

func (v Version) String() string { return fmt.Sprintf("v%d", int(v)) }
