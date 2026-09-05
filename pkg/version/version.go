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

	// Current is the newest cluster version this binary can run.
	Current = V11
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
