// Package keys defines the datax keyspace layout.
//
// The keyspace has two worlds:
//
//   - Global (addressable) keys: routed to ranges by [StartKey, EndKey)
//     bounds, stored with MVCC versioning. Prefixes: /meta (0x02),
//     /system (0x03), /t table data (0x04).
//
//   - Local keys (prefix 0x01): never routed, never scanned by MVCC.
//     Two flavors:
//     0x01 'u' — store/replica-local, unreplicated (store ident, Raft
//     HardState, log, applied index, descriptor copy);
//     0x01 'k' — range-local *addressed* keys, replicated: keys that
//     belong to the range covering an embedded global key but
//     must stay invisible to user scans (transaction records).
//
// Ordering is plain bytes.Compare everywhere.
package keys

import (
	"bytes"
	"fmt"

	"github.com/google/uuid"
	"github.com/sthorne/datax/pkg/base"
	"github.com/sthorne/datax/pkg/util/encoding"
)

// Key is a raw key. Global keys are compared and routed directly.
type Key []byte

func (k Key) Equal(o Key) bool     { return bytes.Equal(k, o) }
func (k Key) Compare(o Key) int    { return bytes.Compare(k, o) }
func (k Key) Less(o Key) bool      { return bytes.Compare(k, o) < 0 }
func (k Key) Clone() Key           { return append(Key(nil), k...) }
func (k Key) HasPrefix(p Key) bool { return bytes.HasPrefix(k, p) }
func (k Key) String() string       { return Pretty(k) }
func (k Key) Next() Key            { return append(k.Clone(), 0) }

// PrefixEnd returns the smallest key greater than every key with prefix k:
// the last byte is incremented (with carry). Used for prefix scans.
func (k Key) PrefixEnd() Key {
	end := k.Clone()
	for i := len(end) - 1; i >= 0; i-- {
		if end[i] != 0xff {
			end[i]++
			return end[:i+1]
		}
	}
	return Key(MaxKey).Clone()
}

const (
	localPrefixByte  byte = 0x01
	metaPrefixByte   byte = 0x02
	systemPrefixByte byte = 0x03
	tablePrefixByte  byte = 0x04

	localUnreplicatedByte byte = 'u'
	localAddressedByte    byte = 'k'
)

var (
	// MinKey / MaxKey bound the global (addressable) keyspace. Range 1
	// initially spans [MinKey, MaxKey).
	MinKey = Key{metaPrefixByte}
	MaxKey = Key{0xff}

	LocalPrefix  = Key{localPrefixByte}
	MetaPrefix   = Key{metaPrefixByte}
	SystemPrefix = Key{systemPrefixByte}
	TablePrefix  = Key{tablePrefixByte}

	localUnreplicatedPrefix = Key{localPrefixByte, localUnreplicatedByte}
	localAddressedPrefix    = Key{localPrefixByte, localAddressedByte}
)

// IsLocal reports whether k is a local (non-addressable) key.
func IsLocal(k Key) bool {
	return len(k) > 0 && k[0] == localPrefixByte
}

// ---------------------------------------------------------------------------
// Store-local, unreplicated keys.

// StoreIdentKey holds the JSON store ident (cluster UUID, node ID, store ID).
func StoreIdentKey() Key {
	return append(localUnreplicatedPrefix.Clone(), []byte("store-ident")...)
}

// StoreRegistryKey holds this store's last known node registry (JSON), so a
// restarted node can reach its peers before any range has a leader.
func StoreRegistryKey() Key {
	return append(localUnreplicatedPrefix.Clone(), []byte("store-registry")...)
}

// StoreClusterVersionKey mirrors the last replicated cluster version this
// store observed (decimal string), refreshed by the heartbeat loop. Read at
// startup — before quorum — to refuse a binary downgrade past a finalized
// upgrade.
func StoreClusterVersionKey() Key {
	return append(localUnreplicatedPrefix.Clone(), []byte("store-cluster-version")...)
}

// StoreRaftEngineKey marks a store whose raft state lives on its own
// engine (issue #105): set when the store migrates, never cleared. A
// binary that does not know the key cannot open such a store correctly,
// which the cluster-version gate prevents (the store's cluster version is
// raised to v13 with the marker).
func StoreRaftEngineKey() Key {
	return append(localUnreplicatedPrefix.Clone(), []byte("store-raft-engine")...)
}

// ---------------------------------------------------------------------------
// Replica-local, unreplicated Raft state, per range.

func makeRangeLocalKey(rangeID base.RangeID, suffix string) Key {
	k := append(localUnreplicatedPrefix.Clone(), 'r')
	k = Key(encoding.EncodeUint64(k, uint64(rangeID)))
	return append(k, suffix...)
}

// RaftHardStateKey stores raftpb.HardState.
func RaftHardStateKey(rangeID base.RangeID) Key { return makeRangeLocalKey(rangeID, "hs") }

// RaftLogKey stores the Raft log entry at index.
func RaftLogKey(rangeID base.RangeID, index uint64) Key {
	k := makeRangeLocalKey(rangeID, "log")
	return Key(encoding.EncodeUint64(k, index))
}

// RaftLogPrefix is the prefix of all log entries for a range.
func RaftLogPrefix(rangeID base.RangeID) Key { return makeRangeLocalKey(rangeID, "log") }

// RaftAppliedStateKey stores the applied index (and later, stats), written
// atomically with every application batch.
func RaftAppliedStateKey(rangeID base.RangeID) Key { return makeRangeLocalKey(rangeID, "as") }

// RaftTruncatedStateKey stores the raft log's truncation point (index and
// term) on a split store's raft engine, written atomically with the
// deletion of the entries at or below it (issue #105). On a single-engine
// store the truncated state lives in the applied state instead.
func RaftTruncatedStateKey(rangeID base.RangeID) Key { return makeRangeLocalKey(rangeID, "ts") }

// IsRaftEngineKey reports whether a replica-local key belongs on the raft
// engine of a split store: the HardState, the log entries and the
// truncated state. Everything else under the range-local prefix (the
// descriptor copy, the applied state, the tombstone) is state-machine
// state.
func IsRaftEngineKey(k Key) bool {
	pre := len(localUnreplicatedPrefix) + 1 + 8 // prefix, 'r', range ID
	if len(k) < pre || k[len(localUnreplicatedPrefix)] != 'r' {
		return false
	}
	suffix := k[pre:]
	switch {
	case string(suffix) == "hs", string(suffix) == "ts":
		return true
	case len(suffix) == 3+8 && string(suffix[:3]) == "log":
		return true
	}
	return false
}

// RangeDescriptorKey stores this replica's copy of the range descriptor.
func RangeDescriptorKey(rangeID base.RangeID) Key { return makeRangeLocalKey(rangeID, "desc") }

// RangeUnreplicatedPrefix is the prefix of ALL of a range's replica-local
// unreplicated keys (Raft state, applied state, descriptor copy) — used to
// wipe a removed replica.
func RangeUnreplicatedPrefix(rangeID base.RangeID) Key {
	return makeRangeLocalKey(rangeID, "")
}

// RangeTombstoneKey marks a removed replica so it cannot be revived by stale
// Raft messages.
func RangeTombstoneKey(rangeID base.RangeID) Key { return makeRangeLocalKey(rangeID, "tomb") }

// ---------------------------------------------------------------------------
// Range-local addressed keys (replicated, routed by embedded global key).

// TransactionKey is the transaction record's storage key, addressed by the
// transaction's anchor key: it is owned by (and replicated with) whichever
// range covers that global key, but invisible to user scans.
func TransactionKey(anchor Key, txnID uuid.UUID) Key {
	k := localAddressedPrefix.Clone()
	k = Key(encoding.EncodeBytes(k, anchor))
	k = append(k, []byte("txn")...)
	return append(k, txnID[:]...)
}

// Addr returns the global key a key is addressed by: global keys address
// themselves; range-local addressed keys address their embedded global key.
// Unreplicated local keys are not addressable.
func Addr(k Key) (Key, error) {
	if !IsLocal(k) {
		return k, nil
	}
	if bytes.HasPrefix(k, localAddressedPrefix) {
		_, addr, err := encoding.DecodeBytes(k[len(localAddressedPrefix):])
		if err != nil {
			return nil, fmt.Errorf("malformed range-local key %q: %w", []byte(k), err)
		}
		return Key(addr), nil
	}
	return nil, fmt.Errorf("key %q is store-local and not addressable", []byte(k))
}

// RangeLocalAddressedSpan returns the span of range-local addressed keys
// belonging to the range [start, end): needed when snapshotting a range so
// its transaction records travel with it.
func RangeLocalAddressedSpan(start, end Key) (Key, Key) {
	s := Key(encoding.EncodeBytes(localAddressedPrefix.Clone(), start))
	// Trim the terminator so the span covers all keys with the encoded
	// prefix >= start.
	s = s[:len(s)-2]
	e := Key(encoding.EncodeBytes(localAddressedPrefix.Clone(), end))
	e = e[:len(e)-2]
	return s, e
}

// ---------------------------------------------------------------------------
// Meta (range addressing) keys.

// RangeMetaKey returns the meta record key for a range ending at endKey:
// /meta/<endKey>. Range lookup for key K scans for the first meta key
// >= /meta/<K.Next()> — the record of the first range whose end is > K.
func RangeMetaKey(endKey Key) Key {
	return append(MetaPrefix.Clone(), endKey...)
}

// MetaSpan is the span of all meta records.
func MetaSpan() (Key, Key) { return MetaPrefix.Clone(), MetaPrefix.PrefixEnd() }

// ---------------------------------------------------------------------------
// System keys.

func systemKey(parts ...string) Key {
	k := SystemPrefix.Clone()
	for _, p := range parts {
		k = Key(encoding.EncodeString(k, p))
	}
	return k
}

// ClusterVersionKey holds the cluster's finalized protocol version
// (decimal string, e.g. "2"; missing = version 1). It only moves forward,
// via the "upgrade-cluster" admin op.
func ClusterVersionKey() Key { return systemKey("cluster-version") }

// NodeRegistryKey holds a node's registration (address, locality, liveness).
func NodeRegistryKey(nodeID base.NodeID) Key {
	k := systemKey("nodes")
	return Key(encoding.EncodeUint64(k, uint64(nodeID)))
}

// NodeRegistrySpan covers all node registrations.
func NodeRegistrySpan() (Key, Key) {
	p := systemKey("nodes")
	return p, p.PrefixEnd()
}

// NodeIDGenKey is the counter for allocating node IDs.
func NodeIDGenKey() Key { return systemKey("idgen", "node") }

// RangeIDGenKey is the counter for allocating range IDs.
func RangeIDGenKey() Key { return systemKey("idgen", "range") }

// DescIDGenKey is the counter for allocating SQL descriptor (table) IDs.
func DescIDGenKey() Key { return systemKey("idgen", "desc") }

// UserKey holds a SQL user's SCRAM verifier (JSON; never a plaintext
// password).
func UserKey(name string) Key {
	k := systemKey("users")
	return Key(encoding.EncodeString(k, name))
}

// AdminUserKey marks a SQL user as a member of the admin role (root is
// implicitly a member and has no row).
func AdminUserKey(name string) Key {
	k := systemKey("admins")
	return Key(encoding.EncodeString(k, name))
}

// TableDescKey holds the JSON table descriptor for a table ID.
func TableDescKey(tableID uint64) Key {
	k := systemKey("desc")
	return Key(encoding.EncodeUint64(k, tableID))
}

// TableDescSpan covers all table descriptors.
func TableDescSpan() (Key, Key) {
	p := systemKey("desc")
	return p, p.PrefixEnd()
}

// TableStatsKey holds the JSON table statistics (row count, per-column
// distinct estimates) for a table ID. Deliberately a separate key from
// the descriptor: the descriptor is re-read constantly by lease renewals
// and every version bump churns gateway leases, while stats writes are
// routine background traffic. Missing = no statistics (planner falls
// back to structural ranking).
func TableStatsKey(tableID uint64) Key {
	k := systemKey("stats")
	return Key(encoding.EncodeUint64(k, tableID))
}

// TableStatsSpan covers all table statistics (orphan sweeps).
func TableStatsSpan() (Key, Key) {
	p := systemKey("stats")
	return p, p.PrefixEnd()
}

// TableStatsID extracts the table ID a stats key refers to.
func TableStatsID(k Key) (uint64, bool) {
	p := systemKey("stats")
	if len(k) <= len(p) || string(k[:len(p)]) != string(p) {
		return 0, false
	}
	_, id, err := encoding.DecodeUint64(k[len(p):])
	if err != nil {
		return 0, false
	}
	return id, true
}

// NamespaceKey maps a table name to its ID in the flat, pre-v6 layout.
// Tables created before the cluster finalized v6 live here until the
// migration moves them under their database (TableNamespaceKey); the
// catalog still reads it for the default database.
func NamespaceKey(name string) Key {
	k := systemKey("ns")
	return Key(encoding.EncodeString(k, name))
}

// DatabaseDescKey holds the JSON database descriptor for a database ID.
func DatabaseDescKey(dbID uint64) Key {
	k := systemKey("db")
	return Key(encoding.EncodeUint64(k, dbID))
}

// DatabaseDescSpan covers all database descriptors.
func DatabaseDescSpan() (Key, Key) {
	p := systemKey("db")
	return p, p.PrefixEnd()
}

// DatabaseNamespaceKey maps a database name to its ID.
func DatabaseNamespaceKey(name string) Key {
	k := systemKey("dbns")
	return Key(encoding.EncodeString(k, name))
}

// DatabaseNamespaceSpan covers all database names.
func DatabaseNamespaceSpan() (Key, Key) {
	p := systemKey("dbns")
	return p, p.PrefixEnd()
}

// TableNamespaceKey maps a table name within a database to its table ID
// (the v6 layout). Schemas are not encoded: public is the only one, and
// a schema level would slot in between the database and the name.
func TableNamespaceKey(dbID uint64, name string) Key {
	k := Key(encoding.EncodeUint64(systemKey("nsdb"), dbID))
	return Key(encoding.EncodeString(k, name))
}

// TableNamespaceSpan covers one database's table names.
func TableNamespaceSpan(dbID uint64) (Key, Key) {
	p := Key(encoding.EncodeUint64(systemKey("nsdb"), dbID))
	return p, p.PrefixEnd()
}

// AllTableNamespaceSpan covers every database's table names.
func AllTableNamespaceSpan() (Key, Key) {
	p := systemKey("nsdb")
	return p, p.PrefixEnd()
}

// SequenceDescKey holds the JSON sequence descriptor for a sequence ID
// (IDs come from the shared descriptor counter, so tables and sequences
// never collide).
func SequenceDescKey(seqID uint64) Key {
	return Key(encoding.EncodeUint64(systemKey("seqdesc"), seqID))
}

// SequenceDescSpan covers all sequence descriptors.
func SequenceDescSpan() (Key, Key) {
	p := systemKey("seqdesc")
	return p, p.PrefixEnd()
}

// SequenceNamespaceKey maps a sequence name within a database to its ID.
func SequenceNamespaceKey(dbID uint64, name string) Key {
	k := Key(encoding.EncodeUint64(systemKey("seqns"), dbID))
	return Key(encoding.EncodeString(k, name))
}

// SequenceNamespaceSpan covers one database's sequence names.
func SequenceNamespaceSpan(dbID uint64) (Key, Key) {
	p := Key(encoding.EncodeUint64(systemKey("seqns"), dbID))
	return p, p.PrefixEnd()
}

// AllSequenceNamespaceSpan covers every database's sequence names.
func AllSequenceNamespaceSpan() (Key, Key) {
	p := systemKey("seqns")
	return p, p.PrefixEnd()
}

// TypeDescKey holds the JSON descriptor of a user-defined type (an
// enum) for a type ID (from the shared descriptor counter).
func TypeDescKey(typeID uint64) Key {
	return Key(encoding.EncodeUint64(systemKey("typedesc"), typeID))
}

// TypeDescSpan covers all type descriptors.
func TypeDescSpan() (Key, Key) {
	p := systemKey("typedesc")
	return p, p.PrefixEnd()
}

// TypeNamespaceKey maps a type name within a database to its ID.
func TypeNamespaceKey(dbID uint64, name string) Key {
	k := Key(encoding.EncodeUint64(systemKey("typens"), dbID))
	return Key(encoding.EncodeString(k, name))
}

// TypeNamespaceSpan covers one database's type names.
func TypeNamespaceSpan(dbID uint64) (Key, Key) {
	p := Key(encoding.EncodeUint64(systemKey("typens"), dbID))
	return p, p.PrefixEnd()
}

// AllTypeNamespaceSpan covers every database's type names.
func AllTypeNamespaceSpan() (Key, Key) {
	p := systemKey("typens")
	return p, p.PrefixEnd()
}

// SequenceValueKey is a sequence's counter: the last value handed out
// (decimal, advanced with Increment outside any transaction — never
// rolled back, as in PostgreSQL).
func SequenceValueKey(seqID uint64) Key {
	return Key(encoding.EncodeUint64(systemKey("seq"), seqID))
}

// SequenceValueSpan covers every sequence counter.
func SequenceValueSpan() (Key, Key) {
	p := systemKey("seq")
	return p, p.PrefixEnd()
}

// DescLeaseKey holds one gateway's lease on a table descriptor: proof that
// the gateway may be using the descriptor at the recorded version until the
// recorded expiration. DDL drains against these.
func DescLeaseKey(descID uint64, gateway uuid.UUID) Key {
	k := systemKey("lease")
	k = Key(encoding.EncodeUint64(k, descID))
	return append(k, gateway[:]...)
}

// DescLeaseSpan covers all gateways' leases on one descriptor.
func DescLeaseSpan(descID uint64) (Key, Key) {
	p := Key(encoding.EncodeUint64(systemKey("lease"), descID))
	return p, p.PrefixEnd()
}

// NamespaceSpan covers all table-name mappings.
func NamespaceSpan() (Key, Key) {
	p := systemKey("ns")
	return p, p.PrefixEnd()
}

// UserSpan covers all SQL user credential records.
func UserSpan() (Key, Key) {
	p := systemKey("users")
	return p, p.PrefixEnd()
}

// AdminUserSpan covers all admin-role membership markers.
func AdminUserSpan() (Key, Key) {
	p := systemKey("admins")
	return p, p.PrefixEnd()
}

// RoleKey holds a role's descriptor (pkg/sql/catalog.RoleDescriptor:
// login flag, SCRAM verifier, memberships) at /system/roles/<name>.
// Cluster version v11 supersedes the /system/users and /system/admins
// layouts with it; the finalize migration rewrites them.
func RoleKey(name string) Key {
	k := systemKey("roles")
	return Key(encoding.EncodeString(k, name))
}

// RoleSpan covers every role descriptor.
func RoleSpan() (Key, Key) {
	p := systemKey("roles")
	return p, p.PrefixEnd()
}

// ---------------------------------------------------------------------------
// Table data keys.

// TableDataPrefix is the prefix of all rows of a table.
func TableDataPrefix(tableID uint64) Key {
	return Key(encoding.EncodeUint64(TablePrefix.Clone(), tableID))
}

// TableDataSpan covers all rows of a table (every index).
func TableDataSpan(tableID uint64) (Key, Key) {
	p := TableDataPrefix(tableID)
	return p, p.PrefixEnd()
}

// TableIndexPrefix is the prefix of one index of a table:
// /t/<tableID>/<indexID>/. Primary rows are index 1 (see pkg/sql/rowenc).
func TableIndexPrefix(tableID, indexID uint64) Key {
	return Key(encoding.EncodeUint64(TableDataPrefix(tableID), indexID))
}

// TableIndexSpan covers all entries of one index of a table.
func TableIndexSpan(tableID, indexID uint64) (Key, Key) {
	p := TableIndexPrefix(tableID, indexID)
	return p, p.PrefixEnd()
}
