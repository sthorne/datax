package kvpb

import (
	"fmt"

	"github.com/google/uuid"
	"github.com/sthorne/datax/pkg/base"
	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/storage"
	"github.com/sthorne/datax/pkg/storage/enginepb"
	"github.com/sthorne/datax/pkg/util/hlc"
)

// RequestHeader is common to all requests: the key (span) it operates on.
type RequestHeader struct {
	Key    keys.Key `json:"key"`
	EndKey keys.Key `json:"end_key,omitempty"` // only for ranged requests
}

// GetRequest reads a key. With ForUpdate set (transactional batches only)
// it is a LOCKING read: evaluated on the write path, it re-reads the key at
// the transaction's read timestamp and lays a write intent pinning the
// observed state — the current value for an existing row, a tombstone for
// an absent one — so no other transaction can change the key until this
// one finishes. A committed version above the read timestamp fails the
// lock with WriteTooOld (the snapshot is stale), exactly like a write.
//
// With SkipLocked set (plain reads only; the write path ignores it) a key
// holding another transaction's intent is not a conflict: the response
// reports it absent and carries the intent in Skipped; so is a key with a
// committed version inside the read's uncertainty window, which would
// otherwise fail the read with an UncertaintyError. Not version-gated:
// a server that predates the field ignores it and answers with the
// write-intent conflict, which the sender handles by waiting as it always
// has.
type GetRequest struct {
	RequestHeader
	ForUpdate  bool `json:"for_update,omitempty"`
	SkipLocked bool `json:"skip_locked,omitempty"`
}

// PutRequest writes a value.
type PutRequest struct {
	RequestHeader
	Value []byte `json:"value"`
}

// DeleteRequest deletes a key (writes a tombstone).
type DeleteRequest struct {
	RequestHeader
}

// IncrementRequest atomically adds By to the varint-encoded value at Key,
// returning the new value. Evaluated under Raft, so it is atomic without a
// transaction. Used for ID allocation.
type IncrementRequest struct {
	RequestHeader
	By int64 `json:"by"`
}

// ScanRequest returns keys in [Key, EndKey). With ForUpdate set
// (transactional batches only) it is a LOCKING scan: each returned row
// gets a write intent pinning its observed value (see GetRequest); absent
// keys in the span are not locked. With Reverse set the span is iterated
// from the end backwards (rows come back largest-key-first, Resume is the
// exclusive END of the next page); Reverse may only be SENT once the
// cluster version has reached v3 — an older node ignores the field and
// runs a forward scan (pkg/version rule 4). Reverse+ForUpdate is not
// supported. With SkipLocked set (plain scans only) rows holding another
// transaction's intent are left out of Rows and their intents listed in
// the response's Skipped (see GetRequest).
type ScanRequest struct {
	RequestHeader
	MaxRows    int64 `json:"max_rows,omitempty"`
	ForUpdate  bool  `json:"for_update,omitempty"`
	Reverse    bool  `json:"reverse,omitempty"`
	SkipLocked bool  `json:"skip_locked,omitempty"`
}

// ExportRequest returns, for every key in [Key, EndKey) that changed in
// (StartTS, batch timestamp], the newest version at or below the batch
// timestamp — deletions included, as tombstone records. StartTS zero
// exports everything live at the batch timestamp (a full backup); non-zero
// exports the delta since a prior export at StartTS (an incremental).
// Evaluated as a consistent read: intents at or below the batch timestamp
// conflict, exactly like a Scan — an inconsistent read would silently miss
// a transaction committing just below the export timestamp.
type ExportRequest struct {
	RequestHeader
	StartTS    hlc.Timestamp `json:"start_ts,omitempty"`
	MaxRecords int64         `json:"max_records,omitempty"`
	// AllRevisions asks for every version in the window rather than the
	// newest, each record carrying its timestamp (a backup with revision
	// history, issue #302). A key's versions are never split across
	// responses, so a response may exceed MaxRecords by the versions of
	// its last key. A replica that predates the field ignores it and
	// answers with the newest versions only, so it is sent only once the
	// cluster has finalized v21 (version.V21).
	AllRevisions bool `json:"all_revisions,omitempty"`
}

// EndTxnRequest commits or aborts the transaction: the atomic flip of the
// transaction record. Routed to the transaction's anchor range.
type EndTxnRequest struct {
	RequestHeader      // Key = the transaction's anchor key
	Commit        bool `json:"commit"`
	// IntentKeys is the transaction's write set (commit only). It is stored
	// on the finalized record so GC can prove every intent was resolved
	// before reclaiming the record — without it, an intent orphaned by a
	// crashed coordinator would be judged expired and wrongly aborted after
	// the record is gone.
	IntentKeys []keys.Key `json:"intent_keys,omitempty"`
	// InFlight names writes pipelined IN PARALLEL with this commit — not
	// yet proven applied. Non-empty InFlight stages the record (STAGING)
	// instead of committing it: the transaction is then implicitly
	// committed once every in-flight write has applied at or below the
	// staged timestamp, and explicitly finalized by a second EndTxn (or by
	// status recovery, if the coordinator dies first).
	InFlight []keys.Key `json:"in_flight,omitempty"`
	// All marks this batch as the transaction's ENTIRE write set — the
	// one-phase-commit hint. A server that recognizes it (and finds the
	// batch 1PC-shaped: this EndTxn last, only writes before it) evaluates
	// the writes as committed values in one proposal, creating no record
	// and no intents, and answers OnePhase. An old server drops the field
	// and evaluates record + intents + commit classically — correct, just
	// unoptimized — which the missing OnePhase in its response reveals.
	All bool `json:"all,omitempty"`
}

// HeartbeatTxnRequest refreshes the transaction record's liveness and
// publishes the coordinator's current wait edge (WaitingFor, uuid.Nil =
// not waiting) for deadlock detection.
type HeartbeatTxnRequest struct {
	RequestHeader               // Key = anchor key
	Now           hlc.Timestamp `json:"now"`
	WaitingFor    uuid.UUID     `json:"waiting_for,omitempty"`
	WaitingForKey keys.Key      `json:"waiting_for_key,omitempty"`
}

// PushTxnRequest asks the pushee's record range to resolve a conflict.
type PushTxnRequest struct {
	RequestHeader                  // Key = pushee's anchor key
	PusherTxn     *Transaction     `json:"pusher_txn,omitempty"` // nil for non-txn pushers
	PusheeTxn     enginepb.TxnMeta `json:"pushee_txn"`
	// PushAbort: abort the pushee outright (write-write conflict); otherwise
	// the push only succeeds if the pushee is already finalized or expired.
	PushAbort bool          `json:"push_abort"`
	Now       hlc.Timestamp `json:"now"`
	// QueryOnly reads the record without any state change (no expiry
	// poisoning, no abort), reporting status, priority, and the pushee's
	// advertised wait edge — the deadlock detector's chain walk. Served on
	// the read path.
	QueryOnly bool `json:"query_only,omitempty"`
	// ForceAbort aborts a PENDING pushee regardless of priority: sent only
	// at a detected deadlock cycle's chosen victim.
	ForceAbort bool `json:"force_abort,omitempty"`
}

// ResolveIntentRequest resolves an intent according to its transaction's
// final status.
type ResolveIntentRequest struct {
	RequestHeader
	TxnID    uuid.UUID          `json:"txn_id"`
	Status   enginepb.TxnStatus `json:"status"`
	CommitTS hlc.Timestamp      `json:"commit_ts"`
	// Poison marks an ABORTED resolution by someone other than the
	// transaction's own coordinator (cluster version v22, issue #320):
	// when it removes an intent of the transaction, the range records an
	// abort-span entry, and the transaction's next request on the range
	// fails with a TxnAbortedError instead of reading past its own
	// removed write. An ABORTED resolution without it — the
	// coordinator's own cleanup — removes the range's entry. Sent only
	// once the cluster is at v22: a v21 replica would drop the field.
	Poison bool `json:"poison,omitempty"`
}

// RollbackIntentRequest rolls the transaction's intent on Key back to its
// newest state at or below Sequence — one key of a savepoint rollback.
// No-op on other transactions' intents and on state at or below Sequence,
// so the coordinator may send one for every key it ever wrote.
type RollbackIntentRequest struct {
	RequestHeader
	TxnID    uuid.UUID `json:"txn_id"`
	Sequence int32     `json:"sequence"`
}

// RefreshRequest verifies that no other transaction wrote into [Key,
// EndKey) (EndKey empty = the single key) within (FromTS, the request
// transaction's ReadTimestamp]. Sent by the coordinator to move a
// transaction's read timestamp forward without restarting; read-only, so it
// takes shared latches and bumps the timestamp cache to the NEW read
// timestamp before evaluating — after success no write can slip beneath it.
type RefreshRequest struct {
	RequestHeader
	FromTS hlc.Timestamp `json:"from_ts"`
}

// GCVersion names one MVCC version to reclaim. Bytes is its stored size
// (engine key + value), filled by the enumerating leader so every replica
// subtracts the same amount from the range's size accounting.
type GCVersion struct {
	Key   keys.Key      `json:"key"`
	TS    hlc.Timestamp `json:"ts"`
	Bytes int64         `json:"bytes,omitempty"`
}

// GCRequest reclaims garbage below Threshold: the listed MVCC versions
// (enumerated by the leader from a consistent snapshot — all superseded
// below the threshold, hence immutable) and finalized transaction records
// (raw storage keys). Applying it also raises the range's replicated GC
// threshold, below which reads are rejected. Replicated so replicas stay
// byte-identical and the threshold survives leadership changes and
// snapshots. Spans the whole range (Key/EndKey = range bounds) so its
// exclusive latch serializes it against readers.
type GCRequest struct {
	RequestHeader
	Threshold     hlc.Timestamp `json:"threshold"`
	Versions      []GCVersion   `json:"versions,omitempty"`
	TxnRecordKeys []keys.Key    `json:"txn_record_keys,omitempty"`
	// AbortSpanCutoff, when set, removes the range's abort-span entries
	// written at or below it (cluster version v22, issue #320). The
	// replicas decide which entries those are from their own replicated
	// state at the command's log position, so they agree.
	AbortSpanCutoff hlc.Timestamp `json:"abort_span_cutoff,omitempty"`
}

// ClearRangeRequest removes every MVCC key of one whole range in a single
// replicated command: the reclaim of a dropped table's span (cluster
// version v22, issue #316; kvserver.Store.ClearRange). It is never sent
// by clients: the range's leader builds it under an exclusive latch on
// the range, having found no intent in the span, and proposes it.
// Applying it deletes the span's MVCC keys with one range deletion,
// raises the range's GC threshold to Timestamp and sets the range's size
// to zero (its MVCC span is empty). A replica whose range no longer has
// these bounds at Generation, or whose threshold is already at or past
// Timestamp, applies nothing and answers Cleared=false.
type ClearRangeRequest struct {
	RequestHeader               // Key/EndKey = the range's bounds
	Timestamp     hlc.Timestamp `json:"timestamp"`
	Generation    int64         `json:"generation,omitempty"`
}

// TruncateLogRequest discards the range's Raft log at or below Index
// (whose term is Term). Proposed by the leader's housekeeping loop with
// Index <= min(every voter's durably-appended Match, the leader's applied
// index) minus a safety floor — so no live voter can ever need a truncated
// entry, across elections included. Replicated: each replica deletes its own
// (unreplicated) log prefix when the command applies, at which point it has
// durably applied everything at or below Index.
type TruncateLogRequest struct {
	RequestHeader        // Key = range start key
	Index         uint64 `json:"index"`
	Term          uint64 `json:"term"`
}

// AdminSplitRequest splits the range containing SplitKey at SplitKey.
//
// StickyUntil makes it a manual split — one an operator asked for, as
// opposed to the size- and load-based splits and the pre-splits DDL makes
// for itself. The new right-hand range records the bit
// (RangeDescriptor.StickyUntil) and the merge queue leaves its start
// boundary alone until then; and because a manual split asks for a state
// ("a sticky boundary at this key") rather than an action, a key that is
// already a boundary is not an error: the range starting there gets the
// bit. Zero is an ordinary split, merged back when both halves are small.
// A leader below cluster version v20 splits without recording the bit.
type AdminSplitRequest struct {
	RequestHeader       // Key = split key
	StickyUntil   int64 `json:"sticky_until,omitempty"`
}

// AdminUnsplitRequest clears the sticky bit of the range that starts at
// Key, so the merge queue may fold it into its left neighbor again. The
// key must be a range boundary; a boundary that is not sticky is left as
// it is. Sent only once the cluster has finalized v20 (older leaders do
// not know the request).
type AdminUnsplitRequest struct {
	RequestHeader // Key = the range's start key
}

// AdminChangeReplicasRequest adds and/or removes one replica of the range
// covering Key. Adds are preceded by a snapshot preseed of the target.
type AdminChangeReplicasRequest struct {
	RequestHeader
	AddNode    base.NodeID `json:"add_node,omitempty"`
	RemoveNode base.NodeID `json:"remove_node,omitempty"`
}

// AdminTransferLeaseRequest moves the range's raft leadership (and with it
// the lease, since leaseholder = leader) to Target, which must already hold
// a replica.
type AdminTransferLeaseRequest struct {
	RequestHeader             // Key = any key in the range
	Target        base.NodeID `json:"target"`
}

// SubsumeRequest freezes the range for a merge into MergeInto: applied on
// every replica, persisted in replicaState, and from then on the range
// refuses all traffic until it is absorbed (or unfrozen).
type SubsumeRequest struct {
	RequestHeader              // Key = start key, EndKey = end key
	MergeInto     base.RangeID `json:"merge_into"`
	// MergeEpoch ties the freeze to the left-hand range as the driver saw
	// it: that range's generation plus one (0: not stated — a driver from
	// before cluster version v20). A merge may be built under a freeze
	// only while the left-hand range is still at that generation, which
	// is what lets a freeze left behind by a dead attempt be unwound
	// safely: move the left-hand range's generation on, and nothing built
	// or yet to be built under the old freeze can apply (issue #266).
	MergeEpoch int64 `json:"merge_epoch,omitempty"`
}

// UpdateMetaRequest writes the range addressing record at Key — or, with
// a nil Desc, deletes it — only if that advances the record's generation.
// Splits and merges repair addressing after they commit, outside any
// latch, so two of them on the same key lineage can land in either
// order; a blind put or delete from the earlier one could then overwrite
// the later one's record with a range that no longer exists, and nothing
// re-cleans /meta. Generations are strictly increasing along a lineage
// (every split and merge takes max(inputs)+1), so "newer generation wins"
// orders them. A delete is applied only while the record still names
// IfRangeID at IfGeneration or older. Sent only once the cluster has
// finalized v4 (older leaders do not know the request); before that the
// blind writes are used. Issue #74.
type UpdateMetaRequest struct {
	RequestHeader
	Desc         *RangeDescriptor `json:"desc,omitempty"`
	IfRangeID    base.RangeID     `json:"if_range_id,omitempty"`
	IfGeneration int64            `json:"if_generation,omitempty"`
}

// UnfreezeRequest clears a Subsume freeze (the merge was abandoned).
type UnfreezeRequest struct {
	RequestHeader // Key = start key, EndKey = end key
	// MergeEpoch, when not 0, names the freeze to clear (see
	// SubsumeRequest.MergeEpoch): a range frozen under another epoch is
	// left as it is. An attempt giving up on its own freeze must not
	// clear the freeze a later attempt has made since.
	MergeEpoch int64 `json:"merge_epoch,omitempty"`
}

// AdminMergeRequest merges the range containing Key with its right
// neighbor. Driven by the node leading both sides.
type AdminMergeRequest struct {
	RequestHeader // Key = any key in the left-hand range
}

// RequestUnion holds exactly one request.
type RequestUnion struct {
	Get                 *GetRequest                 `json:"get,omitempty"`
	Put                 *PutRequest                 `json:"put,omitempty"`
	Delete              *DeleteRequest              `json:"delete,omitempty"`
	Increment           *IncrementRequest           `json:"increment,omitempty"`
	Scan                *ScanRequest                `json:"scan,omitempty"`
	Export              *ExportRequest              `json:"export,omitempty"`
	EndTxn              *EndTxnRequest              `json:"end_txn,omitempty"`
	HeartbeatTxn        *HeartbeatTxnRequest        `json:"heartbeat_txn,omitempty"`
	PushTxn             *PushTxnRequest             `json:"push_txn,omitempty"`
	ResolveIntent       *ResolveIntentRequest       `json:"resolve_intent,omitempty"`
	RollbackIntent      *RollbackIntentRequest      `json:"rollback_intent,omitempty"`
	RecoverTxn          *RecoverTxnRequest          `json:"recover_txn,omitempty"`
	Refresh             *RefreshRequest             `json:"refresh,omitempty"`
	GC                  *GCRequest                  `json:"gc,omitempty"`
	TruncateLog         *TruncateLogRequest         `json:"truncate_log,omitempty"`
	AdminSplit          *AdminSplitRequest          `json:"admin_split,omitempty"`
	AdminChangeReplicas *AdminChangeReplicasRequest `json:"admin_change_replicas,omitempty"`
	AdminTransferLease  *AdminTransferLeaseRequest  `json:"admin_transfer_lease,omitempty"`
	AdminMerge          *AdminMergeRequest          `json:"admin_merge,omitempty"`
	Subsume             *SubsumeRequest             `json:"subsume,omitempty"`
	Unfreeze            *UnfreezeRequest            `json:"unfreeze,omitempty"`
	UpdateMeta          *UpdateMetaRequest          `json:"update_meta,omitempty"`
	AdminUnsplit        *AdminUnsplitRequest        `json:"admin_unsplit,omitempty"`
	ClearRange          *ClearRangeRequest          `json:"clear_range,omitempty"`
}

// GetInner returns the wrapped request.
func (u RequestUnion) GetInner() Request {
	switch {
	case u.Get != nil:
		return u.Get
	case u.Put != nil:
		return u.Put
	case u.Delete != nil:
		return u.Delete
	case u.Increment != nil:
		return u.Increment
	case u.Scan != nil:
		return u.Scan
	case u.Export != nil:
		return u.Export
	case u.EndTxn != nil:
		return u.EndTxn
	case u.HeartbeatTxn != nil:
		return u.HeartbeatTxn
	case u.PushTxn != nil:
		return u.PushTxn
	case u.ResolveIntent != nil:
		return u.ResolveIntent
	case u.RollbackIntent != nil:
		return u.RollbackIntent
	case u.RecoverTxn != nil:
		return u.RecoverTxn
	case u.Refresh != nil:
		return u.Refresh
	case u.GC != nil:
		return u.GC
	case u.TruncateLog != nil:
		return u.TruncateLog
	case u.AdminSplit != nil:
		return u.AdminSplit
	case u.AdminChangeReplicas != nil:
		return u.AdminChangeReplicas
	case u.AdminTransferLease != nil:
		return u.AdminTransferLease
	case u.AdminMerge != nil:
		return u.AdminMerge
	case u.Subsume != nil:
		return u.Subsume
	case u.Unfreeze != nil:
		return u.Unfreeze
	case u.UpdateMeta != nil:
		return u.UpdateMeta
	case u.AdminUnsplit != nil:
		return u.AdminUnsplit
	case u.ClearRange != nil:
		return u.ClearRange
	}
	return nil
}

// Request is implemented by all request types.
type Request interface {
	Header() RequestHeader
	Method() string
	// IsReadOnly requests can be served without a Raft proposal (via
	// ReadIndex on the leader).
	IsReadOnly() bool
}

func (h *GetRequest) Header() RequestHeader                 { return h.RequestHeader }
func (h *PutRequest) Header() RequestHeader                 { return h.RequestHeader }
func (h *DeleteRequest) Header() RequestHeader              { return h.RequestHeader }
func (h *IncrementRequest) Header() RequestHeader           { return h.RequestHeader }
func (h *ScanRequest) Header() RequestHeader                { return h.RequestHeader }
func (h *ExportRequest) Header() RequestHeader              { return h.RequestHeader }
func (h *EndTxnRequest) Header() RequestHeader              { return h.RequestHeader }
func (h *HeartbeatTxnRequest) Header() RequestHeader        { return h.RequestHeader }
func (h *PushTxnRequest) Header() RequestHeader             { return h.RequestHeader }
func (h *ResolveIntentRequest) Header() RequestHeader       { return h.RequestHeader }
func (h *RollbackIntentRequest) Header() RequestHeader      { return h.RequestHeader }
func (h *RecoverTxnRequest) Header() RequestHeader          { return h.RequestHeader }
func (h *RefreshRequest) Header() RequestHeader             { return h.RequestHeader }
func (h *GCRequest) Header() RequestHeader                  { return h.RequestHeader }
func (h *TruncateLogRequest) Header() RequestHeader         { return h.RequestHeader }
func (h *AdminSplitRequest) Header() RequestHeader          { return h.RequestHeader }
func (h *AdminChangeReplicasRequest) Header() RequestHeader { return h.RequestHeader }
func (h *AdminTransferLeaseRequest) Header() RequestHeader  { return h.RequestHeader }
func (h *AdminMergeRequest) Header() RequestHeader          { return h.RequestHeader }
func (h *SubsumeRequest) Header() RequestHeader             { return h.RequestHeader }
func (h *UnfreezeRequest) Header() RequestHeader            { return h.RequestHeader }
func (h *UpdateMetaRequest) Header() RequestHeader          { return h.RequestHeader }
func (h *AdminUnsplitRequest) Header() RequestHeader        { return h.RequestHeader }
func (h *ClearRangeRequest) Header() RequestHeader          { return h.RequestHeader }

func (*GetRequest) Method() string                 { return "Get" }
func (*PutRequest) Method() string                 { return "Put" }
func (*DeleteRequest) Method() string              { return "Delete" }
func (*IncrementRequest) Method() string           { return "Increment" }
func (*ScanRequest) Method() string                { return "Scan" }
func (*ExportRequest) Method() string              { return "Export" }
func (*EndTxnRequest) Method() string              { return "EndTxn" }
func (*HeartbeatTxnRequest) Method() string        { return "HeartbeatTxn" }
func (*PushTxnRequest) Method() string             { return "PushTxn" }
func (*ResolveIntentRequest) Method() string       { return "ResolveIntent" }
func (*RollbackIntentRequest) Method() string      { return "RollbackIntent" }
func (*RecoverTxnRequest) Method() string          { return "RecoverTxn" }
func (*RefreshRequest) Method() string             { return "Refresh" }
func (*GCRequest) Method() string                  { return "GC" }
func (*TruncateLogRequest) Method() string         { return "TruncateLog" }
func (*AdminSplitRequest) Method() string          { return "AdminSplit" }
func (*AdminChangeReplicasRequest) Method() string { return "AdminChangeReplicas" }
func (*AdminTransferLeaseRequest) Method() string  { return "AdminTransferLease" }
func (*AdminMergeRequest) Method() string          { return "AdminMerge" }
func (*SubsumeRequest) Method() string             { return "Subsume" }
func (*UnfreezeRequest) Method() string            { return "Unfreeze" }
func (*UpdateMetaRequest) Method() string          { return "UpdateMeta" }
func (*AdminUnsplitRequest) Method() string        { return "AdminUnsplit" }
func (*ClearRangeRequest) Method() string          { return "ClearRange" }

func (r *GetRequest) IsReadOnly() bool               { return !r.ForUpdate }
func (*PutRequest) IsReadOnly() bool                 { return false }
func (*DeleteRequest) IsReadOnly() bool              { return false }
func (*IncrementRequest) IsReadOnly() bool           { return false }
func (r *ScanRequest) IsReadOnly() bool              { return !r.ForUpdate }
func (*ExportRequest) IsReadOnly() bool              { return true }
func (*EndTxnRequest) IsReadOnly() bool              { return false }
func (*HeartbeatTxnRequest) IsReadOnly() bool        { return false }
func (r *PushTxnRequest) IsReadOnly() bool           { return r.QueryOnly }
func (*ResolveIntentRequest) IsReadOnly() bool       { return false }
func (*RollbackIntentRequest) IsReadOnly() bool      { return false }
func (*RecoverTxnRequest) IsReadOnly() bool          { return false }
func (*RefreshRequest) IsReadOnly() bool             { return true }
func (*GCRequest) IsReadOnly() bool                  { return false }
func (*TruncateLogRequest) IsReadOnly() bool         { return false }
func (*AdminSplitRequest) IsReadOnly() bool          { return false }
func (*AdminChangeReplicasRequest) IsReadOnly() bool { return false }
func (*AdminTransferLeaseRequest) IsReadOnly() bool  { return false }
func (*AdminMergeRequest) IsReadOnly() bool          { return false }
func (*SubsumeRequest) IsReadOnly() bool             { return false }
func (*UnfreezeRequest) IsReadOnly() bool            { return false }
func (*UpdateMetaRequest) IsReadOnly() bool          { return false }
func (*AdminUnsplitRequest) IsReadOnly() bool        { return false }
func (*ClearRangeRequest) IsReadOnly() bool          { return false }

// Response types.

type GetResponse struct {
	Value []byte `json:"value,omitempty"` // nil = not found
	// Skipped is set when a SkipLocked read passed the key over (Value
	// is then nil): the intent it holds, or just the key (an empty Txn)
	// when it was passed over for a committed version the read could not
	// order itself against.
	Skipped *storage.Intent `json:"skipped,omitempty"`
}

type PutResponse struct{}

type DeleteResponse struct{}

type IncrementResponse struct {
	NewValue int64 `json:"new_value"`
}

type ScanResponse struct {
	Rows   []KeyValue `json:"rows,omitempty"`
	Resume keys.Key   `json:"resume,omitempty"`
	// Skipped lists the rows a SkipLocked scan passed over (see
	// GetResponse.Skipped).
	Skipped []storage.Intent `json:"skipped,omitempty"`
}

// ExportRecord is one exported key: its newest visible value at the export
// timestamp, or a tombstone marker when the change in the window was a
// deletion.
type ExportRecord struct {
	Key     keys.Key `json:"key"`
	Value   []byte   `json:"value,omitempty"`
	Deleted bool     `json:"deleted,omitempty"`
	// Timestamp is the version's timestamp. A replica at cluster version
	// v22 sets it on every record; before, only in answer to an
	// AllRevisions export.
	Timestamp hlc.Timestamp `json:"ts,omitzero"`
}

type ExportResponse struct {
	Records []ExportRecord `json:"records,omitempty"`
	Resume  keys.Key       `json:"resume,omitempty"`
}

type EndTxnResponse struct {
	// CommitTimestamp is the timestamp the transaction committed at.
	CommitTimestamp hlc.Timestamp `json:"commit_ts"`
	// OnePhase reports that the batch committed via the one-phase fast
	// path: values written committed, no record, nothing to resolve.
	OnePhase bool `json:"one_phase,omitempty"`
}

type HeartbeatTxnResponse struct {
	// Status reflects the record after the heartbeat; ABORTED tells the
	// coordinator it has been pushed away.
	Status enginepb.TxnStatus `json:"status"`
}

type PushTxnResponse struct {
	// Status of the pushee after the push: COMMITTED or ABORTED mean the
	// pusher may resolve the intent it found; PENDING means the pushee is
	// alive and the pusher must wait.
	Status   enginepb.TxnStatus `json:"status"`
	CommitTS hlc.Timestamp      `json:"commit_ts"`
	// Chain-walk fields, populated for QueryOnly pushes on a live record.
	WaitingFor    uuid.UUID `json:"waiting_for,omitempty"`
	WaitingForKey keys.Key  `json:"waiting_for_key,omitempty"`
	Priority      int32     `json:"priority,omitempty"`
	// InFlightKeys, when Status is STAGING, is the staged write set the
	// pusher needs to run status recovery.
	InFlightKeys []keys.Key `json:"in_flight_keys,omitempty"`
}

// RecoverTxnRequest finalizes a STAGING transaction record after status
// recovery: Commit reports whether every staged in-flight write was found
// present (at or below the staged timestamp). Idempotent: a record no
// longer STAGING is left as is. Routed to the record's anchor range.
type RecoverTxnRequest struct {
	RequestHeader           // Key = the transaction's anchor key
	TxnID         uuid.UUID `json:"txn_id"`
	Commit        bool      `json:"commit"`
}

type RecoverTxnResponse struct {
	Status enginepb.TxnStatus `json:"status"`
}

type ResolveIntentResponse struct{}

type RollbackIntentResponse struct{}

// AdminSplitResponse carries both halves. When a manual split found its
// key already a boundary, nothing was split: Right is the range starting
// at the key and Left is empty (RangeID 0).
type AdminSplitResponse struct {
	Left  RangeDescriptor `json:"left"`
	Right RangeDescriptor `json:"right"`
}

// AdminUnsplitResponse carries the range's descriptor after the call.
type AdminUnsplitResponse struct {
	Desc RangeDescriptor `json:"desc"`
}

type RefreshResponse struct{}

type GCResponse struct{}

type TruncateLogResponse struct{}

// ClearRangeResponse: Cleared is false when the replica found the command
// had already applied (its threshold is at or past the clear's
// timestamp) or the range had changed since the leader evaluated it.
type ClearRangeResponse struct {
	Cleared bool `json:"cleared,omitempty"`
}

type AdminChangeReplicasResponse struct {
	Desc RangeDescriptor `json:"desc"`
}

type AdminTransferLeaseResponse struct {
	Desc RangeDescriptor `json:"desc"`
}

type AdminMergeResponse struct {
	Desc RangeDescriptor `json:"desc"`
}

// SubsumeResponse: AlreadyFrozen reports that the range was frozen before
// this Subsume applied — a re-drive of an interrupted merge, or a second
// driver racing the first — so the caller did not make the freeze and
// must not unwind it (issue #263).
type SubsumeResponse struct {
	AlreadyFrozen bool `json:"already_frozen,omitempty"`
	// MergeEpoch is the epoch of the freeze the range is under after this
	// Subsume: the request's own when it made the freeze, the earlier
	// freeze's when the range was frozen already.
	MergeEpoch int64 `json:"merge_epoch,omitempty"`
}

type UnfreezeResponse struct{}

// UpdateMetaResponse reports whether the record was written (or deleted);
// false means a newer record was left in place.
type UpdateMetaResponse struct {
	Applied bool `json:"applied"`
}

// ResponseUnion holds exactly one response.
type ResponseUnion struct {
	Get                 *GetResponse                 `json:"get,omitempty"`
	Put                 *PutResponse                 `json:"put,omitempty"`
	Delete              *DeleteResponse              `json:"delete,omitempty"`
	Increment           *IncrementResponse           `json:"increment,omitempty"`
	Scan                *ScanResponse                `json:"scan,omitempty"`
	Export              *ExportResponse              `json:"export,omitempty"`
	EndTxn              *EndTxnResponse              `json:"end_txn,omitempty"`
	HeartbeatTxn        *HeartbeatTxnResponse        `json:"heartbeat_txn,omitempty"`
	PushTxn             *PushTxnResponse             `json:"push_txn,omitempty"`
	ResolveIntent       *ResolveIntentResponse       `json:"resolve_intent,omitempty"`
	RollbackIntent      *RollbackIntentResponse      `json:"rollback_intent,omitempty"`
	RecoverTxn          *RecoverTxnResponse          `json:"recover_txn,omitempty"`
	Refresh             *RefreshResponse             `json:"refresh,omitempty"`
	GC                  *GCResponse                  `json:"gc,omitempty"`
	TruncateLog         *TruncateLogResponse         `json:"truncate_log,omitempty"`
	AdminSplit          *AdminSplitResponse          `json:"admin_split,omitempty"`
	AdminChangeReplicas *AdminChangeReplicasResponse `json:"admin_change_replicas,omitempty"`
	AdminTransferLease  *AdminTransferLeaseResponse  `json:"admin_transfer_lease,omitempty"`
	AdminMerge          *AdminMergeResponse          `json:"admin_merge,omitempty"`
	Subsume             *SubsumeResponse             `json:"subsume,omitempty"`
	Unfreeze            *UnfreezeResponse            `json:"unfreeze,omitempty"`
	UpdateMeta          *UpdateMetaResponse          `json:"update_meta,omitempty"`
	AdminUnsplit        *AdminUnsplitResponse        `json:"admin_unsplit,omitempty"`
	ClearRange          *ClearRangeResponse          `json:"clear_range,omitempty"`
}

// BatchHeader carries batch-wide state.
type BatchHeader struct {
	// Timestamp is the read/write timestamp for non-transactional batches;
	// transactional batches use the transaction's timestamps.
	Timestamp hlc.Timestamp `json:"timestamp"`
	Txn       *Transaction  `json:"txn,omitempty"`
	// RangeID the sender believes owns the batch's span (0 = let the server
	// route by key, single-range only).
	RangeID base.RangeID `json:"range_id,omitempty"`
	// CreateTxnRecord: this batch contains the transaction's first write on
	// its anchor range; the server creates the transaction record
	// atomically with the writes.
	CreateTxnRecord bool `json:"create_txn_record,omitempty"`
	// ReadInconsistent makes reads ignore intents (reading the newest
	// committed version beneath them). Meta/registry scans only.
	ReadInconsistent bool `json:"read_inconsistent,omitempty"`
	// StaleRead marks a read-only batch at a FIXED past timestamp that a
	// follower replica may serve locally when the timestamp is at or below
	// its closed timestamp. Historical reads need no uncertainty interval:
	// the closed timestamp proves nothing new can commit at or below the
	// read timestamp anywhere.
	StaleRead bool `json:"stale_read,omitempty"`
}

// BatchRequest is the unit of KV RPC.
type BatchRequest struct {
	Header   BatchHeader    `json:"header"`
	Requests []RequestUnion `json:"requests"`
}

// IsReadOnly reports whether every request in the batch is read-only.
func (b *BatchRequest) IsReadOnly() bool {
	for _, u := range b.Requests {
		r := u.GetInner()
		if r == nil || !r.IsReadOnly() {
			return false
		}
	}
	return true
}

// HasMVCCWrites reports whether the batch writes MVCC versions (Put, Delete,
// Increment, and locking reads, whose intents commit as versions) — the
// writes the timestamp cache must gate. Transaction-record operations write
// no versions and are exempt.
func (b *BatchRequest) HasMVCCWrites() bool {
	for _, u := range b.Requests {
		switch r := u.GetInner().(type) {
		case *PutRequest, *DeleteRequest, *IncrementRequest, *UpdateMetaRequest:
			return true
		case *GetRequest:
			if r.ForUpdate {
				return true
			}
		case *ScanRequest:
			if r.ForUpdate {
				return true
			}
		}
	}
	return false
}

// Add appends a request to the batch.
func (b *BatchRequest) Add(r Request) {
	var u RequestUnion
	switch t := r.(type) {
	case *GetRequest:
		u.Get = t
	case *PutRequest:
		u.Put = t
	case *DeleteRequest:
		u.Delete = t
	case *IncrementRequest:
		u.Increment = t
	case *ScanRequest:
		u.Scan = t
	case *ExportRequest:
		u.Export = t
	case *EndTxnRequest:
		u.EndTxn = t
	case *HeartbeatTxnRequest:
		u.HeartbeatTxn = t
	case *PushTxnRequest:
		u.PushTxn = t
	case *ResolveIntentRequest:
		u.ResolveIntent = t
	case *RollbackIntentRequest:
		u.RollbackIntent = t
	case *RecoverTxnRequest:
		u.RecoverTxn = t
	case *RefreshRequest:
		u.Refresh = t
	case *GCRequest:
		u.GC = t
	case *TruncateLogRequest:
		u.TruncateLog = t
	case *AdminSplitRequest:
		u.AdminSplit = t
	case *AdminChangeReplicasRequest:
		u.AdminChangeReplicas = t
	case *AdminTransferLeaseRequest:
		u.AdminTransferLease = t
	case *AdminMergeRequest:
		u.AdminMerge = t
	case *SubsumeRequest:
		u.Subsume = t
	case *UnfreezeRequest:
		u.Unfreeze = t
	case *UpdateMetaRequest:
		u.UpdateMeta = t
	case *AdminUnsplitRequest:
		u.AdminUnsplit = t
	case *ClearRangeRequest:
		u.ClearRange = t
	default:
		panic(fmt.Sprintf("unknown request type %T", r))
	}
	b.Requests = append(b.Requests, u)
}

// BatchResponse mirrors BatchRequest.
type BatchResponse struct {
	// Txn echoes the (possibly updated) transaction state.
	Txn       *Transaction    `json:"txn,omitempty"`
	Timestamp hlc.Timestamp   `json:"timestamp"`
	Responses []ResponseUnion `json:"responses"`
}
