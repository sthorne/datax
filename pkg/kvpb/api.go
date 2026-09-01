package kvpb

import (
	"fmt"

	"github.com/google/uuid"
	"github.com/sthorne/datax/pkg/base"
	"github.com/sthorne/datax/pkg/keys"
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
type GetRequest struct {
	RequestHeader
	ForUpdate bool `json:"for_update,omitempty"`
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
// supported.
type ScanRequest struct {
	RequestHeader
	MaxRows   int64 `json:"max_rows,omitempty"`
	ForUpdate bool  `json:"for_update,omitempty"`
	Reverse   bool  `json:"reverse,omitempty"`
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
type AdminSplitRequest struct {
	RequestHeader // Key = split key
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
}

// UnfreezeRequest clears a Subsume freeze (the merge was abandoned).
type UnfreezeRequest struct {
	RequestHeader // Key = start key, EndKey = end key
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

// Response types.

type GetResponse struct {
	Value []byte `json:"value,omitempty"` // nil = not found
}

type PutResponse struct{}

type DeleteResponse struct{}

type IncrementResponse struct {
	NewValue int64 `json:"new_value"`
}

type ScanResponse struct {
	Rows   []KeyValue `json:"rows,omitempty"`
	Resume keys.Key   `json:"resume,omitempty"`
}

// ExportRecord is one exported key: its newest visible value at the export
// timestamp, or a tombstone marker when the change in the window was a
// deletion.
type ExportRecord struct {
	Key     keys.Key `json:"key"`
	Value   []byte   `json:"value,omitempty"`
	Deleted bool     `json:"deleted,omitempty"`
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

type AdminSplitResponse struct {
	Left  RangeDescriptor `json:"left"`
	Right RangeDescriptor `json:"right"`
}

type RefreshResponse struct{}

type GCResponse struct{}

type TruncateLogResponse struct{}

type AdminChangeReplicasResponse struct {
	Desc RangeDescriptor `json:"desc"`
}

type AdminTransferLeaseResponse struct {
	Desc RangeDescriptor `json:"desc"`
}

type AdminMergeResponse struct {
	Desc RangeDescriptor `json:"desc"`
}

type SubsumeResponse struct{}

type UnfreezeResponse struct{}

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
		case *PutRequest, *DeleteRequest, *IncrementRequest:
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
