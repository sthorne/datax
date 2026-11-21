package kvpb

import (
	"errors"
	"fmt"
	"strings"

	"github.com/sthorne/datax/pkg/base"
	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/storage"
	"github.com/sthorne/datax/pkg/util/hlc"
)

// Error is the KV wire error: a message plus at most one typed detail that
// clients dispatch on. It serializes to JSON and implements error.
type Error struct {
	Message string `json:"message"`

	NotLeader        *NotLeaderError        `json:"not_leader,omitempty"`
	RangeNotFound    *RangeNotFoundError    `json:"range_not_found,omitempty"`
	RangeKeyMismatch *RangeKeyMismatchError `json:"range_key_mismatch,omitempty"`
	WriteIntent      *WriteIntentError      `json:"write_intent,omitempty"`
	WriteTooOld      *WriteTooOldError      `json:"write_too_old,omitempty"`
	Uncertainty      *UncertaintyError      `json:"uncertainty,omitempty"`
	TxnAborted       *TxnAbortedError       `json:"txn_aborted,omitempty"`
	TxnRetry         *TxnRetryError         `json:"txn_retry,omitempty"`
	TxnNotFound      *TxnNotFoundError      `json:"txn_not_found,omitempty"`
	Ambiguous        *AmbiguousResultError  `json:"ambiguous,omitempty"`
	// StorageOverloaded accompanies TxnRetry when the leader's engine shed
	// the write under backpressure: retry, but with backoff.
	StorageOverloaded *StorageOverloadedError `json:"storage_overloaded,omitempty"`
	// TooLarge: the gateway refused to send a write batch that no
	// replica could have replicated (kvclient.MaxWriteBatchBytes). It is
	// raised and read on the gateway and never crosses the wire.
	TooLarge *WriteTooLargeError `json:"-"`
	// DescriptorChanged: a split, merge or descriptor update was refused
	// at apply because the range changed after the change was built.
	DescriptorChanged *DescriptorChangedError `json:"descriptor_changed,omitempty"`
	// BelowGCThreshold: the batch's timestamp is at or below the range's
	// GC threshold (issue #313).
	BelowGCThreshold *BelowGCThresholdError `json:"below_gc_threshold,omitempty"`
	// ConditionFailed: a CPutRequest found a live value at its key.
	ConditionFailed *ConditionFailedError `json:"condition_failed,omitempty"`
}

// ConditionFailedError: a conditional put (CPutRequest) found a live
// value at Key, visible at the batch's read timestamp, and wrote
// nothing. It is final, not a conflict to retry: the transaction's
// snapshot holds the value.
type ConditionFailedError struct {
	Key keys.Key `json:"key"`
	// NoneApplied is set by the gateway, and never crosses the wire, when
	// it knows that no part of the batch applied: the part that failed
	// was the only one sent.
	NoneApplied bool `json:"-"`
}

func (e *Error) Error() string { return e.Message }

// BelowGCThresholdError: a batch at or below the range's GC threshold,
// refused because the versions it would need may have been collected.
// Who should act depends on who chose the timestamp: a transaction the
// cluster timestamped succeeds when restarted at a new one, and a read
// at a timestamp the client named (AS OF SYSTEM TIME) never will.
type BelowGCThresholdError struct {
	Timestamp hlc.Timestamp `json:"timestamp"`
	Threshold hlc.Timestamp `json:"threshold"`
}

// belowGCThresholdText is in every such refusal's message; it is how
// one from a peer that predates the typed detail is recognized.
const belowGCThresholdText = "is below the GC threshold"

// NewBelowGCThresholdError is the refusal of a batch at ts by a range
// whose threshold is thr.
func NewBelowGCThresholdError(rangeID base.RangeID, ts, thr hlc.Timestamp) *Error {
	e := NewErrorf("%s: batch timestamp %s %s %s", rangeID, ts, belowGCThresholdText, thr)
	e.BelowGCThreshold = &BelowGCThresholdError{Timestamp: ts, Threshold: thr}
	return e
}

// IsBelowGCThreshold reports whether e refuses a batch at or below a GC
// threshold — by the typed detail, or, from a peer that predates it, by
// the message.
func (e *Error) IsBelowGCThreshold() bool {
	return e != nil && (e.BelowGCThreshold != nil || strings.Contains(e.Message, belowGCThresholdText))
}

// WriteTooLargeError: a write batch for one range is larger than one
// replicated write may be. Bytes is its size and Limit the bound.
type WriteTooLargeError struct {
	Bytes, Limit int64
}

// NotLeaderError: the replica is not the Raft leader; retry at LeaderHint's
// replica if known.
type NotLeaderError struct {
	RangeID    base.RangeID `json:"range_id"`
	LeaderHint base.NodeID  `json:"leader_hint,omitempty"` // 0 = unknown
}

// RangeNotFoundError: this store has no replica of the range.
type RangeNotFoundError struct {
	RangeID base.RangeID `json:"range_id"`
}

// RangeKeyMismatchError: the range does not cover the request's key (stale
// routing). ActualDescriptors, if set, are fresher descriptors the server
// knows (e.g. both halves after a split).
//
// Frozen marks the one mismatch that is not stale routing: the range does
// cover the key but is frozen for a merge in progress, so it refuses all
// traffic until the merge lands (the merged neighbor then answers) or is
// abandoned (the range serves again). A client that sees it holds its
// descriptor and waits briefly instead of re-resolving (issue #263). A
// server that predates the field never sets it; a client that predates
// it ignores it and re-resolves as before.
type RangeKeyMismatchError struct {
	RequestKey        keys.Key          `json:"request_key"`
	ActualDescriptors []RangeDescriptor `json:"actual_descriptors,omitempty"`
	Frozen            bool              `json:"frozen,omitempty"`
}

// DescriptorChangedError: a descriptor change — a split, a merge, a
// sticky-bit update — was built from the range at generation BuiltFrom,
// but by the time it applied another change had landed and the range was
// at Current, so it was refused on every replica alike (issue #200).
// Nothing happened; the request is still meaningful against the current
// descriptor, and whoever built the change re-reads it and tries again
// (the admin split does so itself, issue #290). A peer that predates the
// detail sees only the message.
type DescriptorChangedError struct {
	RangeID   base.RangeID `json:"range_id"`
	BuiltFrom int64        `json:"built_from"`
	Current   int64        `json:"current"`
}

// WriteIntentError mirrors storage.WriteIntentError across the wire.
type WriteIntentError struct {
	Intents []storage.Intent `json:"intents"`
}

// WriteTooOldError mirrors storage.WriteTooOldError.
type WriteTooOldError struct {
	Timestamp       hlc.Timestamp `json:"timestamp"`
	ActualTimestamp hlc.Timestamp `json:"actual_timestamp"`
}

// UncertaintyError mirrors storage.UncertaintyError.
type UncertaintyError struct {
	ReadTimestamp     hlc.Timestamp `json:"read_timestamp"`
	ExistingTimestamp hlc.Timestamp `json:"existing_timestamp"`
}

// TxnAbortedError: the transaction record is ABORTED (a pusher won).
type TxnAbortedError struct{}

// TxnRetryError: the transaction must restart at a higher timestamp (e.g.
// pushed by the timestamp cache).
type TxnRetryError struct {
	RetryTimestamp hlc.Timestamp `json:"retry_timestamp"`
}

// TxnNotFoundError: no record for the transaction (e.g. heartbeat after the
// record was cleaned up).
type TxnNotFoundError struct{}

// AmbiguousResultError: the outcome of a proposal is unknown (leadership
// changed while it was in flight). Idempotent operations may retry.
type AmbiguousResultError struct{}

// StorageOverloadedError: the leader's engine crossed its backpressure
// thresholds and the write was shed before proposal. Retryable — but
// clients back off instead of retrying hot.
type StorageOverloadedError struct{}

// NewError builds a wire error from any error, preserving known typed
// details (including storage-layer errors).
func NewError(err error) *Error {
	if err == nil {
		return nil
	}
	var we *Error
	if errors.As(err, &we) {
		return we
	}
	e := &Error{Message: err.Error()}
	var (
		wie *storage.WriteIntentError
		wto *storage.WriteTooOldError
		ue  *storage.UncertaintyError
		cfe *storage.ConditionFailedError
	)
	switch {
	case errors.As(err, &wie):
		e.WriteIntent = &WriteIntentError{Intents: wie.Intents}
	case errors.As(err, &wto):
		e.WriteTooOld = &WriteTooOldError{Timestamp: wto.Timestamp, ActualTimestamp: wto.ActualTimestamp}
	case errors.As(err, &ue):
		e.Uncertainty = &UncertaintyError{ReadTimestamp: ue.ReadTimestamp, ExistingTimestamp: ue.ExistingTimestamp}
	case errors.As(err, &cfe):
		e.ConditionFailed = &ConditionFailedError{Key: cfe.Key}
	}
	return e
}

// NewErrorf builds a plain wire error.
func NewErrorf(format string, args ...any) *Error {
	return &Error{Message: fmt.Sprintf(format, args...)}
}

// IsRetryableTxnError reports whether the error means "restart the
// transaction at a higher timestamp" — the situations that map to SQLSTATE
// 40001 when they reach a client (see docs/transactions.md).
func (e *Error) IsRetryableTxnError() bool {
	return e.WriteTooOld != nil || e.Uncertainty != nil || e.TxnRetry != nil || e.TxnAborted != nil
}

// RetryTimestamp returns the minimum timestamp a restart must use.
func (e *Error) RetryTimestamp(fallback hlc.Timestamp) hlc.Timestamp {
	switch {
	case e.WriteTooOld != nil:
		return fallback.Forward(e.WriteTooOld.ActualTimestamp)
	case e.Uncertainty != nil:
		return fallback.Forward(e.Uncertainty.ExistingTimestamp.Next())
	case e.TxnRetry != nil:
		return fallback.Forward(e.TxnRetry.RetryTimestamp)
	case e.BelowGCThreshold != nil:
		// A restart must read above the threshold, which may come from
		// a clock ahead of the restarting node's.
		return fallback.Forward(e.BelowGCThreshold.Threshold.Next())
	}
	return fallback
}
