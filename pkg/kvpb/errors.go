package kvpb

import (
	"errors"
	"fmt"

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
}

func (e *Error) Error() string { return e.Message }

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
type RangeKeyMismatchError struct {
	RequestKey        keys.Key          `json:"request_key"`
	ActualDescriptors []RangeDescriptor `json:"actual_descriptors,omitempty"`
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
	)
	switch {
	case errors.As(err, &wie):
		e.WriteIntent = &WriteIntentError{Intents: wie.Intents}
	case errors.As(err, &wto):
		e.WriteTooOld = &WriteTooOldError{Timestamp: wto.Timestamp, ActualTimestamp: wto.ActualTimestamp}
	case errors.As(err, &ue):
		e.Uncertainty = &UncertaintyError{ReadTimestamp: ue.ReadTimestamp, ExistingTimestamp: ue.ExistingTimestamp}
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
	}
	return fallback
}
