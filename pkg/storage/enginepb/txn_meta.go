// Package enginepb holds the small types shared between the storage engine
// and the transaction layer. (The name follows CockroachDB convention; the
// types are plain Go with JSON serialization in this prototype.)
package enginepb

import (
	"github.com/google/uuid"
	"github.com/sthorne/datax/pkg/util/hlc"
)

// TxnStatus is the state of a transaction record.
type TxnStatus int32

const (
	PENDING TxnStatus = iota
	COMMITTED
	ABORTED
	// STAGING: a parallel commit is in flight — the record names the
	// writes that were pipelined with it. The transaction is implicitly
	// committed iff every one of them is present at or below the record's
	// timestamp; status recovery decides (see docs/transactions.md).
	STAGING
)

func (s TxnStatus) String() string {
	switch s {
	case PENDING:
		return "PENDING"
	case COMMITTED:
		return "COMMITTED"
	case ABORTED:
		return "ABORTED"
	case STAGING:
		return "STAGING"
	default:
		return "UNKNOWN"
	}
}

// TxnMeta is the subset of transaction state that rides along with every
// write intent and request. The full transaction record embeds it.
type TxnMeta struct {
	ID uuid.UUID `json:"id"`
	// Key is the anchor key: the key of the transaction's first write. The
	// transaction record lives on the range covering this key.
	Key []byte `json:"key,omitempty"`
	// Epoch increments each time the transaction restarts (retries at a new
	// timestamp). Intents from older epochs are ignored and rewritten.
	Epoch int32 `json:"epoch"`
	// WriteTimestamp is the timestamp at which the transaction writes (and,
	// in datax's retry-only design, must equal the read timestamp at commit).
	WriteTimestamp hlc.Timestamp `json:"write_ts"`
	// MinTimestamp is the timestamp the transaction first started at, across
	// all epochs. Used when pushing: the record's expiry is judged from it
	// if the record does not exist yet.
	MinTimestamp hlc.Timestamp `json:"min_ts"`
	// Priority breaks conflicts: a pusher with higher priority may abort a
	// pending transaction.
	Priority int32 `json:"priority"`
	// Sequence orders the transaction's own writes (bumped by the
	// coordinator before each write). Savepoint rollback restores every
	// intent to its newest state at or below the savepoint's sequence.
	Sequence int32 `json:"seq,omitempty"`
}

// IntentValue is one superseded provisional value of the SAME transaction,
// kept in the intent's history so a savepoint rollback can restore it.
type IntentValue struct {
	Sequence  int32  `json:"seq"`
	Value     []byte `json:"value,omitempty"`
	Tombstone bool   `json:"tombstone,omitempty"`
}

// MVCCMetadata is the value stored at an intent's metadata key.
type MVCCMetadata struct {
	Txn TxnMeta `json:"txn"`
	// Timestamp is the timestamp of the provisional value this intent
	// protects (== Txn.WriteTimestamp at the time the intent was written).
	Timestamp hlc.Timestamp `json:"ts"`
	// History holds the transaction's own SUPERSEDED provisional values for
	// this key, oldest first (same epoch only; a new epoch clears it).
	// Consulted exclusively by savepoint rollback.
	History []IntentValue `json:"history,omitempty"`
}
