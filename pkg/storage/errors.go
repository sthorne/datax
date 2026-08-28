package storage

import (
	"fmt"

	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/storage/enginepb"
	"github.com/sthorne/datax/pkg/util/hlc"
)

// Intent describes a write intent encountered by a reader or writer.
type Intent struct {
	Key keys.Key         `json:"key"`
	Txn enginepb.TxnMeta `json:"txn"`
}

// WriteIntentError: the operation ran into intents owned by other
// transactions. The caller must push those transactions and retry.
type WriteIntentError struct {
	Intents []Intent
}

func (e *WriteIntentError) Error() string {
	return fmt.Sprintf("conflicting write intents (%d) starting at key %s", len(e.Intents), e.Intents[0].Key)
}

// WriteTooOldError: a write at Timestamp found a committed version at or
// above it. The transaction must restart at least at ActualTimestamp.
type WriteTooOldError struct {
	Timestamp       hlc.Timestamp // the attempted write timestamp
	ActualTimestamp hlc.Timestamp // smallest timestamp the write could succeed at
}

func (e *WriteTooOldError) Error() string {
	return fmt.Sprintf("write at %s too old; try at least %s", e.Timestamp, e.ActualTimestamp)
}

// UncertaintyError: a read at ReadTimestamp saw a value in its uncertainty
// window (ReadTimestamp, ExistingTimestamp]. The transaction must restart at
// or above ExistingTimestamp.
type UncertaintyError struct {
	ReadTimestamp     hlc.Timestamp
	ExistingTimestamp hlc.Timestamp
}

func (e *UncertaintyError) Error() string {
	return fmt.Sprintf("read at %s within uncertainty interval of value at %s", e.ReadTimestamp, e.ExistingTimestamp)
}
