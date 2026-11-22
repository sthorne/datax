package kvclient

import (
	"testing"

	"github.com/google/uuid"

	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/kvpb"
	"github.com/sthorne/datax/pkg/storage"
	"github.com/sthorne/datax/pkg/storage/enginepb"
)

// TestStagedWritePresent: status recovery's probe counts an intent of the
// staged transaction as its in-flight write only at the staged epoch and
// at the staged sequence or above. An earlier write of the transaction to
// the same key (an earlier statement, or an earlier epoch) is not the
// staged write. A push from a leaseholder that predates the staged
// fields (StagedSequence 0) falls back to matching the ID, as before.
func TestStagedWritePresent(t *testing.T) {
	id, other := uuid.New(), uuid.New()
	intent := func(txn uuid.UUID, epoch, seq int32) []storage.Intent {
		return []storage.Intent{{Key: keys.Key("k"), Txn: enginepb.TxnMeta{ID: txn, Epoch: epoch, Sequence: seq}}}
	}
	staged := &kvpb.PushTxnResponse{Status: enginepb.STAGING, StagedEpoch: 1, StagedSequence: 5}
	legacy := &kvpb.PushTxnResponse{Status: enginepb.STAGING}
	for _, tt := range []struct {
		name    string
		intents []storage.Intent
		push    *kvpb.PushTxnResponse
		want    bool
	}{
		{"staged write", intent(id, 1, 5), staged, true},
		{"later sequence", intent(id, 1, 6), staged, true},
		{"earlier statement", intent(id, 1, 4), staged, false},
		{"earlier epoch", intent(id, 0, 5), staged, false},
		{"later epoch", intent(id, 2, 5), staged, false},
		{"another transaction", intent(other, 1, 5), staged, false},
		{"legacy leaseholder", intent(id, 0, 1), legacy, true},
		{"legacy leaseholder, another transaction", intent(other, 0, 1), legacy, false},
		{"one of several", append(intent(other, 0, 9), intent(id, 1, 5)...), staged, true},
	} {
		if got := stagedWritePresent(tt.intents, id, tt.push); got != tt.want {
			t.Errorf("%s: present = %v, want %v", tt.name, got, tt.want)
		}
	}
}
