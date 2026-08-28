package kvserver

import (
	"context"
	"encoding/json"
	"fmt"

	"go.etcd.io/raft/v3/raftpb"

	"github.com/sthorne/datax/pkg/base"
	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/kvpb"
	"github.com/sthorne/datax/pkg/storage"
	"github.com/sthorne/datax/pkg/util/hlc"
	"github.com/sthorne/datax/pkg/util/log"
)

// Splits. The split itself is a replicated command on the left-hand range:
// at apply time every replica atomically writes both descriptors, and after
// commit starts the right-hand side's Raft group locally (all RHS replicas
// live on the same stores as the LHS's, and no data moves — range bounds
// are logical). The leader then repairs the /meta addressing records; until
// that lands, stale routing is corrected by RangeKeyMismatch errors that
// carry fresh descriptors.

// adminSplit executes an AdminSplitRequest on the leader.
func (r *Replica) adminSplit(ctx context.Context, splitKey keys.Key) (*kvpb.AdminSplitResponse, *kvpb.Error) {
	desc := r.Desc()
	if splitKey.Equal(desc.StartKey) {
		return nil, kvpb.NewErrorf("%s: split key %s is already a range boundary", r.rangeID, splitKey)
	}
	if !desc.ContainsKey(splitKey) {
		e := kvpb.NewErrorf("%s: split key %s outside range", r.rangeID, splitKey)
		e.RangeKeyMismatch = &kvpb.RangeKeyMismatchError{RequestKey: splitKey, ActualDescriptors: []kvpb.RangeDescriptor{desc}}
		return nil, e
	}
	sender := r.store.getSender()
	if sender == nil {
		return nil, kvpb.NewErrorf("store has no client; cannot split")
	}

	// Allocate the new range ID through the replicated counter.
	inc := &kvpb.BatchRequest{Header: kvpb.BatchHeader{Timestamp: r.store.cfg.Clock.Now()}}
	inc.Add(&kvpb.IncrementRequest{RequestHeader: kvpb.RequestHeader{Key: keys.RangeIDGenKey()}, By: 1})
	ibr, kerr := sender.Send(ctx, inc)
	if kerr != nil {
		return nil, kerr
	}
	newID := base.RangeID(ibr.Responses[0].Increment.NewValue)

	left := desc
	left.StartKey = desc.StartKey.Clone()
	left.EndKey = splitKey.Clone()
	left.Generation = desc.Generation + 1
	right := kvpb.RangeDescriptor{
		RangeID:       newID,
		StartKey:      splitKey.Clone(),
		EndKey:        desc.EndKey.Clone(),
		Replicas:      append([]kvpb.ReplicaDescriptor(nil), desc.Replicas...),
		NextReplicaID: desc.NextReplicaID,
		Generation:    desc.Generation + 1,
	}
	trig := &splitTrigger{Left: left, Right: right}

	// Serialize against writes: nothing may be in flight through the old
	// bounds while the split lands.
	r.latch.Lock()
	if _, kerr := r.proposeCmd(ctx, &kvpb.BatchRequest{Header: kvpb.BatchHeader{RangeID: r.rangeID}}, trig); kerr != nil {
		r.latch.Unlock()
		return nil, kerr
	}
	r.latch.Unlock()

	// Repair the addressing records (one batch, both on the meta range, so
	// the update is atomic). Done outside the latch: if the split was on
	// the meta-carrying range itself this would deadlock inside it.
	if err := putMetaRecords(ctx, sender, r.store.cfg.Clock.Now(), left, right); err != nil {
		// Routing still works via mismatch corrections; log and continue.
		log.Warnf("%s: split committed but meta update failed: %v", r.rangeID, err)
	}
	return &kvpb.AdminSplitResponse{Left: left, Right: right}, nil
}

// putMetaRecords writes the /meta addressing records for the given
// descriptors in one atomic batch (all meta keys live on the meta range).
func putMetaRecords(ctx context.Context, sender Sender, now hlc.Timestamp, descs ...kvpb.RangeDescriptor) error {
	ba := &kvpb.BatchRequest{Header: kvpb.BatchHeader{Timestamp: now}}
	for _, d := range descs {
		raw, err := json.Marshal(d)
		if err != nil {
			return err
		}
		ba.Add(&kvpb.PutRequest{RequestHeader: kvpb.RequestHeader{Key: keys.RangeMetaKey(d.EndKey)}, Value: raw})
	}
	if _, kerr := sender.Send(ctx, ba); kerr != nil {
		return kerr
	}
	return nil
}

func (r *Replica) stageSplit(b *storage.Batch, trig *splitTrigger) error {
	if err := PutRangeDescriptor(b, trig.Left); err != nil {
		return err
	}
	return PutRangeDescriptor(b, trig.Right)
}

func (r *Replica) finishSplit(trig *splitTrigger) {
	r.mu.Lock()
	r.mu.desc = trig.Left
	r.mu.Unlock()
	r.rs.setConfState(trig.Left)

	rep, ok := trig.Right.GetReplica(r.store.cfg.NodeID)
	if !ok {
		return
	}
	if _, err := r.store.startReplica(trig.Right, rep.ReplicaID, true /* bootstrap */); err != nil {
		log.Errorf("%s: starting RHS replica after split: %v", trig.Right.RangeID, err)
		return
	}
	log.Infof("split %s at %s → %s [%s, %s)", r.rangeID, trig.Left.EndKey, trig.Right.RangeID, trig.Right.StartKey, trig.Right.EndKey)
}

func (r *Replica) applySnapshot(snap raftpb.Snapshot) error {
	return fmt.Errorf("incoming raft snapshots not implemented yet")
}

func (r *Replica) maybeCompleteConfChange(cc raftpb.ConfChange) {}
