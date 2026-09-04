package kvserver

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"go.etcd.io/raft/v3/raftpb"

	"github.com/sthorne/datax/pkg/base"
	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/kvpb"
	"github.com/sthorne/datax/pkg/storage"
	"github.com/sthorne/datax/pkg/util/hlc"
	"github.com/sthorne/datax/pkg/util/log"
)

// Replica membership changes ("preseed then ConfChange"):
//
//  1. The leader streams a snapshot of the range — its data span, its
//     range-local addressed keys (transaction records), the applied index
//     and matching log term, all captured from ONE consistent engine
//     snapshot — to the target node, which creates the new replica.
//  2. The leader proposes a Raft ConfChange whose context carries the
//     updated descriptor; at apply time every replica persists the new
//     descriptor (a removed replica destroys itself).
//  3. The leader repairs the /meta addressing record.
//
// Seeding before the ConfChange avoids the classic stall where a new voter
// cannot vote until it receives a snapshot through Raft itself.

// SnapshotKV is one raw engine key/value pair in a range snapshot stream.
type SnapshotKV struct {
	Key, Value []byte
}

// SnapshotSender streams a range snapshot to another node (implemented by
// pkg/rpc).
type SnapshotSender interface {
	SendSnapshot(ctx context.Context, to base.NodeID, header []byte, next func() ([]SnapshotKV, error)) error
}

// snapshotHeader is the JSON header of a snapshot stream.
type snapshotHeader struct {
	Desc         kvpb.RangeDescriptor `json:"desc"`
	ReplicaID    base.ReplicaID       `json:"replica_id"`
	AppliedIndex uint64               `json:"applied_index"`
	Term         uint64               `json:"term"`
	GCThreshold  hlc.Timestamp        `json:"gc_threshold,omitempty"`
	SizeBytes    int64                `json:"size_bytes,omitempty"`
}

// adminChangeReplicas executes an AdminChangeReplicasRequest on the leader.
func (r *Replica) adminChangeReplicas(ctx context.Context, req *kvpb.AdminChangeReplicasRequest) (*kvpb.AdminChangeReplicasResponse, *kvpb.Error) {
	sender := r.store.getSender()
	if sender == nil {
		return nil, kvpb.NewErrorf("store has no client; cannot change replicas")
	}
	if r.isFrozen() {
		return nil, kvpb.NewErrorf("%s: cannot change replicas: range is frozen for a merge", r.rangeID)
	}
	desc := r.Desc()

	if req.AddNode != 0 {
		if _, ok := desc.GetReplica(req.AddNode); ok {
			return nil, kvpb.NewErrorf("%s: node %s already has a replica", r.rangeID, req.AddNode)
		}
		newReplicaID := desc.NextReplicaID
		newDesc := desc
		newDesc.Replicas = append(append([]kvpb.ReplicaDescriptor(nil), desc.Replicas...), kvpb.ReplicaDescriptor{
			NodeID: req.AddNode, StoreID: base.StoreID(req.AddNode), ReplicaID: newReplicaID,
		})
		newDesc.NextReplicaID = desc.NextReplicaID + 1
		newDesc.Generation = desc.Generation + 1

		if err := r.sendSnapshotTo(ctx, req.AddNode, newDesc, newReplicaID); err != nil {
			return nil, kvpb.NewErrorf("%s: preseeding n%d: %v", r.rangeID, req.AddNode, err)
		}
		if err := r.proposeConfChange(ctx, raftpb.ConfChangeAddNode, newReplicaID, newDesc); err != nil {
			return nil, err
		}
		desc = newDesc
	}

	if req.RemoveNode != 0 {
		rep, ok := desc.GetReplica(req.RemoveNode)
		if !ok {
			return nil, kvpb.NewErrorf("%s: node %s has no replica to remove", r.rangeID, req.RemoveNode)
		}
		if req.RemoveNode == r.store.cfg.NodeID {
			return nil, kvpb.NewErrorf("%s: refusing to remove the leader's own replica; transfer leadership first", r.rangeID)
		}
		newDesc := desc
		newDesc.Replicas = nil
		for _, rd := range desc.Replicas {
			if rd.NodeID != req.RemoveNode {
				newDesc.Replicas = append(newDesc.Replicas, rd)
			}
		}
		newDesc.Generation = desc.Generation + 1
		if err := r.proposeConfChange(ctx, raftpb.ConfChangeRemoveNode, rep.ReplicaID, newDesc); err != nil {
			return nil, err
		}
		desc = newDesc
	}

	if err := putMetaRecords(ctx, sender, r.store.cfg.Clock.Now(), r.store.orderedMetaUpdates(), desc); err != nil {
		log.Warnf("%s: replica change committed but meta update failed: %v", r.rangeID, err)
	}
	return &kvpb.AdminChangeReplicasResponse{Desc: desc}, nil
}

// confChangeContext travels inside the raft ConfChange and carries the
// descriptor every replica must adopt at apply time.
type confChangeContext struct {
	Desc kvpb.RangeDescriptor `json:"desc"`
}

func (r *Replica) proposeConfChange(ctx context.Context, typ raftpb.ConfChangeType, replicaID base.ReplicaID, newDesc kvpb.RangeDescriptor) *kvpb.Error {
	ctxJSON, err := json.Marshal(confChangeContext{Desc: newDesc})
	if err != nil {
		return kvpb.NewError(err)
	}
	cc := raftpb.ConfChange{Type: typ, NodeID: uint64(replicaID), Context: ctxJSON}
	if err := r.node.ProposeConfChange(ctx, cc); err != nil {
		return kvpb.NewError(err)
	}
	// Wait for the change to apply locally (the descriptor generation is
	// bumped by the apply path).
	for {
		if r.Desc().Generation >= newDesc.Generation {
			return nil
		}
		select {
		case <-ctx.Done():
			return kvpb.NewErrorf("%s: conf change did not apply: %v", r.rangeID, ctx.Err())
		case <-time.After(20 * time.Millisecond):
		}
	}
}

const snapshotChunkSize = 256

// sendSnapshotTo captures a consistent snapshot of the range and streams it
// to the target node (the preseed path for new replicas).
func (r *Replica) sendSnapshotTo(ctx context.Context, target base.NodeID, newDesc kvpb.RangeDescriptor, newReplicaID base.ReplicaID) error {
	_, err := r.streamSnapshot(ctx, target, newDesc, newReplicaID)
	return err
}

// streamSnapshot captures a consistent engine snapshot of the range and
// streams its content (transaction records + MVCC data) to the target,
// returning the header it sent. While the stream is in flight, the
// replica's log truncation holds back so the receiver can still be caught
// up from the applied index the stream carries.
func (r *Replica) streamSnapshot(ctx context.Context, target base.NodeID, newDesc kvpb.RangeDescriptor, newReplicaID base.ReplicaID) (snapshotHeader, error) {
	sender := r.store.cfg.SnapshotSender
	if sender == nil {
		return snapshotHeader{}, fmt.Errorf("no snapshot sender configured")
	}
	snap := r.store.cfg.Engine.NewSnapshot()
	defer func() { _ = snap.Close() }()

	st, err := loadReplicaStateFrom(snap, r.rangeID)
	if err != nil {
		return snapshotHeader{}, err
	}
	r.mu.Lock()
	r.mu.snapInFlight[uint64(newReplicaID)] = st.AppliedIndex
	r.mu.Unlock()
	defer func() {
		r.mu.Lock()
		delete(r.mu.snapInFlight, uint64(newReplicaID))
		r.mu.Unlock()
	}()
	var term uint64
	if st.AppliedIndex > 0 {
		term, err = r.rs.Term(st.AppliedIndex)
		if err != nil {
			return snapshotHeader{}, fmt.Errorf("term of applied index %d: %w", st.AppliedIndex, err)
		}
	}
	h := snapshotHeader{
		Desc:         newDesc,
		ReplicaID:    newReplicaID,
		AppliedIndex: st.AppliedIndex,
		Term:         term,
		GCThreshold:  st.GCThreshold,
		SizeBytes:    st.SizeBytes,
	}
	header, err := json.Marshal(h)
	if err != nil {
		return snapshotHeader{}, err
	}

	// The range's replicated content: its MVCC data span and its
	// range-local addressed keys (transaction records).
	type span struct{ lower, upper []byte }
	loLocal, hiLocal := keys.RangeLocalAddressedSpan(newDesc.StartKey, newDesc.EndKey)
	spans := []span{
		{[]byte(loLocal), []byte(hiLocal)},
		{storage.EncodeMVCCKey(newDesc.StartKey, hlc.Timestamp{}), storage.EncodeMVCCKey(newDesc.EndKey, hlc.Timestamp{})},
	}
	spanIdx := 0
	var it storage.Iterator
	var valid bool
	defer func() {
		if it != nil {
			_ = it.Close()
		}
	}()
	next := func() ([]SnapshotKV, error) {
		var out []SnapshotKV
		for len(out) < snapshotChunkSize {
			if it == nil {
				if spanIdx >= len(spans) {
					return out, nil
				}
				it = snap.NewIter(spans[spanIdx].lower, spans[spanIdx].upper)
				valid = it.SeekGE(spans[spanIdx].lower)
				spanIdx++
			}
			for valid && len(out) < snapshotChunkSize {
				out = append(out, SnapshotKV{
					Key:   append([]byte(nil), it.Key()...),
					Value: append([]byte(nil), it.Value()...),
				})
				valid = it.Next()
			}
			if !valid {
				if err := it.Close(); err != nil {
					it = nil
					return nil, err
				}
				it = nil
			}
		}
		return out, nil
	}
	if err := sender.SendSnapshot(ctx, target, header, next); err != nil {
		return snapshotHeader{}, err
	}
	return h, nil
}

// ApplySnapshotStream is the receiving side of a snapshot stream: it
// materializes a NEW replica (the preseed path), or replaces an EXISTING
// replica's state machine (a raft catch-up snapshot for a follower that
// fell behind the leader's truncated log). next returns nil when the
// stream ends.
func (s *Store) ApplySnapshotStream(header []byte, next func() ([]SnapshotKV, error)) error {
	var h snapshotHeader
	if err := json.Unmarshal(header, &h); err != nil {
		return fmt.Errorf("corrupt snapshot header: %w", err)
	}
	if r, ok := s.GetReplica(h.Desc.RangeID); ok {
		return s.installSnapshot(r, h, next)
	}
	b := s.cfg.Engine.NewBatch()
	count := 0
	for {
		kvs, err := next()
		if err != nil {
			_ = b.Close()
			return err
		}
		if len(kvs) == 0 {
			break
		}
		for _, kv := range kvs {
			if err := b.Put(kv.Key, kv.Value); err != nil {
				_ = b.Close()
				return err
			}
			count++
		}
	}
	if err := putReplicaState(b, h.Desc.RangeID, replicaState{
		AppliedIndex:   h.AppliedIndex,
		TruncatedIndex: h.AppliedIndex,
		TruncatedTerm:  h.Term,
		GCThreshold:    h.GCThreshold,
		SizeBytes:      h.SizeBytes,
	}); err != nil {
		_ = b.Close()
		return err
	}
	if err := PutRangeDescriptor(b, h.Desc); err != nil {
		_ = b.Close()
		return err
	}
	if err := b.Commit(true); err != nil {
		return err
	}
	if _, err := s.startReplica(h.Desc, h.ReplicaID, false); err != nil {
		return err
	}
	log.Infof("%s: replica %d seeded from snapshot (%d keys, applied index %d)",
		h.Desc.RangeID, h.ReplicaID, count, h.AppliedIndex)
	s.cfg.Events.Record("snapshot", "%s: replica %d seeded from snapshot (%d keys, applied index %d)", h.Desc.RangeID, h.ReplicaID, count, h.AppliedIndex)
	return nil
}

// removeReplica destroys a replica that was removed from its range,
// wiping its replicated data (the store no longer serves this span — any
// sibling replicas on this store cover disjoint spans), its transaction
// records, and its unreplicated Raft state. A tombstone prevents revival
// via LoadReplicas.
func (s *Store) removeReplica(rangeID base.RangeID, desc kvpb.RangeDescriptor) {
	s.mu.Lock()
	delete(s.mu.replicas, rangeID)
	s.mu.Unlock()
	b := s.cfg.Engine.NewBatch()
	_ = b.DeleteRange(storage.EncodeMVCCKey(desc.StartKey, hlc.Timestamp{}), storage.EncodeMVCCKey(desc.EndKey, hlc.Timestamp{}))
	loL, hiL := keys.RangeLocalAddressedSpan(desc.StartKey, desc.EndKey)
	_ = b.DeleteRange(loL, hiL)
	pre := keys.RangeUnreplicatedPrefix(rangeID)
	_ = b.DeleteRange(pre, pre.PrefixEnd())
	_ = b.Put(keys.RangeTombstoneKey(rangeID), []byte("removed"))
	if err := b.Commit(true); err != nil {
		log.Warnf("%s: removing replica state: %v", rangeID, err)
	}
	log.Infof("%s: replica and its data removed from this store", rangeID)
	s.cfg.Events.Record("replica-removed", "%s: replica and its data removed from this store", rangeID)
}
