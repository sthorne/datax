package kvserver

import (
	"encoding/json"
	"fmt"
	"math"
	"strconv"

	"go.etcd.io/raft/v3/raftpb"

	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/kvpb"
	"github.com/sthorne/datax/pkg/storage"
	"github.com/sthorne/datax/pkg/storage/enginepb"
	"github.com/sthorne/datax/pkg/util/hlc"
	"github.com/sthorne/datax/pkg/util/log"
)

var maxTimestamp = hlc.Timestamp{WallTime: math.MaxInt64, Logical: math.MaxInt32}

func addrOf(k keys.Key) (keys.Key, error) { return keys.Addr(k) }

// applyEntry applies one committed Raft entry. Application is idempotent:
// entries at or below the persisted applied index are skipped, which makes
// crash-recovery replay safe.
func (r *Replica) applyEntry(ent raftpb.Entry) error {
	r.mu.Lock()
	applied := r.mu.appliedIndex
	r.mu.Unlock()
	if ent.Index <= applied {
		return nil
	}

	switch ent.Type {
	case raftpb.EntryConfChange:
		var cc raftpb.ConfChange
		if err := cc.Unmarshal(ent.Data); err != nil {
			return fmt.Errorf("corrupt conf change: %w", err)
		}
		state := r.node.ApplyConfChange(cc)
		r.rs.setConfStateRaw(*state)

		// Membership-change conf changes carry the new descriptor; adopt it
		// atomically with the applied index. (Bootstrap conf changes from
		// StartNode carry no context.)
		var newDesc *kvpb.RangeDescriptor
		if len(cc.Context) > 0 {
			var ccCtx confChangeContext
			if err := json.Unmarshal(cc.Context, &ccCtx); err != nil {
				return fmt.Errorf("corrupt conf change context: %w", err)
			}
			newDesc = &ccCtx.Desc
		}
		b := r.store.cfg.Engine.NewBatch()
		if newDesc != nil {
			if err := PutRangeDescriptor(b, *newDesc); err != nil {
				_ = b.Close()
				return err
			}
		}
		if err := r.stageAppliedIndex(b, ent.Index); err != nil {
			_ = b.Close()
			return err
		}
		if err := b.Commit(false); err != nil {
			return err
		}
		if newDesc != nil {
			r.mu.Lock()
			r.mu.desc = *newDesc
			r.mu.Unlock()
		}
		r.setApplied(ent.Index)

		if cc.Type == raftpb.ConfChangeRemoveNode && newDesc != nil {
			if _, stillMember := newDesc.GetReplica(r.store.cfg.NodeID); !stillMember {
				return errReplicaRemoved
			}
		}
		return nil

	case raftpb.EntryNormal:
		if len(ent.Data) == 0 {
			// Leader no-op entry at term start.
			if err := r.persistAppliedIndex(ent.Index); err != nil {
				return err
			}
			r.setApplied(ent.Index)
			return nil
		}
		var cmd raftCommand
		if err := json.Unmarshal(ent.Data, &cmd); err != nil {
			return fmt.Errorf("corrupt raft command at index %d: %w", ent.Index, err)
		}
		resp, aerr := r.applyCommand(&cmd, ent.Index)

		// Deliver the outcome to a local waiter, if this replica proposed it.
		r.mu.Lock()
		ch, ok := r.mu.proposals[cmd.ID]
		if ok {
			delete(r.mu.proposals, cmd.ID)
		}
		r.mu.Unlock()
		if ok {
			ch <- proposalResult{resp: resp, err: aerr}
		} else if aerr != nil {
			log.Debugf("%s: applied command %s with error (no waiter): %v", r.rangeID, cmd.ID, aerr)
		}
		return nil

	default:
		return fmt.Errorf("unhandled entry type %v", ent.Type)
	}
}

func (r *Replica) persistAppliedIndex(idx uint64) error {
	b := r.store.cfg.Engine.NewBatch()
	if err := r.stageAppliedIndex(b, idx); err != nil {
		_ = b.Close()
		return err
	}
	return b.Commit(false)
}

func (r *Replica) stageAppliedIndex(b *storage.Batch, idx uint64) error {
	tr := r.rs.truncatedState()
	return putReplicaState(b, r.rangeID, replicaState{
		AppliedIndex:   idx,
		TruncatedIndex: tr.Index,
		TruncatedTerm:  tr.Term,
	})
}

// applyCommand evaluates a replicated write batch. The MVCC effects and the
// applied-index advance land in ONE engine batch — atomic, so replay after a
// crash cannot double-apply. Evaluation is deterministic (pure function of
// the state machine at this log position), so every replica computes the
// same result, including errors: on error the command's writes are discarded
// and only the applied index advances.
func (r *Replica) applyCommand(cmd *raftCommand, idx uint64) (*kvpb.BatchResponse, *kvpb.Error) {
	eng := r.store.cfg.Engine
	b := eng.NewBatch()
	resp, aerr := r.evalWriteBatch(b, &cmd.Batch)
	if aerr != nil {
		_ = b.Close()
		b = eng.NewBatch()
	}
	if aerr == nil && cmd.Split != nil {
		if err := r.stageSplit(b, cmd.Split); err != nil {
			log.Errorf("%s: split application failed: %v", r.rangeID, err)
			_ = b.Close()
			b = eng.NewBatch()
			aerr = kvpb.NewError(err)
		}
	}
	if err := r.stageAppliedIndex(b, idx); err != nil {
		_ = b.Close()
		return nil, kvpb.NewError(err)
	}
	if err := b.Commit(false); err != nil {
		// Failing to commit the state machine is not recoverable.
		log.Fatalf("%s: applying entry %d: %v", r.rangeID, idx, err)
	}
	r.setApplied(idx)
	if aerr == nil && cmd.Split != nil {
		r.finishSplit(cmd.Split)
	}
	return resp, aerr
}

// evalWriteBatch executes the batch's requests against the engine batch.
func (r *Replica) evalWriteBatch(b *storage.Batch, ba *kvpb.BatchRequest) (*kvpb.BatchResponse, *kvpb.Error) {
	br := &kvpb.BatchResponse{Txn: ba.Header.Txn, Timestamp: ba.Header.Timestamp}
	var txnMeta *enginepb.TxnMeta
	if ba.Header.Txn != nil {
		txnMeta = &ba.Header.Txn.TxnMeta
	}
	ts := writeTimestamp(ba)

	if ba.Header.CreateTxnRecord {
		if err := r.createTxnRecord(b, ba.Header.Txn); err != nil {
			return nil, err
		}
	}

	for i := range ba.Requests {
		var ru kvpb.ResponseUnion
		switch req := ba.Requests[i].GetInner().(type) {
		case *kvpb.PutRequest:
			if err := storage.MVCCPut(b, req.Key, ts, req.Value, txnMeta); err != nil {
				return nil, kvpb.NewError(err)
			}
			ru.Put = &kvpb.PutResponse{}
		case *kvpb.DeleteRequest:
			if err := storage.MVCCDelete(b, req.Key, ts, txnMeta); err != nil {
				return nil, kvpb.NewError(err)
			}
			ru.Delete = &kvpb.DeleteResponse{}
		case *kvpb.IncrementRequest:
			// Increments are serialized by Raft: read the latest value, not
			// a snapshot — that is what makes them atomic counters.
			cur, err := storage.MVCCGet(b, req.Key, maxTimestamp, storage.MVCCGetOptions{Txn: txnMeta})
			if err != nil {
				return nil, kvpb.NewError(err)
			}
			var v int64
			if cur != nil {
				v, err = strconv.ParseInt(string(cur), 10, 64)
				if err != nil {
					return nil, kvpb.NewErrorf("increment on non-numeric value at %s", req.Key)
				}
			}
			v += req.By
			if err := storage.MVCCPut(b, req.Key, ts, []byte(strconv.FormatInt(v, 10)), txnMeta); err != nil {
				return nil, kvpb.NewError(err)
			}
			ru.Increment = &kvpb.IncrementResponse{NewValue: v}
		case *kvpb.GetRequest:
			val, err := storage.MVCCGet(b, req.Key, readTimestamp(ba), storage.MVCCGetOptions{Txn: txnMeta})
			if err != nil {
				return nil, kvpb.NewError(err)
			}
			ru.Get = &kvpb.GetResponse{Value: val}
		case *kvpb.ScanRequest:
			res, err := storage.MVCCScan(b, req.Key, req.EndKey, readTimestamp(ba), req.MaxRows, storage.MVCCGetOptions{Txn: txnMeta})
			if err != nil {
				return nil, kvpb.NewError(err)
			}
			ru.Scan = scanResponse(res)
		case *kvpb.EndTxnRequest:
			resp, err := r.evalEndTxn(b, ba.Header.Txn, req)
			if err != nil {
				return nil, err
			}
			ru.EndTxn = resp
		case *kvpb.HeartbeatTxnRequest:
			resp, err := r.evalHeartbeatTxn(b, ba.Header.Txn, req)
			if err != nil {
				return nil, err
			}
			ru.HeartbeatTxn = resp
		case *kvpb.PushTxnRequest:
			resp, err := r.evalPushTxn(b, req)
			if err != nil {
				return nil, err
			}
			ru.PushTxn = resp
		case *kvpb.ResolveIntentRequest:
			if err := storage.MVCCResolveIntent(b, req.Key, req.TxnID, req.Status, req.CommitTS); err != nil {
				return nil, kvpb.NewError(err)
			}
			ru.ResolveIntent = &kvpb.ResolveIntentResponse{}
		default:
			return nil, kvpb.NewErrorf("unsupported request in write batch: %T", ba.Requests[i].GetInner())
		}
		br.Responses = append(br.Responses, ru)
	}
	return br, nil
}

// evalReadOnly serves a read-only batch directly from the engine (after the
// ReadIndex dance made it linearizable).
func (r *Replica) evalReadOnly(ba *kvpb.BatchRequest) (*kvpb.BatchResponse, *kvpb.Error) {
	br := &kvpb.BatchResponse{Txn: ba.Header.Txn, Timestamp: ba.Header.Timestamp}
	opts := storage.MVCCGetOptions{Inconsistent: ba.Header.ReadInconsistent}
	ts := readTimestamp(ba)
	if ba.Header.Txn != nil {
		opts.Txn = &ba.Header.Txn.TxnMeta
		opts.UncertaintyLimit = ba.Header.Txn.ReadTimestamp.AddNanos(int64(r.store.cfg.Clock.MaxOffset()))
	}
	eng := r.store.cfg.Engine
	for i := range ba.Requests {
		var ru kvpb.ResponseUnion
		switch req := ba.Requests[i].GetInner().(type) {
		case *kvpb.GetRequest:
			val, err := storage.MVCCGet(eng, req.Key, ts, opts)
			if err != nil {
				return nil, kvpb.NewError(err)
			}
			ru.Get = &kvpb.GetResponse{Value: val}
		case *kvpb.ScanRequest:
			res, err := storage.MVCCScan(eng, req.Key, req.EndKey, ts, req.MaxRows, opts)
			if err != nil {
				return nil, kvpb.NewError(err)
			}
			ru.Scan = scanResponse(res)
		default:
			return nil, kvpb.NewErrorf("non-read request in read-only batch: %T", ba.Requests[i].GetInner())
		}
		br.Responses = append(br.Responses, ru)
	}
	return br, nil
}

func scanResponse(res storage.ScanResult) *kvpb.ScanResponse {
	out := &kvpb.ScanResponse{Resume: res.Resume}
	for _, kv := range res.KVs {
		out.Rows = append(out.Rows, kvpb.KeyValue{Key: kv.Key, Value: kv.Value})
	}
	return out
}
