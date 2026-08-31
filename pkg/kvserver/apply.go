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
// crash-recovery replay safe — and also lets a catch-up snapshot install
// (which raises the applied index under applyMu) turn any entries it
// superseded into no-ops.
func (r *Replica) applyEntry(ent raftpb.Entry) error {
	r.applyMu.Lock()
	defer r.applyMu.Unlock()
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
		cmd, err := decodeRaftCommand(ent.Data)
		if err != nil {
			return fmt.Errorf("corrupt raft command at index %d: %w", ent.Index, err)
		}
		resp, aerr := r.applyCommand(cmd, ent.Index)

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
	r.mu.Lock()
	thr := r.mu.gcThreshold
	size := r.mu.sizeBytes
	frozen := r.mu.frozen
	mergedInto := r.mu.mergedInto
	closedTS := r.mu.closedTS
	r.mu.Unlock()
	return putReplicaState(b, r.rangeID, replicaState{
		AppliedIndex:   idx,
		TruncatedIndex: tr.Index,
		TruncatedTerm:  tr.Term,
		GCThreshold:    thr,
		SizeBytes:      size,
		Frozen:         frozen,
		MergedInto:     mergedInto,
		ClosedTS:       closedTS,
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
	if aerr == nil {
		// A GC command raises the replicated threshold atomically with its
		// deletions (stageAppliedIndex persists it below).
		var newThr hlc.Timestamp
		for _, u := range cmd.Batch.Requests {
			if u.GC != nil {
				newThr = newThr.Forward(u.GC.Threshold)
			}
		}
		if !newThr.IsEmpty() {
			r.mu.Lock()
			if r.mu.gcThreshold.Less(newThr) {
				r.mu.gcThreshold = newThr
			}
			r.mu.Unlock()
		}
		// A closed-timestamp publication: by log order, every write at or
		// below it has already applied on this replica — from here on,
		// reads at or below it are servable locally (stageAppliedIndex
		// persists it below).
		if !cmd.ClosedTS.IsEmpty() {
			r.mu.Lock()
			if r.mu.closedTS.Less(cmd.ClosedTS) {
				r.mu.closedTS = cmd.ClosedTS
			}
			r.mu.Unlock()
		}
		// Size accounting: a pure function of the command, so every replica
		// stays in agreement (stageAppliedIndex persists it below).
		if delta := commandSizeDelta(&cmd.Batch); delta != 0 {
			r.mu.Lock()
			r.mu.sizeBytes += delta
			if r.mu.sizeBytes < 0 {
				r.mu.sizeBytes = 0
			}
			r.mu.Unlock()
		}
	}
	if aerr == nil && cmd.Split != nil {
		if err := r.stageSplit(b, cmd.Split); err != nil {
			log.Errorf("%s: split application failed: %v", r.rangeID, err)
			_ = b.Close()
			b = eng.NewBatch()
			aerr = kvpb.NewError(err)
		}
	}
	if aerr == nil && cmd.Merge != nil {
		if err := r.stageMerge(b, cmd.Merge); err != nil {
			log.Warnf("%s: merge application refused: %v", r.rangeID, err)
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
	if aerr == nil && cmd.Merge != nil {
		r.finishMerge(cmd.Merge)
	}
	if cmd.Load != nil {
		r.mu.Lock()
		r.mu.loadHandoff = cmd.Load
		r.mu.Unlock()
	}
	if cmd.Checksum != nil {
		// Post-commit, still under applyMu: the engine snapshot taken here
		// is exactly this entry's state on every replica.
		r.startChecksum(cmd.Checksum.ID, idx)
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
			if req.ForUpdate {
				// Locking read: pin the observed state with an intent. The
				// lock's stale-snapshot check (any version above readTS →
				// WriteTooOld) subsumes uncertainty, and a foreign intent —
				// even one the read looked beneath — conflicts here.
				if txnMeta == nil {
					return nil, kvpb.NewErrorf("locking read outside a transaction")
				}
				if err := storage.MVCCLock(b, req.Key, readTimestamp(ba), val, txnMeta); err != nil {
					return nil, kvpb.NewError(err)
				}
			}
			ru.Get = &kvpb.GetResponse{Value: val}
		case *kvpb.ScanRequest:
			res, err := storage.MVCCScan(b, req.Key, req.EndKey, readTimestamp(ba), req.MaxRows, storage.MVCCGetOptions{Txn: txnMeta})
			if err != nil {
				return nil, kvpb.NewError(err)
			}
			if req.ForUpdate {
				if txnMeta == nil {
					return nil, kvpb.NewErrorf("locking scan outside a transaction")
				}
				for _, kv := range res.KVs {
					if err := storage.MVCCLock(b, kv.Key, readTimestamp(ba), kv.Value, txnMeta); err != nil {
						return nil, kvpb.NewError(err)
					}
				}
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
		case *kvpb.RollbackIntentRequest:
			if err := storage.MVCCRollbackIntent(b, req.Key, req.TxnID, req.Sequence); err != nil {
				return nil, kvpb.NewError(err)
			}
			ru.RollbackIntent = &kvpb.RollbackIntentResponse{}
		case *kvpb.RecoverTxnRequest:
			resp, rerr := r.evalRecoverTxn(b, req)
			if rerr != nil {
				return nil, rerr
			}
			ru.RecoverTxn = resp
		case *kvpb.GCRequest:
			// Delete exactly the versions the leader enumerated (all
			// superseded below the threshold, hence immutable) and the
			// finalized transaction records.
			for _, v := range req.Versions {
				if err := b.Delete(storage.EncodeMVCCKey(v.Key, v.TS)); err != nil {
					return nil, kvpb.NewError(err)
				}
			}
			for _, k := range req.TxnRecordKeys {
				if err := b.Delete(k); err != nil {
					return nil, kvpb.NewError(err)
				}
			}
			ru.GC = &kvpb.GCResponse{}
		case *kvpb.TruncateLogRequest:
			// Applying this entry means everything at or below it — Index
			// included — is durably applied here, so the local log prefix is
			// no longer needed. The new truncated state is persisted by
			// stageAppliedIndex in this same batch.
			if err := r.rs.stageTruncate(b, req.Index, req.Term); err != nil {
				return nil, kvpb.NewError(err)
			}
			ru.TruncateLog = &kvpb.TruncateLogResponse{}
		case *kvpb.SubsumeRequest:
			// Freeze for a merge; persisted by stageAppliedIndex in this
			// same batch, so every future leader and restart honors it.
			r.mu.Lock()
			r.mu.frozen = true
			r.mu.mergedInto = req.MergeInto
			r.mu.Unlock()
			ru.Subsume = &kvpb.SubsumeResponse{}
		case *kvpb.UnfreezeRequest:
			r.mu.Lock()
			r.mu.frozen = false
			r.mu.mergedInto = 0
			r.mu.Unlock()
			ru.Unfreeze = &kvpb.UnfreezeResponse{}
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
	if ba.Header.StaleRead {
		// A stale read is pinned to a fixed past timestamp; the closed
		// timestamp proves nothing can commit at or below it anywhere, so
		// there is no uncertainty window to restart over.
		opts.UncertaintyLimit = ts
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
		case *kvpb.ExportRequest:
			res, err := storage.MVCCExport(eng, req.Key, req.EndKey, req.StartTS, ts, req.MaxRecords)
			if err != nil {
				return nil, kvpb.NewError(err)
			}
			resp := &kvpb.ExportResponse{Resume: res.Resume}
			for _, rec := range res.Records {
				resp.Records = append(resp.Records, kvpb.ExportRecord{Key: rec.Key, Value: rec.Value, Deleted: rec.Deleted})
			}
			ru.Export = resp
		case *kvpb.RefreshRequest:
			if ba.Header.Txn == nil {
				return nil, kvpb.NewErrorf("Refresh without a transaction")
			}
			end := req.EndKey
			if len(end) == 0 {
				end = req.Key.Next()
			}
			// ts here is the transaction's NEW read timestamp; the read path
			// has already bumped the timestamp cache to it (invariant L2),
			// so a success cannot be invalidated by a later write.
			if err := storage.MVCCCheckForWrites(eng, req.Key, end, req.FromTS, ts, ba.Header.Txn.ID); err != nil {
				return nil, kvpb.NewError(err)
			}
			ru.Refresh = &kvpb.RefreshResponse{}
		case *kvpb.PushTxnRequest:
			// Query-only pushes read the record with no state change — the
			// deadlock detector's chain walk. Mutating pushes go through
			// the write path.
			if !req.QueryOnly {
				return nil, kvpb.NewErrorf("non-query PushTxn in read-only batch")
			}
			rec, err := loadTxnRecord(eng, txnRecordKey(&req.PusheeTxn))
			if err != nil {
				return nil, kvpb.NewError(err)
			}
			resp := &kvpb.PushTxnResponse{Status: enginepb.PENDING}
			if rec != nil {
				resp.Status = rec.Status
				resp.CommitTS = rec.WriteTimestamp
				resp.WaitingFor = rec.WaitingFor
				resp.WaitingForKey = rec.WaitingForKey
				resp.Priority = rec.Priority
			}
			ru.PushTxn = resp
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

// mvccVersionOverhead approximates the engine-key overhead of one MVCC
// version beyond the user key and value: escaping terminator plus the
// 12-byte timestamp suffix.
const mvccVersionOverhead = 16

// commandSizeDelta is the replicated size-accounting rule: an
// approximation, but a deterministic one — every replica computes the same
// value for the same command. Splits recompute exact sizes; GC subtracts
// the exact bytes its enumerating leader measured.
func commandSizeDelta(ba *kvpb.BatchRequest) int64 {
	var delta int64
	for _, u := range ba.Requests {
		switch req := u.GetInner().(type) {
		case *kvpb.PutRequest:
			delta += int64(len(req.Key)+len(req.Value)) + mvccVersionOverhead
		case *kvpb.DeleteRequest:
			delta += int64(len(req.Key)) + mvccVersionOverhead // tombstones occupy space until GC
		case *kvpb.IncrementRequest:
			delta += int64(len(req.Key)) + mvccVersionOverhead + 8
		case *kvpb.GCRequest:
			for _, v := range req.Versions {
				delta -= v.Bytes
			}
		}
	}
	return delta
}
