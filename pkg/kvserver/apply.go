package kvserver

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strconv"

	"go.etcd.io/raft/v3"
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
// errApplyAborted marks an apply abandoned because the node is shutting
// down (see stageMerge). It is NOT deterministic across replicas, so the
// command's effects and applied-index advance must both be discarded —
// the restart replays the entry from the log and applies it for real.
var errApplyAborted = errors.New("apply aborted: node shutting down")

// applyAbortedError is an errApplyAborted with its own message — the
// merge wait's exit when the local RHS raft loop is dead (issue #70).
// errors.Is(err, errApplyAborted) matches it, so every caller treats it
// exactly like the shutdown abort.
type applyAbortedError struct{ msg string }

func (e *applyAbortedError) Error() string        { return e.msg }
func (e *applyAbortedError) Is(target error) bool { return target == errApplyAborted }

func (r *Replica) applyEntry(ctx context.Context, ent raftpb.Entry) error {
	r.applyMu.Lock()
	defer r.applyMu.Unlock()
	r.mu.Lock()
	applied := r.mu.appliedIndex
	r.mu.Unlock()
	if ent.Index <= applied {
		return nil
	}
	if k := r.store.cfg.TestingKnobs.FailApply; k != nil {
		if err := k(r.rangeID, ent.Index); err != nil {
			return err
		}
	}

	switch ent.Type {
	case raftpb.EntryConfChange:
		var cc raftpb.ConfChange
		if err := cc.Unmarshal(ent.Data); err != nil {
			return fmt.Errorf("corrupt conf change: %w", err)
		}
		var state *raftpb.ConfState
		if err := r.withRaftGroup(func(rn *raft.RawNode) error {
			state = rn.ApplyConfChange(cc)
			return nil
		}); err != nil {
			return err
		}
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
		resp, aerr, abort := r.applyCommand(ctx, cmd, ent.Index)
		if abort != nil {
			// Nothing was applied and no waiter is answered: the command
			// replays after restart.
			return abort
		}

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
// The trailing error is non-nil only when the apply was ABANDONED whole
// (errApplyAborted): no effects landed, the applied index did not move,
// and the raft loop must stop instead of advancing.
func (r *Replica) applyCommand(ctx context.Context, cmd *raftCommand, idx uint64) (*kvpb.BatchResponse, *kvpb.Error, error) {
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
		if err := r.stageMerge(ctx, b, cmd.Merge); err != nil {
			_ = b.Close()
			if errors.Is(err, errApplyAborted) {
				return nil, nil, err
			}
			log.Warnf("%s: merge application refused: %v", r.rangeID, err)
			b = eng.NewBatch()
			aerr = kvpb.NewError(err)
		}
	}
	if err := r.stageAppliedIndex(b, idx); err != nil {
		_ = b.Close()
		return nil, kvpb.NewError(err), nil
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
	return resp, aerr, nil
}

// evalWriteBatch executes the batch's requests against the engine batch.
func (r *Replica) evalWriteBatch(b *storage.Batch, ba *kvpb.BatchRequest) (*kvpb.BatchResponse, *kvpb.Error) {
	br := &kvpb.BatchResponse{Txn: ba.Header.Txn, Timestamp: ba.Header.Timestamp}
	var txnMeta *enginepb.TxnMeta
	if ba.Header.Txn != nil {
		txnMeta = &ba.Header.Txn.TxnMeta
	}
	ts := writeTimestamp(ba)

	// One-phase commit: the whole transaction in one proposal — writes land
	// as committed values, no record, no intents. Detected from batch
	// content (never the header's CreateTxnRecord, which stays set for old
	// servers' classic evaluation).
	if is1PC(ba) {
		return r.evalOnePhase(b, ba)
	}

	if ba.Header.CreateTxnRecord {
		if err := r.createTxnRecord(b, ba.Header.Txn); err != nil {
			return nil, err
		}
	}

	// A write-intent conflict does not stop evaluation: the batch's other
	// requests are still evaluated (into a batch that is discarded anyway)
	// so that EVERY conflicting intent is reported at once. Reporting only
	// the first made a batch that overlapped n stale intents of one dead
	// transaction cost n proposals, each failing on the next key after the
	// client resolved the previous one (issue #74).
	var conflicts intentCollector
	for i := range ba.Requests {
		var ru kvpb.ResponseUnion
		switch req := ba.Requests[i].GetInner().(type) {
		case *kvpb.PutRequest:
			if err := storage.MVCCPut(b, req.Key, ts, req.Value, txnMeta); err != nil {
				if conflicts.collect(err) {
					continue
				}
				return nil, kvpb.NewError(err)
			}
			ru.Put = &kvpb.PutResponse{}
		case *kvpb.DeleteRequest:
			if err := storage.MVCCDelete(b, req.Key, ts, txnMeta); err != nil {
				if conflicts.collect(err) {
					continue
				}
				return nil, kvpb.NewError(err)
			}
			ru.Delete = &kvpb.DeleteResponse{}
		case *kvpb.IncrementRequest:
			// Increments are serialized by Raft: read the latest value, not
			// a snapshot — that is what makes them atomic counters.
			cur, err := storage.MVCCGet(b, req.Key, maxTimestamp, storage.MVCCGetOptions{Txn: txnMeta})
			if err != nil {
				if conflicts.collect(err) {
					continue
				}
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
				if conflicts.collect(err) {
					continue
				}
				return nil, kvpb.NewError(err)
			}
			ru.Increment = &kvpb.IncrementResponse{NewValue: v}
		case *kvpb.GetRequest:
			val, err := storage.MVCCGet(b, req.Key, readTimestamp(ba), storage.MVCCGetOptions{Txn: txnMeta})
			if err != nil {
				if conflicts.collect(err) {
					continue
				}
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
					if conflicts.collect(err) {
						continue
					}
					return nil, kvpb.NewError(err)
				}
			}
			ru.Get = &kvpb.GetResponse{Value: val}
		case *kvpb.ScanRequest:
			scan := storage.MVCCScan
			if req.Reverse {
				if req.ForUpdate {
					return nil, kvpb.NewErrorf("reverse locking scans are not supported")
				}
				scan = storage.MVCCReverseScan
			}
			res, err := scan(b, req.Key, req.EndKey, readTimestamp(ba), req.MaxRows, storage.MVCCGetOptions{Txn: txnMeta, TargetBytes: scanTargetBytes})
			if err != nil {
				if conflicts.collect(err) {
					continue
				}
				return nil, kvpb.NewError(err)
			}
			if req.ForUpdate {
				if txnMeta == nil {
					return nil, kvpb.NewErrorf("locking scan outside a transaction")
				}
				locked := true
				for _, kv := range res.KVs {
					if err := storage.MVCCLock(b, kv.Key, readTimestamp(ba), kv.Value, txnMeta); err != nil {
						if conflicts.collect(err) {
							locked = false
							continue
						}
						return nil, kvpb.NewError(err)
					}
				}
				if !locked {
					continue
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
		case *kvpb.UpdateMetaRequest:
			applied, err := evalUpdateMeta(b, req, ts)
			if err != nil {
				return nil, kvpb.NewError(err)
			}
			ru.UpdateMeta = &kvpb.UpdateMetaResponse{Applied: applied}
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
	if err := conflicts.err(); err != nil {
		return nil, kvpb.NewError(err)
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
			scan := storage.MVCCScan
			if req.Reverse {
				scan = storage.MVCCReverseScan
			}
			sopts := opts
			sopts.TargetBytes = scanTargetBytes
			res, err := scan(eng, req.Key, req.EndKey, ts, req.MaxRows, sopts)
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

// scanTargetBytes pages a scan's response: a range answers with at most
// this many row bytes and a Resume key, so a large result crosses the
// internode RPC in pages that fit its message limit (pkg/rpc), and the
// client (kvclient sendScan) stitches the pages back together.
const scanTargetBytes = 8 << 20

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

// intentCollector gathers the write-intent conflicts a batch evaluation
// runs into so they are reported together (see evalWriteBatch).
type intentCollector struct {
	intents []storage.Intent
	seen    map[string]bool
}

// collect records err's intents and reports true when err was a
// write-intent conflict (evaluation continues); any other error is left
// to the caller.
func (c *intentCollector) collect(err error) bool {
	var wie *storage.WriteIntentError
	if !errors.As(err, &wie) {
		return false
	}
	if c.seen == nil {
		c.seen = map[string]bool{}
	}
	for _, in := range wie.Intents {
		if k := string(in.Key); !c.seen[k] {
			c.seen[k] = true
			c.intents = append(c.intents, in)
		}
	}
	return true
}

func (c *intentCollector) err() error {
	if len(c.intents) == 0 {
		return nil
	}
	return &storage.WriteIntentError{Intents: c.intents}
}

// evalUpdateMeta applies an ordered range-addressing update: the record
// at req.Key is replaced by req.Desc only if no record is there, the
// record's generation is older, or it is the same range at the same
// generation (an idempotent repeat); a delete (nil Desc) applies only
// while the record still names req.IfRangeID at req.IfGeneration or
// older. Evaluated at apply, in log order, so every replica agrees.
func evalUpdateMeta(b *storage.Batch, req *kvpb.UpdateMetaRequest, ts hlc.Timestamp) (bool, error) {
	raw, err := storage.MVCCGet(b, req.Key, maxTimestamp, storage.MVCCGetOptions{})
	if err != nil {
		return false, err
	}
	var existing *kvpb.RangeDescriptor
	if raw != nil {
		var d kvpb.RangeDescriptor
		if err := json.Unmarshal(raw, &d); err != nil {
			return false, fmt.Errorf("corrupt meta record at %s: %w", req.Key, err)
		}
		existing = &d
	}
	if !metaUpdateApplies(existing, req) {
		return false, nil
	}
	if req.Desc == nil {
		return true, storage.MVCCDelete(b, req.Key, ts, nil)
	}
	val, err := json.Marshal(req.Desc)
	if err != nil {
		return false, err
	}
	return true, storage.MVCCPut(b, req.Key, ts, val, nil)
}

// metaUpdateApplies is evalUpdateMeta's rule, kept pure for tests.
func metaUpdateApplies(existing *kvpb.RangeDescriptor, req *kvpb.UpdateMetaRequest) bool {
	if req.Desc == nil {
		return existing != nil && existing.RangeID == req.IfRangeID && existing.Generation <= req.IfGeneration
	}
	if existing == nil || existing.Generation < req.Desc.Generation {
		return true
	}
	return existing.RangeID == req.Desc.RangeID && existing.Generation == req.Desc.Generation
}
