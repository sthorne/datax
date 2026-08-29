package kvpb

import (
	"fmt"

	"github.com/google/uuid"
	"github.com/sthorne/datax/pkg/base"
	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/rpc/rpcpb"
	"github.com/sthorne/datax/pkg/storage"
	"github.com/sthorne/datax/pkg/storage/enginepb"
	"github.com/sthorne/datax/pkg/util/hlc"
	"google.golang.org/protobuf/proto"
)

// Converters between the hand-written Go API types (the in-memory
// representation used everywhere) and their rpcpb protobuf mirrors, used on
// the hot wire/log boundaries: the Batch RPC body and the raft command
// payload (issue #8). Cold paths stay JSON.

// MarshalBatchRequest encodes a batch for the wire.
func MarshalBatchRequest(b *BatchRequest) ([]byte, error) {
	return proto.Marshal(BatchRequestToProto(b))
}

// UnmarshalBatchRequest decodes a wire batch.
func UnmarshalBatchRequest(data []byte) (*BatchRequest, error) {
	var pb rpcpb.BatchRequest
	if err := proto.Unmarshal(data, &pb); err != nil {
		return nil, err
	}
	return BatchRequestFromProto(&pb)
}

// ---- timestamps / ids ----

func tsToProto(t hlc.Timestamp) *rpcpb.Hlc {
	if t.IsEmpty() {
		return nil
	}
	return &rpcpb.Hlc{WallTime: t.WallTime, Logical: t.Logical}
}

func tsFromProto(p *rpcpb.Hlc) hlc.Timestamp {
	if p == nil {
		return hlc.Timestamp{}
	}
	return hlc.Timestamp{WallTime: p.WallTime, Logical: p.Logical}
}

func uuidToProto(id uuid.UUID) []byte {
	if id == uuid.Nil {
		return nil
	}
	out := make([]byte, 16)
	copy(out, id[:])
	return out
}

func uuidFromProto(b []byte) (uuid.UUID, error) {
	if len(b) == 0 {
		return uuid.Nil, nil
	}
	return uuid.FromBytes(b)
}

// ---- transactions ----

func txnMetaToProto(m enginepb.TxnMeta) *rpcpb.TxnMeta {
	return &rpcpb.TxnMeta{
		Id:             uuidToProto(m.ID),
		Key:            m.Key,
		Epoch:          m.Epoch,
		WriteTimestamp: tsToProto(m.WriteTimestamp),
		MinTimestamp:   tsToProto(m.MinTimestamp),
		Priority:       m.Priority,
	}
}

func txnMetaFromProto(p *rpcpb.TxnMeta) (enginepb.TxnMeta, error) {
	if p == nil {
		return enginepb.TxnMeta{}, nil
	}
	id, err := uuidFromProto(p.Id)
	if err != nil {
		return enginepb.TxnMeta{}, err
	}
	return enginepb.TxnMeta{
		ID:             id,
		Key:            p.Key,
		Epoch:          p.Epoch,
		WriteTimestamp: tsFromProto(p.WriteTimestamp),
		MinTimestamp:   tsFromProto(p.MinTimestamp),
		Priority:       p.Priority,
	}, nil
}

func txnToProto(t *Transaction) *rpcpb.Transaction {
	if t == nil {
		return nil
	}
	pb := &rpcpb.Transaction{
		Meta:          txnMetaToProto(t.TxnMeta),
		Name:          t.Name,
		Status:        int32(t.Status),
		ReadTimestamp: tsToProto(t.ReadTimestamp),
		LastHeartbeat: tsToProto(t.LastHeartbeat),
	}
	for _, k := range t.IntentKeys {
		pb.IntentKeys = append(pb.IntentKeys, k)
	}
	return pb
}

func txnFromProto(p *rpcpb.Transaction) (*Transaction, error) {
	if p == nil {
		return nil, nil
	}
	meta, err := txnMetaFromProto(p.Meta)
	if err != nil {
		return nil, err
	}
	t := &Transaction{
		TxnMeta:       meta,
		Name:          p.Name,
		Status:        enginepb.TxnStatus(p.Status),
		ReadTimestamp: tsFromProto(p.ReadTimestamp),
		LastHeartbeat: tsFromProto(p.LastHeartbeat),
	}
	for _, k := range p.IntentKeys {
		t.IntentKeys = append(t.IntentKeys, keys.Key(k))
	}
	return t, nil
}

// ---- descriptors ----

// RangeDescriptorToProto converts a descriptor (exported for kvserver's
// raft trigger encoding).
func RangeDescriptorToProto(d RangeDescriptor) *rpcpb.RangeDescriptor {
	pb := &rpcpb.RangeDescriptor{
		RangeId:       int64(d.RangeID),
		StartKey:      d.StartKey,
		EndKey:        d.EndKey,
		NextReplicaId: int64(d.NextReplicaID),
		Generation:    d.Generation,
	}
	for _, r := range d.Replicas {
		pb.Replicas = append(pb.Replicas, &rpcpb.ReplicaDescriptor{
			NodeId: int64(r.NodeID), StoreId: int64(r.StoreID), ReplicaId: int64(r.ReplicaID),
		})
	}
	return pb
}

// RangeDescriptorFromProto is the inverse of RangeDescriptorToProto.
func RangeDescriptorFromProto(p *rpcpb.RangeDescriptor) RangeDescriptor {
	if p == nil {
		return RangeDescriptor{}
	}
	d := RangeDescriptor{
		RangeID:       base.RangeID(p.RangeId),
		StartKey:      p.StartKey,
		EndKey:        p.EndKey,
		NextReplicaID: base.ReplicaID(p.NextReplicaId),
		Generation:    p.Generation,
	}
	for _, r := range p.Replicas {
		d.Replicas = append(d.Replicas, ReplicaDescriptor{
			NodeID: base.NodeID(r.NodeId), StoreID: base.StoreID(r.StoreId), ReplicaID: base.ReplicaID(r.ReplicaId),
		})
	}
	return d
}

// ---- requests ----

func reqHeaderToProto(h RequestHeader) *rpcpb.RequestHeader {
	return &rpcpb.RequestHeader{Key: h.Key, EndKey: h.EndKey}
}

func reqHeaderFromProto(p *rpcpb.RequestHeader) RequestHeader {
	if p == nil {
		return RequestHeader{}
	}
	return RequestHeader{Key: p.Key, EndKey: p.EndKey}
}

func requestUnionToProto(u RequestUnion) (*rpcpb.RequestUnion, error) {
	out := &rpcpb.RequestUnion{}
	switch r := u.GetInner().(type) {
	case *GetRequest:
		out.Value = &rpcpb.RequestUnion_Get{Get: &rpcpb.GetRequest{Header: reqHeaderToProto(r.RequestHeader)}}
	case *PutRequest:
		out.Value = &rpcpb.RequestUnion_Put{Put: &rpcpb.PutRequest{Header: reqHeaderToProto(r.RequestHeader), Value: r.Value}}
	case *DeleteRequest:
		out.Value = &rpcpb.RequestUnion_Delete{Delete: &rpcpb.DeleteRequest{Header: reqHeaderToProto(r.RequestHeader)}}
	case *IncrementRequest:
		out.Value = &rpcpb.RequestUnion_Increment{Increment: &rpcpb.IncrementRequest{Header: reqHeaderToProto(r.RequestHeader), By: r.By}}
	case *ScanRequest:
		out.Value = &rpcpb.RequestUnion_Scan{Scan: &rpcpb.ScanRequest{Header: reqHeaderToProto(r.RequestHeader), MaxRows: r.MaxRows}}
	case *EndTxnRequest:
		pb := &rpcpb.EndTxnRequest{Header: reqHeaderToProto(r.RequestHeader), Commit: r.Commit}
		for _, k := range r.IntentKeys {
			pb.IntentKeys = append(pb.IntentKeys, k)
		}
		out.Value = &rpcpb.RequestUnion_EndTxn{EndTxn: pb}
	case *HeartbeatTxnRequest:
		out.Value = &rpcpb.RequestUnion_HeartbeatTxn{HeartbeatTxn: &rpcpb.HeartbeatTxnRequest{Header: reqHeaderToProto(r.RequestHeader), Now: tsToProto(r.Now)}}
	case *PushTxnRequest:
		out.Value = &rpcpb.RequestUnion_PushTxn{PushTxn: &rpcpb.PushTxnRequest{
			Header:    reqHeaderToProto(r.RequestHeader),
			PusherTxn: txnToProto(r.PusherTxn),
			PusheeTxn: txnMetaToProto(r.PusheeTxn),
			PushAbort: r.PushAbort,
			Now:       tsToProto(r.Now),
		}}
	case *ResolveIntentRequest:
		out.Value = &rpcpb.RequestUnion_ResolveIntent{ResolveIntent: &rpcpb.ResolveIntentRequest{
			Header:   reqHeaderToProto(r.RequestHeader),
			TxnId:    uuidToProto(r.TxnID),
			Status:   int32(r.Status),
			CommitTs: tsToProto(r.CommitTS),
		}}
	case *RefreshRequest:
		out.Value = &rpcpb.RequestUnion_Refresh{Refresh: &rpcpb.RefreshRequest{Header: reqHeaderToProto(r.RequestHeader), FromTs: tsToProto(r.FromTS)}}
	case *GCRequest:
		pb := &rpcpb.GcRequest{Header: reqHeaderToProto(r.RequestHeader), Threshold: tsToProto(r.Threshold)}
		for _, v := range r.Versions {
			pb.Versions = append(pb.Versions, &rpcpb.GcVersion{Key: v.Key, Ts: tsToProto(v.TS), BytesSize: v.Bytes})
		}
		for _, k := range r.TxnRecordKeys {
			pb.TxnRecordKeys = append(pb.TxnRecordKeys, k)
		}
		out.Value = &rpcpb.RequestUnion_Gc{Gc: pb}
	case *TruncateLogRequest:
		out.Value = &rpcpb.RequestUnion_TruncateLog{TruncateLog: &rpcpb.TruncateLogRequest{Header: reqHeaderToProto(r.RequestHeader), Index: r.Index, Term: r.Term}}
	case *AdminSplitRequest:
		out.Value = &rpcpb.RequestUnion_AdminSplit{AdminSplit: &rpcpb.AdminSplitRequest{Header: reqHeaderToProto(r.RequestHeader)}}
	case *AdminChangeReplicasRequest:
		out.Value = &rpcpb.RequestUnion_AdminChangeReplicas{AdminChangeReplicas: &rpcpb.AdminChangeReplicasRequest{
			Header: reqHeaderToProto(r.RequestHeader), AddNode: int64(r.AddNode), RemoveNode: int64(r.RemoveNode),
		}}
	case *AdminTransferLeaseRequest:
		out.Value = &rpcpb.RequestUnion_AdminTransferLease{AdminTransferLease: &rpcpb.AdminTransferLeaseRequest{
			Header: reqHeaderToProto(r.RequestHeader), Target: int64(r.Target),
		}}
	case *AdminMergeRequest:
		out.Value = &rpcpb.RequestUnion_AdminMerge{AdminMerge: &rpcpb.AdminMergeRequest{Header: reqHeaderToProto(r.RequestHeader)}}
	case *SubsumeRequest:
		out.Value = &rpcpb.RequestUnion_Subsume{Subsume: &rpcpb.SubsumeRequest{Header: reqHeaderToProto(r.RequestHeader), MergeInto: int64(r.MergeInto)}}
	case *UnfreezeRequest:
		out.Value = &rpcpb.RequestUnion_Unfreeze{Unfreeze: &rpcpb.UnfreezeRequest{Header: reqHeaderToProto(r.RequestHeader)}}
	default:
		return nil, fmt.Errorf("unencodable request type %T", r)
	}
	return out, nil
}

func requestUnionFromProto(p *rpcpb.RequestUnion) (RequestUnion, error) {
	var u RequestUnion
	switch v := p.Value.(type) {
	case *rpcpb.RequestUnion_Get:
		u.Get = &GetRequest{RequestHeader: reqHeaderFromProto(v.Get.Header)}
	case *rpcpb.RequestUnion_Put:
		u.Put = &PutRequest{RequestHeader: reqHeaderFromProto(v.Put.Header), Value: v.Put.Value}
	case *rpcpb.RequestUnion_Delete:
		u.Delete = &DeleteRequest{RequestHeader: reqHeaderFromProto(v.Delete.Header)}
	case *rpcpb.RequestUnion_Increment:
		u.Increment = &IncrementRequest{RequestHeader: reqHeaderFromProto(v.Increment.Header), By: v.Increment.By}
	case *rpcpb.RequestUnion_Scan:
		u.Scan = &ScanRequest{RequestHeader: reqHeaderFromProto(v.Scan.Header), MaxRows: v.Scan.MaxRows}
	case *rpcpb.RequestUnion_EndTxn:
		r := &EndTxnRequest{RequestHeader: reqHeaderFromProto(v.EndTxn.Header), Commit: v.EndTxn.Commit}
		for _, k := range v.EndTxn.IntentKeys {
			r.IntentKeys = append(r.IntentKeys, keys.Key(k))
		}
		u.EndTxn = r
	case *rpcpb.RequestUnion_HeartbeatTxn:
		u.HeartbeatTxn = &HeartbeatTxnRequest{RequestHeader: reqHeaderFromProto(v.HeartbeatTxn.Header), Now: tsFromProto(v.HeartbeatTxn.Now)}
	case *rpcpb.RequestUnion_PushTxn:
		pusher, err := txnFromProto(v.PushTxn.PusherTxn)
		if err != nil {
			return u, err
		}
		pushee, err := txnMetaFromProto(v.PushTxn.PusheeTxn)
		if err != nil {
			return u, err
		}
		u.PushTxn = &PushTxnRequest{
			RequestHeader: reqHeaderFromProto(v.PushTxn.Header),
			PusherTxn:     pusher,
			PusheeTxn:     pushee,
			PushAbort:     v.PushTxn.PushAbort,
			Now:           tsFromProto(v.PushTxn.Now),
		}
	case *rpcpb.RequestUnion_ResolveIntent:
		id, err := uuidFromProto(v.ResolveIntent.TxnId)
		if err != nil {
			return u, err
		}
		u.ResolveIntent = &ResolveIntentRequest{
			RequestHeader: reqHeaderFromProto(v.ResolveIntent.Header),
			TxnID:         id,
			Status:        enginepb.TxnStatus(v.ResolveIntent.Status),
			CommitTS:      tsFromProto(v.ResolveIntent.CommitTs),
		}
	case *rpcpb.RequestUnion_Refresh:
		u.Refresh = &RefreshRequest{RequestHeader: reqHeaderFromProto(v.Refresh.Header), FromTS: tsFromProto(v.Refresh.FromTs)}
	case *rpcpb.RequestUnion_Gc:
		r := &GCRequest{RequestHeader: reqHeaderFromProto(v.Gc.Header), Threshold: tsFromProto(v.Gc.Threshold)}
		for _, ver := range v.Gc.Versions {
			r.Versions = append(r.Versions, GCVersion{Key: ver.Key, TS: tsFromProto(ver.Ts), Bytes: ver.BytesSize})
		}
		for _, k := range v.Gc.TxnRecordKeys {
			r.TxnRecordKeys = append(r.TxnRecordKeys, keys.Key(k))
		}
		u.GC = r
	case *rpcpb.RequestUnion_TruncateLog:
		u.TruncateLog = &TruncateLogRequest{RequestHeader: reqHeaderFromProto(v.TruncateLog.Header), Index: v.TruncateLog.Index, Term: v.TruncateLog.Term}
	case *rpcpb.RequestUnion_AdminSplit:
		u.AdminSplit = &AdminSplitRequest{RequestHeader: reqHeaderFromProto(v.AdminSplit.Header)}
	case *rpcpb.RequestUnion_AdminChangeReplicas:
		u.AdminChangeReplicas = &AdminChangeReplicasRequest{
			RequestHeader: reqHeaderFromProto(v.AdminChangeReplicas.Header),
			AddNode:       base.NodeID(v.AdminChangeReplicas.AddNode),
			RemoveNode:    base.NodeID(v.AdminChangeReplicas.RemoveNode),
		}
	case *rpcpb.RequestUnion_AdminTransferLease:
		u.AdminTransferLease = &AdminTransferLeaseRequest{
			RequestHeader: reqHeaderFromProto(v.AdminTransferLease.Header),
			Target:        base.NodeID(v.AdminTransferLease.Target),
		}
	case *rpcpb.RequestUnion_AdminMerge:
		u.AdminMerge = &AdminMergeRequest{RequestHeader: reqHeaderFromProto(v.AdminMerge.Header)}
	case *rpcpb.RequestUnion_Subsume:
		u.Subsume = &SubsumeRequest{RequestHeader: reqHeaderFromProto(v.Subsume.Header), MergeInto: base.RangeID(v.Subsume.MergeInto)}
	case *rpcpb.RequestUnion_Unfreeze:
		u.Unfreeze = &UnfreezeRequest{RequestHeader: reqHeaderFromProto(v.Unfreeze.Header)}
	default:
		return u, fmt.Errorf("undecodable request union %T", p.Value)
	}
	return u, nil
}

// BatchRequestToProto converts a batch to its wire mirror.
func BatchRequestToProto(b *BatchRequest) *rpcpb.BatchRequest {
	pb := &rpcpb.BatchRequest{
		Header: &rpcpb.BatchHeader{
			Timestamp:        tsToProto(b.Header.Timestamp),
			Txn:              txnToProto(b.Header.Txn),
			RangeId:          int64(b.Header.RangeID),
			CreateTxnRecord:  b.Header.CreateTxnRecord,
			ReadInconsistent: b.Header.ReadInconsistent,
		},
	}
	for _, u := range b.Requests {
		pu, err := requestUnionToProto(u)
		if err != nil {
			// Every request type is covered above; an unknown one is a
			// programming error surfaced at development time.
			panic(err)
		}
		pb.Requests = append(pb.Requests, pu)
	}
	return pb
}

// BatchRequestFromProto is the inverse of BatchRequestToProto.
func BatchRequestFromProto(pb *rpcpb.BatchRequest) (*BatchRequest, error) {
	b := &BatchRequest{}
	if h := pb.Header; h != nil {
		txn, err := txnFromProto(h.Txn)
		if err != nil {
			return nil, err
		}
		b.Header = BatchHeader{
			Timestamp:        tsFromProto(h.Timestamp),
			Txn:              txn,
			RangeID:          base.RangeID(h.RangeId),
			CreateTxnRecord:  h.CreateTxnRecord,
			ReadInconsistent: h.ReadInconsistent,
		}
	}
	for _, pu := range pb.Requests {
		u, err := requestUnionFromProto(pu)
		if err != nil {
			return nil, err
		}
		b.Requests = append(b.Requests, u)
	}
	return b, nil
}

// ---- responses ----

func responseUnionToProto(u ResponseUnion) *rpcpb.ResponseUnion {
	out := &rpcpb.ResponseUnion{}
	switch {
	case u.Get != nil:
		out.Value = &rpcpb.ResponseUnion_Get{Get: &rpcpb.GetResponse{Value: u.Get.Value, Found: u.Get.Value != nil}}
	case u.Put != nil:
		out.Value = &rpcpb.ResponseUnion_Put{Put: &rpcpb.PutResponse{}}
	case u.Delete != nil:
		out.Value = &rpcpb.ResponseUnion_Delete{Delete: &rpcpb.DeleteResponse{}}
	case u.Increment != nil:
		out.Value = &rpcpb.ResponseUnion_Increment{Increment: &rpcpb.IncrementResponse{NewValue: u.Increment.NewValue}}
	case u.Scan != nil:
		pb := &rpcpb.ScanResponse{Resume: u.Scan.Resume}
		for _, kv := range u.Scan.Rows {
			pb.Rows = append(pb.Rows, &rpcpb.KeyValue{Key: kv.Key, Value: kv.Value})
		}
		out.Value = &rpcpb.ResponseUnion_Scan{Scan: pb}
	case u.EndTxn != nil:
		out.Value = &rpcpb.ResponseUnion_EndTxn{EndTxn: &rpcpb.EndTxnResponse{CommitTimestamp: tsToProto(u.EndTxn.CommitTimestamp)}}
	case u.HeartbeatTxn != nil:
		out.Value = &rpcpb.ResponseUnion_HeartbeatTxn{HeartbeatTxn: &rpcpb.HeartbeatTxnResponse{Status: int32(u.HeartbeatTxn.Status)}}
	case u.PushTxn != nil:
		out.Value = &rpcpb.ResponseUnion_PushTxn{PushTxn: &rpcpb.PushTxnResponse{Status: int32(u.PushTxn.Status), CommitTs: tsToProto(u.PushTxn.CommitTS)}}
	case u.ResolveIntent != nil:
		out.Value = &rpcpb.ResponseUnion_ResolveIntent{ResolveIntent: &rpcpb.ResolveIntentResponse{}}
	case u.Refresh != nil:
		out.Value = &rpcpb.ResponseUnion_Refresh{Refresh: &rpcpb.RefreshResponse{}}
	case u.GC != nil:
		out.Value = &rpcpb.ResponseUnion_Gc{Gc: &rpcpb.GcResponse{}}
	case u.TruncateLog != nil:
		out.Value = &rpcpb.ResponseUnion_TruncateLog{TruncateLog: &rpcpb.TruncateLogResponse{}}
	case u.AdminSplit != nil:
		out.Value = &rpcpb.ResponseUnion_AdminSplit{AdminSplit: &rpcpb.AdminSplitResponse{
			Left: RangeDescriptorToProto(u.AdminSplit.Left), Right: RangeDescriptorToProto(u.AdminSplit.Right),
		}}
	case u.AdminChangeReplicas != nil:
		out.Value = &rpcpb.ResponseUnion_AdminChangeReplicas{AdminChangeReplicas: &rpcpb.AdminChangeReplicasResponse{Desc: RangeDescriptorToProto(u.AdminChangeReplicas.Desc)}}
	case u.AdminTransferLease != nil:
		out.Value = &rpcpb.ResponseUnion_AdminTransferLease{AdminTransferLease: &rpcpb.AdminTransferLeaseResponse{Desc: RangeDescriptorToProto(u.AdminTransferLease.Desc)}}
	case u.AdminMerge != nil:
		out.Value = &rpcpb.ResponseUnion_AdminMerge{AdminMerge: &rpcpb.AdminMergeResponse{Desc: RangeDescriptorToProto(u.AdminMerge.Desc)}}
	case u.Subsume != nil:
		out.Value = &rpcpb.ResponseUnion_Subsume{Subsume: &rpcpb.SubsumeResponse{}}
	case u.Unfreeze != nil:
		out.Value = &rpcpb.ResponseUnion_Unfreeze{Unfreeze: &rpcpb.UnfreezeResponse{}}
	}
	return out
}

func responseUnionFromProto(p *rpcpb.ResponseUnion) (ResponseUnion, error) {
	var u ResponseUnion
	switch v := p.Value.(type) {
	case *rpcpb.ResponseUnion_Get:
		val := v.Get.Value
		if !v.Get.Found {
			val = nil
		} else if val == nil {
			val = []byte{}
		}
		u.Get = &GetResponse{Value: val}
	case *rpcpb.ResponseUnion_Put:
		u.Put = &PutResponse{}
	case *rpcpb.ResponseUnion_Delete:
		u.Delete = &DeleteResponse{}
	case *rpcpb.ResponseUnion_Increment:
		u.Increment = &IncrementResponse{NewValue: v.Increment.NewValue}
	case *rpcpb.ResponseUnion_Scan:
		r := &ScanResponse{Resume: v.Scan.Resume}
		for _, kv := range v.Scan.Rows {
			r.Rows = append(r.Rows, KeyValue{Key: kv.Key, Value: kv.Value})
		}
		u.Scan = r
	case *rpcpb.ResponseUnion_EndTxn:
		u.EndTxn = &EndTxnResponse{CommitTimestamp: tsFromProto(v.EndTxn.CommitTimestamp)}
	case *rpcpb.ResponseUnion_HeartbeatTxn:
		u.HeartbeatTxn = &HeartbeatTxnResponse{Status: enginepb.TxnStatus(v.HeartbeatTxn.Status)}
	case *rpcpb.ResponseUnion_PushTxn:
		u.PushTxn = &PushTxnResponse{Status: enginepb.TxnStatus(v.PushTxn.Status), CommitTS: tsFromProto(v.PushTxn.CommitTs)}
	case *rpcpb.ResponseUnion_ResolveIntent:
		u.ResolveIntent = &ResolveIntentResponse{}
	case *rpcpb.ResponseUnion_Refresh:
		u.Refresh = &RefreshResponse{}
	case *rpcpb.ResponseUnion_Gc:
		u.GC = &GCResponse{}
	case *rpcpb.ResponseUnion_TruncateLog:
		u.TruncateLog = &TruncateLogResponse{}
	case *rpcpb.ResponseUnion_AdminSplit:
		u.AdminSplit = &AdminSplitResponse{
			Left:  RangeDescriptorFromProto(v.AdminSplit.Left),
			Right: RangeDescriptorFromProto(v.AdminSplit.Right),
		}
	case *rpcpb.ResponseUnion_AdminChangeReplicas:
		u.AdminChangeReplicas = &AdminChangeReplicasResponse{Desc: RangeDescriptorFromProto(v.AdminChangeReplicas.Desc)}
	case *rpcpb.ResponseUnion_AdminTransferLease:
		u.AdminTransferLease = &AdminTransferLeaseResponse{Desc: RangeDescriptorFromProto(v.AdminTransferLease.Desc)}
	case *rpcpb.ResponseUnion_AdminMerge:
		u.AdminMerge = &AdminMergeResponse{Desc: RangeDescriptorFromProto(v.AdminMerge.Desc)}
	case *rpcpb.ResponseUnion_Subsume:
		u.Subsume = &SubsumeResponse{}
	case *rpcpb.ResponseUnion_Unfreeze:
		u.Unfreeze = &UnfreezeResponse{}
	default:
		return u, fmt.Errorf("undecodable response union %T", p.Value)
	}
	return u, nil
}

// BatchResponseToProto converts a batch response to its wire mirror.
func BatchResponseToProto(b *BatchResponse) *rpcpb.BatchResponse {
	if b == nil {
		return nil
	}
	pb := &rpcpb.BatchResponse{
		Txn:       txnToProto(b.Txn),
		Timestamp: tsToProto(b.Timestamp),
	}
	for _, u := range b.Responses {
		pb.Responses = append(pb.Responses, responseUnionToProto(u))
	}
	return pb
}

// BatchResponseFromProto is the inverse of BatchResponseToProto.
func BatchResponseFromProto(pb *rpcpb.BatchResponse) (*BatchResponse, error) {
	if pb == nil {
		return nil, nil
	}
	txn, err := txnFromProto(pb.Txn)
	if err != nil {
		return nil, err
	}
	b := &BatchResponse{Txn: txn, Timestamp: tsFromProto(pb.Timestamp)}
	for _, pu := range pb.Responses {
		u, err := responseUnionFromProto(pu)
		if err != nil {
			return nil, err
		}
		b.Responses = append(b.Responses, u)
	}
	return b, nil
}

// ---- errors ----

// ErrorToProto converts a wire error to its proto mirror.
func ErrorToProto(e *Error) *rpcpb.Error {
	if e == nil {
		return nil
	}
	pb := &rpcpb.Error{Message: e.Message}
	if e.NotLeader != nil {
		pb.NotLeader = &rpcpb.NotLeaderError{RangeId: int64(e.NotLeader.RangeID), LeaderHint: int64(e.NotLeader.LeaderHint)}
	}
	if e.RangeNotFound != nil {
		pb.RangeNotFound = &rpcpb.RangeNotFoundError{RangeId: int64(e.RangeNotFound.RangeID)}
	}
	if e.RangeKeyMismatch != nil {
		m := &rpcpb.RangeKeyMismatchError{RequestKey: e.RangeKeyMismatch.RequestKey}
		for _, d := range e.RangeKeyMismatch.ActualDescriptors {
			m.ActualDescriptors = append(m.ActualDescriptors, RangeDescriptorToProto(d))
		}
		pb.RangeKeyMismatch = m
	}
	if e.WriteIntent != nil {
		m := &rpcpb.WriteIntentError{}
		for _, in := range e.WriteIntent.Intents {
			m.Intents = append(m.Intents, &rpcpb.Intent{Key: in.Key, Txn: txnMetaToProto(in.Txn)})
		}
		pb.WriteIntent = m
	}
	if e.WriteTooOld != nil {
		pb.WriteTooOld = &rpcpb.WriteTooOldError{Timestamp: tsToProto(e.WriteTooOld.Timestamp), ActualTimestamp: tsToProto(e.WriteTooOld.ActualTimestamp)}
	}
	if e.Uncertainty != nil {
		pb.Uncertainty = &rpcpb.UncertaintyError{ReadTimestamp: tsToProto(e.Uncertainty.ReadTimestamp), ExistingTimestamp: tsToProto(e.Uncertainty.ExistingTimestamp)}
	}
	if e.TxnAborted != nil {
		pb.TxnAborted = &rpcpb.TxnAbortedError{}
	}
	if e.TxnRetry != nil {
		pb.TxnRetry = &rpcpb.TxnRetryError{RetryTimestamp: tsToProto(e.TxnRetry.RetryTimestamp)}
	}
	if e.TxnNotFound != nil {
		pb.TxnNotFound = &rpcpb.TxnNotFoundError{}
	}
	if e.Ambiguous != nil {
		pb.Ambiguous = &rpcpb.AmbiguousResultError{}
	}
	return pb
}

// ErrorFromProto is the inverse of ErrorToProto.
func ErrorFromProto(pb *rpcpb.Error) (*Error, error) {
	if pb == nil {
		return nil, nil
	}
	e := &Error{Message: pb.Message}
	if pb.NotLeader != nil {
		e.NotLeader = &NotLeaderError{RangeID: base.RangeID(pb.NotLeader.RangeId), LeaderHint: base.NodeID(pb.NotLeader.LeaderHint)}
	}
	if pb.RangeNotFound != nil {
		e.RangeNotFound = &RangeNotFoundError{RangeID: base.RangeID(pb.RangeNotFound.RangeId)}
	}
	if pb.RangeKeyMismatch != nil {
		m := &RangeKeyMismatchError{RequestKey: pb.RangeKeyMismatch.RequestKey}
		for _, d := range pb.RangeKeyMismatch.ActualDescriptors {
			m.ActualDescriptors = append(m.ActualDescriptors, RangeDescriptorFromProto(d))
		}
		e.RangeKeyMismatch = m
	}
	if pb.WriteIntent != nil {
		m := &WriteIntentError{}
		for _, in := range pb.WriteIntent.Intents {
			txn, err := txnMetaFromProto(in.Txn)
			if err != nil {
				return nil, err
			}
			m.Intents = append(m.Intents, storage.Intent{Key: in.Key, Txn: txn})
		}
		e.WriteIntent = m
	}
	if pb.WriteTooOld != nil {
		e.WriteTooOld = &WriteTooOldError{Timestamp: tsFromProto(pb.WriteTooOld.Timestamp), ActualTimestamp: tsFromProto(pb.WriteTooOld.ActualTimestamp)}
	}
	if pb.Uncertainty != nil {
		e.Uncertainty = &UncertaintyError{ReadTimestamp: tsFromProto(pb.Uncertainty.ReadTimestamp), ExistingTimestamp: tsFromProto(pb.Uncertainty.ExistingTimestamp)}
	}
	if pb.TxnAborted != nil {
		e.TxnAborted = &TxnAbortedError{}
	}
	if pb.TxnRetry != nil {
		e.TxnRetry = &TxnRetryError{RetryTimestamp: tsFromProto(pb.TxnRetry.RetryTimestamp)}
	}
	if pb.TxnNotFound != nil {
		e.TxnNotFound = &TxnNotFoundError{}
	}
	if pb.Ambiguous != nil {
		e.Ambiguous = &AmbiguousResultError{}
	}
	return e, nil
}

// MarshalBatchEnvelope encodes a Batch RPC outcome (response or error).
func MarshalBatchEnvelope(br *BatchResponse, kerr *Error) ([]byte, error) {
	return proto.Marshal(&rpcpb.BatchEnvelope{
		Response: BatchResponseToProto(br),
		Error:    ErrorToProto(kerr),
	})
}

// UnmarshalBatchEnvelope decodes a Batch RPC outcome.
func UnmarshalBatchEnvelope(data []byte) (*BatchResponse, *Error, error) {
	var pb rpcpb.BatchEnvelope
	if err := proto.Unmarshal(data, &pb); err != nil {
		return nil, nil, err
	}
	br, err := BatchResponseFromProto(pb.Response)
	if err != nil {
		return nil, nil, err
	}
	kerr, err := ErrorFromProto(pb.Error)
	if err != nil {
		return nil, nil, err
	}
	return br, kerr, nil
}
