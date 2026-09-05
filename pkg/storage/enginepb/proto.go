package enginepb

import (
	"github.com/google/uuid"

	"github.com/sthorne/datax/pkg/rpc/rpcpb"
	"github.com/sthorne/datax/pkg/util/hlc"
)

// Conversions between the Go types and their protobuf messages: the
// wire encoding of transactions (pkg/kvpb) and, from cluster version
// v14, the stored encoding of intent metadata and transaction records
// (issue #141).

// TimestampToProto converts a timestamp; nil for the zero value.
func TimestampToProto(t hlc.Timestamp) *rpcpb.Hlc {
	if t.IsEmpty() {
		return nil
	}
	return &rpcpb.Hlc{WallTime: t.WallTime, Logical: t.Logical}
}

// TimestampFromProto converts a timestamp; nil is the zero value.
func TimestampFromProto(p *rpcpb.Hlc) hlc.Timestamp {
	if p == nil {
		return hlc.Timestamp{}
	}
	return hlc.Timestamp{WallTime: p.WallTime, Logical: p.Logical}
}

// UUIDToProto converts an id; nil for uuid.Nil.
func UUIDToProto(id uuid.UUID) []byte {
	if id == uuid.Nil {
		return nil
	}
	out := make([]byte, 16)
	copy(out, id[:])
	return out
}

// UUIDFromProto converts an id; empty is uuid.Nil.
func UUIDFromProto(b []byte) (uuid.UUID, error) {
	if len(b) == 0 {
		return uuid.Nil, nil
	}
	return uuid.FromBytes(b)
}

// TxnMetaToProto converts a transaction's metadata.
func TxnMetaToProto(m TxnMeta) *rpcpb.TxnMeta {
	return &rpcpb.TxnMeta{
		Id:             UUIDToProto(m.ID),
		Key:            m.Key,
		Epoch:          m.Epoch,
		WriteTimestamp: TimestampToProto(m.WriteTimestamp),
		MinTimestamp:   TimestampToProto(m.MinTimestamp),
		Priority:       m.Priority,
		Sequence:       m.Sequence,
		HistoryFloor:   m.HistoryFloor,
		BinaryMeta:     m.BinaryMeta,
	}
}

// TxnMetaFromProto converts a transaction's metadata; nil is the zero
// value.
func TxnMetaFromProto(p *rpcpb.TxnMeta) (TxnMeta, error) {
	if p == nil {
		return TxnMeta{}, nil
	}
	id, err := UUIDFromProto(p.Id)
	if err != nil {
		return TxnMeta{}, err
	}
	return TxnMeta{
		ID:             id,
		Key:            p.Key,
		Epoch:          p.Epoch,
		WriteTimestamp: TimestampFromProto(p.WriteTimestamp),
		MinTimestamp:   TimestampFromProto(p.MinTimestamp),
		Priority:       p.Priority,
		Sequence:       p.Sequence,
		HistoryFloor:   p.HistoryFloor,
		BinaryMeta:     p.BinaryMeta,
	}, nil
}

// MetadataToProto converts an intent's metadata.
func MetadataToProto(m MVCCMetadata) *rpcpb.MVCCMetadata {
	pb := &rpcpb.MVCCMetadata{Txn: TxnMetaToProto(m.Txn), Timestamp: TimestampToProto(m.Timestamp)}
	if len(m.History) > 0 {
		pb.History = make([]*rpcpb.IntentValue, len(m.History))
		for i, h := range m.History {
			pb.History[i] = &rpcpb.IntentValue{Sequence: h.Sequence, Value: h.Value, Tombstone: h.Tombstone}
		}
	}
	return pb
}

// MetadataFromProto converts an intent's metadata.
func MetadataFromProto(p *rpcpb.MVCCMetadata) (MVCCMetadata, error) {
	txn, err := TxnMetaFromProto(p.Txn)
	if err != nil {
		return MVCCMetadata{}, err
	}
	m := MVCCMetadata{Txn: txn, Timestamp: TimestampFromProto(p.Timestamp)}
	if len(p.History) > 0 {
		m.History = make([]IntentValue, len(p.History))
		for i, h := range p.History {
			m.History[i] = IntentValue{Sequence: h.Sequence, Value: h.Value, Tombstone: h.Tombstone}
		}
	}
	return m, nil
}
