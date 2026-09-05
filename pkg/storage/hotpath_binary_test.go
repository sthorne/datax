package storage

import (
	"encoding/binary"
	"fmt"

	"github.com/sthorne/datax/pkg/storage/enginepb"
	"github.com/sthorne/datax/pkg/util/hlc"
)

// A hand-rolled fixed-layout encoding of MVCCMetadata, used only as a
// reference point in BenchmarkIntentMetaCodec: it sizes what dropping
// encoding/json from the intent path would buy. Not a proposal for the
// wire format (protobuf via enginepb would be), just a lower bound.
//
// Layout: version(1) | uuid(16) | epoch(4) | writeTS(12) | minTS(12) |
//         priority(4) | seq(4) | ts(12) | anchorLen(uvarint) | anchor |
//         historyCount(uvarint) [| seq(4) | tombstone(1) | len | value ]*

func appendTS(b []byte, t hlc.Timestamp) []byte {
	b = binary.BigEndian.AppendUint64(b, uint64(t.WallTime))
	return binary.BigEndian.AppendUint32(b, uint32(t.Logical))
}

func readTS(b []byte) (hlc.Timestamp, []byte) {
	t := hlc.Timestamp{
		WallTime: int64(binary.BigEndian.Uint64(b[:8])),
		Logical:  int32(binary.BigEndian.Uint32(b[8:12])),
	}
	return t, b[12:]
}

func appendMetaBinary(b []byte, m enginepb.MVCCMetadata) []byte {
	b = append(b, 0x01)
	b = append(b, m.Txn.ID[:]...)
	b = binary.BigEndian.AppendUint32(b, uint32(m.Txn.Epoch))
	b = appendTS(b, m.Txn.WriteTimestamp)
	b = appendTS(b, m.Txn.MinTimestamp)
	b = binary.BigEndian.AppendUint32(b, uint32(m.Txn.Priority))
	b = binary.BigEndian.AppendUint32(b, uint32(m.Txn.Sequence))
	b = appendTS(b, m.Timestamp)
	b = binary.AppendUvarint(b, uint64(len(m.Txn.Key)))
	b = append(b, m.Txn.Key...)
	b = binary.AppendUvarint(b, uint64(len(m.History)))
	for _, h := range m.History {
		b = binary.BigEndian.AppendUint32(b, uint32(h.Sequence))
		if h.Tombstone {
			b = append(b, 1)
		} else {
			b = append(b, 0)
		}
		b = binary.AppendUvarint(b, uint64(len(h.Value)))
		b = append(b, h.Value...)
	}
	return b
}

func decodeMetaBinary(b []byte) (enginepb.MVCCMetadata, error) {
	var m enginepb.MVCCMetadata
	if len(b) < 1 || b[0] != 0x01 {
		return m, fmt.Errorf("bad meta version")
	}
	b = b[1:]
	copy(m.Txn.ID[:], b[:16])
	b = b[16:]
	m.Txn.Epoch = int32(binary.BigEndian.Uint32(b[:4]))
	b = b[4:]
	m.Txn.WriteTimestamp, b = readTS(b)
	m.Txn.MinTimestamp, b = readTS(b)
	m.Txn.Priority = int32(binary.BigEndian.Uint32(b[:4]))
	b = b[4:]
	m.Txn.Sequence = int32(binary.BigEndian.Uint32(b[:4]))
	b = b[4:]
	m.Timestamp, b = readTS(b)
	n, adv := binary.Uvarint(b)
	b = b[adv:]
	if n > 0 {
		m.Txn.Key = b[:n]
		b = b[n:]
	}
	hn, adv := binary.Uvarint(b)
	b = b[adv:]
	if hn > 0 {
		m.History = make([]enginepb.IntentValue, hn)
		for i := range m.History {
			m.History[i].Sequence = int32(binary.BigEndian.Uint32(b[:4]))
			b = b[4:]
			m.History[i].Tombstone = b[0] == 1
			b = b[1:]
			vn, adv := binary.Uvarint(b)
			b = b[adv:]
			m.History[i].Value = b[:vn]
			b = b[vn:]
		}
	}
	return m, nil
}
