package storage

import (
	"encoding/binary"
	"fmt"

	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/util/encoding"
	"github.com/sthorne/datax/pkg/util/hlc"
)

// MVCC key encoding. For a user key K the engine stores:
//
//	metadata (intent) key:  escape(K) 0x00 0x01
//	version key:            escape(K) 0x00 0x01 ^wall(8B BE) ^logical(4B BE)
//
// Properties, all under plain bytes.Compare:
//   - the metadata key sorts immediately before every version of K
//     (it is a strict prefix of them);
//   - versions of K sort newest-timestamp-first (bit-inverted timestamps);
//   - keys of different user keys never interleave (escaping gives the
//     prefix property).
//
// So a single forward scan sees: K's intent (if any), K's versions newest to
// oldest, then the next user key.

const versionSuffixLen = 12

// EncodeMVCCKey encodes a user key with a version timestamp. A zero
// timestamp encodes the metadata (intent) key.
func EncodeMVCCKey(key keys.Key, ts hlc.Timestamp) []byte {
	b := encoding.EncodeBytes(nil, key)
	if ts.IsEmpty() {
		return b
	}
	return appendVersionSuffix(b, ts)
}

func appendVersionSuffix(b []byte, ts hlc.Timestamp) []byte {
	b = binary.BigEndian.AppendUint64(b, ^uint64(ts.WallTime))
	b = binary.BigEndian.AppendUint32(b, ^uint32(ts.Logical))
	return b
}

// DecodeMVCCKey splits an encoded engine key into user key and version
// timestamp (zero for a metadata key).
func DecodeMVCCKey(enc []byte) (keys.Key, hlc.Timestamp, error) {
	rest, user, err := encoding.DecodeBytes(enc)
	if err != nil {
		return nil, hlc.Timestamp{}, err
	}
	switch len(rest) {
	case 0:
		return user, hlc.Timestamp{}, nil
	case versionSuffixLen:
		wall := ^binary.BigEndian.Uint64(rest[:8])
		logical := ^binary.BigEndian.Uint32(rest[8:12])
		return user, hlc.Timestamp{WallTime: int64(wall), Logical: int32(logical)}, nil
	default:
		return nil, hlc.Timestamp{}, fmt.Errorf("malformed MVCC key %x: version suffix of %d bytes", enc, len(rest))
	}
}

// MVCC value encoding: a one-byte header distinguishes deletion tombstones
// from real (possibly empty) values.

const (
	valueHeaderTombstone byte = 0x00
	valueHeaderData      byte = 0x01
)

func encodeMVCCValue(data []byte, tombstone bool) []byte {
	if tombstone {
		return []byte{valueHeaderTombstone}
	}
	out := make([]byte, 1+len(data))
	out[0] = valueHeaderData
	copy(out[1:], data)
	return out
}

// IsTombstoneValue reports whether a raw MVCC value is a deletion
// tombstone (used by GC's garbage enumeration).
func IsTombstoneValue(raw []byte) (bool, error) {
	_, tomb, err := decodeMVCCValue(raw)
	return tomb, err
}

func decodeMVCCValue(raw []byte) (data []byte, tombstone bool, err error) {
	if len(raw) == 0 {
		return nil, false, fmt.Errorf("malformed MVCC value: empty")
	}
	switch raw[0] {
	case valueHeaderTombstone:
		return nil, true, nil
	case valueHeaderData:
		return raw[1:], false, nil
	default:
		return nil, false, fmt.Errorf("malformed MVCC value: header 0x%02x", raw[0])
	}
}
