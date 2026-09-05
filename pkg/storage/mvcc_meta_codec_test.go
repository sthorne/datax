package storage

import (
	"bytes"
	"encoding/json"
	"reflect"
	"testing"

	"github.com/google/uuid"

	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/storage/enginepb"
	"github.com/sthorne/datax/pkg/util/hlc"
)

func sampleMeta(binary bool, history int) enginepb.MVCCMetadata {
	m := enginepb.MVCCMetadata{
		Txn: enginepb.TxnMeta{
			ID: uuid.MustParse("6ba7b810-9dad-11d1-80b4-00c04fd430c8"), Key: []byte("anchor\x00key"),
			Epoch: 2, WriteTimestamp: hlc.Timestamp{WallTime: 1_700_000_000_000_000_000, Logical: 3},
			MinTimestamp: hlc.Timestamp{WallTime: 1_699_999_999_000_000_000}, Priority: 77, Sequence: 9,
			HistoryFloor: 4, BinaryMeta: binary,
		},
		Timestamp: hlc.Timestamp{WallTime: 1_700_000_000_000_000_000, Logical: 3},
	}
	for i := 0; i < history; i++ {
		m.History = append(m.History, enginepb.IntentValue{Sequence: int32(5 + i), Value: bytes.Repeat([]byte{byte(i)}, 8), Tombstone: i%2 == 1})
	}
	return m
}

// TestMetaCodecBothEncodings (issue #141): intent metadata round-trips
// through both encodings, the first byte tells them apart, a record
// written as JSON (every store before cluster version v14) decodes, and
// the binary form is a fraction of the JSON's size.
func TestMetaCodecBothEncodings(t *testing.T) {
	for _, history := range []int{0, 1, 3} {
		jm, bm := sampleMeta(false, history), sampleMeta(true, history)
		jraw, braw := encodeMeta(jm), encodeMeta(bm)
		if jraw[0] != '{' {
			t.Fatalf("JSON metadata does not open with '{': %q", jraw[:4])
		}
		if braw[0] == '{' {
			t.Fatalf("binary metadata opens with '{': %q", braw[:4])
		}
		if len(braw)*2 > len(jraw) {
			t.Fatalf("binary %d bytes vs JSON %d: expected well under half", len(braw), len(jraw))
		}
		for _, raw := range [][]byte{jraw, braw} {
			got, err := decodeMeta(raw)
			if err != nil {
				t.Fatal(err)
			}
			want := jm
			if raw[0] != '{' {
				want = bm
			}
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("round trip (history %d):\n got %+v\nwant %+v", history, got, want)
			}
		}
	}
	// A hand-written legacy JSON record, as a v13 store holds them.
	legacy, _ := json.Marshal(map[string]any{
		"txn": map[string]any{"id": "6ba7b810-9dad-11d1-80b4-00c04fd430c8", "epoch": 1, "write_ts": map[string]any{"wall": 5, "logical": 0}, "min_ts": map[string]any{"wall": 5}, "priority": 1},
		"ts":  map[string]any{"wall": 5},
	})
	got, err := decodeMeta(legacy)
	if err != nil || got.Txn.Epoch != 1 || got.Txn.BinaryMeta {
		t.Fatalf("legacy JSON: %+v, %v", got, err)
	}
	// Garbage is an error, not a panic, in either branch.
	if _, err := decodeMeta([]byte("{not json")); err == nil {
		t.Fatal("malformed JSON accepted")
	}
	if _, err := decodeMeta([]byte{0xff, 0xff, 0xff}); err == nil {
		t.Fatal("malformed protobuf accepted")
	}
}

// TestIntentEncodingFollowsTheTransaction: the intents a transaction lays
// down take its encoding; a rewrite and a savepoint rollback keep it; a
// JSON intent (pre-v14) is read and resolved by the v14 code.
func TestIntentEncodingFollowsTheTransaction(t *testing.T) {
	eng := openTestEngine(t)
	key := keys.Key("k")
	ts := hlc.Timestamp{WallTime: 100}
	for _, binary := range []bool{false, true} {
		b := eng.NewBatch()
		txn := &enginepb.TxnMeta{ID: uuid.New(), WriteTimestamp: ts, BinaryMeta: binary, HistoryFloor: 1}
		for seq := int32(1); seq <= 3; seq++ {
			txn.Sequence = seq
			if err := MVCCPut(b, key, ts, []byte{byte(seq)}, txn); err != nil {
				t.Fatal(err)
			}
		}
		metaKey, _ := mvccKeyBounds(key)
		raw, err := b.Get(metaKey)
		if err != nil {
			t.Fatal(err)
		}
		if (raw[0] == '{') == binary {
			t.Fatalf("binary=%v: metadata opens with %q", binary, raw[:1])
		}
		if err := MVCCRollbackIntent(b, key, txn.ID, 2); err != nil {
			t.Fatal(err)
		}
		if raw, err = b.Get(metaKey); err != nil || (raw[0] == '{') == binary {
			t.Fatalf("after rollback, binary=%v: %q, %v", binary, raw[:1], err)
		}
		if v, err := MVCCGet(b, key, ts, MVCCGetOptions{Txn: txn}); err != nil || !bytes.Equal(v, []byte{2}) {
			t.Fatalf("after rollback: %v, %v", v, err)
		}
		_ = b.Close()
	}
}

// BenchmarkIntentMetaCodec: the two encodings of a representative intent
// metadata record (issue #141).
func BenchmarkIntentMetaCodec(b *testing.B) {
	for _, tc := range []struct {
		name   string
		binary bool
	}{{"json", false}, {"binary", true}} {
		m := sampleMeta(tc.binary, 0)
		raw := encodeMeta(m)
		b.Run(tc.name+"/encode", func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				encodeMeta(m)
			}
		})
		b.Run(tc.name+"/decode", func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(len(raw)))
			for i := 0; i < b.N; i++ {
				if _, err := decodeMeta(raw); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkIntentPath: laying an intent and reading it back through the
// transaction, under each encoding.
func BenchmarkIntentPath(b *testing.B) {
	eng := openTestEngine(b)
	ts := hlc.Timestamp{WallTime: 100}
	for _, tc := range []struct {
		name   string
		binary bool
	}{{"json", false}, {"binary", true}} {
		b.Run(tc.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				bt := eng.NewBatch()
				key := keys.Key("k" + string(rune('a'+i%26)))
				txn := &enginepb.TxnMeta{ID: uuid.New(), WriteTimestamp: ts, BinaryMeta: tc.binary, HistoryFloor: -1, Sequence: 1}
				if err := MVCCPut(bt, key, ts, []byte("value-of-128-bytes-................................................................................................"), txn); err != nil {
					b.Fatal(err)
				}
				txn.Sequence = 2
				if err := MVCCPut(bt, key, ts, []byte("second"), txn); err != nil {
					b.Fatal(err)
				}
				if _, err := MVCCGet(bt, key, ts, MVCCGetOptions{Txn: txn}); err != nil {
					b.Fatal(err)
				}
				_ = bt.Close()
			}
		})
	}
}
