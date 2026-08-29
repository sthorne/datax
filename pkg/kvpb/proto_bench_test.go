package kvpb

import (
	"encoding/json"
	"testing"

	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/util/hlc"
)

// A typical transactional single-put batch, the hot-path shape.
func benchBatch() *BatchRequest {
	ba := &BatchRequest{Header: BatchHeader{
		Timestamp: hlc.Timestamp{WallTime: 1234567890123456789},
		Txn:       testTxn(),
		RangeID:   42,
	}}
	ba.Add(&PutRequest{
		RequestHeader: RequestHeader{Key: keys.Key("/table/52/1/some-primary-key/0")},
		Value:         []byte("0123456789abcdef0123456789abcdef"),
	})
	return ba
}

func BenchmarkBatchRequestJSON(b *testing.B) {
	ba := benchBatch()
	data, _ := json.Marshal(ba)
	b.ReportMetric(float64(len(data)), "wire-bytes")
	for i := 0; i < b.N; i++ {
		data, err := json.Marshal(ba)
		if err != nil {
			b.Fatal(err)
		}
		var out BatchRequest
		if err := json.Unmarshal(data, &out); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkBatchRequestProto(b *testing.B) {
	ba := benchBatch()
	data, _ := MarshalBatchRequest(ba)
	b.ReportMetric(float64(len(data)), "wire-bytes")
	for i := 0; i < b.N; i++ {
		data, err := MarshalBatchRequest(ba)
		if err != nil {
			b.Fatal(err)
		}
		if _, err := UnmarshalBatchRequest(data); err != nil {
			b.Fatal(err)
		}
	}
}
