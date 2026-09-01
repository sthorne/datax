package storage

import (
	"bytes"
	"errors"
	"fmt"
	"testing"

	"github.com/sthorne/datax/pkg/keys"
)

func reverseScan(t *testing.T, eng *Engine, start, end string, at int64, max int64, opts MVCCGetOptions) ScanResult {
	t.Helper()
	res, err := MVCCReverseScan(eng, keys.Key(start), keys.Key(end), ts(at, 0), max, opts)
	if err != nil {
		t.Fatalf("reverse scan [%s,%s)@%d: %v", start, end, at, err)
	}
	return res
}

// TestMVCCReverseScan: reverse order, visibility at the read timestamp,
// tombstone skipping, and exact agreement with the forward scan.
func TestMVCCReverseScan(t *testing.T) {
	eng := openTestEngine(t)
	for i := 0; i < 10; i++ {
		mustPut(t, eng, fmt.Sprintf("k%02d", i), ts(10, 0), fmt.Sprintf("v%02d", i), nil)
	}
	mustPut(t, eng, "k03", ts(20, 0), "v03-new", nil) // newer version
	mustDelete(t, eng, "k05", ts(20, 0), nil)         // tombstone at 20

	// At ts 30: k05 is deleted, k03 reads its newest version, order is
	// descending, and forward-reversed equals reverse.
	res := reverseScan(t, eng, "k00", "k99", 30, 0, MVCCGetOptions{})
	if len(res.KVs) != 9 || res.Resume != nil {
		t.Fatalf("kvs=%d resume=%v", len(res.KVs), res.Resume)
	}
	for i := 1; i < len(res.KVs); i++ {
		if res.KVs[i-1].Key.Compare(res.KVs[i].Key) <= 0 {
			t.Fatalf("not descending: %s then %s", res.KVs[i-1].Key, res.KVs[i].Key)
		}
	}
	if string(res.KVs[0].Key) != "k09" || string(res.KVs[len(res.KVs)-1].Key) != "k00" {
		t.Fatalf("bounds: %s .. %s", res.KVs[0].Key, res.KVs[len(res.KVs)-1].Key)
	}
	fwd, err := MVCCScan(eng, keys.Key("k00"), keys.Key("k99"), ts(30, 0), 0, MVCCGetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if len(fwd.KVs) != len(res.KVs) {
		t.Fatalf("forward %d vs reverse %d", len(fwd.KVs), len(res.KVs))
	}
	for i := range fwd.KVs {
		r := res.KVs[len(res.KVs)-1-i]
		if !bytes.Equal(fwd.KVs[i].Key, r.Key) || !bytes.Equal(fwd.KVs[i].Value, r.Value) {
			t.Fatalf("mismatch at %d: %s=%q vs %s=%q", i, fwd.KVs[i].Key, fwd.KVs[i].Value, r.Key, r.Value)
		}
	}

	// At ts 15: k05 still alive, k03 reads the old version.
	res = reverseScan(t, eng, "k00", "k99", 15, 0, MVCCGetOptions{})
	if len(res.KVs) != 10 {
		t.Fatalf("kvs at 15: %d", len(res.KVs))
	}
	if string(res.KVs[6].Key) != "k03" || string(res.KVs[6].Value) != "v03" {
		t.Fatalf("k03 at 15: %s=%q", res.KVs[6].Key, res.KVs[6].Value)
	}

	// Sub-span bounds are honored (end exclusive).
	res = reverseScan(t, eng, "k02", "k04", 30, 0, MVCCGetOptions{})
	if len(res.KVs) != 2 || string(res.KVs[0].Key) != "k03" || string(res.KVs[1].Key) != "k02" {
		t.Fatalf("subspan: %+v", res.KVs)
	}
}

// TestMVCCReverseScanPagination: max stops early with Resume = the
// exclusive END of the continuation page; chaining pages walks the whole
// span backwards.
func TestMVCCReverseScanPagination(t *testing.T) {
	eng := openTestEngine(t)
	for i := 0; i < 10; i++ {
		mustPut(t, eng, fmt.Sprintf("k%02d", i), ts(10, 0), "v", nil)
	}
	var got []string
	end := "k99"
	for pages := 0; ; pages++ {
		if pages > 10 {
			t.Fatal("pagination did not terminate")
		}
		res := reverseScan(t, eng, "k00", end, 30, 3, MVCCGetOptions{})
		for _, kv := range res.KVs {
			got = append(got, string(kv.Key))
		}
		if res.Resume == nil {
			break
		}
		end = string(res.Resume)
	}
	if len(got) != 10 || got[0] != "k09" || got[9] != "k00" {
		t.Fatalf("paged rows: %v", got)
	}
}

// TestMVCCReverseScanIntents: a foreign intent surfaces as a
// WriteIntentError; the txn's own intent reads its provisional value.
func TestMVCCReverseScanIntents(t *testing.T) {
	eng := openTestEngine(t)
	mustPut(t, eng, "a", ts(10, 0), "va", nil)
	mustPut(t, eng, "b", ts(10, 0), "vb", nil)
	txn := newTxn(ts(20, 0))
	mustPut(t, eng, "b", ts(20, 0), "vb-mine", txn)

	// Foreign reader at 30: blocked on b's intent.
	_, err := MVCCReverseScan(eng, keys.Key("a"), keys.Key("z"), ts(30, 0), 0, MVCCGetOptions{})
	var wie *WriteIntentError
	if !errors.As(err, &wie) || len(wie.Intents) != 1 || string(wie.Intents[0].Key) != "b" {
		t.Fatalf("foreign intent: %v", err)
	}

	// The owner reads its own provisional write.
	res, err := MVCCReverseScan(eng, keys.Key("a"), keys.Key("z"), ts(30, 0), 0, MVCCGetOptions{Txn: txn})
	if err != nil {
		t.Fatal(err)
	}
	if len(res.KVs) != 2 || string(res.KVs[0].Value) != "vb-mine" || string(res.KVs[1].Value) != "va" {
		t.Fatalf("own-intent read: %+v", res.KVs)
	}

	// An inconsistent reader reads beneath the intent.
	res, err = MVCCReverseScan(eng, keys.Key("a"), keys.Key("z"), ts(30, 0), 0, MVCCGetOptions{Inconsistent: true})
	if err != nil {
		t.Fatal(err)
	}
	if len(res.KVs) != 2 || string(res.KVs[0].Value) != "vb" {
		t.Fatalf("inconsistent read: %+v", res.KVs)
	}
}
