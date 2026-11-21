package storage

import (
	"bytes"
	"errors"
	"fmt"
	"testing"

	"github.com/sthorne/datax/pkg/keys"
)

// TestMVCCScanRowsAreTheirOwn: a scan decodes every row's key into one
// reused buffer and copies each row's key and value off it and the
// iterator into chunks it allocates for the scan; what it returns must
// still be each row's own. Keys (NULs included, which the key encoding escapes) and
// values survive the scan moving on; appending to a returned key does
// not run into its value; an empty value stays nil, as it was; and the
// keys of the intents a scan reports are their own too.
func TestMVCCScanRowsAreTheirOwn(t *testing.T) {
	eng := openTestEngine(t)
	want := map[string]string{}
	for i := 0; i < 50; i++ {
		k := fmt.Sprintf("k%02d\x00%s", i, bytes.Repeat([]byte{0}, i%3))
		v := fmt.Sprintf("value-%d-%s", i, bytes.Repeat([]byte("x"), i))
		if i%10 == 0 {
			v = "" // an empty, live value
		}
		if i%7 == 3 {
			v = string(bytes.Repeat([]byte{byte('a' + i%26)}, 5000+i)) // larger than a quarter chunk
		}
		mustPut(t, eng, k, ts(10, 0), v, nil)
		want[k] = v
	}
	res, err := MVCCScan(eng, keys.Key("k"), keys.Key("l"), ts(20, 0), 0, MVCCGetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if len(res.KVs) != len(want) {
		t.Fatalf("%d rows, want %d", len(res.KVs), len(want))
	}
	for i, kv := range res.KVs {
		w, ok := want[string(kv.Key)]
		if !ok || string(kv.Value) != w {
			t.Fatalf("row %d: %q = %q", i, kv.Key, kv.Value)
		}
		if w == "" && kv.Value != nil {
			t.Fatalf("row %d: an empty value came back as %#v, want nil", i, kv.Value)
		}
		before := string(kv.Value)
		_ = append(kv.Key, "clobber"...)
		if string(kv.Value) != before {
			t.Fatalf("row %d: appending to the key changed the value to %q", i, kv.Value)
		}
	}
	for i := 1; i < len(res.KVs); i++ {
		if bytes.Compare(res.KVs[i-1].Key, res.KVs[i].Key) >= 0 {
			t.Fatalf("rows %d and %d out of order: %q, %q", i-1, i, res.KVs[i-1].Key, res.KVs[i].Key)
		}
	}

	// Two foreign intents, then more rows: each intent keeps its key.
	txn := newTxn(ts(30, 0))
	mustPut(t, eng, "m1", txn.WriteTimestamp, "p1", txn)
	mustPut(t, eng, "m2", txn.WriteTimestamp, "p2", txn)
	mustPut(t, eng, "m3", ts(10, 0), "v3", nil)
	_, err = MVCCScan(eng, keys.Key("m"), keys.Key("n"), ts(40, 0), 0, MVCCGetOptions{})
	var wie *WriteIntentError
	if !errors.As(err, &wie) || len(wie.Intents) != 2 || string(wie.Intents[0].Key) != "m1" || string(wie.Intents[1].Key) != "m2" {
		t.Fatalf("want intents on m1 and m2, got %v", err)
	}
}
