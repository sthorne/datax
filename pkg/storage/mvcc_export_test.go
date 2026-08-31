package storage

import (
	"errors"
	"testing"

	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/util/hlc"
)

// TestMVCCExportWindow: full and incremental window semantics — newest
// version at or below endTS, only for keys that changed in (startTS,
// endTS], deletions surfacing as tombstone records.
func TestMVCCExportWindow(t *testing.T) {
	eng := openTestEngine(t)
	// History:
	//   a: v1@10, v2@30           (updated inside the window (20, 40])
	//   b: v1@10                  (untouched since 10)
	//   c: v1@25, deleted@35      (created and deleted inside the window)
	//   d: created@50             (after endTS)
	//   e: deleted@15             (deleted before startTS)
	mustPut(t, eng, "a", ts(10, 0), "a1", nil)
	mustPut(t, eng, "b", ts(10, 0), "b1", nil)
	mustPut(t, eng, "e", ts(10, 0), "e1", nil)
	mustDelete(t, eng, "e", ts(15, 0), nil)
	mustPut(t, eng, "c", ts(25, 0), "c1", nil)
	mustPut(t, eng, "a", ts(30, 0), "a2", nil)
	mustDelete(t, eng, "c", ts(35, 0), nil)
	mustPut(t, eng, "d", ts(50, 0), "d1", nil)

	// Full export at 40: the live set plus tombstones for dead keys.
	res, err := MVCCExport(eng, keys.Key("a"), keys.Key("z"), hlc.Timestamp{}, ts(40, 0), 0)
	if err != nil {
		t.Fatal(err)
	}
	want := []ExportedRecord{
		{Key: keys.Key("a"), Value: []byte("a2")},
		{Key: keys.Key("b"), Value: []byte("b1")},
		{Key: keys.Key("c"), Deleted: true},
		{Key: keys.Key("e"), Deleted: true},
	}
	assertRecords(t, res.Records, want)

	// Incremental (20, 40]: a's update, c's deletion; b and e unchanged, d
	// invisible.
	res, err = MVCCExport(eng, keys.Key("a"), keys.Key("z"), ts(20, 0), ts(40, 0), 0)
	if err != nil {
		t.Fatal(err)
	}
	want = []ExportedRecord{
		{Key: keys.Key("a"), Value: []byte("a2")},
		{Key: keys.Key("c"), Deleted: true},
	}
	assertRecords(t, res.Records, want)

	// Resume: max=1 stops after a with a resume key that continues to c.
	res, err = MVCCExport(eng, keys.Key("a"), keys.Key("z"), ts(20, 0), ts(40, 0), 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Records) != 1 || !res.Records[0].Key.Equal(keys.Key("a")) || res.Resume == nil {
		t.Fatalf("paged export: %+v", res)
	}
	res, err = MVCCExport(eng, res.Resume, keys.Key("z"), ts(20, 0), ts(40, 0), 0)
	if err != nil {
		t.Fatal(err)
	}
	assertRecords(t, res.Records, []ExportedRecord{{Key: keys.Key("c"), Deleted: true}})
}

// TestMVCCExportIntents: an intent at or below endTS conflicts (it may
// commit inside the window); one above endTS is read beneath.
func TestMVCCExportIntents(t *testing.T) {
	eng := openTestEngine(t)
	mustPut(t, eng, "a", ts(10, 0), "a1", nil)
	mustPut(t, eng, "b", ts(10, 0), "b1", nil)

	txnLow := newTxn(ts(20, 0))
	mustPut(t, eng, "a", ts(20, 0), "a-provisional", txnLow)

	_, err := MVCCExport(eng, keys.Key("a"), keys.Key("z"), hlc.Timestamp{}, ts(40, 0), 0)
	var wie *WriteIntentError
	if !errors.As(err, &wie) || len(wie.Intents) != 1 || !wie.Intents[0].Key.Equal(keys.Key("a")) {
		t.Fatalf("expected intent conflict on a, got %v", err)
	}

	// An intent strictly above endTS cannot commit at or below it: the
	// export reads beneath.
	txnHigh := newTxn(ts(60, 0))
	mustPut(t, eng, "b", ts(60, 0), "b-provisional", txnHigh)
	res, err := MVCCExport(eng, keys.Key("b"), keys.Key("z"), hlc.Timestamp{}, ts(40, 0), 0)
	if err != nil {
		t.Fatal(err)
	}
	assertRecords(t, res.Records, []ExportedRecord{{Key: keys.Key("b"), Value: []byte("b1")}})
}

func assertRecords(t *testing.T, got, want []ExportedRecord) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("got %d records %+v, want %d %+v", len(got), got, len(want), want)
	}
	for i := range want {
		g, w := got[i], want[i]
		if !g.Key.Equal(w.Key) || g.Deleted != w.Deleted || string(g.Value) != string(w.Value) {
			t.Fatalf("record %d: got %+v, want %+v", i, g, w)
		}
	}
}
