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

// TestMVCCExportRevisions: an export of every revision returns each
// version in the window, newest first within a key, with its timestamp —
// including the versions an ordinary export skips because a newer one
// exists — and never splits a key's versions across a page.
func TestMVCCExportRevisions(t *testing.T) {
	eng := openTestEngine(t)
	// History:
	//   a: v1@10, v2@25, v3@30    (two versions inside (20, 40])
	//   b: v1@10                  (untouched since 10)
	//   c: v1@22, deleted@35, v2@38
	//   d: created@50             (after endTS)
	mustPut(t, eng, "a", ts(10, 0), "a1", nil)
	mustPut(t, eng, "b", ts(10, 0), "b1", nil)
	mustPut(t, eng, "c", ts(22, 0), "c1", nil)
	mustPut(t, eng, "a", ts(25, 0), "a2", nil)
	mustPut(t, eng, "a", ts(30, 0), "a3", nil)
	mustDelete(t, eng, "c", ts(35, 0), nil)
	mustPut(t, eng, "c", ts(38, 0), "c2", nil)
	mustPut(t, eng, "d", ts(50, 0), "d1", nil)

	want := []ExportedRecord{
		{Key: keys.Key("a"), Value: []byte("a3"), Timestamp: ts(30, 0)},
		{Key: keys.Key("a"), Value: []byte("a2"), Timestamp: ts(25, 0)},
		{Key: keys.Key("c"), Value: []byte("c2"), Timestamp: ts(38, 0)},
		{Key: keys.Key("c"), Deleted: true, Timestamp: ts(35, 0)},
		{Key: keys.Key("c"), Value: []byte("c1"), Timestamp: ts(22, 0)},
	}
	check := func(got []ExportedRecord, want []ExportedRecord) {
		t.Helper()
		assertRecords(t, got, want)
		for i := range want {
			if got[i].Timestamp != want[i].Timestamp {
				t.Fatalf("record %d (%s): timestamp %s, want %s", i, got[i].Key, got[i].Timestamp, want[i].Timestamp)
			}
		}
	}
	res, err := MVCCExportRevisions(eng, keys.Key("a"), keys.Key("z"), ts(20, 0), ts(40, 0), 0)
	if err != nil {
		t.Fatal(err)
	}
	check(res.Records, want)

	// The window's edges: a version exactly at startTS is outside, one
	// exactly at endTS inside.
	res, err = MVCCExportRevisions(eng, keys.Key("a"), keys.Key("z"), ts(25, 0), ts(38, 0), 0)
	if err != nil {
		t.Fatal(err)
	}
	check(res.Records, []ExportedRecord{want[0], want[2], want[3]})

	// Paging stops between keys only: max=1 still returns both of a's
	// versions, and the resume key continues at c.
	res, err = MVCCExportRevisions(eng, keys.Key("a"), keys.Key("z"), ts(20, 0), ts(40, 0), 1)
	if err != nil {
		t.Fatal(err)
	}
	check(res.Records, want[:2])
	if res.Resume == nil {
		t.Fatal("paged export of revisions: no resume key")
	}
	res, err = MVCCExportRevisions(eng, res.Resume, keys.Key("z"), ts(20, 0), ts(40, 0), 1)
	if err != nil {
		t.Fatal(err)
	}
	check(res.Records, want[2:])

	// An ordinary export stamps each record with the newest version's
	// timestamp, its commit time (issue #316): a's v3 at 30 and c's v2
	// at 38.
	res, err = MVCCExport(eng, keys.Key("a"), keys.Key("z"), ts(20, 0), ts(40, 0), 0)
	if err != nil {
		t.Fatal(err)
	}
	wantTS := map[string]hlc.Timestamp{"a": ts(30, 0), "c": ts(38, 0)}
	if len(res.Records) != len(wantTS) {
		t.Fatalf("ordinary export: %+v", res.Records)
	}
	for _, rec := range res.Records {
		if rec.Timestamp != wantTS[string(rec.Key)] {
			t.Fatalf("an ordinary export stamped %s with %s, want %s", rec.Key, rec.Timestamp, wantTS[string(rec.Key)])
		}
	}
}
