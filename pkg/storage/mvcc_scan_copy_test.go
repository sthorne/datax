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

// TestMVCCReverseScanRowsAreTheirOwn: the reverse scan decodes keys into
// one reused buffer and copies rows into scan-owned chunks too. Its rows
// must equal the forward scan's, reversed, byte for byte (nil empty
// values included), survive the scan moving on and appends to their
// keys, and each page's Resume must be its own copy, not the decode
// buffer (which the next row rewrites).
func TestMVCCReverseScanRowsAreTheirOwn(t *testing.T) {
	eng := openTestEngine(t)
	for i := 0; i < 60; i++ {
		k := fmt.Sprintf("k%02d\x00%s", i, bytes.Repeat([]byte{0}, i%3))
		v := fmt.Sprintf("value-%d-%s", i, bytes.Repeat([]byte("x"), i))
		if i%10 == 0 {
			v = ""
		}
		if i%7 == 3 {
			v = string(bytes.Repeat([]byte{byte('a' + i%26)}, 5000+i))
		}
		mustPut(t, eng, k, ts(10, 0), v, nil)
		if i%4 == 1 {
			mustPut(t, eng, k, ts(12, 0), v+"-new", nil) // a second version
		}
	}
	fwd, err := MVCCScan(eng, keys.Key("k"), keys.Key("l"), ts(20, 0), 0, MVCCGetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	rev, err := MVCCReverseScan(eng, keys.Key("k"), keys.Key("l"), ts(20, 0), 0, MVCCGetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if len(rev.KVs) != len(fwd.KVs) || len(rev.KVs) != 60 {
		t.Fatalf("%d reverse rows, %d forward", len(rev.KVs), len(fwd.KVs))
	}
	for i, kv := range rev.KVs {
		f := fwd.KVs[len(fwd.KVs)-1-i]
		if !bytes.Equal(kv.Key, f.Key) || !bytes.Equal(kv.Value, f.Value) || (kv.Value == nil) != (f.Value == nil) {
			t.Fatalf("reverse row %d = %q:%q, forward has %q:%q", i, kv.Key, kv.Value, f.Key, f.Value)
		}
		before := string(kv.Value)
		_ = append(kv.Key, "clobber"...)
		if string(kv.Value) != before {
			t.Fatalf("row %d: appending to the key changed the value to %q", i, kv.Value)
		}
	}

	// Paged by max rows and by bytes: the pages, joined, are the same
	// rows, and each Resume (taken before the next page runs) survives.
	for _, opts := range []struct {
		max   int64
		bytes int64
	}{{7, 0}, {0, 9000}} {
		var got []KeyValue
		var resumes []keys.Key
		end := keys.Key("l")
		for {
			page, err := MVCCReverseScan(eng, keys.Key("k"), end, ts(20, 0), opts.max, MVCCGetOptions{TargetBytes: opts.bytes})
			if err != nil {
				t.Fatal(err)
			}
			got = append(got, page.KVs...)
			if page.Resume == nil {
				break
			}
			if !bytes.Equal(page.Resume, page.KVs[len(page.KVs)-1].Key) {
				t.Fatalf("resume %q, want the page's last key %q", page.Resume, page.KVs[len(page.KVs)-1].Key)
			}
			resumes = append(resumes, page.Resume)
			end = page.Resume
		}
		if len(got) != len(rev.KVs) {
			t.Fatalf("paged (%v): %d rows, want %d", opts, len(got), len(rev.KVs))
		}
		for i := range got {
			if !bytes.Equal(got[i].Key, rev.KVs[i].Key) || !bytes.Equal(got[i].Value, rev.KVs[i].Value) {
				t.Fatalf("paged (%v) row %d = %q, want %q", opts, i, got[i].Key, rev.KVs[i].Key)
			}
		}
		for i := 1; i < len(resumes); i++ {
			if bytes.Compare(resumes[i], resumes[i-1]) >= 0 {
				t.Fatalf("resumes %d, %d not descending: %q, %q", i-1, i, resumes[i-1], resumes[i])
			}
		}
	}
}

// TestMVCCScanAllocsPerRow: forward and reverse scans allocate per chunk,
// not per row. 500 small rows fit in a few 16 KiB chunks; a scan that
// allocated each row's key or value (the reverse scan did, twice per
// row) would make about a thousand allocations.
func TestMVCCScanAllocsPerRow(t *testing.T) {
	if raceEnabled {
		t.Skip("allocation counts are not meaningful under the race detector")
	}
	eng := openTestEngine(t)
	const rows = 500
	for i := 0; i < rows; i++ {
		mustPut(t, eng, fmt.Sprintf("r%04d", i), ts(10, 0), fmt.Sprintf("value-%d", i), nil)
	}
	for _, reverse := range []bool{false, true} {
		allocs := testing.AllocsPerRun(20, func() {
			var res ScanResult
			var err error
			if reverse {
				res, err = MVCCReverseScan(eng, keys.Key("r"), keys.Key("s"), ts(20, 0), 0, MVCCGetOptions{})
			} else {
				res, err = MVCCScan(eng, keys.Key("r"), keys.Key("s"), ts(20, 0), 0, MVCCGetOptions{})
			}
			if err != nil || len(res.KVs) != rows {
				t.Fatalf("%d rows, %v", len(res.KVs), err)
			}
		})
		if allocs > rows/10 {
			t.Errorf("reverse=%v: %.0f allocations for %d rows, want under %d", reverse, allocs, rows, rows/10)
		}
	}
}
