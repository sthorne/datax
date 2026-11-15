package storage

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/sthorne/datax/pkg/util/hlc"
)

func filesHolding(t *testing.T, dir string, marker []byte) []string {
	t.Helper()
	var out []string
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		raw, err := os.ReadFile(filepath.Join(dir, e.Name()))
		if err == nil && bytes.Contains(raw, marker) {
			out = append(out, e.Name())
		}
	}
	return out
}

// TestCompactSpanAndScrubLogs: after a value is deleted, its bytes are
// still in the engine's files — in an sstable until a compaction rewrites
// it, and in a write-ahead log for as long as the engine keeps that log
// to reuse. CompactSpan and ScrubLogs are what remove them (issue #295).
func TestCompactSpanAndScrubLogs(t *testing.T) {
	dir := t.TempDir()
	eng, err := Open(dir, Options{Raft: true})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = eng.Close() }()
	marker := []byte("MARKER-Qw3Er5Ty7Ui9Op1As2Df")
	if err := eng.Put([]byte("k/1"), marker); err != nil {
		t.Fatal(err)
	}
	if err := eng.Put([]byte("k/2"), []byte("kept")); err != nil {
		t.Fatal(err)
	}
	if err := eng.Flush(); err != nil {
		t.Fatal(err)
	}
	if err := eng.Delete([]byte("k/1")); err != nil {
		t.Fatal(err)
	}
	// Deleted, flushed, and still on disk: in the sstable the delete
	// shadows, and in the log the write went through.
	if err := eng.Flush(); err != nil {
		t.Fatal(err)
	}
	held := filesHolding(t, dir, marker)
	var sst, wal bool
	for _, name := range held {
		sst = sst || strings.HasSuffix(name, ".sst")
		wal = wal || strings.HasSuffix(name, ".log")
	}
	if !sst || !wal {
		t.Fatalf("expected the marker in an sstable and in a log after the delete, found it in %v", held)
	}

	ctx := context.Background()
	if err := eng.CompactSpan(ctx, []byte("k/"), []byte("k0")); err != nil {
		t.Fatal(err)
	}
	// The engine deletes the files a compaction replaced a moment after
	// it returns.
	deadline := time.Now().Add(10 * time.Second)
	for {
		var left []string
		for _, name := range filesHolding(t, dir, marker) {
			if strings.HasSuffix(name, ".sst") {
				left = append(left, name)
			}
		}
		if len(left) == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("the marker is still in %v after CompactSpan", left)
		}
		time.Sleep(10 * time.Millisecond)
	}
	if err := eng.ScrubLogs(ctx); err != nil {
		t.Fatal(err)
	}
	if held := filesHolding(t, dir, marker); len(held) != 0 {
		t.Fatalf("the marker is still in %v after ScrubLogs", held)
	}
	// The engine is as it was to a reader, and goes on recycling logs.
	if v, err := eng.Get([]byte("k/2")); err != nil || string(v) != "kept" {
		t.Fatalf("kept key: %q %v", v, err)
	}
	if n := eng.logGate.pending(); n != 0 {
		t.Fatalf("%d log file(s) still marked after the scrub", n)
	}
	for i := 0; i < 4; i++ {
		if err := eng.Put([]byte("k/3"), []byte("more")); err != nil {
			t.Fatal(err)
		}
		if err := eng.Flush(); err != nil {
			t.Fatal(err)
		}
	}
	// An engine without a write-ahead log has nothing to scrub.
	noWAL, err := Open("", Options{DisableWAL: true})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = noWAL.Close() }()
	if err := noWAL.ScrubLogs(ctx); err != nil {
		t.Fatal(err)
	}
}

// TestMVCCSpanCensus: the count a purge reports is taken from the engine
// keys, so it sees what a read at a timestamp does not — superseded
// versions, tombstones and the values under them, intents — and stays
// inside the span it is asked about.
func TestMVCCSpanCensus(t *testing.T) {
	eng, err := Open("", Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = eng.Close() }()
	ts := func(w int64) hlc.Timestamp { return hlc.Timestamp{WallTime: w} }
	put := func(key string, w int64, value []byte) {
		t.Helper()
		b := eng.NewBatch()
		var err error
		if value == nil {
			err = MVCCDelete(b, []byte(key), ts(w), nil)
		} else {
			err = MVCCPut(b, []byte(key), ts(w), value, nil)
		}
		if err != nil {
			t.Fatal(err)
		}
		if err := b.Commit(false); err != nil {
			t.Fatal(err)
		}
	}
	put("s/a", 1, []byte("v1")) // superseded
	put("s/a", 2, []byte("v2")) // live
	put("s/b", 1, []byte("v1")) // under a tombstone
	put("s/b", 2, nil)          // tombstone
	put("s/c", 1, []byte("v1")) // live, single version
	put("t/x", 1, []byte("outside"))
	put("t/x", 2, nil)

	c, err := MVCCSpanCensus(eng, []byte("s/"), []byte("s0"))
	if err != nil {
		t.Fatal(err)
	}
	if want := (SpanCensus{LiveKeys: 2, Garbage: 2, Tombstones: 1}); c != want {
		t.Fatalf("census %+v, want %+v", c, want)
	}
	if c, err := MVCCSpanCensus(eng, []byte("u/"), []byte("u0")); err != nil || c != (SpanCensus{}) {
		t.Fatalf("empty span: %+v %v", c, err)
	}
}

// TestScrubLogsEncrypted: an encrypted store never recycles a log (a
// reused file would reuse its keystream), so the scrub has only obsolete
// files waiting to be removed; it must still finish, and leave the engine
// working.
func TestScrubLogsEncrypted(t *testing.T) {
	key := bytes.Repeat([]byte{7}, 32)
	eng, err := Open(t.TempDir(), Options{EncryptionKey: key, Raft: true})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = eng.Close() }()
	for i := 0; i < 3; i++ {
		if err := eng.Put([]byte{'k', byte(i)}, []byte("v")); err != nil {
			t.Fatal(err)
		}
		if err := eng.Flush(); err != nil {
			t.Fatal(err)
		}
	}
	ctx := context.Background()
	if err := eng.CompactSpan(ctx, []byte("k"), []byte("l")); err != nil {
		t.Fatal(err)
	}
	if err := eng.ScrubLogs(ctx); err != nil {
		t.Fatal(err)
	}
	if v, err := eng.Get([]byte{'k', 1}); err != nil || string(v) != "v" {
		t.Fatalf("after the scrub: %q %v", v, err)
	}
}
