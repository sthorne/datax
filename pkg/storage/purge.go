package storage

import (
	"context"
	"errors"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/cockroachdb/pebble/v2/vfs"

	"github.com/sthorne/datax/pkg/util/hlc"
)

// What a purge needs of an engine (issue #295). Garbage collection
// deletes versions the way everything is deleted in an LSM: it writes
// tombstones over them. The bytes stay in the sstables until a compaction
// rewrites those files, and that happens when the engine finds it worth
// doing, which for a cold span may be never. A purge cannot wait on that,
// so it asks: CompactSpan rewrites the files a span touches, ScrubLogs
// retires the write-ahead logs that may still hold what was deleted, and
// MVCCSpanCensus counts what the span holds afterwards, read below the
// MVCC layer.

// CompactSpan flushes the memtable and compacts every file overlapping
// [start, end) into the lowest level the span reaches. A deletion and the
// value under it meet in that compaction and both are dropped, so the
// rewritten files hold neither; the files they replace are deleted by the
// engine once no iterator pins them.
func (e *Engine) CompactSpan(ctx context.Context, start, end []byte) error {
	if err := e.Flush(); err != nil {
		return err
	}
	return e.db.Compact(ctx, start, end, false)
}

// ScrubLogs retires every write-ahead log file the engine holds now. A
// flush makes a log obsolete, but the engine keeps obsolete logs to
// reuse: a recycled log is overwritten from its start and keeps its old
// content beyond wherever the new writes have reached, which may be for
// as long as the store is quiet. ScrubLogs rolls the log until each file
// that existed when it was called has been deleted, or replaced by a new
// file under its successor's name instead of being reused. A no-op on an
// engine without a write-ahead log.
func (e *Engine) ScrubLogs(ctx context.Context) error {
	if e.walDisabled || e.logGate == nil {
		return nil
	}
	if err := e.logGate.begin(); err != nil {
		return err
	}
	defer e.logGate.end()
	// Each flush rolls the log once and makes its predecessor obsolete;
	// the engine notes that shortly after the flush returns, so a round
	// that retired nothing is followed by a short wait, not a failure.
	for i := 0; e.logGate.pending() > 0; i++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		if i >= scrubMaxRounds {
			return errors.New("write-ahead logs from before the purge are still on disk after rolling the log repeatedly")
		}
		if err := e.Flush(); err != nil {
			return err
		}
		if e.logGate.pending() > 0 {
			time.Sleep(scrubRoundWait)
		}
	}
	return nil
}

const (
	scrubMaxRounds = 64
	scrubRoundWait = 5 * time.Millisecond
)

// logGate is the file system an engine opens over. It passes everything
// through except the reuse of a log file a scrub has marked: that file is
// removed and a new one created in its place.
type logGate struct {
	vfs.FS
	dir string

	mu     sync.Mutex
	marked map[string]struct{}
}

func newLogGate(fs vfs.FS, dir string) *logGate {
	return &logGate{FS: fs, dir: dir, marked: map[string]struct{}{}}
}

// begin marks every log file now in the engine's directory.
func (g *logGate) begin() error {
	names, err := g.FS.List(g.dir)
	if err != nil {
		return err
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	for _, n := range names {
		if strings.HasSuffix(n, ".log") {
			g.marked[g.FS.PathJoin(g.dir, n)] = struct{}{}
		}
	}
	return nil
}

func (g *logGate) end() {
	g.mu.Lock()
	g.marked = map[string]struct{}{}
	g.mu.Unlock()
}

func (g *logGate) pending() int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return len(g.marked)
}

func (g *logGate) unmark(name string) bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	_, ok := g.marked[name]
	delete(g.marked, name)
	return ok
}

func (g *logGate) Remove(name string) error {
	err := g.FS.Remove(name)
	if err == nil || errors.Is(err, os.ErrNotExist) {
		g.unmark(name)
	}
	return err
}

func (g *logGate) ReuseForWrite(oldname, newname string, category vfs.DiskWriteCategory) (vfs.File, error) {
	if !g.unmark(oldname) {
		return g.FS.ReuseForWrite(oldname, newname, category)
	}
	if err := g.FS.Remove(oldname); err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	return g.FS.Create(newname, category)
}

// Unwrap returns the file system beneath (Pebble walks the chain for the
// base file system's disk-health and category bookkeeping).
func (g *logGate) Unwrap() vfs.FS { return g.FS }

// SpanCensus is what an MVCC key span holds, counted from the raw engine
// keys rather than through a read at a timestamp.
type SpanCensus struct {
	// LiveKeys is the number of user keys whose newest version is a
	// value: what a current read sees.
	LiveKeys int64
	// Garbage is the number of versions lying beneath a newer version of
	// the same key, Tombstones the number of deletion markers, and
	// Intents the number of keys with an unresolved transactional write.
	// A purged span holds none of the three.
	Garbage    int64
	Tombstones int64
	Intents    int64
}

// MVCCSpanCensus scans the engine keys of the user-key span [start, end)
// and counts them by kind.
func MVCCSpanCensus(r Reader, start, end []byte) (SpanCensus, error) {
	var c SpanCensus
	lower := EncodeMVCCKey(start, hlc.Timestamp{})
	upper := EncodeMVCCKey(end, hlc.Timestamp{})
	it := r.NewIter(lower, upper)
	defer func() { _ = it.Close() }()
	var cur []byte
	seen := false // a version of cur has been counted
	for ok := it.SeekGE(lower); ok; ok = it.Next() {
		userKey, ts, err := DecodeMVCCKey(it.Key())
		if err != nil {
			return c, err
		}
		if cur == nil || string(cur) != string(userKey) {
			cur = append(cur[:0], userKey...)
			seen = false
		}
		if ts.IsEmpty() {
			c.Intents++
			continue
		}
		tomb, err := IsTombstoneValue(it.Value())
		if err != nil {
			return c, err
		}
		switch {
		case tomb:
			c.Tombstones++
		case seen:
			c.Garbage++
		default:
			c.LiveKeys++
		}
		seen = true
	}
	return c, nil
}
