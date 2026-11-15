package server

import (
	"context"
	"errors"

	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/kvpb"
	"github.com/sthorne/datax/pkg/util/encoding"
	"github.com/sthorne/datax/pkg/util/hlc"
)

// descWatcher follows the table descriptor span for one background job
// that keeps its own state about every table (issue #313, the scale
// follow-ups of #311). Those jobs — the retention provider behind GC,
// the re-shard janitor — used to read every descriptor on every pass, a
// cost that grows with the catalog however little of it changes. The
// watcher reads every descriptor once; after that each pass is an
// incremental export of the span over the interval since the last one —
// the read an incremental backup makes — which returns only the
// descriptors written or removed in it. Whatever the job derives from a
// descriptor it updates from those.
//
// The export window is (since, now]. It is a read at now, so no write can
// commit at or below now afterwards; an intent in the window is waited
// out (exportSpan), and one that commits above now is in the next
// window. When the window cannot be read — it opens below the span's GC
// threshold because the job did not run for a GC window, say — the
// watcher reads everything again and says so.
//
// A job that keeps state about more than the descriptors — the
// statistics sampler (#316) also follows the statistics records — names
// its spans; they are all read as of one timestamp, so what the job
// derives from one span agrees with what it derives from the others.
type descWatcher struct {
	n *Node
	// spans are the spans followed; nil is the descriptor span alone.
	spans []watchSpan
	// since is the timestamp the job's state is as of; zero before the
	// first pass.
	since hlc.Timestamp
}

// watchSpan is one [start, end) span a descWatcher follows.
type watchSpan struct{ start, end keys.Key }

func (w *descWatcher) watched() []watchSpan {
	if len(w.spans) > 0 {
		return w.spans
	}
	lo, hi := keys.TableDescSpan()
	return []watchSpan{{lo, hi}}
}

// next returns the descriptors written since the last pass, or, with
// full true, every descriptor: the job then rebuilds its state from them
// rather than updating it. A removed descriptor comes as a record with
// Deleted set. On an error the job keeps its state, and the next pass
// reads the same interval again.
func (w *descWatcher) next(ctx context.Context) (full bool, recs []kvpb.ExportRecord, err error) {
	spans := w.watched()
	now := w.n.clock.Now()
	if !w.since.IsEmpty() {
		var err error
		for _, sp := range spans {
			if err = w.n.exportAll(ctx, sp.start, sp.end, w.since, now, func(rec kvpb.ExportRecord) error {
				recs = append(recs, rec)
				return nil
			}); err != nil {
				break
			}
		}
		var kerr *kvpb.Error
		switch {
		case err == nil:
			w.since = now
			return false, recs, nil
		case errors.As(err, &kerr) && kerr.IsBelowGCThreshold():
			recs = nil // the interval is gone: start over
		default:
			return false, nil, err
		}
	}
	for _, sp := range spans {
		if err := w.n.exportAll(ctx, sp.start, sp.end, hlc.Timestamp{}, now, func(rec kvpb.ExportRecord) error {
			if !rec.Deleted {
				recs = append(recs, rec)
			}
			return nil
		}); err != nil {
			return true, nil, err
		}
	}
	w.since = now
	return true, recs, nil
}

// descIDOfKey is the table id a descriptor key names.
func descIDOfKey(k keys.Key) (uint64, bool) {
	lo, _ := keys.TableDescSpan()
	if len(k) <= len(lo) || string(k[:len(lo)]) != string(lo) {
		return 0, false
	}
	_, id, err := encoding.DecodeUint64(k[len(lo):])
	return id, err == nil
}
