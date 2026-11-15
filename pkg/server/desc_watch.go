package server

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/kvpb"
	"github.com/sthorne/datax/pkg/sql/catalog"
	"github.com/sthorne/datax/pkg/util/encoding"
	"github.com/sthorne/datax/pkg/util/hlc"
	"github.com/sthorne/datax/pkg/util/log"
)

// descWatcher follows the table descriptor span for one background job
// that keeps its own state about every table (issue #313, the scale
// follow-ups of #311). Those jobs — the retention provider behind GC,
// the re-shard janitor — used to read every descriptor on every pass, a
// cost that grows with the catalog however little of it changes. The
// watcher reads every descriptor once — or, resuming from the state its
// job persisted (catalogFollower, #316), none — and after that each pass
// is an incremental export of the span over the interval since the last
// one — the read an incremental backup makes — which returns only the
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

// followedState is what one job derives from the spans its descWatcher
// follows (issue #316): built from a full read, kept up to date from the
// records written since, and persisted so the job's first pass after a
// start — or, for the jobs that follow range 1's leader, after a new
// leader takes over — reads what changed since the job's last persisted
// pass instead of everything.
//
// It must be a function of the spans' contents at the watcher's
// timestamp and nothing else: a pass applies the records of (since, now]
// to the state as of since, and a full read builds it from nothing. A
// job that acts on the state afterwards changes its own copy only in
// ways the next pass's records repeat (an entry noted for a write it
// made, or removed for a key it deleted).
type followedState interface {
	// reset empties the state, before a full read.
	reset()
	// apply updates the state from one record: a key written in the
	// interval, or removed (Deleted). It reports whether the record
	// changed anything the job keeps.
	apply(rec kvpb.ExportRecord) bool
	// encode is the persisted form of the state; it is deterministic, so
	// two equal states encode alike. decode reverses it into an empty
	// state, and fails on anything it cannot read.
	encode() []byte
	decode([]byte) error
}

// resumeStore is where a catalogFollower keeps its resume record: the
// node's own engine for a job every node runs on its own behalf, the
// replicated keyspace for one that follows range 1's leader, so that a
// new leader starts where the last one left off.
type resumeStore interface {
	load(ctx context.Context) ([]byte, error)
	save(ctx context.Context, raw []byte) error
}

// localResume keeps a resume record on this node's engine, unreplicated.
type localResume struct {
	n   *Node
	key keys.Key
}

func (r localResume) load(context.Context) ([]byte, error) {
	if r.n.engine == nil {
		return nil, nil
	}
	return r.n.engine.Get(r.key)
}

func (r localResume) save(_ context.Context, raw []byte) error {
	if r.n.engine == nil {
		return nil
	}
	return r.n.engine.Put(r.key, raw)
}

// clusterResume keeps a resume record in the replicated keyspace.
type clusterResume struct {
	n   *Node
	key keys.Key
}

func (r clusterResume) load(ctx context.Context) ([]byte, error) { return r.n.db.Get(ctx, r.key) }

func (r clusterResume) save(ctx context.Context, raw []byte) error {
	return r.n.db.Put(ctx, r.key, raw)
}

// catalogFollower is a descWatcher with the job's state beside it, and
// the record that lets the job resume after a start (issue #316).
//
// The resume record is one value: the format version, the watcher's
// timestamp, and the state as of that timestamp. State and timestamp are
// written together or not at all, so a crash leaves either the new
// record or the one before, and the job resumes from a state and the
// timestamp it is as of — reading again what came after, never skipping
// it. A record is written after a pass at most every `every`, so the
// first pass after a start reads what changed in at most that long plus
// the time the node was down. Any record is a valid starting point,
// whoever wrote it and however old: two nodes that both believe they
// lead range 1 for a moment each write a state and its own timestamp.
//
// A record is not used, and the first pass reads everything, when it is
// missing, its version is not the job's (an upgrade that changes the
// state, or what a job keeps of a descriptor, bumps the version), it
// does not decode, its timestamp is too far ahead of this node's clock,
// or the interval since it has fallen below the spans' GC threshold
// (descWatcher.next).
type catalogFollower struct {
	w     descWatcher
	state followedState
	// fresh makes an empty state of the job's kind: what a full read
	// starts from (TestingCatalogFollowers compares with one).
	fresh func() followedState
	// store, version and every: where the resume record is kept, its
	// format version, and how often it is written. A nil store keeps
	// none.
	store   resumeStore
	version uint64
	every   time.Duration

	tried   bool      // the resume record has been looked for
	savedAt time.Time // when the record was last written
	// resumed is whether the state came from a resume record and has not
	// been rebuilt by a full read since.
	resumed bool
}

func newCatalogFollower(n *Node, spans []watchSpan, fresh func() followedState, store resumeStore, version uint64, every time.Duration) *catalogFollower {
	return &catalogFollower{
		w:     descWatcher{n: n, spans: spans},
		state: fresh(), fresh: fresh,
		store: store, version: version, every: every,
	}
}

// pass brings the state up to the present: on the first call from the
// resume record if there is a usable one, then from what changed since
// (or from everything, full true). changed reports whether anything the
// job keeps changed. On an error the state is as it was.
func (f *catalogFollower) pass(ctx context.Context) (full, changed bool, err error) {
	if !f.tried {
		f.tried = true
		f.resume(ctx)
	}
	full, recs, err := f.w.next(ctx)
	if err != nil {
		return false, false, err
	}
	if full {
		f.state.reset()
		f.resumed = false
		changed = true
	}
	for _, rec := range recs {
		if f.state.apply(rec) {
			changed = true
		}
	}
	if f.store != nil && (f.savedAt.IsZero() || time.Since(f.savedAt) >= f.every) {
		if err := f.store.save(ctx, encodeResume(f.version, f.w.since, f.state.encode())); err != nil {
			log.Debugf("catalog follower: saving where to resume: %v", err)
		} else {
			f.savedAt = time.Now()
		}
	}
	return full, changed, nil
}

// resume loads the resume record into the state and the watcher's
// timestamp, if it is usable; otherwise it leaves both empty, for a full
// read.
func (f *catalogFollower) resume(ctx context.Context) {
	if f.store == nil {
		return
	}
	raw, err := f.store.load(ctx)
	if err != nil || raw == nil {
		if err != nil {
			log.Debugf("catalog follower: reading where to resume: %v", err)
		}
		return
	}
	v, since, payload, ok := decodeResume(raw)
	if !ok || v != f.version || since.IsEmpty() {
		return
	}
	// The record may come from a node whose clock ran ahead of this
	// one's: reads from it must be at later timestamps. One further ahead
	// than the clock offset allows is not trusted.
	if err := f.w.n.clock.Update(since); err != nil {
		log.Debugf("catalog follower: not resuming: %v", err)
		return
	}
	st := f.fresh()
	if err := st.decode(payload); err != nil {
		log.Debugf("catalog follower: not resuming: %v", err)
		return
	}
	f.state, f.w.since, f.resumed = st, since, true
}

// fullStateAt is the state a full read of the followed spans as of ts
// derives: what f's state must equal when its watcher is at ts.
func (f *catalogFollower) fullStateAt(ctx context.Context, ts hlc.Timestamp) (followedState, error) {
	st := f.fresh()
	for _, sp := range f.w.watched() {
		if err := f.w.n.exportAll(ctx, sp.start, sp.end, hlc.Timestamp{}, ts, func(rec kvpb.ExportRecord) error {
			if !rec.Deleted {
				st.apply(rec)
			}
			return nil
		}); err != nil {
			return nil, err
		}
	}
	return st, nil
}

// check compares f's state with what a full read at its timestamp
// derives.
func (f *catalogFollower) check(ctx context.Context) error {
	if f.w.since.IsEmpty() {
		return fmt.Errorf("no pass yet")
	}
	want, err := f.fullStateAt(ctx, f.w.since)
	if err != nil {
		return err
	}
	if got := f.state.encode(); !bytes.Equal(got, want.encode()) {
		return fmt.Errorf("state as of %s differs from a full read: %d bytes, want %d", f.w.since, len(got), len(want.encode()))
	}
	return nil
}

// encodeResume is a resume record: the format version, the timestamp,
// then the state.
func encodeResume(version uint64, since hlc.Timestamp, state []byte) []byte {
	b := binary.AppendUvarint(nil, version)
	b = binary.AppendVarint(b, since.WallTime)
	b = binary.AppendVarint(b, int64(since.Logical))
	return append(b, state...)
}

func decodeResume(raw []byte) (version uint64, since hlc.Timestamp, state []byte, ok bool) {
	r := byteReader{b: raw}
	version = r.uvarint()
	since.WallTime = r.varint()
	since.Logical = int32(r.varint())
	if r.bad {
		return 0, hlc.Timestamp{}, nil, false
	}
	return version, since, r.b, true
}

// byteReader reads varints off a byte slice; bad is set, and every read
// after returns zero, once one fails.
type byteReader struct {
	b   []byte
	bad bool
}

func (r *byteReader) uvarint() uint64 {
	if r.bad {
		return 0
	}
	v, n := binary.Uvarint(r.b)
	if n <= 0 {
		r.bad = true
		return 0
	}
	r.b = r.b[n:]
	return v
}

func (r *byteReader) varint() int64 {
	if r.bad {
		return 0
	}
	v, n := binary.Varint(r.b)
	if n <= 0 {
		r.bad = true
		return 0
	}
	r.b = r.b[n:]
	return v
}

func (r *byteReader) bytes() []byte {
	n := r.uvarint()
	if r.bad || n > uint64(len(r.b)) {
		r.bad = true
		return nil
	}
	v := r.b[:n:n]
	r.b = r.b[n:]
	return v
}

// rawDescs is a followedState for a job that keeps, of the descriptors,
// only those a predicate selects, raw: the retention provider (tables
// with a GC window or retention) and the re-shard janitor (tables with a
// retired layout). Persisting the descriptors rather than what the job
// derives from them leaves the derivation to the binary that resumes;
// only a change to the predicate bumps the record's version.
type rawDescs struct {
	keep func(*catalog.TableDescriptor) bool
	byID map[uint64][]byte
}

func (s *rawDescs) reset() { s.byID = map[uint64][]byte{} }

func (s *rawDescs) apply(rec kvpb.ExportRecord) bool {
	id, ok := descIDOfKey(rec.Key)
	if !ok {
		return false
	}
	if s.byID == nil {
		s.reset()
	}
	_, had := s.byID[id]
	delete(s.byID, id)
	if rec.Deleted {
		return had
	}
	var d catalog.TableDescriptor
	if json.Unmarshal(rec.Value, &d) == nil && d.ID != 0 && s.keep(&d) {
		s.byID[id] = rec.Value
		return true
	}
	return had
}

// encode: the count, then id and descriptor length-prefixed, by id.
func (s *rawDescs) encode() []byte {
	ids := sortedIDs(s.byID)
	b := binary.AppendUvarint(nil, uint64(len(ids)))
	for _, id := range ids {
		b = binary.AppendUvarint(b, id)
		b = binary.AppendUvarint(b, uint64(len(s.byID[id])))
		b = append(b, s.byID[id]...)
	}
	return b
}

func (s *rawDescs) decode(raw []byte) error {
	r := byteReader{b: raw}
	n := r.uvarint()
	if n > uint64(len(raw)) {
		return fmt.Errorf("resume record: %d descriptors in %d bytes", n, len(raw))
	}
	s.reset()
	for i := uint64(0); i < n && !r.bad; i++ {
		id := r.uvarint()
		v := r.bytes()
		if !r.bad {
			s.byID[id] = append([]byte(nil), v...)
		}
	}
	if r.bad || len(r.b) != 0 {
		return fmt.Errorf("resume record: malformed descriptors")
	}
	return nil
}

func sortedIDs[V any](m map[uint64]V) []uint64 {
	ids := make([]uint64, 0, len(m))
	for id := range m {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids
}

// TestingCheckCatalogFollower brings the state a background job keeps
// about the catalog up to date — job is "retention" (the provider behind
// GC), "reshard" (the re-shard janitor) or "stats" (the statistics
// sampler) — without acting on it, and compares it with what a full
// read at the same timestamp derives (issue #316). resumed reports
// whether the job's state was loaded from its resume record, rather
// than built by a full read, since the job's first pass on this node.
func (n *Node) TestingCheckCatalogFollower(ctx context.Context, job string) (resumed bool, err error) {
	var mu *sync.Mutex
	var f **catalogFollower
	switch job {
	case "retention":
		mu, f = &n.retention.mu, &n.retention.follow
	case "reshard":
		mu, f = &n.reshardMu, &n.reshardFollow
	case "stats":
		mu, f = &n.statsRefresh.mu, &n.statsRefresh.follow
	default:
		return false, fmt.Errorf("no job %q", job)
	}
	mu.Lock()
	defer mu.Unlock()
	if *f == nil {
		return false, fmt.Errorf("%s: no pass yet", job)
	}
	if _, _, err := (*f).pass(ctx); err != nil {
		return (*f).resumed, err
	}
	if err := (*f).check(ctx); err != nil {
		return (*f).resumed, fmt.Errorf("%s: %w", job, err)
	}
	return (*f).resumed, nil
}
