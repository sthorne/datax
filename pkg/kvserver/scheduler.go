package kvserver

import (
	"context"
	"runtime"
	"sync"
	"time"

	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"

	"github.com/sthorne/datax/pkg/base"
	"github.com/sthorne/datax/pkg/metrics"
	"github.com/sthorne/datax/pkg/storage"
	"github.com/sthorne/datax/pkg/util/faultpoint"
)

// raftScheduler drives every raft group on a store from one fixed pool of
// workers instead of a goroutine and a ticker per replica (issue #102).
// Message receipt, proposals, read-index requests and the store ticker
// enqueue a replica; a worker dequeues a group of them, ticks or steps
// each RawNode, and handles one Ready per replica (persist — one synced
// commit for the whole group — send, and hand the committed entries to
// the apply workers). A replica is never processed by two workers at
// once: while a pass runs, new work only raises its flags, and the
// worker re-queues it when the pass ends.
//
// Applying is pipelined off the raft passes (issue #106): a second pool
// of apply workers drains each replica's queue of committed entries in
// log order, one worker per replica at a time, so a range's next append
// and sync proceed while its previous entries apply, and a slow apply on
// one range never holds the group commit of the others. A pass whose
// committed entries include a conf change applies them inline instead
// (after draining the queue): raft learns of a conf change through
// ApplyConfChange, which must happen before Advance admits the next one.
// A replica's queue is bounded (applyQueueMaxBytes): a replica over it
// is not given a pass until an apply run brings it back under, so a
// follower whose apply falls behind its leader's appends holds a bounded
// backlog in memory (and the leader's flow control holds the rest).
type raftScheduler struct {
	store   *Store
	workers int

	startOnce sync.Once
	startErr  error

	mu struct {
		sync.Mutex
		// work wakes workers (the queue grew or the scheduler stops);
		// applyWork wakes apply workers; idle wakes waiters for a pass or
		// an apply run to finish (stopReplica, drainApply, backpressure).
		work, applyWork, idle *sync.Cond
		queue                 []base.RangeID
		state                 map[base.RangeID]*schedState
		applyQueue            []base.RangeID
		apply                 map[base.RangeID]*applyState
		stopping              bool
	}
}

// applyState is a replica's queue of committed entries awaiting apply.
type applyState struct {
	pending []raftpb.Entry
	// bytes counts the queued entries and the run in progress: what the
	// replica still has to apply.
	bytes   int64
	queued  bool
	running bool
	// deferred holds the flags of a pass skipped while the queue was
	// over its bound; the apply run that empties it re-enqueues them.
	deferred raftSchedFlags
}

// applyQueueMaxBytes bounds one replica's queued committed entries; a
// replica over it gets no pass until an apply run drains it.
const applyQueueMaxBytes = 64 << 20

// raftSchedFlags say why a replica was enqueued.
type raftSchedFlags uint8

const (
	// schedTick advances the group's election and heartbeat timers.
	schedTick raftSchedFlags = 1 << iota
	// schedReady asks for a Ready check after a step, proposal, read
	// index, progress report or a previous pass that left work behind.
	schedReady
)

// schedState is a replica's place in the queue.
type schedState struct {
	flags   raftSchedFlags
	queued  bool
	running bool
	worker  int       // the worker running it
	since   time.Time // first enqueue of the pending flags
}

func newRaftScheduler(s *Store, workers int) *raftScheduler {
	if workers <= 0 {
		workers = runtime.GOMAXPROCS(0)
	}
	if workers < 2 {
		workers = 2 // a pass that waits (a merge apply on its RHS) must not starve the RHS
	}
	sc := &raftScheduler{store: s, workers: workers}
	sc.mu.work = sync.NewCond(&sc.mu.Mutex)
	sc.mu.applyWork = sync.NewCond(&sc.mu.Mutex)
	sc.mu.idle = sync.NewCond(&sc.mu.Mutex)
	sc.mu.state = make(map[base.RangeID]*schedState)
	sc.mu.apply = make(map[base.RangeID]*applyState)
	return sc
}

// start launches the raft workers, as many apply workers, and the store
// ticker (once; later calls return the first outcome).
func (sc *raftScheduler) start() error {
	sc.startOnce.Do(func() {
		st := sc.store.cfg.Stopper
		for i := 0; i < sc.workers; i++ {
			w := i
			if err := st.RunWorker(func(ctx context.Context) { sc.worker(ctx, w) }); err != nil {
				sc.startErr = err
				return
			}
			if err := st.RunWorker(sc.applyWorker); err != nil {
				sc.startErr = err
				return
			}
		}
		sc.startErr = st.RunWorker(sc.tickLoop)
	})
	return sc.startErr
}

// enqueue schedules a replica with the given flags: a queued replica
// gains them, a running one is re-queued with them when its pass ends.
func (sc *raftScheduler) enqueue(id base.RangeID, f raftSchedFlags) {
	sc.mu.Lock()
	sc.enqueueLocked(id, f, time.Now())
	sc.mu.Unlock()
	sc.mu.work.Signal()
}

func (sc *raftScheduler) enqueueLocked(id base.RangeID, f raftSchedFlags, now time.Time) {
	st := sc.mu.state[id]
	if st == nil {
		st = &schedState{}
		sc.mu.state[id] = st
	}
	if st.flags == 0 {
		st.since = now
	}
	st.flags |= f
	if !st.queued && !st.running {
		st.queued = true
		sc.mu.queue = append(sc.mu.queue, id)
	}
}

// tickLoop is the store's one ticker: every RaftTickInterval it enqueues
// a tick for every replica the store holds that is not quiescent.
func (sc *raftScheduler) tickLoop(ctx context.Context) {
	ticker := time.NewTicker(sc.store.cfg.RaftTickInterval)
	defer ticker.Stop()
	defer sc.stopAll()
	var ids []base.RangeID
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		ids = ids[:0]
		sc.store.mu.Lock()
		for id, r := range sc.store.mu.replicas {
			if !r.isQuiescent() {
				ids = append(ids, id)
			}
		}
		sc.store.mu.Unlock()
		now := time.Now()
		sc.mu.Lock()
		for _, id := range ids {
			sc.enqueueLocked(id, schedTick, now)
		}
		sc.mu.Unlock()
		sc.mu.work.Broadcast()
	}
}

// raftReadyGroupSize caps how many replicas one pass takes from the
// queue: their log writes share one synced commit, and their applies run
// back to back before the worker returns to the queue.
const raftReadyGroupSize = 64

// readyWork is one replica's share of a pass.
type readyWork struct {
	id    base.RangeID
	st    *schedState
	flags raftSchedFlags
	r     *Replica
	rd    raft.Ready
	has   bool
	err   error
}

// worker runs passes until the stopper stops. A pass drains up to
// raftReadyGroupSize replicas from the queue, takes every one's Ready,
// stages all their HardStates and entries into ONE batch and syncs it
// once (group commit: ten ranges appending in the same moment cost one
// fsync, not ten), then sends, applies and advances each in turn.
func (sc *raftScheduler) worker(ctx context.Context, worker int) {
	stop := context.AfterFunc(ctx, func() {
		sc.mu.Lock()
		sc.mu.stopping = true
		sc.mu.Unlock()
		sc.mu.work.Broadcast()
		sc.mu.applyWork.Broadcast()
		sc.mu.idle.Broadcast()
	})
	defer stop()
	group := make([]readyWork, 0, raftReadyGroupSize)
	for {
		sc.mu.Lock()
		for len(sc.mu.queue) == 0 && !sc.mu.stopping {
			sc.mu.work.Wait()
		}
		if sc.mu.stopping {
			sc.mu.Unlock()
			return
		}
		group = group[:0]
		now := time.Now()
		for len(sc.mu.queue) > 0 && len(group) < raftReadyGroupSize {
			id := sc.mu.queue[0]
			sc.mu.queue[0] = 0
			sc.mu.queue = sc.mu.queue[1:]
			st := sc.mu.state[id]
			if as := sc.mu.apply[id]; as != nil && as.bytes > sc.applyBound() {
				// Over its apply bound: no pass until it drains.
				metrics.RaftApplyBackpressure.Inc()
				as.deferred |= st.flags
				st.flags, st.queued = 0, false
				continue
			}
			st.queued, st.running, st.worker = false, true, worker
			group = append(group, readyWork{id: id, st: st, flags: st.flags})
			st.flags = 0
			metrics.RaftSchedulerLatency.Observe(now.Sub(st.since).Seconds())
		}
		sc.mu.Unlock()

		sc.runGroup(ctx, group)

		sc.mu.Lock()
		for i := range group {
			w := &group[i]
			w.st.running = false
			if w.st.flags != 0 && w.r != nil {
				w.st.queued = true
				sc.mu.queue = append(sc.mu.queue, w.id)
			} else {
				delete(sc.mu.state, w.id)
			}
		}
		sc.mu.Unlock()
		sc.mu.idle.Broadcast()
	}
}

// runGroup handles one pass's replicas: take, stage, one commit, finish.
func (sc *raftScheduler) runGroup(ctx context.Context, group []readyWork) {
	var (
		gb       *storage.Batch
		mustSync bool
		staged   int
	)
	for i := range group {
		w := &group[i]
		var ok bool
		if w.r, ok = sc.store.GetReplica(w.id); !ok {
			continue // removed since it was queued
		}
		if w.rd, w.has = w.r.takeReady(w.flags); !w.has {
			continue
		}
		if gb == nil {
			gb = sc.store.raftEngine().NewBatch()
		}
		if w.err = w.r.stageReady(gb, w.rd); w.err == nil {
			staged++
			mustSync = mustSync || w.rd.MustSync
		}
	}
	if gb != nil {
		var commitErr error
		if gb.Empty() {
			_ = gb.Close()
		} else if commitErr = gb.Commit(mustSync); commitErr == nil {
			faultpoint.Hit("raft-append")
			if mustSync {
				metrics.RaftLogSyncs.Inc()
				metrics.RaftReadiesPerSync.Observe(float64(staged))
			}
		}
		if commitErr != nil {
			for i := range group {
				if w := &group[i]; w.has && w.err == nil {
					w.err = commitErr
				}
			}
		}
	}
	for i := range group {
		w := &group[i]
		if !w.has {
			continue
		}
		metrics.RaftReadyPasses.Inc()
		if w.err == nil && w.r.raftStopped() {
			// Detached by a sibling's apply earlier in this pass (a merge
			// absorbed it): its keys are gone, nothing may write them.
			w.r.markStopped()
			continue
		}
		if w.err == nil {
			w.err = w.r.finishReady(ctx, w.rd)
		}
		if w.err != nil {
			w.r.failReady(w.err)
			continue
		}
		w.r.advanceReady(w.rd)
	}
	sc.store.sendQueuedHeartbeats(ctx)
}

// stopReplica takes a replica out of the scheduler: it waits for a pass
// in progress on another worker to finish, then marks the raft group
// stopped so no further pass, proposal or step touches it, and closes
// stoppedCh. from, when set, is a replica whose own pass is doing the
// stopping (a merge apply detaching its RHS): a pass that holds both
// does not wait for itself — the worker skips the stopped member's
// finish instead.
func (sc *raftScheduler) stopReplica(r, from *Replica) {
	sc.mu.Lock()
	for {
		st := sc.mu.state[r.rangeID]
		as := sc.mu.apply[r.rangeID]
		if (st == nil || !st.running) && (as == nil || !as.running) {
			break
		}
		if from != nil && st != nil && st.running {
			if fst := sc.mu.state[from.rangeID]; fst != nil && fst.running && fst.worker == st.worker {
				break
			}
		}
		sc.mu.idle.Wait()
	}
	// Queued entries never apply: the replica is gone (a merge absorbed
	// it, or it was removed); a restart replays a survivor's log.
	if as := sc.mu.apply[r.rangeID]; as != nil && !as.running {
		as.pending, as.bytes = nil, 0
	}
	r.raftMu.Lock()
	r.raftMu.stopped = true
	r.raftMu.Unlock()
	sc.mu.Unlock()
	r.markStopped()
}

// enqueueApply hands a pass's committed entries to the apply workers, in
// log order behind whatever the replica already has queued.
func (sc *raftScheduler) enqueueApply(r *Replica, ents []raftpb.Entry) {
	bytes := entriesBytes(ents)
	sc.mu.Lock()
	as := sc.mu.apply[r.rangeID]
	if as == nil {
		as = &applyState{}
		sc.mu.apply[r.rangeID] = as
	}
	as.pending = append(as.pending, ents...)
	as.bytes += bytes
	if !as.queued && !as.running {
		as.queued = true
		sc.mu.applyQueue = append(sc.mu.applyQueue, r.rangeID)
	}
	sc.mu.Unlock()
	sc.mu.applyWork.Signal()
}

// entriesBytes is what a slice of entries counts against the apply bound.
func entriesBytes(ents []raftpb.Entry) int64 {
	var bytes int64
	for i := range ents {
		bytes += int64(len(ents[i].Data)) + 32
	}
	return bytes
}

// drainApply waits until the replica has nothing queued and no apply run
// in progress: the caller applies inline next (a conf change). false
// when the scheduler is stopping before that, with entries still queued:
// applying past them would leave a gap.
func (sc *raftScheduler) drainApply(r *Replica) bool {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	for {
		as := sc.mu.apply[r.rangeID]
		if as == nil || (!as.running && len(as.pending) == 0) {
			return true
		}
		if sc.mu.stopping {
			return false
		}
		sc.mu.idle.Wait()
	}
}

// applyWorker runs apply runs until the stopper stops: take a replica's
// queued entries, apply them in order, re-queue the replica if more
// arrived meanwhile. A failed apply stops the group (failReady) and
// drops the rest of the queue.
func (sc *raftScheduler) applyWorker(ctx context.Context) {
	stop := context.AfterFunc(ctx, func() {
		sc.mu.Lock()
		sc.mu.stopping = true
		sc.mu.Unlock()
		sc.mu.applyWork.Broadcast()
		sc.mu.idle.Broadcast()
	})
	defer stop()
	for {
		sc.mu.Lock()
		for len(sc.mu.applyQueue) == 0 && !sc.mu.stopping {
			sc.mu.applyWork.Wait()
		}
		if sc.mu.stopping {
			sc.mu.Unlock()
			return
		}
		id := sc.mu.applyQueue[0]
		sc.mu.applyQueue[0] = 0
		sc.mu.applyQueue = sc.mu.applyQueue[1:]
		as := sc.mu.apply[id]
		if as == nil {
			sc.mu.Unlock()
			continue
		}
		as.queued = false
		as.running = true
		ents := as.pending
		as.pending = nil
		sc.mu.Unlock()

		var err error
		r, ok := sc.store.GetReplica(id)
		if ok && !r.raftStopped() {
			err = r.applyEntries(ctx, ents)
			if err != nil {
				r.failReady(err)
			}
		}

		sc.mu.Lock()
		as.running = false
		as.bytes -= entriesBytes(ents)
		if as.deferred != 0 && as.bytes <= sc.applyBound() {
			sc.enqueueLocked(id, as.deferred, time.Now())
			as.deferred = 0
			sc.mu.work.Signal()
		}
		if err == nil && ok && len(as.pending) > 0 {
			as.queued = true
			sc.mu.applyQueue = append(sc.mu.applyQueue, id)
			sc.mu.applyWork.Signal()
		} else {
			as.pending, as.bytes = nil, 0
			delete(sc.mu.apply, id)
		}
		sc.mu.Unlock()
		sc.mu.idle.Broadcast()
	}
}

// applyBound is the per-replica apply queue bound in effect.
func (sc *raftScheduler) applyBound() int64 {
	if b := sc.store.cfg.TestingKnobs.ApplyQueueMaxBytes; b > 0 {
		return b
	}
	return applyQueueMaxBytes
}

// applyQueueLen reports how many entries a replica has queued for apply
// (tests).
func (sc *raftScheduler) applyQueueLen(id base.RangeID) int {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	if as := sc.mu.apply[id]; as != nil {
		return len(as.pending)
	}
	return 0
}

// stopAll marks every replica's raft group stopped at shutdown. Stopper
// workers (this one included) exit before the closers run, so no step
// arriving over the network afterwards — a stream handler is not a
// worker — can reach raft, which would read the log from an engine the
// closers have shut.
func (sc *raftScheduler) stopAll() {
	sc.store.mu.Lock()
	replicas := make([]*Replica, 0, len(sc.store.mu.replicas))
	for _, r := range sc.store.mu.replicas {
		replicas = append(replicas, r)
	}
	sc.store.mu.Unlock()
	for _, r := range replicas {
		r.stopRaftGroup()
		r.markStopped()
	}
}

// queueLen reports the queue depth (for status and tests).
func (sc *raftScheduler) queueLen() int {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	return len(sc.mu.queue)
}
