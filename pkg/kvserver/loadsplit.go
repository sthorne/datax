package kvserver

import (
	"math/rand/v2"
	"sort"
	"sync"
	"time"

	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/kvpb"
)

// Load-based splitting. A range serving sustained hot traffic below the
// size threshold never splits on size alone, and one raft group then
// serializes all of it. Each replica tracks its own request rate and a
// small sample of request keys — leader-local and UNREPLICATED, in
// deliberate contrast to SizeBytes (replicated state): load is a property
// of this leader's tenure, resets on leadership changes, and must never
// enter the replicated state machine. The housekeeping pass splits a
// mature, sustained-hot range at a sampled key that balances observed
// traffic, and the merge pass refuses to undo it while the halves are hot
// or too young to have proven themselves cold.

const (
	// loadRateWindow is the request-rate measurement window (two-bucket
	// sliding approximation).
	loadRateWindow = 10 * time.Second
	// loadSampleSize is the request-key reservoir size. Every recorded
	// request updates each sample's left/right counter, so this also
	// bounds per-request comparisons.
	loadSampleSize = 32
	// DefaultLoadSplitThreshold is the sustained QPS above which a range
	// is split by load (LoadSplitThreshold 0 selects it; negative
	// disables load splitting).
	DefaultLoadSplitThreshold = 500
)

// loadSample is one reservoir-sampled request start key with counts of
// subsequent traffic observed strictly left of it vs at-or-right of it.
type loadSample struct {
	key         keys.Key
	left, right int64
}

// replicaLoad is a replica's leader-local load tracker. It has its own
// tiny mutex: r.mu is contended on the hot path and must not widen.
type replicaLoad struct {
	nowFn func() int64 // UnixNano; test clock via TestingKnobs.LoadNowNanos

	mu          sync.Mutex
	cur, prev   int64 // request counts: current and previous window
	windowStart int64
	rotations   int64 // full windows completed since the last reset
	seen        int64 // keys offered to the reservoir since the last reset
	samples     []loadSample
	// lastLoadSplitAt is stamped (leader-locally, on both halves) when a
	// LOAD split lands, and guards those halves from re-merging until
	// their fresh trackers have had time to prove them cold. Size and
	// manual splits do not stamp it — their merge behavior is unchanged.
	lastLoadSplitAt int64
}

func (l *replicaLoad) init(nowFn func() int64) {
	l.nowFn = nowFn
	l.mu.Lock()
	l.windowStart = nowFn()
	l.mu.Unlock()
}

// rotateLocked advances the two-bucket window to contain now.
func (l *replicaLoad) rotateLocked(now int64) {
	w := loadRateWindow.Nanoseconds()
	if l.windowStart == 0 {
		l.windowStart = now
		return
	}
	elapsed := now - l.windowStart
	if elapsed < w {
		return
	}
	if elapsed < 2*w {
		l.prev, l.cur = l.cur, 0
		l.windowStart += w
	} else {
		// Idle gap: both windows are stale.
		l.prev, l.cur = 0, 0
		l.windowStart = now
	}
	l.rotations++
}

// record counts one batch and offers its start key to the reservoir.
func (l *replicaLoad) record(start keys.Key) {
	now := l.nowFn()
	l.mu.Lock()
	defer l.mu.Unlock()
	l.rotateLocked(now)
	l.cur++
	// Left/right accounting against the existing samples — this is what
	// lets chooseSplitKey pick a traffic-balancing key.
	for i := range l.samples {
		if start.Compare(l.samples[i].key) < 0 {
			l.samples[i].left++
		} else {
			l.samples[i].right++
		}
	}
	// Reservoir sampling (Algorithm R).
	l.seen++
	if len(l.samples) < loadSampleSize {
		l.samples = append(l.samples, loadSample{key: start.Clone()})
	} else if j := rand.Int64N(l.seen); j < loadSampleSize {
		l.samples[j] = loadSample{key: start.Clone()}
	}
}

// qps returns the sliding-window request rate and whether the tracker is
// mature (has observed at least one full window).
func (l *replicaLoad) qps() (float64, bool) {
	now := l.nowFn()
	l.mu.Lock()
	defer l.mu.Unlock()
	l.rotateLocked(now)
	w := loadRateWindow.Seconds()
	frac := float64(now-l.windowStart) / float64(loadRateWindow.Nanoseconds())
	if frac > 1 {
		frac = 1
	}
	rate := (float64(l.prev)*(1-frac) + float64(l.cur)) / w
	return rate, l.rotations >= 1
}

// chooseSplitKey returns the sampled key that best balances observed
// traffic (minimizing |left − right|), clamped strictly inside
// (desc.StartKey, desc.EndKey). Nil when no sample splits the traffic —
// e.g. every request hits one key — and the caller falls back to the byte
// midpoint.
func (l *replicaLoad) chooseSplitKey(desc kvpb.RangeDescriptor) keys.Key {
	l.mu.Lock()
	defer l.mu.Unlock()
	var best keys.Key
	var bestImbalance int64 = -1
	for i := range l.samples {
		s := &l.samples[i]
		if s.left == 0 || s.right == 0 {
			continue // splitting here would leave one cold half
		}
		if s.key.Compare(desc.StartKey) <= 0 || s.key.Compare(desc.EndKey) >= 0 {
			continue
		}
		imbalance := s.left - s.right
		if imbalance < 0 {
			imbalance = -imbalance
		}
		if bestImbalance < 0 || imbalance < bestImbalance {
			bestImbalance = imbalance
			best = s.key
		}
	}
	if best == nil {
		return nil
	}
	return best.Clone()
}

// resetForSplit clears the tracker (the span it measured no longer
// exists) and, for a load split, stamps the guard time.
func (l *replicaLoad) resetForSplit(loadSplit bool) {
	now := l.nowFn()
	l.mu.Lock()
	defer l.mu.Unlock()
	l.cur, l.prev = 0, 0
	l.windowStart = now
	l.rotations = 0
	l.seen = 0
	l.samples = l.samples[:0]
	if loadSplit {
		l.lastLoadSplitAt = now
	}
}

// recentLoadSplit reports whether a load split stamped this replica
// within the settle window.
func (l *replicaLoad) recentLoadSplit(settle time.Duration) bool {
	now := l.nowFn()
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.lastLoadSplitAt != 0 && now-l.lastLoadSplitAt < settle.Nanoseconds()
}

// QPS is the replica's measured request rate (leader-local; ~0 on
// followers and fresh leaders).
func (r *Replica) QPS() float64 {
	q, _ := r.load.qps()
	return q
}

// loadNow is the store's load-tracking clock (test-overridable).
func (s *Store) loadNow() int64 {
	if k := s.cfg.TestingKnobs.LoadNowNanos; k != nil {
		return k()
	}
	return time.Now().UnixNano()
}

// loadSplitThreshold resolves the configured QPS threshold (0 = default;
// negative = load splitting disabled).
func (s *Store) loadSplitThreshold() float64 {
	t := s.cfg.LoadSplitThreshold
	if t == 0 {
		return DefaultLoadSplitThreshold
	}
	return t
}

// loadSettleWindow is how long a fresh load-split half is protected from
// merging (and how long its rate must have settled).
func (s *Store) loadSettleWindow() time.Duration {
	if s.cfg.LoadSettleWindow > 0 {
		return s.cfg.LoadSettleWindow
	}
	return 2 * loadRateWindow
}

// effectiveQPS returns the replica's request rate and whether the value
// is trustworthy for split/merge decisions (mature tracker, or a testing
// override).
func (s *Store) effectiveQPS(r *Replica) (float64, bool) {
	if k := s.cfg.TestingKnobs.OverrideReplicaQPS; k != nil {
		if q, ok := k(r.rangeID); ok {
			return q, true
		}
	}
	return r.load.qps()
}

// NodeLoad aggregates the store's replica load for the registry heartbeat
// (kvpb.NodeDescriptor's load fields).
type NodeLoad struct {
	LeaderQPS    float64
	LeaderCount  int
	ReplicaBytes int64
	HotRanges    []kvpb.HotRange // top-K mature leaseholders by QPS
	BigRanges    []kvpb.HotRange // top-K replicas by size
}

// LoadSummary walks the store's replicas and aggregates what the
// allocator needs to weigh this node's load: total mature-leaseholder
// QPS, leader count, total replica bytes, and the top-K hot and big
// ranges. Immature QPS trackers (mid-window after a leadership change)
// are excluded entirely — a misleading partial rate is worse than none.
func (s *Store) LoadSummary(k int) NodeLoad {
	var out NodeLoad
	s.VisitReplicas(func(r *Replica) bool {
		size := r.SizeBytes()
		out.ReplicaBytes += size
		out.BigRanges = append(out.BigRanges, kvpb.HotRange{RangeID: r.rangeID, Bytes: size})
		if r.IsLeader() {
			out.LeaderCount++
			if q, ok := s.effectiveQPS(r); ok {
				out.LeaderQPS += q
				out.HotRanges = append(out.HotRanges, kvpb.HotRange{RangeID: r.rangeID, QPS: q, Bytes: size})
			}
		}
		return true
	})
	sort.Slice(out.HotRanges, func(i, j int) bool { return out.HotRanges[i].QPS > out.HotRanges[j].QPS })
	sort.Slice(out.BigRanges, func(i, j int) bool { return out.BigRanges[i].Bytes > out.BigRanges[j].Bytes })
	if len(out.HotRanges) > k {
		out.HotRanges = out.HotRanges[:k]
	}
	if len(out.BigRanges) > k {
		out.BigRanges = out.BigRanges[:k]
	}
	return out
}
