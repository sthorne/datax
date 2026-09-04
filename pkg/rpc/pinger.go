package rpc

import (
	"context"
	"sort"
	"sync"
	"time"

	"github.com/sthorne/datax/pkg/base"
	"github.com/sthorne/datax/pkg/kvpb"
	"github.com/sthorne/datax/pkg/metrics"
	"github.com/sthorne/datax/pkg/util/log"
)

// Pinger measures the network to every peer: one ping per peer per
// interval, one in flight per peer, a short timeout, a smoothed round
// trip with a ring for the p99, and the clock offset the exchange
// yields. Nothing in datax measured the network before this; raft
// heartbeats flow constantly but record no timing, and clock offset was
// only ever enforced against --max-offset, never observed. A peer whose
// ping fails or times out is reported unreachable (never as a huge RTT),
// and an offset past half the clock's tolerance is logged, since past the
// tolerance the HLC guarantees are gone.
type Pinger struct {
	trans   *Transport
	peers   func() []base.NodeID
	self    base.NodeID
	timeout time.Duration

	mu    sync.Mutex
	stats map[base.NodeID]*peerStats
	warns map[base.NodeID]time.Time
}

// pingRing is how many recent round trips feed the p99.
const pingRing = 64

type peerStats struct {
	ewma      time.Duration
	ring      [pingRing]time.Duration
	n         int // samples in the ring (<= pingRing)
	next      int
	offset    time.Duration
	reachable bool
	lastOK    time.Time
	inFlight  bool
}

// NewPinger returns a pinger over trans for the peers fn lists (self is
// skipped). timeout bounds one ping; a peer that misses it is unreachable
// until the next success.
func NewPinger(trans *Transport, self base.NodeID, peers func() []base.NodeID, timeout time.Duration) *Pinger {
	return &Pinger{trans: trans, peers: peers, self: self, timeout: timeout,
		stats: make(map[base.NodeID]*peerStats), warns: make(map[base.NodeID]time.Time)}
}

// Run pings every peer every interval until ctx ends.
func (p *Pinger) Run(ctx context.Context, interval time.Duration) {
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		p.tick(ctx)
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

// tick launches one ping per peer that has none in flight and forgets
// peers that left the registry.
func (p *Pinger) tick(ctx context.Context) {
	peers := p.peers()
	live := make(map[base.NodeID]bool, len(peers))
	p.mu.Lock()
	for _, id := range peers {
		if id == p.self {
			continue
		}
		live[id] = true
		st, ok := p.stats[id]
		if !ok {
			st = &peerStats{}
			p.stats[id] = st
		}
		if st.inFlight {
			continue
		}
		st.inFlight = true
		go p.pingOne(ctx, id)
	}
	for id := range p.stats {
		if !live[id] {
			delete(p.stats, id)
		}
	}
	p.mu.Unlock()
}

func (p *Pinger) pingOne(ctx context.Context, id base.NodeID) {
	pctx, cancel := context.WithTimeout(ctx, p.timeout)
	rtt, offset, err := p.trans.Ping(pctx, id)
	cancel()
	peer := id.String()
	p.mu.Lock()
	defer p.mu.Unlock()
	st, ok := p.stats[id]
	if !ok {
		return // dropped from the registry meanwhile
	}
	st.inFlight = false
	if err != nil {
		if st.reachable {
			log.Infof("peer n%d unreachable: ping failed: %v", id, err)
		}
		st.reachable = false
		metrics.PeerReachable.WithLabelValues(peer).Set(0)
		return
	}
	if !st.reachable && st.n > 0 {
		log.Infof("peer n%d reachable again (rtt %s)", id, rtt)
	}
	st.reachable = true
	st.lastOK = time.Now()
	if st.n == 0 {
		st.ewma = rtt
	} else {
		st.ewma = (st.ewma*4 + rtt) / 5 // alpha 0.2
	}
	st.ring[st.next] = rtt
	st.next = (st.next + 1) % pingRing
	if st.n < pingRing {
		st.n++
	}
	st.offset = offset
	metrics.RPCRoundTrip.WithLabelValues(peer).Observe(rtt.Seconds())
	metrics.ClockOffset.WithLabelValues(peer).Set(offset.Seconds())
	metrics.PeerReachable.WithLabelValues(peer).Set(1)
	if max := p.trans.clock.MaxOffset(); max > 0 && (offset > max/2 || -offset > max/2) {
		if last := p.warns[id]; time.Since(last) > time.Minute {
			p.warns[id] = time.Now()
			log.Warnf("clock offset to n%d is %s, past half the tolerated %s; beyond it the uncertainty guarantee fails — check NTP", id, offset, max)
		}
	}
}

// Snapshot returns the current view of every peer, sorted by node ID.
func (p *Pinger) Snapshot() []kvpb.PeerLatency {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]kvpb.PeerLatency, 0, len(p.stats))
	now := time.Now()
	for id, st := range p.stats {
		pl := kvpb.PeerLatency{Peer: id, Reachable: st.reachable}
		if st.n > 0 {
			pl.RTTMicros = st.ewma.Microseconds()
			pl.P99Micros = st.p99().Microseconds()
			pl.OffsetMicros = st.offset.Microseconds()
			pl.AgeMillis = now.Sub(st.lastOK).Milliseconds()
		} else {
			pl.AgeMillis = -1 // never reached
		}
		out = append(out, pl)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Peer < out[j].Peer })
	return out
}

func (st *peerStats) p99() time.Duration {
	vals := make([]time.Duration, st.n)
	copy(vals, st.ring[:st.n])
	sort.Slice(vals, func(i, j int) bool { return vals[i] < vals[j] })
	idx := (len(vals)*99 + 99) / 100
	if idx >= len(vals) {
		idx = len(vals) - 1
	}
	return vals[idx]
}
