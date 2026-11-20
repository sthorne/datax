// Package notify carries LISTEN / NOTIFY (issue #297): the node's
// registry of listening sessions, and the fan-out of committed
// notifications to the other nodes of the cluster.
//
// A notification exists only in memory. A session that commits a NOTIFY
// hands it to its node's Hub, which queues it for every local listener of
// the channel and for every other node; each of those hands it to its own
// listeners. Nothing is written to the store, so what is promised is what
// PostgreSQL promises a single server, stretched over the cluster:
//
//   - A session listening at the moment of the commit receives the
//     notification once, or is disconnected. It is never left connected
//     having silently missed one: a listener too slow to drain its queue,
//     and every listener of a node that a peer had to give up on, is
//     ended with a FATAL error, so the client reconnects, listens again
//     and re-reads whatever the notifications stood for.
//   - The exception is a node that dies between committing and handing
//     the notification to a peer (normally well under a millisecond):
//     the notification dies with it, and listeners elsewhere are not
//     told.
//   - A notification committed on another node shortly before a
//     session's LISTEN can arrive after it, and is delivered to it.
//     PostgreSQL would not deliver it. Filtering on commit timestamps
//     would be exact only with clocks the nodes do not share: a node
//     whose clock lags would drop a notification committed after the
//     LISTEN, which breaks the promise above. An extra notification is
//     harmless where a notification means "look now".
//   - Order: every node sees one node's notifications in the order that
//     node published them (the order its sessions' commits returned).
//     Notifications from different nodes are not ordered against each
//     other.
package notify

import (
	"context"
	"errors"
	"sync"
	"time"
)

// Limits. The first two are PostgreSQL's (a channel is an identifier, a
// payload must be shorter than 8000 bytes).
const (
	MaxChannelLen = 63
	MaxPayloadLen = 7999

	// DefaultSessionQueueBytes bounds what one listening session may have
	// waiting: a session that does not read (or sits in a transaction,
	// where delivery waits for its end) while this much arrives is
	// disconnected rather than left to grow without bound or to stall the
	// sessions notifying it.
	DefaultSessionQueueBytes = 16 << 20
	// DefaultPeerQueueBytes bounds what a node keeps for a peer: what it
	// has not managed to hand over yet, which for a peer it cannot reach
	// is everything. Past it the queue is dropped and the peer is told,
	// when it is next reached, that it missed notifications. It is the
	// larger of the two because overflowing it costs every listener of
	// the peer its connection, so a burst that merely outruns the
	// network for a moment must fit.
	DefaultPeerQueueBytes = 64 << 20

	// DefaultPeerRetention is how long a node keeps trying to hand a
	// peer it cannot reach what it owes it. Past it the queue is dropped
	// as if it had overflowed: a notification that late is better
	// replaced by the reconnect that tells the client to look again, and
	// a peer that is never coming back must not hold memory until enough
	// has been published to overflow its queue.
	DefaultPeerRetention = 5 * time.Minute

	// peerBatch and peerBatchBytes bound what one call to a peer carries.
	peerBatch      = 512
	peerBatchBytes = 1 << 20
	// sendTimeout bounds one call to a peer; a slower one is retried.
	sendTimeout = 5 * time.Second
	// The pause between attempts at an unreachable peer.
	minBackoff = 50 * time.Millisecond
	maxBackoff = 2 * time.Second
)

// ErrUnsupported is what Config.Send returns for a peer that does not
// know the call: a node running a release from before LISTEN / NOTIFY,
// which therefore has no listeners. The batch is dropped, not retried,
// and nothing counts as lost.
var ErrUnsupported = errors.New("peer does not support notifications")

// Notification is one committed NOTIFY. Channels belong to a database,
// as in PostgreSQL: a listener hears only the notifications of the
// database it was in when it listened.
type Notification struct {
	Database string `json:"db"`
	Channel  string `json:"ch"`
	Payload  string `json:"p,omitempty"`
	// PID is the notifying session's backend process ID.
	PID int32 `json:"pid,omitempty"`
}

// size is what a notification counts for against a queue's bound.
func (n Notification) size() int { return len(n.Database) + len(n.Channel) + len(n.Payload) + 32 }

// Batch is what one node sends another: a run of its notifications, in
// order. Seq numbers them per sender incarnation and destination, so a
// call that is retried after its answer was lost is not delivered twice.
type Batch struct {
	From int32 `json:"from"`
	// Epoch identifies the sending node's incarnation (its start time):
	// sequence numbers start over when it restarts.
	Epoch int64 `json:"epoch"`
	// Seq is the sequence number of the first notification (of the next
	// one to come, for an empty batch).
	Seq uint64 `json:"seq"`
	// Lost says the sender dropped notifications numbered below Seq
	// without delivering them (its queue for this node overflowed while
	// the node was unreachable).
	Lost          bool           `json:"lost,omitempty"`
	Notifications []Notification `json:"n,omitempty"`
}

// Config wires a Hub to its cluster. The zero value is a single node.
type Config struct {
	// NodeID identifies this node to its peers.
	NodeID int32
	// Peers lists the other nodes of the cluster as currently known; nil
	// means there are none.
	Peers func() []int32
	// Send delivers a batch to a peer and returns once the peer has taken
	// it. ErrUnsupported means the peer does not know the call.
	Send func(ctx context.Context, to int32, b *Batch) error
	// SessionQueueBytes, PeerQueueBytes and PeerRetention override the
	// defaults.
	SessionQueueBytes int
	PeerQueueBytes    int
	PeerRetention     time.Duration
}

type chanKey struct{ db, channel string }

// Hub is one node's notification exchange.
type Hub struct {
	cfg   Config
	ctx   context.Context
	epoch int64

	// mu orders everything: a notification is queued for the local
	// listeners and for every peer under it, so all of them see this
	// node's notifications in one order.
	mu        sync.Mutex
	listeners map[chanKey]map[*Listener]struct{}
	all       map[*Listener]struct{}
	peers     map[int32]*peerQueue
	// seen is the last sequence number taken from each sending node, for
	// dropping a batch that is delivered again.
	seen map[int32]peerSeen
}

type peerSeen struct {
	epoch int64
	seq   uint64
}

// NewHub makes a node's hub. ctx ends its senders (the node's stopper).
func NewHub(ctx context.Context, cfg Config) *Hub {
	if cfg.SessionQueueBytes <= 0 {
		cfg.SessionQueueBytes = DefaultSessionQueueBytes
	}
	if cfg.PeerQueueBytes <= 0 {
		cfg.PeerQueueBytes = DefaultPeerQueueBytes
	}
	if cfg.PeerRetention <= 0 {
		cfg.PeerRetention = DefaultPeerRetention
	}
	return &Hub{
		cfg: cfg, ctx: ctx, epoch: time.Now().UnixNano(),
		listeners: make(map[chanKey]map[*Listener]struct{}),
		all:       make(map[*Listener]struct{}),
		peers:     make(map[int32]*peerQueue),
		seen:      make(map[int32]peerSeen),
	}
}

// Publish delivers a committed transaction's notifications: to this
// node's listeners before it returns (so a session finds its own
// notifications waiting when its COMMIT completes), and to the other
// nodes in the background. It never waits on a listener or a peer.
func (h *Hub) Publish(ns []Notification) {
	if len(ns) == 0 {
		return
	}
	var peers []int32
	if h.cfg.Peers != nil && h.cfg.Send != nil {
		peers = h.cfg.Peers()
	}
	h.mu.Lock()
	full := h.deliverLocked(ns)
	h.forwardLocked(peers, ns)
	h.mu.Unlock()
	for _, l := range full {
		l.endConn(ReasonQueueFull)
	}
}

// deliverLocked queues ns for the local listeners of their channels and
// returns the listeners whose queue overflowed: they take nothing
// further, and the caller has them disconnected once it has let go of
// the lock.
func (h *Hub) deliverLocked(ns []Notification) (full []*Listener) {
	for _, n := range ns {
		for l := range h.listeners[chanKey{n.Database, n.Channel}] {
			if !l.enqueue(n) && l.markEnded() {
				full = append(full, l)
			}
		}
	}
	return full
}

// Deliver takes a batch from another node.
func (h *Hub) Deliver(b *Batch) {
	if b == nil {
		return
	}
	h.mu.Lock()
	st, known := h.seen[b.From]
	switch {
	case known && b.Epoch < st.epoch:
		// From an incarnation of the sender that has since restarted.
		h.mu.Unlock()
		return
	case !known || b.Epoch > st.epoch:
		st = peerSeen{epoch: b.Epoch}
	}
	var lost []*Listener
	if b.Lost && b.Seq > st.seq+1 {
		// The sender gave up on notifications this node never saw. Which
		// channels they were for is not known, so every listener here may
		// have missed one.
		// They are marked before the rest of the batch is queued, so
		// that none of them is sent something from after the gap.
		for l := range h.all {
			if l.listening() && l.markEnded() {
				lost = append(lost, l)
			}
		}
		st.seq = b.Seq - 1
	}
	var fresh []Notification
	for i, n := range b.Notifications {
		if seq := b.Seq + uint64(i); seq > st.seq {
			st.seq = seq
			fresh = append(fresh, n)
		}
	}
	h.seen[b.From] = st
	full := h.deliverLocked(fresh)
	h.mu.Unlock()
	for _, l := range lost {
		l.endConn(ReasonPeerLost)
	}
	for _, l := range full {
		l.endConn(ReasonQueueFull)
	}
}

// Listening reports how many sessions on this node listen to anything.
func (h *Hub) Listening() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	n := 0
	for l := range h.all {
		if l.listening() {
			n++
		}
	}
	return n
}

// Reason says why a listening session had to be ended.
type Reason int

const (
	// ReasonQueueFull: the session did not take its notifications and its
	// queue reached the bound.
	ReasonQueueFull Reason = iota + 1
	// ReasonPeerLost: another node dropped notifications it could not
	// deliver to this one.
	ReasonPeerLost
)

// Listener is one session's subscription state and its queue of
// notifications not yet sent to the client.
type Listener struct {
	hub *Hub
	// wake is called when a notification is queued, and endConn when
	// the session must be disconnected because it missed (or would miss)
	// notifications. Neither may block: both are called from the
	// notifying session's commit.
	wake    func()
	endConn func(Reason)

	mu       sync.Mutex
	channels map[chanKey]struct{}
	queue    []Notification
	bytes    int
	ended    bool
}

// NewListener registers a session. wake is called (possibly often, and
// from other goroutines) whenever a notification is queued; end when the
// session has to be disconnected.
func (h *Hub) NewListener(wake func(), end func(Reason)) *Listener {
	if wake == nil {
		wake = func() {}
	}
	if end == nil {
		end = func(Reason) {}
	}
	l := &Listener{hub: h, wake: wake, endConn: end, channels: make(map[chanKey]struct{})}
	h.mu.Lock()
	h.all[l] = struct{}{}
	h.mu.Unlock()
	return l
}

// Listen subscribes the session to a channel of a database. Listening
// twice is listening once.
func (l *Listener) Listen(db, channel string) {
	k := chanKey{db, channel}
	h := l.hub
	h.mu.Lock()
	defer h.mu.Unlock()
	if _, ok := h.all[l]; !ok {
		return // closed
	}
	set := h.listeners[k]
	if set == nil {
		set = make(map[*Listener]struct{})
		h.listeners[k] = set
	}
	set[l] = struct{}{}
	l.mu.Lock()
	l.channels[k] = struct{}{}
	l.mu.Unlock()
}

// Unlisten ends the session's subscription to a channel, if it has one.
func (l *Listener) Unlisten(db, channel string) {
	h := l.hub
	h.mu.Lock()
	defer h.mu.Unlock()
	l.unlistenLocked(chanKey{db, channel})
}

// UnlistenAll ends every subscription of the session.
func (l *Listener) UnlistenAll() {
	h := l.hub
	h.mu.Lock()
	defer h.mu.Unlock()
	l.mu.Lock()
	keys := make([]chanKey, 0, len(l.channels))
	for k := range l.channels {
		keys = append(keys, k)
	}
	l.mu.Unlock()
	for _, k := range keys {
		l.unlistenLocked(k)
	}
}

func (l *Listener) unlistenLocked(k chanKey) {
	h := l.hub
	if set := h.listeners[k]; set != nil {
		delete(set, l)
		if len(set) == 0 {
			delete(h.listeners, k)
		}
	}
	l.mu.Lock()
	delete(l.channels, k)
	l.mu.Unlock()
}

// Channels lists the channels of db the session listens to.
func (l *Listener) Channels(db string) []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	var out []string
	for k := range l.channels {
		if k.db == db {
			out = append(out, k.channel)
		}
	}
	return out
}

func (l *Listener) listening() bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.channels) > 0
}

// Close forgets the session (its connection ended).
func (l *Listener) Close() {
	l.UnlistenAll()
	h := l.hub
	h.mu.Lock()
	delete(h.all, l)
	h.mu.Unlock()
	l.mu.Lock()
	l.queue, l.bytes, l.ended = nil, 0, true
	l.mu.Unlock()
}

// enqueue queues n for the session, reporting false when the queue is
// full: the notification is not queued and the session must end. A
// session that is already ending takes nothing more.
func (l *Listener) enqueue(n Notification) bool {
	l.mu.Lock()
	if l.ended {
		l.mu.Unlock()
		return true
	}
	if l.bytes+n.size() > l.hub.cfg.SessionQueueBytes {
		l.mu.Unlock()
		return false
	}
	l.queue = append(l.queue, n)
	l.bytes += n.size()
	l.mu.Unlock()
	l.wake()
	return true
}

// markEnded stops the session's queue from taking anything more and
// reports whether this call was the one that did: the caller then has
// the session disconnected (endConn), once.
func (l *Listener) markEnded() bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.ended {
		return false
	}
	l.ended = true
	return true
}

// Take returns the notifications waiting for the session, oldest first,
// and empties the queue.
func (l *Listener) Take() []Notification {
	l.mu.Lock()
	defer l.mu.Unlock()
	out := l.queue
	l.queue, l.bytes = nil, 0
	return out
}

// Pending reports whether notifications wait for the session.
func (l *Listener) Pending() bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.queue) > 0
}

// peerQueue is what this node still owes one peer, in order.
type peerQueue struct {
	to    int32
	queue []Notification
	bytes int
	// next is the sequence number the next notification queued will take;
	// the queue's first entry is numbered next - len(queue).
	next uint64
	// lost: notifications below the queue's first were dropped, and the
	// peer has not been told yet. skipped counts the notifications not
	// even queued since (see forwardLocked).
	lost    bool
	skipped uint64
	// failing marks a peer the last call did not reach (Flush does not
	// wait for it), since when; gone one that has left the cluster.
	failing      bool
	failingSince time.Time
	gone         bool
	wake         chan struct{}
}

// forwardLocked queues ns for every peer, starting a sender for a peer
// seen for the first time and retiring the queue of one that has left
// the cluster.
func (h *Hub) forwardLocked(peers []int32, ns []Notification) {
	current := make(map[int32]bool, len(peers))
	for _, id := range peers {
		if id == h.cfg.NodeID {
			continue
		}
		current[id] = true
		q := h.peers[id]
		if q == nil {
			q = &peerQueue{to: id, next: 1, wake: make(chan struct{}, 1)}
			h.peers[id] = q
			go h.sendLoop(q)
		}
		for _, n := range ns {
			if q.bytes+n.size() > h.cfg.PeerQueueBytes {
				// The peer has been out of reach, or too slow, for as
				// long as it took to fill the queue. Nothing queued can
				// be delivered without a gap any more, so all of it
				// goes, and the peer's listeners are ended when it is
				// next reached.
				q.queue, q.bytes, q.lost = nil, 0, true
			}
			q.next++
			if q.lost && q.failing {
				// Whatever reaches the peer next ends every listener it
				// has, so there is nobody to queue this for: a peer that
				// never comes back holds no memory. The count lets the
				// sender notice a notification skipped while the call
				// that finally got through was already on its way.
				q.queue, q.bytes = nil, 0
				q.skipped++
				continue
			}
			q.queue = append(q.queue, n)
			q.bytes += n.size()
		}
		select {
		case q.wake <- struct{}{}:
		default:
		}
	}
	for id, q := range h.peers {
		if !current[id] {
			q.gone = true
			delete(h.peers, id)
			select {
			case q.wake <- struct{}{}:
			default:
			}
		}
	}
}

// sendLoop delivers one peer's queue, in order, retrying a batch until
// the peer has taken it.
func (h *Hub) sendLoop(q *peerQueue) {
	backoff := minBackoff
	for {
		h.mu.Lock()
		if q.gone {
			h.mu.Unlock()
			return
		}
		n, size := 0, 0
		for n < len(q.queue) && n < peerBatch && size < peerBatchBytes {
			size += q.queue[n].size()
			n++
		}
		b := &Batch{From: h.cfg.NodeID, Epoch: h.epoch, Seq: q.next - uint64(len(q.queue)), Lost: q.lost}
		b.Notifications = append(b.Notifications, q.queue[:n]...)
		skipped := q.skipped
		h.mu.Unlock()

		if n == 0 && !b.Lost {
			select {
			case <-q.wake:
				continue
			case <-h.ctx.Done():
				return
			}
		}

		ctx, cancel := context.WithTimeout(h.ctx, sendTimeout)
		err := h.cfg.Send(ctx, q.to, b)
		cancel()
		if err != nil && !errors.Is(err, ErrUnsupported) {
			h.mu.Lock()
			if !q.failing {
				q.failing, q.failingSince = true, time.Now()
			}
			if len(q.queue) > 0 && time.Since(q.failingSince) > h.cfg.PeerRetention {
				q.queue, q.bytes, q.lost = nil, 0, true
			}
			h.mu.Unlock()
			select {
			case <-time.After(backoff):
			case <-h.ctx.Done():
				return
			}
			if backoff *= 2; backoff > maxBackoff {
				backoff = maxBackoff
			}
			continue
		}
		backoff = minBackoff
		h.mu.Lock()
		q.failing = false
		// The queue may have been dropped and refilled while the call was
		// out; what was sent is then no longer at its head, and the next
		// call says what was lost in the meantime.
		if first := q.next - uint64(len(q.queue)); first == b.Seq {
			for i := 0; i < n; i++ {
				q.bytes -= q.queue[i].size()
			}
			q.queue = q.queue[n:]
			if len(q.queue) == 0 {
				q.queue = nil
			}
			if b.Lost && q.skipped == skipped {
				q.lost = false
			}
		}
		h.mu.Unlock()
	}
}

// Flush waits until every peer that can be reached has been handed what
// this node owes it, or ctx ends: a node that is stopping in an orderly
// way does not take committed notifications down with it.
func (h *Hub) Flush(ctx context.Context) {
	for {
		h.mu.Lock()
		owing := false
		for _, q := range h.peers {
			if (len(q.queue) > 0 || q.lost) && !q.failing {
				owing = true
			}
		}
		h.mu.Unlock()
		if !owing {
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-h.ctx.Done():
			return
		case <-time.After(5 * time.Millisecond):
		}
	}
}
