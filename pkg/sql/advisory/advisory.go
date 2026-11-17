// Package advisory implements PostgreSQL's advisory locks (issue #353):
// pg_advisory_lock and its relatives, held by a session or by its
// transaction, exclusive or shared, and cluster-wide — a session on any
// node contends with sessions on every other.
//
// # Design
//
// A lock is held by writing a record, and released by deleting it. The
// records live in the replicated system keyspace (keys.AdvisoryLockKey):
// one per (lock, holding session, mode), all of one lock's records under
// one prefix. Taking a lock is a short serializable transaction that
// scans that prefix and writes the session's own record only if no live
// record of another session conflicts with it (an exclusive request
// conflicts with every other record, a shared one only with exclusive
// ones). Two sessions racing for a lock are two transactions that read
// what the other writes, so one of them restarts and then sees the
// other's record: the KV layer's serializability is the whole of the
// mutual exclusion. Nothing of an advisory lock touches table data, so
// advisory locks and row locks (FOR UPDATE, write intents) never wait on
// each other. A lock's transaction holds nothing while its session
// waits: a waiter looks again when a lock of its own node is released
// (at once) and otherwise polls, backing off to maxPoll, so a release
// on another node is seen within maxPoll.
//
// The hard part is a holder that cannot release: its client vanished
// (the session's end releases its locks, so only a node that stops can
// strand them) or its node died or was cut off. Each record therefore
// names its holder's node and the term of that node's advisory lease
// (keys.AdvisoryLeaseKey): a record whose node's lease is gone, belongs
// to another term, or has expired holds nothing, and the session that
// next finds it in its way deletes it — and deletes the expired lease,
// revoking the term, so the node cannot later renew it. A node keeps its
// lease while its sessions hold or wait for advisory locks, renewing it
// every TTL/4; it lapses on its own a TTL after a node stops renewing, so
// a dead node's locks are free at most TTL plus the maximum clock offset
// after its last renewal.
//
// A lease only means anything if the holder stops relying on it before
// anyone else may decide it has expired. A node counts its own lease as
// valid until TTL minus the maximum clock offset after the moment it
// started the renewal that wrote it, by its monotonic clock; another
// node takes it for expired only TTL plus the maximum clock offset after
// that moment, by its own clock. When a node's lease lapses — it could
// not reach the cluster to renew in time, or a renewal found the term
// revoked — every session of the node holding an advisory lock is ended
// with a FATAL error before another session can be granted the lock, as
// a lost connection would end it: what the session believed it held is
// no longer guaranteed. (Like every lease, this assumes a process is not
// frozen between checking its lease and acting on it for longer than
// the margin; PostgreSQL, a single server, has no such caveat.)
//
// The alternative of holding each lock as the write intent of a
// long-running KV transaction would reuse the transaction heartbeat for
// the liveness, but an intent cannot be shared, would sit in the way of
// anything scanning the keyspace it lives in, and would still need the
// same fencing of the holder; a lease per node costs one write every
// TTL/4 per node using advisory locks and none otherwise.
//
// What differs from PostgreSQL: waiters are not queued in arrival order
// (whoever looks first after a release wins), a deadlock between
// advisory locks is not detected (it ends when lock_timeout or
// statement_timeout does), and pg_locks shows granted locks only.
package advisory

import (
	"context"
	"encoding/json"
	"errors"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/kvclient"
	"github.com/sthorne/datax/pkg/util/log"
)

// Mode is a lock's strength.
type Mode uint8

const (
	// Exclusive conflicts with every hold by another session.
	Exclusive Mode = 1
	// Share conflicts only with an exclusive hold by another session.
	Share Mode = 2
)

// String is PostgreSQL's name for the mode (pg_locks.mode, and the
// warning of an unlock that held nothing).
func (m Mode) String() string {
	if m == Share {
		return "ShareLock"
	}
	return "ExclusiveLock"
}

// Key names a lock as PostgreSQL does: its database and three numbers.
// A one-bigint key k is (k>>32, k&0xffffffff, 1); a two-int4 key (a, b)
// is (a, b, 2), so the two forms never name the same lock.
type Key struct {
	DB      string
	ClassID uint32
	ObjID   uint32
	SubID   uint16
}

// Int64Key is the lock pg_advisory_lock(bigint) names.
func Int64Key(db string, k int64) Key {
	return Key{DB: db, ClassID: uint32(uint64(k) >> 32), ObjID: uint32(uint64(k)), SubID: 1}
}

// Int32Key is the lock pg_advisory_lock(int4, int4) names.
func Int32Key(db string, k1, k2 int32) Key {
	return Key{DB: db, ClassID: uint32(k1), ObjID: uint32(k2), SubID: 2}
}

const (
	// DefaultTTL is how long a node's lease lasts unrenewed: the bound,
	// plus the maximum clock offset, on how long the locks of a node
	// that died stay held.
	DefaultTTL = 10 * time.Second
	// minPoll and maxPoll bound a waiter's pause between looks.
	minPoll = 5 * time.Millisecond
	maxPoll = 100 * time.Millisecond
	// releaseTimeout bounds one attempt at deleting released records.
	releaseTimeout = 5 * time.Second
)

// ErrLost is what a session whose node lost its lease gets from every
// call: the session is being ended.
var ErrLost = errors.New("this node lost its advisory-lock lease: the session's advisory locks may have been granted to another session")

// errTermLost is a lease that turned out to have been revoked or
// replaced.
var errTermLost = errors.New("advisory lease term lost")

// Config wires a Manager to its node.
type Config struct {
	DB     *kvclient.DB
	NodeID int32
	// TTL overrides DefaultTTL. It must exceed twice the maximum clock
	// offset.
	TTL time.Duration
}

// lease is a node's lease record.
type lease struct {
	Node    int32  `json:"node"`
	Term    uint64 `json:"term"`
	Expires int64  `json:"exp"` // wall nanoseconds
}

// record is one hold of a lock.
type record struct {
	DB      string `json:"db"`
	ClassID uint32 `json:"c"`
	ObjID   uint32 `json:"o"`
	SubID   uint16 `json:"s"`
	Mode    Mode   `json:"m"`
	Node    int32  `json:"n"`
	Term    uint64 `json:"t"`
	Session uint64 `json:"sid"`
	PID     int32  `json:"pid"`
	At      int64  `json:"at"`
}

// Manager is one node's share of the advisory locks: its lease and the
// sessions holding locks under it.
type Manager struct {
	ctx       context.Context
	db        *kvclient.DB
	node      int32
	ttl       time.Duration
	maxOffset time.Duration
	sessions  atomic.Uint64

	mu sync.Mutex
	// term is the lease term the node's sessions take locks under (0:
	// none), valid until validUntil by the monotonic clock.
	term       uint64
	validUntil time.Time
	watchdog   *time.Timer
	// starting is closed when the term being started is in place.
	starting chan struct{}
	// holders are the sessions holding records under term; users counts
	// lock calls in flight. The lease is given up once both have been
	// empty for a TTL.
	holders   map[*Holder]struct{}
	users     int
	idleSince time.Time
	// released is closed (and replaced) whenever a lock is released here.
	released chan struct{}
}

// NewManager makes a node's manager; ctx ends with the node.
func NewManager(ctx context.Context, cfg Config) *Manager {
	ttl := cfg.TTL
	if ttl <= 0 {
		ttl = DefaultTTL
	}
	off := cfg.DB.Clock().MaxOffset()
	if ttl <= 2*off {
		ttl = 2*off + time.Second
	}
	return &Manager{
		ctx: ctx, db: cfg.DB, node: cfg.NodeID, ttl: ttl, maxOffset: off,
		holders: map[*Holder]struct{}{}, released: make(chan struct{}),
	}
}

func getLease(ctx context.Context, txn *kvclient.Txn, node int32) (*lease, error) {
	raw, err := txn.Get(ctx, keys.AdvisoryLeaseKey(node))
	if err != nil || raw == nil {
		return nil, err
	}
	var l lease
	if err := json.Unmarshal(raw, &l); err != nil {
		return nil, nil // unreadable: as good as none
	}
	return &l, nil
}

// live reports whether l is a live lease of term at wall time now, as
// a node other than its owner judges it.
func (m *Manager) live(l *lease, term uint64, now int64) bool {
	return l != nil && l.Term == term && now <= l.Expires+int64(m.maxOffset)
}

// putLease writes the node's lease for term, valid for a TTL from now;
// it returns how long the node may count on it.
func (m *Manager) putLease(ctx context.Context, txn *kvclient.Txn, term uint64) (time.Time, error) {
	t0 := time.Now()
	v, err := json.Marshal(lease{Node: m.node, Term: term, Expires: m.db.Clock().PhysicalNow() + int64(m.ttl)})
	if err != nil {
		return time.Time{}, err
	}
	if err := txn.Put(ctx, keys.AdvisoryLeaseKey(m.node), v); err != nil {
		return time.Time{}, err
	}
	return t0.Add(m.ttl - m.maxOffset), nil
}

// currentTerm returns a valid term to take a lock under, starting one
// when the node has none.
func (m *Manager) currentTerm(ctx context.Context) (uint64, error) {
	for {
		m.mu.Lock()
		if term := m.term; term != 0 {
			valid := time.Now().Before(m.validUntil)
			m.mu.Unlock()
			if valid {
				return term, nil
			}
			m.fence(term) // lapsed before the watchdog fired
			continue
		}
		if ch := m.starting; ch != nil {
			m.mu.Unlock()
			select {
			case <-ch:
				continue
			case <-ctx.Done():
				return 0, ctx.Err()
			}
		}
		ch := make(chan struct{})
		m.starting = ch
		m.mu.Unlock()

		term := rand.Uint64() | 1
		var valid time.Time
		err := m.db.RunTxn(ctx, "advisory-lease", func(ctx context.Context, txn *kvclient.Txn) error {
			var err error
			valid, err = m.putLease(ctx, txn, term)
			return err
		})
		m.mu.Lock()
		m.starting = nil
		close(ch)
		if err != nil {
			m.mu.Unlock()
			return 0, err
		}
		m.term, m.validUntil, m.idleSince = term, valid, time.Now()
		m.armLocked(term)
		m.mu.Unlock()
		go m.renewLoop(term)
		return term, nil
	}
}

// armLocked (re)sets the watchdog that ends the term when its validity
// runs out without a renewal.
func (m *Manager) armLocked(term uint64) {
	if m.watchdog != nil {
		m.watchdog.Stop()
	}
	m.watchdog = time.AfterFunc(time.Until(m.validUntil), func() {
		m.mu.Lock()
		lapsed := m.term == term && !time.Now().Before(m.validUntil)
		m.mu.Unlock()
		if lapsed {
			m.fence(term)
		}
	})
}

// renewLoop keeps term's lease alive while the node uses it, and gives
// it up once the node has not for a TTL.
func (m *Manager) renewLoop(term uint64) {
	every := m.ttl / 4
	for {
		select {
		case <-m.ctx.Done():
			return
		case <-time.After(every):
		}
		m.mu.Lock()
		if m.term != term {
			m.mu.Unlock()
			return
		}
		if len(m.holders) == 0 && m.users == 0 && time.Since(m.idleSince) >= m.ttl {
			m.term = 0
			m.watchdog.Stop()
			m.mu.Unlock()
			m.retire(term)
			return
		}
		deadline := m.validUntil
		m.mu.Unlock()

		ctx, cancel := context.WithDeadline(m.ctx, deadline)
		var valid time.Time
		err := m.db.RunTxn(ctx, "advisory-lease", func(ctx context.Context, txn *kvclient.Txn) error {
			l, err := getLease(ctx, txn, m.node)
			if err != nil {
				return err
			}
			if l == nil || l.Term != term {
				return errTermLost
			}
			valid, err = m.putLease(ctx, txn, term)
			return err
		})
		cancel()
		if errors.Is(err, errTermLost) {
			m.fence(term)
			return
		}
		if err != nil {
			// The watchdog ends the term if no renewal succeeds in time.
			log.Debugf("advisory lease renewal: %v", err)
			continue
		}
		m.mu.Lock()
		if m.term == term && time.Now().Before(m.validUntil) {
			m.validUntil = valid
			m.armLocked(term)
		}
		m.mu.Unlock()
	}
}

// fence ends term: every session holding a lock under it is told it has
// lost its locks, and the lease is given up so that others need not
// wait out its expiry.
func (m *Manager) fence(term uint64) {
	m.mu.Lock()
	if m.term != term {
		m.mu.Unlock()
		return
	}
	m.term = 0
	if m.watchdog != nil {
		m.watchdog.Stop()
	}
	hs := m.holders
	m.holders = map[*Holder]struct{}{}
	m.idleSince = time.Now()
	m.mu.Unlock()
	if len(hs) > 0 && m.ctx.Err() == nil {
		log.Warnf("advisory locks: node %d could not keep its lease; ending the %d sessions that held advisory locks under it", m.node, len(hs))
	}
	for h := range hs {
		h.lose()
	}
	m.wake()
	go m.retire(term)
}

// retire deletes the node's lease if it is still term's.
func (m *Manager) retire(term uint64) {
	ctx, cancel := context.WithTimeout(m.ctx, releaseTimeout)
	defer cancel()
	_ = m.db.RunTxn(ctx, "advisory-lease", func(ctx context.Context, txn *kvclient.Txn) error {
		l, err := getLease(ctx, txn, m.node)
		if err != nil || l == nil || l.Term != term {
			return err
		}
		return txn.Delete(ctx, keys.AdvisoryLeaseKey(m.node))
	})
}

// register records h as holding a record under term; false when term
// is no longer the node's (or no longer valid), and the record written
// under it holds nothing.
func (m *Manager) register(h *Holder, term uint64) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.term != term || !time.Now().Before(m.validUntil) {
		return false
	}
	m.holders[h] = struct{}{}
	return true
}

func (m *Manager) unregister(h *Holder) {
	m.mu.Lock()
	if _, ok := m.holders[h]; ok {
		delete(m.holders, h)
		if len(m.holders) == 0 {
			m.idleSince = time.Now()
		}
	}
	m.mu.Unlock()
}

func (m *Manager) use(delta int) {
	m.mu.Lock()
	m.users += delta
	if m.users == 0 {
		m.idleSince = time.Now()
	}
	m.mu.Unlock()
}

// releasedChan is closed at the next release on this node.
func (m *Manager) releasedChan() <-chan struct{} {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.released
}

func (m *Manager) wake() {
	m.mu.Lock()
	close(m.released)
	m.released = make(chan struct{})
	m.mu.Unlock()
}

// tryAcquire makes one attempt at taking k in mode for h under term. It
// reports whether the lock was taken.
func (m *Manager) tryAcquire(ctx context.Context, h *Holder, k Key, mode Mode, term uint64, pid int32) (bool, error) {
	prefix := keys.AdvisoryLockPrefix(k.DB, k.ClassID, k.ObjID, k.SubID)
	mine := keys.AdvisoryLockKey(k.DB, k.ClassID, k.ObjID, k.SubID, m.node, term, h.id, uint8(mode))
	var ok bool
	err := m.db.RunTxn(ctx, "advisory-lock", func(ctx context.Context, txn *kvclient.Txn) error {
		ok = false
		own, err := getLease(ctx, txn, m.node)
		if err != nil {
			return err
		}
		if own == nil || own.Term != term {
			return errTermLost
		}
		kvs, err := txn.Scan(ctx, prefix, prefix.PrefixEnd(), 0)
		if err != nil {
			return err
		}
		now := m.db.Clock().PhysicalNow()
		leases := map[int32]*lease{m.node: own}
		conflict := false
		for _, kv := range kvs {
			var r record
			if json.Unmarshal(kv.Value, &r) == nil {
				if r.Node == m.node && r.Term == term && r.Session == h.id {
					continue // this session's own hold, in another mode
				}
				if mode == Share && r.Mode == Share {
					continue
				}
				l, seen := leases[r.Node]
				if !seen {
					if l, err = getLease(ctx, txn, r.Node); err != nil {
						return err
					}
					leases[r.Node] = l
				}
				if r.Node == m.node && r.Term == term || m.live(l, r.Term, now) {
					conflict = true
					continue
				}
				if l != nil && l.Term == r.Term {
					// The holder's node let its lease expire: revoke the
					// term, so that a late renewal cannot revive it.
					if err := txn.Delete(ctx, keys.AdvisoryLeaseKey(r.Node)); err != nil {
						return err
					}
					leases[r.Node] = nil
				}
			}
			// A hold that holds nothing goes.
			if err := txn.Delete(ctx, kv.Key); err != nil {
				return err
			}
		}
		if conflict {
			return nil
		}
		v, err := json.Marshal(record{DB: k.DB, ClassID: k.ClassID, ObjID: k.ObjID, SubID: k.SubID, Mode: mode,
			Node: m.node, Term: term, Session: h.id, PID: pid, At: now})
		if err != nil {
			return err
		}
		if err := txn.Put(ctx, mine, v); err != nil {
			return err
		}
		ok = true
		return nil
	})
	return ok, err
}

// release deletes records, retrying in the background if it cannot.
func (m *Manager) release(ks []keys.Key) {
	if len(ks) == 0 {
		return
	}
	defer m.wake()
	if m.ctx.Err() != nil {
		return // the node is stopping: its lease lapses
	}
	if m.deleteRecords(ks) == nil {
		return
	}
	go func() {
		backoff := 100 * time.Millisecond
		for m.ctx.Err() == nil {
			select {
			case <-m.ctx.Done():
				return
			case <-time.After(backoff):
			}
			if err := m.deleteRecords(ks); err == nil {
				m.wake()
				return
			} else {
				log.Debugf("advisory unlock: %v", err)
			}
			backoff = min(2*backoff, 2*time.Second)
		}
	}()
}

func (m *Manager) deleteRecords(ks []keys.Key) error {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(m.ctx), releaseTimeout)
	defer cancel()
	return m.db.RunTxn(ctx, "advisory-unlock", func(ctx context.Context, txn *kvclient.Txn) error {
		for _, k := range ks {
			if err := txn.Delete(ctx, k); err != nil {
				return err
			}
		}
		return nil
	})
}

// Info is one granted hold, as pg_locks lists it.
type Info struct {
	Key
	Mode Mode
	PID  int32
	Node int32
}

// List returns every hold in force across the cluster.
func (m *Manager) List(ctx context.Context) ([]Info, error) {
	var out []Info
	err := m.db.RunTxn(ctx, "advisory-list", func(ctx context.Context, txn *kvclient.Txn) error {
		out = nil
		ls, le := keys.AdvisoryLeaseSpan()
		lkvs, err := txn.Scan(ctx, ls, le, 0)
		if err != nil {
			return err
		}
		leases := map[int32]*lease{}
		for _, kv := range lkvs {
			var l lease
			if json.Unmarshal(kv.Value, &l) == nil {
				leases[l.Node] = &l
			}
		}
		s, e := keys.AdvisoryLockSpan()
		kvs, err := txn.Scan(ctx, s, e, 0)
		if err != nil {
			return err
		}
		now := m.db.Clock().PhysicalNow()
		for _, kv := range kvs {
			var r record
			if json.Unmarshal(kv.Value, &r) != nil {
				continue
			}
			if !m.live(leases[r.Node], r.Term, now) {
				continue
			}
			out = append(out, Info{Key: Key{DB: r.DB, ClassID: r.ClassID, ObjID: r.ObjID, SubID: r.SubID}, Mode: r.Mode, PID: r.PID, Node: r.Node})
		}
		return nil
	})
	return out, err
}
