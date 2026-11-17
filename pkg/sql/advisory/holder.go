package advisory

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/kvclient"
)

// Holder is one session's advisory locks. A lock is held at most once
// per mode in the store, however many times the session took it: the
// counts are kept here, separately for the session and its transaction,
// and the record goes when both reach zero. A Holder is used by its
// session's goroutine only (the node's lease watchdog may mark it lost
// at any time).
type Holder struct {
	m      *Manager
	id     uint64
	onLost func()

	mu    sync.Mutex
	pid   int32
	lost  bool
	locks map[lockID]*held
	// xact lists the transaction-level acquisitions in order, so that a
	// rollback to a savepoint can give back those made after it.
	xact []lockID
}

type lockID struct {
	Key
	Mode Mode
}

type held struct {
	session, xact int
	term          uint64
}

// NewHolder makes the advisory-lock state of a session. onLost is called
// (once, from another goroutine) when the node loses the lease the
// session's locks are held under: the session must be ended.
func (m *Manager) NewHolder(onLost func()) *Holder {
	return &Holder{m: m, id: m.sessions.Add(1), onLost: onLost, locks: map[lockID]*held{}}
}

// SetPID records the session's backend process ID (pg_locks.pid).
func (h *Holder) SetPID(pid int32) {
	h.mu.Lock()
	h.pid = pid
	h.mu.Unlock()
}

// Manager returns the node's manager.
func (h *Holder) Manager() *Manager { return h.m }

func (h *Holder) lose() {
	h.mu.Lock()
	already := h.lost
	h.lost = true
	h.mu.Unlock()
	if !already && h.onLost != nil {
		h.onLost()
	}
}

func (h *Holder) recordKey(id lockID, term uint64) keys.Key {
	return keys.AdvisoryLockKey(id.DB, id.ClassID, id.ObjID, id.SubID, h.m.node, term, h.id, uint8(id.Mode))
}

// countLocked adds one hold of id at the given level.
func (h *Holder) countLocked(st *held, id lockID, xact bool) {
	if xact {
		st.xact++
		h.xact = append(h.xact, id)
	} else {
		st.session++
	}
}

// Lock takes k in mode, at the transaction's level when xact is set and
// the session's otherwise. With wait it waits until the lock is free,
// ctx ends (cancel, statement_timeout) or the lock timeout ctx carries
// (kvclient.WithLockTimeout) passes (a kvclient.LockTimeoutError);
// without, it reports false at once when another session holds the
// lock in a conflicting mode.
func (h *Holder) Lock(ctx context.Context, k Key, mode Mode, xact, wait bool) (bool, error) {
	id := lockID{Key: k, Mode: mode}
	h.mu.Lock()
	if h.lost {
		h.mu.Unlock()
		return false, ErrLost
	}
	if st := h.locks[id]; st != nil {
		h.countLocked(st, id, xact)
		h.mu.Unlock()
		return true, nil
	}
	pid := h.pid
	h.mu.Unlock()

	m := h.m
	m.use(1)
	defer m.use(-1)
	start := time.Now()
	lockTimeout := kvclient.LockTimeoutFrom(ctx)
	poll := minPoll
	for {
		term, err := m.currentTerm(ctx)
		if err != nil {
			return false, ctxErr(ctx, err)
		}
		woken := m.releasedChan()
		ok, err := m.tryAcquire(ctx, h, k, mode, term, pid)
		if err != nil && !errors.Is(err, errTermLost) && !kvclient.IsRetryable(err) {
			// The attempt may have committed without learning so (a
			// cancel or a timeout in the commit's round trip): make sure
			// it left no hold behind.
			m.release([]keys.Key{h.recordKey(id, term)})
		}
		switch {
		case errors.Is(err, errTermLost):
			m.fence(term)
			continue
		case err != nil && kvclient.IsRetryable(err) && ctx.Err() == nil:
			// Contention on the lock's records outlasted the retries:
			// as good as finding it held.
		case err != nil:
			return false, ctxErr(ctx, err)
		case ok:
			h.mu.Lock()
			if !h.lost && m.register(h, term) {
				st := &held{term: term}
				h.locks[id] = st
				h.countLocked(st, id, xact)
				h.mu.Unlock()
				return true, nil
			}
			lost := h.lost
			h.mu.Unlock()
			// The term ended while the record was written: it holds
			// nothing; take the lock again under a new one.
			m.release([]keys.Key{h.recordKey(id, term)})
			if lost {
				return false, ErrLost
			}
			continue
		}
		if !wait {
			return false, nil
		}
		d := poll
		poll = min(2*poll, maxPoll)
		if lockTimeout > 0 {
			left := lockTimeout - time.Since(start)
			if left <= 0 {
				return false, &kvclient.LockTimeoutError{Waited: time.Since(start)}
			}
			d = min(d, left)
		}
		t := time.NewTimer(d)
		select {
		case <-ctx.Done():
			t.Stop()
			return false, ctx.Err()
		case <-woken:
			poll = minPoll
		case <-t.C:
		}
		t.Stop()
	}
}

// dropLocked takes n holds of id away at one level and returns the
// record to delete when none is left.
func (h *Holder) dropLocked(id lockID, xact bool, n int) (keys.Key, bool) {
	st := h.locks[id]
	if st == nil {
		return nil, false
	}
	if xact {
		st.xact = max(st.xact-n, 0)
	} else {
		st.session = max(st.session-n, 0)
	}
	if st.session+st.xact > 0 {
		return nil, false
	}
	delete(h.locks, id)
	return h.recordKey(id, st.term), true
}

// finish deletes released records, and gives the session up as a
// holder once it holds nothing.
func (h *Holder) finish(ks []keys.Key) {
	h.mu.Lock()
	empty := len(h.locks) == 0
	h.mu.Unlock()
	if empty {
		h.m.unregister(h)
	}
	h.m.release(ks)
}

// Unlock gives back one session-level hold of k in mode, reporting
// false when the session held none (PostgreSQL then warns).
func (h *Holder) Unlock(k Key, mode Mode) (bool, error) {
	id := lockID{Key: k, Mode: mode}
	h.mu.Lock()
	if h.lost {
		h.mu.Unlock()
		return false, ErrLost
	}
	if st := h.locks[id]; st == nil || st.session == 0 {
		h.mu.Unlock()
		return false, nil
	}
	rk, gone := h.dropLocked(id, false, 1)
	h.mu.Unlock()
	if gone {
		h.finish([]keys.Key{rk})
	}
	return true, nil
}

// UnlockAll gives back every session-level hold; transaction-level
// ones stay until the transaction ends.
func (h *Holder) UnlockAll() error {
	h.mu.Lock()
	if h.lost {
		h.mu.Unlock()
		return ErrLost
	}
	var ks []keys.Key
	for id, st := range h.locks {
		if rk, gone := h.dropLocked(id, false, st.session); gone {
			ks = append(ks, rk)
		}
	}
	h.mu.Unlock()
	h.finish(ks)
	return nil
}

// XactMark is where the transaction's acquisitions stand (a savepoint).
func (h *Holder) XactMark() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.xact)
}

// RollbackXact gives back the transaction-level holds taken since mark
// (ROLLBACK TO SAVEPOINT); EndXact, those of the whole transaction.
func (h *Holder) RollbackXact(mark int) {
	h.mu.Lock()
	if mark < 0 || mark > len(h.xact) {
		mark = 0
	}
	var ks []keys.Key
	for _, id := range h.xact[mark:] {
		if rk, gone := h.dropLocked(id, true, 1); gone {
			ks = append(ks, rk)
		}
	}
	h.xact = h.xact[:mark]
	h.mu.Unlock()
	h.finish(ks)
}

// EndXact gives back every transaction-level hold (COMMIT, ROLLBACK, the
// end of a statement outside a transaction block).
func (h *Holder) EndXact() {
	h.mu.Lock()
	has := len(h.xact) > 0
	h.mu.Unlock()
	if has {
		h.RollbackXact(0)
	}
}

// Close gives back everything (the session's end).
func (h *Holder) Close() {
	h.mu.Lock()
	var ks []keys.Key
	for id, st := range h.locks {
		ks = append(ks, h.recordKey(id, st.term))
	}
	h.locks, h.xact = map[lockID]*held{}, nil
	h.mu.Unlock()
	h.finish(ks)
}

// ctxErr is err, or the context's own error when the context ended: a
// KV call cut short by a cancel or a deadline reports it in its own
// words, and the session must see the cancel or the timeout.
func ctxErr(ctx context.Context, err error) error {
	if cerr := ctx.Err(); cerr != nil {
		return cerr
	}
	return err
}
