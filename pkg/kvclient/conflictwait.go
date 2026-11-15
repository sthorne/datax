package kvclient

import (
	"context"
	"math/rand/v2"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/sthorne/datax/pkg/storage"
)

// Waiting on a live conflicting intent (docs/transactions.md "Conflicts
// and pushes"). A transaction blocked on another's intent re-pushes the
// holder's record until the holder finishes, but it does not only poll:
//
//   - When the holder is coordinated by this node — every conflict on a
//     single node, and any between two sessions on the same gateway —
//     its coordinator wakes the waiters the moment the holder's fate is
//     settled and its intents resolved (txnWaits.finished), and the
//     waiter's re-push finds the record final. So does a pusher on this
//     node that finds the holder's record final, or aborts it.
//   - Otherwise (the holder runs on another gateway, or a wake was
//     missed) the waiter re-pushes after a backoff: conflictPollMin,
//     doubling to conflictPollMax, each wait drawn at random from its
//     upper half so waiters queued behind one holder do not push in
//     step. Most holders finish within a few milliseconds, so the short
//     first waits catch them; the cap keeps a long wait as cheap as the
//     fixed 100 ms poll it replaces.
//
// Each round re-runs deadlock detection, so a cycle is still found
// within a few rounds; the rounds just start sooner.
const (
	conflictPollMin = 5 * time.Millisecond
	conflictPollMax = 100 * time.Millisecond
)

// conflictBackoff returns how long the n'th wait (from 0) on one holder
// lasts before the waiter re-pushes: conflictPollMin doubling to
// conflictPollMax, drawn from the upper half of that.
func conflictBackoff(n int) time.Duration {
	d := conflictPollMax
	if n < 8 {
		d = min(conflictPollMin<<n, conflictPollMax)
	}
	return d/2 + rand.N(d/2+1)
}

// txnWaits wakes the transactions on this node that are blocked on
// another transaction's intents when that transaction finishes. Entries
// exist only while someone waits; a wake for a transaction nobody waits
// on costs a map lookup.
type txnWaits struct {
	mu sync.Mutex
	m  map[uuid.UUID]*txnWait
}

type txnWait struct {
	ch chan struct{} // closed when the transaction finishes
	n  int           // registered watchers
}

// conflictWatch is a set of registrations taken before a push, so a wake
// that comes between the push finding the holder live and the wait
// starting is not lost.
type conflictWatch struct {
	w   *txnWaits
	ids []uuid.UUID
	es  []*txnWait
}

// watch registers for the finish of every transaction owning one of
// intents. Release it (wait does) when done.
func (w *txnWaits) watch(intents []storage.Intent) *conflictWatch {
	cw := &conflictWatch{w: w}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.m == nil {
		w.m = map[uuid.UUID]*txnWait{}
	}
next:
	for i := range intents {
		id := intents[i].Txn.ID
		for _, seen := range cw.ids {
			if seen == id {
				continue next
			}
		}
		e := w.m[id]
		if e == nil {
			e = &txnWait{ch: make(chan struct{})}
			w.m[id] = e
		}
		e.n++
		cw.ids = append(cw.ids, id)
		cw.es = append(cw.es, e)
	}
	return cw
}

// finished wakes everyone waiting on transaction id. Called once the
// transaction's record is final (or it may be: a wake that comes too
// early costs the waiter one push) and, where this node resolves them,
// its intents are resolved.
func (w *txnWaits) finished(id uuid.UUID) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if e := w.m[id]; e != nil {
		close(e.ch)
		delete(w.m, id)
	}
}

// release drops the registrations. Idempotent.
func (cw *conflictWatch) release() {
	if cw.w == nil {
		return
	}
	cw.w.mu.Lock()
	for i, e := range cw.es {
		e.n--
		if e.n == 0 && cw.w.m[cw.ids[i]] == e {
			delete(cw.w.m, cw.ids[i])
		}
	}
	cw.w.mu.Unlock()
	cw.w = nil
}

// wait blocks until transaction holder finishes, d elapses, or ctx is
// done, then releases the watch.
func (cw *conflictWatch) wait(ctx context.Context, holder uuid.UUID, d time.Duration) error {
	defer cw.release()
	var woken <-chan struct{}
	for i, id := range cw.ids {
		if id == holder {
			woken = cw.es[i].ch
		}
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-woken:
	case <-timer.C:
	}
	return nil
}

// conflictWait tracks one operation's waits on live conflicts.
type conflictWait struct {
	start  time.Time     // when the first wait began
	holder uuid.UUID     // the transaction the current run of waits is on
	n      int           // waits on holder so far
	logged time.Duration // waited() at the last progress log
}

// waited is how long the operation has been waiting on live conflicts.
func (c *conflictWait) waited() time.Duration {
	if c.start.IsZero() {
		return 0
	}
	return time.Since(c.start)
}

// next returns how long to wait on holder before re-pushing: the backoff
// restarts for a new holder, and the wait ends no later than a lock
// timeout (0 = none) would.
func (c *conflictWait) next(holder uuid.UUID, lockTimeout time.Duration) time.Duration {
	if c.start.IsZero() {
		c.start = time.Now()
	}
	if holder != c.holder {
		c.holder, c.n = holder, 0
	}
	d := conflictBackoff(c.n)
	c.n++
	if lockTimeout > 0 {
		if rem := lockTimeout - c.waited(); rem < d {
			d = max(rem, time.Millisecond)
		}
	}
	return d
}

// dueLog reports whether another 2s of waiting has passed since the last
// progress log.
func (c *conflictWait) dueLog() bool {
	if w := c.waited(); w >= c.logged+2*time.Second {
		c.logged = w
		return true
	}
	return false
}

// wakeWaiters wakes the transactions on this node waiting on t.
func (t *Txn) wakeWaiters() {
	t.mu.Lock()
	id := t.mu.txn.ID
	t.mu.Unlock()
	t.db.waits.finished(id)
}
