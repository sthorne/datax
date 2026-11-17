// Package stop provides a Stopper that manages goroutine lifecycles and
// coordinates graceful shutdown.
package stop

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sthorne/datax/pkg/util/log"
)

// ErrStopped is returned by RunWorker and RunTask when the Stopper is
// shutting down.
var ErrStopped = errors.New("stopper is stopping")

// DefaultDrainTimeout bounds how long Stop waits for running tasks
// before it runs the closers anyway (see SetDrainTimeout).
const DefaultDrainTimeout = time.Minute

// Stopper tracks server goroutines and closers so a node can shut down
// cleanly: Stop cancels the shared context, waits for all workers and
// tasks to exit, then runs closers in reverse registration order.
//
// Workers are the node's own long-running goroutines (RunWorker). Tasks
// are bounded pieces of work started from outside — an RPC or HTTP
// request, a SQL connection — that use what the closers release (the
// storage engine above all). Neither can start once Stop has begun, and
// the closers do not run while either is still running (issue #324),
// so a closer never releases something a handler is still using.
type Stopper struct {
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup // workers

	// Tasks are admitted and tracked in shards, each with its own
	// mutex, so that tasks on different cores do not contend for one
	// lock and one map (issue #331). Stop marks every shard stopped
	// before anything else, under its lock: a task admitted to a shard
	// before the mark is counted in left, and one asked for after it is
	// refused.
	shards [numTaskShards]taskShard
	// left counts the tasks Stop is waiting for, plus one while Stop is
	// still marking the shards; drained is closed when it reaches zero.
	left    atomic.Int64
	drained chan struct{}

	mu           sync.Mutex
	stopping     bool // Stop has begun: no new workers or tasks
	closing      bool // Stop has taken the closers: a new one runs at once
	closers      []func()
	drainTimeout time.Duration
	onDrain      func(tasks int)
	done         chan struct{} // closed when Stop has finished
}

// numTaskShards is the number of task shards: enough that tasks on a
// many-core machine rarely meet on one, few enough that Stop's pass
// over them is cheap.
const numTaskShards = 32

// taskShard is one shard of the running tasks. Its slots hold the
// running tasks' names (for the drain log) and the cancel functions of
// their contexts (for Stop); a free slot has a nil cancel and its index
// in free.
type taskShard struct {
	mu      sync.Mutex
	stopped bool
	live    int
	slots   []taskSlot
	free    []int32
	_       [64]byte // keep neighbouring shards' mutexes off one cache line
}

type taskSlot struct {
	name   string
	cancel context.CancelFunc
}

func NewStopper() *Stopper {
	ctx, cancel := context.WithCancel(context.Background())
	return &Stopper{
		ctx: ctx, cancel: cancel, done: make(chan struct{}),
		drained: make(chan struct{}), drainTimeout: DefaultDrainTimeout,
	}
}

// Ctx returns a context that is canceled when Stop begins.
func (s *Stopper) Ctx() context.Context { return s.ctx }

// ShouldQuiesce returns a channel closed when Stop begins.
func (s *Stopper) ShouldQuiesce() <-chan struct{} { return s.ctx.Done() }

// RunWorker runs f in a goroutine tracked by the Stopper. f should exit when
// the Stopper's context is canceled.
func (s *Stopper) RunWorker(f func(ctx context.Context)) error {
	s.mu.Lock()
	if s.stopping {
		s.mu.Unlock()
		return ErrStopped
	}
	s.wg.Add(1)
	s.mu.Unlock()
	go func() {
		defer s.wg.Done()
		f(s.ctx)
	}()
	return nil
}

// RunTask runs f on the calling goroutine as a task of the Stopper: Stop
// runs no closer until f has returned. The context f is given is
// canceled when ctx is and when Stop begins, and f is expected to give
// up promptly once it is. Once Stop has begun RunTask does not run f and
// returns ErrStopped. name says what the task is in the log Stop writes
// when tasks hold it up.
func (s *Stopper) RunTask(ctx context.Context, name string, f func(ctx context.Context)) error {
	ctx, cancel := context.WithCancel(ctx)
	sh := &s.shards[rand.Uint32()%numTaskShards]
	sh.mu.Lock()
	if sh.stopped {
		sh.mu.Unlock()
		cancel()
		return ErrStopped
	}
	var i int32
	if n := len(sh.free); n > 0 {
		i = sh.free[n-1]
		sh.free = sh.free[:n-1]
	} else {
		i = int32(len(sh.slots))
		sh.slots = append(sh.slots, taskSlot{})
	}
	sh.slots[i] = taskSlot{name: name, cancel: cancel}
	sh.live++
	sh.mu.Unlock()
	defer s.endTask(sh, i)
	f(ctx)
	return nil
}

// endTask releases the slot of a task that has returned and, once Stop
// has counted it, tells Stop it is no longer waiting for it.
func (s *Stopper) endTask(sh *taskShard, i int32) {
	sh.mu.Lock()
	cancel := sh.slots[i].cancel
	sh.slots[i] = taskSlot{}
	sh.free = append(sh.free, i)
	sh.live--
	counted := sh.stopped
	sh.mu.Unlock()
	cancel()
	if counted && s.left.Add(-1) == 0 {
		close(s.drained)
	}
}

// SetDrainTimeout bounds how long Stop waits for running tasks once the
// workers have exited (DefaultDrainTimeout unless set; d <= 0 waits
// without a bound). A task still running at the bound is logged, and
// the closers run anyway: shutdown stays bounded even if a task ignores
// its canceled context.
func (s *Stopper) SetDrainTimeout(d time.Duration) {
	s.mu.Lock()
	s.drainTimeout = d
	s.mu.Unlock()
}

// SetTestingDrainHook installs f, which Stop calls (with their number)
// when it finds tasks still running once the workers have exited, just
// before it waits for them. Tests use it to tell a Stop held up by a
// task.
func (s *Stopper) SetTestingDrainHook(f func(tasks int)) {
	s.mu.Lock()
	s.onDrain = f
	s.mu.Unlock()
}

// AddCloser registers f to run during Stop, after all workers and tasks
// have exited. Closers run in reverse registration order. A closer
// registered once Stop has run the closers (or is running them) runs at
// once, on the caller's goroutine: the resource it releases still needs
// releasing, and a closer silently dropped is a leak (issue #139).
func (s *Stopper) AddCloser(f func()) {
	s.mu.Lock()
	if s.closing {
		s.mu.Unlock()
		f()
		return
	}
	s.closers = append(s.closers, f)
	s.mu.Unlock()
}

// Stop cancels the context, waits for workers and then for tasks
// (bounded, see SetDrainTimeout), then runs closers. Safe to call more
// than once and concurrently: every caller returns only once the
// shutdown — closers included — has finished. Stop must not be called
// from inside a task: it would wait for the task calling it.
func (s *Stopper) Stop() {
	s.mu.Lock()
	if s.stopping {
		s.mu.Unlock()
		<-s.done
		return
	}
	s.stopping = true
	s.mu.Unlock()
	defer close(s.done)

	s.stopTasks()
	s.cancel()
	s.wg.Wait()
	s.drainTasks()

	s.mu.Lock()
	closers := s.closers
	s.closers = nil
	s.closing = true
	s.mu.Unlock()
	for i := len(closers) - 1; i >= 0; i-- {
		closers[i]()
	}
}

// stopTasks refuses new tasks and cancels the contexts of the running
// ones. Every shard is marked before any context is canceled, so a task
// that sees its context canceled and asks for another is refused.
func (s *Stopper) stopTasks() {
	s.left.Store(1) // held while marking: left cannot reach zero early
	for i := range s.shards {
		sh := &s.shards[i]
		sh.mu.Lock()
		sh.stopped = true
		s.left.Add(int64(sh.live))
		sh.mu.Unlock()
	}
	if s.left.Add(-1) == 0 {
		close(s.drained)
	}
	for i := range s.shards {
		sh := &s.shards[i]
		sh.mu.Lock()
		for _, t := range sh.slots {
			if t.cancel != nil {
				t.cancel()
			}
		}
		sh.mu.Unlock()
	}
}

// drainTasks waits for the running tasks, up to the drain timeout.
func (s *Stopper) drainTasks() {
	n := int(s.left.Load())
	if n == 0 {
		return
	}
	s.mu.Lock()
	timeout, hook := s.drainTimeout, s.onDrain
	s.mu.Unlock()
	if hook != nil {
		hook(n)
	}
	var bound <-chan time.Time
	if timeout > 0 {
		t := time.NewTimer(timeout)
		defer t.Stop()
		bound = t.C
	}
	select {
	case <-s.drained:
	case <-bound:
		log.Errorf("stopper: tasks still running %s after the stop began, closing anyway: %s", timeout, s.describeTasks())
	}
}

// describeTasks lists the running tasks by name, with their counts.
func (s *Stopper) describeTasks() string {
	running := make(map[string]int)
	for i := range s.shards {
		sh := &s.shards[i]
		sh.mu.Lock()
		for _, t := range sh.slots {
			if t.cancel != nil {
				running[t.name]++
			}
		}
		sh.mu.Unlock()
	}
	names := make([]string, 0, len(running))
	for name, c := range running {
		names = append(names, fmt.Sprintf("%s×%d", name, c))
	}
	sort.Strings(names)
	return strings.Join(names, ", ")
}
