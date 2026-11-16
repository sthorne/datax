// Package stop provides a Stopper that manages goroutine lifecycles and
// coordinates graceful shutdown.
package stop

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
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
	tasks  sync.WaitGroup // tasks

	mu           sync.Mutex
	stopping     bool // Stop has begun: no new workers or tasks
	closing      bool // Stop has taken the closers: a new one runs at once
	closers      []func()
	running      map[string]int // running tasks by name, for the drain log
	drainTimeout time.Duration
	onDrain      func(tasks int)
	done         chan struct{} // closed when Stop has finished
}

func NewStopper() *Stopper {
	ctx, cancel := context.WithCancel(context.Background())
	return &Stopper{
		ctx: ctx, cancel: cancel, done: make(chan struct{}),
		running: make(map[string]int), drainTimeout: DefaultDrainTimeout,
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
	s.mu.Lock()
	if s.stopping {
		s.mu.Unlock()
		return ErrStopped
	}
	s.tasks.Add(1)
	s.running[name]++
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		if s.running[name]--; s.running[name] == 0 {
			delete(s.running, name)
		}
		s.mu.Unlock()
		s.tasks.Done()
	}()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	defer context.AfterFunc(s.ctx, cancel)()
	f(ctx)
	return nil
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

// drainTasks waits for the running tasks, up to the drain timeout.
func (s *Stopper) drainTasks() {
	s.mu.Lock()
	n, timeout, hook := s.runningLocked(), s.drainTimeout, s.onDrain
	s.mu.Unlock()
	if n == 0 {
		return
	}
	if hook != nil {
		hook(n)
	}
	drained := make(chan struct{})
	go func() {
		s.tasks.Wait()
		close(drained)
	}()
	var bound <-chan time.Time
	if timeout > 0 {
		t := time.NewTimer(timeout)
		defer t.Stop()
		bound = t.C
	}
	select {
	case <-drained:
	case <-bound:
		s.mu.Lock()
		still := s.describeLocked()
		s.mu.Unlock()
		log.Errorf("stopper: tasks still running %s after the stop began, closing anyway: %s", timeout, still)
	}
}

func (s *Stopper) runningLocked() int {
	n := 0
	for _, c := range s.running {
		n += c
	}
	return n
}

func (s *Stopper) describeLocked() string {
	names := make([]string, 0, len(s.running))
	for name, c := range s.running {
		names = append(names, fmt.Sprintf("%s×%d", name, c))
	}
	sort.Strings(names)
	return strings.Join(names, ", ")
}
