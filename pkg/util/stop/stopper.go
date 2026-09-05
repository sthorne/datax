// Package stop provides a Stopper that manages goroutine lifecycles and
// coordinates graceful shutdown.
package stop

import (
	"context"
	"errors"
	"sync"
)

// ErrStopped is returned by RunTask when the Stopper is shutting down.
var ErrStopped = errors.New("stopper is stopping")

// Stopper tracks server goroutines and closers so a node can shut down
// cleanly: Stop cancels the shared context, waits for all tasks to exit,
// then runs closers in reverse registration order.
type Stopper struct {
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	mu       sync.Mutex
	stopping bool // Stop has begun: no new workers
	closing  bool // Stop has taken the closers: a new one runs at once
	closers  []func()
	done     chan struct{} // closed when Stop has finished
}

func NewStopper() *Stopper {
	ctx, cancel := context.WithCancel(context.Background())
	return &Stopper{ctx: ctx, cancel: cancel, done: make(chan struct{})}
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

// AddCloser registers f to run during Stop, after all workers have exited.
// Closers run in reverse registration order. A closer registered once
// Stop has run the closers (or is running them) runs at once, on the
// caller's goroutine: the resource it releases still needs releasing,
// and a closer silently dropped is a leak (issue #139).
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

// Stop cancels the context, waits for workers, then runs closers.
// Safe to call more than once and concurrently: every caller returns
// only once the shutdown — closers included — has finished.
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

	s.mu.Lock()
	closers := s.closers
	s.closers = nil
	s.closing = true
	s.mu.Unlock()
	for i := len(closers) - 1; i >= 0; i-- {
		closers[i]()
	}
}
