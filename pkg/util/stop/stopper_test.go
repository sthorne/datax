package stop

import (
	"context"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestStopWaitsForClosersOnEveryCaller (issue #139): Stop returns to every
// caller — the one that runs the shutdown and the ones that found it in
// progress — only once the closers have run.
func TestStopWaitsForClosersOnEveryCaller(t *testing.T) {
	for i := 0; i < 50; i++ {
		s := NewStopper()
		var ran atomic.Bool
		s.AddCloser(func() {
			time.Sleep(2 * time.Millisecond)
			ran.Store(true)
		})
		if err := s.RunWorker(func(ctx context.Context) { <-ctx.Done() }); err != nil {
			t.Fatal(err)
		}
		var wg sync.WaitGroup
		for j := 0; j < 4; j++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				s.Stop()
				if !ran.Load() {
					t.Error("Stop returned before the closer ran")
				}
			}()
		}
		wg.Wait()
		if t.Failed() {
			return
		}
	}
}

// TestAddCloserDuringAndAfterStop (issue #139): a closer registered while
// Stop waits for the workers runs in Stop, before it returns and before
// the closers registered earlier; one registered after Stop has taken
// the closers runs at once. None is dropped.
func TestAddCloserDuringAndAfterStop(t *testing.T) {
	s := NewStopper()
	release := make(chan struct{})
	if err := s.RunWorker(func(ctx context.Context) {
		<-ctx.Done()
		<-release
	}); err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	var order []string
	note := func(what string) func() {
		return func() {
			mu.Lock()
			defer mu.Unlock()
			order = append(order, what)
		}
	}
	s.AddCloser(note("first"))
	stopped := make(chan struct{})
	go func() {
		s.Stop()
		close(stopped)
	}()
	<-s.ShouldQuiesce() // Stop has begun; the worker holds it up
	s.AddCloser(note("late"))
	close(release)
	<-stopped
	if err := s.RunWorker(func(context.Context) {}); err != ErrStopped {
		t.Fatalf("RunWorker after Stop: %v", err)
	}
	s.AddCloser(note("after"))
	mu.Lock()
	defer mu.Unlock()
	if want := []string{"late", "first", "after"}; !reflect.DeepEqual(order, want) {
		t.Fatalf("closers ran as %v, want %v", order, want)
	}
}

// TestStopWaitsForTasks (issue #324): a task running when Stop begins
// has its context canceled, and no closer runs until it has returned —
// a closer releases what the task may still be using. A task asked for
// once Stop has begun is refused and never runs.
func TestStopWaitsForTasks(t *testing.T) {
	s := NewStopper()
	var closed atomic.Bool
	s.AddCloser(func() { closed.Store(true) })
	inTask, release := make(chan struct{}), make(chan struct{})
	taskDone := make(chan error, 1)
	var closedInTask atomic.Bool
	sawCancel := make(chan struct{})
	go func() {
		taskDone <- s.RunTask(context.Background(), "test", func(ctx context.Context) {
			close(inTask)
			<-ctx.Done() // Stop began
			close(sawCancel)
			<-release
			closedInTask.Store(closed.Load())
		})
	}()
	<-inTask
	draining := make(chan int, 1)
	s.SetTestingDrainHook(func(n int) { draining <- n })
	stopped := make(chan struct{})
	go func() {
		s.Stop()
		close(stopped)
	}()
	if n := <-draining; n != 1 {
		t.Fatalf("Stop found %d tasks running, want 1", n)
	}
	select {
	case <-sawCancel:
	case <-time.After(10 * time.Second):
		t.Fatal("the task's context was not canceled when Stop began")
	}
	if err := s.RunTask(context.Background(), "late", func(context.Context) {
		t.Error("a task ran after Stop began")
	}); err != ErrStopped {
		t.Fatalf("RunTask after Stop began: %v, want ErrStopped", err)
	}
	select {
	case <-stopped:
		t.Fatal("Stop returned while a task was still running")
	default:
	}
	close(release)
	if err := <-taskDone; err != nil {
		t.Fatal(err)
	}
	<-stopped
	if closedInTask.Load() {
		t.Fatal("a closer ran while a task was still running")
	}
	if !closed.Load() {
		t.Fatal("Stop returned before the closer ran")
	}
}

// TestStopDrainIsBounded (issue #324): a task that ignores its canceled
// context holds Stop up for the drain timeout, no longer.
func TestStopDrainIsBounded(t *testing.T) {
	s := NewStopper()
	s.SetDrainTimeout(50 * time.Millisecond)
	var closed atomic.Bool
	s.AddCloser(func() { closed.Store(true) })
	inTask, release := make(chan struct{}), make(chan struct{})
	defer close(release)
	go func() {
		_ = s.RunTask(context.Background(), "stuck", func(context.Context) {
			close(inTask)
			<-release
		})
	}()
	<-inTask
	stopped := make(chan struct{})
	go func() {
		s.Stop()
		close(stopped)
	}()
	select {
	case <-stopped:
	case <-time.After(10 * time.Second):
		t.Fatal("Stop did not return at the drain timeout")
	}
	if !closed.Load() {
		t.Fatal("Stop returned without running the closers")
	}
}

// TestRunTaskWithoutStop: a task's context is its caller's while the
// Stopper runs, and is released when the task returns.
func TestRunTaskWithoutStop(t *testing.T) {
	s := NewStopper()
	defer s.Stop()
	ctx, cancel := context.WithCancel(context.Background())
	var ran bool
	if err := s.RunTask(ctx, "test", func(tctx context.Context) {
		ran = true
		if tctx.Err() != nil {
			t.Error("task context canceled before Stop")
		}
		cancel()
		<-tctx.Done() // the caller's cancellation reaches the task
	}); err != nil {
		t.Fatal(err)
	}
	if !ran {
		t.Fatal("RunTask did not run the task")
	}
}
