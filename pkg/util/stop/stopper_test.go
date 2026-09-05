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
