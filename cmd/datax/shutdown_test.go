package main

import (
	"bytes"
	"context"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/sthorne/datax/pkg/server"
)

// TestAwaitShutdown: one signal drains then stops; a second cuts the
// drain short; a third, or a stop that hangs past the timeout, exits.
func TestAwaitShutdown(t *testing.T) {
	type calls struct {
		mu                     sync.Mutex
		drained, stopped       bool
		drainCtxCancelled      bool
		exitCode               int
		exited                 chan struct{}
		drainBlocksUntilCancel bool
		stopHangs              bool
	}
	run := func(t *testing.T, signals int, c *calls, timeout time.Duration) *lockedBuffer {
		t.Helper()
		sig := make(chan os.Signal, 3)
		for i := 0; i < signals; i++ {
			sig <- os.Interrupt
		}
		out := &lockedBuffer{}
		finished := make(chan struct{})
		go func() {
			awaitShutdown(sig, shutdownHooks{
				timeout: timeout,
				drain: func(ctx context.Context) server.DrainReport {
					c.mu.Lock()
					c.drained = true
					c.mu.Unlock()
					if c.drainBlocksUntilCancel {
						<-ctx.Done()
						c.mu.Lock()
						c.drainCtxCancelled = true
						c.mu.Unlock()
					}
					return server.DrainReport{LeasesTransferred: 1}
				},
				stop: func() {
					c.mu.Lock()
					c.stopped = true
					c.mu.Unlock()
					if c.stopHangs {
						select {}
					}
				},
				exit: func(code int) {
					c.mu.Lock()
					c.exitCode = code
					c.mu.Unlock()
					close(c.exited)
					select {} // a real exit never returns
				},
				out: out,
			})
			close(finished)
		}()
		select {
		case <-finished:
		case <-c.exited:
		case <-time.After(5 * time.Second):
			t.Fatal("awaitShutdown did not finish")
		}
		return out
	}

	t.Run("one signal drains and stops", func(t *testing.T) {
		c := &calls{exited: make(chan struct{})}
		out := run(t, 1, c, time.Second)
		if !c.drained || !c.stopped || c.exitCode != 0 {
			t.Fatalf("drained=%v stopped=%v exit=%d", c.drained, c.stopped, c.exitCode)
		}
		if !bytes.Contains(out.Bytes(), []byte("1 leases transferred")) || !bytes.Contains(out.Bytes(), []byte("stopped")) {
			t.Fatalf("output: %s", out.String())
		}
	})
	t.Run("no drain when the timeout is zero", func(t *testing.T) {
		c := &calls{exited: make(chan struct{})}
		run(t, 1, c, 0)
		if c.drained || !c.stopped {
			t.Fatalf("drained=%v stopped=%v", c.drained, c.stopped)
		}
	})
	t.Run("second signal cuts the drain short", func(t *testing.T) {
		c := &calls{exited: make(chan struct{}), drainBlocksUntilCancel: true}
		out := run(t, 2, c, time.Minute)
		if !c.drainCtxCancelled || !c.stopped || c.exitCode != 0 {
			t.Fatalf("cancelled=%v stopped=%v exit=%d: %s", c.drainCtxCancelled, c.stopped, c.exitCode, out.String())
		}
	})
	t.Run("third signal exits a hung stop", func(t *testing.T) {
		c := &calls{exited: make(chan struct{}), drainBlocksUntilCancel: true, stopHangs: true}
		out := run(t, 3, c, time.Minute)
		if c.exitCode != 1 || !bytes.Contains(out.Bytes(), []byte("third signal")) {
			t.Fatalf("exit=%d: %s", c.exitCode, out.String())
		}
	})
	t.Run("the timeout exits a hung stop after the second signal", func(t *testing.T) {
		c := &calls{exited: make(chan struct{}), drainBlocksUntilCancel: true, stopHangs: true}
		out := run(t, 2, c, 50*time.Millisecond)
		if c.exitCode != 1 || !bytes.Contains(out.Bytes(), []byte("did not complete")) {
			t.Fatalf("exit=%d: %s", c.exitCode, out.String())
		}
	})
}

// lockedBuffer is an io.Writer the sequencer's goroutines share.
type lockedBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (l *lockedBuffer) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.Write(p)
}

func (l *lockedBuffer) Bytes() []byte {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]byte(nil), l.b.Bytes()...)
}

func (l *lockedBuffer) String() string { return string(l.Bytes()) }
