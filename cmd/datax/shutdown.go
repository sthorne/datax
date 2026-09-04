package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/sthorne/datax/pkg/server"
)

// shutdownHooks are what awaitShutdown drives: the drain, the stop, and
// the way out when neither completes.
type shutdownHooks struct {
	// timeout bounds the drain; zero or negative skips it.
	timeout time.Duration
	drain   func(ctx context.Context) server.DrainReport
	stop    func()
	exit    func(code int)
	out     io.Writer
}

// awaitShutdown is the operator-facing stop sequence. The first signal
// drains (leases to peers, SQL connections finished) within the timeout
// and then stops the node. A second signal cuts the drain short and
// goes straight to the stop. A third — or the stop taking longer than
// the timeout after the second — exits the process without waiting, for
// the day a worker refuses to wind down.
func awaitShutdown(sig <-chan os.Signal, h shutdownHooks) {
	<-sig
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if h.timeout > 0 {
		ctx, cancel = context.WithTimeout(ctx, h.timeout)
		defer cancel()
		fmt.Fprintf(h.out, "shutting down: draining for up to %s (signal again to skip)...\n", h.timeout)
	} else {
		fmt.Fprintln(h.out, "shutting down...")
	}
	done := make(chan struct{})
	go func() {
		select {
		case <-done:
			return
		case <-sig:
		}
		fmt.Fprintln(h.out, "second signal: skipping the rest of the drain")
		cancel()
		wait := h.timeout
		if wait <= 0 {
			wait = server.DefaultDrainTimeout
		}
		select {
		case <-done:
			return
		case <-sig:
			fmt.Fprintln(h.out, "third signal: exiting without waiting")
		case <-time.After(wait):
			fmt.Fprintf(h.out, "shutdown did not complete within %s: exiting\n", wait)
		}
		h.exit(1)
	}()
	if h.timeout > 0 {
		fmt.Fprintln(h.out, h.drain(ctx))
	}
	h.stop()
	close(done)
	fmt.Fprintln(h.out, "stopped")
}
