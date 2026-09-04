// Package cli holds the connection plumbing shared by the datax command-line
// clients (sql, debug, backup, restore): building a SQL connection from a
// certificate directory, and reporting what a client is doing while it
// connects — so a slow, firewalled, or dead remote node reads as "still
// connecting to 10.0.0.1:26257 ... 5s" rather than as a blank terminal.
package cli

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"time"
)

// DefaultConnectTimeout bounds the connect phase of every CLI client — the
// TCP dial, TLS handshake, and (for SQL) authentication — separately from
// the operation that follows, which keeps its own budget (a backup may run
// for half an hour; a dial that has not completed in ten seconds never
// will).
const DefaultConnectTimeout = 10 * time.Second

// Progress controls how Connect reports on a connection attempt.
type Progress struct {
	// Out receives the progress text; nil means os.Stderr.
	Out io.Writer
	// TTY selects the interactive rendering: an immediate "connecting to"
	// line that is rewritten in place with the elapsed time and erased
	// once the connection is up. Without it, nothing is printed for a
	// connection that completes within After, and each later update is
	// appended as its own line (readable in logs and CI output).
	TTY bool
	// After is how long a silent attempt may run before the first update
	// (non-TTY only; a TTY reports immediately). Zero means one second.
	After time.Duration
	// Every is the interval between updates. Zero means one second.
	Every time.Duration
}

// DefaultProgress reports on os.Stderr, interactively when stderr is a
// terminal.
func DefaultProgress() *Progress {
	return &Progress{Out: os.Stderr, TTY: IsTerminal(os.Stderr)}
}

// IsTerminal reports whether f is a character device (a terminal rather
// than a pipe or file).
func IsTerminal(f *os.File) bool {
	if f == nil {
		return false
	}
	st, err := f.Stat()
	return err == nil && st.Mode()&os.ModeCharDevice != 0
}

// Connect runs dial under timeout, reporting progress to p. target names
// the address being connected to and what the kind of connection ("sql",
// "admin rpc", "http"), both of which appear in every progress line and in
// the error. The returned error names the target, the elapsed time when
// the timeout expired, and the underlying cause.
func Connect(ctx context.Context, p *Progress, target, what string, timeout time.Duration, dial func(ctx context.Context) error) error {
	if p == nil {
		p = DefaultProgress()
	}
	out := p.Out
	if out == nil {
		out = os.Stderr
	}
	after, every := p.After, p.Every
	if after <= 0 {
		after = time.Second
	}
	if every <= 0 {
		every = time.Second
	}
	if timeout <= 0 {
		timeout = DefaultConnectTimeout
	}
	dctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	start := time.Now()
	done := make(chan error, 1)
	go func() { done <- dial(dctx) }()

	shown := false
	if p.TTY {
		fmt.Fprintf(out, "connecting to %s (%s) ...", target, what)
		shown = true
	}
	next := time.NewTimer(after)
	defer next.Stop()
	for {
		select {
		case err := <-done:
			if shown && p.TTY {
				fmt.Fprint(out, "\r\033[K") // erase the progress line
			}
			if err == nil {
				return nil
			}
			var ce ConnectedError
			if errors.As(err, &ce) {
				return ce.Err // the connection was made; the failure is the operation's
			}
			if dctx.Err() != nil && ctx.Err() == nil {
				return fmt.Errorf("could not connect to %s (%s) after %s: %w", target, what, time.Since(start).Round(100*time.Millisecond), unwrapDeadline(err))
			}
			return fmt.Errorf("could not connect to %s (%s): %w", target, what, err)
		case <-next.C:
			elapsed := time.Since(start).Round(time.Second)
			if p.TTY {
				fmt.Fprintf(out, "\r\033[Kstill connecting to %s (%s) ... %s", target, what, elapsed)
			} else {
				fmt.Fprintf(out, "still connecting to %s (%s) ... %s\n", target, what, elapsed)
			}
			shown = true
			next.Reset(every)
		}
	}
}

// ConnectedError lets a dial function report that the connection was
// established and something after it failed (an HTTP error status, say):
// Connect returns Err as is, without describing it as a connection
// failure.
type ConnectedError struct{ Err error }

func (e ConnectedError) Error() string { return e.Err.Error() }
func (e ConnectedError) Unwrap() error { return e.Err }

// unwrapDeadline returns a cause worth showing when the connect timeout
// expired: the error as it stands unless it is nothing more than the
// context's own "deadline exceeded", which the message already states.
func unwrapDeadline(err error) error {
	if errors.Is(err, context.DeadlineExceeded) && err.Error() == context.DeadlineExceeded.Error() {
		return noResponseError{}
	}
	return err
}

// noResponseError reads as the cause of a timed-out connect while still
// matching errors.Is(err, context.DeadlineExceeded) for callers.
type noResponseError struct{}

func (noResponseError) Error() string { return "no response from the server" }
func (noResponseError) Unwrap() error { return context.DeadlineExceeded }
