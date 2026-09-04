package cli

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

// TestConnectQuietOnFastSuccess: a connection that completes before the
// first update prints nothing when stderr is not a terminal.
func TestConnectQuietOnFastSuccess(t *testing.T) {
	var out bytes.Buffer
	p := &Progress{Out: &out, After: 50 * time.Millisecond, Every: 50 * time.Millisecond}
	err := Connect(context.Background(), p, "10.0.0.1:26433", "sql", time.Second, func(context.Context) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	if out.Len() != 0 {
		t.Fatalf("expected no output, got %q", out.String())
	}
}

// TestConnectReportsWhileWaitingAndTimesOut: a dial that never completes
// produces elapsed-time updates naming the target and kind, then an error
// that names the target, the elapsed time, and the cause.
func TestConnectReportsWhileWaitingAndTimesOut(t *testing.T) {
	var out bytes.Buffer
	p := &Progress{Out: &out, After: 20 * time.Millisecond, Every: 20 * time.Millisecond}
	err := Connect(context.Background(), p, "10.255.255.1:26257", "admin rpc", 150*time.Millisecond, func(ctx context.Context) error {
		<-ctx.Done()
		return ctx.Err()
	})
	if err == nil {
		t.Fatal("expected an error")
	}
	msg := err.Error()
	for _, want := range []string{"could not connect to 10.255.255.1:26257 (admin rpc) after", "no response from the server"} {
		if !strings.Contains(msg, want) {
			t.Errorf("error %q lacks %q", msg, want)
		}
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("error should wrap the deadline: %v", err)
	}
	text := out.String()
	if !strings.Contains(text, "still connecting to 10.255.255.1:26257 (admin rpc) ...") {
		t.Errorf("progress output lacks the update line: %q", text)
	}
	if strings.Count(text, "still connecting") < 2 {
		t.Errorf("expected repeated updates, got %q", text)
	}
	if strings.Contains(text, "\r") {
		t.Errorf("non-TTY output must not rewrite lines: %q", text)
	}
}

// TestConnectDialErrorNamesTarget: a dial that fails outright (refused,
// verification error) is wrapped with the target and kind, without an
// elapsed-time clause.
func TestConnectDialErrorNamesTarget(t *testing.T) {
	var out bytes.Buffer
	p := &Progress{Out: &out, After: time.Second, Every: time.Second}
	cause := errors.New("dial tcp 127.0.0.1:1: connect: connection refused")
	err := Connect(context.Background(), p, "127.0.0.1:1", "sql", time.Second, func(context.Context) error { return cause })
	if err == nil || !errors.Is(err, cause) {
		t.Fatalf("expected the cause to be wrapped, got %v", err)
	}
	if want := "could not connect to 127.0.0.1:1 (sql): dial tcp"; !strings.Contains(err.Error(), want) {
		t.Errorf("error %q lacks %q", err.Error(), want)
	}
	if strings.Contains(err.Error(), "after") {
		t.Errorf("a plain dial failure must not claim a timeout: %q", err.Error())
	}
}

// TestConnectTTYRendering: on a terminal the line appears immediately, is
// rewritten in place with the elapsed time, and is erased on success.
func TestConnectTTYRendering(t *testing.T) {
	var out bytes.Buffer
	p := &Progress{Out: &out, TTY: true, After: 20 * time.Millisecond, Every: 20 * time.Millisecond}
	err := Connect(context.Background(), p, "db1:26433", "sql, TLS", time.Second, func(ctx context.Context) error {
		time.Sleep(60 * time.Millisecond)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	text := out.String()
	if !strings.HasPrefix(text, "connecting to db1:26433 (sql, TLS) ...") {
		t.Errorf("TTY output should open with the connecting line: %q", text)
	}
	if !strings.Contains(text, "\r\033[Kstill connecting to db1:26433 (sql, TLS) ...") {
		t.Errorf("TTY updates should rewrite in place: %q", text)
	}
	if !strings.HasSuffix(text, "\r\033[K") {
		t.Errorf("TTY output should end by erasing the line: %q", text)
	}
}

// TestConnectCallerCancel: cancellation by the caller is reported as such,
// not as the connect timeout.
func TestConnectCallerCancel(t *testing.T) {
	var out bytes.Buffer
	p := &Progress{Out: &out, After: time.Second, Every: time.Second}
	ctx, cancel := context.WithCancel(context.Background())
	go func() { time.Sleep(20 * time.Millisecond); cancel() }()
	err := Connect(ctx, p, "db1:26433", "sql", time.Minute, func(ctx context.Context) error {
		<-ctx.Done()
		return ctx.Err()
	})
	if err == nil || strings.Contains(err.Error(), "after") {
		t.Fatalf("expected a plain cancellation error, got %v", err)
	}
}

// TestConnectedErrorPassesThrough: a failure after the connection was
// made is returned as is, not described as a connection failure.
func TestConnectedErrorPassesThrough(t *testing.T) {
	var out bytes.Buffer
	p := &Progress{Out: &out, After: time.Second, Every: time.Second}
	cause := errors.New("403 Forbidden: admin role required")
	err := Connect(context.Background(), p, "db1:8080", "http", time.Second, func(context.Context) error {
		return ConnectedError{Err: cause}
	})
	if err != cause {
		t.Fatalf("expected the operation error unchanged, got %v", err)
	}
}
