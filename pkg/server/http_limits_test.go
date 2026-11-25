package server

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/pprof"
	"strings"
	"testing"
	"time"
)

// serveHTTPBounded serves h the way startHTTP does, with the node's
// bounds, on a loopback port, and returns its address.
func serveHTTPBounded(t *testing.T, cfg Config, h http.Handler) string {
	t.Helper()
	raw, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	n := &Node{cfg: cfg}
	srv, lis := n.newHTTPServer(h, raw)
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(func() { _ = srv.Close() })
	return raw.Addr().String()
}

// closedWithin reads nc until the server closes it, failing if that
// takes longer than limit.
func closedWithin(t *testing.T, what string, nc net.Conn, limit time.Duration) {
	t.Helper()
	_ = nc.SetReadDeadline(time.Now().Add(limit))
	buf := make([]byte, 4096)
	for {
		_, err := nc.Read(buf)
		if err == nil {
			continue
		}
		var ne net.Error
		if errors.As(err, &ne) && ne.Timeout() {
			t.Fatalf("%s: the server still held the connection after %s", what, limit)
		}
		return
	}
}

// TestHTTPIdleKeepAliveIsClosed: a connection that sends one request and
// then nothing is closed at the idle timeout. ReadHeaderTimeout alone
// never closes it: Go's idle timeout comes only from IdleTimeout or
// ReadTimeout.
func TestHTTPIdleKeepAliveIsClosed(t *testing.T) {
	prev := httpIdleTimeout
	httpIdleTimeout = 300 * time.Millisecond
	defer func() { httpIdleTimeout = prev }()
	addr := serveHTTPBounded(t, Config{}, http.NotFoundHandler())

	nc, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()
	if _, err := io.WriteString(nc, "GET /nothing HTTP/1.1\r\nHost: x\r\n\r\n"); err != nil {
		t.Fatal(err)
	}
	resp, err := http.ReadResponse(bufio.NewReader(nc), nil)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("status %d", resp.StatusCode)
	}
	closedWithin(t, "idle keep-alive", nc, 3*time.Second)
}

// TestHTTPTrickledBodyIsCutOff: a body trickled a byte at a time, as to
// /api/login before authentication, is cut off at the authentication
// deadline rather than held for as long as the peer keeps sending.
func TestHTTPTrickledBodyIsCutOff(t *testing.T) {
	read := make(chan error, 1)
	login := http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		// As serveLogin reads its body.
		_, err := io.ReadAll(io.LimitReader(req.Body, 4096))
		read <- err
		if err != nil {
			http.Error(w, "bad body", http.StatusBadRequest)
		}
	})
	addr := serveHTTPBounded(t, Config{AuthTimeout: 500 * time.Millisecond}, login)

	nc, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()
	if _, err := io.WriteString(nc, "POST /api/login HTTP/1.1\r\nHost: x\r\nContent-Length: 4096\r\n\r\n"); err != nil {
		t.Fatal(err)
	}
	stop := make(chan struct{})
	defer close(stop)
	go func() {
		for {
			select {
			case <-stop:
				return
			case <-time.After(50 * time.Millisecond):
				if _, err := nc.Write([]byte("x")); err != nil {
					return
				}
			}
		}
	}()
	select {
	case err := <-read:
		if err == nil {
			t.Fatal("the trickled body was read to its end")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("a body trickled a byte every 50ms was still being read after 5s (deadline 500ms)")
	}
	closedWithin(t, "trickled body", nc, 3*time.Second)
}

// TestHTTPConnectionCap: past --http-max-conns a new connection is
// closed as it is accepted, and a slot freed is usable again.
func TestHTTPConnectionCap(t *testing.T) {
	addr := serveHTTPBounded(t, Config{HTTPMaxConns: 2}, http.NotFoundHandler())
	get := func(nc net.Conn) error {
		_ = nc.SetDeadline(time.Now().Add(3 * time.Second))
		if _, err := io.WriteString(nc, "GET / HTTP/1.1\r\nHost: x\r\n\r\n"); err != nil {
			return err
		}
		resp, err := http.ReadResponse(bufio.NewReader(nc), nil)
		if err != nil {
			return err
		}
		_, _ = io.Copy(io.Discard, resp.Body)
		return resp.Body.Close()
	}
	var held []net.Conn
	for i := 0; i < 2; i++ {
		nc, err := net.Dial("tcp", addr)
		if err != nil {
			t.Fatal(err)
		}
		defer nc.Close()
		if err := get(nc); err != nil {
			t.Fatalf("connection %d under the cap: %v", i+1, err)
		}
		held = append(held, nc)
	}
	over, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer over.Close()
	if err := get(over); err == nil {
		t.Fatal("a third connection was served with --http-max-conns 2")
	}
	// Free a slot; the server notices the close and a new connection is
	// served.
	_ = held[0].Close()
	deadline := time.Now().Add(3 * time.Second)
	for {
		nc, err := net.Dial("tcp", addr)
		if err != nil {
			t.Fatal(err)
		}
		err = get(nc)
		_ = nc.Close()
		if err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("no connection served after a slot was freed: %v", err)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// TestHTTPLongRequestsOutliveTheDeadlines: the bounds are on reading
// requests and on idle connections, not on handlers. A request that
// runs well past the authentication deadline — with no body, or after
// reading its body — keeps its context and gets its answer, and so does
// a pprof CPU profile.
func TestHTTPLongRequestsOutliveTheDeadlines(t *testing.T) {
	prev := httpIdleTimeout
	httpIdleTimeout = 200 * time.Millisecond
	defer func() { httpIdleTimeout = prev }()
	const deadline = 200 * time.Millisecond
	mux := http.NewServeMux()
	slow := func(w http.ResponseWriter, req *http.Request) {
		if req.Body != nil {
			if _, err := io.ReadAll(req.Body); err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
		}
		select {
		case <-time.After(4 * deadline):
			_, _ = io.WriteString(w, "done")
		case <-req.Context().Done():
			http.Error(w, "context canceled under a running request", http.StatusInternalServerError)
		}
	}
	mux.HandleFunc("/slow", slow)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	addr := serveHTTPBounded(t, Config{AuthTimeout: deadline}, mux)
	client := &http.Client{Timeout: 10 * time.Second}

	check := func(what string) func(*http.Response, error) string {
		return func(resp *http.Response, err error) string {
			t.Helper()
			if err != nil {
				t.Fatalf("%s: %v", what, err)
			}
			defer resp.Body.Close()
			body, _ := io.ReadAll(resp.Body)
			if resp.StatusCode != http.StatusOK {
				t.Fatalf("%s: status %d: %s", what, resp.StatusCode, body)
			}
			return string(body)
		}
	}
	base := "http://" + addr
	if got := check("GET")(client.Get(base + "/slow")); got != "done" {
		t.Fatalf("GET: %q", got)
	}
	if got := check("POST")(client.Post(base+"/slow", "application/json", strings.NewReader(`{"a":1}`))); got != "done" {
		t.Fatalf("POST: %q", got)
	}
	// The keep-alive connection reused after an idle pause shorter than
	// the timeout, and a fresh one after a longer pause, both serve.
	time.Sleep(3 * httpIdleTimeout)
	if got := check("GET after idle")(client.Get(base + "/slow")); got != "done" {
		t.Fatalf("GET after idle: %q", got)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, fmt.Sprintf("%s/debug/pprof/profile?seconds=1", base), nil)
	start := time.Now()
	if got := check("pprof profile")(client.Do(req)); len(got) == 0 {
		t.Fatal("pprof profile: empty")
	}
	if took := time.Since(start); took < 900*time.Millisecond {
		t.Fatalf("pprof profile returned after %s, before its 1s ran out: its context was canceled", took)
	}
}
