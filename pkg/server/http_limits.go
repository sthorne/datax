package server

import (
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sthorne/datax/pkg/metrics"
	"github.com/sthorne/datax/pkg/util/log"
)

// The HTTP port's connection-level bounds. ReadHeaderTimeout alone bounds
// only a request's headers: Go closes an idle keep-alive connection only
// at IdleTimeout (or ReadTimeout), so without these a peer could hold a
// connection forever by sending one request and then nothing, or by
// trickling a body to /api/login, which runs before authentication, and
// nothing capped how many connections it could hold that way.
const (
	// DefaultHTTPMaxConns caps the HTTP port's open connections, the
	// same figure as the SQL port's pre-authentication cap: a browser
	// holds a handful, a Prometheus scraper one.
	DefaultHTTPMaxConns = 512
	// httpMaxHeaderBytes caps a request's headers. Basic credentials and
	// the console's session cookie are well under a kilobyte; Go's own
	// default is 1 MiB.
	httpMaxHeaderBytes = 64 << 10
)

// httpIdleTimeout closes a keep-alive connection that has sent no new
// request for this long. A variable so tests can shorten it.
var httpIdleTimeout = 2 * time.Minute

// httpMaxConns is the cap in force (0 = no cap).
func (n *Node) httpMaxConns() int {
	switch {
	case n.cfg.HTTPMaxConns < 0:
		return 0
	case n.cfg.HTTPMaxConns == 0:
		return DefaultHTTPMaxConns
	}
	return n.cfg.HTTPMaxConns
}

// newHTTPServer wraps h and lis in the HTTP port's bounds:
//   - the authentication deadline (--auth-timeout) bounds reading a
//     request: its headers (ReadHeaderTimeout) and its headers and body
//     together (ReadTimeout). Every body this server reads is a few
//     kilobytes (a sign-in, viewer preferences), so a peer still sending
//     one at the deadline is trickling it;
//   - an idle keep-alive connection is closed at httpIdleTimeout;
//   - headers are capped at httpMaxHeaderBytes;
//   - at most httpMaxConns connections are open at once; past it a new
//     one is closed as it is accepted (datax_http_conns_refused_total).
//
// ReadTimeout bounds only the reading: net/http clears the connection's
// read deadline once a request's body has been read (when it starts
// watching for the client going away), so a handler that runs longer —
// the console's long polls, a pprof profile — keeps its context. There
// is no WriteTimeout, which would cut long responses (a pprof profile or
// trace, a large /metrics).
func (n *Node) newHTTPServer(h http.Handler, lis net.Listener) (*http.Server, net.Listener) {
	auth := n.authTimeout()
	srv := &http.Server{
		Handler:           h,
		ReadHeaderTimeout: auth,
		ReadTimeout:       auth,
		IdleTimeout:       httpIdleTimeout,
		MaxHeaderBytes:    httpMaxHeaderBytes,
	}
	if limit := n.httpMaxConns(); limit > 0 {
		lis = &limitListener{Listener: lis, max: int64(limit)}
	}
	return srv, lis
}

// limitListener caps the connections open through it. Past the cap a
// new connection is closed as it is accepted rather than left in the
// kernel's backlog, so a client learns at once, as the SQL port's
// pending-auth cap refuses rather than holds.
type limitListener struct {
	net.Listener
	max  int64
	open atomic.Int64
}

func (l *limitListener) Accept() (net.Conn, error) {
	for {
		c, err := l.Listener.Accept()
		if err != nil {
			return nil, err
		}
		if l.open.Add(1) > l.max {
			l.open.Add(-1)
			_ = c.Close()
			metrics.HTTPConnsRefused.Inc()
			log.Debugf("http: refused a connection from %s: %d connections already open (--http-max-conns)", c.RemoteAddr(), l.max)
			continue
		}
		return &limitedConn{Conn: c, l: l}, nil
	}
}

// limitedConn gives its slot back when closed, once.
type limitedConn struct {
	net.Conn
	l    *limitListener
	once sync.Once
}

func (c *limitedConn) Close() error {
	err := c.Conn.Close()
	c.once.Do(func() { c.l.open.Add(-1) })
	return err
}
