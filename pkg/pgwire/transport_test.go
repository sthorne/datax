package pgwire

import (
	"net"
	"testing"
)

// TestIsLoopbackAddr (issue #310): what counts as a connection from this
// machine, for the key functions' transport rule.
func TestIsLoopbackAddr(t *testing.T) {
	for _, c := range []struct {
		addr net.Addr
		want bool
	}{
		{&net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 5}, true},
		{&net.TCPAddr{IP: net.ParseIP("127.8.0.2"), Port: 5}, true},
		{&net.TCPAddr{IP: net.ParseIP("::1"), Port: 5}, true},
		{&net.TCPAddr{IP: net.ParseIP("10.0.0.9"), Port: 5}, false},
		{&net.TCPAddr{IP: net.ParseIP("::ffff:192.168.1.1"), Port: 5}, false},
		{&net.UnixAddr{Name: "/tmp/.s.PGSQL.26257", Net: "unix"}, true},
		{nil, false},
	} {
		if got := isLoopbackAddr(c.addr); got != c.want {
			t.Errorf("%v: %v, want %v", c.addr, got, c.want)
		}
	}
}
