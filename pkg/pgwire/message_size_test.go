package pgwire

import (
	"encoding/binary"
	"net"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgproto3"
)

// The client message size caps: a small one until the client has
// authenticated, a generous one after. pgproto3 allocates a message's
// declared length before reading its body, so the declared length is
// all an attacker has to send.

// expectFatalClose reads the server's answer to an oversized message:
// a FATAL 08P01, then the close.
func expectFatalClose(t *testing.T, nc net.Conn, fe *pgproto3.Frontend) {
	t.Helper()
	_ = nc.SetReadDeadline(time.Now().Add(5 * time.Second))
	msg, err := fe.Receive()
	if err != nil {
		t.Fatalf("no answer to an oversized message: %v", err)
	}
	e, ok := msg.(*pgproto3.ErrorResponse)
	if !ok || e.Severity != "FATAL" || e.Code != "08P01" {
		t.Fatalf("got %#v, want a FATAL 08P01", msg)
	}
	expectClosed(t, nc, 5*time.Second)
}

// header is a message header declaring a body of n bytes.
func header(typ byte, n int) []byte {
	return binary.BigEndian.AppendUint32([]byte{typ}, uint32(n+4))
}

// TestPreAuthMessageSizeCap: before authentication a client message is
// capped at a few KiB. A client that had only sent its StartupMessage
// could declare a ~2 GiB SASL response in a 5-byte header and the
// server allocated it all, per connection, before reading a byte of it.
func TestPreAuthMessageSizeCap(t *testing.T) {
	auth := newVerifierFor(t, "alice", "hunter2")
	opts := scramOpts(auth)
	opts.AuthTimeout = 30 * time.Second
	s := startServer(t, opts)

	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	nc, err := net.Dial("tcp", s.Addr())
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()
	fe, msg := startSCRAM(t, nc, "nobody")
	if _, ok := msg.(*pgproto3.AuthenticationSASL); !ok {
		t.Fatalf("expected the SASL advertisement, got %T", msg)
	}
	if _, err := nc.Write(header('p', 0x7ffffff0-4)); err != nil {
		t.Fatal(err)
	}
	expectFatalClose(t, nc, fe)
	runtime.ReadMemStats(&after)
	if grew := int64(after.TotalAlloc) - int64(before.TotalAlloc); grew > 256<<20 {
		t.Fatalf("one 5-byte header cost the server %d MiB", grew>>20)
	}
	waitFor(t, 5*time.Second, "the connection to be released", func() bool { return s.connCount() == 0 && s.pending.Load() == 0 })

	// One just over the cap is refused the same way; the cap leaves the
	// real exchange alone (scramLogin's messages are well under it).
	nc2, err := net.Dial("tcp", s.Addr())
	if err != nil {
		t.Fatal(err)
	}
	defer nc2.Close()
	fe2, _ := startSCRAM(t, nc2, "nobody")
	if _, err := nc2.Write(header('p', maxPreAuthMessageSize+1)); err != nil {
		t.Fatal(err)
	}
	expectFatalClose(t, nc2, fe2)
	nc3, err := net.Dial("tcp", s.Addr())
	if err != nil {
		t.Fatal(err)
	}
	defer nc3.Close()
	if !scramLogin(t, nc3, "alice", "hunter2") {
		t.Fatal("a genuine login failed under the pre-authentication cap")
	}
}

// TestMessageSizeCapAfterAuth: once authenticated, the cap is
// MaxMessageSize: a message far over the pre-authentication cap is
// served, and one over MaxMessageSize ends the connection with a FATAL
// 08P01 instead of being allocated.
func TestMessageSizeCapAfterAuth(t *testing.T) {
	s := startServer(t, ServerOptions{MaxMessageSize: 1 << 20})
	nc, fe := trustConn(t, s)
	big := "SELECT '" + strings.Repeat("x", 512<<10) + "'"
	got := exchange(t, fe, &pgproto3.Parse{Query: big})
	if len(got) != 1 {
		t.Fatalf("a 512 KiB Parse answered %v", got)
	}
	if _, ok := got[0].(*pgproto3.ParseComplete); !ok {
		t.Fatalf("a 512 KiB Parse answered %#v", got[0])
	}
	if _, err := nc.Write(header('Q', 1<<20+1)); err != nil {
		t.Fatal(err)
	}
	expectFatalClose(t, nc, fe)

	// The default cap is DefaultMaxMessageSize.
	s2 := startServer(t, ServerOptions{})
	nc2, fe2 := trustConn(t, s2)
	if _, err := nc2.Write(header('Q', DefaultMaxMessageSize+1)); err != nil {
		t.Fatal(err)
	}
	expectFatalClose(t, nc2, fe2)
}

// TestMessageSizeCapAfterSCRAM: a SCRAM login lifts the
// pre-authentication cap too (the backend is the same object the
// exchange read with).
func TestMessageSizeCapAfterSCRAM(t *testing.T) {
	auth := newVerifierFor(t, "alice", "hunter2")
	s := startServer(t, scramOpts(auth))
	nc, err := net.Dial("tcp", s.Addr())
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()
	_ = nc.SetDeadline(time.Now().Add(10 * time.Second))
	fe, ok := scramLoginFE(t, nc, "alice", "hunter2")
	if !ok {
		t.Fatal("login failed")
	}
	for {
		msg, err := fe.Receive()
		if err != nil {
			t.Fatal(err)
		}
		if _, ok := msg.(*pgproto3.ReadyForQuery); ok {
			break
		}
	}
	got := exchange(t, fe, &pgproto3.Parse{Query: "SELECT '" + strings.Repeat("x", 64<<10) + "'"})
	if len(got) != 1 {
		t.Fatalf("a 64 KiB Parse after SCRAM answered %v", got)
	}
	if _, ok := got[0].(*pgproto3.ParseComplete); !ok {
		t.Fatalf("a 64 KiB Parse after SCRAM answered %#v", got[0])
	}
}
