package rpc

import (
	"context"
	"net"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/sthorne/datax/pkg/base"
	"github.com/sthorne/datax/pkg/kvpb"
	"github.com/sthorne/datax/pkg/util/hlc"
	"github.com/sthorne/datax/pkg/util/stop"
)

// replyClockChildEnv selects the in-child half of
// TestReplyFromFutureClockDoesNotKillCaller: a regression calls
// log.Fatalf, which exits the process, so the scenario runs in a child
// and the parent asserts that it survived.
const replyClockChildEnv = "DATAX_TEST_REPLY_CLOCK"

// TestReplyFromFutureClockDoesNotKillCaller: a reply whose timestamp is
// past the caller's --max-offset used to reach log.Fatalf in the
// Transport, so any server a node or an operator command called — a peer
// whose clock had run away, or whatever listened at the address a
// `datax debug` command or a joining node dialed — could stop the caller
// with one answer. The call must fail instead, the caller's clock must
// not move, and a Ping must still measure the offset.
func TestReplyFromFutureClockDoesNotKillCaller(t *testing.T) {
	cmd := exec.Command(os.Args[0], "-test.run=^TestReplyClockChild$", "-test.v", "-test.count=1")
	cmd.Env = append(os.Environ(), replyClockChildEnv+"=1")
	out, err := cmd.CombinedOutput()
	if err != nil || !strings.Contains(string(out), "--- PASS: TestReplyClockChild") {
		t.Fatalf("calling process did not survive (err=%v):\n%s", err, out)
	}
}

func TestReplyClockChild(t *testing.T) {
	if os.Getenv(replyClockChildEnv) == "" {
		t.Skip("run by TestReplyFromFutureClockDoesNotKillCaller")
	}
	const ahead = time.Hour
	serverClock := hlc.NewClock(func() int64 { return time.Now().Add(ahead).UnixNano() }, 500*time.Millisecond)
	payload := func(ctx context.Context, in []byte) ([]byte, error) { return []byte(`{}`), nil }
	gs := NewServer(serverClock, ServerHandlers{
		Admin: payload,
		Join:  payload,
		Batch: func(ctx context.Context, ba *kvpb.BatchRequest) (*kvpb.BatchResponse, *kvpb.Error) {
			return &kvpb.BatchResponse{}, nil
		},
	}, nil)
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = gs.Serve(lis) }()
	defer gs.Stop()

	clock := hlc.NewClock(nil, 500*time.Millisecond)
	stopper := stop.NewStopper()
	defer stopper.Stop()
	tr := NewTransport(clock, stopper, func(base.NodeID) (string, error) { return lis.Addr().String(), nil })
	defer tr.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	unmoved := func(what string) {
		t.Helper()
		if got := clock.Now().WallTime; got > time.Now().Add(ahead/2).UnixNano() {
			t.Fatalf("%s: a rejected reply moved the clock to %d", what, got)
		}
	}
	wantViolation := func(what string, err error) {
		t.Helper()
		if err == nil || !strings.Contains(err.Error(), "clock synchronization violated") {
			t.Fatalf("%s: got %v, want a clock synchronization error", what, err)
		}
		unmoved(what)
	}
	var resp struct{}
	wantViolation("Call admin", tr.Call(ctx, lis.Addr().String(), "admin", struct{}{}, &resp))
	wantViolation("Call join", tr.Call(ctx, lis.Addr().String(), "join", struct{}{}, &resp))
	wantViolation("Admin", tr.Admin(ctx, 2, struct{}{}, &resp))
	_, _, err = tr.SendBatch(ctx, 2, &kvpb.BatchRequest{})
	wantViolation("SendBatch", err)

	// The probe still answers, and reports the peer's offset.
	_, offset, err := tr.Ping(ctx, 2)
	if err != nil {
		t.Fatalf("Ping: %v", err)
	}
	if offset < ahead/2 {
		t.Fatalf("Ping measured an offset of %s, want about %s", offset, ahead)
	}
	unmoved("Ping")
}
