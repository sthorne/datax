package rpc

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/sthorne/datax/pkg/base"
	"github.com/sthorne/datax/pkg/kvserver"
	"github.com/sthorne/datax/pkg/util/hlc"
	"github.com/sthorne/datax/pkg/util/stop"
)

// TestSendSnapshotReportsTheReceiversRefusal: a receiver that refuses a
// snapshot at its header ends the stream while the sender still has
// chunks to send. gRPC then fails the sender's Send with a bare io.EOF,
// and the receiver's reason is only read from the stream afterwards.
// The sender must report that reason — a refusal is acted on by its
// message (issue #200's leftover replacement) — whichever side is
// faster, so the receiver here refuses as soon as it has the header,
// without reading a chunk, and the sender has many to send.
func TestSendSnapshotReportsTheReceiversRefusal(t *testing.T) {
	const refusal = "r7: snapshot refused: overlapping live replica r9"
	clock := hlc.NewClock(nil, 0)
	handlers := ServerHandlers{
		Snapshot: func(header []byte, _ func() ([]kvserver.SnapshotKV, error)) error {
			return errors.New(refusal)
		},
	}
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	gs := NewServer(clock, handlers, nil)
	go func() { _ = gs.Serve(lis) }()
	defer gs.Stop()

	stopper := stop.NewStopper()
	defer stopper.Stop()
	tr := NewTransport(clock, stopper, func(base.NodeID) (string, error) { return lis.Addr().String(), nil })
	defer tr.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	for attempt := 0; attempt < 20; attempt++ {
		sent := 0
		next := func() ([]kvserver.SnapshotKV, error) {
			if sent == 200 {
				return nil, nil
			}
			sent++
			return []kvserver.SnapshotKV{{Key: []byte(fmt.Sprintf("k%03d", sent)), Value: make([]byte, 4096)}}, nil
		}
		err := tr.SendSnapshot(ctx, 2, []byte(`{}`), next)
		if err == nil {
			t.Fatalf("attempt %d: a refused snapshot was reported sent", attempt)
		}
		if !strings.Contains(err.Error(), refusal) {
			t.Fatalf("attempt %d (after %d chunks): got %q, want the receiver's refusal %q", attempt, sent, err, refusal)
		}
	}
}
