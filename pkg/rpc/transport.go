package rpc

import (
	"context"
	"encoding/json"
	"sync"
	"time"

	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/sthorne/datax/pkg/base"
	"github.com/sthorne/datax/pkg/kvpb"
	"github.com/sthorne/datax/pkg/rpc/rpcpb"
	"github.com/sthorne/datax/pkg/util/hlc"
	"github.com/sthorne/datax/pkg/util/log"
	"github.com/sthorne/datax/pkg/util/stop"
)

// Resolver maps a node ID to its RPC address.
type Resolver func(base.NodeID) (string, error)

// Transport is the client side of internode RPC: cached connections, one
// buffered Raft stream per destination node, and unary helpers for
// batches/joins/admin.
type Transport struct {
	clock    *hlc.Clock
	stopper  *stop.Stopper
	resolver Resolver

	mu struct {
		sync.Mutex
		conns map[base.NodeID]*conn
		raftQ map[base.NodeID]chan *rpcpb.RaftEnvelope
	}
}

type conn struct {
	addr string
	cc   *grpc.ClientConn
}

func NewTransport(clock *hlc.Clock, stopper *stop.Stopper, resolver Resolver) *Transport {
	t := &Transport{clock: clock, stopper: stopper, resolver: resolver}
	t.mu.conns = make(map[base.NodeID]*conn)
	t.mu.raftQ = make(map[base.NodeID]chan *rpcpb.RaftEnvelope)
	return t
}

func (t *Transport) now() *rpcpb.Hlc {
	ts := t.clock.Now()
	return &rpcpb.Hlc{WallTime: ts.WallTime, Logical: ts.Logical}
}

// Dial returns a (cached) connection to the node, redialing if the node's
// address changed.
func (t *Transport) Dial(nodeID base.NodeID) (*grpc.ClientConn, error) {
	addr, err := t.resolver(nodeID)
	if err != nil {
		return nil, err
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if c, ok := t.mu.conns[nodeID]; ok {
		if c.addr == addr {
			return c.cc, nil
		}
		_ = c.cc.Close()
		delete(t.mu.conns, nodeID)
	}
	cc, err := DialAddr(addr)
	if err != nil {
		return nil, err
	}
	t.mu.conns[nodeID] = &conn{addr: addr, cc: cc}
	return cc, nil
}

// DialAddr opens a raw connection to an address (used for joins, before the
// registry knows any node IDs).
func DialAddr(addr string) (*grpc.ClientConn, error) {
	return grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
}

// SendRaftMessage enqueues a Raft message for delivery. Delivery is
// best-effort (Raft tolerates loss); a full queue drops the message. An
// error is returned only when the queue worker cannot even be started —
// callers use it to report unreachability to Raft.
func (t *Transport) SendRaftMessage(ctx context.Context, to base.NodeID, rangeID base.RangeID, m raftpb.Message) error {
	raw, err := m.Marshal()
	if err != nil {
		return err
	}
	env := &rpcpb.RaftEnvelope{
		RangeId:     int64(rangeID),
		ToReplica:   m.To,
		FromReplica: m.From,
		Now:         t.now(),
		Message:     raw,
	}
	t.mu.Lock()
	q, ok := t.mu.raftQ[to]
	if !ok {
		q = make(chan *rpcpb.RaftEnvelope, 1024)
		t.mu.raftQ[to] = q
		if err := t.stopper.RunWorker(func(ctx context.Context) { t.raftWorker(ctx, to, q) }); err != nil {
			delete(t.mu.raftQ, to)
			t.mu.Unlock()
			return err
		}
	}
	t.mu.Unlock()

	select {
	case q <- env:
	default:
		log.Debugf("raft send queue to n%d full; dropping message", to)
	}
	return nil
}

// raftWorker owns the stream to one destination node, reconnecting with
// backoff on failure.
func (t *Transport) raftWorker(ctx context.Context, to base.NodeID, q <-chan *rpcpb.RaftEnvelope) {
	var stream rpcpb.Internode_RaftMessagesClient
	for {
		var env *rpcpb.RaftEnvelope
		select {
		case <-ctx.Done():
			return
		case env = <-q:
		}
		for attempt := 0; ; attempt++ {
			if stream == nil {
				cc, err := t.Dial(to)
				if err == nil {
					stream, err = rpcpb.NewInternodeClient(cc).RaftMessages(ctx)
				}
				if err != nil {
					stream = nil
					if attempt >= 1 {
						break // drop this message; raft will retry
					}
					select {
					case <-ctx.Done():
						return
					case <-time.After(100 * time.Millisecond):
					}
					continue
				}
			}
			if err := stream.Send(env); err != nil {
				stream = nil
				continue // redial once, then drop
			}
			break
		}
	}
}

// SendBatch executes a KV batch on a remote node. The outer error covers
// transport failure; the kvpb.Error is the KV-level outcome.
func (t *Transport) SendBatch(ctx context.Context, to base.NodeID, ba *kvpb.BatchRequest) (*kvpb.BatchResponse, *kvpb.Error, error) {
	cc, err := t.Dial(to)
	if err != nil {
		return nil, nil, err
	}
	data, err := json.Marshal(ba)
	if err != nil {
		return nil, nil, err
	}
	out, err := rpcpb.NewInternodeClient(cc).Batch(ctx, &rpcpb.Payload{Json: data, Now: t.now()})
	if err != nil {
		return nil, nil, err
	}
	t.updateClock(out.Now)
	br, kerr, err := UnmarshalBatchResult(out.Json)
	return br, kerr, err
}

// Call performs a unary JSON RPC (join/admin) against an address.
func (t *Transport) Call(ctx context.Context, addr string, method string, req, resp any) error {
	cc, err := DialAddr(addr)
	if err != nil {
		return err
	}
	defer func() { _ = cc.Close() }()
	data, err := json.Marshal(req)
	if err != nil {
		return err
	}
	client := rpcpb.NewInternodeClient(cc)
	var out *rpcpb.Payload
	in := &rpcpb.Payload{Json: data, Now: t.now()}
	switch method {
	case "join":
		out, err = client.Join(ctx, in)
	case "admin":
		out, err = client.Admin(ctx, in)
	default:
		return kvpb.NewErrorf("unknown method %q", method)
	}
	if err != nil {
		return err
	}
	t.updateClock(out.Now)
	return json.Unmarshal(out.Json, resp)
}

func (t *Transport) updateClock(now *rpcpb.Hlc) {
	if now == nil {
		return
	}
	if err := t.clock.Update(hlc.Timestamp{WallTime: now.WallTime, Logical: now.Logical}); err != nil {
		log.Fatalf("clock synchronization violated: %v", err)
	}
}

// Close closes all cached connections.
func (t *Transport) Close() {
	t.mu.Lock()
	defer t.mu.Unlock()
	for id, c := range t.mu.conns {
		_ = c.cc.Close()
		delete(t.mu.conns, id)
	}
}
