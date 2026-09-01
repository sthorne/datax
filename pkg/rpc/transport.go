package rpc

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/sthorne/datax/pkg/base"
	"github.com/sthorne/datax/pkg/kvpb"
	"github.com/sthorne/datax/pkg/kvserver"
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
	tlsCfg   *tls.Config // nil = cleartext

	// testingDrop, when set, vetoes outbound traffic per destination —
	// the fault-injection hook for partition tests. Never set in
	// production.
	testingDrop atomic.Pointer[func(to base.NodeID) bool]

	localMu   sync.Mutex
	localNode base.NodeID
	localAddr string

	// healthProv supplies this node's storage-health snapshot, piggybacked
	// on outgoing raft envelopes (refreshed at most every healthCacheFor).
	healthProv  atomic.Pointer[func() *rpcpb.StorageHealth]
	healthCache atomic.Pointer[rpcpb.StorageHealth]
	healthAt    atomic.Int64

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

// SetTLS installs the client TLS configuration used for all outbound
// connections (call before any dialing; nil keeps cleartext).
func (t *Transport) SetTLS(cfg *tls.Config) { t.tlsCfg = cfg }

// healthCacheFor bounds how often the health provider is consulted — raft
// traffic is per-range and hot, the snapshot is per-node and slow-moving.
const healthCacheFor = 500 * time.Millisecond

// SetHealthProvider installs the source of this node's storage-health
// snapshot, attached to every outgoing Raft envelope so peers' leaders can
// factor this node's health into their write path (nil = no piggyback).
func (t *Transport) SetHealthProvider(fn func() *rpcpb.StorageHealth) {
	if fn == nil {
		t.healthProv.Store(nil)
		return
	}
	t.healthProv.Store(&fn)
}

func (t *Transport) health() *rpcpb.StorageHealth {
	p := t.healthProv.Load()
	if p == nil {
		return nil
	}
	now := time.Now().UnixNano()
	if h := t.healthCache.Load(); h != nil && now-t.healthAt.Load() < int64(healthCacheFor) {
		return h
	}
	h := (*p)()
	if h != nil {
		t.healthCache.Store(h)
		t.healthAt.Store(now)
	}
	return h
}

// SetLocalInfo records this node's identity, piggybacked on outgoing Raft
// envelopes so peers learn our address from Raft traffic itself.
func (t *Transport) SetLocalInfo(id base.NodeID, addr string) {
	t.localMu.Lock()
	t.localNode, t.localAddr = id, addr
	t.localMu.Unlock()
}

func (t *Transport) localInfo() (base.NodeID, string) {
	t.localMu.Lock()
	defer t.localMu.Unlock()
	return t.localNode, t.localAddr
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
	cc, err := DialAddr(addr, t.tlsCfg)
	if err != nil {
		return nil, err
	}
	t.mu.conns[nodeID] = &conn{addr: addr, cc: cc}
	return cc, nil
}

// DialAddr opens a raw connection to an address (used for joins, before the
// registry knows any node IDs). tlsCfg nil dials cleartext.
func DialAddr(addr string, tlsCfg *tls.Config) (*grpc.ClientConn, error) {
	if tlsCfg != nil {
		return grpc.NewClient(addr, grpc.WithTransportCredentials(credentials.NewTLS(tlsCfg)))
	}
	return grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
}

// SetTestingDrop installs (or clears, with nil) a per-destination veto on
// all outbound traffic — the partition hook for fault-injection tests.
func (t *Transport) SetTestingDrop(fn func(to base.NodeID) bool) {
	if fn == nil {
		t.testingDrop.Store(nil)
		return
	}
	t.testingDrop.Store(&fn)
}

// dropTo reports whether the partition hook vetoes traffic to a node.
func (t *Transport) dropTo(to base.NodeID) bool {
	if fn := t.testingDrop.Load(); fn != nil {
		return (*fn)(to)
	}
	return false
}

// SendRaftMessage enqueues a Raft message for delivery. Delivery is
// best-effort (Raft tolerates loss); a full queue drops the message. An
// error is returned only when the queue worker cannot even be started —
// callers use it to report unreachability to Raft.
func (t *Transport) SendRaftMessage(ctx context.Context, to base.NodeID, rangeID base.RangeID, m raftpb.Message) error {
	if t.dropTo(to) {
		return nil // injected partition: silently dropped, like a lost packet
	}
	raw, err := m.Marshal()
	if err != nil {
		return err
	}
	localNode, localAddr := t.localInfo()
	env := &rpcpb.RaftEnvelope{
		RangeId:     int64(rangeID),
		ToReplica:   m.To,
		FromReplica: m.From,
		FromNode:    int32(localNode),
		FromAddr:    localAddr,
		Now:         t.now(),
		Message:     raw,
		Health:      t.health(),
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
					log.Debugf("transport: raft dial/stream to n%d failed: %v", to, err)
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
				log.Debugf("transport: raft stream send to n%d failed: %v", to, err)
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
	if t.dropTo(to) {
		return nil, nil, fmt.Errorf("to n%d: injected partition", to)
	}
	cc, err := t.Dial(to)
	if err != nil {
		return nil, nil, err
	}
	data, err := kvpb.MarshalBatchRequest(ba)
	if err != nil {
		return nil, nil, err
	}
	out, err := rpcpb.NewInternodeClient(cc).Batch(ctx, &rpcpb.Payload{Proto: data, Now: t.now()})
	if err != nil {
		return nil, nil, err
	}
	t.updateClock(out.Now)
	if len(out.Proto) > 0 {
		return kvpb.UnmarshalBatchEnvelope(out.Proto)
	}
	// An older server replies in JSON.
	br, kerr, err := UnmarshalBatchResult(out.Json)
	return br, kerr, err
}

// SendSnapshot streams a range snapshot to another node. next returns
// key/value chunks and an empty slice at end of stream.
func (t *Transport) SendSnapshot(ctx context.Context, to base.NodeID, header []byte, next func() ([]kvserver.SnapshotKV, error)) error {
	if t.dropTo(to) {
		return fmt.Errorf("to n%d: injected partition", to)
	}
	cc, err := t.Dial(to)
	if err != nil {
		return err
	}
	stream, err := rpcpb.NewInternodeClient(cc).Snapshot(ctx)
	if err != nil {
		return err
	}
	if err := stream.Send(&rpcpb.SnapshotChunk{HeaderJson: header, Now: t.now()}); err != nil {
		return err
	}
	for {
		kvs, err := next()
		if err != nil {
			return err
		}
		if len(kvs) == 0 {
			break
		}
		chunk := &rpcpb.SnapshotChunk{Now: t.now()}
		for _, kv := range kvs {
			chunk.Kvs = append(chunk.Kvs, &rpcpb.SnapshotKV{Key: kv.Key, Value: kv.Value})
		}
		if err := stream.Send(chunk); err != nil {
			return err
		}
	}
	_, err = stream.CloseAndRecv()
	return err
}

// Call performs a unary JSON RPC (join/admin) against an address.
func (t *Transport) Call(ctx context.Context, addr string, method string, req, resp any) error {
	cc, err := DialAddr(addr, t.tlsCfg)
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
