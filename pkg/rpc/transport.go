package rpc

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"net"
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
	// The cached snapshot and its timestamp travel as ONE pointer so a
	// racing reader can never pair a fresh snapshot with a stale stamp or
	// vice versa.
	healthProv  atomic.Pointer[func() *rpcpb.StorageHealth]
	healthCache atomic.Pointer[cachedHealth]

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

// cachedHealth is one health snapshot with the time it was taken.
type cachedHealth struct {
	health *rpcpb.StorageHealth
	at     int64 // unix nanos
}

func (t *Transport) health() *rpcpb.StorageHealth {
	p := t.healthProv.Load()
	if p == nil {
		return nil
	}
	now := time.Now().UnixNano()
	if c := t.healthCache.Load(); c != nil && now-c.at < int64(healthCacheFor) {
		return c.health
	}
	h := (*p)()
	if h != nil {
		t.healthCache.Store(&cachedHealth{health: h, at: now})
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
	sizes := grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(MaxMessageBytes), grpc.MaxCallSendMsgSize(MaxMessageBytes))
	if tlsCfg != nil {
		return grpc.NewClient(addr, grpc.WithTransportCredentials(credentials.NewTLS(tlsCfg)), sizes)
	}
	return grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()), sizes)
}

// Probe establishes and immediately closes a connection to addr — the TCP
// dial and, with TLS configured, the full handshake against the node's
// certificate — under ctx. It exists so a CLI client can separate "the
// node is unreachable" (a dial or verification error, with its cause)
// from "the operation is slow": gRPC dials lazily on the first call and
// reports only the deadline when that dial never completes.
func (t *Transport) Probe(ctx context.Context, addr string) error {
	var d net.Dialer
	conn, err := d.DialContext(ctx, "tcp", addr)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()
	if t.tlsCfg == nil {
		return nil
	}
	cfg := t.tlsCfg.Clone()
	if cfg.ServerName == "" {
		if host, _, err := net.SplitHostPort(addr); err == nil {
			cfg.ServerName = host
		}
	}
	// The node's gRPC listener requires ALPN; offer h2 so the handshake
	// matches what the real connection will do.
	cfg.NextProtos = []string{"h2"}
	tc := tls.Client(conn, cfg)
	if err := tc.HandshakeContext(ctx); err != nil {
		return err
	}
	return nil
}

// Ping measures the round trip to a peer and the peer's clock offset
// relative to this node (positive: the peer runs ahead), by the NTP
// exchange: rtt = (t4 - t1) - (t3 - t2), offset = ((t2 - t1) + (t3 - t4)) / 2
// with t1/t4 this node's physical clock at send and receive and t2/t3
// the peer's at receipt and reply. Honors the testing partition hook, so
// a partitioned peer reads as unreachable.
func (t *Transport) Ping(ctx context.Context, to base.NodeID) (rtt, offset time.Duration, err error) {
	if t.dropTo(to) {
		return 0, 0, fmt.Errorf("ping to n%d dropped (partitioned)", to)
	}
	cc, err := t.Dial(to)
	if err != nil {
		return 0, 0, err
	}
	self, _ := t.localInfo()
	t1 := t.clock.PhysicalNow()
	resp, err := rpcpb.NewInternodeClient(cc).Ping(ctx, &rpcpb.PingRequest{FromNode: int64(self), SendWall: t1, Now: t.now()})
	t4 := t.clock.PhysicalNow()
	if err != nil {
		return 0, 0, err
	}
	t.updateClock(resp.Now)
	rtt = time.Duration((t4 - t1) - (resp.SendWall - resp.RecvWall))
	if rtt < 0 {
		rtt = 0
	}
	offset = time.Duration(((resp.RecvWall - t1) + (resp.SendWall - t4)) / 2)
	return rtt, offset, nil
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
	env := t.envelope()
	env.RangeId, env.ToReplica, env.FromReplica, env.Message = int64(rangeID), m.To, m.From, raw
	return t.enqueueRaft(to, env)
}

// SendRaftHeartbeats sends one envelope carrying a pass's heartbeats and
// responses for a peer node (cluster v12; see kvserver/quiesce.go).
func (t *Transport) SendRaftHeartbeats(ctx context.Context, to base.NodeID, beats, resps, closed []kvserver.RaftHeartbeat) error {
	if t.dropTo(to) {
		return nil
	}
	env := t.envelope()
	conv := func(in []kvserver.RaftHeartbeat) []*rpcpb.RaftHeartbeat {
		out := make([]*rpcpb.RaftHeartbeat, len(in))
		for i, hb := range in {
			out[i] = &rpcpb.RaftHeartbeat{
				RangeId: int64(hb.RangeID), ToReplica: hb.To, FromReplica: hb.From, Term: hb.Term, Commit: hb.Commit, Quiesce: hb.Quiesce,
				Index: hb.Index, ClosedWall: hb.ClosedTS.WallTime, ClosedLogical: hb.ClosedTS.Logical,
			}
		}
		return out
	}
	env.Heartbeats, env.HeartbeatResponses, env.ClosedTimestamps = conv(beats), conv(resps), conv(closed)
	return t.enqueueRaft(to, env)
}

// envelope starts a raft envelope with this node's identity, clock and
// health.
func (t *Transport) envelope() *rpcpb.RaftEnvelope {
	localNode, localAddr := t.localInfo()
	return &rpcpb.RaftEnvelope{
		FromNode: int32(localNode),
		FromAddr: localAddr,
		Now:      t.now(),
		Health:   t.health(),
	}
}

// enqueueRaft hands an envelope to the destination's stream worker
// (started on first use); a full queue drops it.
func (t *Transport) enqueueRaft(to base.NodeID, env *rpcpb.RaftEnvelope) error {
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
