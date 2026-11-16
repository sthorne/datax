// Package rpc implements the internode transport: a gRPC service carrying
// Raft messages (streamed), KV batches, join and admin calls. Every message
// carries an HLC reading; receivers ratchet their clocks, and a clock beyond
// the configured max offset is fatal (see docs/transactions.md).
package rpc

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"io"
	"net"

	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"

	"github.com/sthorne/datax/pkg/base"
	"github.com/sthorne/datax/pkg/kvpb"
	"github.com/sthorne/datax/pkg/kvserver"
	"github.com/sthorne/datax/pkg/rpc/rpcpb"
	"github.com/sthorne/datax/pkg/util/hlc"
	"github.com/sthorne/datax/pkg/util/log"
	"github.com/sthorne/datax/pkg/util/stop"
)

// PayloadHandler serves a JSON request payload and returns a JSON response.
type PayloadHandler func(ctx context.Context, data []byte) ([]byte, error)

// SnapshotHandler consumes an incoming range snapshot stream.
type SnapshotHandler func(header []byte, kvs func() ([]kvserver.SnapshotKV, error)) error

// ServerHandlers are the node-side callbacks the transport dispatches into.
type ServerHandlers struct {
	// Batch executes a KV batch. Wire encoding (proto on the hot path,
	// JSON from older senders) is the rpc layer's concern, not the node's.
	Batch func(ctx context.Context, ba *kvpb.BatchRequest) (*kvpb.BatchResponse, *kvpb.Error)
	Join  PayloadHandler
	Admin PayloadHandler
	Raft  func(ctx context.Context, rangeID base.RangeID, m raftpb.Message)
	// RaftHeartbeats receives a peer's coalesced heartbeats and responses
	// (cluster v12; see kvserver/quiesce.go).
	RaftHeartbeats func(ctx context.Context, from base.NodeID, beats, resps, closed []kvserver.RaftHeartbeat)
	Snapshot       SnapshotHandler
	// NodeInfo learns a peer's address from its Raft envelopes.
	NodeInfo func(id base.NodeID, addr string)
	// NodeHealth learns a peer's storage-health snapshot from its Raft
	// envelopes (see rpcpb.StorageHealth).
	NodeHealth func(id base.NodeID, h *rpcpb.StorageHealth)
	// Stopper, when set, is the node's: every dispatch into a handler
	// runs as one of its tasks (a unary call whole, a stream message by
	// message, a snapshot stream whole), with a context canceled when
	// the node begins to stop. Once it has, a call is answered
	// Unavailable without reaching a handler, and the node's closers —
	// the storage engine's among them — wait for the handlers already
	// running (issue #324).
	Stopper *stop.Stopper
}

// Server implements rpcpb.InternodeServer.
type Server struct {
	rpcpb.UnimplementedInternodeServer
	clock    *hlc.Clock
	handlers ServerHandlers
	// secure records whether the listener requires client certificates;
	// the node-only surfaces fail closed on it (see requireNode).
	secure bool
	// sealed marks the server a node runs before its store is open (see
	// NewSealedServer): only Admin is served.
	sealed bool
}

// stoppingMessage answers a call that arrives once the node has begun to
// stop.
const stoppingMessage = "this node is shutting down"

// errStopping is what a call that arrives once the node has begun to
// stop is answered: Unavailable, as from a node that is down.
func errStopping(method string) error {
	return status.Errorf(codes.Unavailable, "%s: %s", method, stoppingMessage)
}

// runTask runs f as a task of the node's stopper (see
// ServerHandlers.Stopper), or directly when there is none.
func (s *Server) runTask(ctx context.Context, method string, f func(ctx context.Context)) error {
	if s.handlers.Stopper == nil {
		f(ctx)
		return nil
	}
	if err := s.handlers.Stopper.RunTask(ctx, method, f); err != nil {
		return errStopping(method)
	}
	return nil
}

// taskInterceptor runs every unary call as a task of st: a method added
// later is covered without anyone remembering to.
func taskInterceptor(st *stop.Stopper) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp any, err error) {
		if terr := st.RunTask(ctx, info.FullMethod, func(ctx context.Context) {
			resp, err = handler(ctx, req)
		}); terr != nil {
			return nil, errStopping(info.FullMethod)
		}
		return resp, err
	}
}

// MaxMessageBytes bounds one internode message in either direction:
// gRPC's default (4 MiB) is smaller than a page of scan rows or a
// snapshot chunk. Scan responses are paged well below it
// (kvserver.scanTargetBytes), so a message that reaches it is a bug.
const MaxMessageBytes = 64 << 20

// NewServer returns a gRPC server with the Internode service registered.
// A non-nil tlsCfg enables mutual TLS (the config must require and verify
// client certificates); nil serves cleartext.
func NewServer(clock *hlc.Clock, handlers ServerHandlers, tlsCfg *tls.Config) *grpc.Server {
	opts := []grpc.ServerOption{grpc.MaxRecvMsgSize(MaxMessageBytes)}
	if tlsCfg != nil {
		opts = append(opts, grpc.Creds(credentials.NewTLS(tlsCfg)))
	}
	if handlers.Stopper != nil {
		opts = append(opts, grpc.UnaryInterceptor(taskInterceptor(handlers.Stopper)))
	}
	gs := grpc.NewServer(opts...)
	rpcpb.RegisterInternodeServer(gs, &Server{clock: clock, handlers: handlers, secure: tlsCfg != nil})
	return gs
}

// SealedMessage is what a sealed node answers to everything but the
// admin calls that unseal it.
const SealedMessage = "this node is sealed: its store is closed until enough shares of the root key have been submitted (datax unseal)"

// NewSealedServer returns the server a node runs while it is sealed: its
// store is not open, so there is nothing to serve but the admin calls
// that unseal it. Every other method answers Unavailable, which is what
// a peer sees of a node that is down — and for the cluster's purposes a
// sealed node is down.
func NewSealedServer(clock *hlc.Clock, admin PayloadHandler, tlsCfg *tls.Config) *grpc.Server {
	var opts []grpc.ServerOption
	if tlsCfg != nil {
		opts = append(opts, grpc.Creds(credentials.NewTLS(tlsCfg)))
	}
	gs := grpc.NewServer(opts...)
	rpcpb.RegisterInternodeServer(gs, &Server{clock: clock, handlers: ServerHandlers{Admin: admin}, secure: tlsCfg != nil, sealed: true})
	return gs
}

// PeerIsLoopback reports whether the caller connected from this machine.
func PeerIsLoopback(ctx context.Context) bool {
	p, ok := peer.FromContext(ctx)
	if !ok || p.Addr == nil {
		return false
	}
	host, _, err := net.SplitHostPort(p.Addr.String())
	if err != nil {
		return false
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

// PeerHost returns the IP address the caller connected from, or "".
func PeerHost(ctx context.Context) string {
	p, ok := peer.FromContext(ctx)
	if !ok || p.Addr == nil {
		return ""
	}
	host, _, err := net.SplitHostPort(p.Addr.String())
	if err != nil {
		return ""
	}
	return host
}

// PeerCN returns the CommonName of the caller's CA-verified client
// certificate, or "" on a cleartext (insecure-mode) connection. The
// internode server requires and verifies client certificates, so in
// secure mode every call carries exactly one verified identity: "node"
// for cluster peers, or a SQL username for a client certificate issued
// by `datax cert create-client`.
func PeerCN(ctx context.Context) string {
	p, ok := peer.FromContext(ctx)
	if !ok || p.AuthInfo == nil {
		return ""
	}
	ti, ok := p.AuthInfo.(credentials.TLSInfo)
	if !ok {
		return ""
	}
	if len(ti.State.VerifiedChains) == 0 || len(ti.State.VerifiedChains[0]) == 0 {
		return ""
	}
	return ti.State.VerifiedChains[0][0].Subject.CommonName
}

// requireNode admits a call only from a cluster peer: in secure mode the
// caller must present the node certificate (CN "node"). Client
// certificates issued to SQL users authenticate operators to the Admin
// RPC, which authorizes per op; they carry no authority over the raw KV,
// Raft, snapshot, and join surfaces, where a Batch over any key span
// would bypass every SQL privilege check. Insecure mode has no
// identities to check, matching pgwire's trust semantics.
func (s *Server) requireNode(ctx context.Context, method string) error {
	if s.sealed {
		return status.Errorf(codes.Unavailable, "%s: %s", method, SealedMessage)
	}
	if !s.secure {
		return nil
	}
	if cn := PeerCN(ctx); cn != "node" {
		log.Audit("rpc-denied", "method", method, "principal", cn)
		return status.Errorf(codes.PermissionDenied,
			"%s requires the cluster's node certificate (connected as %q)", method, cn)
	}
	return nil
}

func (s *Server) updateClock(now *rpcpb.Hlc) {
	if now == nil {
		return
	}
	ts := hlc.Timestamp{WallTime: now.WallTime, Logical: now.Logical}
	if err := s.clock.Update(ts); err != nil {
		// A clock this far off undermines the uncertainty guarantee;
		// continuing risks serving inconsistent reads.
		log.Fatalf("clock synchronization violated: %v", err)
	}
}

func (s *Server) RaftMessages(stream rpcpb.Internode_RaftMessagesServer) error {
	if err := s.requireNode(stream.Context(), "RaftMessages"); err != nil {
		return err
	}
	for {
		env, err := stream.Recv()
		if err == io.EOF {
			return stream.SendAndClose(&rpcpb.RaftAck{})
		}
		if err != nil {
			return err
		}
		// The stream lives as long as the connection, so each message,
		// not the stream, is the task: a stop waits for the message in
		// hand, not for the peer to hang up.
		if err := s.runTask(stream.Context(), "RaftMessages", func(ctx context.Context) {
			s.raftEnvelope(ctx, env)
		}); err != nil {
			return err
		}
	}
}

// raftEnvelope dispatches one envelope of a RaftMessages stream.
func (s *Server) raftEnvelope(ctx context.Context, env *rpcpb.RaftEnvelope) {
	s.updateClock(env.Now)
	if s.handlers.NodeInfo != nil && env.FromNode != 0 && env.FromAddr != "" {
		s.handlers.NodeInfo(base.NodeID(env.FromNode), env.FromAddr)
	}
	if s.handlers.NodeHealth != nil && env.FromNode != 0 && env.Health != nil {
		s.handlers.NodeHealth(base.NodeID(env.FromNode), env.Health)
	}
	if len(env.Heartbeats) > 0 || len(env.HeartbeatResponses) > 0 || len(env.ClosedTimestamps) > 0 {
		if s.handlers.RaftHeartbeats != nil {
			s.handlers.RaftHeartbeats(ctx, base.NodeID(env.FromNode),
				heartbeatsOf(env.Heartbeats), heartbeatsOf(env.HeartbeatResponses), heartbeatsOf(env.ClosedTimestamps))
		}
		return
	}
	var m raftpb.Message
	if err := m.Unmarshal(env.Message); err != nil {
		log.Warnf("dropping undecodable raft message: %v", err)
		return
	}
	s.handlers.Raft(ctx, base.RangeID(env.RangeId), m)
}

// heartbeatsOf converts a coalesced envelope's heartbeats.
func heartbeatsOf(in []*rpcpb.RaftHeartbeat) []kvserver.RaftHeartbeat {
	out := make([]kvserver.RaftHeartbeat, 0, len(in))
	for _, hb := range in {
		if hb == nil {
			continue
		}
		out = append(out, kvserver.RaftHeartbeat{
			RangeID: base.RangeID(hb.RangeId), To: hb.ToReplica, From: hb.FromReplica, Term: hb.Term, Commit: hb.Commit, Quiesce: hb.Quiesce,
			Index: hb.Index, ClosedTS: hlc.Timestamp{WallTime: hb.ClosedWall, Logical: hb.ClosedLogical},
		})
	}
	return out
}

// Ping answers a peer's latency probe with this node's physical clock at
// receipt and at reply (the NTP exchange the caller completes with its
// own send and receive times).
func (s *Server) Ping(ctx context.Context, in *rpcpb.PingRequest) (*rpcpb.PingResponse, error) {
	recv := s.clock.PhysicalNow()
	if err := s.requireNode(ctx, "Ping"); err != nil {
		return nil, err
	}
	s.updateClock(in.Now)
	now := s.clock.Now()
	return &rpcpb.PingResponse{
		RecvWall: recv,
		SendWall: s.clock.PhysicalNow(),
		Now:      &rpcpb.Hlc{WallTime: now.WallTime, Logical: now.Logical},
	}, nil
}

func (s *Server) payload(ctx context.Context, h PayloadHandler, in *rpcpb.Payload) (*rpcpb.Payload, error) {
	s.updateClock(in.Now)
	out, err := h(ctx, in.Json)
	if err != nil {
		return nil, err
	}
	now := s.clock.Now()
	return &rpcpb.Payload{Json: out, Now: &rpcpb.Hlc{WallTime: now.WallTime, Logical: now.Logical}}, nil
}

// Batch decodes a KV batch (proto on the hot path; JSON accepted from
// older senders) and replies in the encoding the request used, so a mixed
// pair degrades to JSON instead of failing.
func (s *Server) Batch(ctx context.Context, in *rpcpb.Payload) (*rpcpb.Payload, error) {
	if err := s.requireNode(ctx, "Batch"); err != nil {
		return nil, err
	}
	s.updateClock(in.Now)
	var (
		ba  *kvpb.BatchRequest
		err error
	)
	useProto := len(in.Proto) > 0
	if useProto {
		ba, err = kvpb.UnmarshalBatchRequest(in.Proto)
	} else {
		ba = &kvpb.BatchRequest{}
		err = json.Unmarshal(in.Json, ba)
	}
	if err != nil {
		return nil, err
	}
	br, kerr := s.handlers.Batch(ctx, ba)
	now := s.clock.Now()
	out := &rpcpb.Payload{Now: &rpcpb.Hlc{WallTime: now.WallTime, Logical: now.Logical}}
	if useProto {
		out.Proto, err = kvpb.MarshalBatchEnvelope(br, kerr)
	} else {
		out.Json, err = MarshalBatchResult(br, kerr)
	}
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (s *Server) Join(ctx context.Context, in *rpcpb.Payload) (*rpcpb.Payload, error) {
	if err := s.requireNode(ctx, "Join"); err != nil {
		return nil, err
	}
	return s.payload(ctx, s.handlers.Join, in)
}

func (s *Server) Admin(ctx context.Context, in *rpcpb.Payload) (*rpcpb.Payload, error) {
	return s.payload(ctx, s.handlers.Admin, in)
}

func (s *Server) Snapshot(stream rpcpb.Internode_SnapshotServer) error {
	if err := s.requireNode(stream.Context(), "Snapshot"); err != nil {
		return err
	}
	if s.handlers.Snapshot == nil {
		return io.EOF
	}
	first, err := stream.Recv()
	if err != nil {
		return err
	}
	s.updateClock(first.Now)
	// The whole stream is one task: the snapshot is applied through one
	// engine batch as it arrives. A stop cancels ctx, and the next chunk
	// asked for ends it.
	var ctx context.Context
	next := func() ([]kvserver.SnapshotKV, error) {
		if err := ctx.Err(); err != nil {
			return nil, status.Errorf(codes.Unavailable, "Snapshot: %s", stoppingMessage)
		}
		chunk, err := stream.Recv()
		if err == io.EOF {
			return nil, nil
		}
		if err != nil {
			return nil, err
		}
		s.updateClock(chunk.Now)
		out := make([]kvserver.SnapshotKV, len(chunk.Kvs))
		for i, kv := range chunk.Kvs {
			out[i] = kvserver.SnapshotKV{Key: kv.Key, Value: kv.Value}
		}
		return out, nil
	}
	var herr error
	if err := s.runTask(stream.Context(), "Snapshot", func(tctx context.Context) {
		ctx = tctx
		herr = s.handlers.Snapshot(first.HeaderJson, next)
	}); err != nil {
		return err
	}
	if herr != nil {
		return herr
	}
	return stream.SendAndClose(&rpcpb.SnapshotAck{})
}

// batchRPCEnvelope is the JSON body of Batch calls: response or wire error.
type batchRPCEnvelope struct {
	Response *kvpb.BatchResponse `json:"response,omitempty"`
	Error    *kvpb.Error         `json:"error,omitempty"`
}

// MarshalBatchResult encodes a KV execution outcome for the wire.
func MarshalBatchResult(br *kvpb.BatchResponse, kerr *kvpb.Error) ([]byte, error) {
	return json.Marshal(batchRPCEnvelope{Response: br, Error: kerr})
}

// UnmarshalBatchResult decodes a KV execution outcome.
func UnmarshalBatchResult(data []byte) (*kvpb.BatchResponse, *kvpb.Error, error) {
	var env batchRPCEnvelope
	if err := json.Unmarshal(data, &env); err != nil {
		return nil, nil, err
	}
	return env.Response, env.Error, nil
}
