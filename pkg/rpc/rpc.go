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

	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"

	"github.com/sthorne/datax/pkg/base"
	"github.com/sthorne/datax/pkg/kvpb"
	"github.com/sthorne/datax/pkg/kvserver"
	"github.com/sthorne/datax/pkg/rpc/rpcpb"
	"github.com/sthorne/datax/pkg/util/hlc"
	"github.com/sthorne/datax/pkg/util/log"
)

// PayloadHandler serves a JSON request payload and returns a JSON response.
type PayloadHandler func(ctx context.Context, data []byte) ([]byte, error)

// SnapshotHandler consumes an incoming range snapshot stream.
type SnapshotHandler func(header []byte, kvs func() ([]kvserver.SnapshotKV, error)) error

// ServerHandlers are the node-side callbacks the transport dispatches into.
type ServerHandlers struct {
	// Batch executes a KV batch. Wire encoding (proto on the hot path,
	// JSON from older senders) is the rpc layer's concern, not the node's.
	Batch    func(ctx context.Context, ba *kvpb.BatchRequest) (*kvpb.BatchResponse, *kvpb.Error)
	Join     PayloadHandler
	Admin    PayloadHandler
	Raft     func(ctx context.Context, rangeID base.RangeID, m raftpb.Message)
	Snapshot SnapshotHandler
	// NodeInfo learns a peer's address from its Raft envelopes.
	NodeInfo func(id base.NodeID, addr string)
}

// Server implements rpcpb.InternodeServer.
type Server struct {
	rpcpb.UnimplementedInternodeServer
	clock    *hlc.Clock
	handlers ServerHandlers
}

// NewServer returns a gRPC server with the Internode service registered.
// A non-nil tlsCfg enables mutual TLS (the config must require and verify
// client certificates); nil serves cleartext.
func NewServer(clock *hlc.Clock, handlers ServerHandlers, tlsCfg *tls.Config) *grpc.Server {
	var opts []grpc.ServerOption
	if tlsCfg != nil {
		opts = append(opts, grpc.Creds(credentials.NewTLS(tlsCfg)))
	}
	gs := grpc.NewServer(opts...)
	rpcpb.RegisterInternodeServer(gs, &Server{clock: clock, handlers: handlers})
	return gs
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
	for {
		env, err := stream.Recv()
		if err == io.EOF {
			return stream.SendAndClose(&rpcpb.RaftAck{})
		}
		if err != nil {
			return err
		}
		s.updateClock(env.Now)
		if s.handlers.NodeInfo != nil && env.FromNode != 0 && env.FromAddr != "" {
			s.handlers.NodeInfo(base.NodeID(env.FromNode), env.FromAddr)
		}
		var m raftpb.Message
		if err := m.Unmarshal(env.Message); err != nil {
			log.Warnf("dropping undecodable raft message: %v", err)
			continue
		}
		s.handlers.Raft(stream.Context(), base.RangeID(env.RangeId), m)
	}
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
	return s.payload(ctx, s.handlers.Join, in)
}

func (s *Server) Admin(ctx context.Context, in *rpcpb.Payload) (*rpcpb.Payload, error) {
	return s.payload(ctx, s.handlers.Admin, in)
}

func (s *Server) Snapshot(stream rpcpb.Internode_SnapshotServer) error {
	if s.handlers.Snapshot == nil {
		return io.EOF
	}
	first, err := stream.Recv()
	if err != nil {
		return err
	}
	s.updateClock(first.Now)
	next := func() ([]kvserver.SnapshotKV, error) {
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
	if err := s.handlers.Snapshot(first.HeaderJson, next); err != nil {
		return err
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
