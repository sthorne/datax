// Package server wires a datax node together: engine, clock, transport,
// store, cluster membership, KV client, and (later phases) SQL + pgwire.
package server

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"

	"github.com/sthorne/datax/pkg/base"
	"github.com/sthorne/datax/pkg/cluster"
	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/kvclient"
	"github.com/sthorne/datax/pkg/kvpb"
	"github.com/sthorne/datax/pkg/kvserver"
	"github.com/sthorne/datax/pkg/pgwire"
	"github.com/sthorne/datax/pkg/rpc"
	"github.com/sthorne/datax/pkg/security"
	"github.com/sthorne/datax/pkg/storage"
	"github.com/sthorne/datax/pkg/util/hlc"
	"github.com/sthorne/datax/pkg/util/log"
	"github.com/sthorne/datax/pkg/util/stop"
)

// Config configures a node.
type Config struct {
	// Dir is the data directory ("" = in-memory, for tests/demo).
	Dir string
	// Listen is the internode RPC address (host:port).
	Listen string
	// PGListen is the SQL (PostgreSQL protocol) address; empty disables.
	PGListen string
	// Join is an existing node's RPC address; empty with BootstrapSelf set
	// starts a new cluster.
	Join          string
	BootstrapSelf bool
	Locality      base.Locality
	MaxOffset     time.Duration
	// UpreplicationInterval is how often the repair loop scans for
	// under-replicated ranges (default 3s).
	UpreplicationInterval time.Duration
	// GCTTL is how long MVCC history is retained before garbage collection
	// (default 25h; negative disables GC).
	GCTTL time.Duration
	// GCInterval is how often the GC loop scans led ranges (default 60s).
	GCInterval time.Duration
	// LivenessGrace is how stale a node's heartbeat may be before the
	// allocator stops treating it as live (default 15s).
	LivenessGrace time.Duration
	// DeadNodeThreshold is how stale a node's heartbeat must be before its
	// replicas are repaired away (default 30s; must exceed LivenessGrace).
	DeadNodeThreshold time.Duration
	// DisableLeaseReads makes every read confirm leadership with a quorum
	// round trip (v1 behavior) instead of the leader's lease.
	DisableLeaseReads bool
	// SplitSizeThreshold is the range size that triggers an automatic split
	// (default 64 MiB; negative disables).
	SplitSizeThreshold int64
	// CertsDir enables secure mode: mutual TLS between nodes, TLS +
	// SCRAM-SHA-256 authentication on the SQL listener. Empty = insecure
	// (cleartext, trust auth).
	CertsDir string
	// RootPassword, in secure mode, seeds the root user's credential at
	// startup if no verifier exists yet.
	RootPassword string
	// HTTPListen serves /metrics (Prometheus) and /status (JSON); empty
	// disables.
	HTTPListen string
	// DescLeaseTTL is the gateway descriptor-lease lifetime (0 = default
	// 10s; negative disables leasing, restoring pre-lease cache semantics).
	DescLeaseTTL time.Duration
	// MergeSizeThreshold: a range and its right neighbor both below it (and
	// colocated) merge automatically (0 = a quarter of SplitSizeThreshold;
	// negative disables).
	MergeSizeThreshold int64
	// RebalanceThreshold is the range-count spread (most- minus least-loaded
	// live node) at which the allocator moves one replica per tick toward
	// balance (0 = default 2, the minimum with hysteresis; negative
	// disables automatic rebalancing).
	RebalanceThreshold int
	// ClosedTimestampLag is how far behind now() published closed
	// timestamps sit — the follower-read staleness floor (0 = default 3s;
	// negative disables closed timestamps and follower reads).
	// ClosedTimestampInterval is the publication cadence (0 = default 1s).
	ClosedTimestampLag      time.Duration
	ClosedTimestampInterval time.Duration

	// Test hooks.
	TestingKnobs    kvserver.TestingKnobs
	Engine          *storage.Engine
	Listener        net.Listener
	PGListener      net.Listener
	HTTPListener    net.Listener
	Clock           *hlc.Clock
	StaticBootstrap *StaticBootstrap
	AdvertiseAddr   string
}

// StaticBootstrap seeds a multi-node cluster with pre-agreed membership
// (testcluster): every seed node boots the same range-1 descriptor.
type StaticBootstrap struct {
	ClusterID uuid.UUID
	NodeID    base.NodeID
	Range1    kvpb.RangeDescriptor
	Nodes     []kvpb.NodeDescriptor
}

// Node is a running datax node.
type Node struct {
	cfg      Config
	tlsCfgs  *security.TLSConfigs // nil in insecure mode
	stopper  *stop.Stopper
	clock    *hlc.Clock
	engine   *storage.Engine
	registry *cluster.Registry
	trans    *rpc.Transport
	store    *kvserver.Store
	db       *kvclient.DB
	ident    cluster.StoreIdent
	addr     string
	// joinRange1 carries the routing bootstrap from a join response until
	// the DB exists to seed.
	joinRange1 kvpb.RangeDescriptor

	pgServer *pgwire.Server // set when PGListen/PGListener is configured
	httpAddr string         // set when HTTPListen/HTTPListener is configured

	// draining mirrors this node's registry Draining flag. The heartbeat
	// loop adopts it from the node's own registry row (a decommission may
	// be initiated from any node) and re-asserts it on every beat, so the
	// flag survives both heartbeat overwrites and restarts.
	draining atomic.Bool
}

// Start boots the node and returns once it is serving.
func Start(cfg Config) (*Node, error) {
	n := &Node{cfg: cfg, stopper: stop.NewStopper()}
	if err := n.start(); err != nil {
		n.stopper.Stop()
		return nil, err
	}
	return n, nil
}

func (n *Node) start() error {
	if n.cfg.MaxOffset == 0 {
		n.cfg.MaxOffset = base.DefaultMaxClockOffset
	}
	n.clock = n.cfg.Clock
	if n.clock == nil {
		n.clock = hlc.NewClock(nil, n.cfg.MaxOffset)
	}

	var err error
	n.engine = n.cfg.Engine
	if n.engine == nil {
		n.engine, err = storage.Open(n.cfg.Dir)
		if err != nil {
			return err
		}
		n.stopper.AddCloser(func() { _ = n.engine.Close() })
	}

	lis := n.cfg.Listener
	if lis == nil {
		lis, err = net.Listen("tcp", n.cfg.Listen)
		if err != nil {
			return err
		}
	}
	n.addr = n.cfg.AdvertiseAddr
	if n.addr == "" {
		n.addr = lis.Addr().String()
	}

	n.registry = cluster.NewRegistry()
	n.registry.SetClock(func() int64 { return n.clock.Now().WallTime })
	n.trans = rpc.NewTransport(n.clock, n.stopper, n.registry.Resolve)
	n.stopper.AddCloser(n.trans.Close)
	if n.cfg.CertsDir != "" {
		n.tlsCfgs, err = security.LoadNodeTLS(n.cfg.CertsDir)
		if err != nil {
			return fmt.Errorf("loading TLS certificates: %w", err)
		}
		n.trans.SetTLS(n.tlsCfgs.Client)
	}

	// Identity: read from disk, or establish via bootstrap/join.
	ident, initialized, err := cluster.ReadStoreIdent(n.engine)
	if err != nil {
		return err
	}
	var freshRange1 *kvpb.RangeDescriptor
	switch {
	case initialized:
		n.ident = ident
		// A restarting node must reach its peers before any range can
		// elect a leader: reload the last known registry.
		if nodes, err := cluster.LoadPersistedRegistry(n.engine); err == nil {
			for _, nd := range nodes {
				if nd.NodeID != n.ident.NodeID {
					n.registry.Upsert(nd)
				}
			}
		}
	case n.cfg.BootstrapSelf:
		id := cluster.StoreIdent{ClusterID: uuid.New(), NodeID: 1, StoreID: 1}
		desc := cluster.Range1Descriptor([]base.NodeID{1})
		if err := cluster.BootstrapEngine(n.engine, id, desc, 1); err != nil {
			return err
		}
		n.ident = id
		freshRange1 = &desc
		log.Infof("bootstrapped new cluster %s as %s", id.ClusterID, id.NodeID)
	case n.cfg.StaticBootstrap != nil:
		sb := n.cfg.StaticBootstrap
		id := cluster.StoreIdent{ClusterID: sb.ClusterID, NodeID: sb.NodeID, StoreID: base.StoreID(sb.NodeID)}
		if err := cluster.BootstrapEngine(n.engine, id, sb.Range1, len(sb.Range1.Replicas)); err != nil {
			return err
		}
		n.ident = id
		desc := sb.Range1
		freshRange1 = &desc
		for _, nd := range sb.Nodes {
			n.registry.Upsert(nd)
		}
	case n.cfg.Join != "":
		if err := n.join(); err != nil {
			return err
		}
	default:
		return fmt.Errorf("store is uninitialized: start with --join or use 'datax init'")
	}

	// Serve RPC before starting replicas so peers can reach us as soon as
	// raft groups spin up.
	n.store = kvserver.NewStore(kvserver.StoreConfig{
		NodeID:                  n.ident.NodeID,
		StoreID:                 n.ident.StoreID,
		Engine:                  n.engine,
		Clock:                   n.clock,
		Transport:               n.trans,
		SnapshotSender:          n.trans,
		Stopper:                 n.stopper,
		DisableLeaseReads:       n.cfg.DisableLeaseReads,
		SplitSizeThreshold:      n.cfg.SplitSizeThreshold,
		MergeSizeThreshold:      n.cfg.MergeSizeThreshold,
		ClosedTimestampLag:      n.cfg.ClosedTimestampLag,
		ClosedTimestampInterval: n.cfg.ClosedTimestampInterval,
		TestingKnobs:            n.cfg.TestingKnobs,
	})
	n.db = kvclient.NewDB(n.store, n.trans, n.clock)
	n.db.EnableMetaLookup()
	n.db.SetNodeLister(func() []base.NodeID {
		nodes := n.registry.All()
		ids := make([]base.NodeID, len(nodes))
		for i, nd := range nodes {
			ids[i] = nd.NodeID
		}
		return ids
	})
	n.store.SetSender(n.db)

	n.trans.SetLocalInfo(n.ident.NodeID, n.addr)
	var serverTLS *tls.Config
	if n.tlsCfgs != nil {
		serverTLS = n.tlsCfgs.Server
	}
	grpcServer := rpc.NewServer(n.clock, rpc.ServerHandlers{
		Batch:    n.handleBatch,
		Join:     n.handleJoin,
		Admin:    n.handleAdmin,
		Raft:     n.store.HandleRaftMessage,
		Snapshot: n.store.ApplySnapshotStream,
		NodeInfo: n.registry.UpsertAddress,
	}, serverTLS)
	// Not a stopper worker: Serve exits when the closer calls Stop, and
	// closers run after workers — a worker here would deadlock shutdown.
	go func() {
		if err := grpcServer.Serve(lis); err != nil {
			log.Debugf("grpc server stopped: %v", err)
		}
	}()
	n.stopper.AddCloser(grpcServer.Stop)

	if initialized {
		// A restarted node may be back on a different address. Announce it
		// to every peer we can reach — the configured join target plus the
		// persisted registry — BEFORE raft groups spin up, so peers stop
		// dialing the old address and their responses can find us. Purely
		// best-effort: peers that moved too are healed by piggybacked
		// addresses on raft envelopes once any link forms.
		n.reannounce()
	}

	// Bring up replicas: restart from disk, or create the fresh range 1.
	if err := n.store.LoadReplicas(); err != nil {
		return err
	}
	if freshRange1 != nil {
		if _, err := n.store.CreateReplica(*freshRange1, true /* bootstrap */); err != nil {
			return err
		}
	}
	if desc, ok, err := loadLocalRange1(n.engine); err != nil {
		return err
	} else if ok {
		n.db.SeedDescriptor(desc)
	}
	if n.joinRange1.RangeID != 0 {
		n.db.SeedDescriptor(n.joinRange1)
	}

	n.registry.Upsert(kvpb.NodeDescriptor{
		NodeID: n.ident.NodeID, Address: n.addr, Locality: n.cfg.Locality,
		LivenessTime: n.clock.Now().WallTime,
	})
	if err := cluster.PersistRegistry(n.engine, n.registry.All()); err != nil {
		log.Warnf("persisting registry: %v", err)
	}

	if err := n.stopper.RunWorker(n.heartbeatLoop); err != nil {
		return err
	}
	if err := n.stopper.RunWorker(n.upreplicationLoop); err != nil {
		return err
	}
	gcTTL := n.cfg.GCTTL
	if gcTTL == 0 {
		gcTTL = base.DefaultGCTTL
	}
	gcInterval := n.cfg.GCInterval
	if gcInterval == 0 {
		gcInterval = base.DefaultGCInterval
	}
	if err := n.store.StartHousekeeping(gcTTL, gcInterval); err != nil {
		return err
	}
	if err := n.store.StartClosedTimestamps(); err != nil {
		return err
	}
	log.Infof("node %s serving internode RPC at %s", n.ident.NodeID, n.addr)
	if err := n.startHTTP(); err != nil {
		return err
	}
	return n.startSQL()
}

func loadLocalRange1(eng *storage.Engine) (kvpb.RangeDescriptor, bool, error) {
	var desc kvpb.RangeDescriptor
	raw, err := eng.Get(keys.RangeDescriptorKey(1))
	if err != nil || raw == nil {
		return desc, false, err
	}
	if err := json.Unmarshal(raw, &desc); err != nil {
		return desc, false, err
	}
	return desc, true, nil
}

// reannounce advertises this (already-initialized) node's identity and
// current address to the configured join target and every persisted-registry
// peer, in parallel, best-effort. Successful responses carry the fresh node
// list the peer holds — including addresses of other nodes that also moved
// — which is what re-forms a cluster restarted entirely on new addresses.
func (n *Node) reannounce() {
	targets := make(map[string]struct{})
	if n.cfg.Join != "" {
		targets[n.cfg.Join] = struct{}{}
	}
	for _, nd := range n.registry.All() {
		if nd.NodeID != n.ident.NodeID && nd.Address != "" && nd.Address != n.addr {
			targets[nd.Address] = struct{}{}
		}
	}
	if len(targets) == 0 {
		return
	}
	req := cluster.JoinRequest{
		Address:   n.addr,
		Locality:  n.cfg.Locality,
		NodeID:    n.ident.NodeID,
		ClusterID: n.ident.ClusterID,
	}
	var (
		wg sync.WaitGroup
		mu sync.Mutex // guards n.joinRange1 across the announce goroutines
	)
	for addr := range targets {
		wg.Add(1)
		go func(addr string) {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(n.stopper.Ctx(), 3*time.Second)
			defer cancel()
			var resp cluster.JoinResponse
			if err := n.trans.Call(ctx, addr, "join", req, &resp); err != nil {
				log.Debugf("re-announce to %s failed: %v", addr, err)
				return
			}
			if resp.Error != "" {
				log.Warnf("re-announce to %s rejected: %s", addr, resp.Error)
				return
			}
			for _, nd := range resp.Nodes {
				if nd.NodeID != n.ident.NodeID {
					n.registry.Upsert(nd)
				}
			}
			mu.Lock()
			if resp.Range1.RangeID != 0 && n.joinRange1.RangeID == 0 {
				n.joinRange1 = resp.Range1
			}
			mu.Unlock()
			log.Infof("re-announced %s at %s via %s", n.ident.NodeID, n.addr, addr)
		}(addr)
	}
	wg.Wait()
	if err := cluster.PersistRegistry(n.engine, n.registry.All()); err != nil {
		log.Debugf("persisting registry: %v", err)
	}
}

// join contacts an existing node to obtain identity and routing bootstrap.
func (n *Node) join() error {
	req := cluster.JoinRequest{Address: n.addr, Locality: n.cfg.Locality}
	var resp cluster.JoinResponse
	ctx, cancel := context.WithTimeout(n.stopper.Ctx(), 30*time.Second)
	defer cancel()
	if err := n.trans.Call(ctx, n.cfg.Join, "join", req, &resp); err != nil {
		return fmt.Errorf("joining via %s: %w", n.cfg.Join, err)
	}
	if resp.Error != "" {
		return fmt.Errorf("join rejected: %s", resp.Error)
	}
	n.ident = cluster.StoreIdent{ClusterID: resp.ClusterID, NodeID: resp.NodeID, StoreID: base.StoreID(resp.NodeID)}
	if err := cluster.WriteStoreIdent(n.engine, n.ident); err != nil {
		return err
	}
	for _, nd := range resp.Nodes {
		n.registry.Upsert(nd)
	}
	// Persist range 1's descriptor? No — we hold no replica of it. Seed the
	// routing cache only, after the DB exists (start() does it via
	// SeedDescriptor below).
	n.joinRange1 = resp.Range1
	log.Infof("joined cluster %s as %s", resp.ClusterID, resp.NodeID)
	return nil
}

// Stop shuts the node down gracefully.
func (n *Node) Stop() { n.stopper.Stop() }

// Accessors used by tests, the CLI, and later phases.
func (n *Node) NodeID() base.NodeID { return n.ident.NodeID }
func (n *Node) Addr() string        { return n.addr }

// SQLAddr is the SQL listener address (real listener from Phase 6 on).
func (n *Node) SQLAddr() string {
	if n.pgServer == nil {
		return ""
	}
	return n.pgServer.Addr()
}

// HTTPAddr is the observability listener address ("" when disabled).
func (n *Node) HTTPAddr() string            { return n.httpAddr }
func (n *Node) DB() *kvclient.DB            { return n.db }
func (n *Node) Store() *kvserver.Store      { return n.store }
func (n *Node) Clock() *hlc.Clock           { return n.clock }
func (n *Node) Stopper() *stop.Stopper      { return n.stopper }
func (n *Node) Registry() *cluster.Registry { return n.registry }
