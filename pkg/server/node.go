// Package server wires a datax node together: engine, clock, transport,
// store, cluster membership, KV client, and (later phases) SQL + pgwire.
package server

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"net"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/pebble/v2/vfs"
	"github.com/google/uuid"

	"github.com/sthorne/datax/pkg/base"
	"github.com/sthorne/datax/pkg/cluster"
	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/kvclient"
	"github.com/sthorne/datax/pkg/kvpb"
	"github.com/sthorne/datax/pkg/kvserver"
	"github.com/sthorne/datax/pkg/pgwire"
	"github.com/sthorne/datax/pkg/rpc"
	"github.com/sthorne/datax/pkg/rpc/rpcpb"
	"github.com/sthorne/datax/pkg/security"
	"github.com/sthorne/datax/pkg/sql/catalog"
	"github.com/sthorne/datax/pkg/storage"
	"github.com/sthorne/datax/pkg/storage/enc"
	"github.com/sthorne/datax/pkg/util/events"
	"github.com/sthorne/datax/pkg/util/hlc"
	"github.com/sthorne/datax/pkg/util/log"
	"github.com/sthorne/datax/pkg/util/stop"
	"github.com/sthorne/datax/pkg/util/sysstats"
	"github.com/sthorne/datax/pkg/version"
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
	// ConsistencyInterval paces the replica consistency sweep: every
	// interval, one led range's replicas are checksummed and compared.
	// 0 (the default) disables the sweep — hashing a range reads all of
	// it, so operators opt in and pick the pace.
	ConsistencyInterval time.Duration
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
	// RaftWorkers sizes the store's raft scheduler pool (0 = GOMAXPROCS).
	RaftWorkers int
	// DisableQuiescence keeps idle ranges ticking and heartbeating
	// (kvserver/quiesce.go); the v12 coalescing of heartbeats stays on.
	DisableQuiescence bool
	// SplitSizeThreshold is the range size that triggers an automatic split
	// (default 64 MiB; negative disables).
	SplitSizeThreshold int64
	// SlowStatementThreshold is the statement duration past which the SQL
	// server records the statement for the dashboard's slow list (0 =
	// 500 ms).
	SlowStatementThreshold time.Duration
	// MutexProfileFraction and BlockProfileRate set the runtime's
	// contention sampling so mutex and block profiles are always
	// available from /debug/pprof/ (0 = the defaults, 1 in 100 contended
	// mutex events and blocking events of 10 ms or more; negative
	// disables). Process-wide: the first node started sets them.
	MutexProfileFraction int
	BlockProfileRate     time.Duration
	// TruncationFlushAfter bounds how long a split store's deferred log
	// truncation waits for a natural state-engine flush before the
	// housekeeping tick forces one (0 = 30 s; negative = never).
	TruncationFlushAfter time.Duration
	// StorageMemTableSize overrides the storage profile's memtable size
	// (0 = the profile's). Crash-consistency tests shrink it.
	StorageMemTableSize int
	// StorageCacheSize is the block cache size in bytes (0 = the
	// profile's share of the machine's memory; --cache-size).
	StorageCacheSize int64
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
	// DrainTimeout bounds Drain (lease transfers and the SQL connection
	// drain a stop performs first); 0 = 10s.
	DrainTimeout time.Duration
	// MetricsRecordInterval paces the metrics recorder, which writes this
	// node's metrics into the datax_metrics system table (see
	// metrics_recorder.go). 0 = 10s; negative disables recording.
	MetricsRecordInterval time.Duration
	// StatsRefreshInterval paces the background statistics sampler: each
	// tick, the node leading range 1 re-collects statistics for at most
	// ONE table whose stats are missing or older than StatsStaleness
	// (0 = default 60s; negative disables the sampler — ANALYZE still
	// works).
	StatsRefreshInterval time.Duration
	// StatsStaleness is the statistics age that triggers a background
	// re-collection (0 = default 10m).
	StatsStaleness time.Duration
	// ReshardRetireFor is how long a completed re-shard's superseded
	// layout stays on disk serving AS OF SYSTEM TIME below the swap
	// before the janitor reclaims it (0 = the GC TTL, matching the
	// deepest admissible historical read).
	ReshardRetireFor time.Duration
	// ClosedTimestampLag is how far behind now() published closed
	// timestamps sit — the follower-read staleness floor (0 = default 3s;
	// negative disables closed timestamps and follower reads).
	// ClosedTimestampInterval is the publication cadence (0 = default 1s).
	ClosedTimestampLag      time.Duration
	ClosedTimestampInterval time.Duration
	// LoadSplitThreshold is the sustained per-range QPS above which the
	// housekeeping loop splits a range by load (0 = default; negative
	// disables).
	LoadSplitThreshold float64
	// LeaseShedFactor: a node whose aggregate leader QPS exceeds the
	// live-set mean by this factor sheds one hot lease per tick (0 =
	// default 1.5). LeaseShedMinQPS is the absolute spread floor below
	// which shedding never fires (0 = default 100) — idle clusters must
	// not shuffle leases over noise.
	LeaseShedFactor float64
	LeaseShedMinQPS float64
	// RebalanceBytesThreshold is the replica-byte spread (fullest minus
	// emptiest live node, with a 20%-of-mean floor) at which one replica
	// of a large range moves per tick (0 = default 64 MiB; negative
	// disables byte-weighted moves).
	RebalanceBytesThreshold int64
	// LoadCooldown is the per-range hold-off between load-driven ops (0 =
	// default 60s; it must exceed the QPS maturity window, or the shed
	// pass acts on the post-transfer blind spot and ping-pongs).
	LoadCooldown time.Duration
	// StorageProfile selects the engine's Pebble tuning ("" = balanced).
	StorageProfile storage.Profile
	// EncKeyPath names the file holding the 32-byte store encryption key
	// (raw or hex); empty = plaintext storage. A comma-separated list of
	// files is tried in order against the store's key registry, so a new
	// key can be staged beside the current one before an online rotation
	// and a restart in between still opens the store (see
	// enc.MatchStoreKey).
	EncKeyPath string
	// BinaryVersionOverride makes the node advertise (and gate on) this
	// protocol version instead of version.Current — used by tests to
	// simulate mixed-version clusters. 0 = version.Current.
	BinaryVersionOverride version.Version

	// Test hooks.
	TestingKnobs kvserver.TestingKnobs
	// Engine injects the state engine (tests: in-memory); RaftEngine,
	// with it, injects a raft engine so the store runs split. With
	// Engine set the node never opens or migrates a store itself.
	Engine          *storage.Engine
	RaftEngine      *storage.Engine
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
// sysSampleInterval is how often the node samples its host; rates on the
// dashboard are deltas over this interval.
const sysSampleInterval = 5 * time.Second

// pingInterval is how often a node pings every peer for the latency and
// clock-offset matrix; pingTimeout is when a ping counts as unreachable.
const (
	pingInterval = 2 * time.Second
	pingTimeout  = time.Second
)

type Node struct {
	cfg     Config
	tlsCfgs *security.TLSConfigs // nil in insecure mode
	stopper *stop.Stopper
	clock   *hlc.Clock
	engine  *storage.Engine
	// raftEngine holds the raft log when the store is split (engines.go);
	// raftEncKey is its store key when a rotation left it sealed under a
	// different candidate than the state engine.
	raftEngine *storage.Engine
	raftEncKey []byte
	registry   *cluster.Registry
	trans      *rpc.Transport
	store      *kvserver.Store
	db         *kvclient.DB
	ident      cluster.StoreIdent
	addr       string
	// joinRange1 carries the routing bootstrap from a join response until
	// the DB exists to seed.
	joinRange1 kvpb.RangeDescriptor

	// pgServer is set by startSQL after the heartbeat loop already runs,
	// which reads it to publish SQL activity: an atomic pointer, not a
	// plain field.
	pgServer atomic.Pointer[pgwire.Server] // set when PGListen/PGListener is configured
	httpAddr string                        // set when HTTPListen/HTTPListener is configured

	// encKey is the loaded store encryption key; non-nil means the store is
	// encrypted and node-written artifacts (metadata backup) are sealed too.
	// Guarded by encKeyMu: online rotation (rotate-store-key) swaps it.
	encKeyMu sync.Mutex
	encKey   []byte
	// encKeyPaths are the key files EncKeyPath named (the one that opened
	// the store first), consulted after an online rotation to tell the
	// operator whether a restart would find the new key.
	encKeyPaths []string
	// rotateMu serializes online store-key rotations end to end: registry
	// reseal, encKey swap, and metadata-backup reseal (issue #66).
	rotateMu sync.Mutex

	// Background re-encryption state (see reencrypt.go).
	reencMu        sync.Mutex
	reencActive    bool
	reencRewritten int64
	// Background rewrite of the tables written before prefix mode (see
	// rewrite.go); prefixBloomNoticed: the next-restart notice was logged.
	rewriteMu          sync.Mutex
	rewriteActive      bool
	rewriteRewritten   int64
	prefixBloomNoticed atomic.Bool

	// consoleVersion digests the embedded console page (http.go);
	// loginPage is the rendered sign-in page (session.go).
	consoleVersion string
	loginPage      []byte

	// clusterVersion caches the last observed finalized cluster version
	// (0 reads as v1). Seeded from the store-local mirror at startup and
	// refreshed by the heartbeat loop from the replicated row.
	clusterVersion atomic.Int64

	// draining mirrors this node's registry Draining flag. The heartbeat
	// loop adopts it from the node's own registry row (a decommission may
	// be initiated from any node) and re-asserts it on every beat, so the
	// flag survives both heartbeat overwrites and restarts.
	draining atomic.Bool
	// shuttingDown is set by Drain: the node is shedding its leases and
	// SQL connections ahead of a stop, and says so in its heartbeats.
	shuttingDown atomic.Bool
	// sys samples the host (CPU, memory, disk, network, runtime) every
	// few seconds for the heartbeat summary, /status and /metrics.
	sys *sysstats.Sampler
	// pinger measures round trips and clock offsets to every peer.
	pinger *rpc.Pinger
	// schema caches the schema browser's document (see schema_api.go).
	schema schemaCache
	// rangeList is the last /meta listing served to the dashboard.
	rangeList rangeListCache
	// events is the node's operational event ring (see health_api.go).
	events *events.Ring
	// consistencyFailures counts checksum mismatches this node's sweeps
	// found (readable, unlike the Prometheus counter).
	consistencyFailures atomic.Int64
	// health caches the problems panel (see health_api.go).
	health healthCache
	// cat is the node's own catalog accessor (the SQL server and the
	// metrics recorder share it); catOnce builds it on first use.
	cat     *catalog.Accessor
	catOnce sync.Once
	catErr  error
	// authSecret is the cluster's authentication secret
	// (keys.AuthSecretKey), read or created on first use and cached.
	authSecretMu sync.Mutex
	authSecret   []byte
	// metricsReady is set once the datax_metrics table is known to exist;
	// metricsPaused holds the recorder off while a restore runs.
	metricsReady  atomic.Bool
	metricsPaused atomic.Bool
	// metricsLastWarn rate-limits the recorder's write-failure warning
	// (recorder goroutine only).
	metricsLastWarn time.Time

	// loadCooldown stamps the last load-driven op (lease shed / byte move)
	// per range while this node acts as the allocator; see loadOpAllowed.
	loadCooldownMu sync.Mutex
	loadCooldown   map[base.RangeID]time.Time
}

// Start boots the node and returns once it is serving.
func Start(cfg Config) (*Node, error) {
	enableProfiling(cfg)
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
	if paths := enc.SplitKeyPaths(n.cfg.EncKeyPath); len(paths) > 0 {
		candidates, err := enc.LoadKeyFiles(paths)
		if err != nil {
			return fmt.Errorf("loading encryption key: %w", err)
		}
		// Several key files are candidates: the one sealing the store's
		// registry wins, so a new key staged before an online rotation is
		// found by a restart on either side of it (issue #67).
		var idx int
		if len(candidates) > 1 {
			// A single key is validated by Open itself, as before.
			if n.cfg.Engine != nil {
				idx, err = n.cfg.Engine.MatchStoreKey(candidates)
			} else {
				idx, err = enc.MatchStoreKey(vfs.Default, n.cfg.Dir, candidates)
			}
			if err != nil {
				return fmt.Errorf("selecting the store encryption key: %w", err)
			}
		}
		n.encKey, n.encKeyPaths = candidates[idx], paths
		if len(paths) > 1 {
			log.Infof("store encryption key: %s matches the store (%d key files given)", paths[idx], len(paths))
		}
		n.matchRaftEncKey(candidates)
	}
	n.engine, n.raftEngine = n.cfg.Engine, n.cfg.RaftEngine
	if n.engine == nil {
		if err := n.openEngines(); err != nil {
			return err
		}
		// Both close after the store's raft groups have stopped (no
		// append can follow); the state engine's shutdown flush is what
		// makes the next start need no replay.
		n.stopper.AddCloser(func() { _ = n.engine.Close() })
		n.stopper.AddCloser(func() {
			if n.raftEngine != nil {
				_ = n.raftEngine.Close()
			}
		})
	}
	if n.raftEngine != nil {
		log.Infof("store layout: split (raft log on its own engine; state engine without a WAL)")
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

	n.events = events.New()
	n.installAuditSink()
	n.sys = sysstats.New(n.cfg.Dir)
	n.sys.Sample() // the first heartbeat should already carry a summary
	if err := n.stopper.RunWorker(func(ctx context.Context) { n.sys.Run(ctx, sysSampleInterval) }); err != nil {
		return err
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
		// Downgrade gate: a store that observed a finalized cluster
		// version newer than this binary supports must not rejoin — it
		// would misapply version-gated payloads. Checked store-locally so
		// it works before quorum.
		if stored, err := readStoreClusterVersion(n.engine); err != nil {
			return err
		} else if stored > n.binaryVersion() {
			return fmt.Errorf(
				"store last ran at cluster version %s but this binary supports at most %s: "+
					"downgrading a node after the cluster upgrade was finalized is not supported",
				stored, n.binaryVersion())
		} else if stored != 0 {
			n.clusterVersion.Store(int64(stored))
			n.ratchetStoreFormat()
		}
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
		if err := cluster.BootstrapEngine(n.engine, id, desc, 1, n.binaryVersion()); err != nil {
			return err
		}
		n.ident = id
		freshRange1 = &desc
		if err := n.adoptBootstrapVersion(); err != nil {
			return err
		}
		log.Infof("bootstrapped new cluster %s as %s (version %s)", id.ClusterID, id.NodeID, n.binaryVersion())
	case n.cfg.StaticBootstrap != nil:
		sb := n.cfg.StaticBootstrap
		id := cluster.StoreIdent{ClusterID: sb.ClusterID, NodeID: sb.NodeID, StoreID: base.StoreID(sb.NodeID)}
		if err := cluster.BootstrapEngine(n.engine, id, sb.Range1, len(sb.Range1.Replicas), n.binaryVersion()); err != nil {
			return err
		}
		n.ident = id
		desc := sb.Range1
		freshRange1 = &desc
		if err := n.adoptBootstrapVersion(); err != nil {
			return err
		}
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
	defaultGCTTL := n.cfg.GCTTL
	if defaultGCTTL == 0 {
		defaultGCTTL = base.DefaultGCTTL
	}
	retention := &retentionProvider{node: n, defaultTTL: defaultGCTTL}
	n.store = kvserver.NewStore(kvserver.StoreConfig{
		NodeID:                  n.ident.NodeID,
		Events:                  n.events,
		StoreID:                 n.ident.StoreID,
		Engine:                  n.engine,
		RaftEngine:              n.raftEngine,
		TruncationFlushAfter:    n.cfg.TruncationFlushAfter,
		Clock:                   n.clock,
		Transport:               n.trans,
		SnapshotSender:          n.trans,
		Stopper:                 n.stopper,
		DisableLeaseReads:       n.cfg.DisableLeaseReads,
		RaftWorkers:             n.cfg.RaftWorkers,
		DisableQuiescence:       n.cfg.DisableQuiescence,
		SplitSizeThreshold:      n.cfg.SplitSizeThreshold,
		MergeSizeThreshold:      n.cfg.MergeSizeThreshold,
		ClosedTimestampLag:      n.cfg.ClosedTimestampLag,
		ClosedTimestampInterval: n.cfg.ClosedTimestampInterval,
		LoadSplitThreshold:      n.cfg.LoadSplitThreshold,
		RetentionOverride:       retention.override,
		MergeBarrier:            n.placementBarrier,
		RowExpiry:               retention.rowExpiry,
		TestingKnobs:            n.cfg.TestingKnobs,
		WaitForApplication:      n.waitForApplication,
		ClusterVersion: func() version.Version {
			if cv := version.Version(n.clusterVersion.Load()); cv > 0 {
				return cv
			}
			return version.V1 // not yet mirrored: assume the floor
		},
	})
	n.db = kvclient.NewDB(n.store, n.trans, n.clock)
	n.db.EnableMetaLookup()
	// Version-gated request shapes key off the node's cluster-version
	// mirror (refreshed by the heartbeat loop and at finalize).
	n.db.SetVersionGate(func() version.Version {
		if cv := version.Version(n.clusterVersion.Load()); cv > 0 {
			return cv
		}
		return version.V1 // not yet mirrored: assume the floor
	})
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
	n.pinger = rpc.NewPinger(n.trans, n.ident.NodeID, func() []base.NodeID {
		all := n.registry.All()
		ids := make([]base.NodeID, 0, len(all))
		for _, nd := range all {
			ids = append(ids, nd.NodeID)
		}
		return ids
	}, pingTimeout)
	if err := n.stopper.RunWorker(func(ctx context.Context) { n.pinger.Run(ctx, pingInterval) }); err != nil {
		return err
	}
	// Piggyback this node's storage-health verdict on outgoing raft
	// envelopes, so peers' leaders can factor it into their write path.
	// The testing knob is consulted first — the advertised verdict must
	// match the one this node's own gate would apply.
	n.trans.SetHealthProvider(func() *rpcpb.StorageHealth {
		var (
			over bool
			why  string
		)
		if k := n.cfg.TestingKnobs.OverrideOverloaded; k != nil {
			over, why = k()
		} else if n.engine != nil {
			over, _, why = n.engine.OverloadedCause()
		}
		// No wall_time: receivers stamp the verdict with their own receipt
		// time, so clocks need not agree.
		h := &rpcpb.StorageHealth{Overloaded: over, Reason: why}
		if n.engine != nil {
			m := n.engine.StorageMetrics()
			h.L0Sublevels = int64(m.L0Sublevels)
			h.L0Files = int64(m.L0Files)
			h.CompactionDebtBytes = int64(m.CompactionDebtBytes)
			h.MemtableBytes = int64(m.MemtableBytes)
		}
		return h
	})
	var serverTLS *tls.Config
	if n.tlsCfgs != nil {
		serverTLS = n.tlsCfgs.Server
	}
	grpcServer := rpc.NewServer(n.clock, rpc.ServerHandlers{
		Batch:          n.handleBatch,
		Join:           n.handleJoin,
		Admin:          n.handleAdmin,
		Raft:           n.store.HandleRaftMessage,
		RaftHeartbeats: n.store.HandleRaftHeartbeats,
		Snapshot:       n.store.ApplySnapshotStream,
		NodeInfo:       n.registry.UpsertAddress,
		NodeHealth: func(id base.NodeID, h *rpcpb.StorageHealth) {
			n.store.UpdateNodeHealth(id, h.Overloaded, h.Reason)
		},
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
	n.startConsistencyLoop()
	if err := n.stopper.RunWorker(n.statsRefreshLoop); err != nil {
		return err
	}
	if err := n.stopper.RunWorker(n.reshardJanitorLoop); err != nil {
		return err
	}
	if err := n.stopper.RunWorker(n.metricsRecorderLoop); err != nil {
		return err
	}
	if err := n.stopper.RunWorker(n.ensureDatabaseCatalog); err != nil {
		return err
	}
	n.maybeStartFilterRewrite()
	if err := n.stopper.RunWorker(n.ensureRoleCatalog); err != nil {
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
		Address:       n.addr,
		Locality:      n.cfg.Locality,
		NodeID:        n.ident.NodeID,
		ClusterID:     n.ident.ClusterID,
		BinaryVersion: int(n.binaryVersion()),
		MinSupported:  int(n.minSupportedVersion()),
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

// binaryVersion is the protocol version this node runs (and advertises):
// version.Current, unless a test overrides it to simulate skew.
func (n *Node) binaryVersion() version.Version {
	if n.cfg.BinaryVersionOverride != 0 {
		return n.cfg.BinaryVersionOverride
	}
	return version.Current
}

// minSupportedVersion is the oldest cluster version this node can join.
func (n *Node) minSupportedVersion() version.Version {
	if bv := n.binaryVersion(); bv < version.MinSupported {
		// A simulated older binary supports only its own version window.
		return bv
	}
	return version.MinSupported
}

// readStoreClusterVersion loads the store-local mirror of the last observed
// cluster version (0 = never recorded, i.e. v1-era store).
func readStoreClusterVersion(eng *storage.Engine) (version.Version, error) {
	raw, err := eng.Get(keys.StoreClusterVersionKey())
	if err != nil || raw == nil {
		return 0, err
	}
	v, err := strconv.Atoi(string(raw))
	if err != nil {
		return 0, fmt.Errorf("corrupt store cluster version %q: %w", raw, err)
	}
	return version.Version(v), nil
}

// join contacts an existing node to obtain identity and routing bootstrap.
func (n *Node) join() error {
	req := cluster.JoinRequest{
		Address:       n.addr,
		Locality:      n.cfg.Locality,
		BinaryVersion: int(n.binaryVersion()),
		MinSupported:  int(n.minSupportedVersion()),
	}
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
	if cv := version.Version(resp.ClusterVersion); cv > 0 {
		n.clusterVersion.Store(int64(cv))
		if err := n.persistStoreClusterVersion([]byte(fmt.Sprintf("%d", int(cv)))); err != nil {
			return err
		}
		if cv >= version.V13 {
			// A fresh store joining a v13 cluster is split from the start.
			if err := n.activateSplitStore(); err != nil {
				return fmt.Errorf("activating the split store: %w", err)
			}
		}
		n.ratchetStoreFormat()
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

// EngineMode reports the store layout: "split" (the raft log on its own
// engine, the state engine without a WAL) or "single".
func (n *Node) EngineMode() string { return n.engineMode() }

// StoreFormat is the state engine's Pebble format major version.
func (n *Node) StoreFormat() int { return n.storeFormat() }

// StorePrefixBloom reports whether the state engine runs with the MVCC
// comparer and prefix bloom filters (cluster version v15, issue #161).
func (n *Node) StorePrefixBloom() bool { return n.storePrefixBloom() }

// PrefixBloomRewriteRemaining is the number of live sstables of the state
// engine still carrying whole-key filters (tests).
func (n *Node) PrefixBloomRewriteRemaining() int {
	if n.engine == nil {
		return 0
	}
	_, files, _ := n.engine.FilterRewriteStatus()
	return files
}

// RaftStoreFormat is the raft engine's Pebble format major version (0
// on a single-engine store).
func (n *Node) RaftStoreFormat() int {
	if n.raftEngine == nil {
		return 0
	}
	return n.raftEngine.Format()
}

// ClusterVersion is the finalized cluster version this node has observed.
func (n *Node) ClusterVersion() version.Version { return version.Version(n.clusterVersion.Load()) }

// Accessors used by tests, the CLI, and later phases.
func (n *Node) NodeID() base.NodeID { return n.ident.NodeID }
func (n *Node) Addr() string        { return n.addr }

// SQLAddr is the SQL listener address (real listener from Phase 6 on).
func (n *Node) SQLAddr() string {
	if n.sqlServer() == nil {
		return ""
	}
	return n.sqlServer().Addr()
}

// HTTPAddr is the observability listener address ("" when disabled).
func (n *Node) HTTPAddr() string            { return n.httpAddr }
func (n *Node) DB() *kvclient.DB            { return n.db }
func (n *Node) Store() *kvserver.Store      { return n.store }
func (n *Node) Clock() *hlc.Clock           { return n.clock }
func (n *Node) Stopper() *stop.Stopper      { return n.stopper }
func (n *Node) Registry() *cluster.Registry { return n.registry }

// InjectRPCDrop installs (or clears, with nil) a per-destination veto on
// this node's outbound RPC traffic — the fault-injection hook partition
// tests use. Never call in production.
func (n *Node) InjectRPCDrop(fn func(to base.NodeID) bool) { n.trans.SetTestingDrop(fn) }

// Pinger exposes the node's peer-latency measurements (tests and the
// cluster API).
func (n *Node) Pinger() *rpc.Pinger { return n.pinger }

// Events exposes the node's event ring (tests and the API).
func (n *Node) Events() *events.Ring { return n.events }

var profilingOnce sync.Once

// enableProfiling turns on the runtime's contention sampling at low
// rates (issue #100), once per process.
func enableProfiling(cfg Config) {
	profilingOnce.Do(func() {
		frac, rate := cfg.MutexProfileFraction, cfg.BlockProfileRate
		if frac == 0 {
			frac = 100
		}
		if rate == 0 {
			rate = 10 * time.Millisecond
		}
		if frac > 0 {
			runtime.SetMutexProfileFraction(frac)
		}
		if rate > 0 {
			runtime.SetBlockProfileRate(int(rate))
		}
	})
}

// ensureRoleCatalog runs the v11 role migration once the cluster is at
// v11: a cluster finalized by a node that then crashed mid-migration
// gets the rest of its records rewritten. Idempotent; range 1's leader
// runs it.
func (n *Node) ensureRoleCatalog(ctx context.Context) {
	delay := 500 * time.Millisecond
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(delay):
		}
		if n.readClusterVersion(ctx) < version.V11 {
			delay = 5 * time.Second
			continue
		}
		if r1, ok := n.store.GetReplica(1); !ok || !r1.IsLeader() {
			delay = time.Second
			continue
		}
		if moved, err := catalog.MigrateRoles(ctx, n.db); err != nil {
			log.Debugf("role catalog: %v (retrying)", err)
			delay = 2 * time.Second
			continue
		} else if moved > 0 {
			log.Infof("role migration: %d record(s) rewritten as role descriptors", moved)
		}
		return
	}
}

// ensureDatabaseCatalog runs the v6 catalog migration once the cluster is
// at v6: a freshly bootstrapped cluster gets its default and system
// databases, and a cluster finalized by a node that then crashed
// mid-migration gets the rest of its tables moved. Idempotent.
func (n *Node) ensureDatabaseCatalog(ctx context.Context) {
	delay := 500 * time.Millisecond
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(delay):
		}
		if n.readClusterVersion(ctx) < version.V6 {
			delay = 5 * time.Second
			continue
		}
		// One node at a time: range 1's leader (the cluster-singleton
		// idiom), so a fresh cluster's nodes do not race identical
		// transactions against each other.
		if r1, ok := n.store.GetReplica(1); !ok || !r1.IsLeader() {
			delay = time.Second
			continue
		}
		if moved, err := catalog.MigrateNamespace(ctx, n.db); err != nil {
			log.Debugf("database catalog: %v (retrying)", err)
			delay = 2 * time.Second
			continue
		} else if moved > 0 {
			log.Infof("catalog migration: %d table(s) moved under database %q", moved, catalog.DefaultDatabase)
		}
		return
	}
}
