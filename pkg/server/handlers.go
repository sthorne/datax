package server

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/sthorne/datax/pkg/cluster"
	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/kvpb"
	"github.com/sthorne/datax/pkg/util/log"
	"github.com/sthorne/datax/pkg/version"
)

// handleBatch serves incoming KV batches against the local store. Wire
// encoding is the rpc layer's concern.
func (n *Node) handleBatch(ctx context.Context, ba *kvpb.BatchRequest) (*kvpb.BatchResponse, *kvpb.Error) {
	return n.store.ExecuteBatch(ctx, ba)
}

// handleJoin admits a new node: allocates a node ID through the replicated
// counter, registers the node, and returns the routing bootstrap.
func (n *Node) handleJoin(ctx context.Context, data []byte) ([]byte, error) {
	var req cluster.JoinRequest
	if err := json.Unmarshal(data, &req); err != nil {
		return nil, err
	}
	resp := n.serveJoin(ctx, req)
	return json.Marshal(resp)
}

func (n *Node) serveJoin(ctx context.Context, req cluster.JoinRequest) cluster.JoinResponse {
	if req.NodeID != 0 {
		return n.serveReannounce(req)
	}
	// Version gate: the joiner's supported window must contain the
	// cluster's finalized version. Absent fields are a pre-versioning
	// binary, i.e. [1, 1].
	cv := n.readClusterVersion(ctx)
	jbv, jmin := version.Version(req.BinaryVersion), version.Version(req.MinSupported)
	if jbv == 0 {
		jbv = version.V1
	}
	if jmin == 0 {
		jmin = version.V1
	}
	if cv > jbv || cv < jmin {
		return cluster.JoinResponse{Error: fmt.Sprintf(
			"cluster is at version %s but the joining binary supports [%s, %s]: run a binary that supports %s",
			cv, jmin, jbv, cv)}
	}
	newID, err := n.db.Increment(ctx, keys.NodeIDGenKey(), 1)
	if err != nil {
		return cluster.JoinResponse{Error: err.Error()}
	}
	nd := kvpb.NodeDescriptor{
		NodeID:       kvNodeID(newID),
		Address:      req.Address,
		Locality:     req.Locality,
		LivenessTime: n.clock.Now().WallTime,
	}
	raw, err := json.Marshal(nd)
	if err != nil {
		return cluster.JoinResponse{Error: err.Error()}
	}
	if err := n.db.Put(ctx, keys.NodeRegistryKey(nd.NodeID), raw); err != nil {
		return cluster.JoinResponse{Error: err.Error()}
	}
	n.registry.Upsert(nd)

	resp := cluster.JoinResponse{
		ClusterID: n.ident.ClusterID,
		NodeID:    nd.NodeID,
		Nodes:     append(n.registry.All(), nd),
	}
	if r1, ok := n.store.GetReplica(1); ok {
		resp.Range1 = r1.Desc()
	} else if desc, ok := n.db.CachedDescriptor(keys.MinKey); ok {
		resp.Range1 = desc
	}
	log.Infof("node %s joined from %s (locality %s)", nd.NodeID, req.Address, req.Locality)
	return resp
}

// serveReannounce handles a join request from an already-initialized node
// (re)advertising its address after a restart. It deliberately performs no
// KV writes — only the in-memory registry is updated — so it works while
// quorum is still down (the whole-cluster-restart-on-new-addresses case);
// durable registry rows follow from the announcer's own heartbeat once
// range 1 has a leader again. The response's node list gives the announcer
// every fresh address this node has already learned from other announcers.
func (n *Node) serveReannounce(req cluster.JoinRequest) cluster.JoinResponse {
	if req.ClusterID != n.ident.ClusterID {
		return cluster.JoinResponse{Error: fmt.Sprintf(
			"re-announce from node %s of cluster %s, but this is cluster %s",
			req.NodeID, req.ClusterID, n.ident.ClusterID)}
	}
	// Advisory version check, deliberately KV-free (this path must work
	// with quorum down): if the announcer's window and this binary's
	// window are disjoint, the two binaries cannot serve one cluster.
	// The startup downgrade gate is the enforcing check.
	if req.BinaryVersion != 0 {
		abv, amin := version.Version(req.BinaryVersion), version.Version(req.MinSupported)
		if amin == 0 {
			amin = abv
		}
		if amin > n.binaryVersion() || abv < n.minSupportedVersion() {
			return cluster.JoinResponse{Error: fmt.Sprintf(
				"node %s runs versions [%s, %s] but this node runs [%s, %s]: version windows are disjoint",
				req.NodeID, amin, abv, n.minSupportedVersion(), n.binaryVersion())}
		}
	}
	n.registry.UpsertAddress(req.NodeID, req.Address)
	if err := cluster.PersistRegistry(n.engine, n.registry.All()); err != nil {
		log.Debugf("persisting registry: %v", err)
	}
	resp := cluster.JoinResponse{
		ClusterID: n.ident.ClusterID,
		NodeID:    req.NodeID,
		Nodes:     n.registry.All(),
	}
	if r1, ok := n.store.GetReplica(1); ok {
		resp.Range1 = r1.Desc()
	} else if desc, ok := n.db.CachedDescriptor(keys.MinKey); ok {
		resp.Range1 = desc
	}
	log.Infof("node %s re-announced at %s", req.NodeID, req.Address)
	return resp
}

// heartbeatLoop periodically writes this node's liveness into the registry
// keys (range 1) and refreshes the in-memory registry from them. The first
// beat runs immediately: a restarted node (possibly on a new address) must
// publish its row as soon as quorum allows, not a tick later.
func (n *Node) heartbeatLoop(ctx context.Context) {
	ticker := time.NewTicker(3 * time.Second)
	defer ticker.Stop()
	for first := true; ; first = false {
		if !first {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
		}
		hctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		// Adopt a Draining flag someone else wrote into our row (a
		// decommission can be initiated from any node) before overwriting
		// it; once adopted, this node re-asserts it on every beat.
		if cur, err := n.db.Get(hctx, keys.NodeRegistryKey(n.ident.NodeID)); err == nil && cur != nil {
			var prev kvpb.NodeDescriptor
			if json.Unmarshal(cur, &prev) == nil && prev.Draining {
				n.draining.Store(true)
			}
		}
		load := n.store.LoadSummary(loadAdvertiseTopK)
		nd := kvpb.NodeDescriptor{
			NodeID:        n.ident.NodeID,
			Address:       n.addr,
			Locality:      n.cfg.Locality,
			LivenessTime:  n.clock.Now().WallTime,
			Draining:      n.draining.Load(),
			BinaryVersion: int(n.binaryVersion()),
			LeaderQPS:     load.LeaderQPS,
			LeaderCount:   load.LeaderCount,
			ReplicaBytes:  load.ReplicaBytes,
			HotRanges:     load.HotRanges,
			BigRanges:     load.BigRanges,
			Machine:       n.machineSummary(),
		}
		if n.pinger != nil {
			nd.Latency = n.pinger.Snapshot()
		}
		if n.pgServer != nil {
			nd.SQL = n.pgServer.Activity().Summary()
		}
		raw, _ := json.Marshal(nd)
		if err := n.db.Put(hctx, keys.NodeRegistryKey(n.ident.NodeID), raw); err != nil {
			log.Debugf("liveness heartbeat failed: %v", err)
		}
		start, end := keys.NodeRegistrySpan()
		if rows, err := n.db.Scan(hctx, start, end, 0); err == nil {
			for _, kv := range rows {
				var other kvpb.NodeDescriptor
				if json.Unmarshal(kv.Value, &other) == nil {
					n.registry.Upsert(other)
				}
			}
			if err := cluster.PersistRegistry(n.engine, n.registry.All()); err != nil {
				log.Debugf("persisting registry: %v", err)
			}
			n.exportMetadata(hctx)
		} else {
			log.Debugf("%s registry scan failed: %v", n.ident.NodeID, err)
		}
		n.mirrorClusterVersion(hctx)
		cancel()
	}
}

// readClusterVersion returns the cluster's finalized protocol version: the
// freshest of the replicated row and this node's last observed value (the
// version only ever moves forward). Missing everywhere = version 1.
func (n *Node) readClusterVersion(ctx context.Context) version.Version {
	cv := version.Version(n.clusterVersion.Load())
	if raw, err := n.db.Get(ctx, keys.ClusterVersionKey()); err == nil && raw != nil {
		if v, aerr := strconv.Atoi(string(raw)); aerr == nil && version.Version(v) > cv {
			cv = version.Version(v)
		}
	}
	if cv == 0 {
		cv = version.V1
	}
	return cv
}

// mirrorClusterVersion refreshes the in-memory cluster version from the
// replicated row and persists a store-local copy when it advances, so the
// startup downgrade gate can read it before quorum.
func (n *Node) mirrorClusterVersion(ctx context.Context) {
	raw, err := n.db.Get(ctx, keys.ClusterVersionKey())
	if err != nil || raw == nil {
		return
	}
	v, err := strconv.Atoi(string(raw))
	if err != nil {
		log.Warnf("corrupt cluster version row %q: %v", raw, err)
		return
	}
	if int64(v) <= n.clusterVersion.Load() {
		return
	}
	n.clusterVersion.Store(int64(v))
	if err := n.engine.Put(keys.StoreClusterVersionKey(), raw); err != nil {
		log.Warnf("persisting store cluster version: %v", err)
	}
	log.Infof("cluster version is now %s", version.Version(v))
}

// machineSummary condenses the latest host sample for the heartbeat.
func (n *Node) machineSummary() *kvpb.MachineSummary {
	if n.sys == nil {
		return nil
	}
	m := n.sys.Latest()
	if m.At.IsZero() {
		return nil
	}
	return &kvpb.MachineSummary{
		CPUPercent:    m.CPUPercent,
		Load1:         m.Load1,
		Cores:         m.Cores,
		MemTotal:      m.MemTotal,
		MemAvailable:  m.MemAvailable,
		RSS:           m.RSS,
		DiskTotal:     m.DiskTotal,
		DiskFree:      m.DiskFree,
		OpenFDs:       m.OpenFDs,
		FDLimit:       m.FDLimit,
		UptimeSeconds: m.ProcessUp,
	}
}
