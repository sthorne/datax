package server

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/sthorne/datax/pkg/cluster"
	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/kvpb"
	"github.com/sthorne/datax/pkg/util/log"
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
		nd := kvpb.NodeDescriptor{
			NodeID:       n.ident.NodeID,
			Address:      n.addr,
			Locality:     n.cfg.Locality,
			LivenessTime: n.clock.Now().WallTime,
			Draining:     n.draining.Load(),
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
		cancel()
	}
}
