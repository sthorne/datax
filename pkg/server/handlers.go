package server

import (
	"context"
	"encoding/json"
	"time"

	"github.com/sthorne/datax/pkg/cluster"
	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/kvpb"
	"github.com/sthorne/datax/pkg/rpc"
	"github.com/sthorne/datax/pkg/util/log"
)

// handleBatch serves incoming KV batches against the local store.
func (n *Node) handleBatch(ctx context.Context, data []byte) ([]byte, error) {
	var ba kvpb.BatchRequest
	if err := json.Unmarshal(data, &ba); err != nil {
		return nil, err
	}
	br, kerr := n.store.ExecuteBatch(ctx, &ba)
	return rpc.MarshalBatchResult(br, kerr)
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

// heartbeatLoop periodically writes this node's liveness into the registry
// keys (range 1) and refreshes the in-memory registry from them.
func (n *Node) heartbeatLoop(ctx context.Context) {
	ticker := time.NewTicker(3 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		hctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		nd := kvpb.NodeDescriptor{
			NodeID:       n.ident.NodeID,
			Address:      n.addr,
			Locality:     n.cfg.Locality,
			LivenessTime: n.clock.Now().WallTime,
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
