package server

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/sthorne/datax/pkg/base"
	"github.com/sthorne/datax/pkg/cluster"
	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/kvpb"
	"github.com/sthorne/datax/pkg/util/log"
	"github.com/sthorne/datax/pkg/version"
)

// handleAdmin serves cluster admin RPCs (datax debug ...).
func (n *Node) handleAdmin(ctx context.Context, data []byte) ([]byte, error) {
	var req cluster.AdminRequest
	if err := json.Unmarshal(data, &req); err != nil {
		return nil, err
	}
	resp := n.serveAdmin(ctx, req)
	return json.Marshal(resp)
}

func (n *Node) serveAdmin(ctx context.Context, req cluster.AdminRequest) cluster.AdminResponse {
	switch req.Op {
	case "split":
		if len(req.Key) == 0 {
			return cluster.AdminResponse{Error: "split requires a key"}
		}
		sr, err := n.db.AdminSplit(ctx, keys.Key(req.Key))
		if err != nil {
			return cluster.AdminResponse{Error: err.Error()}
		}
		return cluster.AdminResponse{Left: &sr.Left, Right: &sr.Right}

	case "ranges":
		descs, err := n.listRanges(ctx)
		if err != nil {
			return cluster.AdminResponse{Error: err.Error()}
		}
		return cluster.AdminResponse{Ranges: descs}

	case "nodes":
		return cluster.AdminResponse{
			Nodes:          n.registry.All(),
			ClusterVersion: int(n.readClusterVersion(ctx)),
		}

	case "upgrade-cluster":
		return n.serveUpgradeCluster(ctx, req)

	case "rebalance":
		return n.serveRebalance(ctx, req)

	case "transfer-lease":
		return n.serveTransferLease(ctx, req)

	case "decommission":
		return n.serveDecommission(ctx, req)

	case "backup":
		if req.Path == "" {
			return cluster.AdminResponse{Error: "backup requires a destination path"}
		}
		sum, err := n.RunBackup(ctx, req.Path, req.BasePath, req.AllowPlaintext)
		if err != nil {
			return cluster.AdminResponse{Error: err.Error()}
		}
		return cluster.AdminResponse{Backup: sum}

	case "restore":
		sum, err := n.RunRestore(ctx, req.Paths)
		if err != nil {
			return cluster.AdminResponse{Error: err.Error()}
		}
		return cluster.AdminResponse{Backup: sum}

	case "collect-checksum":
		if req.RangeID == 0 || req.CheckID == "" {
			return cluster.AdminResponse{Error: "collect-checksum requires a range and check ID"}
		}
		sum, idx, ok := n.store.LookupChecksum(ctx, req.RangeID, req.CheckID)
		if !ok {
			return cluster.AdminResponse{Error: "checksum not available"}
		}
		return cluster.AdminResponse{Checksum: sum, AppliedIndex: idx}

	case "merge":
		if req.RangeID == 0 {
			return cluster.AdminResponse{Error: "merge requires --range"}
		}
		desc, err := n.findRange(ctx, req.RangeID)
		if err != nil {
			return cluster.AdminResponse{Error: err.Error()}
		}
		mr, err := n.db.AdminMerge(ctx, desc.StartKey)
		if err != nil {
			return cluster.AdminResponse{Error: err.Error()}
		}
		return cluster.AdminResponse{Ranges: []kvpb.RangeDescriptor{mr.Desc}}

	default:
		return cluster.AdminResponse{Error: "unknown admin op " + req.Op}
	}
}

// serveUpgradeCluster finalizes a cluster upgrade: once every live,
// non-draining node runs a binary at or above the target version, the
// replicated cluster version advances to it. The version only ever moves
// forward; repeating the op is idempotent.
func (n *Node) serveUpgradeCluster(ctx context.Context, req cluster.AdminRequest) cluster.AdminResponse {
	target := version.Version(req.Version)
	if target == 0 {
		target = n.binaryVersion()
	}
	if target > n.binaryVersion() {
		return cluster.AdminResponse{Error: fmt.Sprintf(
			"cannot upgrade to %s: this node's binary supports at most %s", target, n.binaryVersion())}
	}
	var stragglers []string
	for _, nd := range n.liveNodes() {
		if nd.Draining {
			continue
		}
		bv := version.Version(nd.BinaryVersion)
		if nd.NodeID == n.ident.NodeID {
			// Our own registry row may lag a heartbeat; we know our binary.
			bv = n.binaryVersion()
		} else if bv == 0 {
			bv = version.V1
		}
		if bv < target {
			stragglers = append(stragglers, fmt.Sprintf("%s (%s)", nd.NodeID, bv))
		}
	}
	if len(stragglers) > 0 {
		sort.Strings(stragglers)
		return cluster.AdminResponse{Error: fmt.Sprintf(
			"cannot finalize %s: live nodes still run older binaries: %s",
			target, strings.Join(stragglers, ", "))}
	}
	cur := n.readClusterVersion(ctx)
	if cur >= target {
		return cluster.AdminResponse{ClusterVersion: int(cur)}
	}
	if err := n.db.Put(ctx, keys.ClusterVersionKey(), []byte(strconv.Itoa(int(target)))); err != nil {
		return cluster.AdminResponse{Error: err.Error()}
	}
	n.mirrorClusterVersion(ctx)
	log.Infof("cluster version finalized at %s", target)
	return cluster.AdminResponse{ClusterVersion: int(target)}
}

// listRanges reads all range descriptors from the /meta records. The
// inconsistent scan can briefly surface two records for one range (a split
// or merge whose meta repair has not landed), so duplicates are collapsed
// to the highest generation.
func (n *Node) listRanges(ctx context.Context) ([]kvpb.RangeDescriptor, error) {
	start, end := keys.MetaSpan()
	ba := &kvpb.BatchRequest{Header: kvpb.BatchHeader{Timestamp: n.clock.Now(), ReadInconsistent: true}}
	ba.Add(&kvpb.ScanRequest{RequestHeader: kvpb.RequestHeader{Key: start, EndKey: end}})
	br, kerr := n.db.Send(ctx, ba)
	if kerr != nil {
		return nil, kerr
	}
	byID := map[base.RangeID]kvpb.RangeDescriptor{}
	var order []base.RangeID
	for _, kv := range br.Responses[0].Scan.Rows {
		var d kvpb.RangeDescriptor
		if err := json.Unmarshal(kv.Value, &d); err != nil {
			continue
		}
		if cur, ok := byID[d.RangeID]; !ok {
			byID[d.RangeID] = d
			order = append(order, d.RangeID)
		} else if d.Generation > cur.Generation {
			byID[d.RangeID] = d
		}
	}
	out := make([]kvpb.RangeDescriptor, 0, len(order))
	for _, id := range order {
		out = append(out, byID[id])
	}
	return out, nil
}
