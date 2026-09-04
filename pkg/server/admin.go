package server

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/sthorne/datax/pkg/base"
	"github.com/sthorne/datax/pkg/cluster"
	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/kvpb"
	"github.com/sthorne/datax/pkg/metrics"
	"github.com/sthorne/datax/pkg/rpc"
	"github.com/sthorne/datax/pkg/security"
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

// adminReadOnlyOps are admin ops any authenticated principal may run.
// Everything else changes cluster state or exposes per-replica internals:
// admin role required.
var adminReadOnlyOps = map[string]bool{
	"ranges":           true,
	"nodes":            true,
	"collect-checksum": true,
	"wait-applied":     true,
	"reencrypt-status": true,
}

// adminUnauditedOps skip the audit log: the read-only ops (collect-checksum
// fires on every consistency sweep) and node-status, which /api/range fans
// out to every replica holder under the node identity on each dashboard
// drill-down. node-status is admin-only like /api/range, whose data
// source it is; it is just not worth a record per fan-out.
var adminUnauditedOps = map[string]bool{
	"ranges":           true,
	"nodes":            true,
	"collect-checksum": true,
	"wait-applied":     true,
	"reencrypt-status": true,
	"node-status":      true,
}

// isAdminPrincipal reports whether an authenticated identity carries
// admin authority: the cluster's own certificate ("node" — internode
// forwarding and probes act with operator authority), root, or an
// admin-role member (the same rule the SQL layer applies).
func (n *Node) isAdminPrincipal(ctx context.Context, cn string) bool {
	if cn == security.NodePrincipal || cn == "root" {
		return true
	}
	if cn == "" {
		return false
	}
	v, err := n.db.Get(ctx, keys.AdminUserKey(cn))
	return err == nil && v != nil
}

func (n *Node) serveAdmin(ctx context.Context, req cluster.AdminRequest) cluster.AdminResponse {
	// In secure mode the caller's identity is its CA-verified client
	// certificate (mutual TLS is mandatory on this port); state-changing
	// ops require the admin role and every one is audited with the
	// principal. Insecure mode has no identities to check, matching
	// pgwire's trust semantics.
	principal := "(insecure)"
	if n.tlsCfgs != nil {
		cn := rpc.PeerCN(ctx)
		principal = cn
		if !adminReadOnlyOps[req.Op] && !n.isAdminPrincipal(ctx, cn) {
			metrics.AdminDenied.Inc()
			log.Audit("admin-denied", "op", req.Op, "principal", cn)
			return cluster.AdminResponse{Error: fmt.Sprintf(
				"permission denied: admin operation %q requires the admin role (connected as %q)", req.Op, cn)}
		}
	}
	if adminUnauditedOps[req.Op] {
		return n.serveAdminOp(ctx, req)
	}
	resp := n.serveAdminOp(ctx, req)
	// Audited after the fact so the record carries the outcome and the
	// target: an operator tracing a rejected op, or a restore from the
	// wrong path, can tell what actually happened. Store-key material
	// (rotate-store-key) is never logged.
	kv := []any{"op", req.Op, "principal", principal, "range", int64(req.RangeID), "node", int64(req.NodeID)}
	if req.ToNode != 0 {
		kv = append(kv, "to_node", int64(req.ToNode))
	}
	if req.FromNode != 0 {
		kv = append(kv, "from_node", int64(req.FromNode))
	}
	if req.Cancel {
		kv = append(kv, "cancel", true)
	}
	if len(req.Key) != 0 {
		kv = append(kv, "key", fmt.Sprintf("%q", req.Key))
	}
	if req.Path != "" {
		kv = append(kv, "path", req.Path)
	}
	if req.BasePath != "" {
		kv = append(kv, "base_path", req.BasePath)
	}
	if len(req.Paths) != 0 {
		kv = append(kv, "paths", strings.Join(req.Paths, ","))
	}
	if req.Version != 0 {
		kv = append(kv, "version", int64(req.Version))
	}
	if resp.Error != "" {
		kv = append(kv, "outcome", "error", "error", resp.Error)
	} else {
		kv = append(kv, "outcome", "ok")
	}
	log.Audit("admin-op", kv...)
	return resp
}

// serveAdminOp dispatches an admin op the caller is already authorized for.
func (n *Node) serveAdminOp(ctx context.Context, req cluster.AdminRequest) cluster.AdminResponse {
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
		sum, err := n.RunBackup(ctx, req.Path, req.BasePath, req.AllowPlaintext, req.IncludeMetrics)
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

	case "wait-applied":
		// The merge driver's pre-proposal check (see kvserver.adminMerge):
		// block until this node's replica has applied req.Index, bounded
		// here as well as by the caller's deadline.
		if req.RangeID == 0 {
			return cluster.AdminResponse{Error: "wait-applied requires a range"}
		}
		wctx, cancel := context.WithTimeout(ctx, 15*time.Second)
		defer cancel()
		idx, err := n.store.WaitForApplied(wctx, req.RangeID, req.Index)
		if err != nil {
			return cluster.AdminResponse{Error: err.Error()}
		}
		return cluster.AdminResponse{AppliedIndex: idx}

	case "rotate-store-key":
		return n.serveRotateStoreKey(ctx, req)

	case "reencrypt":
		return n.serveReencrypt(req)

	case "reencrypt-status":
		if n.engine == nil || !n.engine.Encrypted() {
			return cluster.AdminResponse{Error: "store is not encrypted"}
		}
		return cluster.AdminResponse{Reencryption: n.reencryptionStatus()}

	case "node-status":
		// This node's /status document, optionally filtered to one range —
		// the cross-node drill-down's data source (/api/range fans this out
		// to every replica holder). During a mixed-version roll an old
		// receiver answers "unknown admin op", which the caller surfaces as
		// that replica's error rather than failing the whole document.
		st := n.statusSummary()
		if req.RangeID != 0 {
			var only []RangeStatus
			for _, rs := range st.Ranges {
				if rs.RangeID == int64(req.RangeID) {
					only = append(only, rs)
				}
			}
			st.Ranges = only
		}
		raw, err := json.Marshal(st)
		if err != nil {
			return cluster.AdminResponse{Error: err.Error()}
		}
		return cluster.AdminResponse{Status: raw}

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
	n.events.Record("upgrade", "cluster version finalized at %s", target)
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
