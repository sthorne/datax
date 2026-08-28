package server

import (
	"context"
	"encoding/json"

	"github.com/sthorne/datax/pkg/cluster"
	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/kvpb"
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
		return cluster.AdminResponse{Nodes: n.registry.All()}

	case "rebalance":
		return n.serveRebalance(ctx, req)

	default:
		return cluster.AdminResponse{Error: "unknown admin op " + req.Op}
	}
}

// listRanges reads all range descriptors from the /meta records.
func (n *Node) listRanges(ctx context.Context) ([]kvpb.RangeDescriptor, error) {
	start, end := keys.MetaSpan()
	ba := &kvpb.BatchRequest{Header: kvpb.BatchHeader{Timestamp: n.clock.Now(), ReadInconsistent: true}}
	ba.Add(&kvpb.ScanRequest{RequestHeader: kvpb.RequestHeader{Key: start, EndKey: end}})
	br, kerr := n.db.Send(ctx, ba)
	if kerr != nil {
		return nil, kerr
	}
	var out []kvpb.RangeDescriptor
	for _, kv := range br.Responses[0].Scan.Rows {
		var d kvpb.RangeDescriptor
		if err := json.Unmarshal(kv.Value, &d); err == nil {
			out = append(out, d)
		}
	}
	return out, nil
}
