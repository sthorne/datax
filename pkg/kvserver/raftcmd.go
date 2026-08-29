package kvserver

import (
	"encoding/json"
	"fmt"

	"google.golang.org/protobuf/proto"

	"github.com/sthorne/datax/pkg/kvpb"
	"github.com/sthorne/datax/pkg/rpc/rpcpb"
	"github.com/sthorne/datax/pkg/util/hlc"
)

func tsToProto(t hlc.Timestamp) *rpcpb.Hlc {
	if t.IsEmpty() {
		return nil
	}
	return &rpcpb.Hlc{WallTime: t.WallTime, Logical: t.Logical}
}

func tsFromProto(p *rpcpb.Hlc) hlc.Timestamp {
	if p == nil {
		return hlc.Timestamp{}
	}
	return hlc.Timestamp{WallTime: p.WallTime, Logical: p.Logical}
}

// Raft log entry payload format. Entries written before the binary switch
// (issue #8) are bare JSON objects, which always begin with '{'; new
// entries carry a one-byte format version followed by a proto RaftCommand.
// The raft log is persistent state, so decode accepts both forever: a
// restarted node replays its pre-upgrade log tail. (Same shape as rowenc's
// value versioning.) Mixed-version CLUSTERS are a documented coordinated
// break: an old binary cannot decode new entries.
const raftCommandVersionProto = 0x01

// encodeRaftCommand serializes a command for proposal.
func encodeRaftCommand(cmd *raftCommand) ([]byte, error) {
	pb := &rpcpb.RaftCommand{
		Id:    cmd.ID,
		Batch: kvpb.BatchRequestToProto(&cmd.Batch),
	}
	if cmd.Split != nil {
		pb.Split = &rpcpb.SplitTrigger{
			Left:  kvpb.RangeDescriptorToProto(cmd.Split.Left),
			Right: kvpb.RangeDescriptorToProto(cmd.Split.Right),
		}
	}
	if cmd.Merge != nil {
		pb.Merge = &rpcpb.MergeTrigger{
			Left:              kvpb.RangeDescriptorToProto(cmd.Merge.Left),
			Right:             kvpb.RangeDescriptorToProto(cmd.Merge.Right),
			Merged:            kvpb.RangeDescriptorToProto(cmd.Merge.Merged),
			RightAppliedIndex: cmd.Merge.RightAppliedIndex,
			RightSizeBytes:    cmd.Merge.RightSizeBytes,
			RightGcThreshold:  tsToProto(cmd.Merge.RightGCThreshold),
		}
	}
	raw, err := proto.Marshal(pb)
	if err != nil {
		return nil, err
	}
	out := make([]byte, 1+len(raw))
	out[0] = raftCommandVersionProto
	copy(out[1:], raw)
	return out, nil
}

// decodeRaftCommand deserializes an applied entry's payload, accepting the
// legacy JSON format for entries persisted before the switch.
func decodeRaftCommand(data []byte) (*raftCommand, error) {
	if len(data) == 0 {
		return nil, fmt.Errorf("empty raft command")
	}
	if data[0] == '{' {
		var cmd raftCommand
		if err := json.Unmarshal(data, &cmd); err != nil {
			return nil, err
		}
		return &cmd, nil
	}
	if data[0] != raftCommandVersionProto {
		return nil, fmt.Errorf("unknown raft command format byte 0x%02x", data[0])
	}
	var pb rpcpb.RaftCommand
	if err := proto.Unmarshal(data[1:], &pb); err != nil {
		return nil, err
	}
	batch, err := kvpb.BatchRequestFromProto(pb.Batch)
	if err != nil {
		return nil, err
	}
	cmd := &raftCommand{ID: pb.Id, Batch: *batch}
	if pb.Split != nil {
		cmd.Split = &splitTrigger{
			Left:  kvpb.RangeDescriptorFromProto(pb.Split.Left),
			Right: kvpb.RangeDescriptorFromProto(pb.Split.Right),
		}
	}
	if pb.Merge != nil {
		cmd.Merge = &mergeTrigger{
			Left:              kvpb.RangeDescriptorFromProto(pb.Merge.Left),
			Right:             kvpb.RangeDescriptorFromProto(pb.Merge.Right),
			Merged:            kvpb.RangeDescriptorFromProto(pb.Merge.Merged),
			RightAppliedIndex: pb.Merge.RightAppliedIndex,
			RightSizeBytes:    pb.Merge.RightSizeBytes,
			RightGCThreshold:  tsFromProto(pb.Merge.RightGcThreshold),
		}
	}
	return cmd, nil
}
