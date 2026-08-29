package server

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/sthorne/datax/pkg/base"
	"github.com/sthorne/datax/pkg/cluster"
	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/kvpb"
	"github.com/sthorne/datax/pkg/kvserver"
	"github.com/sthorne/datax/pkg/storage"
	"github.com/sthorne/datax/pkg/util/encoding"
	"github.com/sthorne/datax/pkg/util/log"
)

// Disaster tooling for range quorum loss (most critically range 1, whose
// loss takes cluster metadata — /meta addressing, descriptors, users — with
// it): a periodic local metadata export turns "bricked" into "recoverable",
// and UnsafeRecover rewrites a surviving store's descriptors to
// single-replica membership so it can serve again.

// MetadataBackupFile is the export's filename inside the data directory.
const MetadataBackupFile = "metadata-backup.json"

// MetadataBackup is the periodically exported cluster metadata snapshot.
type MetadataBackup struct {
	CapturedAt time.Time                  `json:"captured_at"`
	NodeID     base.NodeID                `json:"node_id"`
	Ranges     []kvpb.RangeDescriptor     `json:"ranges"`    // decoded /meta records
	Tables     []json.RawMessage          `json:"tables"`    // raw table descriptors
	Namespace  map[string]string          `json:"namespace"` // table name → id
	Users      map[string]json.RawMessage `json:"users"`     // name → verifier (hashed; no plaintext)
	Nodes      []kvpb.NodeDescriptor      `json:"nodes"`     // registry rows
}

// exportMetadata writes the current cluster metadata to the data directory
// (best-effort; failures are logged at debug and retried next heartbeat).
func (n *Node) exportMetadata(ctx context.Context) {
	if n.cfg.Dir == "" {
		return
	}
	bak := MetadataBackup{CapturedAt: time.Now(), NodeID: n.ident.NodeID, Namespace: map[string]string{}, Users: map[string]json.RawMessage{}}

	mLo, mHi := keys.MetaSpan()
	rows, err := n.db.Scan(ctx, mLo, mHi, 0)
	if err != nil {
		log.Debugf("metadata export: meta scan: %v", err)
		return
	}
	for _, kv := range rows {
		var d kvpb.RangeDescriptor
		if json.Unmarshal(kv.Value, &d) == nil {
			bak.Ranges = append(bak.Ranges, d)
		}
	}
	tLo, tHi := keys.TableDescSpan()
	if rows, err = n.db.Scan(ctx, tLo, tHi, 0); err != nil {
		log.Debugf("metadata export: descriptor scan: %v", err)
		return
	}
	for _, kv := range rows {
		bak.Tables = append(bak.Tables, json.RawMessage(kv.Value))
	}
	nsLo, nsHi := keys.NamespaceSpan()
	if rows, err = n.db.Scan(ctx, nsLo, nsHi, 0); err == nil {
		for _, kv := range rows {
			if _, name, derr := encoding.DecodeString(kv.Key[len(nsLo):]); derr == nil {
				bak.Namespace[name] = string(kv.Value)
			}
		}
	}
	uLo, uHi := keys.UserSpan()
	if rows, err = n.db.Scan(ctx, uLo, uHi, 0); err == nil {
		for _, kv := range rows {
			if _, name, derr := encoding.DecodeString(kv.Key[len(uLo):]); derr == nil {
				bak.Users[name] = json.RawMessage(kv.Value)
			}
		}
	}
	bak.Nodes = n.registry.All()

	raw, err := json.MarshalIndent(&bak, "", "  ")
	if err != nil {
		return
	}
	tmp := filepath.Join(n.cfg.Dir, MetadataBackupFile+".tmp")
	final := filepath.Join(n.cfg.Dir, MetadataBackupFile)
	if err := os.WriteFile(tmp, raw, 0o600); err != nil {
		log.Debugf("metadata export: %v", err)
		return
	}
	if err := os.Rename(tmp, final); err != nil {
		log.Debugf("metadata export: %v", err)
	}
}

// UnsafeRecover rewrites a stopped store's range descriptors to
// single-replica membership (this store's own replica), so ranges that lost
// quorum can elect themselves and serve again. rangeID 0 recovers every
// range on the store.
//
// THIS DISCARDS THE OTHER REPLICAS' VOTES AND ANY WRITES ONLY THEY
// ACKNOWLEDGED. Run it on exactly ONE survivor per range, with the node
// stopped, and never restart the removed peers with their old data — wipe
// them and rejoin fresh. Replication is restored by upreplication once new
// nodes join.
func UnsafeRecover(dir string, rangeID base.RangeID) ([]kvpb.RangeDescriptor, error) {
	eng, err := storage.Open(dir)
	if err != nil {
		return nil, fmt.Errorf("opening store (is the node stopped?): %w", err)
	}
	defer func() { _ = eng.Close() }()

	ident, initialized, err := cluster.ReadStoreIdent(eng)
	if err != nil {
		return nil, err
	}
	if !initialized {
		return nil, fmt.Errorf("store %s is not an initialized datax store", dir)
	}
	descs, err := kvserver.LoadLocalRangeDescriptors(eng)
	if err != nil {
		return nil, err
	}

	b := eng.NewBatch()
	var out []kvpb.RangeDescriptor
	for _, d := range descs {
		if rangeID != 0 && d.RangeID != rangeID {
			continue
		}
		rep, ok := d.GetReplica(ident.NodeID)
		if !ok {
			continue // descriptor present but this node is not a member
		}
		if len(d.Replicas) == 1 {
			continue // already solo
		}
		d.Replicas = []kvpb.ReplicaDescriptor{rep}
		d.Generation++
		if err := kvserver.PutRangeDescriptor(b, d); err != nil {
			_ = b.Close()
			return nil, err
		}
		out = append(out, d)
	}
	if len(out) == 0 {
		_ = b.Close()
		return nil, nil
	}
	if err := b.Commit(true); err != nil {
		return nil, err
	}
	return out, nil
}
