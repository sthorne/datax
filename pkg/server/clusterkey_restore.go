package server

import (
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"sort"
	"strings"

	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/keystore"
	"github.com/sthorne/datax/pkg/kvpb"
	"github.com/sthorne/datax/pkg/sql/catalog"
	"github.com/sthorne/datax/pkg/storage/enc"
)

// Whole-cluster restore and the named keys (issue #310). A backup carries
// datax_keys as it is stored: each key's material sealed by the source
// cluster's key. Restoring it is useful only where that key can be
// opened, so a restore that brings named keys back first checks that
// this cluster holds every cluster key they are sealed under — its own,
// or one imported from the source (`datax debug cluster-key import`) —
// and refuses before applying anything if not. After the data is in, the
// restored keys are rewrapped under this cluster's current key.

// restoreKeyRefs lists the cluster keys the datax_keys rows of a restore
// chain are sealed under, from the backup files.
func restoreKeyRefs(chain *restoreChain) (map[enc.ClusterKeyRef]int, error) {
	out := map[enc.ClusterKeyRef]int{}
	final := chain.final()
	var desc *catalog.TableDescriptor
	for _, t := range final.Tables {
		if t.ID == catalog.KeysTableID {
			var d catalog.TableDescriptor
			if err := json.Unmarshal(t.Descriptor, &d); err != nil {
				return nil, fmt.Errorf("the backup's %s descriptor: %w", catalog.KeysTableName, err)
			}
			desc = &d
		}
	}
	if desc == nil {
		return out, nil
	}
	// Later elements of the chain overwrite earlier ones key by key, so
	// the last record of a key is what the restore leaves.
	last := map[string]enc.ClusterKeyRef{}
	for i, man := range chain.mans {
		for _, t := range man.Tables {
			if t.ID != catalog.KeysTableID {
				continue
			}
			path := filepath.Join(chain.srcs[i], backupDataFile(t.ID))
			err := readTableRecords(path, chain.dataKeys[i], t, chain.cutoffFor(i), false, func(rec kvpb.ExportRecord) error {
				if rec.Deleted {
					delete(last, string(rec.Key))
					return nil
				}
				ref, err := keystore.RefOfRow(desc, keys.Key(rec.Key), rec.Value)
				if err != nil {
					return err
				}
				last[string(rec.Key)] = ref
				return nil
			})
			if err != nil {
				return nil, fmt.Errorf("reading the backup's %s: %w", catalog.KeysTableName, err)
			}
		}
	}
	for _, ref := range last {
		if !ref.IsZero() {
			out[ref]++
		}
	}
	return out, nil
}

// checkRestoreClusterKeys refuses a restore whose named keys this cluster
// could not open.
func (n *Node) checkRestoreClusterKeys(ctx context.Context, chain *restoreChain) error {
	refs, err := restoreKeyRefs(chain)
	if err != nil || len(refs) == 0 {
		return err
	}
	var names []string
	for r, c := range refs {
		names = append(names, fmt.Sprintf("%s (%d key(s))", r, c))
	}
	sort.Strings(names)
	m, err := n.readCKMarker(ctx)
	if err != nil {
		return err
	}
	if m == nil {
		return fmt.Errorf("the backup holds named keys sealed under cluster key %s, and this cluster's key is not enabled: enable it (`datax debug cluster-key init`), import the source cluster's key (`datax debug cluster-key export` there, `import` here), then restore", strings.Join(names, ", "))
	}
	var missing []string
	for r := range refs {
		c, ok := m.find(r)
		k, held := n.heldClusterKeyCopy(r)
		good := ok && held && enc.CheckClusterKey(k, r, c.Check)
		enc.Zero(k)
		if !good {
			missing = append(missing, r.String())
		}
	}
	if len(missing) > 0 {
		sort.Strings(missing)
		return fmt.Errorf("the backup holds named keys sealed under cluster key %s, which this cluster does not hold: export the source cluster's key there (`datax debug cluster-key export`) and import it here (`datax debug cluster-key import`) before restoring", strings.Join(missing, ", "))
	}
	return nil
}

// rewrapRestoredKeys seals the restored named keys under this cluster's
// current key and says what it did.
func (n *Node) rewrapRestoredKeys(ctx context.Context, chain *restoreChain) ([]string, error) {
	restored := false
	for _, t := range chain.final().Tables {
		restored = restored || t.ID == catalog.KeysTableID
	}
	if !restored {
		return nil, nil
	}
	if m, err := n.readCKMarker(ctx); err != nil || m == nil {
		return nil, err
	}
	ks, err := n.KeyStore()
	if err != nil {
		return nil, err
	}
	res, err := ks.RewrapAll(ctx, true)
	if err != nil {
		return nil, fmt.Errorf("the data is restored, but rewrapping the restored named keys under this cluster's key failed: %w", err)
	}
	if res.Rewrapped == 0 {
		return nil, nil
	}
	return []string{fmt.Sprintf("%d named key(s) rewrapped under this cluster's key", res.Rewrapped)}, nil
}
