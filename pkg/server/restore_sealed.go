package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"strings"

	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/keystore"
	"github.com/sthorne/datax/pkg/kvclient"
	"github.com/sthorne/datax/pkg/kvpb"
	"github.com/sthorne/datax/pkg/sql/catalog"
	"github.com/sthorne/datax/pkg/sql/rowenc"
	"github.com/sthorne/datax/pkg/sql/rowseal"
	"github.com/sthorne/datax/pkg/storage/enc"
)

// Table restore of sealed tables (issue #310, step 3). A sealed table's
// rows are copied as the backup holds them — sealed — under the table's
// new id; they open because their associated data names the table's seal
// id and key id, which the restored descriptor keeps
// (catalog.TableEncryptionKey), and the key itself must be one this
// cluster can use:
//
//   - a key this cluster has, not destroyed (the source table is still
//     here, or was restored before), is shared: the restored table names
//     the same key, and DROP TABLE destroys it only with the last table
//     that names it;
//   - a key this cluster lacks or has destroyed (the source table was
//     dropped after the backup) is brought back from the backup's
//     datax_keys, in the transaction that publishes the table
//     (keystore.Store.Reinstate) — which is the documented dependency on
//     backup retention at work: a backup taken before a key was destroyed
//     still holds it.
//
// A sealed table whose key is in neither place, or whose key the backup
// holds already destroyed, is refused before anything is written.
//
// A table sealed per tenant (step 4) has its naming key handled so, and
// also needs its tenants' keys: every tenant key of its seal id the
// backup holds, not destroyed. They are brought back the same way (a key
// this cluster has is shared, one it lacks or destroyed comes back from
// the backup), but in batches of tenantReinstateBatch keys per
// transaction before the publish — there may be millions, more than one
// transaction may write — with the restore's progress record naming the
// seal id so that the tenant key janitor leaves them alone. A tenant
// whose key was destroyed before the backup was taken has no key in it,
// and its rows in the backup stay unreadable: the erasure held.

// sealedRestore is what the restore of a plan's sealed tables needs.
type sealedRestore struct {
	keysDesc *catalog.TableDescriptor
	// records are the backup's datax_keys rows by key id: the last record
	// of each, as the chain leaves it.
	records map[int64]kvpb.KeyValue
	// ids are the keys the plan's sealed tables name, each once.
	ids []int64
	// tables names the first table naming each key, for messages.
	tables map[int64]string
	// tenantSeals are the seal ids of the plan's per-tenant tables, each
	// once, and tenantKeys the backup's records of their tenants' keys
	// (live in the backup), by key id.
	tenantSeals []uint64
	tenantKeys  []kvpb.KeyValue
	// tenantBrought counts the tenant keys brought back.
	tenantBrought int
}

// tenantSealIDs is sr's tenantSeals (nil for no plan).
func (sr *sealedRestore) tenantSealIDs() []uint64 {
	if sr == nil {
		return nil
	}
	return sr.tenantSeals
}

// tenantReinstateBatch is how many tenant keys one transaction of a
// table restore brings back: a few hundred kilobytes of datax_keys rows,
// far under what one transaction may write to a range.
const tenantReinstateBatch = 500

// backupKeyRecords reads the chain's datax_keys rows: the backup's
// descriptor of the table and, by key id, each key's last record.
func backupKeyRecords(chain *restoreChain) (*catalog.TableDescriptor, map[int64]kvpb.KeyValue, error) {
	var desc *catalog.TableDescriptor
	for _, t := range chain.final().Tables {
		if t.ID == catalog.KeysTableID {
			var d catalog.TableDescriptor
			if err := json.Unmarshal(t.Descriptor, &d); err != nil {
				return nil, nil, fmt.Errorf("the backup's %s descriptor: %w", catalog.KeysTableName, err)
			}
			desc = &d
		}
	}
	out := map[int64]kvpb.KeyValue{}
	if desc == nil {
		return nil, out, nil
	}
	for i, man := range chain.mans {
		for _, t := range man.Tables {
			if t.ID != catalog.KeysTableID {
				continue
			}
			path := filepath.Join(chain.srcs[i], backupDataFile(t.ID))
			err := readTableRecords(path, chain.dataKeys[i], t, chain.cutoffFor(i), false, func(rec kvpb.ExportRecord) error {
				pk, err := rowenc.DecodePK(desc, keys.Key(rec.Key))
				if err != nil || len(pk) != 1 {
					return nil // an index entry
				}
				if rec.Deleted {
					delete(out, pk[0].I)
					return nil
				}
				out[pk[0].I] = kvpb.KeyValue{Key: append(keys.Key(nil), rec.Key...), Value: append([]byte(nil), rec.Value...)}
				return nil
			})
			if err != nil {
				return nil, nil, fmt.Errorf("reading the backup's %s: %w", catalog.KeysTableName, err)
			}
		}
	}
	return desc, out, nil
}

// planSealedRestore checks, before anything is written, that every
// sealed table of the plan will have its key, and returns what the
// publishing transaction needs; nil when the plan has no sealed table.
func (n *Node) planSealedRestore(ctx context.Context, chain *restoreChain, plan *restorePlan) (*sealedRestore, error) {
	sr := &sealedRestore{tables: map[int64]string{}}
	for _, p := range plan.tables {
		if !p.srcDesc.Sealed() {
			continue
		}
		id := p.srcDesc.EncryptionKey.KeyID
		if _, seen := sr.tables[id]; !seen {
			sr.tables[id] = p.from()
			sr.ids = append(sr.ids, id)
			if p.srcDesc.SealedPerTenant() {
				sr.tenantSeals = append(sr.tenantSeals, p.srcDesc.EncryptionKey.SealID)
			}
		}
	}
	if len(sr.ids) == 0 {
		return nil, nil
	}
	var err error
	if sr.keysDesc, sr.records, err = backupKeyRecords(chain); err != nil {
		return nil, err
	}
	ks, err := n.KeyStore()
	if err != nil {
		return nil, err
	}
	for _, id := range sr.ids {
		k, err := ks.GetByID(ctx, id)
		if err == nil && k.State != keystore.StateDestroyed {
			continue // shared with the table it was taken from
		}
		if err != nil && !errors.Is(err, keystore.ErrNotFound) {
			return nil, err
		}
		rec, ok := sr.records[id]
		if !ok {
			return nil, fmt.Errorf("table %s is sealed, and its key (%d) is neither in this cluster nor in the backup: it cannot be restored", sr.tables[id], id)
		}
		if err := n.clusterKeyReady(ctx); err != nil {
			return nil, fmt.Errorf("table %s is sealed, and bringing its key back needs this cluster's key: %w", sr.tables[id], err)
		}
		r, err := keystore.RefOfRow(sr.keysDesc, keys.Key(rec.Key), rec.Value)
		if err != nil {
			return nil, err
		}
		if r.IsZero() {
			return nil, fmt.Errorf("table %s is sealed, and its key (%d) was already destroyed when the backup was taken: its rows cannot be read, so it is not restored", sr.tables[id], id)
		}
		kb, err := (nodeClusterKeys{n}).Key(ctx, r)
		enc.Zero(kb)
		if err != nil {
			return nil, fmt.Errorf("table %s is sealed, and its key is sealed under cluster key %s, which this cluster does not hold: export the source cluster's key there (`datax debug cluster-key export`) and import it here (`datax debug cluster-key import`) before restoring", sr.tables[id], r)
		}
	}
	if err := n.planTenantKeys(ctx, sr); err != nil {
		return nil, err
	}
	return sr, nil
}

// planTenantKeys picks the backup's records of the plan's per-tenant
// tables' tenant keys and checks this cluster can open each of them.
func (n *Node) planTenantKeys(ctx context.Context, sr *sealedRestore) error {
	if len(sr.tenantSeals) == 0 {
		return nil
	}
	prefixes := make([]string, len(sr.tenantSeals))
	for i, seal := range sr.tenantSeals {
		prefixes[i] = rowseal.TenantNamePrefix(seal)
	}
	refs := map[enc.ClusterKeyRef]bool{}
	for _, rec := range sr.records {
		k, err := keystore.KeyOfRecord(sr.keysDesc, keys.Key(rec.Key), rec.Value)
		if err != nil {
			return fmt.Errorf("the backup's record of a key: %w", err)
		}
		if k.Kind != keystore.KindTenant || k.State == keystore.StateDestroyed {
			continue
		}
		match := false
		for _, p := range prefixes {
			match = match || strings.HasPrefix(k.Name, p)
		}
		if !match {
			continue
		}
		sr.tenantKeys = append(sr.tenantKeys, rec)
		refs[k.ClusterKey] = true
	}
	if len(sr.tenantKeys) == 0 {
		return nil
	}
	if err := n.clusterKeyReady(ctx); err != nil {
		return fmt.Errorf("bringing back the tenant keys of a per-tenant table needs this cluster's key: %w", err)
	}
	for r := range refs {
		kb, err := (nodeClusterKeys{n}).Key(ctx, r)
		enc.Zero(kb)
		if err != nil {
			return fmt.Errorf("tenant keys in the backup are sealed under cluster key %s, which this cluster does not hold: export the source cluster's key there (`datax debug cluster-key export`) and import it here (`datax debug cluster-key import`) before restoring", r)
		}
	}
	return nil
}

// reinstateTenantKeys brings back every tenant key the plan's per-tenant
// tables need, tenantReinstateBatch keys per transaction; a key this
// cluster has (not destroyed) is left as it is.
func (n *Node) reinstateTenantKeys(ctx context.Context, sr *sealedRestore) error {
	if sr == nil || len(sr.tenantKeys) == 0 {
		return nil
	}
	ks, err := n.KeyStore()
	if err != nil {
		return err
	}
	for at := 0; at < len(sr.tenantKeys); at += tenantReinstateBatch {
		batch := sr.tenantKeys[at:min(at+tenantReinstateBatch, len(sr.tenantKeys))]
		var brought int
		if err := n.db.RunTxn(ctx, "restore-tenant-keys", func(ctx context.Context, txn *kvclient.Txn) error {
			brought = 0
			for _, rec := range batch {
				_, b, err := ks.Reinstate(ctx, txn, sr.keysDesc, keys.Key(rec.Key), rec.Value)
				if err != nil {
					return fmt.Errorf("bringing back a tenant key: %w", err)
				}
				if b {
					brought++
				}
			}
			return nil
		}); err != nil {
			return err
		}
		sr.tenantBrought += brought
	}
	return nil
}

// reinstateSealedKeys makes every key the plan's sealed tables name
// usable, in the publishing transaction, and says which it brought back.
func (n *Node) reinstateSealedKeys(ctx context.Context, txn *kvclient.Txn, sr *sealedRestore) ([]string, error) {
	if sr == nil {
		return nil, nil
	}
	ks, err := n.KeyStore()
	if err != nil {
		return nil, err
	}
	var notes []string
	if sr.tenantBrought > 0 {
		notes = append(notes, fmt.Sprintf("%d tenant keys brought back from the backup", sr.tenantBrought))
	}
	for _, id := range sr.ids {
		rec, ok := sr.records[id]
		if !ok {
			// Checked before the load to be live here; still so?
			k, err := ks.GetByID(ctx, id)
			if err != nil || k.State == keystore.StateDestroyed {
				return nil, fmt.Errorf("table %s is sealed, and its key (%d) went away during the restore", sr.tables[id], id)
			}
			continue
		}
		k, brought, err := ks.Reinstate(ctx, txn, sr.keysDesc, keys.Key(rec.Key), rec.Value)
		if err != nil {
			return nil, fmt.Errorf("table %s is sealed: %w", sr.tables[id], err)
		}
		if brought {
			notes = append(notes, fmt.Sprintf("%s: its key %d (%s) brought back from the backup", sr.tables[id], id, k.Name))
		}
	}
	return notes, nil
}
