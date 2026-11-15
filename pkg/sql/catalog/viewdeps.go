package catalog

import (
	"context"
	"encoding/json"

	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/kvclient"
	"github.com/sthorne/datax/pkg/util/encoding"
)

// The views reading a relation (issue #313). A view names what its query
// reads in its own descriptor (TableDescriptor.ViewDepends); what DROP,
// RENAME and DROP COLUMN need is the reverse — which views read this
// relation — and finding that by reading every descriptor made each such
// statement cost the size of the catalog. Each dependency is therefore
// also recorded under the relation (keys.ViewDependencyKey), written and
// removed in the transactions that create, replace and drop the view.
// The records are trusted only once keys.ViewDependencyIndexKey says
// every view has them (see sql.viewsDependingOn).

// PutViewDependencies records view d's dependencies under each relation
// it reads.
func PutViewDependencies(ctx context.Context, txn *kvclient.Txn, d *TableDescriptor) error {
	for _, dep := range d.ViewDepends {
		if err := txn.Put(ctx, keys.ViewDependencyKey(dep, d.ID), []byte{1}); err != nil {
			return err
		}
	}
	return nil
}

// DeleteViewDependencies removes view viewID's records under deps.
func DeleteViewDependencies(ctx context.Context, txn *kvclient.Txn, viewID uint64, deps []uint64) error {
	for _, dep := range deps {
		if err := txn.Delete(ctx, keys.ViewDependencyKey(dep, viewID)); err != nil {
			return err
		}
	}
	return nil
}

// ViewIDsRecordedFor lists the ids of the views recorded as reading
// relID.
func ViewIDsRecordedFor(ctx context.Context, txn *kvclient.Txn, relID uint64) ([]uint64, error) {
	lo, hi := keys.ViewDependencySpan(relID)
	kvs, err := txn.Scan(ctx, lo, hi, 0)
	if err != nil {
		return nil, err
	}
	ids := make([]uint64, 0, len(kvs))
	for _, kv := range kvs {
		if _, id, err := encoding.DecodeUint64(kv.Key[len(lo):]); err == nil {
			ids = append(ids, id)
		}
	}
	return ids, nil
}

// RecordAllViewDependencies writes every view's records and marks them
// complete, in one transaction, unless they are marked already; it
// reports whether they are complete when it returns. The caller runs it
// only at cluster version v22, when every node writes the records for
// the views it creates: the transaction reads every descriptor, so a
// view created or dropped while it runs conflicts with it rather than
// slipping past.
func RecordAllViewDependencies(ctx context.Context, db *kvclient.DB) (bool, error) {
	err := db.RunTxn(ctx, "view-dependency-records", func(ctx context.Context, txn *kvclient.Txn) error {
		if ready, err := txn.Get(ctx, keys.ViewDependencyIndexKey()); err != nil || ready != nil {
			return err
		}
		lo, hi := keys.TableDescSpan()
		rows, err := txn.Scan(ctx, lo, hi, 0)
		if err != nil {
			return err
		}
		for _, kv := range rows {
			var d TableDescriptor
			if json.Unmarshal(kv.Value, &d) != nil || !d.IsView() {
				continue
			}
			if err := PutViewDependencies(ctx, txn, &d); err != nil {
				return err
			}
		}
		return txn.Put(ctx, keys.ViewDependencyIndexKey(), []byte{1})
	})
	return err == nil, err
}
