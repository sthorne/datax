package keystore

import (
	"context"
	"errors"
	"fmt"

	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/kvclient"
	"github.com/sthorne/datax/pkg/sql/catalog"
	"github.com/sthorne/datax/pkg/storage/enc"
)

// ErrReinstateDestroyed: the key's record in the backup is itself a
// destroyed key's, with no material to bring back.
var ErrReinstateDestroyed = errors.New("the key was already destroyed when the backup was taken")

// Reinstate brings one key back from a backup, in txn: the record value
// of datax_keys row key as backupDesc (the backup's datax_keys
// descriptor) reads it. A table restore uses it for a sealed table's key
// (issue #310), so the restored table's descriptor names a key that
// opens its rows.
//
//   - A key this cluster has and has not destroyed is left as it is: the
//     restored table shares it with the table it was taken from. The
//     result is false.
//   - Otherwise — the key is absent (a restore into another cluster) or
//     destroyed here (the table was dropped after the backup) — the
//     backup's record is written back under the key's id, every version
//     resealed under this cluster's current key, its generation moved
//     past any this cluster has seen so that no gateway's cache mistakes
//     it for a state it knew. A destroyed key coming back is the
//     documented dependency on backup retention, not a bypass of it: the
//     backup held the key all along. If another key has taken the name
//     here, the restored key is renamed. The result is true.
//
// The cluster key the backup's record is under must be one this node
// holds (its own, or one imported from the source).
func (s *Store) Reinstate(ctx context.Context, txn *kvclient.Txn, backupDesc *catalog.TableDescriptor, key keys.Key, value []byte) (Key, bool, error) {
	d, err := s.desc(ctx, true)
	if err != nil {
		return Key{}, false, err
	}
	br, err := decodeRow(backupDesc, key, value)
	if err != nil {
		return Key{}, false, fmt.Errorf("the backup's record of a key: %w", err)
	}
	live, pk, err := s.read(ctx, txn, d, br.ID, true)
	switch {
	case err == nil && live.State != StateDestroyed:
		return live.Key, false, nil
	case err != nil && !errors.Is(err, ErrNotFound):
		return Key{}, false, err
	}
	if br.State == StateDestroyed || len(br.versions) == 0 {
		return Key{}, false, fmt.Errorf("key %d (%s): %w", br.ID, br.Name, ErrReinstateDestroyed)
	}
	if pk == nil {
		if pk, err = pkKey(d, br.ID); err != nil {
			return Key{}, false, err
		}
	}
	// The name: the key's own, unless another key holds it here.
	name := br.Name
	if live == nil {
		probe := &row{Key: Key{ID: br.ID, Name: name}}
		ie, err := probe.indexEntry(d)
		if err != nil {
			return Key{}, false, err
		}
		if v, err := txn.Get(ctx, ie.Key); err != nil {
			return Key{}, false, err
		} else if v != nil {
			name = fmt.Sprintf("%s (restored %d)", br.Name, br.ID)
		}
	}
	from, err := s.cfg.ClusterKeys.Key(ctx, br.ClusterKey)
	if err != nil {
		return Key{}, false, fmt.Errorf("key %d (%s) is sealed under cluster key %s, which this cluster does not hold (import the source cluster's key first): %w", br.ID, br.Name, br.ClusterKey, err)
	}
	defer enc.Zero(from)
	ref, to, err := s.cfg.ClusterKeys.Current(ctx)
	if err != nil {
		return Key{}, false, err
	}
	defer enc.Zero(to)
	vs := make([]sealedVersion, len(br.versions))
	for i, v := range br.versions {
		plain, err := UnwrapMaterial(from, br.ID, br.Name, v.Version, br.Kind, v.Sealed)
		if err != nil {
			return Key{}, false, fmt.Errorf("key %d (%s) version %d: %w", br.ID, br.Name, v.Version, err)
		}
		sealed, err := WrapMaterial(to, br.ID, name, v.Version, br.Kind, plain)
		enc.Zero(plain)
		if err != nil {
			return Key{}, false, err
		}
		vs[i] = sealedVersion{Version: v.Version, Sealed: sealed, Created: v.Created}
	}
	r := &row{Key: br.Key, versions: vs}
	r.Name, r.ClusterKey = name, ref
	if live != nil && live.Generation >= r.Generation {
		r.Generation = live.Generation
	}
	r.Generation++
	if err := s.accept(ctx, txn, ref); err != nil {
		return Key{}, false, err
	}
	val, err := r.encode(d)
	if err != nil {
		return Key{}, false, err
	}
	ie, err := r.indexEntry(d)
	if err != nil {
		return Key{}, false, err
	}
	var wb kvclient.WriteBatch
	wb.Put(pk, val)
	if live == nil || live.Name != name {
		wb.Put(ie.Key, ie.Value)
	}
	if err := txn.RunBatch(ctx, &wb); err != nil {
		return Key{}, false, err
	}
	return r.Key, true, nil
}
