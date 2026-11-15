package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/keystore"
	"github.com/sthorne/datax/pkg/kvclient"
	"github.com/sthorne/datax/pkg/sql/catalog"
	"github.com/sthorne/datax/pkg/sql/rowseal"
	"github.com/sthorne/datax/pkg/util/log"
	"github.com/sthorne/datax/pkg/version"
)

// Sealed tables (issue #310, step 3): the gateway's boundary between a
// sealed table's plaintext rows and the sealed values KV stores
// (pkg/sql/rowseal), over the cache of named keys this gateway shares
// with the key service (keystore.Cache, Node.KeyService().Cache(), under
// Config.KeyCacheLease). The catalog accessor carries the boundary to
// every session of the node.

// attachRowSealer gives cat the node's sealed-table boundary. It runs
// while the accessor is being made, which the key service needs, so the
// boundary reaches the service when it is first used.
func (n *Node) attachRowSealer(cat *catalog.Accessor) {
	cat.SetRowSealer(rowseal.New(sealKeys{n}, sealedTableKeys{n}, nil))
}

// keyCache is the node's shared cache of named keys (nil before the
// store is open).
func (n *Node) keyCache() *keystore.Cache {
	if svc := n.KeyService(); svc != nil {
		return svc.Cache()
	}
	return nil
}

// sealKeys is the shared cache as the boundary's source of keys.
type sealKeys struct{ n *Node }

// sealKeyError maps the key store's reasons to the boundary's.
func sealKeyError(err error) error {
	for _, m := range []struct{ from, to error }{
		{keystore.ErrDestroyed, rowseal.ErrKeyDestroyed},
		{keystore.ErrDisabled, rowseal.ErrKeyDisabled},
		{keystore.ErrNotFound, rowseal.ErrKeyNotFound},
		{keystore.ErrNoVersion, rowseal.ErrKeyNoVersion},
	} {
		if errors.Is(err, m.from) {
			return m.to
		}
	}
	return err
}

func (k sealKeys) get(ctx context.Context, id int64, v int) (rowseal.Key, error) {
	c := k.n.keyCache()
	if c == nil {
		return rowseal.Key{}, errors.New("the key store is not open yet")
	}
	aead, ver, err := c.AEAD(ctx, id, v)
	if err != nil {
		return rowseal.Key{}, sealKeyError(err)
	}
	return rowseal.Key{Version: ver, AEAD: aead}, nil
}

func (k sealKeys) Current(ctx context.Context, id int64) (rowseal.Key, error) {
	return k.get(ctx, id, 0)
}

func (k sealKeys) Version(ctx context.Context, id int64, v int) (rowseal.Key, error) {
	if v <= 0 {
		return rowseal.Key{}, rowseal.ErrKeyNoVersion
	}
	return k.get(ctx, id, v)
}

func (k sealKeys) Invalidate(id int64) {
	if c := k.n.keyCache(); c != nil {
		c.Forget(id)
	}
}

// sealedTableKeys is the key store as the SQL layer needs it for sealed
// tables.
type sealedTableKeys struct{ n *Node }

func (k sealedTableKeys) Ready(ctx context.Context) error {
	err := k.n.clusterKeyReady(ctx)
	if err == nil {
		return nil
	}
	if rest, ok := strings.CutPrefix(err.Error(), "named keys "); ok {
		return fmt.Errorf("sealed tables %s", rest)
	}
	return fmt.Errorf("sealed tables need the cluster key: %w", err)
}

func (k sealedTableKeys) CreateTableKey(ctx context.Context, txn *kvclient.Txn, name string) (int64, error) {
	ks, err := k.n.KeyStore()
	if err != nil {
		return 0, err
	}
	key, err := ks.CreateIn(ctx, txn, keystore.KindTable, name)
	return key.ID, err
}

func (k sealedTableKeys) DestroyKey(ctx context.Context, id int64) error {
	ks, err := k.n.KeyStore()
	if err != nil {
		return err
	}
	_, rep, err := ks.Destroy(ctx, id)
	sealKeys(k).Invalidate(id)
	if err != nil {
		return err
	}
	if rep != nil && !rep.Complete {
		return fmt.Errorf("key %d is destroyed, but the purge of its record's history is incomplete: %s", id, strings.Join(rep.Incomplete, "; "))
	}
	return nil
}

// sealedKeyJanitorInterval is how often a node looks for table keys no
// table names.
const sealedKeyJanitorInterval = 5 * time.Minute

// sealedKeyJanitorLoop destroys the keys of sealed tables that no table
// names any more: the backstop for a DROP TABLE whose key destruction,
// which runs after the drop commits, did not happen (the gateway died in
// between, or the destruction failed).
func (n *Node) sealedKeyJanitorLoop(ctx context.Context) {
	wait := time.Minute
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(wait):
		}
		wait = sealedKeyJanitorInterval
		if _, err := n.SweepOrphanTableKeys(ctx); err != nil {
			log.Debugf("sealed key janitor: %v", err)
		}
	}
}

// SweepOrphanTableKeys destroys every key of kind table, not destroyed
// yet, that no table descriptor names, and returns how many it
// destroyed. A table's key is created in the transaction that creates
// the table, and a table restore brings a key back in the transaction
// that publishes the table, so a key read here without a descriptor that
// a later read finds is one whose table is gone.
func (n *Node) SweepOrphanTableKeys(ctx context.Context) (int, error) {
	if n.db.ClusterVersion() < version.V22 {
		return 0, nil
	}
	if m, err := n.readCKMarker(ctx); err != nil || m == nil {
		return 0, err
	}
	ks, err := n.KeyStore()
	if err != nil {
		return 0, err
	}
	all, err := ks.List(ctx)
	if err != nil {
		return 0, err
	}
	var live []keystore.Key
	for _, k := range all {
		if k.Kind == keystore.KindTable && k.State != keystore.StateDestroyed {
			live = append(live, k)
		}
	}
	if len(live) == 0 {
		return 0, nil
	}
	named := map[int64]bool{}
	lo, hi := keys.TableDescSpan()
	kvs, err := n.db.Scan(ctx, lo, hi, 0)
	if err != nil {
		return 0, err
	}
	for _, kv := range kvs {
		var d catalog.TableDescriptor
		if err := json.Unmarshal(kv.Value, &d); err != nil {
			return 0, fmt.Errorf("a table descriptor does not decode, so no key is taken for unused: %w", err)
		}
		if d.Sealed() {
			named[d.EncryptionKey.KeyID] = true
		}
	}
	destroyed := 0
	for _, k := range live {
		if named[k.ID] {
			continue
		}
		if _, _, err := ks.Destroy(ctx, k.ID); err != nil {
			return destroyed, err
		}
		sealKeys{n}.Invalidate(k.ID)
		log.Infof("sealed key janitor: destroyed key %d (%s), which no table names", k.ID, k.Name)
		destroyed++
	}
	return destroyed, nil
}
