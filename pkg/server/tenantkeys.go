package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/keystore"
	"github.com/sthorne/datax/pkg/sql/catalog"
	"github.com/sthorne/datax/pkg/sql/rowseal"
	"github.com/sthorne/datax/pkg/util/log"
	"github.com/sthorne/datax/pkg/version"
)

// Per-tenant keys (issue #310, step 4): the key store's side of tables
// sealed per tenant (pkg/sql/rowseal, tenant.go). A tenant's key is the
// named key of kind tenant that rowseal names
// tenant/<seal id>/<keyed digest of the tenant>; it is found by the
// unique index on datax_keys' names, made on the tenant's first write in
// a transaction of its own, before the write's, and destroyed by
// datax_destroy_tenant_key, which renames it out of the way
// (keystore.DestroyedName).
//
// Making the key before the user's transaction, rather than in it, keeps
// the user's transaction free of writes to datax_keys — which every
// tenant's first write would otherwise share a range with, and which
// would turn a first write of two tenants into two transactions writing
// one range — and makes concurrent first writes of one tenant converge:
// the creations race on the name's index entry, the loser's transaction
// retries and finds the winner's key. Writes of different tenants touch
// different names and different ids, so they do not conflict. The cost
// is that a user transaction that aborts after its tenant's first write
// leaves the tenant's key behind: harmless — a key with no rows, used by
// the tenant's next write — and destroyed with the rest when the table
// is dropped.

// MAC implements rowseal.KeyMACer: the digest that names tenants' keys,
// under the table's naming key.
func (k sealKeys) MAC(ctx context.Context, id int64, version int, label string, data []byte) ([]byte, error) {
	c := k.n.keyCache()
	if c == nil {
		return nil, errors.New("the key store is not open yet")
	}
	m, err := c.MAC(ctx, id, version, label, data)
	return m, sealKeyError(err)
}

// TenantKey implements rowseal.TenantKeyStore.
func (k sealedTableKeys) TenantKey(ctx context.Context, name string, create bool) (int64, error) {
	ks, err := k.n.KeyStore()
	if err != nil {
		return 0, err
	}
	key, err := ks.Get(ctx, name)
	if errors.Is(err, keystore.ErrNotFound) {
		if !create {
			return 0, rowseal.ErrKeyNotFound
		}
		key, err = ks.Create(ctx, keystore.KindTenant, name)
		if errors.Is(err, keystore.ErrExists) {
			key, err = ks.Get(ctx, name) // another gateway's first write won
		}
	}
	if err != nil {
		return 0, sealKeyError(err)
	}
	if key.Kind != keystore.KindTenant {
		return 0, fmt.Errorf("key %q is a %s key, not a tenant's", name, key.Kind)
	}
	return key.ID, nil
}

// DestroyTenantKey implements rowseal.TenantKeyStore.
func (k sealedTableKeys) DestroyTenantKey(ctx context.Context, name string) (rowseal.TenantKeyDestroyed, error) {
	ks, err := k.n.KeyStore()
	if err != nil {
		return rowseal.TenantKeyDestroyed{}, err
	}
	var targets []keystore.Key
	already := false
	key, err := ks.Get(ctx, name)
	switch {
	case err == nil:
		targets = []keystore.Key{key}
	case errors.Is(err, keystore.ErrNotFound):
		// Destroyed before (renamed), or never made.
		prefix := keystore.DestroyedPrefix + name + "/"
		if targets, err = ks.KeysByName(ctx, prefix, keystore.PrefixEnd(prefix), 1000); err != nil {
			return rowseal.TenantKeyDestroyed{}, err
		}
		if len(targets) == 0 {
			return rowseal.TenantKeyDestroyed{}, rowseal.ErrKeyNotFound
		}
		already = true
	default:
		return rowseal.TenantKeyDestroyed{}, err
	}
	out := rowseal.TenantKeyDestroyed{Already: already}
	var incomplete []string
	var latest time.Time
	for _, t := range targets {
		if t.Kind != keystore.KindTenant {
			return out, fmt.Errorf("key %q is a %s key, not a tenant's", t.Name, t.Kind)
		}
		_, rep, err := ks.Destroy(ctx, t.ID)
		sealKeys(k).Invalidate(t.ID)
		if err != nil {
			return out, err
		}
		if rep != nil && !rep.Complete {
			incomplete = append(incomplete, rep.Incomplete...)
		}
		if out.KeyID == 0 || t.Created.After(latest) {
			out.KeyID, latest = t.ID, t.Created
		}
	}
	out.PurgeIncomplete = strings.Join(incomplete, "; ")
	return out, nil
}

// DestroyTenantKeys implements rowseal.TenantKeyStore: it wakes this
// node's tenant key janitor, which destroys the keys of every seal id no
// table names (a dropped table's), however many.
func (k sealedTableKeys) DestroyTenantKeys(uint64) {
	select {
	case k.n.tenantKeyKick <- struct{}{}:
	default:
	}
}

// defaultTenantKeyGrace is how old a tenant key must be before the
// janitor takes it for orphaned (Config.TenantKeyGrace). A tenant's key
// is made, in a transaction of its own, by the first write of the tenant
// — which can come in the same transaction block as the CREATE TABLE,
// before the table's descriptor is committed and visible: a key younger
// than this whose table the janitor cannot see may be such a key.
const defaultTenantKeyGrace = 10 * time.Minute

func (n *Node) tenantKeyGrace() time.Duration {
	switch g := n.cfg.TenantKeyGrace; {
	case g == 0:
		return defaultTenantKeyGrace
	case g < 0:
		return 0
	default:
		return g
	}
}

// tenantKeyPage is how many tenant keys the janitor reads at once.
const tenantKeyPage = 256

// liveTenantSeals returns the seal ids whose tenant keys must stay: those
// of every table sealed per tenant, and those a table restore in flight
// is bringing back (restoreProgress.SealIDs).
func (n *Node) liveTenantSeals(ctx context.Context) (map[uint64]bool, error) {
	live := map[uint64]bool{}
	lo, hi := keys.TableDescSpan()
	kvs, err := n.db.Scan(ctx, lo, hi, 0)
	if err != nil {
		return nil, err
	}
	for _, kv := range kvs {
		var d catalog.TableDescriptor
		if err := json.Unmarshal(kv.Value, &d); err != nil {
			return nil, fmt.Errorf("a table descriptor does not decode, so no tenant key is taken for unused: %w", err)
		}
		if d.SealedPerTenant() {
			live[d.EncryptionKey.SealID] = true
		}
	}
	plo, phi := keys.RestoreProgressSpan()
	recs, err := n.db.Scan(ctx, plo, phi, 0)
	if err != nil {
		return nil, err
	}
	for _, kv := range recs {
		var rec restoreProgress
		if err := json.Unmarshal(kv.Value, &rec); err != nil {
			return nil, fmt.Errorf("a restore's progress record does not decode, so no tenant key is taken for unused: %w", err)
		}
		for _, s := range rec.SealIDs {
			live[s] = true
		}
	}
	return live, nil
}

// sealOfTenantKey reads the seal id out of a tenant key's name.
func sealOfTenantKey(name string) (uint64, bool) {
	rest, ok := strings.CutPrefix(name, keystore.TenantKeyPrefix)
	if !ok {
		return 0, false
	}
	num, _, ok := strings.Cut(rest, "/")
	if !ok {
		return 0, false
	}
	s, err := strconv.ParseUint(num, 10, 64)
	return s, err == nil
}

// SweepOrphanTenantKeys destroys the tenant keys of every seal id no
// table sealed per tenant names any more — a dropped table's, possibly
// many — and returns how many it destroyed. It reads the names of live
// tenant keys in order (destroyed keys are renamed out of that range),
// skipping each live table's keys with one seek, so a pass costs reads
// in the number of per-tenant tables plus the keys it destroys, not the
// number of tenants. It is resumable: a pass cut short leaves the rest
// for the next. A key younger than tenantKeyGrace is left for a later
// pass (see there), and a seal id is checked against the descriptors
// again, read after its keys were, before any of its keys is destroyed.
func (n *Node) SweepOrphanTenantKeys(ctx context.Context) (int, error) {
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
	live, err := n.liveTenantSeals(ctx)
	if err != nil {
		return 0, err
	}
	confirmed := map[uint64]bool{} // orphaned, checked again
	end := keystore.PrefixEnd(keystore.TenantKeyPrefix)
	from := keystore.TenantKeyPrefix
	destroyed := 0
	now, grace := time.Now(), n.tenantKeyGrace()
	for from != "" {
		page, err := ks.KeysByName(ctx, from, end, tenantKeyPage)
		if err != nil {
			return destroyed, err
		}
		if len(page) == 0 {
			break
		}
		next := page[len(page)-1].Name + "\x00"
		var orphans []int64
		for _, k := range page {
			seal, ok := sealOfTenantKey(k.Name)
			if !ok || k.Kind != keystore.KindTenant {
				continue
			}
			if live[seal] {
				// Every other key of this table: one seek past them.
				next = keystore.PrefixEnd(rowseal.TenantNamePrefix(seal))
				break
			}
			if k.State == keystore.StateDestroyed || now.Sub(k.Created) < grace {
				continue
			}
			if !confirmed[seal] {
				again, err := n.liveTenantSeals(ctx)
				if err != nil {
					return destroyed, err
				}
				if again[seal] {
					live[seal] = true
					next = keystore.PrefixEnd(rowseal.TenantNamePrefix(seal))
					break
				}
				confirmed[seal] = true
			}
			orphans = append(orphans, k.ID)
		}
		// A page's keys in a few transactions and one purge.
		got, _, err := ks.DestroyMany(ctx, orphans)
		for _, id := range orphans {
			sealKeys{n}.Invalidate(id)
		}
		destroyed += got
		if err != nil {
			return destroyed, err
		}
		from = next
	}
	if destroyed > 0 {
		log.Infof("tenant key janitor: destroyed %d tenant keys of dropped tables", destroyed)
	}
	return destroyed, nil
}
