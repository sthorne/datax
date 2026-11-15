package sql

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/kvclient"
	"github.com/sthorne/datax/pkg/sql/catalog"
	"github.com/sthorne/datax/pkg/sql/rowenc"
	"github.com/sthorne/datax/pkg/sql/rowseal"
	"github.com/sthorne/datax/pkg/sql/types"
	"github.com/sthorne/datax/pkg/util/log"
	"github.com/sthorne/datax/pkg/version"
)

// Per-tenant keys (issue #310, step 4; docs/encryption.md, "Per-tenant
// keys"). A table created WITH (encryption_key_by = '<column>') seals
// each tenant's rows — each value of its leading primary-key column —
// under a key of that tenant's own, made on the tenant's first write
// (pkg/sql/rowseal, tenant.go). datax_destroy_tenant_key crypto-shreds
// one tenant:
//
//	SELECT datax_destroy_tenant_key('items', $tenant_id);
//
// It needs ownership of the table (or the admin role), runs outside a
// transaction block, destroys the tenant's key — its record tombstoned
// and the record's history purged — and returns once every gateway has
// stopped using the key (the key cache's lease, 10 seconds), with the id
// of the key it destroyed. A tenant whose key was destroyed already gets
// its key destroyed again (its record purged again) and the same id; a
// tenant that never had a key — it never wrote to the table — gets NULL,
// so an erasure that runs the function for every table a tenant might
// have written to needs no special case. A row still sealed under the
// destroyed key fails with XXK01 from then on; the tenant's next write
// makes a fresh key.

// It is evaluated by the session, once per call, with constant
// arguments, as the key functions are (sealFuncs).
const destroyTenantKeyFunc = "datax_destroy_tenant_key"

// evalDestroyTenantKey runs one call of datax_destroy_tenant_key(table,
// tenant).
func (s *Session) evalDestroyTenantKey(ctx context.Context, txn *kvclient.Txn, args []types.Datum) (types.Datum, error) {
	if len(args) != 2 {
		return types.Datum{}, newErrf(CodeUndefinedFunction, "%s() takes two arguments: the table and the tenant (a value of its tenant column)", destroyTenantKeyFunc)
	}
	if args[0].Null || args[1].Null {
		return types.DNull, nil // strict, as PostgreSQL's functions are
	}
	if s.state == StateOpen {
		return types.Datum{}, newErrf(CodeActiveTransaction, "%s() cannot run inside a transaction block: it destroys the key in a transaction of its own and then waits for every gateway to stop using it", destroyTenantKeyFunc)
	}
	if s.db.ClusterVersion() < version.V22 {
		return types.Datum{}, newErrf(CodeFeatureNotSupported, "per-tenant keys need cluster version %s: finalize the upgrade with `datax debug upgrade` first", version.V22)
	}
	b := s.rowSealer()
	ts, ok := b.Store().(rowseal.TenantKeyStore)
	if !ok {
		return types.Datum{}, newErrf(CodeFeatureNotSupported, "per-tenant keys need a key store that keeps tenant keys, and this node has none")
	}
	desc, err := s.lookup(ctx, txn, args[0].Text())
	if err != nil {
		return types.Datum{}, err
	}
	if err := s.checkTableOwner(ctx, txn, desc); err != nil {
		return types.Datum{}, err
	}
	if !desc.SealedPerTenant() {
		return types.Datum{}, newErrf(CodeWrongObjectType, "table %q is not sealed per tenant (CREATE TABLE ... WITH (encryption_key_by = '<column>')), so it has no tenant keys", desc.Name)
	}
	tenant, err := rowseal.TenantValue(desc, args[1])
	if err != nil {
		return types.Datum{}, newErrf(CodeInvalidTextRepresentation, "%s: %v", destroyTenantKeyFunc, err)
	}
	name, err := b.Tenants().Name(ctx, desc, tenant)
	if err != nil {
		return types.Datum{}, err
	}
	res, err := ts.DestroyTenantKey(ctx, name)
	b.Tenants().ForgetTenant(desc, tenant)
	if errors.Is(err, rowseal.ErrKeyNotFound) {
		log.Audit("key-ddl", "stmt", destroyTenantKeyFunc, "target", desc.Name, "outcome", "the tenant has no key", "principal", s.sessionUser, "role", s.actor())
		return types.DNull, nil
	}
	if err != nil {
		return types.Datum{}, err
	}
	b.Invalidate(res.KeyID)
	outcome := "destroyed"
	if res.Already {
		outcome = "already destroyed; purged again"
	}
	if res.PurgeIncomplete != "" {
		outcome += "; purge incomplete"
	}
	log.Audit("key-ddl", "stmt", destroyTenantKeyFunc, "target", desc.Name, "key_id", res.KeyID, "outcome", outcome, "principal", s.sessionUser, "role", s.actor())
	if serr := s.waitKeyLease(ctx); serr != nil {
		return types.Datum{}, serr
	}
	if res.PurgeIncomplete != "" {
		return types.Datum{}, newErrf(CodeObjectNotInState, "the tenant's key (%d) is destroyed and no gateway uses it, but purging its record's history did not finish (%s): run %s again to retry the purge", res.KeyID, res.PurgeIncomplete, destroyTenantKeyFunc)
	}
	return types.NewInt(res.KeyID), nil
}

// removeTenantChunk is how many index entries or rows one transaction
// of datax_remove_tenant_rows reads.
const removeTenantChunk = 200

// evalRemoveTenantRows runs datax_remove_tenant_rows(table, tenant): it
// removes the rows of a tenant of a per-tenant table that no one can read
// any more — sealed under a key that was destroyed (the rows an erasure
// left behind by destroying the key before deleting them) — with their
// secondary index entries, without the key, and returns how many rows it
// removed. The tenant's readable rows (under its live key, or plain while
// the table is being sealed) are left alone: DELETE removes those.
//
// DELETE cannot remove such a row: it reads the row's values to find
// its index entries, check foreign keys and row-security policies, and
// return or count it. This function needs none of those. A row's key is
// its primary key, whose leading column is the tenant, so the tenant's
// rows are one span; and every index entry ends in (or, for a unique
// index, holds) the primary key of its row, so the entries of the
// tenant's rows are found by reading each secondary index and keeping
// the entries whose primary key is in the tenant's span — only the
// tenant's part of an index whose leading column is the tenant column,
// all of any other index. Entries go first, then rows, in transactions
// of removeTenantChunk, each deciding on what it reads: an entry goes
// when its row is unreadable or absent (absent only after an earlier
// call was cut short between the two), a row when it is unreadable. A
// row under a destroyed key is never written again — any write to it
// reads it first and fails — so nothing a chunk decides can be undone
// by a concurrent writer, and a new row of the tenant (under its fresh
// key) is readable and stays, entries and all. Calling again finishes a
// call cut short.
//
// A table other tables reference by foreign key is refused: removing a
// referenced row needs the foreign key's action, which needs the row.
func (s *Session) evalRemoveTenantRows(ctx context.Context, txn *kvclient.Txn, args []types.Datum) (types.Datum, error) {
	desc, ok, err := s.sealFuncTable(ctx, txn, removeTenantRowsFunc, args, 2, 2, "a table and a tenant (a value of its tenant column)")
	if err != nil || !ok {
		return types.DNull, err
	}
	if !desc.SealedPerTenant() {
		return types.Datum{}, newErrf(CodeWrongObjectType, "table %q is not sealed per tenant (encryption_key_by): DELETE removes its rows", desc.Name)
	}
	if len(desc.InboundFKs) > 0 {
		var names []string
		for _, ref := range desc.InboundFKs {
			name := fmt.Sprintf("table %d", ref.TableID)
			if d, err := catalog.ReadTable(ctx, txn, ref.TableID); err == nil && d != nil {
				name = d.Name
			}
			names = append(names, name)
		}
		return types.Datum{}, newErrf(CodeFeatureNotSupported, "table %q is referenced by foreign keys (from %s), whose actions need the rows' values: %s() cannot remove its rows without them", desc.Name, strings.Join(names, ", "), removeTenantRowsFunc)
	}
	tenant, err := rowseal.TenantValue(desc, args[1])
	if err != nil {
		return types.Datum{}, newErrf(CodeInvalidTextRepresentation, "%s: %v", removeTenantRowsFunc, err)
	}
	b := s.rowSealer()
	tenantSpan := append(rowenc.PrimaryKeyPrefixFor(desc), tenant...)
	inTenant := func(pk keys.Key) bool { return pk.HasPrefix(tenantSpan) }
	// Index entries first.
	entries := 0
	for i := range desc.Indexes {
		idx := &desc.Indexes[i]
		lo := keys.TableIndexPrefix(desc.ID, idx.ID)
		if !idx.IsInverted() && !idx.IsVector() && len(idx.ColumnIDs) > 0 && idx.ColumnIDs[0] == desc.EncryptionKey.TenantColumn {
			lo = append(lo, tenant...)
		}
		hi := lo.PrefixEnd()
		for from := lo; ; {
			var next keys.Key
			var n int
			err := s.db.RunTxn(ctx, "remove-tenant-entries", func(ctx context.Context, txn *kvclient.Txn) error {
				next, n = nil, 0
				kvs, err := txn.Scan(ctx, from, hi, removeTenantChunk)
				if err != nil {
					return err
				}
				if len(kvs) == removeTenantChunk {
					next = keys.Key(kvs[len(kvs)-1].Key).Next()
				}
				var ents []keys.Key
				var pks []keys.Key
				for _, kv := range kvs {
					pk, err := rowenc.IndexEntryPrimaryKey(desc, idx, kv.Key, kv.Value)
					if err != nil {
						return err
					}
					if inTenant(pk) {
						ents, pks = append(ents, kv.Key), append(pks, pk)
					}
				}
				if len(pks) == 0 {
					return nil
				}
				rows, err := txn.GetBatch(ctx, pks)
				if err != nil {
					return err
				}
				var wb kvclient.WriteBatch
				for j, v := range rows {
					if v != nil {
						if gone, err := b.ValueKeyGone(ctx, desc, v); err != nil {
							return err
						} else if !gone {
							continue
						}
					}
					wb.Delete(ents[j])
					n++
				}
				return txn.RunBatch(ctx, &wb)
			})
			if err != nil {
				return types.Datum{}, err
			}
			entries += n
			if next == nil {
				break
			}
			from = next
		}
	}
	// Then the rows.
	rows := 0
	hi := keys.Key(tenantSpan).PrefixEnd()
	for from := keys.Key(tenantSpan); ; {
		var next keys.Key
		var n int
		err := s.db.RunTxn(ctx, "remove-tenant-rows", func(ctx context.Context, txn *kvclient.Txn) error {
			next, n = nil, 0
			kvs, err := txn.Scan(ctx, from, hi, removeTenantChunk)
			if err != nil {
				return err
			}
			if len(kvs) == removeTenantChunk {
				next = keys.Key(kvs[len(kvs)-1].Key).Next()
			}
			var wb kvclient.WriteBatch
			for _, kv := range kvs {
				gone, err := b.ValueKeyGone(ctx, desc, kv.Value)
				if err != nil {
					return err
				}
				if gone {
					wb.Delete(kv.Key)
					n++
				}
			}
			return txn.RunBatch(ctx, &wb)
		})
		if err != nil {
			return types.Datum{}, err
		}
		rows += n
		if next == nil {
			break
		}
		from = next
	}
	log.Audit("key-ddl", "stmt", removeTenantRowsFunc, "target", desc.Name, "outcome", fmt.Sprintf("%d unreadable rows and %d index entries removed", rows, entries), "principal", s.sessionUser, "role", s.actor())
	return types.NewInt(int64(rows)), nil
}
