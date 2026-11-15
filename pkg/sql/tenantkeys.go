package sql

import (
	"context"
	"errors"

	"github.com/sthorne/datax/pkg/kvclient"
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

const destroyTenantKeyFunc = "datax_destroy_tenant_key"

func init() {
	// Evaluated by the session, once per call, with constant arguments,
	// as the key functions are.
	volatileFuncs[destroyTenantKeyFunc] = true
	splicedFuncs[destroyTenantKeyFunc] = true
}

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
