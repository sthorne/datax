package sql

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/kvclient"
	"github.com/sthorne/datax/pkg/sql/catalog"
	"github.com/sthorne/datax/pkg/sql/parser"
	"github.com/sthorne/datax/pkg/sql/rowenc"
	"github.com/sthorne/datax/pkg/sql/rowseal"
	"github.com/sthorne/datax/pkg/sql/types"
	"github.com/sthorne/datax/pkg/sql/vtable"
	"github.com/sthorne/datax/pkg/util/log"
	"github.com/sthorne/datax/pkg/version"
)

// Sealing an existing table and rotating a sealed table's keys (issue
// #310, step 5; docs/encryption.md, "Sealing an existing table" and
// "Rotating a sealed table's key").
//
//	ALTER TABLE t SET (encryption_key = 'auto');           -- seal a table that holds rows
//	ALTER TABLE t SET (encryption_key_by = 'tenant_id');   -- the same, a key per tenant
//	ALTER TABLE t ROTATE ENCRYPTION KEY [KEEP OLD VERSIONS];
//	SELECT datax_rotate_tenant_key('t', $tenant [, keep_old]);
//	SELECT datax_destroy_old_key_versions('t' [, $tenant]);
//
// Each change takes effect for new writes at once — the descriptor
// carries the key, or the key has a new version — and leaves rows
// written before it: plain rows of a table now sealed, rows sealed under
// a version that is no longer current. Reads accept both meanwhile (a
// sealed table being sealed reads plain values; a value names the
// version it is under). A seal job (catalog.SealJob, one per table,
// keys.SealJobKey) rewrites them in the background: the node leading
// range 1 runs SealJobStep, batch after batch, each an ordinary
// transaction that rewrites up to a batch of rows and records where it
// got to, so the job survives restarts and moves of the leadership the
// way the row-TTL job does. It works in passes over the table's primary
// rows (a tenant's, for one tenant's rotation) and is done when a whole
// pass finds nothing to rewrite, the pass having started only once every
// gateway writes as the job wants: after the descriptor that sealed the
// table has been adopted by every lease (a gateway still on the old
// descriptor could write a plain row behind the job), or after the key
// cache's lease since the rotation (a gateway could seal under the old
// version until its cache sees the new one). Then a seal job clears the
// descriptor's Sealing — strict reads are back — and a rotation destroys
// the versions below the new one, unless asked to keep them.
//
// Unsealing (ALTER TABLE ... RESET (encryption_key)) is refused: it
// would write a sealed table's values back to KV in plaintext, which is
// the one thing a sealed table promises not to do, and its old sealed
// versions would stay readable in history meanwhile. A table is
// unsealed by copying its rows into a new, plain table.

const (
	rotateTenantKeyFunc       = "datax_rotate_tenant_key"
	destroyOldKeyVersionsFunc = "datax_destroy_old_key_versions"
	removeTenantRowsFunc      = "datax_remove_tenant_rows"
)

// sealFuncs are the functions of sealed tables the session evaluates
// once per call with constant arguments, outside a transaction block.
var sealFuncs = map[string]bool{
	destroyTenantKeyFunc:      true,
	rotateTenantKeyFunc:       true,
	destroyOldKeyVersionsFunc: true,
	removeTenantRowsFunc:      true,
}

func init() {
	for f := range sealFuncs {
		volatileFuncs[f] = true
		splicedFuncs[f] = true
	}
}

// evalSealFunc runs one call of a function of sealFuncs.
func (s *Session) evalSealFunc(ctx context.Context, txn *kvclient.Txn, name string, args []types.Datum) (types.Datum, error) {
	switch name {
	case destroyTenantKeyFunc:
		return s.evalDestroyTenantKey(ctx, txn, args)
	case rotateTenantKeyFunc:
		return s.evalRotateTenantKey(ctx, txn, args)
	case destroyOldKeyVersionsFunc:
		return s.evalDestroyOldKeyVersions(ctx, txn, args)
	case removeTenantRowsFunc:
		return s.evalRemoveTenantRows(ctx, txn, args)
	}
	return types.Datum{}, newErrf(CodeUndefinedFunction, "function %s() does not exist", name)
}

// rotationSlack is added to the key cache's lease before a rotation's
// job starts: clocks differ between nodes.
const rotationSlack = time.Second

// sealJobExists reports whether table id has a seal job, read in txn.
func sealJobExists(ctx context.Context, txn *kvclient.Txn, id uint64) (*catalog.SealJob, error) {
	raw, err := txn.Get(ctx, keys.SealJobKey(id))
	if err != nil || raw == nil {
		return nil, err
	}
	return catalog.DecodeSealJob(raw)
}

func jobBusyError(desc *catalog.TableDescriptor, j *catalog.SealJob) *Error {
	what := "rewriting rows under an older version of its key"
	if j.Kind == catalog.SealJobSeal {
		what = "sealing its existing rows"
	}
	return newErrf(CodeObjectInUse, "table %q has a seal job in progress (%s, %d rows rewritten so far): wait for it to finish (SELECT * FROM datax_encryption_jobs)", desc.Name, what, j.Rewritten)
}

// putSealJob writes a new job's record in txn.
func putSealJob(ctx context.Context, txn *kvclient.Txn, j *catalog.SealJob) error {
	raw, err := j.Encode()
	if err != nil {
		return err
	}
	return txn.Put(ctx, keys.SealJobKey(j.TableID), raw)
}

// rotatingStore is the node's key store as rotation needs it.
func (s *Session) rotatingStore(what string) (rowseal.RotatingKeyStore, *Error) {
	if s.db.ClusterVersion() < version.V22 {
		return nil, newErrf(CodeFeatureNotSupported, "%s needs cluster version %s: finalize the upgrade with `datax debug upgrade` first", what, version.V22)
	}
	rs, ok := s.rowSealer().Store().(rowseal.RotatingKeyStore)
	if !ok {
		return nil, newErrf(CodeFeatureNotSupported, "%s needs a key store, and this node has none", what)
	}
	return rs, nil
}

// execAlterSeal is ALTER TABLE ... SET (encryption_key = 'auto') or SET
// (encryption_key_by = '<column>') on an existing table: the table gets
// its key now, in one descriptor write marked Sealing, together with
// its seal job's record; the statement then waits for every gateway to
// adopt the descriptor, after which every write seals, and returns while
// the job seals the rows already there.
func (s *Session) execAlterSeal(ctx context.Context, at *parser.AlterTable) (*Result, *Error) {
	for k := range at.SetOptions {
		if !sealOption(k) {
			return nil, newErrf(CodeFeatureNotSupported, "ALTER TABLE ... SET (%s) seals the table and takes no other option: set %q in a statement of its own", optEncryptionKey+" | "+optEncryptionKeyBy, k)
		}
	}
	var desc *catalog.TableDescriptor
	err := s.db.RunTxn(ctx, "seal-table", func(ctx context.Context, txn *kvclient.Txn) error {
		shared, err := s.lookup(ctx, txn, at.Table)
		if err != nil {
			return err
		}
		if err := mustBeReal(shared); err != nil {
			return err
		}
		if shared.Sealed() {
			return newErrf(CodeObjectNotInState, "table %q is sealed already; ALTER TABLE ... ROTATE ENCRYPTION KEY gives its key a new version", shared.Name)
		}
		d := shared.Clone()
		var pkNames []string
		for _, id := range d.PrimaryKey {
			if c, ok := d.ColByID(id); ok {
				pkNames = append(pkNames, c.Name)
			}
		}
		sealed, tenantCol, err := s.checkSealOptions(ctx, d, at.SetOptions, pkNames)
		if err != nil {
			return err
		}
		if !sealed {
			return newErrf(CodeInternal, "no sealing option")
		}
		if j, err := sealJobExists(ctx, txn, d.ID); err != nil {
			return err
		} else if j != nil {
			return jobBusyError(d, j)
		}
		if err := s.sealNewTable(ctx, txn, d, tenantCol, "ALTER TABLE ... SET ("+strings.Join(sortedKeys(at.SetOptions), ", ")+")"); err != nil {
			return err
		}
		d.EncryptionKey.Sealing = true
		if err := s.cat.Update(ctx, txn, d); err != nil {
			return err
		}
		desc = d
		return putSealJob(ctx, txn, &catalog.SealJob{
			TableID: d.ID, Table: d.Name, Kind: catalog.SealJobSeal,
			DescVersion: d.Version, Pass: 1, Created: time.Now().UnixNano(),
		})
	})
	if err != nil {
		return nil, ToSQLError(err)
	}
	if err := s.cat.FinishDDLIn(ctx, s.database, at.Table); err != nil {
		return nil, ToSQLError(err)
	}
	res := &Result{Tag: "ALTER TABLE"}
	res.Notices = append(res.Notices, sealedTableNotice(desc),
		fmt.Sprintf("table %q held rows before it was sealed: new writes are sealed now, and a background job seals the rows already there (progress in datax_encryption_jobs); reads accept both until it is done. "+
			"The rows' plaintext versions stay in the table's MVCC history until garbage collection removes them; once the job is done, ALTER TABLE %s PURGE removes them now", desc.Name, desc.Name))
	return res, nil
}

func sortedKeys(m map[string]string) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// refuseResetSeal refuses ALTER TABLE ... RESET of a sealing option.
func refuseResetSeal(names []string) *Error {
	for _, n := range names {
		if sealOption(n) {
			return newErrf(CodeFeatureNotSupported, "ALTER TABLE ... RESET (%s) is not supported: unsealing would write the table's values back in plaintext. Copy the rows into a new table without encryption_key instead", n)
		}
	}
	return nil
}

// execRotateTableKey is ALTER TABLE t ROTATE ENCRYPTION KEY [KEEP OLD
// VERSIONS] on a table sealed whole.
func (s *Session) execRotateTableKey(ctx context.Context, at *parser.AlterTable) (*Result, *Error) {
	rs, serr := s.rotatingStore("ALTER TABLE ... ROTATE ENCRYPTION KEY")
	if serr != nil {
		return nil, serr
	}
	var desc *catalog.TableDescriptor
	check := func(ctx context.Context, txn *kvclient.Txn) error {
		shared, err := s.lookup(ctx, txn, at.Table)
		if err != nil {
			return err
		}
		if err := mustBeReal(shared); err != nil {
			return err
		}
		desc = shared
		switch {
		case !shared.Sealed():
			return newErrf(CodeObjectNotInState, "table %q is not sealed, so it has no key to rotate (ALTER TABLE ... SET (encryption_key = 'auto') seals it)", shared.Name)
		case shared.SealedPerTenant():
			return newErrf(CodeWrongObjectType, "table %q is sealed per tenant: each tenant's key is rotated by itself, with %s('%s', <tenant>)", shared.Name, rotateTenantKeyFunc, shared.Name)
		}
		if j, err := sealJobExists(ctx, txn, shared.ID); err != nil {
			return err
		} else if j != nil {
			return jobBusyError(shared, j)
		}
		return nil
	}
	if err := s.db.RunTxn(ctx, "rotate-table-key-check", check); err != nil {
		return nil, ToSQLError(err)
	}
	keyID := desc.EncryptionKey.KeyID
	v, err := rs.RotateKey(ctx, keyID)
	if err != nil {
		return nil, ToSQLError(fmt.Errorf("rotating the key of table %q: %w", desc.Name, err))
	}
	s.rowSealer().Invalidate(keyID)
	if serr := s.startRotation(ctx, desc, keyID, v, nil, at.RotateKeepOld, rs.Lease(), check); serr != nil {
		return nil, serr
	}
	log.Audit("sealed-table", "stmt", "ALTER TABLE ... ROTATE ENCRYPTION KEY", "target", desc.Name, "key_id", fmt.Sprint(keyID),
		"outcome", fmt.Sprintf("version %d; older versions rewritten in the background", v), "principal", s.sessionUser, "role", s.user)
	res := &Result{Tag: "ALTER TABLE"}
	keep := "then destroys them"
	if at.RotateKeepOld {
		keep = "and keeps them (KEEP OLD VERSIONS): datax_destroy_old_key_versions destroys them later"
	}
	res.Notices = append(res.Notices, fmt.Sprintf("the key of table %q is at version %d: every gateway seals new writes under it within %s. A background job rewrites the rows under older versions (progress in datax_encryption_jobs) %s; MVCC history written under an older version is unreadable once that version is destroyed",
		desc.Name, v, rs.Lease(), keep))
	return res, nil
}

// startRotation records the seal job of a rotation that has happened:
// rows of key id below version v (of tenant, when given) are to be
// rewritten. check runs again in the transaction, so a job that appeared
// meanwhile refuses this one.
func (s *Session) startRotation(ctx context.Context, desc *catalog.TableDescriptor, id int64, v int, tenant []byte, keepOld bool, lease time.Duration, check func(context.Context, *kvclient.Txn) error) *Error {
	now := time.Now()
	err := s.db.RunTxn(ctx, "rotate-key-job", func(ctx context.Context, txn *kvclient.Txn) error {
		if err := check(ctx, txn); err != nil {
			return err
		}
		return putSealJob(ctx, txn, &catalog.SealJob{
			TableID: desc.ID, Table: desc.Name, Kind: catalog.SealJobRotate,
			KeyID: id, Version: v, Tenant: tenant, KeepOld: keepOld,
			NotBefore: now.Add(lease + rotationSlack).UnixNano(), Pass: 1, Created: now.UnixNano(),
		})
	})
	if err != nil {
		se := ToSQLError(err)
		se.Msg = fmt.Sprintf("the key is rotated to version %d, but its rows' rewrite was not scheduled: %s", v, se.Msg)
		return se
	}
	return nil
}

// sealFuncTable is the common opening of the functions of sealed
// tables: the arguments (null is false: the call returns NULL, as
// PostgreSQL's strict functions do), the transaction block, the cluster
// version, and ownership of the table the first argument names.
func (s *Session) sealFuncTable(ctx context.Context, txn *kvclient.Txn, fn string, args []types.Datum, nmin, nmax int, usage string) (*catalog.TableDescriptor, bool, error) {
	if len(args) < nmin || len(args) > nmax {
		return nil, false, newErrf(CodeUndefinedFunction, "%s() takes %s", fn, usage)
	}
	for _, a := range args {
		if a.Null {
			return nil, false, nil
		}
	}
	if s.state == StateOpen {
		return nil, false, newErrf(CodeActiveTransaction, "%s() cannot run inside a transaction block: it works in transactions of its own", fn)
	}
	if s.db.ClusterVersion() < version.V22 {
		return nil, false, newErrf(CodeFeatureNotSupported, "%s() needs cluster version %s: finalize the upgrade with `datax debug upgrade` first", fn, version.V22)
	}
	desc, err := s.lookup(ctx, txn, args[0].Text())
	if err != nil {
		return nil, false, err
	}
	if err := mustBeReal(desc); err != nil {
		return nil, false, err
	}
	if err := s.checkTableOwner(ctx, txn, desc); err != nil {
		return nil, false, err
	}
	return desc, true, nil
}

// tenantKeyOf resolves a tenant of a per-tenant table to its live key
// (ErrKeyNotFound: it has none).
func (s *Session) tenantKeyOf(ctx context.Context, fn string, desc *catalog.TableDescriptor, tenantArg types.Datum) ([]byte, int64, error) {
	if !desc.SealedPerTenant() {
		return nil, 0, newErrf(CodeWrongObjectType, "table %q is not sealed per tenant (encryption_key_by), so it has no tenant keys", desc.Name)
	}
	tenant, err := rowseal.TenantValue(desc, tenantArg)
	if err != nil {
		return nil, 0, newErrf(CodeInvalidTextRepresentation, "%s: %v", fn, err)
	}
	b := s.rowSealer()
	ts, ok := b.Store().(rowseal.TenantKeyStore)
	if !ok {
		return nil, 0, newErrf(CodeFeatureNotSupported, "per-tenant keys need a key store that keeps tenant keys, and this node has none")
	}
	name, err := b.Tenants().Name(ctx, desc, tenant)
	if err != nil {
		return nil, 0, err
	}
	id, err := ts.TenantKey(ctx, name, false)
	return tenant, id, err
}

// evalRotateTenantKey runs datax_rotate_tenant_key(table, tenant [,
// keep_old]): the tenant's key gets a new version, which the tenant's
// new writes use, and a seal job rewrites the tenant's rows under older
// versions and then destroys them (unless keep_old). It returns the new
// version, or NULL for a tenant that has no key.
func (s *Session) evalRotateTenantKey(ctx context.Context, txn *kvclient.Txn, args []types.Datum) (types.Datum, error) {
	desc, ok, err := s.sealFuncTable(ctx, txn, rotateTenantKeyFunc, args, 2, 3, "a table, a tenant (a value of its tenant column) and optionally keep_old (boolean)")
	if err != nil || !ok {
		return types.DNull, err
	}
	keepOld := false
	if len(args) == 3 {
		switch t := strings.ToLower(args[2].Text()); {
		case args[2].Fam == types.Bool:
			keepOld = args[2].B
		case t == "true" || t == "t" || t == "on" || t == "yes" || t == "1":
			keepOld = true
		case t == "false" || t == "f" || t == "off" || t == "no" || t == "0":
		default:
			return types.Datum{}, newErrf(CodeInvalidTextRepresentation, "%s: keep_old must be a boolean, not %q", rotateTenantKeyFunc, args[2].Text())
		}
	}
	rs, serr := s.rotatingStore(rotateTenantKeyFunc + "()")
	if serr != nil {
		return types.Datum{}, serr
	}
	tenant, id, err := s.tenantKeyOf(ctx, rotateTenantKeyFunc, desc, args[1])
	if errors.Is(err, rowseal.ErrKeyNotFound) {
		return types.DNull, nil
	}
	if err != nil {
		return types.Datum{}, err
	}
	check := func(ctx context.Context, txn *kvclient.Txn) error {
		cur, err := catalog.ReadTable(ctx, txn, desc.ID)
		if err != nil {
			return err
		}
		if cur == nil || !cur.SealedPerTenant() {
			return newErrf(CodeObjectNotInState, "table %q changed; call %s again", desc.Name, rotateTenantKeyFunc)
		}
		if j, err := sealJobExists(ctx, txn, desc.ID); err != nil {
			return err
		} else if j != nil {
			return jobBusyError(desc, j)
		}
		return nil
	}
	if err := s.db.RunTxn(ctx, "rotate-tenant-key-check", check); err != nil {
		return types.Datum{}, err
	}
	v, err := rs.RotateKey(ctx, id)
	if err != nil {
		return types.Datum{}, fmt.Errorf("rotating a tenant's key of table %q: %w", desc.Name, err)
	}
	s.rowSealer().Invalidate(id)
	if serr := s.startRotation(ctx, desc, id, v, tenant, keepOld, rs.Lease(), check); serr != nil {
		return types.Datum{}, serr
	}
	log.Audit("key-ddl", "stmt", rotateTenantKeyFunc, "target", desc.Name, "key_id", id, "outcome", fmt.Sprintf("version %d; older versions rewritten in the background", v), "principal", s.sessionUser, "role", s.actor())
	return types.NewInt(int64(v)), nil
}

// evalDestroyOldKeyVersions runs datax_destroy_old_key_versions(table [,
// tenant]): every version of the table's key (a tenant's key, for a
// per-tenant table) but the current one is destroyed now, whether or not
// rows are still sealed under it — they, and MVCC history written under
// it, are unreadable from then on. It returns how many versions it
// destroyed (NULL for a tenant without a key), once every gateway has
// stopped using them.
func (s *Session) evalDestroyOldKeyVersions(ctx context.Context, txn *kvclient.Txn, args []types.Datum) (types.Datum, error) {
	desc, ok, err := s.sealFuncTable(ctx, txn, destroyOldKeyVersionsFunc, args, 1, 2, "a table and, for a table sealed per tenant, a tenant")
	if err != nil || !ok {
		return types.DNull, err
	}
	rs, serr := s.rotatingStore(destroyOldKeyVersionsFunc + "()")
	if serr != nil {
		return types.Datum{}, serr
	}
	if !desc.Sealed() {
		return types.Datum{}, newErrf(CodeObjectNotInState, "table %q is not sealed, so it has no key", desc.Name)
	}
	id := desc.EncryptionKey.KeyID
	switch {
	case desc.SealedPerTenant() && len(args) == 2:
		_, tid, err := s.tenantKeyOf(ctx, destroyOldKeyVersionsFunc, desc, args[1])
		if errors.Is(err, rowseal.ErrKeyNotFound) {
			return types.DNull, nil
		}
		if err != nil {
			return types.Datum{}, err
		}
		id = tid
	case desc.SealedPerTenant():
		return types.Datum{}, newErrf(CodeUndefinedFunction, "table %q is sealed per tenant: name the tenant whose key's old versions to destroy, %s('%s', <tenant>)", desc.Name, destroyOldKeyVersionsFunc, desc.Name)
	case len(args) == 2:
		return types.Datum{}, newErrf(CodeWrongObjectType, "table %q is sealed whole: it has one key, and no tenants", desc.Name)
	}
	n, err := rs.DestroyKeyVersions(ctx, id, 1<<30)
	s.rowSealer().Invalidate(id)
	if err != nil {
		return types.Datum{}, err
	}
	log.Audit("key-ddl", "stmt", destroyOldKeyVersionsFunc, "target", desc.Name, "key_id", id, "outcome", fmt.Sprintf("%d versions destroyed", n), "principal", s.sessionUser, "role", s.actor())
	if n > 0 {
		if serr := s.waitKeyLease(ctx); serr != nil {
			return types.Datum{}, serr
		}
	}
	return types.NewInt(int64(n)), nil
}

// ---- the seal job ----

// SealJobOutcome is what one step of a seal job came to.
type SealJobOutcome int

const (
	// SealJobNone: there is no such job.
	SealJobNone SealJobOutcome = iota
	// SealJobWaiting: the job's start condition is not met yet.
	SealJobWaiting
	// SealJobWorking: the step did a batch; there is more to do.
	SealJobWorking
	// SealJobDone: the job finished and its record is gone.
	SealJobDone
	// SealJobGone: the table, or the key, is gone; so is the record.
	SealJobGone
)

// SealJobStepResult is what one step did.
type SealJobStepResult struct {
	Outcome SealJobOutcome
	// Job is the record as the step left it (as it was, when done).
	Job *catalog.SealJob
	// Rewritten and Unreadable count the step's rows; Destroyed the key
	// versions a finished rotation destroyed.
	Rewritten, Unreadable, Destroyed int
}

// SealJobStep does one step of table id's seal job: when the job has not
// started, checks whether it may; then rewrites up to batch rows of the
// current pass, from where the last step stopped, in one transaction
// that also records the progress; at the end of a pass that found
// nothing to rewrite, finishes the job. now is the job's clock. The
// caller runs steps until the outcome is not SealJobWorking (the node
// leading range 1, pkg/server/sealjobs.go).
func (s *Session) SealJobStep(ctx context.Context, id uint64, batch int, now func() time.Time) (SealJobStepResult, error) {
	if batch <= 0 {
		batch = 100
	}
	var res SealJobStepResult
	// In a transaction, which pushes a lock left on the record by a batch
	// whose node stopped (RunSealJobsOnce).
	var raw []byte
	err := s.db.RunTxn(ctx, "seal-job-read", func(ctx context.Context, txn *kvclient.Txn) error {
		var err error
		raw, err = txn.Get(ctx, keys.SealJobKey(id))
		return err
	})
	if err != nil {
		return res, err
	}
	if raw == nil {
		return res, nil
	}
	job, err := catalog.DecodeSealJob(raw)
	if err != nil {
		return res, err
	}
	res.Job = job
	if job.Done {
		return s.finishRotation(ctx, job)
	}
	if !job.Started {
		ok, err := s.sealJobMayStart(ctx, job, now())
		if err != nil || !ok {
			res.Outcome = SealJobWaiting
			return res, err
		}
	}
	b := s.rowSealer()
	var finished, gone bool
	err = s.db.RunTxn(ctx, "seal-job", func(ctx context.Context, txn *kvclient.Txn) error {
		res.Rewritten, res.Unreadable, finished, gone = 0, 0, false, false
		raw, err := txn.GetForUpdate(ctx, keys.SealJobKey(id))
		if err != nil {
			return err
		}
		if raw == nil {
			res.Job = nil
			return nil
		}
		j, err := catalog.DecodeSealJob(raw)
		if err != nil {
			return err
		}
		res.Job = j
		desc, err := catalog.ReadTable(ctx, txn, id)
		if err != nil {
			return err
		}
		if desc == nil || !desc.Sealed() || (j.Kind == catalog.SealJobSeal && !desc.EncryptionKey.Sealing) ||
			(j.Kind == catalog.SealJobRotate && j.Tenant == nil && desc.EncryptionKey.KeyID != j.KeyID) {
			gone = true
			return txn.Delete(ctx, keys.SealJobKey(id))
		}
		j.Started = true
		lo := append(rowenc.PrimaryKeyPrefixFor(desc), j.Tenant...)
		hi := keys.Key(lo).PrefixEnd()
		from := lo
		if len(j.Resume) > 0 && keys.Key(j.Resume).Compare(lo) > 0 {
			from = j.Resume
		}
		kvs, err := txn.Scan(ctx, from, hi, int64(batch))
		if err != nil {
			return err
		}
		var wb kvclient.WriteBatch
		for _, kv := range kvs {
			if !sealJobWants(desc, j, kv.Value) {
				continue
			}
			plain, err := b.OpenValue(ctx, desc, kv.Key, kv.Value)
			if err != nil {
				var ku *rowseal.KeyUnavailableError
				if errors.As(err, &ku) && rowseal.KeyGone(ku.Err) {
					// No one can read the row: its key, or the version it
					// is under, was destroyed. There is nothing to move.
					res.Unreadable++
					continue
				}
				return err
			}
			stored, err := b.SealValue(ctx, desc, kv.Key, plain)
			if err != nil {
				return err
			}
			wb.Put(kv.Key, stored)
			res.Rewritten++
		}
		if err := txn.RunBatch(ctx, &wb); err != nil {
			return err
		}
		j.Rewritten += int64(res.Rewritten)
		j.PassRewritten += int64(res.Rewritten)
		j.PassUnreadable += int64(res.Unreadable)
		j.Updated, j.LastError = now().UnixNano(), ""
		switch {
		case len(kvs) == batch:
			j.Resume = keys.Key(kvs[len(kvs)-1].Key).Next()
		case j.PassRewritten == 0:
			// A whole pass, begun once every gateway wrote as the job
			// wants, found nothing to rewrite: done.
			finished = true
			return s.sealJobFinishIn(ctx, txn, desc, j)
		default:
			j.Resume, j.Pass, j.PassRewritten, j.PassUnreadable = nil, j.Pass+1, 0, 0
		}
		out, err := j.Encode()
		if err != nil {
			return err
		}
		return txn.Put(ctx, keys.SealJobKey(id), out)
	})
	if err != nil {
		return res, err
	}
	switch {
	case res.Job == nil:
		res.Outcome = SealJobNone
	case gone:
		res.Outcome = SealJobGone
	case finished && res.Job.Kind == catalog.SealJobRotate:
		done, err := s.finishRotation(ctx, res.Job)
		done.Rewritten, done.Unreadable = res.Rewritten, res.Unreadable
		return done, err
	case finished:
		res.Outcome = SealJobDone
	default:
		res.Outcome = SealJobWorking
	}
	return res, nil
}

// sealJobWants reports whether a stored value is one the job rewrites.
func sealJobWants(desc *catalog.TableDescriptor, j *catalog.SealJob, v []byte) bool {
	if j.Kind == catalog.SealJobSeal {
		return !rowseal.IsSealed(v)
	}
	if !rowseal.IsSealed(v) {
		return false
	}
	if desc.SealedPerTenant() {
		id, ver, err := rowseal.TenantKeyOf(v)
		return err == nil && id == j.KeyID && ver < j.Version
	}
	ver, err := rowseal.Version(v)
	return err == nil && ver < j.Version
}

// sealJobMayStart reports whether every gateway now writes as the job
// wants: for a seal job, every lease has adopted the descriptor that
// sealed the table; for a rotation, the key cache's lease has passed
// since the rotation.
func (s *Session) sealJobMayStart(ctx context.Context, j *catalog.SealJob, now time.Time) (bool, error) {
	if j.Kind == catalog.SealJobRotate {
		return now.UnixNano() >= j.NotBefore, nil
	}
	return s.cat.LeasesAdopted(ctx, j.TableID, j.DescVersion)
}

// sealJobFinishIn ends a job whose last pass found nothing to rewrite,
// in the step's transaction (which wrote no row): a seal job clears the
// descriptor's Sealing and removes its record; a rotation marks its
// record Done, and finishRotation destroys the older versions and
// removes it.
func (s *Session) sealJobFinishIn(ctx context.Context, txn *kvclient.Txn, desc *catalog.TableDescriptor, j *catalog.SealJob) error {
	if j.Kind == catalog.SealJobRotate {
		j.Done = true
		out, err := j.Encode()
		if err != nil {
			return err
		}
		return txn.Put(ctx, keys.SealJobKey(j.TableID), out)
	}
	d := desc.Clone()
	d.EncryptionKey.Sealing = false
	if err := s.cat.Update(ctx, txn, d); err != nil {
		return err
	}
	return txn.Delete(ctx, keys.SealJobKey(j.TableID))
}

// finishRotation destroys the versions of a finished rotation's key below
// its new one, unless the job keeps them, and removes the record. A
// failure leaves the record Done, and the next step tries again.
func (s *Session) finishRotation(ctx context.Context, j *catalog.SealJob) (SealJobStepResult, error) {
	res := SealJobStepResult{Job: j}
	if !j.KeepOld {
		rs, ok := s.rowSealer().Store().(rowseal.RotatingKeyStore)
		if !ok {
			return res, errors.New("this node has no key store to destroy the key's older versions with")
		}
		n, err := rs.DestroyKeyVersions(ctx, j.KeyID, j.Version)
		if err != nil && !errors.Is(err, rowseal.ErrKeyDestroyed) && !errors.Is(err, rowseal.ErrKeyNotFound) {
			return res, err
		}
		s.rowSealer().Invalidate(j.KeyID)
		res.Destroyed = n
	}
	err := s.db.RunTxn(ctx, "seal-job-done", func(ctx context.Context, txn *kvclient.Txn) error {
		return txn.Delete(ctx, keys.SealJobKey(j.TableID))
	})
	if err != nil {
		return res, err
	}
	res.Outcome = SealJobDone
	return res, nil
}

// NoteSealJobError records a failed step's error in table id's job
// record, for datax_encryption_jobs; the job retries on its next turn.
func (s *Session) NoteSealJobError(ctx context.Context, id uint64, cause error, now time.Time) error {
	return s.db.RunTxn(ctx, "seal-job-error", func(ctx context.Context, txn *kvclient.Txn) error {
		raw, err := txn.Get(ctx, keys.SealJobKey(id))
		if err != nil || raw == nil {
			return err
		}
		j, err := catalog.DecodeSealJob(raw)
		if err != nil {
			return err
		}
		j.LastError, j.Updated = cause.Error(), now.UnixNano()
		out, err := j.Encode()
		if err != nil {
			return err
		}
		return txn.Put(ctx, keys.SealJobKey(id), out)
	})
}

// listSealJobs reads the seal jobs of the tables a virtual table lists.
func (s *Session) listSealJobs(ctx context.Context, txn *kvclient.Txn) ([]vtable.SealJobInfo, error) {
	if s.db.ClusterVersion() < version.V22 {
		return nil, nil
	}
	lo, hi := keys.SealJobSpan()
	kvs, err := txn.Scan(ctx, lo, hi, 0)
	if err != nil {
		return nil, err
	}
	out := make([]vtable.SealJobInfo, 0, len(kvs))
	for _, kv := range kvs {
		j, err := catalog.DecodeSealJob(kv.Value)
		if err != nil {
			continue
		}
		out = append(out, vtable.SealJobInfo{
			TableID: j.TableID, Table: j.Table, Kind: j.Kind, Tenant: j.Tenant != nil, KeyID: j.KeyID, Version: j.Version,
			KeepOld: j.KeepOld, State: j.State(), Pass: j.Pass, Rewritten: j.Rewritten, Unreadable: j.PassUnreadable,
			Created: j.Created, Updated: j.Updated, LastError: j.LastError,
		})
	}
	return out, nil
}
