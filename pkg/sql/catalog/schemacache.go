package catalog

import (
	"context"
	"encoding/json"
	"strconv"
	"sync"

	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/kvclient"
	"github.com/sthorne/datax/pkg/util/encoding"
	"github.com/sthorne/datax/pkg/util/log"
)

// Schema-set leases. Resolving an unqualified name walks the search
// path, and every schema of it but public is a name to look up in the
// database's schema namespace: under the default path ("$user", public)
// that is the current role's own name, on every statement, and almost
// always a miss. Read in the statement's transaction, it was one more
// KV round trip per statement. A leasing gateway instead caches each
// database's whole schema set — every schema's name and descriptor,
// read in one transaction — and holds a lease on it, as it does on a
// table descriptor (lease.go), so a miss is answered from memory too.
// The set also holds the name entries of the tables filed in those
// schemas: under a path such as (app, public), or "$user" naming a role
// that has its own schema, every unqualified name of a public table is
// first looked up in the schema before it, and that miss too is answered
// from the set rather than read (schemaSet.tableAbsent). Such an answer
// is trusted only when a schema later on the path then holds the name:
// a lookup that finds the name nowhere reads the entries it skipped
// before it reports the table missing. So the set needs to be current
// only for a name that two schemas hold — that one shadows the other —
// and a transaction that files a table or view under a name that
// another schema of its database holds too (CREATE, ALTER ... RENAME
// TO; bumpSchemaGenIfShared), or moves one between schemas (ALTER ...
// SET SCHEMA), changes the schema generation like a schema change, and
// drains. A table created under a name of its own, the common case,
// does not wait for the drain.
//
// The set is versioned by the database's schema generation
// (keys.SchemaGenKey), which every transaction that changes the
// database's schemas increments: CREATE, DROP and ALTER SCHEMA (RENAME,
// OWNER), GRANT and REVOKE on a schema (USAGE decides whether a schema
// of the path is searched: Resolution.CanUse is asked of the cached
// descriptor), DROP OWNED and REASSIGN OWNED through them, DROP DATABASE
// of a database with schemas, a restore that writes schemas or tables
// in them, and the table changes above. The rules are the table leases'
// own:
//
//   - The set and this gateway's lease record (keys.SchemaLeaseKey: the
//     generation and an expiration) are read and written in ONE
//     serializable transaction (acquireSchemaLease), so the record never
//     claims a generation the cached set does not reflect: a schema
//     change committing after the read fails the lease transaction's
//     refresh, and the retry reads the new set.
//   - The cached set expires with the lease record; the renewal loop
//     re-reads and re-leases it every TTL/3, adopting new generations,
//     and gives up the lease of a set no statement has used for the idle
//     time, as for a table. Between renewals it watches the generations
//     (every TTL/20, one read for all of them: pollSchemaGens) and
//     re-leases a set whose generation moved at once, so a schema
//     change's drain waits about that long, not a renewal period.
//   - After a transaction that changed a database's schemas commits, the
//     session drains it (FinishSchemaChanges): it re-leases the set here
//     and waits until every live lease on it is at the new generation.
//     So once CREATE SCHEMA (or DROP, RENAME, GRANT ...) returns, every
//     gateway resolves names through the new set: a schema named after
//     the role is found from then on by every session on every node, and
//     a dropped or renamed one no longer is. Between the commit and the
//     drain a gateway may still resolve through the old set, as a table
//     DDL's drain allows.
//   - Between the lease transaction's commit and the new set's install,
//     the old set is not handed out (schemaAdopting), so a drain that has
//     seen this gateway adopt cannot be followed by a lookup here that
//     does not.
//
// A lookup reads the catalog in its own transaction, as before, when the
// cache cannot answer for it: on an accessor without leasing; in a
// historical (AS OF SYSTEM TIME) transaction, which must see the schemas
// of its timestamp; in a transaction that has itself changed the
// database's schemas (it wrote the generation key), which must read
// through its own writes; and while the set is being leased by another
// lookup, or could not be.
//
// The statements that write a schema's ID into an object — CREATE of a
// table, view, sequence or type (ResolveTarget), ALTER ... SET SCHEMA —
// resolve the schema in their own transaction and never from the cache,
// so a DROP SCHEMA committing concurrently conflicts with them rather
// than leaving an object filed under a schema that is gone.
//
// Unlike a table's, a statement that resolved names through the cached
// set takes no commit deadline from it, and a drain does not wait for
// such statements to finish: as in PostgreSQL, a statement keeps the
// resolution it made, and what it then reads or writes is guarded by
// the leases on the tables themselves.

// schemaSet is one database's schemas other than public, by name, at a
// schema generation, with the name entries of the tables filed in them.
// Immutable once built: lookups share it, so the descriptors in it must
// not be modified.
type schemaSet struct {
	gen    uint64
	byName map[string]*SchemaDescriptor
	// tables holds the name entries (SchemaNamespaceName) of every table
	// and view in the schemas of byName, so that resolving a name through
	// a schema of the path that has no such table — a table of public
	// behind it — is answered without a read; nil when the database has
	// too many to cache, and a miss is then read in the transaction.
	tables map[string]struct{}
}

// tableAbsent reports whether the set answers that schema schemaID
// (not public) holds no table under name entry entry.
func (s *schemaSet) tableAbsent(schemaID uint64, entry string) bool {
	if s == nil || s.tables == nil || schemaID == 0 {
		return false
	}
	_, ok := s.tables[entry]
	return !ok
}

// maxCachedSchemaTables bounds the table name entries a schema set
// caches: a database with more tables outside public than this caches
// none, and a table lookup that misses in a schema reads the catalog.
const maxCachedSchemaTables = 10000

// listSchemaTableEntries reads the name entries of the tables of
// database dbID filed in a schema other than public, or nil when there
// are more than maxCachedSchemaTables.
func listSchemaTableEntries(ctx context.Context, txn *kvclient.Txn, dbID uint64) (map[string]struct{}, error) {
	lo, hi := keys.TableNamespacePrefixSpan(dbID, schemaMarker)
	rows, err := txn.Scan(ctx, lo, hi, maxCachedSchemaTables+1)
	if err != nil || len(rows) > maxCachedSchemaTables {
		return nil, err
	}
	base, _ := keys.TableNamespaceSpan(dbID)
	out := make(map[string]struct{}, len(rows))
	for _, kv := range rows {
		_, entry, err := encoding.DecodeString(kv.Key[len(base):])
		if err != nil {
			return nil, nil // not answerable: misses are read in the transaction
		}
		out[entry] = struct{}{}
	}
	return out, nil
}

// cachedSchemaSet is a leased schema set: it may be used until
// expiration, the value in this gateway's lease record.
type cachedSchemaSet struct {
	set        *schemaSet
	expiration int64
	lastUsed   int64
}

// readSchemaGen is database dbID's schema generation (0 before any
// schema change).
func readSchemaGen(ctx context.Context, txn *kvclient.Txn, dbID uint64) (uint64, error) {
	raw, err := txn.Get(ctx, keys.SchemaGenKey(dbID))
	if err != nil || raw == nil {
		return 0, err
	}
	return strconv.ParseUint(string(raw), 10, 64)
}

// bumpSchemaGen increments database dbID's schema generation in txn:
// every change to the database's schemas calls it, so that gateways
// caching the schema set drop it and the session drains it after the
// commit (FinishSchemaChanges).
func bumpSchemaGen(ctx context.Context, txn *kvclient.Txn, dbID uint64) error {
	gen, err := readSchemaGen(ctx, txn, dbID)
	if err != nil {
		return err
	}
	return txn.Put(ctx, keys.SchemaGenKey(dbID), []byte(strconv.FormatUint(gen+1, 10)))
}

// bumpSchemaGenIfShared bumps database dbID's schema generation, in the
// transaction that files a table or view under name in schema schemaID
// (0: public), when a table or view of the same name is filed in another
// schema of the database (schemacache.go: the schema sets must then
// hold both). It reads those name entries in txn, so a transaction
// filing the other one concurrently conflicts with this one.
func bumpSchemaGenIfShared(ctx context.Context, txn *kvclient.Txn, dbID, schemaID uint64, name string, isDefault bool) error {
	if dbID == 0 {
		return nil // before the migration: no schemas
	}
	lo, hi := keys.SchemaNamespaceSpan(dbID)
	rows, err := txn.Scan(ctx, lo, hi, 0)
	if err != nil || len(rows) == 0 {
		return err
	}
	ks := make([]keys.Key, 0, len(rows)+2)
	if schemaID != 0 {
		ks = append(ks, keys.TableNamespaceKey(dbID, name))
		if isDefault {
			ks = append(ks, keys.NamespaceKey(name))
		}
	}
	for _, kv := range rows {
		id, err := strconv.ParseUint(string(kv.Value), 10, 64)
		if err != nil || id == schemaID {
			continue
		}
		ks = append(ks, keys.TableNamespaceKey(dbID, SchemaNamespaceName(id, name)))
	}
	if len(ks) == 0 {
		return nil
	}
	vals, err := txn.GetBatch(ctx, ks)
	if err != nil {
		return err
	}
	for _, v := range vals {
		if v != nil {
			return bumpSchemaGen(ctx, txn, dbID)
		}
	}
	return nil
}

// BumpSchemaGen is bumpSchemaGen for a writer of schema keys outside
// this package (a restore).
func BumpSchemaGen(ctx context.Context, txn *kvclient.Txn, dbID uint64) error {
	return bumpSchemaGen(ctx, txn, dbID)
}

// schemaGenDB decodes the database ID of a schema generation key.
func schemaGenDB(k keys.Key) (uint64, bool) {
	lo, hi := keys.AllSchemaGenSpan()
	if k.Compare(lo) < 0 || k.Compare(hi) >= 0 {
		return 0, false
	}
	_, id, err := encoding.DecodeUint64(k[len(lo):])
	return id, err == nil
}

// schemaAcquireMu is the mutex that serializes this gateway's lease
// transactions on database dbID's schema set: with one at a time, the
// set installed is always the one the lease record last written claims.
func (a *Accessor) schemaAcquireMu(dbID uint64) *sync.Mutex {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.schemaAcq == nil {
		a.schemaAcq = make(map[uint64]*sync.Mutex)
	}
	m := a.schemaAcq[dbID]
	if m == nil {
		m = &sync.Mutex{}
		a.schemaAcq[dbID] = m
	}
	return m
}

// schemaSnapshot returns database dbID's leased schema set for a lookup
// in txn, leasing it on first use, or nil when the lookup must read the
// catalog in txn (see above for when).
func (a *Accessor) schemaSnapshot(ctx context.Context, txn *kvclient.Txn, dbID uint64) *schemaSet {
	if a == nil || !a.leasing {
		return nil
	}
	if txn != nil && (txn.Historical() || (txn.HasWrites() && txn.HasWritten(keys.SchemaGenKey(dbID)))) {
		return nil
	}
	a.mu.Lock()
	now := a.nowWallLocked()
	if c, ok := a.schemaSets[dbID]; ok && now < c.expiration {
		if gen, adopting := a.schemaAdopting[dbID]; !adopting || gen <= c.set.gen {
			c.lastUsed = now
			set := c.set
			a.mu.Unlock()
			return set
		}
		// A newer generation's lease is committing or committed and not
		// installed yet: the old set must not be handed out.
		a.mu.Unlock()
		return nil
	}
	a.mu.Unlock()
	set, err := a.acquireSchemaLease(ctx, dbID, false, true)
	if err != nil {
		log.Debugf("leasing the schemas of database %d: %v", dbID, err)
		return nil
	}
	return set
}

// acquireSchemaLease reads database dbID's schema set and records this
// gateway's lease on it at the set's generation, in one transaction,
// then installs the set in the cache. With wait false it gives up (nil,
// nil) rather than wait for a lease transaction already running on the
// set; use says the acquisition is a statement's use of the set (a
// renewal is not).
func (a *Accessor) acquireSchemaLease(ctx context.Context, dbID uint64, wait, use bool) (*schemaSet, error) {
	mu := a.schemaAcquireMu(dbID)
	if wait {
		mu.Lock()
	} else if !mu.TryLock() {
		return nil, nil
	}
	defer mu.Unlock()
	var set *schemaSet
	var exp int64
	marked := false
	err := a.db.RunTxn(ctx, "schema-lease", func(ctx context.Context, txn *kvclient.Txn) error {
		set = nil
		gen, err := readSchemaGen(ctx, txn, dbID)
		if err != nil {
			return err
		}
		schemas, err := ListSchemas(ctx, txn, dbID)
		if err != nil {
			return err
		}
		byName := make(map[string]*SchemaDescriptor, len(schemas))
		for _, sd := range schemas {
			byName[sd.Name] = sd
		}
		// No table can be filed under a schema when there is none.
		tables := map[string]struct{}{}
		if len(schemas) > 0 {
			if tables, err = listSchemaTableEntries(ctx, txn, dbID); err != nil {
				return err
			}
		}
		e := a.clock.Now().WallTime + a.ttl.Nanoseconds()
		raw, err := json.Marshal(descLease{Version: gen, Expiration: e})
		if err != nil {
			return err
		}
		// As acquireLease: the write rides with the commit, and the reads
		// before it are validated there.
		txn.EnablePipelining()
		var wb kvclient.WriteBatch
		wb.Put(keys.SchemaLeaseKey(dbID, a.gateway), raw)
		if err := txn.RunBatch(ctx, &wb); err != nil {
			return err
		}
		set, exp = &schemaSet{gen: gen, byName: byName, tables: tables}, e
		a.mu.Lock()
		if a.schemaAdopting == nil {
			a.schemaAdopting = make(map[uint64]uint64)
		}
		a.schemaAdopting[dbID] = gen
		a.mu.Unlock()
		marked = true
		return nil
	})
	a.mu.Lock()
	defer a.mu.Unlock()
	if marked {
		delete(a.schemaAdopting, dbID)
	}
	if err != nil || set == nil {
		// The lease transaction failed, and may still have committed (an
		// ambiguous result): the cached set may be older than the record,
		// so it goes, and lookups read the catalog until the next lease.
		delete(a.schemaSets, dbID)
		return nil, err
	}
	now := a.nowWallLocked()
	entry := &cachedSchemaSet{set: set, expiration: exp, lastUsed: now}
	if old, ok := a.schemaSets[dbID]; ok && !use {
		entry.lastUsed = old.lastUsed
	}
	if a.schemaSets == nil {
		a.schemaSets = make(map[uint64]*cachedSchemaSet)
	}
	a.schemaSets[dbID] = entry
	return set, nil
}

// refreshSchemaSet re-leases database dbID's schema set at once, unless
// a lease transaction on it is already running: a lookup found a table
// the set said a schema did not hold, one created since under a name no
// other schema holds, which does not change the generation (see above).
func (a *Accessor) refreshSchemaSet(ctx context.Context, dbID uint64) {
	if _, err := a.acquireSchemaLease(ctx, dbID, false, true); err != nil {
		log.Debugf("re-leasing the schemas of database %d: %v", dbID, err)
	}
}

// renewSchemaSets gives up the leases of the schema sets no statement
// has used for the idle time and renews the rest, adopting any new
// generation; the first error is returned after every set is tried.
func (a *Accessor) renewSchemaSets(ctx context.Context) error {
	a.mu.Lock()
	idle := a.idleAfter
	if idle <= 0 {
		idle = idleLeasePeriods * a.ttl
	}
	now := a.nowWallLocked()
	type released struct {
		dbID uint64
		exp  int64
	}
	var gone []released
	dbs := make([]uint64, 0, len(a.schemaSets))
	for dbID, c := range a.schemaSets {
		if now-c.lastUsed >= idle.Nanoseconds() {
			delete(a.schemaSets, dbID)
			gone = append(gone, released{dbID, c.expiration})
			continue
		}
		dbs = append(dbs, dbID)
	}
	a.mu.Unlock()
	for _, r := range gone {
		a.releaseSchemaLease(ctx, r.dbID, r.exp)
	}
	var first error
	for _, dbID := range dbs {
		if _, err := a.acquireSchemaLease(ctx, dbID, true, false); err != nil && first == nil {
			first = err
		}
	}
	return first
}

// pollSchemaGens re-leases at once every cached schema set whose
// database's schema generation has moved, rather than at the next
// renewal: a schema change drains as soon as every gateway has adopted
// it, so this is what bounds how long CREATE SCHEMA and the like wait.
// One inconsistent read of the generations, which leaves the intents of
// a schema change in flight alone (an uncommitted generation is not one
// to adopt); nothing when no set is cached. The lease transaction is
// what adopts, as on renewal.
func (a *Accessor) pollSchemaGens(ctx context.Context) error {
	a.mu.Lock()
	if len(a.schemaSets) == 0 {
		a.mu.Unlock()
		return nil
	}
	cached := make(map[uint64]uint64, len(a.schemaSets))
	for dbID, c := range a.schemaSets {
		cached[dbID] = c.set.gen
	}
	now := a.clock.Now()
	a.mu.Unlock()
	lo, hi := keys.AllSchemaGenSpan()
	rows, err := a.db.ScanAt(ctx, lo, hi, 0, now)
	if err != nil {
		return err
	}
	var first error
	for _, kv := range rows {
		dbID, ok := schemaGenDB(kv.Key)
		if !ok {
			continue
		}
		have, isCached := cached[dbID]
		gen, err := strconv.ParseUint(string(kv.Value), 10, 64)
		if !isCached || err != nil || gen <= have {
			continue
		}
		if _, err := a.acquireSchemaLease(ctx, dbID, true, false); err != nil && first == nil {
			first = err
		}
	}
	return first
}

// releaseSchemaLease deletes this gateway's lease record on database
// dbID's schema set, provided it is still the record of the entry given
// up (as releaseLease). Nothing can still be resolving through that
// entry: a lookup hands out the set and is done with it.
func (a *Accessor) releaseSchemaLease(ctx context.Context, dbID uint64, expiration int64) {
	key := keys.SchemaLeaseKey(dbID, a.gateway)
	err := a.db.RunTxn(ctx, "schema-lease-release", func(ctx context.Context, txn *kvclient.Txn) error {
		raw, err := txn.Get(ctx, key)
		if err != nil || raw == nil {
			return err
		}
		var l descLease
		if json.Unmarshal(raw, &l) != nil || l.Expiration != expiration {
			return nil
		}
		return txn.Delete(ctx, key)
	})
	if err != nil {
		log.Debugf("releasing the lease on the schemas of database %d: %v", dbID, err)
	}
}

// FinishSchemaChanges runs after txn commits: for every database whose
// schemas txn changed (it wrote the database's schema generation), it
// re-leases the schema set here and drains — blocks until every live
// gateway lease on the set has adopted the new generation. Nil for an
// unleased accessor.
func (a *Accessor) FinishSchemaChanges(ctx context.Context, txn *kvclient.Txn) error {
	if txn == nil || !a.leasing {
		return nil
	}
	lo, hi := keys.AllSchemaGenSpan()
	for _, k := range txn.WrittenIn(lo, hi) {
		dbID, ok := schemaGenDB(k)
		if !ok {
			continue
		}
		if err := a.FinishSchemaChange(ctx, dbID); err != nil {
			return err
		}
	}
	return nil
}

// FinishSchemaChange is FinishSchemaChanges for one database, whose
// schemas a committed transaction changed.
func (a *Accessor) FinishSchemaChange(ctx context.Context, dbID uint64) error {
	if !a.leasing {
		return nil
	}
	set, err := a.acquireSchemaLease(ctx, dbID, true, false)
	if err != nil {
		return err
	}
	lo, hi := keys.SchemaLeaseSpan(dbID)
	return a.waitForAdoptionIn(ctx, lo, hi, set.gen, "the schemas of database "+strconv.FormatUint(dbID, 10))
}

// TestingSchemaSetCached reports whether this accessor holds a live
// leased schema set for database dbID, and its generation.
func (a *Accessor) TestingSchemaSetCached(dbID uint64) (gen uint64, ok bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	c, ok := a.schemaSets[dbID]
	if !ok || a.nowWallLocked() >= c.expiration {
		return 0, false
	}
	return c.set.gen, true
}
