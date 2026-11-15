package testcluster

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/kvclient"
	"github.com/sthorne/datax/pkg/kvpb"
	"github.com/sthorne/datax/pkg/server"
	"github.com/sthorne/datax/pkg/sql"
	"github.com/sthorne/datax/pkg/sql/catalog"
)

// listInByScan is ListIn as it was before issue #311: every descriptor
// in the cluster, filtered by database. The reference the new ListIn is
// held to.
func listInByScan(ctx context.Context, txn *kvclient.Txn, db *catalog.DatabaseDescriptor) ([]*catalog.TableDescriptor, error) {
	all, err := catalog.NewAccessor().List(ctx, txn)
	if err != nil {
		return nil, err
	}
	var out []*catalog.TableDescriptor
	for _, d := range all {
		if d.DatabaseID == db.ID || (d.DatabaseID == 0 && db.Name == catalog.DefaultDatabase) {
			out = append(out, d)
		}
	}
	return out, nil
}

// compareListIn checks, inside txn, that ListIn agrees with the scan for
// every database the catalog holds and for the default database as it
// was before it had a descriptor (ID 0).
func compareListIn(t *testing.T, ctx context.Context, txn *kvclient.Txn, when string) (listed int) {
	t.Helper()
	dbs, err := catalog.ListDatabases(ctx, txn)
	if err != nil {
		t.Fatal(err)
	}
	dbs = append(dbs,
		&catalog.DatabaseDescriptor{ID: 0, Name: catalog.DefaultDatabase},
		&catalog.DatabaseDescriptor{ID: 0, Name: "no_such_database"},
		&catalog.DatabaseDescriptor{ID: 1 << 40, Name: "never_created"})
	for _, db := range dbs {
		want, err := listInByScan(ctx, txn, db)
		if err != nil {
			t.Fatal(err)
		}
		got, err := catalog.NewAccessor().ListIn(ctx, txn, db)
		if err != nil {
			t.Fatal(err)
		}
		wj, _ := json.Marshal(want)
		gj, _ := json.Marshal(got)
		if len(want) != len(got) || (len(want) > 0 && !bytes.Equal(wj, gj)) {
			name := func(ds []*catalog.TableDescriptor) (out []string) {
				for _, d := range ds {
					out = append(out, fmt.Sprintf("%s(%d)", d.Name, d.ID))
				}
				return out
			}
			t.Fatalf("%s: ListIn(%s, id %d) = %v, the scan of every descriptor gives %v", when, db.Name, db.ID, name(got), name(want))
		}
		listed += len(got)
	}
	return listed
}

// TestListInMatchesAScanOfEveryDescriptor (issue #311): ListIn reads one
// database's names and the descriptors they point at. For every caller
// that must be the list it used to get by scanning every descriptor in
// the cluster and filtering: same tables, same order, in every database,
// across the catalog shapes that exist — tables that predate the v6
// catalog (database ID 0, named in the flat namespace), views, renamed
// and dropped tables, an index mid-backfill, name entries that point at
// nothing or at a descriptor that does not decode — committed, inside a
// transaction that has changed the catalog and not committed, and at a
// timestamp in the past.
func TestListInMatchesAScanOfEveryDescriptor(t *testing.T) {
	tc := Start(t, 3)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	db := tc.Nodes[0].DB()
	root := sql.NewSession(db, catalog.NewAccessor())
	waitForDatabases(t, ctx, root)

	for _, stmt := range []string{
		`CREATE DATABASE app`,
		`CREATE DATABASE empty_db`,
		`CREATE TABLE items (id INT8 PRIMARY KEY, v TEXT)`,
		`CREATE TABLE app.items (id INT8 PRIMARY KEY, v TEXT)`,
		`CREATE TABLE app.orders (id INT8 PRIMARY KEY, item INT8 REFERENCES app.items (id))`,
		`CREATE TABLE app.old_name (id INT8 PRIMARY KEY)`,
		`ALTER TABLE app.old_name RENAME TO new_name`,
		`CREATE TABLE app.gone (id INT8 PRIMARY KEY)`,
		`CREATE VIEW app.item_names AS SELECT v FROM app.items`,
		`CREATE TABLE doomed (id INT8 PRIMARY KEY)`,
	} {
		execSQL(t, ctx, root, stmt)
	}
	app, err := lookupDatabaseDesc(ctx, db, "app")
	if err != nil {
		t.Fatal(err)
	}

	// The shapes SQL no longer produces, written as the code of their day
	// wrote them.
	if err := db.RunTxn(ctx, "old-shapes", func(ctx context.Context, txn *kvclient.Txn) error {
		a := catalog.NewAccessor()
		// Tables created before the cluster finalized v6: database ID 0,
		// named in the flat namespace.
		for _, name := range []string{"pre_v6_a", "pre_v6_b"} {
			if err := a.Create(ctx, txn, &catalog.TableDescriptor{
				Name:       name,
				Columns:    []catalog.Column{{ID: 1, Name: "id"}},
				PrimaryKey: []catalog.ColumnID{1},
			}); err != nil {
				return err
			}
		}
		// An index still being backfilled.
		d, err := a.LookupFreshIn(ctx, txn, "app", "orders")
		if err != nil {
			return err
		}
		d = d.Clone()
		d.Indexes = append(d.Indexes, catalog.IndexDescriptor{ID: 9, Name: "by_item", ColumnIDs: []catalog.ColumnID{2}, State: catalog.IndexStateWriteOnly})
		if err := a.Update(ctx, txn, d); err != nil {
			return err
		}
		// Name entries that point at no descriptor, in both layouts, and
		// one whose descriptor does not decode.
		if err := txn.Put(ctx, keys.NamespaceKey("dangling_flat"), []byte("900001")); err != nil {
			return err
		}
		if err := txn.Put(ctx, keys.TableNamespaceKey(app.ID, "dangling"), []byte("900002")); err != nil {
			return err
		}
		if err := txn.Put(ctx, keys.TableNamespaceKey(app.ID, "corrupt"), []byte("900003")); err != nil {
			return err
		}
		if err := txn.Put(ctx, keys.TableDescKey(900003), []byte("{not json")); err != nil {
			return err
		}
		return txn.Put(ctx, keys.TableNamespaceKey(app.ID, "not_a_number"), []byte("x"))
	}); err != nil {
		t.Fatal(err)
	}
	beforeDrops := tc.Nodes[0].Clock().Now()
	execSQL(t, ctx, root, `DROP TABLE app.gone`)
	execSQL(t, ctx, root, `DROP TABLE doomed`)

	if err := db.RunTxn(ctx, "compare", func(ctx context.Context, txn *kvclient.Txn) error {
		if n := compareListIn(t, ctx, txn, "committed"); n == 0 {
			t.Fatal("the comparison listed no tables at all")
		}
		got, err := catalog.NewAccessor().ListIn(ctx, txn, app)
		if err != nil {
			return err
		}
		var names []string
		for _, d := range got {
			names = append(names, d.Name)
		}
		if fmt.Sprint(names) != "[items orders new_name item_names]" {
			t.Fatalf("ListIn(app) = %v", names)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	// A transaction sees its own uncommitted catalog changes the same way
	// through both: a table it created, one it dropped, one it renamed.
	txn := db.NewTxn("uncommitted")
	a := catalog.NewAccessor()
	if err := a.Create(ctx, txn, &catalog.TableDescriptor{
		Name: "uncommitted", DatabaseID: app.ID,
		Columns: []catalog.Column{{ID: 1, Name: "id"}}, PrimaryKey: []catalog.ColumnID{1},
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := a.DropIn(ctx, txn, "app", "new_name"); err != nil {
		t.Fatal(err)
	}
	d, err := a.LookupFreshIn(ctx, txn, catalog.DefaultDatabase, "pre_v6_a")
	if err != nil {
		t.Fatal(err)
	}
	if err := a.RenameTable(ctx, txn, d.Clone(), "pre_v6_renamed"); err != nil {
		t.Fatal(err)
	}
	compareListIn(t, ctx, txn, "inside the transaction that changed the catalog")
	if err := txn.Rollback(ctx); err != nil {
		t.Fatal(err)
	}

	// And a read in the past lists the tables of the past.
	hist := db.NewHistoricalTxn("historical", beforeDrops)
	compareListIn(t, ctx, hist, "before the drops")
	got, err := catalog.NewAccessor().ListIn(ctx, hist, app)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, d := range got {
		found = found || d.Name == "gone"
	}
	if !found {
		t.Fatal("ListIn at a timestamp before DROP TABLE app.gone does not list it")
	}
	_ = hist.Rollback(ctx)

	// The one difference, stated rather than discovered: a descriptor
	// that no name entry points at. The catalog never writes one — a
	// descriptor and its name are created and removed in one transaction
	// — so it takes a damaged catalog to hold it; the scan returned it
	// and ListIn, which goes by the names, does not.
	if err := db.Put(ctx, keys.TableDescKey(900004), mustJSON(t, &catalog.TableDescriptor{ID: 900004, Name: "orphan", DatabaseID: app.ID})); err != nil {
		t.Fatal(err)
	}
	if err := db.RunTxn(ctx, "orphan", func(ctx context.Context, txn *kvclient.Txn) error {
		byScan, err := listInByScan(ctx, txn, app)
		if err != nil {
			return err
		}
		byName, err := catalog.NewAccessor().ListIn(ctx, txn, app)
		if err != nil {
			return err
		}
		if len(byScan) != len(byName)+1 || byScan[len(byScan)-1].Name != "orphan" {
			t.Fatalf("with an orphan descriptor the scan lists %d tables and ListIn %d", len(byScan), len(byName))
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func mustJSON(t *testing.T, v any) []byte {
	t.Helper()
	raw, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func lookupDatabaseDesc(ctx context.Context, db *kvclient.DB, name string) (*catalog.DatabaseDescriptor, error) {
	var out *catalog.DatabaseDescriptor
	err := db.RunTxn(ctx, "test-database", func(ctx context.Context, txn *kvclient.Txn) error {
		d, err := catalog.LookupDatabase(ctx, txn, name)
		out = d
		return err
	})
	return out, err
}

// leasedAccessor is a gateway of its own on the given node: an accessor
// with leasing on, the cache bound and idle time given (zero keeps the
// default of either).
func leasedAccessor(t *testing.T, tc *TestCluster, node int, ttl time.Duration, maxEntries int, idleAfter time.Duration) *catalog.Accessor {
	t.Helper()
	n := tc.Nodes[node]
	cat := catalog.NewAccessor()
	cat.SetCacheLimits(maxEntries, idleAfter)
	if err := cat.StartLeasing(n.DB(), n.Clock(), n.Stopper(), ttl); err != nil {
		t.Fatal(err)
	}
	return cat
}

// resolve looks a table up through cat, as a statement does.
func resolve(t *testing.T, ctx context.Context, db *kvclient.DB, cat *catalog.Accessor, name string) *catalog.TableDescriptor {
	t.Helper()
	var desc *catalog.TableDescriptor
	if err := db.RunTxn(ctx, "test-resolve", func(ctx context.Context, txn *kvclient.Txn) error {
		d, err := cat.LookupIn(ctx, txn, catalog.DefaultDatabase, name)
		desc = d
		return err
	}); err != nil {
		t.Fatalf("resolving %s: %v", name, err)
	}
	return desc
}

// gatewayLease reads a gateway's lease record on a descriptor (ok false
// when there is none).
func gatewayLease(t *testing.T, ctx context.Context, db *kvclient.DB, cat *catalog.Accessor, descID uint64) (version uint64, expiration int64, ok bool) {
	t.Helper()
	raw, err := db.Get(ctx, keys.DescLeaseKey(descID, cat.TestingGatewayID()))
	if err != nil {
		t.Fatal(err)
	}
	if raw == nil {
		return 0, 0, false
	}
	var l struct {
		Version    uint64 `json:"version"`
		Expiration int64  `json:"expiration"`
	}
	if err := json.Unmarshal(raw, &l); err != nil {
		t.Fatal(err)
	}
	return l.Version, l.Expiration, true
}

func eventually(t *testing.T, within time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(within)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("never happened within %s: %s", within, what)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// TestDescriptorCacheIsBoundedAndForgetsIdleTables (issue #311): a
// gateway's descriptor cache holds no more than its bound — the tables
// past it are resolved from the catalog by each statement, correctly,
// without a lease — and an entry no statement uses for a few lease
// periods is dropped and its lease record deleted, so a gateway that
// touched many tables once ends up holding, and renewing, the leases of
// the ones it still uses and no others. A table used again after that is
// an ordinary first use: read fresh, leased again, and drained against
// by the next schema change.
func TestDescriptorCacheIsBoundedAndForgetsIdleTables(t *testing.T) {
	tc := Start(t, 3)
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Second)
	defer cancel()
	const (
		ttl    = time.Second // idle after three of these
		bound  = 8
		tables = 12
	)
	ddl := leasedSession(t, tc, 0, ttl)
	waitForDatabases(t, ctx, ddl)
	gw := tc.Nodes[1].DB()
	ids := map[string]uint64{}
	name := func(i int) string { return fmt.Sprintf("e%02d", i) }
	for i := 0; i < tables; i++ {
		execSQL(t, ctx, ddl, `CREATE TABLE `+name(i)+` (id INT8 PRIMARY KEY, v INT8)`)
		ids[name(i)] = lookupDescriptor(t, ctx, gw, name(i)).ID
	}

	// Every lease request this node routes, by anyone.
	var leaseRequests atomic.Int64
	leasePrefix := newScaleObserver().leasePrefix
	gw.TestingSetSendObserver(func(_ context.Context, ba *kvpb.BatchRequest, _ *kvpb.BatchResponse) {
		for _, ru := range ba.Requests {
			if bytes.HasPrefix(ru.GetInner().Header().Key, leasePrefix) {
				leaseRequests.Add(1)
			}
		}
	})
	defer gw.TestingSetSendObserver(nil)

	cat := leasedAccessor(t, tc, 1, ttl, bound, 0)
	held := func() (n int) {
		for _, id := range ids {
			if _, _, ok := gatewayLease(t, ctx, gw, cat, id); ok {
				n++
			}
		}
		return n
	}

	// Touch every table once. All resolve; only the bound are cached and
	// leased.
	for i := 0; i < tables; i++ {
		if d := resolve(t, ctx, gw, cat, name(i)); d.ID != ids[name(i)] {
			t.Fatalf("%s resolved to descriptor %d, want %d", name(i), d.ID, ids[name(i)])
		}
	}
	if n := cat.TestingCacheSize(); n != bound {
		t.Fatalf("after touching %d tables the cache holds %d, want the bound %d", tables, n, bound)
	}
	if n := held(); n != bound {
		t.Fatalf("after touching %d tables the gateway holds %d leases, want %d", tables, n, bound)
	}
	// A table past the bound still resolves, twice, with nothing cached.
	for i := 0; i < 2; i++ {
		if d := resolve(t, ctx, gw, cat, name(tables-1)); d.ID != ids[name(tables-1)] {
			t.Fatalf("an uncached table resolved to descriptor %d", d.ID)
		}
	}
	if n := cat.TestingCacheSize(); n != bound {
		t.Fatalf("resolving a table past the bound changed the cache to %d", n)
	}

	// Keep one table in use and leave the rest alone: they go, it stays.
	stop := make(chan struct{})
	var wg sync.WaitGroup
	var useErr atomic.Pointer[error]
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			case <-time.After(100 * time.Millisecond):
				if err := gw.RunTxn(ctx, "test-in-use", func(ctx context.Context, txn *kvclient.Txn) error {
					_, err := cat.LookupIn(ctx, txn, catalog.DefaultDatabase, name(0))
					return err
				}); err != nil {
					useErr.Store(&err)
					return
				}
			}
		}
	}()
	eventually(t, 30*time.Second, "the idle tables leaving the cache, and their leases the catalog", func() bool {
		return cat.TestingCacheSize() == 1 && held() == 1
	})
	if _, _, ok := gatewayLease(t, ctx, gw, cat, ids[name(0)]); !ok {
		t.Fatal("the table still in use lost its lease")
	}
	// With room again, a table that was past the bound is cached.
	resolve(t, ctx, gw, cat, name(tables-1))
	if n := cat.TestingCacheSize(); n != 2 {
		t.Fatalf("a table resolved into a cache with room: the cache holds %d, want 2", n)
	}
	close(stop)
	wg.Wait()
	if err := useErr.Load(); err != nil {
		t.Fatalf("resolving the table in use: %v", *err)
	}

	// Nobody uses anything: the cache empties, the lease records go, and
	// the gateway sends no lease traffic at all.
	eventually(t, 30*time.Second, "an idle gateway giving up every lease", func() bool {
		return cat.TestingCacheSize() == 0 && held() == 0
	})
	before := leaseRequests.Load()
	time.Sleep(4 * ttl) // a dozen renewal ticks
	if n := leaseRequests.Load() - before; n != 0 {
		t.Fatalf("an idle gateway with an empty cache sent %d lease requests in %s", n, 4*ttl)
	}

	// Use after eviction: the descriptor is read again, at its current
	// version, under a new lease — which the next schema change drains
	// against, so the gateway never plans against the old schema once
	// that statement has returned.
	execSQL(t, ctx, ddl, `ALTER TABLE `+name(3)+` ADD COLUMN added_while_evicted TEXT`)
	d := resolve(t, ctx, gw, cat, name(3))
	if _, ok := d.Col("added_while_evicted"); !ok {
		t.Fatalf("a table resolved after eviction is at version %d, without the column added meanwhile", d.Version)
	}
	if v, _, ok := gatewayLease(t, ctx, gw, cat, d.ID); !ok || v != d.Version {
		t.Fatalf("after re-acquiring, the lease record is (version %d, present %v), want version %d", v, ok, d.Version)
	}
	resolve(t, ctx, gw, cat, name(3)) // from the cache: handed out
	execSQL(t, ctx, ddl, `ALTER TABLE `+name(3)+` ADD COLUMN added_after TEXT`)
	if d := resolve(t, ctx, gw, cat, name(3)); func() bool { _, ok := d.Col("added_after"); return !ok }() {
		t.Fatalf("after the schema change returned the gateway still resolves version %d", d.Version)
	}
}

// TestSchemaChangeRacingAnEviction (issue #311): evicting an idle
// descriptor deletes its lease record so that a schema change need not
// wait for a gateway that is not using the table. Three ways that could
// go wrong, each held open with the eviction parked between taking the
// entry out of the cache and releasing the lease:
//
//   - The table is used again first. The lookup takes a new lease; the
//     parked release must not delete it, or the gateway would hold a
//     cached descriptor that no schema change waits for.
//   - The schema changes first. The record is still there and still
//     live, so the change waits it out (no longer than a lease) and the
//     gateway's next use reads the new version.
//   - Neither: once the release has run, a schema change does not wait
//     for the gateway at all.
func TestSchemaChangeRacingAnEviction(t *testing.T) {
	tc := Start(t, 3)
	ctx, cancel := context.WithTimeout(context.Background(), 170*time.Second)
	defer cancel()
	const (
		ttl  = 4 * time.Second
		idle = 5 * time.Second
	)
	ddl := leasedSession(t, tc, 0, ttl)
	waitForDatabases(t, ctx, ddl)
	gw := tc.Nodes[1].DB()
	for _, name := range []string{"used_again", "changed", "left_alone"} {
		execSQL(t, ctx, ddl, `CREATE TABLE `+name+` (id INT8 PRIMARY KEY, v INT8)`)
	}
	cat := leasedAccessor(t, tc, 1, ttl, 0, idle)

	// park arranges for the eviction of table to stop before its lease is
	// released, and returns the channels that report it parked and let
	// it go.
	var hook atomic.Pointer[func(uint64)]
	catalog.SetTestingBeforeLeaseRelease(func(a *catalog.Accessor, descID uint64) {
		if f := hook.Load(); f != nil && a == cat {
			(*f)(descID)
		}
	})
	defer catalog.SetTestingBeforeLeaseRelease(nil)
	// A parked goroutine is one of the node's workers: whatever the test
	// goes on to decide, it is let go before the cluster stops.
	gate := func() (wait <-chan struct{}, open func()) {
		ch := make(chan struct{})
		var once sync.Once
		open = func() { once.Do(func() { close(ch) }) }
		t.Cleanup(open)
		return ch, open
	}
	park := func(id uint64) (parked chan struct{}, release func()) {
		parked = make(chan struct{})
		held, release := gate()
		var once sync.Once
		f := func(descID uint64) {
			if descID != id {
				return
			}
			once.Do(func() {
				close(parked)
				<-held
			})
		}
		hook.Store(&f)
		return parked, release
	}
	// cacheAndIdle resolves the table twice (the second from the cache,
	// so a statement has been handed the entry) and leaves it alone.
	cacheAndIdle := func(name string) uint64 {
		resolve(t, ctx, gw, cat, name)
		return resolve(t, ctx, gw, cat, name).ID
	}
	waitFor := func(ch chan struct{}, what string) {
		t.Helper()
		select {
		case <-ch:
		case <-time.After(60 * time.Second):
			t.Fatalf("timed out waiting for %s", what)
		}
	}

	// 1. Used again while the release is parked.
	id := cacheAndIdle("used_again")
	parked, release := park(id)
	waitFor(parked, "the eviction of used_again")
	if n := cat.TestingCacheSize(); n != 0 {
		t.Fatalf("the evicted entry is still cached (%d entries)", n)
	}
	resolve(t, ctx, gw, cat, "used_again") // a first use again: new lease
	resolve(t, ctx, gw, cat, "used_again") // from the cache: handed out
	_, taken, ok := gatewayLease(t, ctx, gw, cat, id)
	if !ok {
		t.Fatal("the lookup after eviction left no lease record")
	}
	// The renewal loop is the goroutine parked in the release. Let go, it
	// finishes the pass it was in, which renews the entry just cached.
	// That renewal is stopped short of writing, so the record can be
	// read as the release left it: it must be the lease taken above.
	renewing := make(chan struct{})
	held, renew := gate()
	var once sync.Once
	catalog.SetTestingBeforeLeaseWrite(func(a *catalog.Accessor, _ string) {
		if a == cat {
			once.Do(func() {
				close(renewing)
				<-held
			})
		}
	})
	defer catalog.SetTestingBeforeLeaseWrite(nil)
	release()
	waitFor(renewing, "the renewal that follows the release")
	if _, exp, ok := gatewayLease(t, ctx, gw, cat, id); !ok || exp != taken {
		t.Fatalf("releasing the evicted entry's lease touched the lease taken since: record present %v, expiration %d, want %d", ok, exp, taken)
	}
	// From here the gateway renews nothing more, so that what the schema
	// change below waits for is a lease it cannot renew.
	cat.TestingPauseRenewal(true)
	renew()
	time.Sleep(500 * time.Millisecond) // the one renewal in flight lands
	if _, taken, ok = gatewayLease(t, ctx, gw, cat, id); !ok {
		t.Fatal("the gateway's lease record vanished")
	}
	// The gateway has a live cached entry it handed to a statement and
	// cannot renew: the schema change must wait that lease out. Had the
	// record been deleted it would return at once.
	wait := time.Until(time.Unix(0, taken))
	start := time.Now()
	execSQL(t, ctx, ddl, `ALTER TABLE used_again ADD COLUMN c1 TEXT`)
	if elapsed := time.Since(start); elapsed < wait-500*time.Millisecond {
		t.Fatalf("the schema change returned after %s with the gateway's lease good for another %s: it did not wait for it", elapsed, wait)
	}
	cat.TestingPauseRenewal(false)
	if d := resolve(t, ctx, gw, cat, "used_again"); func() bool { _, ok := d.Col("c1"); return !ok }() {
		t.Fatalf("after the schema change returned the gateway resolves version %d, without the new column", d.Version)
	}

	// 2. The schema changes while the release is parked.
	id = cacheAndIdle("changed")
	parked, release = park(id)
	waitFor(parked, "the eviction of changed")
	start = time.Now()
	execSQL(t, ctx, ddl, `ALTER TABLE changed ADD COLUMN c2 TEXT`)
	if elapsed := time.Since(start); elapsed >= 2*ttl {
		t.Fatalf("the schema change took %s: it ran into the drain timeout instead of waiting out one lease", elapsed)
	}
	if d := resolve(t, ctx, gw, cat, "changed"); func() bool { _, ok := d.Col("c2"); return !ok }() {
		t.Fatalf("a table evicted across a schema change resolves at version %d, without the new column", d.Version)
	}
	_, taken, _ = gatewayLease(t, ctx, gw, cat, id)
	release()
	time.Sleep(500 * time.Millisecond)
	if _, exp, ok := gatewayLease(t, ctx, gw, cat, id); !ok || exp < taken {
		t.Fatalf("the parked release deleted the lease taken after the schema change (record present %v)", ok)
	}

	// 3. Evicted and released: nothing of this gateway's is left for a
	// schema change to wait for.
	id = cacheAndIdle("left_alone")
	hook.Store(nil)
	eventually(t, 60*time.Second, "left_alone leaving the cache and its lease the catalog", func() bool {
		if _, _, ok := gatewayLease(t, ctx, gw, cat, id); ok {
			return false
		}
		// "changed" was resolved again in step 2 and idles out with it.
		return cat.TestingCacheSize() == 0
	})
	start = time.Now()
	execSQL(t, ctx, ddl, `ALTER TABLE left_alone ADD COLUMN c3 TEXT`)
	if elapsed := time.Since(start); elapsed >= ttl {
		t.Fatalf("a schema change on a table the gateway had given up took %s, a whole lease", elapsed)
	}
	if d := resolve(t, ctx, gw, cat, "left_alone"); func() bool { _, ok := d.Col("c3"); return !ok }() {
		t.Fatalf("left_alone resolves at version %d, without the new column", d.Version)
	}
}

// TestSchemaDocumentIsBuiltOnDemand (issue #311): the console's schema
// document is built when somebody asks for it and not otherwise. With
// the recorder, the replication loop and the health checks all running,
// a cluster nobody is browsing builds none; a burst of requests to one
// node is one build, on that node only; polling inside the cache window
// builds nothing more; and a table created while the page is open shows
// within the window, which is what keeps the page live. The range labels
// of /api/cluster, which used to come from the document, appear without
// it.
func TestSchemaDocumentIsBuiltOnDemand(t *testing.T) {
	listeners := make([]net.Listener, 3)
	for i := range listeners {
		lis, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		listeners[i] = lis
	}
	li := 0
	tc := StartWithOptions(t, 3, func(c *server.Config) {
		c.HTTPListener = listeners[li]
		c.MetricsRecordInterval = 2 * time.Second
		li++
	})
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	root := sql.NewSession(tc.Nodes[0].DB(), catalog.NewAccessor())
	waitForDatabases(t, ctx, root)
	execSQL(t, ctx, root, `CREATE TABLE browsed (id INT8 PRIMARY KEY, v TEXT)`)
	// A table carved into ranges of its own, for the range labels below.
	execSQL(t, ctx, root, `CREATE TABLE readings (series TEXT, at TIMESTAMPTZ, v FLOAT8, PRIMARY KEY (series, at)) WITH (timeseries = true, shards = 2)`)

	builds := func() (out [3]int64) {
		for i, n := range tc.Nodes {
			out[i] = n.TestingSchemaBuilds()
		}
		return out
	}
	// Longer than the cache window, several recorder and replication
	// ticks: everything that used to ask for the document has run.
	time.Sleep(8 * time.Second)
	if b := builds(); b != [3]int64{} {
		t.Fatalf("with nobody browsing, the nodes built the schema document %v times", b)
	}

	tables := func(node int) map[string]bool {
		code, _, body := httpGet(t, "http://"+tc.Nodes[node].HTTPAddr()+"/api/schema")
		if code != 200 {
			t.Fatalf("/api/schema: %d %s", code, body)
		}
		var doc server.SchemaStatus
		if err := json.Unmarshal([]byte(body), &doc); err != nil {
			t.Fatal(err)
		}
		if doc.Error != "" {
			t.Fatalf("/api/schema: %s", doc.Error)
		}
		out := map[string]bool{}
		for _, tb := range doc.Tables {
			out[tb.Name] = true
		}
		return out
	}
	// Eight requests at once are one build.
	var wg sync.WaitGroup
	var missing atomic.Int64
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if !tables(1)["browsed"] {
				missing.Add(1)
			}
		}()
	}
	wg.Wait()
	if missing.Load() != 0 {
		t.Fatalf("%d of 8 concurrent requests got a document without the table", missing.Load())
	}
	if b := builds(); b != [3]int64{0, 1, 0} {
		t.Fatalf("eight concurrent requests to n2 built the document %v times across the nodes, want once on n2", b)
	}
	// Polling inside the window is served from the cache.
	for i := 0; i < 4; i++ {
		tables(1)
		time.Sleep(300 * time.Millisecond)
	}
	if b := builds(); b != [3]int64{0, 1, 0} {
		t.Fatalf("polling inside the cache window rebuilt the document: %v", b)
	}

	// The page stays live: a new table shows within the cache window (and
	// a slow machine's slack), on a console that polls every second.
	execSQL(t, ctx, root, `CREATE TABLE created_while_open (id INT8 PRIMARY KEY)`)
	created := time.Now()
	eventually(t, 15*time.Second, "the new table appearing in /api/schema", func() bool {
		if tables(1)["created_while_open"] {
			return true
		}
		time.Sleep(time.Second)
		return false
	})
	t.Logf("a table created while the page was open showed after %s", time.Since(created).Round(100*time.Millisecond))
	if b := builds(); b[0] != 0 || b[2] != 0 {
		t.Fatalf("browsing n2 made the other nodes build the document: %v", b)
	}

	// Range labels without the document: n3 has built none, and names
	// the table on its ranges all the same.
	eventually(t, 30*time.Second, "n3 labelling a range with its table", func() bool {
		_, _, body := httpGet(t, "http://"+tc.Nodes[2].HTTPAddr()+"/api/cluster")
		var doc server.ClusterStatus
		if err := json.Unmarshal([]byte(body), &doc); err != nil {
			t.Fatal(err)
		}
		for _, r := range doc.Ranges {
			if r.Table == "readings" {
				return true
			}
		}
		time.Sleep(500 * time.Millisecond)
		return false
	})
	if b := builds(); b[2] != 0 {
		t.Fatalf("labelling ranges on n3 built the schema document %d times", b[2])
	}
}
