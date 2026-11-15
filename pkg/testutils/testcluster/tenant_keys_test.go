package testcluster

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/sthorne/datax/pkg/cluster"
	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/keystore"
	"github.com/sthorne/datax/pkg/server"
	"github.com/sthorne/datax/pkg/sql/catalog"
	"github.com/sthorne/datax/pkg/sql/rowseal"
	"github.com/sthorne/datax/pkg/sql/types"
)

func stringDatum(s string) types.Datum { return types.NewString(s) }

// tableStats reads a table's statistics.
func tableStats(t *testing.T, ctx context.Context, c *ckCluster, id uint64) *catalog.TableStatistics {
	t.Helper()
	raw, err := c.tc.Nodes[0].DB().Get(ctx, keys.TableStatsKey(id))
	if err != nil || raw == nil {
		t.Fatalf("no statistics: %v", err)
	}
	var st catalog.TableStatistics
	if err := json.Unmarshal(raw, &st); err != nil {
		t.Fatal(err)
	}
	return &st
}

// startTenantCluster is startSealedCluster with the tenant key janitor's
// grace off, so that a dropped table's tenant keys go at once.
func startTenantCluster(t *testing.T, ctx context.Context) *ckCluster {
	c := startCKCluster(t, ctx, func(i int, cfg *server.Config) {
		cfg.KeyCacheLease = sealedLease
		cfg.TenantKeyGrace = -1
	})
	c.waitReported(3)
	return c
}

// sealedKeyIDs maps each primary row of a per-tenant table to the key id
// its sealed value names, by the row's tenant (the key bytes of the
// leading column), checking every value is sealed and none holds any of
// plain.
func sealedKeyIDs(t *testing.T, ctx context.Context, c *ckCluster, d *catalog.TableDescriptor, plain ...string) map[string][]int64 {
	t.Helper()
	primary, other := tableKVs(t, ctx, c, d.ID)
	out := map[string][]int64{}
	for _, kv := range primary {
		v := kv[1]
		if len(v) < 9 || v[0] != rowseal.Marker {
			t.Fatalf("a primary value is not sealed: % x", v)
		}
		tenant, err := rowseal.TenantOf(d, kv[0])
		if err != nil {
			t.Fatal(err)
		}
		out[string(tenant)] = append(out[string(tenant)], int64(binary.BigEndian.Uint64(v[1:9])))
	}
	for _, kv := range append(primary, other...) {
		for _, p := range plain {
			if bytes.Contains(kv[0], []byte(p)) || bytes.Contains(kv[1], []byte(p)) {
				t.Fatalf("plaintext %q of a sealed column in KV: %q → %q", p, kv[0], kv[1])
			}
		}
	}
	return out
}

// tenantKeyOf is the key id of a tenant's rows (all of them one key).
func tenantKeyOf(t *testing.T, ids map[string][]int64, d *catalog.TableDescriptor, tenant string) int64 {
	t.Helper()
	enc, err := rowseal.TenantValue(d, stringDatum(tenant))
	if err != nil {
		t.Fatal(err)
	}
	got := ids[string(enc)]
	if len(got) == 0 {
		t.Fatalf("tenant %s has no rows", tenant)
	}
	for _, id := range got {
		if id != got[0] {
			t.Fatalf("tenant %s's rows are under keys %v", tenant, got)
		}
	}
	return got[0]
}

// liveTenantKeys lists a seal id's live tenant keys.
func liveTenantKeys(t *testing.T, ctx context.Context, ks *keystore.Store, seal uint64) []keystore.Key {
	t.Helper()
	p := rowseal.TenantNamePrefix(seal)
	var out []keystore.Key
	for from := p; ; {
		page, err := ks.KeysByName(ctx, from, keystore.PrefixEnd(p), 1000)
		if err != nil {
			t.Fatal(err)
		}
		if len(page) == 0 {
			return out
		}
		for _, k := range page {
			if k.State != keystore.StateDestroyed {
				out = append(out, k)
			}
		}
		from = page[len(page)-1].Name + "\x00"
	}
}

// waitTenantKeysGone waits for the janitor to destroy a seal id's
// tenant keys.
func waitTenantKeysGone(t *testing.T, ctx context.Context, ks *keystore.Store, seal uint64) {
	t.Helper()
	deadline := time.Now().Add(60 * time.Second)
	for {
		left := liveTenantKeys(t, ctx, ks, seal)
		if len(left) == 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("%d tenant keys of seal %d still live after the drop", len(left), seal)
		}
		time.Sleep(200 * time.Millisecond)
	}
}

func tenantUUID(i int) string { return fmt.Sprintf("00000000-0000-4000-8000-%012d", i) }

// TestSealedPerTenant (issue #310, step 4): a table sealed per tenant
// end to end on a secure 3-node cluster — refused before the cluster key
// and in the shapes it cannot take; CRUD; each tenant's values under a
// key of its own, named in each value; concurrent first writes of one
// tenant from three nodes converging on one key; 2,000 tenants made
// concurrently without a conflict; the application guide's erasure
// procedure, after which a row left behind fails on every node and a new
// write gets a fresh key; datax_destroy_tenant_key's refusals and
// idempotence; statistics; DROP TABLE destroying every tenant key.
func TestSealedPerTenant(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 420*time.Second)
	defer cancel()
	c := startTenantCluster(t, ctx)
	var notices noticeLog
	conn := sealedConn(t, ctx, c, 0, &notices)
	conns := []*pgx.Conn{conn, sealedConn(t, ctx, c, 1, nil), sealedConn(t, ctx, c, 2, nil)}

	const ddl = `CREATE TABLE items (
		tenant_id UUID,
		id INT8,
		title TEXT,
		details JSONB,
		tag_hmac BYTES,
		PRIMARY KEY (tenant_id, id)
	) WITH (encryption_key_by = 'tenant_id')`
	wantSQLErr(t, ctx, conn, ddl, "0A000", "cluster key")
	if errText, _ := c.op(0, "init"); errText != "" {
		t.Fatalf("init: %s", errText)
	}
	ks := c.keyStore(0)

	var d *catalog.TableDescriptor
	t.Run("create", func(t *testing.T) {
		for _, bad := range []struct{ q, code, want string }{
			{`CREATE TABLE b1 (t UUID, id INT8, PRIMARY KEY (id, t)) WITH (encryption_key_by = 't')`, "42P17", "leading primary-key column"},
			{`CREATE TABLE b2 (t UUID, id INT8 PRIMARY KEY) WITH (encryption_key_by = 't')`, "42P17", "leading primary-key column"},
			{`CREATE TABLE b3 (t UUID, id INT8, PRIMARY KEY (t, id)) WITH (encryption_key_by = 'nope')`, "42703", "does not exist"},
			{`CREATE TABLE b4 (t FLOAT8, id INT8, PRIMARY KEY (t, id)) WITH (encryption_key_by = 't')`, "0A000", "integer, text, bytea or uuid"},
			{`CREATE TABLE b5 (t UUID, id INT8, PRIMARY KEY (t, id)) WITH (encryption_key_by = 't', encryption_key = 'auto')`, "22023", "not both"},
			{`CREATE TABLE b6 (t INT8, at TIMESTAMPTZ, v FLOAT8, PRIMARY KEY (t, at)) WITH (timeseries = true, encryption_key_by = 't')`, "0A000", "time-series"},
		} {
			wantSQLErr(t, ctx, conn, bad.q, bad.code, bad.want)
		}
		notices.take()
		sealExec(t, ctx, conn, ddl)
		got := notices.take()
		if len(got) != 1 || !strings.Contains(got[0], `table "items" is sealed per tenant`) || !strings.Contains(got[0], "tenant_id (primary key)") ||
			strings.Contains(got[0], "title") || strings.Contains(got[0], "details") {
			t.Fatalf("CREATE TABLE notices: %q", got)
		}
		sealExec(t, ctx, conn, `CREATE INDEX items_tag ON items (tenant_id, tag_hmac)`)
		if got := notices.take(); len(got) != 1 || !strings.Contains(got[0], "tag_hmac") {
			t.Fatalf("CREATE INDEX notices: %q", got)
		}
		d = tableDesc(t, ctx, c, "items")
		if !d.SealedPerTenant() || d.EncryptionKey.SealID != d.ID {
			t.Fatalf("descriptor: %+v", d.EncryptionKey)
		}
		if k, err := ks.GetByID(ctx, d.EncryptionKey.KeyID); err != nil || k.Kind != keystore.KindTable {
			t.Fatalf("the naming key: %+v %v", k, err)
		}
		if n := len(liveTenantKeys(t, ctx, ks, d.ID)); n != 0 {
			t.Fatalf("%d tenant keys before any write", n)
		}
		sealExec(t, ctx, conn, `CREATE TABLE plain_items (tenant_id UUID, id INT8, title TEXT, PRIMARY KEY (tenant_id, id))`)
		wantSQLErr(t, ctx, conn, `SELECT datax_destroy_tenant_key('plain_items', '`+tenantUUID(1)+`')`, "42809", "not sealed per tenant")
	})

	ta, tb := tenantUUID(1), tenantUUID(2)
	t.Run("crud", func(t *testing.T) {
		for i := 1; i <= 4; i++ {
			for _, tn := range []string{ta, tb} {
				sealExec(t, ctx, conn, fmt.Sprintf(`INSERT INTO items VALUES ('%s', %d, 'SEKRIT-%s-%d', '{"n": %d}', '\x%02d')`, tn, i, tn[len(tn)-1:], i, i, i))
			}
		}
		sealExec(t, ctx, conn, fmt.Sprintf(`UPDATE items SET title = 'SEKRIT-1-2b' WHERE tenant_id = '%s' AND id = 2`, ta))
		sealExec(t, ctx, conn, fmt.Sprintf(`DELETE FROM items WHERE tenant_id = '%s' AND id = 4`, tb))
		sealExec(t, ctx, conn, fmt.Sprintf(`INSERT INTO items (tenant_id, id, title) VALUES ('%s', 3, 'x') ON CONFLICT (tenant_id, id) DO UPDATE SET title = items.title || '+'`, ta))
		sealExec(t, ctx, conn, fmt.Sprintf(`UPSERT INTO items (tenant_id, id, title) VALUES ('%s', 5, 'SEKRIT-2-5')`, tb))
		if _, err := conn.CopyFrom(ctx, pgx.Identifier{"items"}, []string{"tenant_id", "id", "title"},
			pgx.CopyFromRows([][]any{{ta, int64(6), "SEKRIT-1-6"}, {tb, int64(6), "SEKRIT-2-6"}})); err != nil {
			t.Fatalf("COPY: %v", err)
		}
		wantA := []string{"1|SEKRIT-1-1", "2|SEKRIT-1-2b", "3|SEKRIT-1-3+", "4|SEKRIT-1-4", "6|SEKRIT-1-6"}
		wantB := []string{"1|SEKRIT-2-1", "2|SEKRIT-2-2", "3|SEKRIT-2-3", "5|SEKRIT-2-5", "6|SEKRIT-2-6"}
		for i, cn := range conns {
			sameStrings(t, fmt.Sprintf("tenant a through n%d", i+1), queryStrings(t, ctx, cn, fmt.Sprintf(`SELECT id, title FROM items WHERE tenant_id = '%s' ORDER BY id`, ta)), wantA...)
			sameStrings(t, fmt.Sprintf("tenant b through n%d", i+1), queryStrings(t, ctx, cn, fmt.Sprintf(`SELECT id, title FROM items WHERE tenant_id = '%s' ORDER BY id`, tb)), wantB...)
		}
		sameStrings(t, "across tenants, filtered on a sealed column", queryStrings(t, ctx, conn, `SELECT count(*) FROM items WHERE title LIKE 'SEKRIT-%-6'`), "2")
		sameStrings(t, "by the index", queryStrings(t, ctx, conn, fmt.Sprintf(`SELECT title FROM items WHERE tenant_id = '%s' AND tag_hmac = '\x01'`, tb)), "SEKRIT-2-1")
	})

	t.Run("a key per tenant, named in each value", func(t *testing.T) {
		ids := sealedKeyIDs(t, ctx, c, d, "SEKRIT")
		ka, kb := tenantKeyOf(t, ids, d, ta), tenantKeyOf(t, ids, d, tb)
		if ka == kb || ka == d.EncryptionKey.KeyID {
			t.Fatalf("tenant keys %d and %d, naming key %d", ka, kb, d.EncryptionKey.KeyID)
		}
		live := liveTenantKeys(t, ctx, ks, d.ID)
		if len(live) != 2 {
			t.Fatalf("%d live tenant keys for two tenants", len(live))
		}
		for _, k := range live {
			if k.Kind != keystore.KindTenant || (k.ID != ka && k.ID != kb) || strings.Contains(k.Name, "0000") {
				t.Fatalf("tenant key %+v", k)
			}
		}
		c.checkConsistent("after per-tenant writes")
	})

	t.Run("concurrent first writes of one tenant converge", func(t *testing.T) {
		tc := tenantUUID(3)
		var wg sync.WaitGroup
		errs := make(chan error, 30)
		for n := 0; n < 3; n++ {
			for j := 0; j < 5; j++ {
				wg.Add(1)
				go func(n, j int) {
					defer wg.Done()
					cn := sealedConn(t, ctx, c, n, nil)
					if _, err := cn.Exec(ctx, fmt.Sprintf(`INSERT INTO items (tenant_id, id, title) VALUES ('%s', %d, 'SEKRIT-c')`, tc, 100+n*10+j)); err != nil {
						errs <- err
					}
				}(n, j)
			}
		}
		wg.Wait()
		close(errs)
		for err := range errs {
			t.Fatalf("a concurrent first write: %v", err)
		}
		ids := sealedKeyIDs(t, ctx, c, d)
		kc := tenantKeyOf(t, ids, d, tc)
		if n := len(liveTenantKeys(t, ctx, ks, d.ID)); n != 3 {
			t.Fatalf("%d live tenant keys for three tenants (tenant c's: %d)", n, kc)
		}
		sameStrings(t, "tenant c", queryStrings(t, ctx, conns[2], fmt.Sprintf(`SELECT count(*) FROM items WHERE tenant_id = '%s'`, tc)), "15")
	})

	t.Run("many tenants", func(t *testing.T) {
		const tenants, workers = 2000, 24
		var next, conflicts int64
		var wg sync.WaitGroup
		errs := make(chan error, workers)
		start := time.Now()
		for w := 0; w < workers; w++ {
			wg.Add(1)
			go func(w int) {
				defer wg.Done()
				cn := sealedConn(t, ctx, c, w%3, nil)
				for {
					i := atomic.AddInt64(&next, 1)
					if i > tenants {
						return
					}
					if _, err := cn.Exec(ctx, fmt.Sprintf(`INSERT INTO items (tenant_id, id, title) VALUES ('%s', 1, 'SEKRIT-many')`, tenantUUID(int(1000+i)))); err != nil {
						if sqlState(err) == "40001" {
							atomic.AddInt64(&conflicts, 1)
						}
						errs <- err
						return
					}
				}
			}(w)
		}
		wg.Wait()
		close(errs)
		for err := range errs {
			t.Fatalf("a tenant's first write (%d conflicts): %v", conflicts, err)
		}
		t.Logf("%d tenants' first writes from %d workers in %s", tenants, workers, time.Since(start).Round(time.Millisecond))
		if n := len(liveTenantKeys(t, ctx, ks, d.ID)); n != tenants+3 {
			t.Fatalf("%d live tenant keys, want %d", n, tenants+3)
		}
		sameStrings(t, "every tenant's row", queryStrings(t, ctx, conns[1], `SELECT count(*) FROM items WHERE title = 'SEKRIT-many'`), fmt.Sprint(tenants))
	})

	t.Run("statistics", func(t *testing.T) {
		sealExec(t, ctx, conn, `ANALYZE items`)
		st := tableStats(t, ctx, c, d.ID)
		got := map[string]bool{}
		for _, cs := range st.Columns {
			got[cs.Name] = true
		}
		if !got["tenant_id"] || !got["id"] || !got["tag_hmac"] || got["title"] || got["details"] {
			t.Fatalf("statistics columns %v", got)
		}
	})

	t.Run("erasure", func(t *testing.T) {
		ids := sealedKeyIDs(t, ctx, c, d)
		ka := tenantKeyOf(t, ids, d, ta)
		kb := tenantKeyOf(t, ids, d, tb)
		// The application guide's procedure: delete, purge, destroy.
		sealExec(t, ctx, conn, fmt.Sprintf(`DELETE FROM items WHERE tenant_id = '%s'`, ta))
		sealExec(t, ctx, conn, `ALTER TABLE items PURGE`)
		start := time.Now()
		var got int64
		if err := conn.QueryRow(ctx, `SELECT datax_destroy_tenant_key('items', $1)`, ta).Scan(&got); err != nil {
			t.Fatalf("destroy: %v", err)
		}
		if got != ka {
			t.Fatalf("destroyed key %d, tenant a's is %d", got, ka)
		}
		if took := time.Since(start); took < sealedLease {
			t.Fatalf("destroy returned after %s, before the key lease (%s)", took, sealedLease)
		}
		if k, err := ks.GetByID(ctx, ka); err != nil || k.State != keystore.StateDestroyed || !strings.HasPrefix(k.Name, keystore.DestroyedPrefix) {
			t.Fatalf("tenant a's key after destroy: %+v %v", k, err)
		}
		// Tenant b is untouched; no plaintext anywhere in the table.
		for i, cn := range conns {
			sameStrings(t, fmt.Sprintf("tenant b through n%d", i+1), queryStrings(t, ctx, cn, fmt.Sprintf(`SELECT count(*) FROM items WHERE tenant_id = '%s' AND title LIKE 'SEKRIT-2-%%'`, tb)), "5")
		}
		after := sealedKeyIDs(t, ctx, c, d, "SEKRIT")
		encA, _ := rowseal.TenantValue(d, stringDatum(ta))
		if len(after[string(encA)]) != 0 {
			t.Fatal("tenant a has rows after the erasure")
		}
		if tenantKeyOf(t, after, d, tb) != kb {
			t.Fatal("tenant b's key changed")
		}
		// Idempotent: again, the same key; a tenant that never wrote: NULL.
		if err := conn.QueryRow(ctx, `SELECT datax_destroy_tenant_key('items', $1)`, ta).Scan(&got); err != nil || got != ka {
			t.Fatalf("destroy again: %d %v", got, err)
		}
		var none *int64
		if err := conn.QueryRow(ctx, `SELECT datax_destroy_tenant_key('items', $1)`, tenantUUID(999999)).Scan(&none); err != nil || none != nil {
			t.Fatalf("destroy of a tenant with no key: %v %v", none, err)
		}
		// A new write of the erased tenant gets a fresh key.
		sealExec(t, ctx, conn, fmt.Sprintf(`INSERT INTO items (tenant_id, id, title) VALUES ('%s', 1, 'SEKRIT-1-new')`, ta))
		ka2 := tenantKeyOf(t, sealedKeyIDs(t, ctx, c, d), d, ta)
		if ka2 == ka {
			t.Fatal("the erased tenant's new row is under the destroyed key")
		}
		for i, cn := range conns {
			sameStrings(t, fmt.Sprintf("the erased tenant's new row through n%d", i+1), queryStrings(t, ctx, cn, fmt.Sprintf(`SELECT title FROM items WHERE tenant_id = '%s'`, ta)), "SEKRIT-1-new")
		}
	})

	t.Run("a row left behind", func(t *testing.T) {
		td := tenantUUID(4)
		sealExec(t, ctx, conn, fmt.Sprintf(`INSERT INTO items (tenant_id, id, title) VALUES ('%s', 1, 'SEKRIT-d1'), ('%s', 2, 'SEKRIT-d2')`, td, td))
		// Every gateway has the key cached.
		for _, cn := range conns {
			sameStrings(t, "before", queryStrings(t, ctx, cn, fmt.Sprintf(`SELECT count(*) FROM items WHERE tenant_id = '%s' AND title IS NOT NULL`, td)), "2")
		}
		kd := tenantKeyOf(t, sealedKeyIDs(t, ctx, c, d), d, td)
		// Destroyed from n2, without deleting the rows first.
		if _, err := conns[1].Exec(ctx, `SELECT datax_destroy_tenant_key('items', $1)`, td); err != nil {
			t.Fatal(err)
		}
		// Once it has returned, no gateway reads the rows.
		for i, cn := range conns {
			wantSQLErr(t, ctx, cn, fmt.Sprintf(`SELECT title FROM items WHERE tenant_id = '%s'`, td), "XXK01", `table "items"`, "destroyed", fmt.Sprint(kd))
			if i == 0 {
				wantSQLErr(t, ctx, cn, `SELECT count(*) FROM items WHERE title LIKE 'x%'`, "XXK01")
			}
		}
		// The tenant's next write makes a fresh key; its old rows stay
		// unreadable.
		sealExec(t, ctx, conns[2], fmt.Sprintf(`INSERT INTO items (tenant_id, id, title) VALUES ('%s', 3, 'SEKRIT-d3')`, td))
		sameStrings(t, "the new row", queryStrings(t, ctx, conn, fmt.Sprintf(`SELECT title FROM items WHERE tenant_id = '%s' AND id = 3`, td)), "SEKRIT-d3")
		wantSQLErr(t, ctx, conn, fmt.Sprintf(`SELECT title FROM items WHERE tenant_id = '%s' AND id = 1`, td), "XXK01")
		// The rows left behind cannot be deleted one by one either (their
		// index entries are found from their values); TRUNCATE removes
		// them, as a table restore would.
		wantSQLErr(t, ctx, conn, fmt.Sprintf(`DELETE FROM items WHERE tenant_id = '%s' AND id <= 2`, td), "XXK01")
	})

	t.Run("refusals", func(t *testing.T) {
		sealExec(t, ctx, conn, `CREATE USER tenant_app WITH PASSWORD 'pw-app'`)
		sealExec(t, ctx, conn, `GRANT SELECT, INSERT, DELETE ON items TO tenant_app`)
		app := ksConn(t, ctx, c, 1, "tenant_app", "pw-app")
		_, err := app.Exec(ctx, `SELECT datax_destroy_tenant_key('items', $1)`, tb)
		wantState(t, "destroy by a non-owner", err, "42501", "owner")
		tx, err := conn.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		_, err = tx.Exec(ctx, fmt.Sprintf(`SELECT datax_destroy_tenant_key('items', '%s')`, tb))
		wantState(t, "destroy in a transaction block", err, "25001", "transaction block")
		_ = tx.Rollback(ctx)
		wantSQLErr(t, ctx, conn, `SELECT datax_destroy_tenant_key('items', 'not-a-uuid')`, "22P02")
		wantSQLErr(t, ctx, conn, `SELECT datax_destroy_tenant_key('items', tenant_id) FROM items`, "0A000", "constants or parameters")
		sameStrings(t, "tenant b still there", queryStrings(t, ctx, conn, fmt.Sprintf(`SELECT count(*) FROM items WHERE tenant_id = '%s'`, tb)), "5")
	})

	t.Run("drop destroys every tenant key", func(t *testing.T) {
		sealExec(t, ctx, conn, `CREATE TABLE gone (t INT8, id INT8, body TEXT, PRIMARY KEY (t, id)) WITH (encryption_key_by = 't')`)
		for i := 0; i < 50; i++ {
			sealExec(t, ctx, conn, fmt.Sprintf(`INSERT INTO gone VALUES (%d, 1, 'SEKRIT-gone')`, i))
		}
		gd := tableDesc(t, ctx, c, "gone")
		if n := len(liveTenantKeys(t, ctx, ks, gd.ID)); n != 50 {
			t.Fatalf("%d tenant keys for 50 tenants", n)
		}
		sealExec(t, ctx, conn, `DROP TABLE gone`)
		if k, err := ks.GetByID(ctx, gd.EncryptionKey.KeyID); err != nil || k.State != keystore.StateDestroyed {
			t.Fatalf("the naming key after the drop: %+v %v", k, err)
		}
		waitTenantKeysGone(t, ctx, ks, gd.ID)
		// Another table's tenant keys are untouched.
		if n := len(liveTenantKeys(t, ctx, ks, d.ID)); n < 2000 {
			t.Fatalf("items has %d live tenant keys after another table's drop", n)
		}
		if n, err := c.tc.Nodes[2].SweepOrphanTenantKeys(ctx); err != nil || n != 0 {
			t.Fatalf("a second sweep: %d %v", n, err)
		}
	})
}

// TestSealedPerTenantRestore (issue #310, step 4): a table restore of a
// per-tenant table beside the live one shares its keys; after the table
// is dropped and its keys destroyed, a table restore brings back every
// tenant key the backup holds (more than one batch of them) and the
// naming key; a tenant whose key was destroyed before the backup stays
// unreadable, and writes again under a fresh key.
func TestSealedPerTenantRestore(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 420*time.Second)
	defer cancel()
	c := startTenantCluster(t, ctx)
	if errText, _ := c.op(0, "init"); errText != "" {
		t.Fatalf("init: %s", errText)
	}
	conn := sealedConn(t, ctx, c, 0, nil)
	ks := c.keyStore(0)
	sealExec(t, ctx, conn, `CREATE TABLE items (t INT8, id INT8, body TEXT, PRIMARY KEY (t, id)) WITH (encryption_key_by = 't')`)
	const tenants = 600 // more than one batch of tenantReinstateBatch
	for i := 0; i < tenants; i += 100 {
		var vals []string
		for j := i; j < i+100; j++ {
			vals = append(vals, fmt.Sprintf("(%d, 1, 'SEKRIT-%d')", j, j))
		}
		sealExec(t, ctx, conn, `INSERT INTO items VALUES `+strings.Join(vals, ", "))
	}
	// Tenant 0 is erased before the backup, leaving a row behind.
	sealExec(t, ctx, conn, `SELECT datax_destroy_tenant_key('items', 0)`)
	d := tableDesc(t, ctx, c, "items")
	if n := len(liveTenantKeys(t, ctx, ks, d.ID)); n != tenants-1 {
		t.Fatalf("%d live tenant keys", n)
	}
	dir := filepath.Join(t.TempDir(), "bk")
	if _, err := c.tc.Nodes[0].RunBackupWith(ctx, server.BackupOptions{Dest: dir, AllowPlaintext: true}); err != nil {
		t.Fatalf("backup: %v", err)
	}
	restore := func(into string) *cluster.BackupSummary {
		t.Helper()
		sum, err := c.tc.Nodes[1].RunRestoreOpts(ctx, server.RestoreOptions{Srcs: []string{dir}, Tables: []server.RestoreTable{{Name: "items", Into: into}}})
		if err != nil {
			t.Fatalf("table restore as %s: %v", into, err)
		}
		return sum
	}
	check := func(table string) {
		t.Helper()
		for i := 0; i < 3; i++ {
			cn := sealedConn(t, ctx, c, i, nil)
			sameStrings(t, table+" through n"+fmt.Sprint(i+1), queryStrings(t, ctx, cn, `SELECT count(*), min(body) FROM `+table+` WHERE t > 0`), fmt.Sprintf("%d|SEKRIT-1", tenants-1))
			wantSQLErr(t, ctx, cn, `SELECT body FROM `+table+` WHERE t = 0`, "XXK01", "destroyed")
		}
	}

	t.Run("beside the live table", func(t *testing.T) {
		restore("items_copy")
		rd := tableDesc(t, ctx, c, "items_copy")
		if !rd.SealedPerTenant() || rd.EncryptionKey.SealID != d.ID || rd.EncryptionKey.KeyID != d.EncryptionKey.KeyID {
			t.Fatalf("restored descriptor: %+v", rd.EncryptionKey)
		}
		check("items_copy")
		if n := len(liveTenantKeys(t, ctx, ks, d.ID)); n != tenants-1 {
			t.Fatalf("%d live tenant keys after a restore beside (shared, want %d)", n, tenants-1)
		}
		// Dropping one of the two keeps the keys for the other.
		sealExec(t, ctx, conn, `DROP TABLE items_copy`)
		time.Sleep(time.Second)
		if n, err := c.tc.Nodes[0].SweepOrphanTenantKeys(ctx); err != nil || n != 0 {
			t.Fatalf("a sweep while items still names the keys: %d %v", n, err)
		}
		sameStrings(t, "items after its copy's drop", queryStrings(t, ctx, conn, `SELECT count(*) FROM items WHERE t > 0`), fmt.Sprint(tenants-1))
	})

	t.Run("after the table was dropped", func(t *testing.T) {
		sealExec(t, ctx, conn, `DROP TABLE items`)
		waitTenantKeysGone(t, ctx, ks, d.ID)
		sum := restore("items_back")
		var notes []string
		for _, n := range sum.Notes {
			if strings.Contains(n, "brought back") {
				notes = append(notes, n)
			}
		}
		// The naming key, and the tenant keys (all of them unless the
		// janitor was still destroying some as the restore began: those
		// it spares, and the count below checks every one is live).
		if len(notes) != 2 || !strings.Contains(strings.Join(notes, ";"), "tenant keys brought back") {
			t.Fatalf("restore notes: %v", sum.Notes)
		}
		time.Sleep(sealedLease + 200*time.Millisecond)
		check("items_back")
		if n := len(liveTenantKeys(t, ctx, ks, d.ID)); n != tenants-1 {
			t.Fatalf("%d live tenant keys after the restore", n)
		}
		// The erased tenant writes again under a fresh key; others keep
		// theirs.
		sealExec(t, ctx, conn, `INSERT INTO items_back VALUES (0, 2, 'SEKRIT-0-new'), (5, 2, 'SEKRIT-5-new')`)
		sameStrings(t, "the erased tenant's new row", queryStrings(t, ctx, conn, `SELECT body FROM items_back WHERE t = 0 AND id = 2`), "SEKRIT-0-new")
		sameStrings(t, "another tenant's new row", queryStrings(t, ctx, conn, `SELECT body FROM items_back WHERE t = 5 ORDER BY id`), "SEKRIT-5", "SEKRIT-5-new")
		if n := len(liveTenantKeys(t, ctx, ks, d.ID)); n != tenants {
			t.Fatalf("%d live tenant keys after the erased tenant's new write, want %d", n, tenants)
		}
		c.checkConsistent("after the per-tenant restore")
	})
}
