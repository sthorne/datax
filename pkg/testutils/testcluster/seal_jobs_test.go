package testcluster

import (
	"bytes"
	"context"
	"encoding/hex"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/keystore"
	"github.com/sthorne/datax/pkg/kvclient"
	"github.com/sthorne/datax/pkg/server"
	"github.com/sthorne/datax/pkg/sql/catalog"
	"github.com/sthorne/datax/pkg/sql/rowenc"
	"github.com/sthorne/datax/pkg/sql/rowseal"
	"github.com/sthorne/datax/pkg/sql/types"
)

// startSealJobCluster is a secure 3-node cluster whose seal jobs run
// every 200 ms in batches of 10 rows, 30 ms apart: slow enough that a
// job of a few hundred rows is still running when a test looks at it.
func startSealJobCluster(t *testing.T, ctx context.Context) *ckCluster {
	c := startCKCluster(t, ctx, func(i int, cfg *server.Config) {
		cfg.KeyCacheLease = sealedLease
		cfg.TenantKeyGrace = -1
		cfg.SealJobInterval = 200 * time.Millisecond
		cfg.SealJobBatchSize = 10
		cfg.SealJobBatchPause = 30 * time.Millisecond
	})
	c.waitReported(3)
	return c
}

// sealJob reads a table's seal job record (nil: none), through any node
// that answers. It reads in a transaction, as the job runner does: a
// batch whose node was stopped leaves its lock on the record, which a
// transaction's read clears once the batch has expired and a plain read
// only fails on.
func sealJob(t *testing.T, ctx context.Context, c *ckCluster, id uint64) *catalog.SealJob {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for {
		for i := range c.tc.Nodes {
			n := c.tc.Node(i)
			if n == nil {
				continue
			}
			rctx, cancel := context.WithTimeout(ctx, 2*time.Second)
			var raw []byte
			err := n.DB().RunTxn(rctx, "test-seal-job", func(ctx context.Context, txn *kvclient.Txn) error {
				var err error
				raw, err = txn.Get(ctx, keys.SealJobKey(id))
				return err
			})
			cancel()
			if err != nil {
				continue
			}
			if raw == nil {
				return nil
			}
			j, err := catalog.DecodeSealJob(raw)
			if err != nil {
				t.Fatal(err)
			}
			return j
		}
		if time.Now().After(deadline) {
			t.Fatal("no node could read the seal job")
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// waitSealJobDone waits for a table's seal job to finish.
func waitSealJobDone(t *testing.T, ctx context.Context, c *ckCluster, id uint64) {
	t.Helper()
	deadline := time.Now().Add(90 * time.Second)
	for sealJob(t, ctx, c, id) != nil {
		if time.Now().After(deadline) {
			t.Fatalf("the seal job of table %d is still running: %+v", id, sealJob(t, ctx, c, id))
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// insertRows inserts rows (one VALUES tuple each) into table, a hundred
// to a statement.
func insertRows(t *testing.T, ctx context.Context, conn *pgx.Conn, table string, rows []string) {
	t.Helper()
	for at := 0; at < len(rows); at += 100 {
		sealExec(t, ctx, conn, fmt.Sprintf(`INSERT INTO %s VALUES %s`, table, strings.Join(rows[at:min(at+100, len(rows))], ", ")))
	}
}

// keyVersions lists a key's versions.
func keyVersions(t *testing.T, ctx context.Context, ks *keystore.Store, id int64) []int {
	t.Helper()
	k, err := ks.GetByID(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	var out []int
	for _, v := range k.Versions {
		out = append(out, v.Version)
	}
	return out
}

// valueVersions counts a whole-sealed table's primary values by key
// version (0: plain).
func valueVersions(t *testing.T, ctx context.Context, c *ckCluster, id uint64) map[int]int {
	t.Helper()
	primary, _ := tableKVs(t, ctx, c, id)
	out := map[int]int{}
	for _, kv := range primary {
		if !rowseal.IsSealed(kv[1]) {
			out[0]++
			continue
		}
		v, err := rowseal.Version(kv[1])
		if err != nil {
			t.Fatal(err)
		}
		out[v]++
	}
	return out
}

// TestSealJobs (issue #310, step 5): rotation of a table's key and of a
// tenant's, with the background job rewriting the rows and destroying
// the old version (resuming after the leader of range 1 restarts
// mid-job); sealing a plain table that holds rows, whole and per tenant,
// with reads of mixed rows during the job and strict reads after it, and
// PURGE removing the plaintext history; a destroyed tenant's leftover
// rows removed with their index entries; listings that read only the
// key service's keys; and the refusals.
func TestSealJobs(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 600*time.Second)
	defer cancel()
	c := startSealJobCluster(t, ctx)
	outer := t // connections made in a subtest that outlive it
	var notices noticeLog
	conns := []*pgx.Conn{sealedConn(t, ctx, c, 0, &notices), sealedConn(t, ctx, c, 1, nil), sealedConn(t, ctx, c, 2, nil)}
	conn := conns[0]

	sealExec(t, ctx, conn, `CREATE TABLE early (id INT8 PRIMARY KEY, body TEXT)`)
	wantSQLErr(t, ctx, conn, `ALTER TABLE early SET (encryption_key = 'auto')`, "0A000", "cluster key")
	if errText, _ := c.op(0, "init"); errText != "" {
		t.Fatalf("init: %s", errText)
	}
	ks := c.keyStore(0)
	alive := 0 // a node that has not been restarted

	t.Run("rotate", func(t *testing.T) {
		sealExec(t, ctx, conn, `CREATE TABLE rot (id INT8 PRIMARY KEY, body TEXT, n INT8) WITH (encryption_key = 'auto')`)
		var rows []string
		for i := 1; i <= 60; i++ {
			rows = append(rows, fmt.Sprintf(`(%d, 'SEKRIT-%d', %d)`, i, i, i))
		}
		insertRows(t, ctx, conn, "rot", rows)
		d := tableDesc(t, ctx, c, "rot")
		if got := valueVersions(t, ctx, c, d.ID); got[1] != 60 || len(got) != 1 {
			t.Fatalf("before: %v", got)
		}
		notices.take()
		sealExec(t, ctx, conn, `ALTER TABLE rot ROTATE ENCRYPTION KEY`)
		if got := notices.take(); len(got) != 1 || !strings.Contains(got[0], "version 2") || !strings.Contains(got[0], "datax_encryption_jobs") {
			t.Fatalf("notices: %q", got)
		}
		// One job per table.
		wantSQLErr(t, ctx, conn, `ALTER TABLE rot ROTATE ENCRYPTION KEY`, "55006", "seal job in progress")
		sameStrings(t, "the job, listed", queryStrings(t, ctx, conns[1], `SELECT table_name, kind, scope, version, keep_old_versions FROM datax_encryption_jobs`), "rot|rotate|table|2|false")
		// Reads and writes while the job runs, on every gateway.
		for i, cn := range conns {
			sealExec(t, ctx, cn, fmt.Sprintf(`UPDATE rot SET body = 'NEW-%d' WHERE id = %d`, i, i+1))
			sealExec(t, ctx, cn, fmt.Sprintf(`INSERT INTO rot VALUES (%d, 'LATE-%d', 0)`, 100+i, i))
		}
		for _, cn := range conns {
			sameStrings(t, "a read during the job", queryStrings(t, ctx, cn, `SELECT count(*), sum(n) FROM rot`), "63|1830")
		}
		waitSealJobDone(t, ctx, c, d.ID)
		if got := valueVersions(t, ctx, c, d.ID); got[2] != 63 || len(got) != 1 {
			t.Fatalf("after the job: values by version %v", got)
		}
		if got := keyVersions(t, ctx, ks, d.EncryptionKey.KeyID); len(got) != 1 || got[0] != 2 {
			t.Fatalf("after the job the key has versions %v, want only 2", got)
		}
		for _, cn := range conns {
			sameStrings(t, "after", queryStrings(t, ctx, cn, `SELECT body FROM rot WHERE id IN (1, 2, 3, 4, 101) ORDER BY id`), "NEW-0", "NEW-1", "NEW-2", "SEKRIT-4", "LATE-1")
		}
		sameStrings(t, "no job listed", queryStrings(t, ctx, conn, `SELECT count(*) FROM datax_encryption_jobs`), "0")

		// KEEP OLD VERSIONS, then an explicit destruction.
		sealExec(t, ctx, conn, `ALTER TABLE rot ROTATE ENCRYPTION KEY KEEP OLD VERSIONS`)
		waitSealJobDone(t, ctx, c, d.ID)
		if got := valueVersions(t, ctx, c, d.ID); got[3] != 63 || len(got) != 1 {
			t.Fatalf("after the second job: %v", got)
		}
		if got := keyVersions(t, ctx, ks, d.EncryptionKey.KeyID); len(got) != 2 {
			t.Fatalf("KEEP OLD VERSIONS: the key has versions %v", got)
		}
		sameStrings(t, "destroying the old version", queryStrings(t, ctx, conn, `SELECT datax_destroy_old_key_versions('rot')`), "1")
		if got := keyVersions(t, ctx, ks, d.EncryptionKey.KeyID); len(got) != 1 || got[0] != 3 {
			t.Fatalf("after the explicit destruction: %v", got)
		}
		sameStrings(t, "nothing more to destroy", queryStrings(t, ctx, conn, `SELECT datax_destroy_old_key_versions('rot')`), "0")
	})

	t.Run("early destruction makes rows unreadable", func(t *testing.T) {
		sealExec(t, ctx, conn, `CREATE TABLE early2 (id INT8 PRIMARY KEY, body TEXT) WITH (encryption_key = 'auto')`)
		sealExec(t, ctx, conn, `INSERT INTO early2 VALUES (1, 'SEKRIT')`)
		d := tableDesc(t, ctx, c, "early2")
		sealExec(t, ctx, conn, `ALTER TABLE early2 ROTATE ENCRYPTION KEY KEEP OLD VERSIONS`)
		// Destroyed before the job could start (it waits out the lease).
		sameStrings(t, "destroyed early", queryStrings(t, ctx, conn, `SELECT datax_destroy_old_key_versions('early2')`), "1")
		wantSQLErr(t, ctx, conn, `SELECT body FROM early2`, "XXK01", "version 1 has been destroyed")
		// The job steps over the row no one can read, and finishes.
		waitSealJobDone(t, ctx, c, d.ID)
		sealExec(t, ctx, conn, `DROP TABLE early2`)
	})

	t.Run("resumes after the leader restarts", func(t *testing.T) {
		sealExec(t, ctx, conn, `CREATE TABLE big (id INT8 PRIMARY KEY, body TEXT) WITH (encryption_key = 'auto')`)
		var rows []string
		for i := 1; i <= 400; i++ {
			rows = append(rows, fmt.Sprintf(`(%d, 'SEKRIT-%d')`, i, i))
		}
		insertRows(t, ctx, conn, "big", rows)
		d := tableDesc(t, ctx, c, "big")
		sealExec(t, ctx, conn, `ALTER TABLE big ROTATE ENCRYPTION KEY`)
		deadline := time.Now().Add(60 * time.Second)
		for {
			j := sealJob(t, ctx, c, d.ID)
			if j == nil {
				t.Fatal("the job finished before the restart; make it slower")
			}
			if j.Rewritten >= 30 {
				if j.Pass != 1 || j.Rewritten >= 400 {
					t.Fatalf("the job is too far along to be cut short: %+v", j)
				}
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("the job did not start: %+v", j)
			}
			time.Sleep(20 * time.Millisecond)
		}
		leader := c.tc.LeaderIndex(1)
		// Restarted with a SQL listener, so that it serves SQL again; the
		// connections to it are made anew. Should this subtest fail
		// before the restart, its cleanup restarts the node, so that the
		// subtests after it run on a whole cluster.
		restarted := false
		restart := func(t *testing.T) {
			t.Helper()
			restarted = true
			pglis, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			if _, err := c.tc.RestartNodeErr(leader, nil, func(cfg *server.Config) {
				cfg.CertsDir = c.certsDir
				c.config(leader, cfg)
				cfg.PGListener = pglis
			}); err != nil {
				t.Fatalf("restarting n%d: %v", leader+1, err)
			}
			// The restarted node may still be fetching the cluster key:
			// use another's key store from here on.
			alive = (leader + 1) % 3
			ks = c.keyStore(alive)
			conns[leader] = sealedConn(outer, ctx, c, leader, nil)
			if leader == 0 {
				conn = sealedConn(outer, ctx, c, 0, &notices)
				conns[0] = conn
			}
		}
		t.Cleanup(func() {
			if !restarted {
				restart(t)
			}
		})
		c.tc.StopNode(leader)
		before := sealJob(t, ctx, c, d.ID)
		restart(t)
		waitSealJobDone(t, ctx, c, d.ID)
		if got := valueVersions(t, ctx, c, d.ID); got[2] != 400 || len(got) != 1 {
			t.Fatalf("after the restart: %v", got)
		}
		if got := keyVersions(t, ctx, ks, d.EncryptionKey.KeyID); len(got) != 1 || got[0] != 2 {
			t.Fatalf("after the restart the key has versions %v", got)
		}
		t.Logf("cut short at %d rows rewritten (pass %d); finished after the restart of n%d", before.Rewritten, before.Pass, leader+1)
		other := conns[(leader+1)%3]
		sameStrings(t, "rows", queryStrings(t, ctx, other, `SELECT count(*), max(body) FROM big`), "400|SEKRIT-99")
	})

	// A batch whose node stops mid-transaction leaves its lock on the
	// job's record (the restart above stops the node gracefully, which
	// mostly lets the batch roll back). The runner reads the record in a
	// transaction, which clears the lock once the batch has expired; it
	// read it plainly before, failed on the lock every turn, and the job
	// stood still until another transaction came to read the record.
	t.Run("a lock left by a stopped batch does not hold the job up", func(t *testing.T) {
		sealExec(t, ctx, conn, `CREATE TABLE stuck (id INT8 PRIMARY KEY, body TEXT) WITH (encryption_key = 'auto')`)
		var rows []string
		for i := 1; i <= 100; i++ {
			rows = append(rows, fmt.Sprintf(`(%d, 'SEKRIT-%d')`, i, i))
		}
		insertRows(t, ctx, conn, "stuck", rows)
		d := tableDesc(t, ctx, c, "stuck")
		sealExec(t, ctx, conn, `ALTER TABLE stuck ROTATE ENCRYPTION KEY`)
		db := c.node(t, alive).DB()
		batch := db.NewTxn("test-stopped-batch")
		if _, err := batch.GetForUpdate(ctx, keys.SealJobKey(d.ID)); err != nil {
			t.Fatal(err)
		}
		batch.AbandonForTesting()
		// Plain reads, which do not clear the lock themselves.
		deadline := time.Now().Add(60 * time.Second)
		for {
			raw, err := db.Get(ctx, keys.SealJobKey(d.ID))
			if err == nil && raw == nil {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("the job is not done a minute after its record was left locked: %v", err)
			}
			time.Sleep(100 * time.Millisecond)
		}
		if got := valueVersions(t, ctx, c, d.ID); got[2] != 100 || len(got) != 1 {
			t.Fatalf("after the job: %v", got)
		}
	})

	t.Run("rotate one tenant", func(t *testing.T) {
		sealExec(t, ctx, conn, `CREATE TABLE ten (tenant_id UUID, id INT8, body TEXT, PRIMARY KEY (tenant_id, id)) WITH (encryption_key_by = 'tenant_id')`)
		var rows []string
		for tn := 1; tn <= 3; tn++ {
			for i := 1; i <= 20; i++ {
				rows = append(rows, fmt.Sprintf(`('%s', %d, 'SEKRIT-%d-%d')`, tenantUUID(tn), i, tn, i))
			}
		}
		insertRows(t, ctx, conn, "ten", rows)
		d := tableDesc(t, ctx, c, "ten")
		k1 := tenantKeyOf(t, sealedKeyIDs(t, ctx, c, d), d, tenantUUID(1))
		wantSQLErr(t, ctx, conn, `ALTER TABLE ten ROTATE ENCRYPTION KEY`, "42809", "datax_rotate_tenant_key")
		sameStrings(t, "rotate tenant 1", queryStrings(t, ctx, conn, fmt.Sprintf(`SELECT datax_rotate_tenant_key('ten', '%s')`, tenantUUID(1))), "2")
		sameStrings(t, "a tenant without a key", queryStrings(t, ctx, conn, fmt.Sprintf(`SELECT datax_rotate_tenant_key('ten', '%s') IS NULL`, tenantUUID(99))), "true")
		sameStrings(t, "the job", queryStrings(t, ctx, conn, `SELECT scope, key_id = `+strconv.FormatInt(k1, 10)+` FROM datax_encryption_jobs WHERE table_name = 'ten'`), "tenant|true")
		sealExec(t, ctx, conns[2], fmt.Sprintf(`UPDATE ten SET body = 'NEW' WHERE tenant_id = '%s' AND id = 5`, tenantUUID(1)))
		waitSealJobDone(t, ctx, c, d.ID)
		primary, _ := tableKVs(t, ctx, c, d.ID)
		for _, kv := range primary {
			id, v, err := rowseal.TenantKeyOf(kv[1])
			if err != nil {
				t.Fatal(err)
			}
			tenant, _ := rowseal.TenantOf(d, kv[0])
			enc1, _ := rowseal.TenantValue(d, stringDatum(tenantUUID(1)))
			want := 1
			if bytes.Equal(tenant, enc1) {
				want = 2
				if id != k1 {
					t.Fatalf("tenant 1's row under key %d, want %d", id, k1)
				}
			}
			if v != want {
				t.Fatalf("a row of tenant %x is under version %d, want %d", tenant, v, want)
			}
		}
		if got := keyVersions(t, ctx, ks, k1); len(got) != 1 || got[0] != 2 {
			t.Fatalf("tenant 1's key has versions %v", got)
		}
		sameStrings(t, "rows", queryStrings(t, ctx, conns[1], fmt.Sprintf(`SELECT count(*), max(body) FROM ten WHERE tenant_id = '%s'`, tenantUUID(1))), "20|SEKRIT-1-9")
		sameStrings(t, "all rows", queryStrings(t, ctx, conns[1], `SELECT count(*) FROM ten`), "60")
	})

	t.Run("seal an existing table", func(t *testing.T) {
		sealExec(t, ctx, conn, `CREATE TABLE old (id INT8 PRIMARY KEY, email TEXT, body TEXT, n INT8)`)
		sealExec(t, ctx, conn, `CREATE INDEX old_email ON old (email)`)
		var rows []string
		for i := 1; i <= 300; i++ {
			rows = append(rows, fmt.Sprintf(`(%d, 'e%d@x', 'PLAINSEKRIT-%d', %d)`, i, i, i, i))
		}
		insertRows(t, ctx, conn, "old", rows)
		before := strconv.FormatInt(c.tc.Nodes[0].Clock().Now().WallTime, 10)
		// The rows' plaintext versions are history from here.
		notices.take()
		sealExec(t, ctx, conn, `ALTER TABLE old SET (encryption_key = 'auto')`)
		got := notices.take()
		if len(got) != 2 || !strings.Contains(got[0], `table "old" is sealed`) || !strings.Contains(got[1], "MVCC history") || !strings.Contains(got[1], "PURGE") {
			t.Fatalf("notices: %q", got)
		}
		d := tableDesc(t, ctx, c, "old")
		if !d.Sealed() || !d.EncryptionKey.Sealing {
			t.Fatalf("descriptor during the job: %+v", d.EncryptionKey)
		}
		wantSQLErr(t, ctx, conn, `ALTER TABLE old SET (encryption_key = 'auto')`, "55000", "sealed already")
		// Mixed rows while the job runs: writes seal, reads take both.
		sealExec(t, ctx, conns[1], `UPDATE old SET body = 'CHANGED' WHERE id = 300`)
		sealExec(t, ctx, conns[2], `INSERT INTO old VALUES (1000, 'new@x', 'FRESH', 0)`)
		mixed := false
		for i := 0; i < 50 && !mixed; i++ {
			vv := valueVersions(t, ctx, c, d.ID)
			mixed = vv[0] > 0 && vv[1] > 0
			for _, cn := range conns {
				sameStrings(t, "a read of mixed rows", queryStrings(t, ctx, cn, `SELECT count(*), sum(n), max(body) FROM old`), "301|45150|PLAINSEKRIT-99")
				sameStrings(t, "through the index", queryStrings(t, ctx, cn, `SELECT body FROM old WHERE email IN ('e300@x', 'new@x', 'e7@x') ORDER BY id`), "PLAINSEKRIT-7", "CHANGED", "FRESH")
			}
		}
		if !mixed {
			t.Log("never caught the table with both plain and sealed rows (the job was too fast)")
		}
		waitSealJobDone(t, ctx, c, d.ID)
		d = tableDesc(t, ctx, c, "old")
		if d.EncryptionKey.Sealing {
			t.Fatal("the job finished and the descriptor still says sealing")
		}
		if got := valueVersions(t, ctx, c, d.ID); got[1] != 301 || len(got) != 1 {
			t.Fatalf("after the job: %v", got)
		}
		primary, other := tableKVs(t, ctx, c, d.ID)
		for _, kv := range append(primary, other...) {
			if bytes.Contains(kv[1], []byte("SEKRIT")) || bytes.Contains(kv[1], []byte("CHANGED")) {
				t.Fatalf("plaintext in a live value: %q", kv[1])
			}
		}
		for _, cn := range conns {
			sameStrings(t, "after", queryStrings(t, ctx, cn, `SELECT count(*), sum(n), max(body) FROM old`), "301|45150|PLAINSEKRIT-99")
		}
		// Strict reads are back: a plain value is corruption again.
		pk, err := rowenc.EncodePK(d, []types.Datum{types.NewInt(2000)})
		if err != nil {
			t.Fatal(err)
		}
		plain, err := rowenc.EncodeValue(d, map[catalog.ColumnID]types.Datum{3: types.NewString("smuggled"), 4: types.NewInt(0)})
		if err != nil {
			t.Fatal(err)
		}
		if err := c.tc.Nodes[0].DB().Put(ctx, pk, plain); err != nil {
			t.Fatal(err)
		}
		// (Once this gateway's descriptor lease has the job's last
		// descriptor: within the lease.)
		for deadline := time.Now().Add(30 * time.Second); ; {
			_, err := conn.Exec(ctx, `SELECT body FROM old WHERE id = 2000`)
			if err != nil || time.Now().After(deadline) {
				break
			}
			time.Sleep(200 * time.Millisecond)
		}
		wantSQLErr(t, ctx, conn, `SELECT body FROM old WHERE id = 2000`, "XX001", "not sealed")
		if err := c.tc.Nodes[0].DB().Delete(ctx, pk); err != nil {
			t.Fatal(err)
		}
		// History still holds the plaintext until PURGE.
		sameStrings(t, "history before PURGE", queryStrings(t, ctx, conn, `SELECT body FROM old AS OF SYSTEM TIME '`+before+`' WHERE id = 5`), "PLAINSEKRIT-5")
		tag, err := conn.Exec(ctx, `ALTER TABLE old PURGE`)
		if err != nil || !strings.HasPrefix(tag.String(), "PURGE ") || tag.String() == "PURGE 0" {
			t.Fatalf("PURGE: %q %v", tag.String(), err)
		}
		wantSQLErr(t, ctx, conn, `SELECT body FROM old AS OF SYSTEM TIME '`+before+`' WHERE id = 5`, "72000")
		c.checkConsistent("after sealing and purging")
	})

	t.Run("seal an existing table per tenant", func(t *testing.T) {
		sealExec(t, ctx, conn, `CREATE TABLE oldt (tenant_id INT8, id INT8, title TEXT, PRIMARY KEY (tenant_id, id))`)
		var rows []string
		for tn := 1; tn <= 4; tn++ {
			for i := 1; i <= 25; i++ {
				rows = append(rows, fmt.Sprintf(`(%d, %d, 'PLAINSEKRIT-%d-%d')`, tn, i, tn, i))
			}
		}
		insertRows(t, ctx, conn, "oldt", rows)
		wantSQLErr(t, ctx, conn, `ALTER TABLE oldt SET (encryption_key_by = 'id')`, "42P17", "leading primary-key column")
		wantSQLErr(t, ctx, conn, `ALTER TABLE oldt SET (encryption_key_by = 'tenant_id', gc_ttl = '1h')`, "0A000", "statement of its own")
		sealExec(t, ctx, conn, `ALTER TABLE oldt SET (encryption_key_by = 'tenant_id')`)
		d := tableDesc(t, ctx, c, "oldt")
		if !d.SealedPerTenant() || !d.EncryptionKey.Sealing {
			t.Fatalf("descriptor: %+v", d.EncryptionKey)
		}
		sealExec(t, ctx, conns[1], `INSERT INTO oldt VALUES (5, 1, 'FRESH')`)
		sameStrings(t, "during the job", queryStrings(t, ctx, conns[2], `SELECT count(*), min(title) FROM oldt`), "101|FRESH")
		// One job per table: no tenant rotation while it seals.
		wantSQLErr(t, ctx, conn, `SELECT datax_rotate_tenant_key('oldt', 5)`, "55006", "seal job in progress")
		waitSealJobDone(t, ctx, c, d.ID)
		d = tableDesc(t, ctx, c, "oldt")
		if d.EncryptionKey.Sealing {
			t.Fatal("still sealing after the job")
		}
		ids := sealedKeyIDs(t, ctx, c, d, "PLAINSEKRIT", "FRESH")
		if len(ids) != 5 {
			t.Fatalf("%d tenants have keys, want 5", len(ids))
		}
		sameStrings(t, "after", queryStrings(t, ctx, conn, `SELECT count(*), max(title) FROM oldt WHERE tenant_id = 3`), "25|PLAINSEKRIT-3-9")
	})

	t.Run("a destroyed tenant's rows removed without its key", func(t *testing.T) {
		sealExec(t, ctx, conn, `CREATE TABLE docs (tenant_id UUID, id INT8, title TEXT, tag TEXT, code TEXT, body JSONB, PRIMARY KEY (tenant_id, id), UNIQUE (code)) WITH (encryption_key_by = 'tenant_id')`)
		sealExec(t, ctx, conn, `CREATE INDEX docs_tenant_tag ON docs (tenant_id, tag)`)
		sealExec(t, ctx, conn, `CREATE INDEX docs_tag ON docs (tag)`)
		sealExec(t, ctx, conn, `CREATE INDEX docs_body ON docs USING GIN (body)`)
		var rows []string
		for tn := 1; tn <= 3; tn++ {
			for i := 1; i <= 30; i++ {
				rows = append(rows, fmt.Sprintf(`('%s', %d, 'SEKRIT-%d-%d', 'tag%d', 'c-%d-%d', '{"k": %d}')`, tenantUUID(tn), i, tn, i, i%4, tn, i, i%3))
			}
		}
		insertRows(t, ctx, conn, "docs", rows)
		d := tableDesc(t, ctx, c, "docs")
		ta, tb := tenantUUID(1), tenantUUID(2)
		_, others := tableKVs(t, ctx, c, d.ID)
		entriesBefore := len(others)
		// The erasure done in the wrong order: the key first.
		if _, err := conns[1].Exec(ctx, `SELECT datax_destroy_tenant_key('docs', $1)`, ta); err != nil {
			t.Fatal(err)
		}
		wantSQLErr(t, ctx, conn, fmt.Sprintf(`DELETE FROM docs WHERE tenant_id = '%s'`, ta), "XXK01")
		// The tenant writes again: a fresh key, a readable row that stays.
		sealExec(t, ctx, conn, fmt.Sprintf(`INSERT INTO docs VALUES ('%s', 100, 'AGAIN', 'tag1', 'c-again', '{"k": 1}')`, ta))
		sameStrings(t, "removed", queryStrings(t, ctx, conns[2], fmt.Sprintf(`SELECT datax_remove_tenant_rows('docs', '%s')`, ta)), "30")
		sameStrings(t, "again: nothing", queryStrings(t, ctx, conn, fmt.Sprintf(`SELECT datax_remove_tenant_rows('docs', '%s')`, ta)), "0")
		// Every read works again, through every index.
		for _, cn := range conns {
			sameStrings(t, "tenant a", queryStrings(t, ctx, cn, fmt.Sprintf(`SELECT id, title FROM docs WHERE tenant_id = '%s'`, ta)), "100|AGAIN")
			sameStrings(t, "all", queryStrings(t, ctx, cn, `SELECT count(*) FROM docs`), "61")
			sameStrings(t, "by tag", queryStrings(t, ctx, cn, `SELECT count(*) FROM docs WHERE tag = 'tag1'`), "17")
			sameStrings(t, "by tenant and tag", queryStrings(t, ctx, cn, fmt.Sprintf(`SELECT count(*) FROM docs WHERE tenant_id = '%s' AND tag = 'tag1'`, tb)), "8")
			sameStrings(t, "by code", queryStrings(t, ctx, cn, `SELECT count(*) FROM docs WHERE code LIKE 'c-1-%'`), "0")
			sameStrings(t, "by body", queryStrings(t, ctx, cn, `SELECT count(*) FROM docs WHERE body @> '{"k": 1}'`), "21")
		}
		// No index entry is left for a removed row: each points at a row.
		primary, others := tableKVs(t, ctx, c, d.ID)
		live := map[string]bool{}
		for _, kv := range primary {
			live[string(kv[0])] = true
		}
		for _, kv := range others {
			var idx *catalog.IndexDescriptor
			for i := range d.Indexes {
				if bytes.HasPrefix(kv[0], keys.TableIndexPrefix(d.ID, d.Indexes[i].ID)) {
					idx = &d.Indexes[i]
				}
			}
			if idx == nil {
				continue
			}
			pk, err := rowenc.IndexEntryPrimaryKey(d, idx, kv[0], kv[1])
			if err != nil {
				t.Fatal(err)
			}
			if !live[string(pk)] {
				t.Fatalf("index %s has an entry for a row that is gone", idx.Name)
			}
		}
		t.Logf("index entries: %d before, %d after", entriesBefore, len(others))
		c.checkConsistent("after removing a tenant's rows")
		wantSQLErr(t, ctx, conn, `SELECT datax_remove_tenant_rows('rot', 1)`, "42809", "not sealed per tenant")
		sealExec(t, ctx, conn, `CREATE TABLE refs (id INT8 PRIMARY KEY, code TEXT REFERENCES docs (code))`)
		wantSQLErr(t, ctx, conn, fmt.Sprintf(`SELECT datax_remove_tenant_rows('docs', '%s')`, ta), "0A000", "foreign keys", "refs")
		sealExec(t, ctx, conn, `DROP TABLE refs`)
	})

	t.Run("listings read only the key service's keys", func(t *testing.T) {
		sealExec(t, ctx, conn, `CREATE ENCRYPTION KEY billing`)
		sealExec(t, ctx, conn, `CREATE USER keyowner WITH PASSWORD 'pw-ko'`)
		sealExec(t, ctx, conn, `CREATE ENCRYPTION KEY billing2`)
		sealExec(t, ctx, conn, `ALTER ENCRYPTION KEY billing2 OWNER TO keyowner`)
		wantSQLErr(t, ctx, conn, `CREATE ENCRYPTION KEY "tenant/1/abc"`, "42939", "reserved")
		wantSQLErr(t, ctx, conn, `CREATE ENCRYPTION KEY "table/9"`, "42939", "reserved")
		// 10,000 tenant keys, made the way first writes make them (2,000
		// under the race detector, where making them takes minutes; the
		// listing's cost is the same either way).
		tenants := 10000
		if raceEnabled {
			tenants = 2000
		}
		// 25 to a transaction: eight of them at once contend on the keys
		// table's ranges, and a restart of a short one loses little. With
		// 100 to a transaction, after the subtests above, making them
		// under the race detector took minutes of restarts, long enough
		// for a purge of the keys' history to refuse one past its retries.
		const per = 25
		start := time.Now()
		db := c.node(t, alive).DB()
		var wg sync.WaitGroup
		errs := make(chan error, tenants/per)
		sem := make(chan struct{}, 8)
		for at := 0; at < tenants; at += per {
			wg.Add(1)
			go func(at int) {
				defer wg.Done()
				sem <- struct{}{}
				defer func() { <-sem }()
				errs <- db.RunTxn(ctx, "test-tenant-keys", func(ctx context.Context, txn *kvclient.Txn) error {
					for i := at; i < at+per; i++ {
						name := rowseal.TenantKeyName(999999, []byte(hex.EncodeToString([]byte(strconv.Itoa(i)))))
						if _, err := ks.CreateIn(ctx, txn, keystore.KindTenant, name); err != nil {
							return err
						}
					}
					return nil
				})
			}(at)
		}
		wg.Wait()
		close(errs)
		for err := range errs {
			if err != nil {
				t.Fatal(err)
			}
		}
		t.Logf("made %d tenant keys in %s", tenants, time.Since(start).Round(time.Millisecond))
		var all []keystore.Key
		was := keystore.ListRowsRead()
		if err := db.RunTxn(ctx, "test-list", func(ctx context.Context, txn *kvclient.Txn) error {
			var err error
			all, err = ks.ListIn(ctx, txn)
			return err
		}); err != nil {
			t.Fatal(err)
		}
		whole := keystore.ListRowsRead() - was
		if len(all) < tenants {
			t.Fatalf("ListIn listed %d keys", len(all))
		}
		showKeys := func(field int) []string {
			var out []string
			for _, r := range queryStrings(t, ctx, conn, `SHOW ENCRYPTION KEYS`) {
				out = append(out, strings.Split(r, "|")[0]+"|"+strings.Split(r, "|")[field])
			}
			return out
		}
		measure := func(what, q string, want ...string) int64 {
			t.Helper()
			was := keystore.ListRowsRead()
			if want != nil {
				sameStrings(t, what, showKeys(1), want...)
			} else {
				sealExec(t, ctx, conn, q)
			}
			n := keystore.ListRowsRead() - was
			t.Logf("%s: %d datax_keys entries read (every key: %d rows)", what, n, whole)
			if n > 100 {
				t.Fatalf("%s read %d entries of datax_keys with %d keys in it", what, n, len(all))
			}
			return n
		}
		measure("SHOW ENCRYPTION KEYS", `SHOW ENCRYPTION KEYS`, "billing|enabled", "billing2|enabled")
		sealExec(t, ctx, conn, `CREATE USER other WITH PASSWORD 'pw-o'`)
		measure("REASSIGN OWNED", `REASSIGN OWNED BY keyowner TO other`)
		wantSQLErr(t, ctx, conn, `DROP OWNED BY other`, "2BP01", "billing2")
		measure("DROP ROLE", `DROP ROLE keyowner`)
		sameStrings(t, "owners", showKeys(3), "billing|root", "billing2|other")
	})

	t.Run("refusals", func(t *testing.T) {
		sealExec(t, ctx, conn, `CREATE TABLE tsr (s INT8, at TIMESTAMPTZ, v FLOAT8, PRIMARY KEY (s, at)) WITH (timeseries = true)`)
		wantSQLErr(t, ctx, conn, `ALTER TABLE tsr SET (encryption_key = 'auto')`, "0A000", "time-series")
		wantSQLErr(t, ctx, conn, `ALTER TABLE tsr ROTATE ENCRYPTION KEY`, "55000", "not sealed")
		wantSQLErr(t, ctx, conn, `ALTER TABLE rot RESET (encryption_key)`, "0A000", "unsealing")
		wantSQLErr(t, ctx, conn, `ALTER TABLE early SET (encryption_key = 'mine')`, "22023", "'auto'")
		sealExec(t, ctx, conn, `CREATE USER sealapp WITH PASSWORD 'pw-sa'`)
		sealExec(t, ctx, conn, `GRANT SELECT, INSERT, UPDATE, DELETE ON early, rot, ten, docs TO sealapp`)
		app := ksConn(t, ctx, c, 1, "sealapp", "pw-sa")
		for _, q := range []string{
			`ALTER TABLE early SET (encryption_key = 'auto')`,
			`ALTER TABLE rot ROTATE ENCRYPTION KEY`,
			fmt.Sprintf(`SELECT datax_rotate_tenant_key('ten', '%s')`, tenantUUID(2)),
			`SELECT datax_destroy_old_key_versions('rot')`,
			fmt.Sprintf(`SELECT datax_remove_tenant_rows('docs', '%s')`, tenantUUID(2)),
		} {
			_, err := app.Exec(ctx, q)
			wantState(t, q+" by a non-owner", err, "42501", "owner")
		}
		tx, err := conn.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		_, err = tx.Exec(ctx, `ALTER TABLE rot ROTATE ENCRYPTION KEY`)
		wantState(t, "ROTATE in a transaction block", err, "25001", "transaction block")
		_ = tx.Rollback(ctx)
		sameStrings(t, "early is still plain", queryStrings(t, ctx, conn, `SELECT count(*) FROM early`), "0")
	})
}
