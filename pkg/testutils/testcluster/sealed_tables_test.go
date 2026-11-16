package testcluster

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/sthorne/datax/pkg/cluster"
	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/keystore"
	"github.com/sthorne/datax/pkg/kvclient"
	"github.com/sthorne/datax/pkg/kvpb"
	"github.com/sthorne/datax/pkg/server"
	"github.com/sthorne/datax/pkg/sql/catalog"
	"github.com/sthorne/datax/pkg/storage/enc"
	"github.com/sthorne/datax/pkg/version"
)

// noticeLog collects the NOTICEs a connection receives.
type noticeLog struct {
	mu  sync.Mutex
	got []string
}

func (l *noticeLog) take() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	out := l.got
	l.got = nil
	return out
}

// sealedConn opens a SQL connection to node i as root, collecting its
// notices in log (when given).
func sealedConn(t *testing.T, ctx context.Context, c *ckCluster, i int, log *noticeLog) *pgx.Conn {
	t.Helper()
	url := fmt.Sprintf("postgres://root:topsecret@%s/datax?sslmode=verify-ca&sslrootcert=%s",
		c.tc.Nodes[i].SQLAddr(), filepath.Join(c.certsDir, "ca.crt"))
	deadline := time.Now().Add(30 * time.Second)
	for {
		cfg, err := pgx.ParseConfig(url)
		if err != nil {
			t.Fatal(err)
		}
		cfg.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol
		if log != nil {
			cfg.OnNotice = func(_ *pgconn.PgConn, n *pgconn.Notice) {
				log.mu.Lock()
				log.got = append(log.got, n.Message)
				log.mu.Unlock()
			}
		}
		conn, err := pgx.ConnectConfig(ctx, cfg)
		if err == nil {
			t.Cleanup(func() { _ = conn.Close(context.Background()) })
			return conn
		}
		if time.Now().After(deadline) {
			t.Fatalf("connecting to n%d: %v", i+1, err)
		}
		time.Sleep(200 * time.Millisecond)
	}
}

func sealExec(t *testing.T, ctx context.Context, conn *pgx.Conn, q string, args ...any) {
	t.Helper()
	if _, err := conn.Exec(ctx, q, args...); err != nil {
		t.Fatalf("%s: %v", q, err)
	}
}

// wantSQLErr runs q and wants it to fail with code, its message holding
// every one of want.
func wantSQLErr(t *testing.T, ctx context.Context, conn *pgx.Conn, q, code string, want ...string) {
	t.Helper()
	_, err := conn.Exec(ctx, q)
	var pe *pgconn.PgError
	if !errors.As(err, &pe) {
		t.Fatalf("%s: %v, want an error %s", q, err, code)
	}
	if pe.Code != code {
		t.Fatalf("%s: %s %q, want code %s", q, pe.Code, pe.Message, code)
	}
	for _, w := range want {
		if !strings.Contains(pe.Message, w) {
			t.Fatalf("%s: %q, want it to mention %q", q, pe.Message, w)
		}
	}
}

func queryStrings(t *testing.T, ctx context.Context, conn *pgx.Conn, q string) []string {
	t.Helper()
	rows, err := conn.Query(ctx, q)
	if err != nil {
		t.Fatalf("%s: %v", q, err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		vals, err := rows.Values()
		if err != nil {
			t.Fatal(err)
		}
		var parts []string
		for _, v := range vals {
			parts = append(parts, fmt.Sprint(v))
		}
		out = append(out, strings.Join(parts, "|"))
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("%s: %v", q, err)
	}
	return out
}

func sameStrings(t *testing.T, what string, got []string, want ...string) {
	t.Helper()
	if strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Fatalf("%s:\n got %q\nwant %q", what, got, want)
	}
}

// tableDesc reads a table's descriptor through node 0.
func tableDesc(t *testing.T, ctx context.Context, c *ckCluster, name string) *catalog.TableDescriptor {
	t.Helper()
	lo, hi := keys.TableDescSpan()
	// In a transaction, so an in-flight descriptor write (a statistics
	// refresh, a lease) is pushed rather than failing the read.
	var kvs []kvpb.KeyValue
	if err := c.tc.Nodes[0].DB().RunTxn(ctx, "test-table-desc", func(ctx context.Context, txn *kvclient.Txn) error {
		var err error
		kvs, err = txn.Scan(ctx, lo, hi, 0)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	for _, kv := range kvs {
		var d catalog.TableDescriptor
		if json.Unmarshal(kv.Value, &d) == nil && d.Name == name {
			return &d
		}
	}
	t.Fatalf("no table %q", name)
	return nil
}

// tableKVs reads every KV pair of a table.
func tableKVs(t *testing.T, ctx context.Context, c *ckCluster, id uint64) (primary, other [][2][]byte) {
	t.Helper()
	lo, hi := keys.TableDataSpan(id)
	// In a transaction, so a write in flight beside the test (a seal job's
	// batch, a concurrent statement) is pushed rather than failing the read.
	var kvs []kvpb.KeyValue
	if err := c.tc.Nodes[0].DB().RunTxn(ctx, "test-table-kvs", func(ctx context.Context, txn *kvclient.Txn) error {
		var err error
		kvs, err = txn.Scan(ctx, lo, hi, 0)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	prim := keys.TableIndexPrefix(id, 1)
	for _, kv := range kvs {
		p := [2][]byte{kv.Key, kv.Value}
		if bytes.HasPrefix(kv.Key, prim) {
			primary = append(primary, p)
		} else {
			other = append(other, p)
		}
	}
	return primary, other
}

// checkConsistent runs the consistency checker over every range.
func (c *ckCluster) checkConsistent(what string) {
	c.t.Helper()
	descs, err := c.tc.ranges(c.ctx)
	if err != nil {
		c.t.Fatal(err)
	}
	for _, d := range descs {
		leader := c.tc.LeaderIndex(d.RangeID)
		if leader < 0 {
			c.t.Fatalf("%s: r%d has no leader", what, d.RangeID)
		}
		mismatch, err := c.tc.Nodes[leader].CheckRangeConsistency(c.ctx, d.RangeID)
		if err != nil {
			c.t.Fatalf("%s: consistency check of r%d: %v", what, d.RangeID, err)
		}
		if mismatch {
			c.t.Fatalf("%s: the replicas of r%d disagree", what, d.RangeID)
		}
	}
}

const sealedLease = time.Second

func startSealedCluster(t *testing.T, ctx context.Context) *ckCluster {
	c := startCKCluster(t, ctx, func(i int, cfg *server.Config) { cfg.KeyCacheLease = sealedLease })
	c.waitReported(3)
	return c
}

// TestSealedTables (issue #310, step 3): a sealed table end to end on a
// secure 3-node cluster — refused before the cluster key, then made with
// its NOTICE; every way of writing and reading rows; ciphertext only
// below the boundary; statistics without the sealed columns; a clean
// consistency check; a destroyed key failing reads on every node within
// the lease; DROP TABLE destroying the key; and the janitor destroying a
// key no table names.
func TestSealedTables(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Second)
	defer cancel()
	c := startSealedCluster(t, ctx)
	var notices noticeLog
	conn := sealedConn(t, ctx, c, 0, &notices)

	const ddl = `CREATE TABLE notes (
		id INT8 PRIMARY KEY,
		email_hmac BYTES,
		n INT8,
		body TEXT CHECK (length(body) < 100),
		doc JSONB,
		v VECTOR(3),
		UNIQUE (email_hmac)
	) WITH (encryption_key = 'auto')`
	wantSQLErr(t, ctx, conn, ddl, "0A000", "cluster key")

	if errText, _ := c.op(0, "init"); errText != "" {
		t.Fatalf("init: %s", errText)
	}
	ks := c.keyStore(0)

	var notesKey int64
	t.Run("create", func(t *testing.T) {
		notices.take()
		sealExec(t, ctx, conn, ddl)
		got := notices.take()
		if len(got) != 1 || !strings.Contains(got[0], `table "notes" is sealed`) ||
			!strings.Contains(got[0], "id (primary key)") || !strings.Contains(got[0], "email_hmac (index") ||
			strings.Contains(got[0], "body") || strings.Contains(got[0], "doc") {
			t.Fatalf("CREATE TABLE notices: %q", got)
		}
		d := tableDesc(t, ctx, c, "notes")
		if !d.Sealed() || d.EncryptionKey.SealID != d.ID {
			t.Fatalf("descriptor: %+v", d.EncryptionKey)
		}
		notesKey = d.EncryptionKey.KeyID
		k, err := ks.GetByID(ctx, notesKey)
		if err != nil || k.Kind != keystore.KindTable || k.State != keystore.StateEnabled || k.Name != fmt.Sprintf("table/%d", d.ID) {
			t.Fatalf("the table's key: %+v %v", k, err)
		}

		wantSQLErr(t, ctx, conn, `CREATE TABLE ts (s INT8, at TIMESTAMPTZ, v FLOAT8, PRIMARY KEY (s, at)) WITH (timeseries = true, encryption_key = 'auto')`,
			"0A000", "time-series tables cannot be sealed")
		wantSQLErr(t, ctx, conn, `CREATE TABLE bad (id INT8 PRIMARY KEY) WITH (encryption_key = 'mine')`, "22023", "'auto'")
		wantSQLErr(t, ctx, conn, `ALTER TABLE notes SET (encryption_key = 'auto')`, "55000", "sealed already")
		sealExec(t, ctx, conn, `CREATE TABLE plain (id INT8 PRIMARY KEY, body TEXT)`)
		wantSQLErr(t, ctx, conn, `ALTER TABLE notes RESET (encryption_key)`, "0A000", "unsealing")
	})

	t.Run("crud", func(t *testing.T) {
		for i := 1; i <= 5; i++ {
			sealExec(t, ctx, conn, fmt.Sprintf(`INSERT INTO notes VALUES (%d, '\x0%d', %d, 'SEKRIT-%d', '{"k": %d}', '[%d, 0, 0]')`, i, i, i, i, i%2, i))
		}
		sameStrings(t, "rows", queryStrings(t, ctx, conn, `SELECT id, n, body FROM notes ORDER BY id`),
			"1|1|SEKRIT-1", "2|2|SEKRIT-2", "3|3|SEKRIT-3", "4|4|SEKRIT-4", "5|5|SEKRIT-5")
		sealExec(t, ctx, conn, `UPDATE notes SET body = 'SEKRIT-2b' WHERE id = 2`)
		sealExec(t, ctx, conn, `DELETE FROM notes WHERE id = 5`)
		// Upserts by the primary key and by the unique index.
		sealExec(t, ctx, conn, `INSERT INTO notes (id, body) VALUES (3, 'x') ON CONFLICT (id) DO UPDATE SET body = notes.body || '+'`)
		sealExec(t, ctx, conn, `INSERT INTO notes (id, email_hmac, body) VALUES (99, '\x04', 'x') ON CONFLICT (email_hmac) DO UPDATE SET body = 'SEKRIT-4u'`)
		sealExec(t, ctx, conn, `UPSERT INTO notes (id, email_hmac, n, body) VALUES (6, '\x06', 6, 'SEKRIT-6')`)
		// COPY.
		if _, err := conn.CopyFrom(ctx, pgx.Identifier{"notes"}, []string{"id", "email_hmac", "n", "body"},
			pgx.CopyFromRows([][]any{{int64(7), []byte{7}, int64(7), "SEKRIT-7"}, {int64(8), []byte{8}, int64(8), "SEKRIT-8"}})); err != nil {
			t.Fatalf("COPY: %v", err)
		}
		// A CHECK on a sealed column holds on plaintext.
		wantSQLErr(t, ctx, conn, `INSERT INTO notes (id, email_hmac, body) VALUES (50, '\x50', '`+strings.Repeat("y", 120)+`')`, "23514")
		want := []string{"1|SEKRIT-1", "2|SEKRIT-2b", "3|SEKRIT-3+", "4|SEKRIT-4u", "6|SEKRIT-6", "7|SEKRIT-7", "8|SEKRIT-8"}
		sameStrings(t, "after writes", queryStrings(t, ctx, conn, `SELECT id, body FROM notes ORDER BY id`), want...)
		// Filters and sorts on a sealed column, a lookup by the unique
		// index, EXPLAIN, and the same rows through the other nodes.
		sameStrings(t, "filter and sort on a sealed column",
			queryStrings(t, ctx, conn, `SELECT id FROM notes WHERE body LIKE 'SEKRIT-%' AND body > 'SEKRIT-3' ORDER BY body DESC LIMIT 3`), "8", "7", "6")
		sameStrings(t, "by the unique index", queryStrings(t, ctx, conn, `SELECT body FROM notes WHERE email_hmac = '\x02'`), "SEKRIT-2b")
		if got := queryStrings(t, ctx, conn, `EXPLAIN SELECT body FROM notes WHERE body = 'SEKRIT-1'`); len(got) == 0 {
			t.Fatal("EXPLAIN printed nothing")
		}
		for i := 1; i < 3; i++ {
			sameStrings(t, fmt.Sprintf("rows through n%d", i+1),
				queryStrings(t, ctx, sealedConn(t, ctx, c, i, nil), `SELECT id, body FROM notes ORDER BY id`), want...)
		}

		// CREATE INDEX backfills from sealed rows and says the column is
		// now plaintext; the inverted and vector indexes too.
		notices.take()
		sealExec(t, ctx, conn, `CREATE INDEX notes_n ON notes (n)`)
		if got := notices.take(); len(got) != 1 || !strings.Contains(got[0], "the values of n are stored in plaintext in index notes_n") {
			t.Fatalf("CREATE INDEX notices: %q", got)
		}
		sameStrings(t, "through the new index", queryStrings(t, ctx, conn, `SELECT body FROM notes WHERE n = 7`), "SEKRIT-7")
		sealExec(t, ctx, conn, `CREATE INDEX notes_doc ON notes USING gin (doc)`)
		if got := notices.take(); len(got) != 1 || !strings.Contains(got[0], "doc") {
			t.Fatalf("GIN notices: %q", got)
		}
		sameStrings(t, "through the inverted index", queryStrings(t, ctx, conn, `SELECT id FROM notes WHERE doc @> '{"k": 0}' ORDER BY id`), "2", "4")
		sealExec(t, ctx, conn, `CREATE INDEX notes_v ON notes USING ivfflat (v vector_l2_ops) WITH (lists = 2)`)
		if got := notices.take(); len(got) != 1 || !strings.Contains(got[0], "vector") {
			t.Fatalf("ivfflat notices: %q", got)
		}
		sameStrings(t, "through the vector index", queryStrings(t, ctx, conn, `SELECT id FROM notes ORDER BY v <-> '[2, 0, 0]' LIMIT 1`), "2")
		sealExec(t, ctx, conn, `INSERT INTO notes VALUES (9, '\x09', 9, 'SEKRIT-9', '{"k": 0}', '[9, 0, 0]')`)
		sameStrings(t, "a write through every index", queryStrings(t, ctx, conn, `SELECT body FROM notes WHERE n = 9 AND doc @> '{"k": 0}'`), "SEKRIT-9")
	})

	t.Run("ciphertext below the boundary", func(t *testing.T) {
		d := tableDesc(t, ctx, c, "notes")
		primary, other := tableKVs(t, ctx, c, d.ID)
		if len(primary) != 8 {
			t.Fatalf("%d primary rows", len(primary))
		}
		for _, kv := range primary {
			if kv[1][0] != 0xD5 {
				t.Fatalf("a primary value is not sealed: % x", kv[1])
			}
		}
		for _, kv := range append(primary, other...) {
			if bytes.Contains(kv[0], []byte("SEKRIT")) || bytes.Contains(kv[1], []byte("SEKRIT")) {
				t.Fatalf("plaintext of a sealed column in KV: %q → %q", kv[0], kv[1])
			}
		}
		// The plain table, for contrast, holds its text as it is.
		sealExec(t, ctx, conn, `INSERT INTO plain VALUES (1, 'SEKRIT-plain')`)
		pp, _ := tableKVs(t, ctx, c, tableDesc(t, ctx, c, "plain").ID)
		if len(pp) != 1 || !bytes.Contains(pp[0][1], []byte("SEKRIT-plain")) {
			t.Fatal("the scan does not see plain values: the check above proves nothing")
		}
		c.checkConsistent("after sealed writes")
	})

	t.Run("statistics", func(t *testing.T) {
		sealExec(t, ctx, conn, `ANALYZE notes`)
		d := tableDesc(t, ctx, c, "notes")
		raw, err := c.tc.Nodes[0].DB().Get(ctx, keys.TableStatsKey(d.ID))
		if err != nil || raw == nil {
			t.Fatalf("no statistics: %v", err)
		}
		var st catalog.TableStatistics
		if err := json.Unmarshal(raw, &st); err != nil {
			t.Fatal(err)
		}
		got := map[string]bool{}
		for _, cs := range st.Columns {
			got[cs.Name] = true
		}
		if st.RowCount != 8 || !got["id"] || !got["n"] || !got["email_hmac"] || got["body"] {
			t.Fatalf("statistics of a sealed table: rows %d, columns %v", st.RowCount, got)
		}
	})

	t.Run("destroyed key", func(t *testing.T) {
		sealExec(t, ctx, conn, `CREATE TABLE shred (id INT8 PRIMARY KEY, body TEXT) WITH (encryption_key = 'auto')`)
		sealExec(t, ctx, conn, `INSERT INTO shred VALUES (1, 'gone soon'), (2, 'also')`)
		conns := []*pgx.Conn{conn, sealedConn(t, ctx, c, 1, nil), sealedConn(t, ctx, c, 2, nil)}
		for _, cn := range conns {
			sameStrings(t, "before", queryStrings(t, ctx, cn, `SELECT count(*) FROM shred`), "2")
		}
		d := tableDesc(t, ctx, c, "shred")
		if _, _, err := ks.Destroy(ctx, d.EncryptionKey.KeyID); err != nil {
			t.Fatal(err)
		}
		time.Sleep(sealedLease + 200*time.Millisecond)
		for _, cn := range conns {
			for _, q := range []string{`SELECT body FROM shred WHERE id = 1`, `SELECT count(*) FROM shred WHERE body IS NOT NULL`} {
				wantSQLErr(t, ctx, cn, q, "XXK01", `table "shred"`, "destroyed")
			}
			wantSQLErr(t, ctx, cn, `INSERT INTO shred VALUES (3, 'x')`, "XXK01", "destroyed")
		}
		sealExec(t, ctx, conn, `DROP TABLE shred`)
	})

	t.Run("schema changes", func(t *testing.T) {
		sealExec(t, ctx, conn, `CREATE TABLE evolve (id INT8 PRIMARY KEY, a INT8, b TEXT) WITH (encryption_key = 'auto')`)
		for i := 1; i <= 4; i++ {
			sealExec(t, ctx, conn, fmt.Sprintf(`INSERT INTO evolve VALUES (%d, %d, 'SEKRIT-e%d')`, i, i*10, i))
		}
		sealExec(t, ctx, conn, `ALTER TABLE evolve ALTER COLUMN a TYPE TEXT`)
		sealExec(t, ctx, conn, `ALTER TABLE evolve ADD COLUMN c INT8 DEFAULT 5`)
		sealExec(t, ctx, conn, `ALTER TABLE evolve ADD CONSTRAINT short_b CHECK (length(b) < 20)`)
		sealExec(t, ctx, conn, `ALTER TABLE evolve ALTER COLUMN b SET NOT NULL`)
		sameStrings(t, "after the schema changes", queryStrings(t, ctx, conn, `SELECT id, a, b, c FROM evolve ORDER BY id`),
			"1|10|SEKRIT-e1|5", "2|20|SEKRIT-e2|5", "3|30|SEKRIT-e3|5", "4|40|SEKRIT-e4|5")
		d := tableDesc(t, ctx, c, "evolve")
		primary, _ := tableKVs(t, ctx, c, d.ID)
		for _, kv := range primary {
			if kv[1][0] != 0xD5 || bytes.Contains(kv[1], []byte("SEKRIT")) {
				t.Fatalf("a rewritten value is not sealed: % x", kv[1])
			}
		}
		sealExec(t, ctx, conn, `TRUNCATE evolve`)
		sealExec(t, ctx, conn, `INSERT INTO evolve (id, a, b) VALUES (9, 'x', 'SEKRIT-after')`)
		sameStrings(t, "after TRUNCATE", queryStrings(t, ctx, conn, `SELECT id, b, c FROM evolve`), "9|SEKRIT-after|5")
	})

	t.Run("drop destroys the key", func(t *testing.T) {
		before := time.Now().UnixNano()
		time.Sleep(50 * time.Millisecond)
		sealExec(t, ctx, conn, `DROP TABLE notes`)
		k, err := ks.GetByID(ctx, notesKey)
		if err != nil || k.State != keystore.StateDestroyed {
			t.Fatalf("the dropped table's key: %+v %v", k, err)
		}
		// A read of the table as it was before the drop finds no key.
		wantSQLErr(t, ctx, conn, fmt.Sprintf(`SELECT body FROM notes AS OF SYSTEM TIME '%d'`, before), "XXK01", `table "notes"`, "destroyed")
	})

	t.Run("janitor", func(t *testing.T) {
		sealExec(t, ctx, conn, `CREATE TABLE kept (id INT8 PRIMARY KEY, body TEXT) WITH (encryption_key = 'auto')`)
		orphan, err := ks.Create(ctx, keystore.KindTable, "table/orphan")
		if err != nil {
			t.Fatal(err)
		}
		wrapping, err := ks.Create(ctx, keystore.KindWrapping, "app")
		if err != nil {
			t.Fatal(err)
		}
		n, err := c.tc.Nodes[1].SweepOrphanTableKeys(ctx)
		if err != nil || n != 1 {
			t.Fatalf("sweep: %d %v", n, err)
		}
		for id, want := range map[int64]keystore.State{orphan.ID: keystore.StateDestroyed, wrapping.ID: keystore.StateEnabled,
			tableDesc(t, ctx, c, "kept").EncryptionKey.KeyID: keystore.StateEnabled} {
			if k, err := ks.GetByID(ctx, id); err != nil || k.State != want {
				t.Fatalf("key %d after the sweep: %+v %v, want %s", id, k, err, want)
			}
		}
	})
}

// TestSealedTablesNeedV22: before v22 is finalized the options are
// refused, per-tenant keys' too (step 4).
func TestSealedTablesNeedV22(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	c := startCKCluster(t, ctx, func(i int, cfg *server.Config) { cfg.BinaryVersionOverride = version.V21 })
	conn := sealedConn(t, ctx, c, 0, nil)
	wantSQLErr(t, ctx, conn, `CREATE TABLE notes (id INT8 PRIMARY KEY, body TEXT) WITH (encryption_key = 'auto')`, "0A000", "need cluster version v22")
	wantSQLErr(t, ctx, conn, `CREATE TABLE items (t UUID, id INT8, body TEXT, PRIMARY KEY (t, id)) WITH (encryption_key_by = 't')`, "0A000", "need cluster version v22")
	// Sealing an existing table, rotating, and the functions of step 5.
	sealExec(t, ctx, conn, `CREATE TABLE plain (t INT8, id INT8, body TEXT, PRIMARY KEY (t, id))`)
	wantSQLErr(t, ctx, conn, `ALTER TABLE plain SET (encryption_key = 'auto')`, "0A000", "need cluster version v22")
	wantSQLErr(t, ctx, conn, `ALTER TABLE plain SET (encryption_key_by = 't')`, "0A000", "need cluster version v22")
	wantSQLErr(t, ctx, conn, `ALTER TABLE plain ROTATE ENCRYPTION KEY`, "0A000", "needs cluster version v22")
	for _, fn := range []string{`datax_rotate_tenant_key('plain', 1)`, `datax_destroy_old_key_versions('plain')`, `datax_remove_tenant_rows('plain', 1)`} {
		wantSQLErr(t, ctx, conn, `SELECT `+fn, "0A000", "needs cluster version v22")
	}
}

// exportClusterKey exports src's cluster key the way the CLI does,
// wrapped under a fresh operator root, and returns what an import on
// another cluster needs.
func exportClusterKey(t *testing.T, ctx context.Context, src *ckCluster) []enc.TransportKey {
	t.Helper()
	x, _ := enc.NewPeerExchange()
	resp := src.call(0, false, cluster.AdminRequest{Op: "cluster-key-export", ClusterKey: &cluster.ClusterKeyRequest{Exchange: x.Public()}})
	if resp.Error != "" {
		t.Fatalf("export: %s", resp.Error)
	}
	srcID := resp.ClusterKey.Status.ClusterID
	exported, err := x.OpenClusterKeys(resp.ClusterKey.Exchange, resp.ClusterKey.Sealed, []byte("datax cluster key export|"+srcID))
	if err != nil {
		t.Fatal(err)
	}
	root, err := enc.ParseRoot(writeRootKey(t, t.TempDir(), "export.key"))
	if err != nil {
		t.Fatal(err)
	}
	file, err := enc.ExportClusterKeys(ctx, root, srcID, enc.ClusterKeyRef{ID: resp.ClusterKey.Status.ID, Version: resp.ClusterKey.Status.Version}, exported)
	if err != nil {
		t.Fatal(err)
	}
	enc.ZeroTransportKeys(exported)
	opened, err := file.Open(ctx, []enc.Root{root})
	if err != nil {
		t.Fatal(err)
	}
	return opened
}

// TestSealedTableBackupRestore (issue #310, step 3): a backup copies a
// sealed table's ciphertext; a table restore beside the live table
// shares its key (and DROP TABLE of either keeps it while the other
// names it); a table restore after the key was destroyed brings the key
// back from the backup; a table restore into another cluster, and a
// restore of everything, bring the table back readable under the target's
// cluster key.
func TestSealedTableBackupRestore(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 420*time.Second)
	defer cancel()
	src := startSealedCluster(t, ctx)
	if errText, _ := src.op(0, "init"); errText != "" {
		t.Fatalf("init: %s", errText)
	}
	conn := sealedConn(t, ctx, src, 0, nil)
	sealExec(t, ctx, conn, `CREATE TABLE notes (id INT8 PRIMARY KEY, n INT8, body TEXT) WITH (encryption_key = 'auto')`)
	sealExec(t, ctx, conn, `CREATE INDEX notes_n ON notes (n)`)
	for i := 1; i <= 20; i++ {
		sealExec(t, ctx, conn, fmt.Sprintf(`INSERT INTO notes VALUES (%d, %d, 'SEKRIT-%d')`, i, i%3, i))
	}
	sealExec(t, ctx, conn, `CREATE TABLE plainref (id INT8 PRIMARY KEY, body TEXT)`)
	sealExec(t, ctx, conn, `INSERT INTO plainref VALUES (1, 'VISIBLE-REF')`)
	want := queryStrings(t, ctx, conn, `SELECT id, n, body FROM notes ORDER BY id`)
	d := tableDesc(t, ctx, src, "notes")
	keyID := d.EncryptionKey.KeyID
	ks := src.keyStore(0)

	dir := filepath.Join(t.TempDir(), "bk")
	if _, err := src.tc.Nodes[0].RunBackupWith(ctx, server.BackupOptions{Dest: dir, AllowPlaintext: true}); err != nil {
		t.Fatalf("backup: %v", err)
	}
	// The backup holds ciphertext: no data file has the sealed text,
	// while the plain table's text is there to be found.
	files, _ := filepath.Glob(filepath.Join(dir, "*"))
	visible := false
	for _, f := range files {
		raw, err := os.ReadFile(f)
		if err != nil {
			continue
		}
		visible = visible || bytes.Contains(raw, []byte("VISIBLE-REF"))
		if bytes.Contains(raw, []byte("SEKRIT")) {
			t.Fatalf("backup file %s holds plaintext of a sealed column", filepath.Base(f))
		}
	}
	if !visible {
		t.Fatal("the plain table's text is in no backup file: the check above proves nothing")
	}

	restoreAs := func(c *ckCluster, into string) *cluster.BackupSummary {
		t.Helper()
		sum, err := c.tc.Nodes[1].RunRestoreOpts(ctx, server.RestoreOptions{
			Srcs: []string{dir}, Tables: []server.RestoreTable{{Name: "notes", Into: into}},
		})
		if err != nil {
			t.Fatalf("table restore as %s: %v", into, err)
		}
		return sum
	}

	t.Run("beside the live table", func(t *testing.T) {
		restoreAs(src, "notes_copy")
		rd := tableDesc(t, ctx, src, "notes_copy")
		if rd.EncryptionKey == nil || rd.EncryptionKey.KeyID != keyID || rd.EncryptionKey.SealID != d.ID || rd.ID == d.ID {
			t.Fatalf("restored descriptor: id %d, %+v", rd.ID, rd.EncryptionKey)
		}
		sameStrings(t, "restored rows", queryStrings(t, ctx, sealedConn(t, ctx, src, 2, nil), `SELECT id, n, body FROM notes_copy ORDER BY id`), want...)
		sameStrings(t, "through the restored index", queryStrings(t, ctx, conn, `SELECT count(*) FROM notes_copy WHERE n = 1`), "7")
		sealExec(t, ctx, conn, `INSERT INTO notes_copy VALUES (100, 1, 'new')`)
		sealExec(t, ctx, conn, `DROP TABLE notes`)
		if k, _ := ks.GetByID(ctx, keyID); k.State != keystore.StateEnabled {
			t.Fatalf("dropping one of two tables sharing a key destroyed it: %+v", k)
		}
		sameStrings(t, "the copy after the original is dropped", queryStrings(t, ctx, conn, `SELECT body FROM notes_copy WHERE id = 100`), "new")
		sealExec(t, ctx, conn, `DROP TABLE notes_copy`)
		if k, _ := ks.GetByID(ctx, keyID); k.State != keystore.StateDestroyed {
			t.Fatalf("dropping the last table naming the key: %+v", k)
		}
		src.checkConsistent("after restore and drop")
	})

	t.Run("after the key was destroyed", func(t *testing.T) {
		sum := restoreAs(src, "notes_back")
		found := false
		for _, n := range sum.Notes {
			found = found || strings.Contains(n, "brought back from the backup")
		}
		if !found {
			t.Fatalf("restore notes: %v", sum.Notes)
		}
		time.Sleep(sealedLease + 200*time.Millisecond)
		for i := 0; i < 3; i++ {
			sameStrings(t, fmt.Sprintf("through n%d", i+1), queryStrings(t, ctx, sealedConn(t, ctx, src, i, nil), `SELECT id, n, body FROM notes_back ORDER BY id`), want...)
		}
		if k, _ := ks.GetByID(ctx, keyID); k.State != keystore.StateEnabled {
			t.Fatalf("the key brought back: %+v", k)
		}
	})

	keysOut := exportClusterKey(t, ctx, src)
	src.tc.StopAll()

	t.Run("into another cluster", func(t *testing.T) {
		dst := startSealedCluster(t, ctx)
		if errText, _ := dst.op(0, "init"); errText != "" {
			t.Fatalf("init: %s", errText)
		}
		if _, err := dst.tc.Nodes[0].RunRestoreOpts(ctx, server.RestoreOptions{
			Srcs: []string{dir}, Tables: []server.RestoreTable{{Name: "notes"}},
		}); err == nil || !strings.Contains(err.Error(), "does not hold") {
			t.Fatalf("a table restore before the import: %v", err)
		}
		if resp := dst.call(1, false, cluster.AdminRequest{Op: "cluster-key-import", ClusterKey: &cluster.ClusterKeyRequest{Keys: keysOut}}); resp.Error != "" {
			t.Fatalf("import: %s", resp.Error)
		}
		restoreAs(dst, "notes")
		sameStrings(t, "restored into another cluster", queryStrings(t, ctx, sealedConn(t, ctx, dst, 0, nil), `SELECT id, n, body FROM notes ORDER BY id`), want...)
		k, err := dst.keyStore(2).GetByID(ctx, keyID)
		if err != nil || k.ClusterKey.ID != dst.status(0).ID {
			t.Fatalf("the key here: %+v %v", k, err)
		}
		dst.tc.StopAll()
	})

	t.Run("everything", func(t *testing.T) {
		dst := startSealedCluster(t, ctx)
		if errText, _ := dst.op(0, "init"); errText != "" {
			t.Fatalf("init: %s", errText)
		}
		if resp := dst.call(1, false, cluster.AdminRequest{Op: "cluster-key-import", ClusterKey: &cluster.ClusterKeyRequest{Keys: keysOut}}); resp.Error != "" {
			t.Fatalf("import: %s", resp.Error)
		}
		if _, err := dst.tc.Nodes[0].RunRestore(ctx, []string{dir}); err != nil {
			t.Fatalf("restore: %v", err)
		}
		sameStrings(t, "restored whole", queryStrings(t, ctx, sealedConn(t, ctx, dst, 1, nil), `SELECT id, n, body FROM notes ORDER BY id`), want...)
		dst.checkConsistent("after a whole restore")
	})
}
