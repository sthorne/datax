package testcluster

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sthorne/datax/pkg/cluster"
	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/server"
	"github.com/sthorne/datax/pkg/sql"
	"github.com/sthorne/datax/pkg/sql/catalog"
	"github.com/sthorne/datax/pkg/version"
)

// rowsText renders a query's rows as one line each, for comparing two
// tables row for row.
func rowsText(t *testing.T, ctx context.Context, s *sql.Session, query string) []string {
	t.Helper()
	res := execSQL(t, ctx, s, query)
	out := make([]string, len(res.Rows))
	for i, row := range res.Rows {
		cells := make([]string, len(row))
		for j, d := range row {
			if d.Null {
				cells[j] = "NULL"
			} else {
				cells[j] = d.Text()
			}
		}
		out[i] = strings.Join(cells, "|")
	}
	return out
}

func requireSameRows(t *testing.T, what string, got, want []string) {
	t.Helper()
	if strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Fatalf("%s:\n got %q\nwant %q", what, got, want)
	}
}

func tableSummary(t *testing.T, sum *cluster.BackupSummary, name string) cluster.BackupTableSummary {
	t.Helper()
	for _, ts := range sum.Tables {
		if ts.Name == name {
			return ts
		}
	}
	t.Fatalf("no table %q in the summary: %+v", name, sum.Tables)
	return cluster.BackupTableSummary{}
}

// restoreRecords is the table restores a cluster has a progress record
// for, and the table ids each is loading under.
func restoreRecords(t *testing.T, ctx context.Context, n *server.Node) [][]uint64 {
	t.Helper()
	start, end := keys.RestoreProgressSpan()
	kvs, err := n.DB().Scan(ctx, start, end, 0)
	if err != nil {
		t.Fatal(err)
	}
	var out [][]uint64
	for _, kv := range kvs {
		var rec struct {
			TableIDs []uint64 `json:"table_ids"`
		}
		if err := json.Unmarshal(kv.Value, &rec); err != nil {
			t.Fatal(err)
		}
		out = append(out, rec.TableIDs)
	}
	return out
}

func descIDGen(t *testing.T, ctx context.Context, n *server.Node) string {
	t.Helper()
	raw, err := n.DB().Get(ctx, keys.DescIDGenKey())
	if err != nil {
		t.Fatal(err)
	}
	return string(raw)
}

func tableRowKeys(t *testing.T, ctx context.Context, n *server.Node, id uint64) int {
	t.Helper()
	start, end := keys.TableDataSpan(id)
	kvs, err := n.DB().Scan(ctx, start, end, 0)
	if err != nil {
		t.Fatal(err)
	}
	return len(kvs)
}

// TestTableRestoreIntoALiveCluster (issue #301): one table of several,
// from a full + incremental chain, restored under a new name into the
// 3-node cluster it came from while writers run on another table. The
// restored table equals the source as of the backup, row for row and by
// checksum; its secondary index is there and agrees with its rows; its
// SERIAL column carries on past the restored values; no other table
// changes; and the refusals apply nothing.
func TestTableRestoreIntoALiveCluster(t *testing.T) {
	tc := Start(t, 3)
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Second)
	defer cancel()
	s := sql.NewSession(tc.Nodes[0].DB(), catalog.NewAccessor())
	waitForDatabases(t, ctx, s)

	execSQL(t, ctx, s, `CREATE TABLE accounts (id INT8 PRIMARY KEY, balance INT8 NOT NULL)`)
	execSQL(t, ctx, s, `CREATE TABLE items (id SERIAL PRIMARY KEY, name TEXT, price DECIMAL, attrs JSONB)`)
	execSQL(t, ctx, s, `CREATE INDEX by_name ON items (name)`)
	execSQL(t, ctx, s, `CREATE UNIQUE INDEX by_price ON items (price)`)
	execSQL(t, ctx, s, `CREATE TABLE customers (id INT8 PRIMARY KEY, name TEXT)`)
	execSQL(t, ctx, s, `CREATE TABLE orders (id INT8 PRIMARY KEY, customer INT8 REFERENCES customers (id), what TEXT)`)
	execSQL(t, ctx, s, `CREATE TABLE metrics (series TEXT, at TIMESTAMPTZ, v FLOAT8,
		PRIMARY KEY (series, at)) WITH (timeseries = true, shards = 4)`)
	for i := 0; i < 40; i++ {
		execSQL(t, ctx, s, `INSERT INTO metrics VALUES ('s`+itoa(i%5)+`', '2026-08-30 10:00:`+itoa(10+i)+`Z', `+itoa(i)+`.5)`)
	}
	execSQL(t, ctx, s, `CREATE USER analyst PASSWORD 'trustno1'`)
	execSQL(t, ctx, s, `GRANT SELECT ON items TO analyst`)
	const nAccounts = 12
	for i := 0; i < nAccounts; i++ {
		execSQL(t, ctx, s, `INSERT INTO accounts VALUES (`+itoa(i)+`, 1000)`)
	}
	for i := 0; i < 300; i++ {
		execSQL(t, ctx, s, `INSERT INTO items (name, price, attrs) VALUES ('item-`+itoa(i%40)+`', `+itoa(i)+`.5, '{"n": `+itoa(i)+`}')`)
	}
	execSQL(t, ctx, s, `INSERT INTO customers VALUES (1, 'ann'), (2, 'bob')`)
	execSQL(t, ctx, s, `INSERT INTO orders VALUES (10, 1, 'anvil'), (11, 2, 'rope')`)

	dirFull := t.TempDir()
	if _, err := tc.Nodes[0].RunBackup(ctx, dirFull, "", false, false); err != nil {
		t.Fatalf("full backup: %v", err)
	}
	// The incremental carries updates, deletions (tombstones that must
	// not come back as rows) and inserts.
	execSQL(t, ctx, s, `UPDATE items SET name = 'renamed' WHERE id <= 20`)
	execSQL(t, ctx, s, `DELETE FROM items WHERE id > 280`)
	execSQL(t, ctx, s, `INSERT INTO items (name, price) VALUES ('late', 9000.25), (NULL, 9001.25)`)
	dirInc := t.TempDir()
	if _, err := tc.Nodes[0].RunBackup(ctx, dirInc, dirFull, false, false); err != nil {
		t.Fatalf("incremental backup: %v", err)
	}
	// The source as of the backup: its rows, and a full backup's checksums
	// taken while items, customers and orders are quiet.
	const itemsQuery = `SELECT id, name, price, attrs FROM %s ORDER BY id`
	wantRows := rowsText(t, ctx, s, strings.Replace(itemsQuery, "%s", "items", 1))
	dirRef := t.TempDir()
	sumRef, err := tc.Nodes[0].RunBackup(ctx, dirRef, "", false, false)
	if err != nil {
		t.Fatal(err)
	}
	srcID := tableSummary(t, sumRef, "items").ID

	// The mistake the restore is for, made after the backup.
	execSQL(t, ctx, s, `UPDATE items SET price = price + 100000`)
	execSQL(t, ctx, s, `DELETE FROM items WHERE id < 50`)
	mangledRows := rowsText(t, ctx, s, strings.Replace(itemsQuery, "%s", "items", 1))

	// Writers on another table for as long as the restores run.
	var stop atomic.Bool
	var transfers atomic.Int64
	var wg sync.WaitGroup
	for w := 0; w < 3; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			ws := sql.NewSession(tc.Nodes[w%3].DB(), catalog.NewAccessor())
			for i := 0; !stop.Load(); i++ {
				from, to := (w*3+i)%nAccounts, (w*5+i+1)%nAccounts
				if from == to {
					continue
				}
				if _, serr := trySQL(ctx, ws, `BEGIN;
					UPDATE accounts SET balance = balance - 7 WHERE id = `+itoa(from)+`;
					UPDATE accounts SET balance = balance + 7 WHERE id = `+itoa(to)+`;
					COMMIT`); serr != nil {
					_, _ = trySQL(ctx, ws, `ROLLBACK`)
				} else {
					transfers.Add(1)
				}
			}
		}(w)
	}
	defer func() { stop.Store(true); wg.Wait() }()

	// Restore on a node other than the one that took the backup.
	chain := []string{dirFull, dirInc}
	before := transfers.Load()
	rsum, err := tc.Nodes[1].RunRestoreOpts(ctx, server.RestoreOptions{
		Srcs: chain, Tables: []server.RestoreTable{{Name: "items", Into: "items_restored"}},
	})
	if err != nil {
		t.Fatalf("table restore: %v", err)
	}
	if len(rsum.Tables) != 1 {
		t.Fatalf("restore summary: %+v", rsum)
	}
	rt := rsum.Tables[0]
	if rt.Name != "datax.items_restored" || rt.From != "datax.items" || rt.FromID != srcID || rt.ID == srcID {
		t.Fatalf("restored table: %+v (source id %d)", rt, srcID)
	}
	// The checksum is the source table's as of the backup, in the
	// source's own key space: what a full backup taken then reports.
	// (A backup's record count includes the tombstones of deleted rows,
	// which a restored table does not have; the live bytes compare.)
	if ref := tableSummary(t, sumRef, "items"); rt.SHA256 != ref.SHA256 || rt.Bytes != ref.Bytes {
		t.Fatalf("restored checksum %s (%d live bytes), the source's was %s (%d)", rt.SHA256, rt.Bytes, ref.SHA256, ref.Bytes)
	}

	// Row for row, from a session on a third node.
	s3 := sql.NewSession(tc.Nodes[2].DB(), catalog.NewAccessor())
	requireSameRows(t, "restored rows", rowsText(t, ctx, s3, strings.Replace(itemsQuery, "%s", "items_restored", 1)), wantRows)
	// The live table is as the mistake left it.
	requireSameRows(t, "the live table", rowsText(t, ctx, s3, strings.Replace(itemsQuery, "%s", "items", 1)), mangledRows)

	// Secondary indexes: usable, and one entry per row that has the
	// indexed column. by_name is index 2, by_price (unique) index 3.
	requireSameRows(t, "lookup through by_name",
		rowsText(t, ctx, s3, `SELECT id FROM items_restored WHERE name = 'late'`), []string{"301"})
	requireSameRows(t, "lookup through by_price",
		rowsText(t, ctx, s3, `SELECT name FROM items_restored WHERE price = 9000.25`), []string{"late"})
	named := execSQL(t, ctx, s3, `SELECT COUNT(*) FROM items_restored WHERE name IS NOT NULL`).Rows[0][0].I
	total := execSQL(t, ctx, s3, `SELECT COUNT(*) FROM items_restored`).Rows[0][0].I
	for idx, want := range map[uint64]int64{2: named, 3: total} {
		lo, hi := keys.TableIndexSpan(rt.ID, idx)
		kvs, err := tc.Nodes[0].DB().Scan(ctx, lo, hi, 0)
		if err != nil {
			t.Fatal(err)
		}
		if int64(len(kvs)) != want {
			t.Fatalf("index %d of the restored table has %d entries, want %d", idx, len(kvs), want)
		}
	}
	// A unique index still refuses a duplicate, and writes maintain the
	// indexes.
	if _, serr := trySQL(ctx, s3, `INSERT INTO items_restored (name, price) VALUES ('dup', 9000.25)`); serr == nil {
		t.Fatal("the restored unique index accepted a duplicate")
	}
	// The SERIAL column draws from a sequence of the restored table's
	// own, past every restored value, and the source's is untouched.
	execSQL(t, ctx, s3, `INSERT INTO items_restored (name, price) VALUES ('after', 1.125)`)
	newID := execSQL(t, ctx, s3, `SELECT id FROM items_restored WHERE name = 'after'`).Rows[0][0].I
	if maxID := execSQL(t, ctx, s3, `SELECT MAX(id) FROM items_restored WHERE name <> 'after'`).Rows[0][0].I; newID <= maxID {
		t.Fatalf("the restored table's sequence handed out %d, at or below a restored id (%d)", newID, maxID)
	}
	// Grants came with it, for the role this cluster has.
	rawDesc, err := tc.Nodes[0].DB().Get(ctx, keys.TableDescKey(rt.ID))
	if err != nil {
		t.Fatal(err)
	}
	var restored catalog.TableDescriptor
	if err := json.Unmarshal(rawDesc, &restored); err != nil {
		t.Fatal(err)
	}
	if got := restored.Privileges["analyst"]; len(got) != 1 || got[0] != "SELECT" || restored.Version != 1 {
		t.Fatalf("the restored descriptor: grants to analyst %q, version %d", got, restored.Version)
	}
	if pl := explainPlan(t, ctx, s3, `SELECT id FROM items_restored WHERE name = 'late'`); !strings.Contains(pl, `index "by_name"`) {
		t.Fatalf("the restored index is not used: %q", pl)
	}

	// Copying rows back is plain SQL from here.
	execSQL(t, ctx, s3, `DELETE FROM items_restored WHERE name = 'after'`)
	execSQL(t, ctx, s3, `DELETE FROM items`)
	execSQL(t, ctx, s3, `INSERT INTO items (id, name, price, attrs) SELECT id, name, price, attrs FROM items_restored`)
	requireSameRows(t, "the live table after copying back", rowsText(t, ctx, s3, strings.Replace(itemsQuery, "%s", "items", 1)), wantRows)

	// Refusals, each applying nothing: no table, no id taken, no record.
	idGen := descIDGen(t, ctx, tc.Nodes[0])
	tablesBefore := rowsText(t, ctx, s3, `SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' ORDER BY table_name`)
	for _, c := range []struct {
		what string
		opts server.RestoreOptions
		want string
	}{
		{"a name that exists", server.RestoreOptions{Tables: []server.RestoreTable{{Name: "items", Into: "items_restored"}}}, "datax.items_restored already exists"},
		{"the table's own name while it exists", server.RestoreOptions{Tables: []server.RestoreTable{{Name: "items"}}}, "datax.items already exists"},
		{"a table the backup lacks", server.RestoreOptions{Tables: []server.RestoreTable{{Name: "nope"}}}, "the backup has no table datax.nope"},
		{"a database that is not there", server.RestoreOptions{Tables: []server.RestoreTable{{Name: "items", Into: "elsewhere.items"}}}, `database "elsewhere" does not exist here`},
		{"a foreign key to a table left behind", server.RestoreOptions{Tables: []server.RestoreTable{{Name: "orders", Into: "orders_r"}}}, "to a table that is not being restored with it"},
		{"a second table that cannot be restored", server.RestoreOptions{Tables: []server.RestoreTable{{Name: "customers", Into: "customers_ok"}, {Name: "orders", Into: "items_restored"}}}, "already exists"},
	} {
		c.opts.Srcs = chain
		_, err := tc.Nodes[1].RunRestoreOpts(ctx, c.opts)
		if err == nil || !strings.Contains(err.Error(), c.want) {
			t.Fatalf("%s: got %v, want a refusal saying %q", c.what, err, c.want)
		}
	}
	if got := descIDGen(t, ctx, tc.Nodes[0]); got != idGen {
		t.Fatalf("a refused restore moved the id generator from %s to %s", idGen, got)
	}
	if recs := restoreRecords(t, ctx, tc.Nodes[0]); len(recs) != 0 {
		t.Fatalf("a refused restore left a progress record: %v", recs)
	}
	requireSameRows(t, "tables after the refusals",
		rowsText(t, ctx, s3, `SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' ORDER BY table_name`), tablesBefore)

	// A sharded timeseries table keeps its layout: the shard bucket is
	// part of each row's key after the table's prefix, and comes across
	// with it.
	if _, err := tc.Nodes[0].RunRestoreOpts(ctx, server.RestoreOptions{Srcs: chain, Tables: []server.RestoreTable{{Name: "metrics", Into: "metrics_r"}}}); err != nil {
		t.Fatalf("restoring the timeseries table: %v", err)
	}
	requireSameRows(t, "the restored timeseries table",
		rowsText(t, ctx, s3, `SELECT series, at, v FROM metrics_r ORDER BY series, at`),
		rowsText(t, ctx, s3, `SELECT series, at, v FROM metrics ORDER BY series, at`))
	if pl := explainPlan(t, ctx, s3, `SELECT v FROM metrics_r WHERE series = 's1' AND at >= '2026-08-30 10:00:00Z'`); !strings.Contains(pl, "fan-out over 4 shard buckets") {
		t.Fatalf("the restored timeseries table's plan: %q", pl)
	}
	execSQL(t, ctx, s3, `INSERT INTO metrics_r VALUES ('s1', '2026-08-30 11:00:00Z', 1.5)`)

	// Foreign keys. Two tables restored together keep the one between
	// them, now between the copies; a table restored without its parent
	// comes back without the constraint when told to.
	fsum, err := tc.Nodes[2].RunRestoreOpts(ctx, server.RestoreOptions{Srcs: chain, Tables: []server.RestoreTable{
		{Name: "orders", Into: "orders_r"}, {Name: "customers", Into: "customers_r"}}})
	if err != nil {
		t.Fatalf("restoring orders with customers: %v", err)
	}
	if len(fsum.Notes) != 0 {
		t.Fatalf("notes: %q", fsum.Notes)
	}
	if _, serr := trySQL(ctx, s3, `INSERT INTO orders_r VALUES (12, 99, 'ghost')`); serr == nil {
		t.Fatal("the restored foreign key let an order reference a customer that is not there")
	}
	execSQL(t, ctx, s3, `INSERT INTO customers_r VALUES (99, 'new')`)
	execSQL(t, ctx, s3, `INSERT INTO orders_r VALUES (12, 99, 'ghost')`)
	if _, serr := trySQL(ctx, s3, `DELETE FROM customers_r WHERE id = 99`); serr == nil {
		t.Fatal("the restored parent let a referenced row go")
	}
	// The originals are not tied to the copies: customer 99 exists only
	// in the copy, and the original orders still refuses it.
	if _, serr := trySQL(ctx, s3, `INSERT INTO orders VALUES (12, 99, 'ghost')`); serr == nil {
		t.Fatal("the original foreign key now checks the restored copy")
	}
	dsum, err := tc.Nodes[2].RunRestoreOpts(ctx, server.RestoreOptions{Srcs: chain, DropForeignKeys: true,
		Tables: []server.RestoreTable{{Name: "orders", Into: "orders_alone"}}})
	if err != nil {
		t.Fatalf("restoring orders without its foreign key: %v", err)
	}
	if len(dsum.Notes) != 1 || !strings.Contains(dsum.Notes[0], "dropped") {
		t.Fatalf("notes: %q", dsum.Notes)
	}
	execSQL(t, ctx, s3, `INSERT INTO orders_alone VALUES (13, 12345, 'free')`)
	requireSameRows(t, "orders restored alone",
		rowsText(t, ctx, s3, `SELECT id, customer, what FROM orders_alone WHERE id < 13 ORDER BY id`), []string{"10|1|anvil", "11|2|rope"})

	// A dropped table comes back under its own name.
	execSQL(t, ctx, s3, `DROP TABLE orders`)
	if _, err := tc.Nodes[0].RunRestoreOpts(ctx, server.RestoreOptions{Srcs: chain, DropForeignKeys: true,
		Tables: []server.RestoreTable{{Name: "orders"}}}); err != nil {
		t.Fatalf("restoring the dropped table: %v", err)
	}
	requireSameRows(t, "the dropped table, restored",
		rowsText(t, ctx, s, `SELECT id, customer, what FROM orders ORDER BY id`), []string{"10|1|anvil", "11|2|rope"})

	// A database, as a new one.
	execSQL(t, ctx, s, `CREATE DATABASE shop`)
	shop := sql.NewSession(tc.Nodes[0].DB(), catalog.NewAccessor())
	execSQL(t, ctx, shop, `USE shop`)
	execSQL(t, ctx, shop, `CREATE TABLE parts (id INT8 PRIMARY KEY, name TEXT)`)
	execSQL(t, ctx, shop, `CREATE TABLE uses (id INT8 PRIMARY KEY, part INT8 REFERENCES parts (id))`)
	execSQL(t, ctx, shop, `INSERT INTO parts VALUES (1, 'bolt'), (2, 'nut')`)
	execSQL(t, ctx, shop, `INSERT INTO uses VALUES (1, 2)`)
	dirShop := t.TempDir()
	if _, err := tc.Nodes[0].RunBackup(ctx, dirShop, "", false, false); err != nil {
		t.Fatal(err)
	}
	if _, err := tc.Nodes[1].RunRestoreOpts(ctx, server.RestoreOptions{Srcs: []string{dirShop}, Database: "shop"}); err == nil || !strings.Contains(err.Error(), `database "shop" already exists`) {
		t.Fatalf("restoring a database over itself: %v", err)
	}
	if _, err := tc.Nodes[1].RunRestoreOpts(ctx, server.RestoreOptions{Srcs: []string{dirShop}, Database: "shop", IntoDatabase: "shop_r"}); err != nil {
		t.Fatalf("database restore: %v", err)
	}
	requireSameRows(t, "the restored database's table",
		rowsText(t, ctx, s3, `SELECT id, name FROM shop_r.parts ORDER BY id`), []string{"1|bolt", "2|nut"})
	if _, serr := trySQL(ctx, s3, `INSERT INTO shop_r.uses VALUES (2, 77)`); serr == nil {
		t.Fatal("the foreign key inside the restored database is not enforced")
	}

	// The writers ran throughout, and the tables no restore named are
	// what they were: a quiet one by checksum, the busy one by its
	// invariant.
	stop.Store(true)
	wg.Wait()
	if transfers.Load() == before {
		t.Fatal("no transfer committed while the restores ran")
	}
	if total := execSQL(t, ctx, s, `SELECT SUM(balance) FROM accounts`).Rows[0][0].I; total != nAccounts*1000 {
		t.Fatalf("accounts total %d after the restores, want %d", total, nAccounts*1000)
	}
	dirAfter := t.TempDir()
	sumAfter, err := tc.Nodes[0].RunBackup(ctx, dirAfter, "", false, false)
	if err != nil {
		t.Fatal(err)
	}
	if a, b := tableSummary(t, sumAfter, "customers"), tableSummary(t, sumRef, "customers"); a.SHA256 != b.SHA256 || a.ID != b.ID {
		t.Fatalf("customers changed across the restores: %+v, was %+v", a, b)
	}
	if recs := restoreRecords(t, ctx, tc.Nodes[0]); len(recs) != 0 {
		t.Fatalf("finished restores left progress records: %v", recs)
	}
}

// TestSealedTableRestore (issues #291, #301): a table restore reads a
// sealed chain under the same rules a restore of everything does — never
// without its key, and nothing applied when a file fails to verify.
func TestSealedTableRestore(t *testing.T) {
	tc := Start(t, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 240*time.Second)
	defer cancel()
	n := tc.Nodes[0]
	s := sql.NewSession(n.DB(), catalog.NewAccessor())
	waitForDatabases(t, ctx, s)
	execSQL(t, ctx, s, `CREATE TABLE ledger (id INT8 PRIMARY KEY, memo TEXT)`)
	execSQL(t, ctx, s, `CREATE INDEX by_memo ON ledger (memo)`)
	execSQL(t, ctx, s, `CREATE TABLE other (id INT8 PRIMARY KEY)`)
	for i := 0; i < 200; i++ {
		execSQL(t, ctx, s, `INSERT INTO ledger VALUES (`+itoa(i)+`, 'sealed-memo-`+itoa(i)+`')`)
	}
	keyA, keyB := newBackupKey(t), newBackupKey(t)
	dirFull, dirInc := t.TempDir(), t.TempDir()
	if _, err := n.RunBackupWith(ctx, server.BackupOptions{Dest: dirFull, Key: keyA}); err != nil {
		t.Fatal(err)
	}
	execSQL(t, ctx, s, `UPDATE ledger SET memo = 'changed' WHERE id < 10`)
	execSQL(t, ctx, s, `DELETE FROM ledger WHERE id >= 190`)
	if _, err := n.RunBackupWith(ctx, server.BackupOptions{Dest: dirInc, BasePath: dirFull, Key: keyB, BaseKey: keyA}); err != nil {
		t.Fatal(err)
	}
	want := rowsText(t, ctx, s, `SELECT id, memo FROM ledger ORDER BY id`)
	execSQL(t, ctx, s, `DELETE FROM ledger`)

	chain := []string{dirFull, dirInc}
	target := []server.RestoreTable{{Name: "ledger", Into: "ledger_r"}}
	idGen := descIDGen(t, ctx, n)
	nothingApplied := func(what string) {
		t.Helper()
		if _, serr := trySQL(ctx, s, `SELECT 1 FROM ledger_r LIMIT 1`); serr == nil {
			t.Fatalf("%s: the table exists", what)
		}
		if got := descIDGen(t, ctx, n); got != idGen {
			t.Fatalf("%s: the id generator moved from %s to %s", what, idGen, got)
		}
		if recs := restoreRecords(t, ctx, n); len(recs) != 0 {
			t.Fatalf("%s: a progress record was left: %v", what, recs)
		}
	}
	for _, c := range []struct {
		what string
		keys [][]byte
		want string
	}{
		{"no key", nil, "is a sealed backup and no key was given"},
		{"one key for a chain under two", [][]byte{keyA}, "does not open this backup"},
		{"the keys the wrong way round", [][]byte{keyB, keyA}, "does not open this backup"},
		{"a key that is neither", [][]byte{newBackupKey(t)}, "does not open this backup"},
	} {
		_, err := n.RunRestoreOpts(ctx, server.RestoreOptions{Srcs: chain, Keys: c.keys, Tables: target})
		if err == nil || !strings.Contains(err.Error(), c.want) {
			t.Fatalf("%s: got %v, want a refusal saying %q", c.what, err, c.want)
		}
		nothingApplied(c.what)
	}

	// A byte changed in the table's data file, in either backup, and a
	// file cut short: refused before anything is written.
	keysAB := [][]byte{keyA, keyB}
	var ledgerFile string
	entries, err := os.ReadDir(dirFull)
	if err != nil {
		t.Fatal(err)
	}
	var biggest int64
	for _, e := range entries {
		if fi, _ := e.Info(); fi != nil && strings.HasPrefix(e.Name(), "table_") && fi.Size() > biggest {
			biggest, ledgerFile = fi.Size(), e.Name()
		}
	}
	for _, dir := range chain {
		path := filepath.Join(dir, ledgerFile)
		orig, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		for what, damaged := range map[string][]byte{
			"a changed byte":   append(append(append([]byte(nil), orig[:len(orig)/2]...), orig[len(orig)/2]^0x01), orig[len(orig)/2+1:]...),
			"a file cut short": orig[:len(orig)-7],
		} {
			if err := os.WriteFile(path, damaged, 0o644); err != nil {
				t.Fatal(err)
			}
			_, err := n.RunRestoreOpts(ctx, server.RestoreOptions{Srcs: chain, Keys: keysAB, Tables: target})
			if err == nil {
				t.Fatalf("%s in %s was restored", what, path)
			}
			nothingApplied(what)
		}
		if err := os.WriteFile(path, orig, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	// Another table's file is not this restore's to read: damage there
	// does not stop it (and a restore of everything would catch it).
	sum, err := n.RunRestoreOpts(ctx, server.RestoreOptions{Srcs: chain, Keys: keysAB, Tables: target})
	if err != nil {
		t.Fatalf("sealed table restore: %v", err)
	}
	requireSameRows(t, "the sealed chain's table", rowsText(t, ctx, s, `SELECT id, memo FROM ledger_r ORDER BY id`), want)
	requireSameRows(t, "a lookup through the restored index",
		rowsText(t, ctx, s, `SELECT id FROM ledger_r WHERE memo = 'sealed-memo-42'`), []string{"42"})
	if len(sum.Tables) != 1 || sum.Tables[0].Records == 0 {
		t.Fatalf("summary: %+v", sum)
	}
	// Nothing of the rows is in the clear in either directory.
	for _, dir := range chain {
		if hits := dirContains(t, dir, []string{"sealed-memo-42", "ledger"}); len(hits) != 0 {
			t.Fatalf("%s holds plaintext: %v", dir, hits)
		}
	}
}

// TestTableRestoreTamperedPlaintext: a plaintext backup has no seal to
// fail, so what stops a changed file is what the restore checks itself —
// a record outside the table's span before anything is written, and the
// loaded rows against the chain's checksum before anything is visible.
func TestTableRestoreTamperedPlaintext(t *testing.T) {
	tc := Start(t, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	n := tc.Nodes[0]
	s := sql.NewSession(n.DB(), catalog.NewAccessor())
	waitForDatabases(t, ctx, s)
	execSQL(t, ctx, s, `CREATE TABLE plain (id INT8 PRIMARY KEY, v TEXT)`)
	execSQL(t, ctx, s, `INSERT INTO plain VALUES (1, 'one'), (2, 'two'), (3, 'three')`)
	dir := t.TempDir()
	sum, err := n.RunBackup(ctx, dir, "", false, false)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "table_"+itoa(int(tableSummary(t, sum, "plain").ID))+".dxbk")
	orig, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	idGen := descIDGen(t, ctx, n)
	target := []server.RestoreTable{{Name: "plain", Into: "plain_r"}}
	// A record for another table's span appended to the file: a restore
	// re-keys what it reads, and must not re-key this.
	foreign := keys.TableDataPrefix(tableSummary(t, sum, "plain").ID + 1000)
	extra := append([]byte{byte(len(foreign))}, foreign...)
	extra = append(extra, 1, 'x', 0)
	// The first record dropped, and a byte of the last value changed:
	// every record is in span and in order, and the file is not the one
	// the backup wrote.
	firstLen := int(orig[0]) + 1
	firstLen += int(orig[firstLen]) + 2
	for what, c := range map[string]struct {
		data []byte
		want string
	}{
		"a record outside the table": {append(append([]byte(nil), orig...), extra...), "outside the table's own span"},
		"a file cut short":           {orig[:len(orig)-2], "truncated record"},
		"a record gone":              {orig[firstLen:], "is not the file its backup's manifest describes"},
		"a value changed":            {append(append([]byte(nil), orig[:len(orig)-2]...), orig[len(orig)-2]^0x20, orig[len(orig)-1]), "is not the file its backup's manifest describes"},
	} {
		if err := os.WriteFile(path, c.data, 0o644); err != nil {
			t.Fatal(err)
		}
		_, err := n.RunRestoreOpts(ctx, server.RestoreOptions{Srcs: []string{dir}, Tables: target})
		if err == nil || !strings.Contains(err.Error(), c.want) {
			t.Fatalf("%s: got %v, want a refusal saying %q", what, err, c.want)
		}
		if got := descIDGen(t, ctx, n); got != idGen {
			t.Fatalf("%s: the id generator moved", what)
		}
	}
	if _, serr := trySQL(ctx, s, `SELECT 1 FROM plain_r LIMIT 1`); serr == nil {
		t.Fatal("a refused restore left a table")
	}
	// Put right, the file restores.
	if err := os.WriteFile(path, orig, 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := n.RunRestoreOpts(ctx, server.RestoreOptions{Srcs: []string{dir}, Tables: target}); err != nil {
		t.Fatal(err)
	}
	requireSameRows(t, "the table from the repaired file",
		rowsText(t, ctx, s, `SELECT id, v FROM plain_r ORDER BY id`), []string{"1|one", "2|two", "3|three"})
}

// TestTableRestoreAbandonedIsReclaimed (issue #301): a table restore
// that stops after loading its rows — its node died — leaves no table
// anyone can see, and the rows it loaded are removed: not by another
// node while the record is fresh, and by the restore's own node once it
// is back.
func TestTableRestoreAbandonedIsReclaimed(t *testing.T) {
	tc, engines := StartWithEngines(t, 3)
	ctx, cancel := context.WithTimeout(context.Background(), 240*time.Second)
	defer cancel()
	s := sql.NewSession(tc.Nodes[0].DB(), catalog.NewAccessor())
	waitForDatabases(t, ctx, s)
	execSQL(t, ctx, s, `CREATE TABLE cargo (id INT8 PRIMARY KEY, v TEXT)`)
	execSQL(t, ctx, s, `CREATE INDEX by_v ON cargo (v)`)
	for i := 0; i < 150; i++ {
		execSQL(t, ctx, s, `INSERT INTO cargo VALUES (`+itoa(i)+`, 'crate-`+itoa(i)+`')`)
	}
	dir := t.TempDir()
	if _, err := tc.Nodes[0].RunBackup(ctx, dir, "", false, false); err != nil {
		t.Fatal(err)
	}
	opts := server.RestoreOptions{Srcs: []string{dir}, Tables: []server.RestoreTable{{Name: "cargo", Into: "cargo_r"}}}

	abandon := opts
	abandon.TestingAbandonAfterLoad = true
	if _, err := tc.Nodes[2].RunRestoreOpts(ctx, abandon); err == nil {
		t.Fatal("the abandoned restore reported success")
	}
	recs := restoreRecords(t, ctx, tc.Nodes[0])
	if len(recs) != 1 || len(recs[0]) != 1 {
		t.Fatalf("progress records after the abandoned restore: %v", recs)
	}
	orphan := recs[0][0]
	if got := tableRowKeys(t, ctx, tc.Nodes[0], orphan); got != 300 {
		t.Fatalf("the abandoned restore left %d keys under table id %d, want the 150 rows and 150 index entries", got, orphan)
	}
	// Loaded, and visible to nobody: no descriptor names the id.
	if _, serr := trySQL(ctx, s, `SELECT COUNT(*) FROM cargo_r`); serr == nil {
		t.Fatal("a table whose restore never published can be read")
	}
	if raw, err := tc.Nodes[0].DB().Get(ctx, keys.TableDescKey(orphan)); err != nil || raw != nil {
		t.Fatalf("a descriptor exists for the unpublished table: %q %v", raw, err)
	}
	// Another node leaves a restore alone while its record is fresh: for
	// all it knows the restore is running.
	if got := tc.Nodes[0].RunRestoreJanitorOnce(ctx); got != 0 {
		t.Fatalf("node 1 reclaimed %d restore(s) of node 3's with a fresh record", got)
	}
	if got := tableRowKeys(t, ctx, tc.Nodes[0], orphan); got != 300 {
		t.Fatalf("%d keys left after another node's janitor pass, want 300", got)
	}

	// The node that ran it comes back from a restart and takes its own
	// leftovers away.
	tc.StopNode(2)
	n3 := tc.RestartNode(2, engines[2])
	deadline := time.Now().Add(60 * time.Second)
	for {
		n3.RunRestoreJanitorOnce(ctx)
		if len(restoreRecords(t, ctx, tc.Nodes[0])) == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the restarted node never reclaimed its abandoned restore")
		}
		time.Sleep(200 * time.Millisecond)
	}
	if got := tableRowKeys(t, ctx, tc.Nodes[0], orphan); got != 0 {
		t.Fatalf("%d keys left under table id %d after the reclaim", got, orphan)
	}
	if _, serr := trySQL(ctx, s, `SELECT COUNT(*) FROM cargo_r`); serr == nil {
		t.Fatal("the reclaimed restore's table can be read")
	}

	// And the restore, run again, completes under an id of its own.
	sum, err := tc.Nodes[1].RunRestoreOpts(ctx, opts)
	if err != nil {
		t.Fatalf("the restore, run again: %v", err)
	}
	if sum.Tables[0].ID == orphan {
		t.Fatalf("the second restore reused table id %d", orphan)
	}
	requireSameRows(t, "the table after the second restore",
		rowsText(t, ctx, s, `SELECT id, v FROM cargo_r ORDER BY id`), rowsText(t, ctx, s, `SELECT id, v FROM cargo ORDER BY id`))
}

// TestTableRestoreNeedsV21: until the cluster has finalized v21 a node
// may be running a binary that knows nothing of a table restore's
// progress record, or of exports that keep history, so neither starts.
func TestTableRestoreNeedsV21(t *testing.T) {
	tc := StartWithOptions(t, 1, func(c *server.Config) { c.BinaryVersionOverride = version.V20 })
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	n := tc.Nodes[0]
	s := sql.NewSession(n.DB(), catalog.NewAccessor())
	waitForDatabases(t, ctx, s)
	execSQL(t, ctx, s, `CREATE TABLE old (a INT8 PRIMARY KEY)`)
	execSQL(t, ctx, s, `INSERT INTO old VALUES (1)`)
	dir := t.TempDir()
	if _, err := n.RunBackup(ctx, dir, "", false, false); err != nil {
		t.Fatal(err)
	}
	_, err := n.RunRestoreOpts(ctx, server.RestoreOptions{Srcs: []string{dir}, Tables: []server.RestoreTable{{Name: "old", Into: "old_r"}}})
	if err == nil || !strings.Contains(err.Error(), "needs cluster version v21") {
		t.Fatalf("table restore at v20: %v", err)
	}
	_, err = n.RunBackupWith(ctx, server.BackupOptions{Dest: t.TempDir(), BasePath: dir, RevisionHistory: true})
	if err == nil || !strings.Contains(err.Error(), "needs cluster version v21") {
		t.Fatalf("backup with revision history at v20: %v", err)
	}
	// What v20 could do, it still does.
	if _, err := n.RunBackup(ctx, t.TempDir(), dir, false, false); err != nil {
		t.Fatalf("a plain incremental at v20: %v", err)
	}
}
