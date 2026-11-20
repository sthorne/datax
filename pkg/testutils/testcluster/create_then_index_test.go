package testcluster

import (
	"context"
	"fmt"
	"math/rand/v2"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/kvclient"
	"github.com/sthorne/datax/pkg/sql/catalog"
)

// The migration of issue #390: the table, then an index on its columns,
// each statement on a connection of its own.
const (
	quarantineTable = `CREATE TABLE IF NOT EXISTS pipeline_quarantine (id INT8 PRIMARY KEY, payload TEXT, held BOOL NOT NULL DEFAULT false, received_at TIMESTAMPTZ NOT NULL DEFAULT now())`
	quarantineIndex = `CREATE INDEX IF NOT EXISTS pipeline_quarantine_age_idx ON pipeline_quarantine (held, received_at)`
)

// descIDOf is the descriptor ID the name resolves to in database db.
func descIDOf(t *testing.T, ctx context.Context, tc *TestCluster, db, name string) uint64 {
	t.Helper()
	var id uint64
	if err := tc.Nodes[0].DB().RunTxn(ctx, "test-table-id", func(ctx context.Context, txn *kvclient.Txn) error {
		d, err := catalog.NewAccessor().LookupFreshIn(ctx, txn, db, name)
		if err != nil {
			return err
		}
		id = d.ID
		return nil
	}); err != nil {
		t.Fatalf("resolving %s.%s: %v", db, name, err)
	}
	return id
}

// TestCreateTableNeverTakesAnIDInUse (issue #390): a CREATE INDEX on a
// table another connection had just created reported one of the table's
// columns missing (42703). The index is built against the descriptor
// the table's name entry leads to, read in the statement's own
// transaction, and every version of the table carries all its columns;
// the descriptor read was another table's, written under the same ID.
// The ID counter is put back here as if it had lost an increment: the
// next table must not take an ID a descriptor already holds and write
// over it, which left the first table's name leading to the second.
func TestCreateTableNeverTakesAnIDInUse(t *testing.T) {
	tc := Start(t, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	a, err := pgx.Connect(ctx, pgURL(tc, 0))
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close(context.Background())
	b, err := pgx.Connect(ctx, pgURL(tc, 0))
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close(context.Background())

	mustExec(t, ctx, a, quarantineTable)
	first := descIDOf(t, ctx, tc, catalog.DefaultDatabase, "pipeline_quarantine")
	if err := tc.Nodes[0].DB().RunTxn(ctx, "test-rewind", func(ctx context.Context, txn *kvclient.Txn) error {
		return txn.Put(ctx, keys.DescIDGenKey(), []byte(strconv.FormatUint(first-1, 10)))
	}); err != nil {
		t.Fatal(err)
	}
	// Every kind of object draws from the counter.
	mustExec(t, ctx, a, `CREATE TABLE pipeline_events (id INT8 PRIMARY KEY, kind TEXT)`)
	mustExec(t, ctx, a, `CREATE SEQUENCE pipeline_seq`)
	mustExec(t, ctx, a, `CREATE TYPE pipeline_state AS ENUM ('new', 'done')`)
	mustExec(t, ctx, a, `CREATE SCHEMA pipeline_private`)
	mustExec(t, ctx, a, `CREATE DATABASE pipeline_archive`)

	// The reported failure: column "held" does not exist.
	mustExec(t, ctx, b, quarantineIndex)
	if got := descIDOf(t, ctx, tc, catalog.DefaultDatabase, "pipeline_events"); got == first {
		t.Fatalf("pipeline_events took descriptor id %d, which pipeline_quarantine holds", got)
	}
	mustExec(t, ctx, b, `INSERT INTO pipeline_quarantine (id, payload) VALUES (1, 'x')`)
	wantRows(t, ctx, b, `SELECT id, held FROM pipeline_quarantine WHERE held = false`, "1|false")
	mustExec(t, ctx, b, `INSERT INTO pipeline_events VALUES (1, 'k')`)
	wantRows(t, ctx, b, `SELECT kind FROM pipeline_events`, "k")
	mustExec(t, ctx, b, `SELECT nextval('pipeline_seq')`)
	mustExec(t, ctx, b, `CREATE TABLE pipeline_private.t (s pipeline_state)`)
}

// TestNameEntryLeadingToAnotherTable (issue #390): a name entry that
// leads to a descriptor of another table is reported as the catalog
// damage it is, not taken for the named table (whose columns would
// then be reported missing, or another table changed in its place).
func TestNameEntryLeadingToAnotherTable(t *testing.T) {
	tc := Start(t, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	conn, err := pgx.Connect(ctx, pgURL(tc, 0))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(context.Background())
	mustExec(t, ctx, conn, quarantineTable)
	mustExec(t, ctx, conn, `CREATE DATABASE other`)
	id := descIDOf(t, ctx, tc, catalog.DefaultDatabase, "pipeline_quarantine")
	other, err := lookupDatabaseDesc(ctx, tc.Nodes[0].DB(), "other")
	if err != nil {
		t.Fatal(err)
	}
	if err := tc.Nodes[0].DB().RunTxn(ctx, "test-damage", func(ctx context.Context, txn *kvclient.Txn) error {
		// Another name of the same database, and the same name in
		// another database, both leading to the table's descriptor.
		if err := txn.Put(ctx, keys.TableNamespaceKey(other.ID, "pipeline_quarantine"), []byte(strconv.FormatUint(id, 10))); err != nil {
			return err
		}
		db, err := catalog.NewAccessor().DatabaseID(ctx, txn, catalog.DefaultDatabase)
		if err != nil {
			return err
		}
		return txn.Put(ctx, keys.TableNamespaceKey(db, "impostor"), []byte(strconv.FormatUint(id, 10)))
	}); err != nil {
		t.Fatal(err)
	}
	for _, q := range []string{
		`CREATE INDEX impostor_idx ON impostor (held)`,
		`SELECT held FROM impostor`,
		`CREATE INDEX other_idx ON other.public.pipeline_quarantine (held)`,
		`SELECT held FROM other.public.pipeline_quarantine`,
	} {
		_, err := conn.Exec(ctx, q)
		if sqlState(err) != "XX000" || !strings.Contains(err.Error(), fmt.Sprintf("points at descriptor %d, which is table \"pipeline_quarantine\" of database", id)) {
			t.Fatalf("%s: got %v, want the damaged name entry reported (XX000)", q, err)
		}
	}
	// The table itself is untouched.
	mustExec(t, ctx, conn, quarantineIndex)
	wantRows(t, ctx, conn, `SELECT count(*) FROM pipeline_quarantine WHERE held`, "0")
}

// TestCreateIndexRightAfterCreateTableUnderDDLLoad (issue #390): the
// reported shape under load. Connection A creates a table and B, at
// once, indexes two of its columns, many times, while other connections
// create, fill and drop databases and every descriptor lease taken on
// the node is slowed down. B must always find the table with all its
// columns.
func TestCreateIndexRightAfterCreateTableUnderDDLLoad(t *testing.T) {
	tc := Start(t, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Second)
	defer cancel()
	catalog.SetTestingBeforeLeaseWrite(func(*catalog.Accessor, string) {
		time.Sleep(time.Duration(rand.IntN(5)) * time.Millisecond)
	})
	defer catalog.SetTestingBeforeLeaseWrite(nil)
	addr := tc.Nodes[0].SQLAddr()
	dbURL := func(db string) string { return fmt.Sprintf("postgres://root@%s/%s?sslmode=disable", addr, db) }

	stop := make(chan struct{})
	var load sync.WaitGroup
	for w := range 3 {
		load.Add(1)
		go func() {
			defer load.Done()
			c, err := pgx.Connect(ctx, pgURL(tc, 0))
			if err != nil {
				t.Error(err)
				return
			}
			defer c.Close(context.Background())
			for i := 0; ; i++ {
				select {
				case <-stop:
					return
				default:
				}
				db := fmt.Sprintf("load_%d_%d", w, i)
				for _, q := range []string{
					`CREATE DATABASE ` + db,
					`CREATE TABLE ` + db + `.public.t (id INT8 PRIMARY KEY, v TEXT, n INT8 NOT NULL DEFAULT 0)`,
					`CREATE INDEX t_v ON ` + db + `.public.t (v, n)`,
					`DROP DATABASE ` + db + ` CASCADE`,
				} {
					if _, err := c.Exec(ctx, q); err != nil && sqlState(err) != "40001" {
						t.Errorf("load %s: %v", q, err)
					}
				}
			}
		}()
	}
	defer func() {
		close(stop)
		load.Wait()
	}()

	mustExec(t, ctx, mustConnect(t, ctx, pgURL(tc, 0)), `CREATE DATABASE app`)
	a := mustConnect(t, ctx, dbURL("app"))
	b := mustConnect(t, ctx, dbURL("app"))
	for i := range 60 {
		tbl := fmt.Sprintf("pipeline_quarantine_%d", i)
		create := strings.ReplaceAll(quarantineTable, "pipeline_quarantine", tbl)
		for attempt := 0; ; attempt++ {
			_, err := a.Exec(ctx, create)
			if err == nil {
				break
			}
			if sqlState(err) != "40001" || attempt == 10 {
				t.Fatalf("%s: %v", create, err)
			}
		}
		index := strings.ReplaceAll(quarantineIndex, "pipeline_quarantine", tbl)
		if _, err := b.Exec(ctx, index); err != nil {
			t.Fatalf("%s, right after the table was created on another connection: %v", index, err)
		}
	}
}

func mustConnect(t *testing.T, ctx context.Context, url string) *pgx.Conn {
	t.Helper()
	c, err := pgx.Connect(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close(context.Background()) })
	return c
}
