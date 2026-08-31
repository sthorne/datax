package testcluster

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/sql"
	"github.com/sthorne/datax/pkg/sql/catalog"
)

// TestTxnSavepointRollback: KV-level savepoint semantics — rolled-back
// writes vanish (own reads and post-commit state), pre-savepoint writes
// survive, and a rewritten key restores its pre-savepoint value.
func TestTxnSavepointRollback(t *testing.T) {
	tc := Start(t, 3)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	db := tc.Nodes[0].DB()
	prefix := keys.TableDataPrefix(920)
	k := func(s string) keys.Key { return append(prefix.Clone(), s...) }

	txn := db.NewTxn("sp")
	if err := txn.Put(ctx, k("a"), []byte("a1")); err != nil {
		t.Fatal(err)
	}
	if err := txn.Savepoint("s1"); err != nil {
		t.Fatal(err)
	}
	if err := txn.Put(ctx, k("a"), []byte("a2")); err != nil { // rewrite
		t.Fatal(err)
	}
	if err := txn.Put(ctx, k("b"), []byte("b1")); err != nil { // new key
		t.Fatal(err)
	}
	if err := txn.RollbackToSavepoint(ctx, "s1"); err != nil {
		t.Fatal(err)
	}

	// Own reads see the savepoint state.
	if v, err := txn.Get(ctx, k("a")); err != nil || !bytes.Equal(v, []byte("a1")) {
		t.Fatalf("own read a = %q, %v; want a1", v, err)
	}
	if v, err := txn.Get(ctx, k("b")); err != nil || v != nil {
		t.Fatalf("own read b = %q, %v; want absent", v, err)
	}

	// New writes after the rollback work, and commit persists exactly the
	// surviving state.
	if err := txn.Put(ctx, k("c"), []byte("c1")); err != nil {
		t.Fatal(err)
	}
	if err := txn.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if v, _ := db.Get(ctx, k("a")); !bytes.Equal(v, []byte("a1")) {
		t.Fatalf("committed a = %q, want a1", v)
	}
	if v, _ := db.Get(ctx, k("b")); v != nil {
		t.Fatalf("committed b = %q, want absent", v)
	}
	if v, _ := db.Get(ctx, k("c")); !bytes.Equal(v, []byte("c1")) {
		t.Fatalf("committed c = %q, want c1", v)
	}
}

// TestSQLSavepoints: the SQL surface — nested savepoint visibility, the
// 25P02 escape via ROLLBACK TO SAVEPOINT, RELEASE semantics, and error
// codes.
func TestSQLSavepoints(t *testing.T) {
	tc := Start(t, 3)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	s := sql.NewSession(tc.Nodes[0].DB(), catalog.NewAccessor())
	execSQL(t, ctx, s, `CREATE TABLE sp (id INT8 PRIMARY KEY, v INT8)`)

	// Partial rollback inside a transaction.
	execSQL(t, ctx, s, `BEGIN`)
	execSQL(t, ctx, s, `INSERT INTO sp VALUES (1, 10)`)
	execSQL(t, ctx, s, `SAVEPOINT s1`)
	execSQL(t, ctx, s, `UPDATE sp SET v = 20 WHERE id = 1`)
	execSQL(t, ctx, s, `INSERT INTO sp VALUES (2, 99)`)
	execSQL(t, ctx, s, `ROLLBACK TO SAVEPOINT s1`)
	res := execSQL(t, ctx, s, `SELECT v FROM sp WHERE id = 1`)
	if res.Rows[0][0].I != 10 {
		t.Fatalf("post-rollback v = %d, want 10", res.Rows[0][0].I)
	}
	res = execSQL(t, ctx, s, `SELECT COUNT(*) FROM sp`)
	if res.Rows[0][0].I != 1 {
		t.Fatalf("post-rollback count = %d, want 1", res.Rows[0][0].I)
	}
	execSQL(t, ctx, s, `COMMIT`)
	res = execSQL(t, ctx, s, `SELECT v FROM sp WHERE id = 1`)
	if res.Rows[0][0].I != 10 {
		t.Fatalf("committed v = %d, want 10", res.Rows[0][0].I)
	}

	// The 25P02 escape: a statement error fails the transaction; ROLLBACK
	// TO SAVEPOINT restores it to working order (the driver retry recipe).
	execSQL(t, ctx, s, `BEGIN`)
	execSQL(t, ctx, s, `SAVEPOINT before_err`)
	if _, serr := trySQL(ctx, s, `INSERT INTO sp VALUES (1, 1)`); serr == nil {
		t.Fatal("duplicate insert succeeded")
	}
	if _, serr := trySQL(ctx, s, `SELECT 1`); serr == nil || serr.Code != "25P02" {
		t.Fatalf("failed txn accepted a statement: %v", serr)
	}
	execSQL(t, ctx, s, `ROLLBACK TO SAVEPOINT before_err`)
	execSQL(t, ctx, s, `INSERT INTO sp VALUES (3, 30)`) // txn usable again
	execSQL(t, ctx, s, `COMMIT`)
	res = execSQL(t, ctx, s, `SELECT v FROM sp WHERE id = 3`)
	if len(res.Rows) != 1 || res.Rows[0][0].I != 30 {
		t.Fatalf("post-escape insert missing: %+v", res.Rows)
	}

	// RELEASE destroys the savepoint and everything after it.
	execSQL(t, ctx, s, `BEGIN`)
	execSQL(t, ctx, s, `SAVEPOINT a`)
	execSQL(t, ctx, s, `SAVEPOINT b`)
	execSQL(t, ctx, s, `RELEASE SAVEPOINT a`)
	if _, serr := trySQL(ctx, s, `ROLLBACK TO SAVEPOINT b`); serr == nil || serr.Code != "3B001" {
		t.Fatalf("released-later savepoint still usable: %v", serr)
	}
	execSQL(t, ctx, s, `ROLLBACK`)

	// Missing savepoint and outside-transaction errors.
	execSQL(t, ctx, s, `BEGIN`)
	if _, serr := trySQL(ctx, s, `ROLLBACK TO SAVEPOINT nope`); serr == nil || serr.Code != "3B001" {
		t.Fatalf("missing savepoint: %v", serr)
	}
	execSQL(t, ctx, s, `ROLLBACK`)
	if _, serr := trySQL(ctx, s, `SAVEPOINT solo`); serr == nil || serr.Code != "25P01" {
		t.Fatalf("savepoint outside txn: %v", serr)
	}
}
