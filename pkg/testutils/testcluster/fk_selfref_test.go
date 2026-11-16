package testcluster

import (
	"context"
	"testing"
	"time"
)

// TestForeignKeySelfReferenceOverPGWire: a table whose foreign key
// references the table itself is created through a gateway that leases
// descriptors (every pgwire session). The reference used to look the
// table up through a lease, whose own transaction waited on the
// creating one's name entry for as long as it ran: forever.
func TestForeignKeySelfReferenceOverPGWire(t *testing.T) {
	tc := Start(t, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	conn := pgConn(t, ctx, tc, 0)
	mustExec(t, ctx, conn, `CREATE TABLE emp (id INT PRIMARY KEY, boss INT REFERENCES emp ON DELETE CASCADE)`)
	mustExec(t, ctx, conn, `CREATE TABLE node (id INT PRIMARY KEY, parent INT)`)
	mustExec(t, ctx, conn, `ALTER TABLE node ADD CONSTRAINT node_parent FOREIGN KEY (parent) REFERENCES datax.node (id)`)
	mustExec(t, ctx, conn, `INSERT INTO emp VALUES (1, NULL), (2, 1)`)
	wantSQLState(t, ctx, conn, "23503", `INSERT INTO emp VALUES (3, 42)`)
	wantSQLState(t, ctx, conn, "23503", `INSERT INTO node VALUES (1, 42)`)
	mustExec(t, ctx, conn, `DELETE FROM emp WHERE id = 1`)
	wantRows(t, ctx, conn, `SELECT count(*) FROM emp`, "0")
}
