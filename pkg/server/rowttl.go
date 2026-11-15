package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/kvclient"
	"github.com/sthorne/datax/pkg/kvpb"
	"github.com/sthorne/datax/pkg/metrics"
	"github.com/sthorne/datax/pkg/sql"
	"github.com/sthorne/datax/pkg/sql/catalog"
	"github.com/sthorne/datax/pkg/sql/parser"
	"github.com/sthorne/datax/pkg/sql/types"
	"github.com/sthorne/datax/pkg/util/log"
	"github.com/sthorne/datax/pkg/version"
)

// Row expiry (issue #298). A table with a row TTL (WITH (ttl_expiration
// = col), or ttl_column with ttl; catalog.RowTTL) has its expired rows
// deleted by this job.
//
// Time-series retention expires rows beneath SQL, as a GC rule on keys
// (retention.go), which is why it has never been able to serve a table
// with secondary indexes: an index entry carries no timestamp to judge it
// by, and a row removed from under its entries leaves the index pointing
// at nothing. An ordinary table has indexes, foreign keys and whatever
// else hangs off a row, so here the row is removed the way an application
// would remove it: DELETE, in a transaction, through the session code
// every other DELETE takes. Index entries go with the row, a cascading
// foreign key takes the children, a restricting one refuses, and the
// deleted versions wait for garbage collection like any others.
//
// The consequence is that expiry is a deadline for the job and not a
// property of a read: a row past its time is visible until the job gets
// to it. A query that must not see one says so in its WHERE clause.
//
// Every node runs the loop and the node leading range 1 acts, the
// cluster-singleton idiom of the statistics sampler and the re-shard
// janitor. Leadership can change hands mid-pass and for a moment two
// nodes may both delete; the deletes are ordinary transactions over the
// same rows, so the second finds nothing or loses a write conflict and
// retries into nothing.
//
// Each pass visits every table with a TTL in turn. It finds expired rows
// through the index on the expiry column — the DDL makes sure there is
// one — oldest first, a batch at a time, and deletes each batch in one
// transaction. A batch a foreign key refuses is retried row by row, and
// the rows that are still refused are stepped over for the rest of the
// pass and counted; they are found again next pass, and go when whatever
// references them does.

const (
	defaultRowTTLInterval   = 30 * time.Second
	defaultRowTTLBatchSize  = 100
	defaultRowTTLBatchPause = 20 * time.Millisecond
)

func (n *Node) rowTTLLoop(ctx context.Context) {
	interval := n.cfg.RowTTLInterval
	if interval < 0 {
		return
	}
	if interval == 0 {
		interval = defaultRowTTLInterval
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		r1, ok := n.store.GetReplica(1)
		if !ok || !r1.IsLeader() || n.ClusterVersion() < version.V21 {
			continue
		}
		n.RunRowTTLOnce(ctx)
	}
}

// RowTTLPass is what one pass over one table did.
type RowTTLPass struct {
	Table   string
	Deleted int
	// Blocked is how many expired rows a foreign key kept.
	Blocked int
	// Lag is how far past its expiry time the most overdue row deleted
	// in this pass was.
	Lag time.Duration
	Err error
}

// RunRowTTLOnce runs one pass over every table with a row TTL and
// returns what it did to each table it touched. Exported for tests,
// which drive passes instead of waiting on the loop.
func (n *Node) RunRowTTLOnce(ctx context.Context) []RowTTLPass {
	tables, err := n.rowTTLTables(ctx)
	if err != nil {
		log.Debugf("row expiry: catalog scan: %v", err)
		return nil
	}
	budget := n.cfg.RowTTLInterval
	if budget <= 0 {
		budget = defaultRowTTLInterval
	}
	var out []RowTTLPass
	var worst time.Duration
	for _, t := range tables {
		if ctx.Err() != nil {
			break
		}
		// One table gets at most one interval of the pass: a table with
		// a backlog must not starve the ones behind it.
		tctx, cancel := context.WithTimeout(ctx, budget)
		pass := n.expireTable(tctx, t)
		cancel()
		if pass.Lag > worst {
			worst = pass.Lag
		}
		if pass.Deleted+pass.Blocked > 0 || pass.Err != nil {
			out = append(out, pass)
		}
	}
	if len(tables) > 0 {
		metrics.RowTTLLagSeconds.Set(worst.Seconds())
	}
	return out
}

// rowTTLTable is a table to expire, with the SQL that does it.
type rowTTLTable struct {
	name  string // qualified, as SQL text
	after time.Duration
	// find selects the primary key and the expiry column of expired
	// rows, oldest first: $1 the cutoff, $2 the batch, $3 rows to skip.
	find string
	// del deletes one row by primary key if it is still expired: $1 the
	// cutoff, then the key.
	del string
	pk  int
}

// rowTTLTables reads the descriptors and builds the statements for every
// table that expires rows.
func (n *Node) rowTTLTables(ctx context.Context) ([]rowTTLTable, error) {
	var descKVs, dbKVs []kvpb.KeyValue
	if err := n.db.RunTxn(ctx, "row-ttl-scan", func(ctx context.Context, txn *kvclient.Txn) error {
		lo, hi := keys.TableDescSpan()
		var err error
		if descKVs, err = txn.Scan(ctx, lo, hi, 0); err != nil {
			return err
		}
		dlo, dhi := keys.DatabaseDescSpan()
		dbKVs, err = txn.Scan(ctx, dlo, dhi, 0)
		return err
	}); err != nil {
		return nil, err
	}
	dbName := map[uint64]string{}
	for _, kv := range dbKVs {
		var d catalog.DatabaseDescriptor
		if json.Unmarshal(kv.Value, &d) == nil && d.ID != 0 {
			dbName[d.ID] = d.Name
		}
	}
	var out []rowTTLTable
	for _, kv := range descKVs {
		var d catalog.TableDescriptor
		if json.Unmarshal(kv.Value, &d) != nil || d.ID == 0 || d.RowTTL == nil || d.IsView() {
			continue
		}
		col, ok := d.ColByID(d.RowTTL.ColumnID)
		if !ok || len(d.PrimaryKey) == 0 {
			log.Warnf("row expiry: table %q names an expiry column it does not have; skipping it", d.Name)
			continue
		}
		db := dbName[d.DatabaseID]
		if db == "" {
			db = catalog.DefaultDatabase
		}
		t := rowTTLTable{
			name:  sqlIdent(db) + "." + sqlIdent(d.Name),
			after: time.Duration(d.RowTTL.AfterSeconds) * time.Second,
			pk:    len(d.PrimaryKey),
		}
		var pkCols, pkEq []string
		for i, id := range d.PrimaryKey {
			pc, ok := d.ColByID(id)
			if !ok {
				pkCols = nil
				break
			}
			pkCols = append(pkCols, sqlIdent(pc.Name))
			pkEq = append(pkEq, fmt.Sprintf("%s = $%d", sqlIdent(pc.Name), i+2))
		}
		if pkCols == nil {
			continue
		}
		c := sqlIdent(col.Name)
		t.find = fmt.Sprintf("SELECT %s, %s FROM %s WHERE %s <= $1 ORDER BY %s LIMIT $2 OFFSET $3",
			strings.Join(pkCols, ", "), c, t.name, c, c)
		t.del = fmt.Sprintf("DELETE FROM %s WHERE %s <= $1 AND %s", t.name, c, strings.Join(pkEq, " AND "))
		out = append(out, t)
	}
	return out, nil
}

// sqlIdent renders a name as an identifier in SQL text.
func sqlIdent(name string) string {
	plain := name != ""
	for i, r := range name {
		if !(r == '_' || r >= 'a' && r <= 'z' || i > 0 && r >= '0' && r <= '9') {
			plain = false
			break
		}
	}
	if plain {
		return name
	}
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}

// expireTable deletes a table's expired rows until none are left, the
// context ends, or an error it cannot step over.
func (n *Node) expireTable(ctx context.Context, t rowTTLTable) RowTTLPass {
	pass := RowTTLPass{Table: t.name}
	batch := n.cfg.RowTTLBatchSize
	if batch <= 0 {
		batch = defaultRowTTLBatchSize
	}
	pause := n.cfg.RowTTLBatchPause
	if pause == 0 {
		pause = defaultRowTTLBatchPause
	}
	sess, err := n.systemSession()
	if err != nil {
		pass.Err = err
		return pass
	}
	// Paired in the operations view only when there is something to
	// delete: a pass that finds nothing is not an operation (the rule
	// the re-shard janitor follows).
	var opID string
	defer func() {
		if opID == "" && pass.Err == nil {
			return
		}
		if opID == "" {
			opID = newOpID()
			n.events.RecordStart("row-ttl", opID, "expiring rows of %s", t.name)
		}
		outcome, detail := "ok", ""
		if pass.Err != nil {
			outcome, detail = "error", ": "+pass.Err.Error()
		}
		n.events.RecordEnd("row-ttl", opID, outcome, "%s: %d expired row(s) deleted, %d kept by a foreign key, most overdue by %s%s",
			t.name, pass.Deleted, pass.Blocked, pass.Lag.Truncate(time.Millisecond), detail)
	}()
	// The cutoff is fixed for the pass: rows that expire while it runs
	// are the next pass's, and the skip count below stays meaningful.
	now := n.clock.Now().WallTime
	cutoff := types.NewTimestamp(now - t.after.Nanoseconds())
	skip := 0
	for {
		if err := ctx.Err(); err != nil {
			// Out of budget with rows left: not an error, the next pass
			// goes on from the oldest.
			return pass
		}
		res, serr := execText(ctx, sess, t.find, cutoff, types.NewInt(int64(batch)), types.NewInt(int64(skip)))
		if serr != nil {
			if ctx.Err() == nil {
				pass.Err = serr
			}
			return pass
		}
		if len(res.Rows) == 0 {
			return pass
		}
		if opID == "" {
			opID = newOpID()
			n.events.RecordStart("row-ttl", opID, "expiring rows of %s", t.name)
		}
		deleted, blocked, err := n.deleteExpired(ctx, sess, t, cutoff, res.Rows)
		pass.Deleted += deleted
		pass.Blocked += blocked
		skip += blocked
		metrics.RowTTLRowsDeleted.Add(float64(deleted))
		metrics.RowTTLRowsBlocked.Add(float64(blocked))
		if deleted > 0 {
			// Oldest first, so the first row is the most overdue.
			if d := res.Rows[0][t.pk]; !d.Null {
				if lag := time.Duration(now - d.I - t.after.Nanoseconds()); lag > pass.Lag {
					pass.Lag = lag
				}
			}
		}
		if err != nil {
			if ctx.Err() == nil {
				pass.Err = err
			}
			return pass
		}
		if len(res.Rows) < batch {
			return pass
		}
		select {
		case <-ctx.Done():
			return pass
		case <-time.After(pause):
		}
	}
}

// deleteExpired deletes one batch in one transaction. If a foreign key
// refuses the batch it deletes the rows one at a time instead, so one
// referenced row costs that row and not the ninety-nine beside it; the
// rows still refused are returned as blocked.
func (n *Node) deleteExpired(ctx context.Context, sess *sql.Session, t rowTTLTable, cutoff types.Datum, rows [][]types.Datum) (deleted, blocked int, _ error) {
	params := func(row []types.Datum) []types.Datum {
		return append([]types.Datum{cutoff}, row[:t.pk]...)
	}
	var serr *sql.Error
	for attempt := 0; attempt < 3; attempt++ {
		deleted = 0
		if _, serr = execText(ctx, sess, "BEGIN"); serr != nil {
			return 0, 0, serr
		}
		for _, row := range rows {
			var res *sql.Result
			if res, serr = execText(ctx, sess, t.del, params(row)...); serr != nil {
				break
			}
			deleted += rowsAffected(res)
		}
		if serr == nil {
			if _, serr = execText(ctx, sess, "COMMIT"); serr == nil {
				return deleted, 0, nil
			}
		}
		// The rollback must run even when the pass's time is what ended
		// the batch, or the session is left inside the transaction.
		rctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		_, _ = execText(rctx, sess, "ROLLBACK")
		cancel()
		if serr.Code != sql.CodeSerializationFailure {
			break
		}
	}
	if serr.Code != sql.CodeForeignKeyViolation {
		return 0, 0, serr
	}
	deleted = 0
	for _, row := range rows {
		res, serr := execText(ctx, sess, t.del, params(row)...)
		switch {
		case serr == nil:
			deleted += rowsAffected(res)
		case serr.Code == sql.CodeForeignKeyViolation:
			blocked++
		default:
			return deleted, blocked, serr
		}
	}
	return deleted, blocked, nil
}

// rowsAffected reads the count from a DELETE's command tag.
func rowsAffected(res *sql.Result) int {
	var n int
	if res != nil {
		_, _ = fmt.Sscanf(res.Tag, "DELETE %d", &n)
	}
	return n
}

// execText parses and runs one statement on sess.
func execText(ctx context.Context, sess *sql.Session, text string, params ...types.Datum) (*sql.Result, *sql.Error) {
	stmts, err := parser.Parse(text)
	if err != nil {
		return nil, sql.ToSQLError(err)
	}
	if len(stmts) != 1 {
		return nil, sql.ToSQLError(errors.New("row expiry: expected one statement"))
	}
	return sess.Execute(ctx, stmts[0], params)
}
