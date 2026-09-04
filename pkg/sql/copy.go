package sql

import (
	"context"
	"fmt"

	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/kvclient"
	"github.com/sthorne/datax/pkg/sql/catalog"
	"github.com/sthorne/datax/pkg/sql/parser"
	"github.com/sthorne/datax/pkg/sql/types"
)

// COPY FROM STDIN executes as a sequence of bounded implicit transactions
// ("chunks"), not one statement-sized transaction: an arbitrarily large
// stream in a single transaction would accumulate an unbounded pipelined
// write batch and one giant raft footprint — the same argument that shapes
// the online index backfill (see online_ddl.go). The price is PostgreSQL's
// atomicity: a mid-COPY failure leaves earlier chunks committed, which the
// error message reports. COPY is refused inside an explicit transaction
// block for the same reason (the CREATE INDEX precedent).
const (
	// copyChunkRows bounds a chunk by row count. Each COPY row costs one
	// PK-uniqueness read, one unique-index read per unique index, and one
	// Put per index — 128 rows keeps a chunk's batch in the low hundreds
	// of operations (cf. backfillChunkSize, sized for contended re-reads,
	// and restoreChunk, sized for raw puts with no reads).
	copyChunkRows = 128
	// copyChunkBytes bounds a chunk by accumulated value size, so wide
	// rows cannot balloon a single raft entry.
	copyChunkBytes = 1 << 20
)

// copyBufRow is one buffered, decoded row awaiting its chunk flush.
type copyBufRow struct {
	ordinal int64 // 1-based position in the COPY stream, for error reports
	vals    []types.Datum
}

// CopyIn is an in-progress COPY FROM STDIN: the wire layer decodes rows
// and feeds them in; CopyIn buffers, chunks, and commits them through the
// shared insert pipeline.
type CopyIn struct {
	s         *Session
	table     string
	target    []catalog.Column
	buf       []copyBufRow
	bufBytes  int
	rowsRead  int64
	committed int64
}

// BeginCopy validates a COPY FROM statement (table, columns, privilege)
// and returns the copy-in state machine. Refused inside an explicit
// transaction block: COPY commits per chunk.
func (s *Session) BeginCopy(ctx context.Context, cf *parser.CopyFrom) (*CopyIn, error) {
	switch s.state {
	case StateOpen:
		return nil, newErrf(CodeActiveTransaction, "COPY FROM cannot run inside a transaction block")
	case StateFailed:
		return nil, newErrf(CodeInFailedTransaction,
			"current transaction is aborted, commands ignored until end of transaction block")
	}
	var target []catalog.Column
	err := s.db.RunTxn(ctx, "copy-begin", func(ctx context.Context, txn *kvclient.Txn) error {
		desc, err := s.lookup(ctx, txn, cf.Table)
		if err != nil {
			return err
		}
		if err := mustBeReal(desc); err != nil {
			return err
		}
		if err := s.checkTablePriv(ctx, txn, desc, "INSERT"); err != nil {
			return err
		}
		target, err = resolveInsertTargets(desc, cf.Columns)
		return err
	})
	if err != nil {
		return nil, err
	}
	return &CopyIn{s: s, table: cf.Table, target: target}, nil
}

// Columns returns the resolved target columns, in stream order — the wire
// layer decodes each row against their types.
func (ci *CopyIn) Columns() []catalog.Column { return ci.target }

// RowsRead returns how many rows have been accepted so far (the current
// row's ordinal is RowsRead()+1 while decoding).
func (ci *CopyIn) RowsRead() int64 { return ci.rowsRead }

// RowsCommitted returns how many rows earlier chunks have durably
// committed.
func (ci *CopyIn) RowsCommitted() int64 { return ci.committed }

// AddRow buffers one decoded row (positionally parallel to Columns) and
// flushes a chunk when full.
func (ci *CopyIn) AddRow(ctx context.Context, vals []types.Datum) error {
	ci.rowsRead++
	if len(vals) != len(ci.target) {
		return newErrf(CodeBadCopyFormat, "COPY %s, row %d: row has %d fields, expected %d",
			ci.table, ci.rowsRead, len(vals), len(ci.target))
	}
	ci.buf = append(ci.buf, copyBufRow{ordinal: ci.rowsRead, vals: vals})
	for i := range vals {
		ci.bufBytes += len(vals[i].S) + 16
	}
	if len(ci.buf) >= copyChunkRows || ci.bufBytes >= copyChunkBytes {
		return ci.flushChunk(ctx)
	}
	return nil
}

// Finish flushes the remaining rows and returns the total committed.
func (ci *CopyIn) Finish(ctx context.Context) (int64, error) {
	if err := ci.flushChunk(ctx); err != nil {
		return ci.committed, err
	}
	return ci.committed, nil
}

// Abort drops buffered (uncommitted) rows. Chunks already committed stay
// committed — the documented COPY failure semantics.
func (ci *CopyIn) Abort() {
	ci.buf = nil
	ci.bufBytes = 0
}

// flushChunk commits the buffered rows in one implicit transaction. The
// closure rebuilds the write batch from the buffered rows on EVERY
// attempt: the per-row uniqueness reads must re-execute inside the
// retry's fresh transaction (replaying a saved batch would skip them).
func (ci *CopyIn) flushChunk(ctx context.Context) error {
	if len(ci.buf) == 0 {
		return nil
	}
	rows := ci.buf
	ci.buf = nil
	ci.bufBytes = 0
	err := ci.s.db.RunTxn(ctx, "copy-chunk", func(ctx context.Context, txn *kvclient.Txn) error {
		// Re-resolve the descriptor inside the transaction so concurrent
		// DDL (a new index mid-COPY) is honored, like the index backfill.
		desc, err := ci.s.lookup(ctx, txn, ci.table)
		if err != nil {
			return err
		}
		target := make([]catalog.Column, len(ci.target))
		for i := range ci.target {
			col, ok := desc.Col(ci.target[i].Name)
			if !ok {
				return newErrf(CodeUndefinedColumn, "column %q dropped during COPY", ci.target[i].Name)
			}
			target[i] = col
		}
		// Probe every row's primary key in ONE batched read (the per-range
		// sub-batches fan out in parallel) instead of a sequential Get per
		// row — the difference between a chunk costing one round trip and
		// costing 128.
		// Expression defaults (sequences, uuids) for the columns the COPY
		// does not supply, evaluated per row.
		defaults, err := ci.s.prepareDefaults(ctx, txn, desc, nil)
		if err != nil {
			return err
		}
		guard, err := ci.s.guard(ctx, txn, desc)
		if err != nil {
			return err
		}
		targets := make([][]catalog.Column, len(rows))
		built := make([]map[catalog.ColumnID]types.Datum, len(rows))
		pkKeys := make([]keys.Key, len(rows))
		for i, r := range rows {
			rt, rv, err := ci.s.expandDefaults(ctx, txn, desc, defaults, target, r.vals, nil)
			if err != nil {
				return ci.rowErr(r.ordinal, err)
			}
			targets[i], rows[i].vals = rt, rv
			row, key, err := buildInsertRow(desc, rt, rv)
			if err != nil {
				return ci.rowErr(r.ordinal, err)
			}
			built[i], pkKeys[i] = row, key
		}
		existing, err := txn.GetBatch(ctx, pkKeys)
		if err != nil {
			return err
		}
		var wb kvclient.WriteBatch
		inserted := map[string]bool{}
		for i, r := range rows {
			if existing[i] != nil {
				return ci.rowErr(r.ordinal, newErrf(CodeUniqueViolation,
					"duplicate key value violates unique constraint on %q", desc.Name))
			}
			if err := insertRow(ctx, txn, desc, targets[i], r.vals, &wb, inserted, true); err != nil {
				return ci.rowErr(r.ordinal, err)
			}
			if err := guard.checkInsert(ctx, txn, built[i], inserted); err != nil {
				return ci.rowErr(r.ordinal, err)
			}
		}
		return txn.RunBatch(ctx, &wb)
	})
	if err != nil {
		return err
	}
	ci.committed += int64(len(rows))
	return nil
}

// rowErr pins a per-row failure to its stream ordinal. Retryable errors
// pass through untouched so RunTxn can retry the chunk.
func (ci *CopyIn) rowErr(ordinal int64, err error) error {
	if kvclient.IsRetryable(err) {
		return err
	}
	if e, ok := err.(*Error); ok {
		return newErrf(e.Code, "COPY %s, row %d: %s", ci.table, ordinal, e.Msg)
	}
	return fmt.Errorf("COPY %s, row %d: %w", ci.table, ordinal, err)
}
