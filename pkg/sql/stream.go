package sql

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/kvclient"
	"github.com/sthorne/datax/pkg/kvpb"
	"github.com/sthorne/datax/pkg/metrics"
	"github.com/sthorne/datax/pkg/sql/catalog"
	"github.com/sthorne/datax/pkg/sql/parser"
	"github.com/sthorne/datax/pkg/sql/rowenc"
	"github.com/sthorne/datax/pkg/sql/types"
)

// Streaming execution (issue #104). A scan-shaped SELECT — one table, no
// join, aggregate, DISTINCT, window, set operation, correlated subquery
// or in-memory sort — does not materialize its result: Execute returns a
// Result whose Stream pulls rows from KV in pages of streamPageSize as
// the wire layer asks for them, so the first row leaves the gateway
// before the last one is read and a full-table SELECT holds one page at
// a time. Everything else (sorts, aggregates, joins, DISTINCT, the
// catalog tables) still materializes, under the statement memory limit.
//
// The retry rule: an implicit transaction re-runs the statement on a
// retryable error only while nothing has been flushed to the client
// (the wire layer reports the first flush with Flushed); after that the
// error is surfaced after the rows already sent, as PostgreSQL does.

// streamPageSize is the number of KV rows fetched by a stream's first
// page; each following page doubles up to streamPageMax, so a short
// result costs one small round trip and a long one few round trips
// while one page at a time stays in memory.
const (
	streamPageSize = 512
	streamPageMax  = 4096
)

// ErrStreamRestarted is returned by RowStream.Next when a retryable
// error re-ran the statement from the beginning before any row had
// been flushed: the caller discards the rows it has buffered and keeps
// pulling.
var ErrStreamRestarted = errors.New("stream restarted")

// TestingStreamHook, when set, runs after every row a stream produces,
// with the number of rows produced so far; an error it returns is
// treated as if the pull had failed (a *kvclient.RetryableError
// exercises the restart rule, a blocking hook the cancellation path).
var TestingStreamHook func(ctx context.Context, rows int64) error

// RowStream is a pull-based result: Next yields one output row at a
// time. Close releases it (rolling back an implicit transaction that
// was not drained); it is idempotent.
type RowStream struct {
	s      *Session
	stmt   parser.Statement
	params []types.Datum
	cols   []ResultColumn

	iter *scanIter
	// rows serves a materialized result through the stream interface
	// (a restart whose re-execution did not stream).
	rows [][]types.Datum
	rpos int

	offset, limit    int64 // limit < 0: none
	skipped, emitted int64

	deadline    time.Time
	lockTimeout time.Duration

	// implicit: the stream owns txn (commits it when drained, rolls it
	// back on error or Close) and may restart on a retryable error;
	// explicit: txn is the session's block and an error fails it.
	implicit bool
	txn      *kvclient.Txn
	attempt  int
	onError  func()

	flushed bool
	done    bool
	closed  bool
	err     error
}

// Flushed records that rows have reached the client: from now on a
// retryable error is surfaced instead of re-running the statement.
func (st *RowStream) Flushed() { st.flushed = true }

// Tag is the command tag once the stream is drained ("SELECT n").
func (st *RowStream) Tag() string { return fmt.Sprintf("SELECT %d", st.emitted) }

// Rows is the number of rows emitted so far.
func (st *RowStream) Rows() int64 { return st.emitted }

// stmtCtx applies the statement's timeout and lock timeout to a pull.
func (st *RowStream) stmtCtx(ctx context.Context) (context.Context, context.CancelFunc) {
	cancel := context.CancelFunc(func() {})
	if !st.deadline.IsZero() {
		ctx, cancel = context.WithDeadline(ctx, st.deadline)
	}
	if st.lockTimeout > 0 {
		ctx = kvclient.WithLockTimeout(ctx, st.lockTimeout)
	}
	return ctx, cancel
}

// Next returns the next output row, or ok=false when the stream is
// drained (the implicit transaction is then committed). An error ends
// the stream; ErrStreamRestarted is the one exception (see above).
func (st *RowStream) Next(callerCtx context.Context) (row []types.Datum, ok bool, err error) {
	if st.done || st.closed {
		return nil, false, st.err
	}
	ctx, cancel := st.stmtCtx(callerCtx)
	defer cancel()
	defer func() {
		// The statement path's panic barrier (issue #136) extends to the
		// rows a stream produces after Execute returned: the stream ends
		// the way it does on any error, with XX000.
		if r := recover(); r != nil {
			row, ok, err = nil, false, st.fail(callerCtx, st.s.recoveredPanic(r, st.stmt))
		}
	}()
	for {
		if st.limit >= 0 && st.emitted >= st.limit {
			return nil, false, st.finish(ctx)
		}
		out, more, err := st.pull(ctx)
		if err == nil && more && TestingStreamHook != nil {
			err = TestingStreamHook(ctx, st.emitted+1)
		}
		if err != nil {
			if st.implicit && !st.flushed && kvclient.IsRetryable(err) && callerCtx.Err() == nil && st.attempt < 20 {
				if rerr := st.restart(ctx); rerr != nil {
					return nil, false, st.fail(callerCtx, rerr)
				}
				metrics.SQLStreamRestarts.Inc()
				return nil, false, ErrStreamRestarted
			}
			return nil, false, st.fail(callerCtx, err)
		}
		if !more {
			return nil, false, st.finish(ctx)
		}
		if st.skipped < st.offset {
			st.skipped++
			continue
		}
		st.emitted++
		metrics.SQLStreamedRows.Inc()
		return out, true, nil
	}
}

// pull produces the next projected row from the iterator or the
// materialized fallback.
func (st *RowStream) pull(ctx context.Context) ([]types.Datum, bool, error) {
	if st.iter == nil {
		if st.rpos >= len(st.rows) {
			return nil, false, nil
		}
		r := st.rows[st.rpos]
		st.rpos++
		return r, true, nil
	}
	fr, ok, err := st.iter.next(ctx)
	if err != nil || !ok {
		return nil, false, err
	}
	out, err := st.iter.project(fr)
	if err != nil {
		return nil, false, err
	}
	return out, true, nil
}

// finish ends a drained stream: the implicit transaction commits.
func (st *RowStream) finish(ctx context.Context) error {
	st.done = true
	if st.implicit && st.txn != nil {
		if err := st.txn.Commit(ctx); err != nil {
			st.err = err
			return err
		}
	}
	return nil
}

// fail ends the stream on err: an implicit transaction rolls back, an
// explicit block is marked failed. The returned error carries the
// statement-timeout / user-cancel distinction the way Execute does.
func (st *RowStream) fail(callerCtx context.Context, err error) error {
	st.done = true
	if st.implicit {
		if st.txn != nil {
			_ = st.txn.Rollback(context.Background())
		}
	} else if st.onError != nil {
		st.onError()
	}
	serr := ToSQLError(err)
	if serr.Code == CodeQueryCanceled && callerCtx.Err() != nil {
		serr = &Error{Code: CodeQueryCanceled, Msg: "canceling statement due to user request"}
	}
	st.err = serr
	return serr
}

// restart re-runs the statement on a fresh implicit transaction (the
// shape of DB.RunTxn's retry loop: the old one rolled back, the new one
// pushing harder, a short backoff).
func (st *RowStream) restart(ctx context.Context) error {
	for {
		_ = st.txn.Rollback(ctx)
		st.attempt++
		next := st.s.db.NewTxn("sql-implicit")
		next.EnablePipelining()
		next.BumpPriority(st.txn)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Duration(st.attempt) * 20 * time.Millisecond):
		}
		st.txn = next
		st.s.streamTop = true
		res, err := st.s.execStmt(ctx, next, st.stmt, st.params)
		st.s.streamTop = false
		if err != nil {
			if kvclient.IsRetryable(err) && ctx.Err() == nil && st.attempt < 20 {
				continue
			}
			return err
		}
		st.skipped, st.emitted = 0, 0
		if res.Stream != nil {
			st.iter, st.rows, st.rpos = res.Stream.iter, nil, 0
		} else {
			// The re-run did not stream (a plan change): serve its rows
			// through the same interface, committing at once.
			st.iter, st.rows, st.rpos = nil, res.Rows, 0
			if err := next.Commit(ctx); err != nil {
				if kvclient.IsRetryable(err) && ctx.Err() == nil && st.attempt < 20 {
					continue
				}
				return err
			}
			st.txn = nil
		}
		return nil
	}
}

// Close releases the stream: an implicit transaction that was not
// drained is rolled back. Safe to call more than once.
func (st *RowStream) Close(ctx context.Context) {
	if st.closed {
		return
	}
	st.closed = true
	if !st.done && st.implicit && st.txn != nil {
		_ = st.txn.Rollback(ctx)
	}
	st.done = true
}

// attachImplicit hands the stream the implicit transaction it runs in
// and what it needs to re-run the statement.
func (st *RowStream) attachImplicit(txn *kvclient.Txn, stmt parser.Statement, params []types.Datum) {
	st.implicit, st.txn, st.stmt, st.params = true, txn, stmt, params
}

// attachExplicit binds the stream to the session's transaction block:
// an error mid-stream fails the block.
func (st *RowStream) attachExplicit(onError func()) {
	st.implicit, st.onError = false, onError
}

// keySpan is one KV span a scan covers.
type keySpan struct{ start, end keys.Key }

// scanIter pulls fetched rows page by page from a primary-key or index
// scan.
type scanIter struct {
	s      *Session
	txn    *kvclient.Txn
	desc   *catalog.TableDescriptor
	plan   accessPlan
	where  []parser.Comparison
	params []types.Datum
	proj   []projCol

	spans   []keySpan // spans still to scan (per shard bucket when fanned)
	cur     keySpan   // the span in progress
	active  bool
	reverse bool
	// fetchLimit caps the rows yielded (LIMIT + OFFSET pushed down); 0
	// is none.
	fetchLimit int64
	yielded    int64

	buf  []fetchedRow
	bpos int
	// pageSize is the next page's row count (doubling to streamPageMax).
	pageSize int64
	// index marks an index scan: pages of entries whose primary rows are
	// fetched in batches.
	index bool
}

// next yields the next matching row.
func (it *scanIter) next(ctx context.Context) (fetchedRow, bool, error) {
	for {
		if it.fetchLimit > 0 && it.yielded >= it.fetchLimit {
			return fetchedRow{}, false, nil
		}
		if it.bpos < len(it.buf) {
			fr := it.buf[it.bpos]
			it.buf[it.bpos] = fetchedRow{}
			it.bpos++
			it.yielded++
			return fr, true, nil
		}
		more, err := it.fill(ctx)
		if err != nil {
			return fetchedRow{}, false, err
		}
		if !more {
			return fetchedRow{}, false, nil
		}
	}
}

// fill fetches the next page into buf; false when every span is done.
func (it *scanIter) fill(ctx context.Context) (bool, error) {
	it.buf, it.bpos = it.buf[:0], 0
	for len(it.buf) == 0 {
		if !it.active {
			if len(it.spans) == 0 {
				return false, nil
			}
			it.cur, it.spans, it.active = it.spans[0], it.spans[1:], true
		}
		if it.pageSize == 0 {
			it.pageSize = streamPageSize
		}
		page := it.pageSize
		if it.fetchLimit > 0 && len(it.plan.residual) == 0 {
			// Every scanned row is a result row: read no more than the
			// limit still allows.
			if left := it.fetchLimit - it.yielded; left < page {
				page = left
			}
		}
		kvs, err := spanScan(ctx, it.txn, it.cur.start, it.cur.end, page, it.reverse)
		if err != nil {
			return false, err
		}
		metrics.SQLRowsScanned.Add(float64(len(kvs)))
		if int64(len(kvs)) < page {
			it.active = false // the span is exhausted with this page
		} else if it.reverse {
			it.cur.end = kvs[len(kvs)-1].Key
		} else {
			it.cur.start = kvs[len(kvs)-1].Key.Next()
		}
		if len(kvs) == 0 {
			continue
		}
		if it.pageSize < streamPageMax {
			it.pageSize *= 2
		}
		if it.index {
			if err := it.fillIndexPage(ctx, kvs); err != nil {
				return false, err
			}
			continue
		}
		for _, kv := range kvs {
			row, err := decodeFullRow(it.desc, kv.Key, kv.Value)
			if err != nil {
				return false, err
			}
			match, err := matchesWhere(it.where, it.desc, row, it.params)
			if err != nil {
				return false, err
			}
			if match {
				it.buf = append(it.buf, fetchedRow{key: kv.Key, row: row})
			}
		}
	}
	return true, nil
}

// fillIndexPage fetches the primary rows behind a page of index entries
// (in batches, issue #103) and filters them into buf.
func (it *scanIter) fillIndexPage(ctx context.Context, kvs []kvpb.KeyValue) error {
	pks := make([]keys.Key, len(kvs))
	for i, kv := range kvs {
		pk, err := rowenc.IndexEntryPrimaryKey(it.desc, it.plan.idx, kv.Key, kv.Value)
		if err != nil {
			return newErrf(CodeInternal, "%v", err)
		}
		pks[i] = pk
	}
	return fetchPrimaryRows(ctx, it.txn, pks, func(first int, raws [][]byte) (bool, error) {
		for i, raw := range raws {
			pk := pks[first+i]
			if raw == nil {
				return false, newErrf(CodeInternal, "index %q entry points at a missing row", it.plan.idx.Name)
			}
			row, err := decodeFullRow(it.desc, pk, raw)
			if err != nil {
				return false, err
			}
			match, err := matchesWhere(it.where, it.desc, row, it.params)
			if err != nil {
				return false, err
			}
			if match {
				it.buf = append(it.buf, fetchedRow{key: pk, row: row})
			}
		}
		return true, nil
	})
}

// project renders a fetched row through the select list.
func (it *scanIter) project(fr fetchedRow) ([]types.Datum, error) {
	out := make([]types.Datum, len(it.proj))
	for i, p := range it.proj {
		if p.expr != nil {
			d, err := evalExpr(*p.expr, it.desc, fr.row, it.params)
			if err != nil {
				return nil, err
			}
			out[i] = conformTo(d, p.col.Type)
			continue
		}
		d, ok := fr.row[p.col.ID]
		if !ok {
			d = types.DNull
		}
		out[i] = d
	}
	return out, nil
}

// streamable reports whether the scan-shaped select can stream: it is
// the statement's top-level select, the session streams, no EXPLAIN ANALYZE accounting, nothing after the
// fetch (sort, DISTINCT, FOR UPDATE, correlated filters or projections),
// a real table, and a plan that walks spans.
func (s *Session) streamable(top bool, desc *catalog.TableDescriptor, plan accessPlan, t *parser.Select, needSort bool, corr []correlatedConjunct, corrProjs []corrProj) bool {
	if !top || !s.streaming || s.explain != nil || needSort || t.Distinct || t.ForUpdate || len(corr) > 0 || len(corrProjs) > 0 || desc.Virtual != "" || plan.mergeFan {
		return false
	}
	switch plan.kind {
	case planFullScan, planPKScan, planIndexScan:
		return true
	}
	return false
}

// newScanStream builds the streaming result for a scan-shaped select.
func (s *Session) newScanStream(txn *kvclient.Txn, desc *catalog.TableDescriptor, plan accessPlan, t *parser.Select, proj []projCol, where []parser.Comparison, params []types.Datum, fetchLimit int64) (*Result, error) {
	it := &scanIter{s: s, txn: txn, desc: desc, plan: plan, where: where, params: params, proj: proj, reverse: plan.reverse, fetchLimit: fetchLimit}
	switch plan.kind {
	case planIndexScan:
		prefix, err := rowenc.EncodeIndexPrefix(desc, plan.idx, plan.idxVals)
		if err != nil {
			return nil, err
		}
		var fam types.Family
		if plan.hasBounds() {
			col, _ := desc.ColByID(plan.idx.ColumnIDs[len(plan.idxVals)])
			fam = col.Type
		}
		start, end, err := plan.spanBounds(prefix, fam)
		if err != nil {
			return nil, err
		}
		it.spans, it.index = []keySpan{{start, end}}, true
	case planPKScan:
		spans, err := pkScanSpans(desc, plan)
		if err != nil {
			return nil, err
		}
		it.spans = spans
	default:
		start, end := rowenc.PrimarySpanFor(desc)
		it.spans = []keySpan{{start, end}}
	}
	res := &Result{Stream: &RowStream{s: s, stmt: t, iter: it, offset: t.Offset, limit: t.Limit}}
	for _, p := range proj {
		res.Columns = append(res.Columns, colResult(p.name, p.col))
	}
	res.Stream.cols = res.Columns
	return res, nil
}

// pkScanSpans builds a primary-key scan's spans: one, or one per shard
// bucket on a sharded table (the pinned prefix and bounds constrain the
// logical key after the bucket).
func pkScanSpans(desc *catalog.TableDescriptor, plan accessPlan) ([]keySpan, error) {
	pkCols := desc.PrimaryKey
	if plan.fanBuckets > 0 {
		pkCols = pkCols[1:]
	}
	buildSpan := func(prefix keys.Key) (keySpan, error) {
		for i, d := range plan.idxVals {
			col, _ := desc.ColByID(pkCols[i])
			var err error
			prefix, err = rowenc.AppendKeyDatum(prefix, col.Type, d)
			if err != nil {
				return keySpan{}, newErrf(CodeInternal, "pk bound: %v", err)
			}
		}
		var fam types.Family
		if plan.hasBounds() {
			col, _ := desc.ColByID(pkCols[len(plan.idxVals)])
			fam = col.Type
		}
		start, end, err := plan.spanBounds(prefix, fam)
		if err != nil {
			return keySpan{}, newErrf(CodeInternal, "pk bound: %v", err)
		}
		return keySpan{start, end}, nil
	}
	if plan.fanBuckets == 0 {
		sp, err := buildSpan(rowenc.PrimaryKeyPrefixFor(desc))
		if err != nil {
			return nil, err
		}
		return []keySpan{sp}, nil
	}
	spans := make([]keySpan, 0, plan.fanBuckets)
	for b := int32(0); b < plan.fanBuckets; b++ {
		bp, err := rowenc.AppendKeyDatum(rowenc.PrimaryKeyPrefixFor(desc), types.Int, types.NewInt(int64(b)))
		if err != nil {
			return nil, newErrf(CodeInternal, "shard bound: %v", err)
		}
		sp, err := buildSpan(bp)
		if err != nil {
			return nil, err
		}
		spans = append(spans, sp)
	}
	return spans, nil
}

// Statement memory accounting: the materializing paths (scans that
// collect their rows, sorts, aggregates, joins, DISTINCT, WITH members)
// charge what they hold against statement_memory_limit and fail with
// 53200 beyond it instead of growing without bound. Sizes are estimates
// of the in-memory footprint (a datum's header plus its variable part).

// charge accounts n bytes to the running statement.
func (s *Session) charge(n int64) error {
	s.memUsed += n
	if lim := s.vars.memoryLimit; lim > 0 && s.memUsed > lim {
		metrics.SQLMemoryLimitHits.Inc()
		return newErrf(CodeOutOfMemory, "statement memory limit of %s exceeded (the statement holds about %s in memory); raise statement_memory_limit or narrow the statement",
			formatMemory(lim), formatMemory(s.memUsed))
	}
	return nil
}

// chargeRow accounts one fetched row.
func (s *Session) chargeRow(row map[catalog.ColumnID]types.Datum) error {
	var n int64 = 64
	for _, d := range row {
		n += datumBytes(d)
	}
	return s.charge(n)
}

// chargeDatums accounts one output row.
func (s *Session) chargeDatums(row []types.Datum) error {
	var n int64 = 24
	for _, d := range row {
		n += datumBytes(d)
	}
	return s.charge(n)
}

// datumBytes estimates a datum's footprint.
func datumBytes(d types.Datum) int64 {
	n := int64(96 + len(d.S))
	for _, e := range d.A {
		n += datumBytes(e)
	}
	return n
}

// formatMemory renders a byte count the way the limit is set.
func formatMemory(n int64) string {
	switch {
	case n >= 1<<30 && n%(1<<30) == 0:
		return fmt.Sprintf("%dGB", n>>30)
	case n >= 1<<20 && n%(1<<20) == 0:
		return fmt.Sprintf("%dMB", n>>20)
	case n >= 1<<10 && n%(1<<10) == 0:
		return fmt.Sprintf("%dkB", n>>10)
	}
	return fmt.Sprintf("%dB", n)
}
