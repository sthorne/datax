package sql

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"

	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/kvclient"
	"github.com/sthorne/datax/pkg/sql/catalog"
	"github.com/sthorne/datax/pkg/sql/parser"
	"github.com/sthorne/datax/pkg/sql/types"
	"github.com/sthorne/datax/pkg/util/log"
	"github.com/sthorne/datax/pkg/version"
)

// Sequences, expression defaults and the volatile row functions
// (nextval, currval, lastval, setval, unique_rowid, gen_random_uuid).
//
// A sequence's counter is one key advanced with Increment OUTSIDE the
// calling transaction — PostgreSQL semantics: a value handed out is
// never handed out again even if the transaction aborts, so gaps are
// normal. Each gateway takes a block of CACHE values per Increment and
// serves nextval from it, so a hot sequence is not a per-row round trip
// to one range; values are unique across gateways but not monotonic
// between them.

// seqBlock is one gateway-local block of a sequence's values.
type seqBlock struct {
	next, last int64 // values still to hand out: next .. last, stepping by incr
	incr       int64
}

// seqState is the session's sequence bookkeeping (currval, lastval),
// created on first use. Value blocks are per gateway (seqBlocks, keyed
// by the node's DB handle): every session on a gateway draws from the
// same block, so short-lived connections do not each burn CACHE values.
type seqState struct {
	currval map[uint64]int64
	lastval *int64
}

func (s *Session) seq() *seqState {
	if s.seqs == nil {
		s.seqs = &seqState{currval: map[uint64]int64{}}
	}
	return s.seqs
}

type seqBlockKey struct {
	db *kvclient.DB
	id uint64
}

var seqBlocks = struct {
	sync.Mutex
	m map[seqBlockKey]*seqBlock
}{m: map[seqBlockKey]*seqBlock{}}

// dropSeqBlock forgets this gateway's block (ALTER/DROP SEQUENCE,
// setval). Other gateways keep serving theirs, as PostgreSQL sessions
// keep their cached values.
func (s *Session) dropSeqBlock(id uint64) {
	seqBlocks.Lock()
	delete(seqBlocks.m, seqBlockKey{s.db, id})
	seqBlocks.Unlock()
}

// requireV7 gates the DDL that writes v7 descriptor fields (expression
// defaults, sequences): a v6 node cannot evaluate them.
func (s *Session) requireV7(what string) error {
	if s.db.ClusterVersion() < version.V7 {
		return newErrf(CodeFeatureNotSupported, "%s needs cluster version v7: finalize the upgrade with `datax debug upgrade` first", what)
	}
	return nil
}

// lookupSequence resolves a (possibly db-qualified) sequence name in the
// session's database.
func (s *Session) lookupSequence(ctx context.Context, txn *kvclient.Txn, name string) (*catalog.SequenceDescriptor, error) {
	db, bare := catalog.SplitTableName(name)
	if db == "" {
		db = s.database
	}
	dbID, err := s.cat.DatabaseID(ctx, txn, db)
	if err != nil {
		return nil, ToSQLError(err)
	}
	d, err := catalog.LookupSequence(ctx, txn, dbID, bare)
	if err != nil {
		var nf *catalog.ErrSequenceNotFound
		if asErr(err, &nf) {
			return nil, newErrf(CodeUndefinedTable, "relation %q does not exist", name)
		}
		return nil, err
	}
	return d, nil
}

// applySequenceOptions folds parsed options into a descriptor.
func applySequenceOptions(d *catalog.SequenceDescriptor, o *parser.SequenceOptions, create bool) error {
	minSet, maxSet, startSet := !create, !create, !create
	if o.Increment != nil {
		d.Increment = *o.Increment
	}
	if o.MinValue != nil {
		d.MinValue, minSet = *o.MinValue, true
	}
	if o.NoMin {
		minSet = false
	}
	if o.MaxValue != nil {
		d.MaxValue, maxSet = *o.MaxValue, true
	}
	if o.NoMax {
		maxSet = false
	}
	if o.Start != nil {
		d.Start, startSet = *o.Start, true
	}
	if o.Cache != nil {
		d.Cache = *o.Cache
	}
	if o.Cycle != nil {
		d.Cycle = *o.Cycle
	}
	if err := d.Normalize(minSet, maxSet, startSet); err != nil {
		return newErrf(CodeInvalidParameter, "%v", err)
	}
	return nil
}

func (s *Session) execCreateSequence(ctx context.Context, txn *kvclient.Txn, t *parser.CreateSequence) (*Result, error) {
	if err := s.requireV7("CREATE SEQUENCE"); err != nil {
		return nil, err
	}
	if err := s.checkCreateInDatabase(ctx, txn, t.Name); err != nil {
		return nil, err
	}
	db, bare := catalog.SplitTableName(t.Name)
	if db == "" {
		db = s.database
	}
	dbID, err := s.cat.DatabaseID(ctx, txn, db)
	if err != nil {
		return nil, ToSQLError(err)
	}
	d := catalog.NewSequenceDescriptor(bare, dbID)
	if err := applySequenceOptions(d, &t.Options, true); err != nil {
		return nil, err
	}
	if t.Options.OwnedBy != "" && t.Options.OwnedBy != "none" {
		table, col, _ := strings.Cut(t.Options.OwnedBy, ".")
		owner, err := s.lookup(ctx, txn, table)
		if err != nil {
			return nil, err
		}
		c, ok := owner.Col(col)
		if !ok {
			return nil, newErrf(CodeUndefinedColumn, "column %q of table %q does not exist", col, table)
		}
		d.OwnerTable, d.OwnerColumn = owner.ID, c.ID
	}
	if err := catalog.CreateSequence(ctx, txn, d); err != nil {
		var ex *catalog.ErrSequenceExists
		if t.IfNotExists && asErr(err, &ex) {
			return &Result{Tag: "CREATE SEQUENCE"}, nil
		}
		return nil, ToSQLError(err)
	}
	log.Audit("sequence-ddl", "stmt", "CREATE SEQUENCE", "target", bare, "principal", s.user)
	return &Result{Tag: "CREATE SEQUENCE"}, nil
}

func (s *Session) execAlterSequence(ctx context.Context, txn *kvclient.Txn, t *parser.AlterSequence) (*Result, error) {
	d, err := s.lookupSequence(ctx, txn, t.Name)
	if err != nil {
		return nil, err
	}
	if err := s.checkSequencePriv(ctx, txn, d); err != nil {
		return nil, err
	}
	if err := applySequenceOptions(d, &t.Options, false); err != nil {
		return nil, err
	}
	if err := catalog.UpdateSequence(ctx, txn, d); err != nil {
		return nil, err
	}
	if t.Options.RestartSet {
		at := d.Start
		if t.Options.Restart != nil {
			at = *t.Options.Restart
		}
		if err := txn.Put(ctx, keys.SequenceValueKey(d.ID), []byte(strconv.FormatInt(at-d.Increment, 10))); err != nil {
			return nil, err
		}
	}
	s.dropSeqBlock(d.ID)
	return &Result{Tag: "ALTER SEQUENCE"}, nil
}

func (s *Session) execDropSequence(ctx context.Context, txn *kvclient.Txn, t *parser.DropSequence) (*Result, error) {
	d, err := s.lookupSequence(ctx, txn, t.Name)
	if err != nil {
		if serr, ok := err.(*Error); ok && serr.Code == CodeUndefinedTable && t.IfExists {
			return &Result{Tag: "DROP SEQUENCE"}, nil
		}
		return nil, err
	}
	if err := s.checkSequencePriv(ctx, txn, d); err != nil {
		return nil, err
	}
	if d.OwnerTable != 0 {
		if owner, err := catalog.ReadTable(ctx, txn, d.OwnerTable); err == nil && owner != nil {
			if c, ok := owner.ColByID(d.OwnerColumn); ok && c.SequenceID == d.ID {
				return nil, newErrf(CodeDependentObjectsExist, "cannot drop sequence %q because column %s.%s depends on it (drop the column, or the table)", d.Name, owner.Name, c.Name)
			}
		}
	}
	if err := catalog.DropSequence(ctx, txn, d); err != nil {
		return nil, err
	}
	s.dropSeqBlock(d.ID)
	log.Audit("sequence-ddl", "stmt", "DROP SEQUENCE", "target", d.Name, "principal", s.user)
	return &Result{Tag: "DROP SEQUENCE"}, nil
}

// checkSequencePriv: admins, and the owner table's writers, may alter or
// drop a sequence; an unowned sequence takes an admin.
func (s *Session) checkSequencePriv(ctx context.Context, txn *kvclient.Txn, d *catalog.SequenceDescriptor) error {
	admin, err := s.isAdmin(ctx, txn)
	if err != nil {
		return err
	}
	if admin {
		return nil
	}
	if d.OwnerTable != 0 {
		if owner, err := catalog.ReadTable(ctx, txn, d.OwnerTable); err == nil && owner != nil {
			if err := s.checkTablePriv(ctx, txn, owner, "INSERT"); err == nil {
				return nil
			}
		}
	}
	return newErrf(CodeInsufficientPriv, "permission denied for sequence %q", d.Name)
}

func (s *Session) execShowSequences(ctx context.Context, txn *kvclient.Txn) (*Result, error) {
	dbID, err := s.cat.DatabaseID(ctx, txn, s.database)
	if err != nil {
		return nil, ToSQLError(err)
	}
	seqs, err := catalog.ListSequences(ctx, txn, dbID)
	if err != nil {
		return nil, err
	}
	res := &Result{Columns: []ResultColumn{
		{Name: "sequence_name", Type: types.String}, {Name: "start", Type: types.Int}, {Name: "increment", Type: types.Int},
		{Name: "min_value", Type: types.Int}, {Name: "max_value", Type: types.Int}, {Name: "cycle", Type: types.Bool},
		{Name: "cache", Type: types.Int}, {Name: "last_value", Type: types.Int}, {Name: "owned_by", Type: types.String},
	}}
	for _, d := range seqs {
		last := types.DNull
		if v, called, err := s.sequenceValue(ctx, d); err == nil && called {
			last = types.NewInt(v)
		}
		owned := types.DNull
		if d.OwnerTable != 0 {
			if owner, err := catalog.ReadTable(ctx, txn, d.OwnerTable); err == nil && owner != nil {
				if c, ok := owner.ColByID(d.OwnerColumn); ok {
					owned = types.NewString(owner.Name + "." + c.Name)
				}
			}
		}
		res.Rows = append(res.Rows, []types.Datum{types.NewString(d.Name), types.NewInt(d.Start), types.NewInt(d.Increment),
			types.NewInt(d.MinValue), types.NewInt(d.MaxValue), types.NewBool(d.Cycle), types.NewInt(d.Cache), last, owned})
	}
	res.Tag = fmt.Sprintf("SHOW SEQUENCES %d", len(res.Rows))
	return res, nil
}

// sequenceValue reads a sequence's counter: the last value handed out
// (or, before any nextval, start - increment with called=false).
func (s *Session) sequenceValue(ctx context.Context, d *catalog.SequenceDescriptor) (int64, bool, error) {
	raw, err := s.db.Get(ctx, keys.SequenceValueKey(d.ID))
	if err != nil {
		return 0, false, err
	}
	if raw == nil {
		return d.Start - d.Increment, false, nil
	}
	v, err := strconv.ParseInt(string(raw), 10, 64)
	if err != nil {
		return 0, false, newErrf(CodeInternal, "corrupt sequence counter for %q: %q", d.Name, raw)
	}
	return v, v != d.Start-d.Increment, nil
}

// nextval hands out the sequence's next value: from the gateway-local
// block when one remains, else after taking a fresh block with one
// Increment outside the transaction.
func (s *Session) nextval(ctx context.Context, txn *kvclient.Txn, name string) (int64, error) {
	d, err := s.lookupSequence(ctx, txn, name)
	if err != nil {
		return 0, err
	}
	seqBlocks.Lock()
	defer seqBlocks.Unlock()
	key := seqBlockKey{s.db, d.ID}
	blk := seqBlocks.m[key]
	if blk == nil || blk.incr != d.Increment || (d.Increment > 0 && blk.next > blk.last) || (d.Increment < 0 && blk.next < blk.last) {
		blk, err = s.takeSequenceBlock(ctx, d)
		if err != nil {
			return 0, err
		}
		seqBlocks.m[key] = blk
	}
	v := blk.next
	blk.next += blk.incr
	st := s.seq()
	st.currval[d.ID] = v
	st.lastval = &v
	return v, nil
}

// takeSequenceBlock advances the counter by CACHE steps and returns the
// block, clipped at the bound: values past it are never handed out, and
// the sequence errors (2200H) or, with CYCLE, restarts at the far bound.
func (s *Session) takeSequenceBlock(ctx context.Context, d *catalog.SequenceDescriptor) (*seqBlock, error) {
	for attempt := 0; attempt < 2; attempt++ {
		step := d.Increment * d.Cache
		newLast, err := s.db.Increment(ctx, keys.SequenceValueKey(d.ID), step)
		if err != nil {
			return nil, err
		}
		first := newLast - step + d.Increment
		if d.Increment > 0 {
			if first > d.MaxValue {
				if !d.Cycle || attempt > 0 {
					return nil, newErrf(CodeSequenceLimit, "nextval: reached maximum value of sequence %q (%d)", d.Name, d.MaxValue)
				}
				// Restart at the minimum; the racing gateway's block, if any,
				// restarts too.
				if err := s.db.Put(ctx, keys.SequenceValueKey(d.ID), []byte(strconv.FormatInt(d.MinValue-d.Increment, 10))); err != nil {
					return nil, err
				}
				continue
			}
			last := newLast
			if last > d.MaxValue {
				last = d.MaxValue
			}
			return &seqBlock{next: first, last: last, incr: d.Increment}, nil
		}
		if first < d.MinValue {
			if !d.Cycle || attempt > 0 {
				return nil, newErrf(CodeSequenceLimit, "nextval: reached minimum value of sequence %q (%d)", d.Name, d.MinValue)
			}
			if err := s.db.Put(ctx, keys.SequenceValueKey(d.ID), []byte(strconv.FormatInt(d.MaxValue-d.Increment, 10))); err != nil {
				return nil, err
			}
			continue
		}
		last := newLast
		if last < d.MinValue {
			last = d.MinValue
		}
		return &seqBlock{next: first, last: last, incr: d.Increment}, nil
	}
	return nil, newErrf(CodeSequenceLimit, "nextval: sequence %q exhausted", d.Name)
}

func (s *Session) currval(ctx context.Context, txn *kvclient.Txn, name string) (int64, error) {
	d, err := s.lookupSequence(ctx, txn, name)
	if err != nil {
		return 0, err
	}
	v, ok := s.seq().currval[d.ID]
	if !ok {
		return 0, newErrf(CodeObjectNotInState, "currval of sequence %q is not yet defined in this session", d.Name)
	}
	return v, nil
}

func (s *Session) lastvalFn() (int64, error) {
	if s.seq().lastval == nil {
		return 0, newErrf(CodeObjectNotInState, "lastval is not yet defined in this session")
	}
	return *s.seq().lastval, nil
}

// setval sets the counter: with is_called (the default) the next value
// is v + increment; without, v itself.
func (s *Session) setval(ctx context.Context, txn *kvclient.Txn, name string, v int64, isCalled bool) (int64, error) {
	d, err := s.lookupSequence(ctx, txn, name)
	if err != nil {
		return 0, err
	}
	if v < d.MinValue || v > d.MaxValue {
		return 0, newErrf(CodeInvalidParameter, "setval: value %d is out of bounds for sequence %q (%d..%d)", v, d.Name, d.MinValue, d.MaxValue)
	}
	stored := v
	if !isCalled {
		stored = v - d.Increment
	}
	if err := s.db.Put(ctx, keys.SequenceValueKey(d.ID), []byte(strconv.FormatInt(stored, 10))); err != nil {
		return 0, err
	}
	s.dropSeqBlock(d.ID)
	s.seq().currval[d.ID] = v
	return v, nil
}

// lastRowID is the process-wide last unique_rowid, so ids stay strictly
// increasing on a node even within one microsecond.
var lastRowID atomic.Int64

// uniqueRowID is a node-local monotonic 64-bit id: 48 bits of microsecond
// wall time above 15 bits of node ID — unique across nodes without
// coordination, ascending on each node, and spread across nodes' key
// ranges unlike a sequence.
func (s *Session) uniqueRowID() int64 {
	node := int64(s.db.LocalNodeID()) & 0x7fff
	for {
		prev := lastRowID.Load()
		micros := time.Now().UnixMicro()
		id := micros<<15 | node
		if id <= prev {
			id = prev + 1<<15
			if id&0x7fff != node {
				id = (id &^ 0x7fff) | node
			}
		}
		if lastRowID.CompareAndSwap(prev, id) {
			return id
		}
	}
}

// volatileFuncs are evaluated per row by the session, not by evalFunc.
var volatileFuncs = map[string]bool{"nextval": true, "currval": true, "lastval": true, "setval": true, "unique_rowid": true, "gen_random_uuid": true}

// spliceVolatile evaluates the volatile row functions inside e into
// literals, for one row. Arguments must be constants.
func (s *Session) spliceVolatile(ctx context.Context, txn *kvclient.Txn, e parser.Expr, params []types.Datum) (parser.Expr, error) {
	out := e
	if e.Func != "" && volatileFuncs[e.Func] {
		args := make([]types.Datum, len(e.Args))
		for i, a := range e.Args {
			d, err := evalExpr(a, nil, nil, params)
			if err != nil {
				return e, err
			}
			args[i] = d
		}
		var d types.Datum
		switch e.Func {
		case "nextval", "currval":
			if len(args) != 1 {
				return e, newErrf(CodeSyntaxError, "%s() takes one argument", e.Func)
			}
			var v int64
			var err error
			if e.Func == "nextval" {
				v, err = s.nextval(ctx, txn, args[0].Text())
			} else {
				v, err = s.currval(ctx, txn, args[0].Text())
			}
			if err != nil {
				return e, err
			}
			d = types.NewInt(v)
		case "lastval":
			v, err := s.lastvalFn()
			if err != nil {
				return e, err
			}
			d = types.NewInt(v)
		case "setval":
			if len(args) < 2 || len(args) > 3 {
				return e, newErrf(CodeSyntaxError, "setval() takes two or three arguments")
			}
			n, err := args[1].Coerce(types.Int)
			if err != nil {
				return e, newErrf(CodeInvalidTextRepresentation, "setval: %v", err)
			}
			called := true
			if len(args) == 3 {
				b, err := args[2].Coerce(types.Bool)
				if err != nil {
					return e, newErrf(CodeInvalidTextRepresentation, "setval: %v", err)
				}
				called = b.B
			}
			v, err := s.setval(ctx, txn, args[0].Text(), n.I, called)
			if err != nil {
				return e, err
			}
			d = types.NewInt(v)
		case "unique_rowid":
			d = types.NewInt(s.uniqueRowID())
		case "gen_random_uuid":
			d = types.NewUUID(uuid.New())
		}
		out.Func, out.Args, out.Lit = "", nil, &d
		return out, nil
	}
	var err error
	if e.Left != nil {
		l, err := s.spliceVolatile(ctx, txn, *e.Left, params)
		if err != nil {
			return e, err
		}
		out.Left = &l
	}
	if e.Right != nil {
		r, err := s.spliceVolatile(ctx, txn, *e.Right, params)
		if err != nil {
			return e, err
		}
		out.Right = &r
	}
	if len(e.Args) > 0 {
		out.Args = make([]parser.Expr, len(e.Args))
		for i, a := range e.Args {
			if out.Args[i], err = s.spliceVolatile(ctx, txn, a, params); err != nil {
				return e, err
			}
		}
	}
	return out, nil
}

// exprIsVolatile reports whether e calls a volatile row function.
func exprIsVolatile(e parser.Expr) bool {
	return exprHas(e, func(x parser.Expr) bool { return x.Func != "" && volatileFuncs[x.Func] })
}

// columnDefaults holds a statement's parsed expression defaults, with
// the once-per-statement parts (now(), current_user) already spliced.
type columnDefaults struct {
	exprs map[catalog.ColumnID]parser.Expr
}

// prepareDefaults parses every expression default of desc for one
// statement (nil when the table has none).
func (s *Session) prepareDefaults(ctx context.Context, txn *kvclient.Txn, desc *catalog.TableDescriptor, params []types.Datum) (*columnDefaults, error) {
	var cd *columnDefaults
	for _, c := range desc.Columns {
		if c.DefaultExpr == "" {
			continue
		}
		e, err := parser.ParseExpr(c.DefaultExpr)
		if err != nil {
			return nil, newErrf(CodeInternal, "column %q: default expression %q: %v", c.Name, c.DefaultExpr, err)
		}
		if e, err = s.resolveValueExprOpts(ctx, txn, e, params, true); err != nil {
			return nil, err
		}
		if cd == nil {
			cd = &columnDefaults{exprs: map[catalog.ColumnID]parser.Expr{}}
		}
		cd.exprs[c.ID] = e
	}
	return cd, nil
}

// value evaluates column col's default for one row: the expression
// default (volatile functions per row), else the constant, else NULL.
func (s *Session) defaultValue(ctx context.Context, txn *kvclient.Txn, cd *columnDefaults, col *catalog.Column, params []types.Datum) (types.Datum, error) {
	if cd != nil {
		if e, ok := cd.exprs[col.ID]; ok {
			e, err := s.spliceVolatile(ctx, txn, e, params)
			if err != nil {
				return types.Datum{}, err
			}
			d, err := evalExpr(e, nil, nil, params)
			if err != nil {
				return types.Datum{}, err
			}
			d, cerr := d.Coerce(col.Type)
			if cerr != nil {
				return types.Datum{}, newErrf(CodeInvalidTextRepresentation, "default for column %q: %v", col.Name, cerr)
			}
			return d, nil
		}
	}
	if col.Default != nil {
		return *col.Default, nil
	}
	return types.DNull, nil
}

// expandDefaults widens one insert row to every column carrying an
// expression default that the statement did not supply (or supplied as
// DEFAULT), evaluating those defaults for this row. Constant defaults
// are left to buildInsertRow.
func (s *Session) expandDefaults(ctx context.Context, txn *kvclient.Txn, desc *catalog.TableDescriptor, cd *columnDefaults, target []catalog.Column, vals []types.Datum, params []types.Datum) ([]catalog.Column, []types.Datum, error) {
	if cd == nil {
		return target, vals, nil
	}
	given := map[catalog.ColumnID]bool{}
	for _, c := range target {
		given[c.ID] = true
	}
	outT, outV := target, vals
	copied := false
	for i := range desc.Columns {
		c := &desc.Columns[i]
		if given[c.ID] || c.DefaultExpr == "" {
			continue
		}
		d, err := s.defaultValue(ctx, txn, cd, c, params)
		if err != nil {
			return nil, nil, err
		}
		if !copied {
			outT = append([]catalog.Column(nil), target...)
			outV = append([]types.Datum(nil), vals...)
			copied = true
		}
		outT = append(outT, *c)
		outV = append(outV, d)
	}
	return outT, outV, nil
}

// sequenceNameFor renders a column's owned sequence, for the catalogs.
func (s *Session) sequenceNameFor(ctx context.Context, txn *kvclient.Txn, id uint64) string {
	d, err := catalog.ReadSequence(ctx, txn, id)
	if err != nil {
		return strconv.FormatUint(id, 10)
	}
	return d.Name
}
