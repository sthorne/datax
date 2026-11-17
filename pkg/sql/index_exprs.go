package sql

import (
	"encoding/json"
	"reflect"
	"strconv"
	"strings"
	"sync"

	"github.com/sthorne/datax/pkg/sql/builtins"
	"github.com/sthorne/datax/pkg/sql/catalog"
	"github.com/sthorne/datax/pkg/sql/parser"
	"github.com/sthorne/datax/pkg/sql/rowenc"
	"github.com/sthorne/datax/pkg/sql/types"
)

// Expression keys and partial indexes (issues #358, #359).
//
// An index key may be an expression over the row's columns
// ((lower(email)), (b + 1)), and an index may have a predicate (CREATE
// INDEX ... WHERE deleted_at IS NULL): only the rows it is true for have
// entries, and a unique index's rule holds among them only. The
// descriptor keeps both as SQL text (catalog.IndexDescriptor.Exprs,
// Predicate). Every encoder of an index's entries — the write path, the
// online backfill and the in-block build, a re-shard's copy — goes
// through rowenc, which asks computeIndexKeys below for the values, so
// none of them needs to know which kind of index it holds.
//
// What either may hold is what PostgreSQL allows, within what datax can
// show it is immutable: the table's own columns, constants, operators
// and casts that do not read the session's TimeZone, and builtins
// registered as immutable (builtins.Builtin.Vol). Subqueries,
// aggregates, window functions, parameters and anything stable or
// volatile are refused with PostgreSQL's errors.

func init() {
	rowenc.SetKeyComputer(computeIndexKeys)
}

// stableInPostgres are builtins datax evaluates without the session but
// PostgreSQL marks STABLE (their result goes through the output
// functions of their arguments' types). An index on one is refused in
// PostgreSQL, so it is here too.
var stableInPostgres = map[string]bool{
	"concat": true, "concat_ws": true, "format": true, "array_to_string": true,
	"to_json": true, "to_jsonb": true, "row_to_json": true,
	"json_build_object": true, "jsonb_build_object": true, "json_build_array": true, "jsonb_build_array": true,
}

// immutableFunc reports whether an index may call fn.
func immutableFunc(fn string) bool {
	if stableInPostgres[fn] {
		return false
	}
	b, ok := builtins.Lookup(fn)
	return ok && b.Vol == builtins.Immutable && !b.Session && !b.ZoneSensitive() && b.Rows == nil && b.Fn != nil
}

// temporalFamily: a value whose reading, rendering or arithmetic can
// depend on the session's TimeZone or DateStyle.
func temporalFamily(f types.Family) bool {
	switch f {
	case types.Timestamp, types.Date, types.Time, types.IntervalFam:
		return true
	}
	return false
}

// indexExprChecker validates the expressions of one CREATE INDEX: what
// names the part being checked in PostgreSQL's messages ("index
// expression" or "index predicate").
type indexExprChecker struct {
	desc *catalog.TableDescriptor
	what string
	// expr is true for a key expression, which must also be one that
	// parser.FormatExpr renders faithfully (it is stored as that text).
	expr bool
}

func (c *indexExprChecker) colType(name string) (types.Family, bool) {
	col, ok := c.desc.Col(name)
	if !ok || col.Hidden {
		return types.Unknown, false
	}
	return col.Type, true
}

// check walks one value expression.
func (c *indexExprChecker) check(e parser.Expr) error {
	plural := c.what + "s"
	switch {
	case e.Sub != nil:
		return newErrf(CodeFeatureNotSupported, "cannot use subquery in %s", c.what)
	case e.Agg != nil:
		return newErrf(CodeGrouping, "aggregate functions are not allowed in %s", plural)
	case e.Window != nil:
		return newErrf(CodeWindowing, "window functions are not allowed in %s", plural)
	case e.Param > 0:
		return newErrf(CodeUndefinedParameter, "there is no parameter $%d", e.Param)
	case e.IsDefault:
		return newErrf(CodeSyntaxError, "DEFAULT is not allowed in %s", c.what)
	case e.Column != "":
		if _, ok := c.colType(e.Column); !ok {
			if strings.Contains(e.Column, ".") {
				return newErrf(CodeUndefinedColumn, "column %q: an index names the table's columns without a qualifier", e.Column)
			}
			return newErrf(CodeUndefinedColumn, "column %q does not exist", e.Column)
		}
	case e.Func != "":
		if b, ok := builtins.Lookup(e.Func); ok && b.Rows != nil {
			return newErrf(CodeFeatureNotSupported, "set-returning functions are not allowed in %s", plural)
		}
		if !immutableFunc(e.Func) {
			return newErrf(CodeInvalidObjectDefinition, "functions in %s must be marked IMMUTABLE", c.what)
		}
	}
	if c.expr && (e.Case != nil || e.Cmp != nil) {
		return newErrf(CodeFeatureNotSupported, "CASE and comparisons are not supported in an index expression (index a column, a function call or arithmetic)")
	}
	if e.Lit != nil && e.Lit.Loc != nil {
		return newErrf(CodeInvalidObjectDefinition, "functions in %s must be marked IMMUTABLE", c.what)
	}
	for _, sub := range []*parser.Expr{e.Left, e.Right} {
		if sub != nil {
			if err := c.check(*sub); err != nil {
				return err
			}
		}
	}
	for _, a := range e.Args {
		if err := c.check(a); err != nil {
			return err
		}
	}
	if e.Case != nil {
		if e.Case.Operand != nil {
			if err := c.check(*e.Case.Operand); err != nil {
				return err
			}
		}
		for _, w := range e.Case.Whens {
			if w.Value != nil {
				if err := c.check(*w.Value); err != nil {
					return err
				}
			}
			for _, cond := range w.Cond {
				if err := c.checkCond(cond); err != nil {
					return err
				}
			}
			if err := c.check(w.Result); err != nil {
				return err
			}
		}
		if e.Case.Else != nil {
			if err := c.check(*e.Case.Else); err != nil {
				return err
			}
		}
	}
	if e.Cmp != nil {
		if err := c.checkCond(*e.Cmp); err != nil {
			return err
		}
	}
	// What reads the session's TimeZone or DateStyle: a cast or an
	// operator with a date/time operand or result (timestamptz +
	// interval, created_at::date, created_at::text).
	if e.Cast != "" || e.BinOp != "" {
		inner := e
		inner.Cast = ""
		if e.Cast != "" && strings.EqualFold(e.Cast, "regclass") {
			return newErrf(CodeInvalidObjectDefinition, "functions in %s must be marked IMMUTABLE", c.what)
		}
		var fams []types.Family
		if e.BinOp != "" {
			left := inner
			left.BinOp, left.Right = "", nil
			if e.Left != nil {
				left = *e.Left
			}
			fams = append(fams, exprFamily(left, c.colType))
			if e.Right != nil {
				fams = append(fams, exprFamily(*e.Right, c.colType))
			}
		}
		fams = append(fams, exprFamily(inner, c.colType))
		if e.Cast != "" {
			fams = append(fams, builtins.CastFamily(e.Cast, types.Unknown))
		}
		for _, f := range fams {
			if temporalFamily(f) {
				return newErrf(CodeInvalidObjectDefinition, "functions in %s must be marked IMMUTABLE", c.what)
			}
		}
	}
	return nil
}

// checkCond walks one conjunct of a predicate (or of a CASE arm).
func (c *indexExprChecker) checkCond(cond parser.Comparison) error {
	if cond.Sub != nil {
		return newErrf(CodeFeatureNotSupported, "cannot use subquery in %s", c.what)
	}
	if cond.Column != "" {
		fam, ok := c.colType(cond.Column)
		if !ok {
			if strings.Contains(cond.Column, ".") {
				return newErrf(CodeUndefinedColumn, "column %q: an index names the table's columns without a qualifier", cond.Column)
			}
			return newErrf(CodeUndefinedColumn, "column %q does not exist", cond.Column)
		}
		if temporalFamily(fam) && cond.Op != "IS NULL" && cond.Op != "IS NOT NULL" {
			// created_at > '2024-01-01': the literal is read in the
			// session's TimeZone when the row is checked. PostgreSQL
			// folds it into a constant when the index is created;
			// datax would read it again on every write, so it is
			// refused rather than depend on the writer's session.
			return newErrf(CodeInvalidObjectDefinition, "functions in %s must be marked IMMUTABLE (a comparison of date/time column %q with a value reads the session's TimeZone; only IS [NOT] NULL is supported on it)", c.what, cond.Column)
		}
	}
	if cond.Expr != nil {
		if err := c.check(*cond.Expr); err != nil {
			return err
		}
		if temporalFamily(exprFamily(*cond.Expr, c.colType)) {
			return newErrf(CodeInvalidObjectDefinition, "functions in %s must be marked IMMUTABLE", c.what)
		}
	}
	switch cond.Op {
	case "EXISTS", "NOT EXISTS":
		return newErrf(CodeFeatureNotSupported, "cannot use subquery in %s", c.what)
	}
	if err := c.check(cond.Value); err != nil {
		return err
	}
	for _, v := range cond.Values {
		if err := c.check(v); err != nil {
			return err
		}
	}
	for _, d := range cond.Or {
		for _, inner := range d {
			if err := c.checkCond(inner); err != nil {
				return err
			}
		}
	}
	return nil
}

// indexKeyExpr validates expression key e of CREATE INDEX on desc and
// returns the text the descriptor stores and its result family.
func indexKeyExpr(desc *catalog.TableDescriptor, e parser.Expr) (string, types.Family, error) {
	c := &indexExprChecker{desc: desc, what: "index expression", expr: true}
	if err := c.check(e); err != nil {
		return "", 0, err
	}
	e = stripTextCasts(desc, e)
	text := parser.FormatExpr(e)
	// The descriptor keeps the text, so the text must say exactly what
	// was written: parsed back, it is the same expression.
	back, err := parser.ParseExpr(text)
	if err != nil || !sameExpr(back, e) {
		return "", 0, newErrf(CodeFeatureNotSupported, "index expression %s is not supported (index a column, a function call, a JSON path or arithmetic)", text)
	}
	fam := exprFamily(e, c.colType)
	if fam == types.Unknown {
		return "", 0, newErrf(CodeFeatureNotSupported, "could not determine the type of index expression %s", text)
	}
	if !types.IsIndexable(fam) {
		return "", 0, newErrf(CodeFeatureNotSupported, "index expression %s of type %s cannot be indexed (no ordered key encoding)", text, fam)
	}
	return text, fam, nil
}

// sameExpr compares two parsed expressions, leaving out what the
// session stamps on them (the TimeZone).
func sameExpr(a, b parser.Expr) bool {
	ja, err1 := json.Marshal(a)
	jb, err2 := json.Marshal(b)
	return err1 == nil && err2 == nil && string(ja) == string(jb)
}

// checkIndexPredicate validates a partial index's WHERE clause.
func checkIndexPredicate(desc *catalog.TableDescriptor, text string) error {
	conds, err := parser.ParsePolicyExpr(text)
	if err != nil {
		return newErrf(CodeSyntaxError, "index predicate: %v", err)
	}
	c := &indexExprChecker{desc: desc, what: "index predicate"}
	for _, cond := range conds {
		if err := c.checkCond(cond); err != nil {
			return err
		}
	}
	return nil
}

// compiledIndex is an index's stored expressions, parsed.
type compiledIndex struct {
	exprs []*parser.Expr // parallel to the key columns; nil for a column
	pred  []parser.Comparison
	err   error
}

// compiledIndexes caches the parse of each distinct (expressions,
// predicate) text. The texts name columns, so the parse is the same for
// every table and descriptor version that has them.
var compiledIndexes sync.Map // string → *compiledIndex

func compileIndex(idx *catalog.IndexDescriptor) *compiledIndex {
	key := idx.Predicate + "\x00" + strings.Join(idx.Exprs, "\x00")
	if c, ok := compiledIndexes.Load(key); ok {
		return c.(*compiledIndex)
	}
	c := &compiledIndex{}
	for i := range idx.ColumnIDs {
		if !idx.IsExprKey(i) {
			c.exprs = append(c.exprs, nil)
			continue
		}
		e, err := parser.ParseExpr(idx.Exprs[i])
		if err != nil {
			c.err = newErrf(CodeInternal, "index %q: stored expression %q: %v", idx.Name, idx.Exprs[i], err)
			break
		}
		c.exprs = append(c.exprs, &e)
	}
	if c.err == nil && idx.Predicate != "" {
		pred, err := parser.ParsePolicyExpr(idx.Predicate)
		if err != nil {
			c.err = newErrf(CodeInternal, "index %q: stored predicate %q: %v", idx.Name, idx.Predicate, err)
		}
		c.pred = pred
	}
	if c.err == nil {
		compiledIndexes.Store(key, c)
	}
	return c
}

// computeIndexKeys is rowenc's KeyComputer: the key values of a
// computed index for row, and whether the row satisfies its predicate.
func computeIndexKeys(desc *catalog.TableDescriptor, idx *catalog.IndexDescriptor, row map[catalog.ColumnID]types.Datum) ([]types.Datum, bool, error) {
	c := compileIndex(idx)
	if c.err != nil {
		return nil, false, c.err
	}
	if c.pred != nil {
		ok, err := matchesWhere(c.pred, desc, row, nil)
		if err != nil || !ok {
			return nil, false, err
		}
	}
	vals := make([]types.Datum, len(idx.ColumnIDs))
	for i, colID := range idx.ColumnIDs {
		if c.exprs[i] == nil {
			d, ok := row[colID]
			if !ok {
				d = types.DNull
			}
			vals[i] = d
			continue
		}
		d, err := evalExpr(*c.exprs[i], desc, row, nil)
		if err != nil {
			return nil, false, err
		}
		if !d.Null && i < len(idx.ExprTypes) && d.Fam != idx.ExprTypes[i] {
			if d, err = d.Coerce(idx.ExprTypes[i]); err != nil {
				return nil, false, newErrf(CodeInternal, "index %q: expression %s: %v", idx.Name, idx.Exprs[i], err)
			}
		}
		vals[i] = d
	}
	return vals, true, nil
}

// indexPredicate is a partial index's predicate, parsed (nil for an
// index without one, or one whose stored text does not parse).
func indexPredicate(idx *catalog.IndexDescriptor) ([]parser.Comparison, bool) {
	if idx.Predicate == "" {
		return nil, true
	}
	c := compileIndex(idx)
	if c.err != nil {
		return nil, false
	}
	return c.pred, true
}

// predicateImplied reports whether every row the WHERE clause can
// return satisfies the index's predicate, so that the index — which has
// no entry for the others — is a complete source of them. The test is
// conservative: each conjunct of the predicate must appear among the
// WHERE clause's conjuncts as it is (deleted_at IS NULL, status =
// 'active'), compared structurally. A missed proof only costs the
// index; a wrong one would lose rows, so anything else is no.
func predicateImplied(desc *catalog.TableDescriptor, idx *catalog.IndexDescriptor, where []parser.Comparison) bool {
	pred, ok := indexPredicate(idx)
	if !ok {
		return false
	}
	for _, p := range pred {
		found := false
		for _, w := range where {
			if sameCond(desc, p, w) {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	return true
}

// sameCond compares a predicate conjunct with a WHERE conjunct. The
// session marks a WHERE clause's literals and operators with its
// TimeZone when it is not UTC; that changes nothing for a conjunct over
// columns that are not dates or times (the only kind a predicate may
// compare with a value), so the marks are cleared before comparing.
func sameCond(desc *catalog.TableDescriptor, p, w parser.Comparison) bool {
	if reflect.DeepEqual(p, w) {
		return true
	}
	p, w = stripCondTextCasts(desc, p), stripCondTextCasts(desc, w)
	ja, err1 := json.Marshal(clearZones(desc, p))
	jb, err2 := json.Marshal(clearZones(desc, w))
	return err1 == nil && err2 == nil && string(ja) == string(jb)
}

// clearZones returns c with every literal's TimeZone cleared — or c
// itself, unless every column it names is of a family the zone does not
// affect (Expr.Zone is never encoded; Datum.Loc is).
func clearZones(desc *catalog.TableDescriptor, c parser.Comparison) parser.Comparison {
	safe := true
	condColumns(c, func(name string) {
		col, ok := desc.Col(name)
		if !ok || temporalFamily(col.Type) {
			safe = false
		}
	})
	if !safe {
		return c
	}
	out := c
	out.Value = clearExprZones(c.Value)
	if c.Expr != nil {
		e := clearExprZones(*c.Expr)
		out.Expr = &e
	}
	if c.Values != nil {
		out.Values = make([]parser.Expr, len(c.Values))
		for i, v := range c.Values {
			out.Values[i] = clearExprZones(v)
		}
	}
	if c.Or != nil {
		out.Or = make([][]parser.Comparison, len(c.Or))
		for i, d := range c.Or {
			out.Or[i] = make([]parser.Comparison, len(d))
			for j, inner := range d {
				out.Or[i][j] = clearZones(desc, inner)
			}
		}
	}
	return out
}

func clearExprZones(e parser.Expr) parser.Expr {
	if e.Lit != nil && e.Lit.Loc != nil {
		d := *e.Lit
		d.Loc = nil
		e.Lit = &d
	}
	e.Zone = nil
	if e.Left != nil {
		l := clearExprZones(*e.Left)
		e.Left = &l
	}
	if e.Right != nil {
		r := clearExprZones(*e.Right)
		e.Right = &r
	}
	if e.Args != nil {
		args := make([]parser.Expr, len(e.Args))
		for i, a := range e.Args {
			args[i] = clearExprZones(a)
		}
		e.Args = args
	}
	return e
}

// indexColumnUses reports whether the index reads column col: as a key
// column, or in an expression key or its predicate.
func indexColumnUses(idx *catalog.IndexDescriptor, col catalog.Column) bool {
	for _, id := range idx.ColumnIDs {
		if id == col.ID {
			return true
		}
	}
	if !idx.Computed() {
		return false
	}
	c := compileIndex(idx)
	if c.err != nil {
		return true // unreadable: assume it does
	}
	uses := false
	visit := func(name string) {
		if name == col.Name {
			uses = true
		}
	}
	for _, e := range c.exprs {
		if e != nil {
			exprColumns(*e, visit)
		}
	}
	for _, p := range c.pred {
		condColumns(p, visit)
	}
	return uses
}

// renameIndexColumnRefs rewrites the stored expressions and predicate of
// desc's indexes for ALTER TABLE ... RENAME COLUMN from TO to.
func renameIndexColumnRefs(desc *catalog.TableDescriptor, col catalog.Column, to string) error {
	for i := range desc.Indexes {
		idx := &desc.Indexes[i]
		if !idx.Computed() || !indexColumnUses(idx, col) {
			continue
		}
		for j, text := range idx.Exprs {
			if text == "" {
				continue
			}
			renamed, err := parser.RenameColumnRefs(text, col.Name, to)
			if err != nil {
				return newErrf(CodeInternal, "index %q: %v", idx.Name, err)
			}
			idx.Exprs[j] = renamed
		}
		if idx.Predicate != "" {
			renamed, err := parser.RenameColumnRefs(idx.Predicate, col.Name, to)
			if err != nil {
				return newErrf(CodeInternal, "index %q: %v", idx.Name, err)
			}
			idx.Predicate = renamed
		}
	}
	return nil
}

// colSlot and exprSlot name what a planner conjunct constrains, so it can be
// matched with an index's key column: a column by ID, an expression by
// its text.
func colSlot(id catalog.ColumnID) string { return "c" + strconv.FormatUint(uint64(id), 10) }

func exprSlot(text string) string { return "e" + text }

// indexSlots is the slot of each key column of an index.
func indexSlots(idx *catalog.IndexDescriptor) []string {
	out := make([]string, len(idx.ColumnIDs))
	for i, id := range idx.ColumnIDs {
		if idx.IsExprKey(i) {
			out[i] = exprSlot(idx.Exprs[i])
		} else {
			out[i] = colSlot(id)
		}
	}
	return out
}

// exprKeysIn reports whether any index of desc has an expression key
// (the planner looks at computed conjuncts only then).
func exprKeysIn(desc *catalog.TableDescriptor) bool {
	for i := range desc.Indexes {
		if desc.Indexes[i].HasExprKeys() {
			return true
		}
	}
	return false
}

// stripTextCasts removes the casts to text that change nothing — of a
// string literal, a TEXT or VARCHAR column, or a call that returns text
// — so that what pg_dump writes for an index on a VARCHAR column,
// lower((email)::text), and what a query writes, lower(email), are one
// expression. A CHAR(n) column's cast is kept: it drops the padding.
func stripTextCasts(desc *catalog.TableDescriptor, e parser.Expr) parser.Expr {
	if e.Left != nil {
		l := stripTextCasts(desc, *e.Left)
		e.Left = &l
	}
	if e.Right != nil {
		r := stripTextCasts(desc, *e.Right)
		e.Right = &r
	}
	if e.Args != nil {
		args := make([]parser.Expr, len(e.Args))
		for i, a := range e.Args {
			args[i] = stripTextCasts(desc, a)
		}
		e.Args = args
	}
	if e.Cast == "" || e.BinOp != "" || e.Left != nil || len(e.Path) > 0 {
		return e
	}
	switch strings.ToLower(strings.TrimSpace(e.Cast)) {
	case "text", "varchar", "character varying", "pg_catalog.text":
	default:
		return e
	}
	noop := false
	switch {
	case e.Lit != nil:
		noop = !e.Lit.Null && e.Lit.Fam == types.String
	case e.Column != "":
		col, ok := desc.Col(e.Column)
		noop = ok && col.Type == types.String && !col.Char
	case e.Func != "":
		if b, ok := builtins.Lookup(e.Func); ok {
			noop = b.Ret == types.String
		}
	}
	if noop {
		e.Cast = ""
	}
	return e
}

// stripCondTextCasts is stripTextCasts over a conjunct's expressions.
func stripCondTextCasts(desc *catalog.TableDescriptor, c parser.Comparison) parser.Comparison {
	c.Value = stripTextCasts(desc, c.Value)
	if c.Expr != nil {
		e := stripTextCasts(desc, *c.Expr)
		if isBareColumn(e) {
			// (status)::text = 'x' is status = 'x'.
			c.Column, c.Path, c.Expr = e.Column, e.Path, nil
		} else {
			c.Expr = &e
		}
	}
	if c.Values != nil {
		vals := make([]parser.Expr, len(c.Values))
		for i, v := range c.Values {
			vals[i] = stripTextCasts(desc, v)
		}
		c.Values = vals
	}
	if c.Or != nil {
		or := make([][]parser.Comparison, len(c.Or))
		for i, d := range c.Or {
			or[i] = make([]parser.Comparison, len(d))
			for j, inner := range d {
				or[i][j] = stripCondTextCasts(desc, inner)
			}
		}
		c.Or = or
	}
	return c
}

// isBareColumn: e is a column reference and nothing else.
func isBareColumn(e parser.Expr) bool {
	return e.Column != "" && e.Cast == "" && e.BinOp == "" && e.Left == nil && e.Func == "" && e.Lit == nil && e.Case == nil && e.Cmp == nil && e.Sub == nil && len(e.Path) == 0
}
