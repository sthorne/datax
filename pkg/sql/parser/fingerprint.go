package parser

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sort"
	"strings"
)

// Statement fingerprints (issue #157).
//
// The question that drives optimisation work is "which statement shape
// costs this cluster the most", and it cannot be answered by a
// slow-statement ring: a statement that takes 8ms and runs forty
// thousand times an hour never appears in one, and is usually the thing
// worth fixing. Answering it means grouping executions by shape, which
// means normalising a statement to that shape.
//
// The normalisation walks the parsed statement rather than the text it
// came from. That is the difference between correct and approximately
// correct: a lexer cannot tell a literal from an identifier that looks
// like one, cannot see through a comment or a cast, and cannot tell that
// two texts differing in whitespace, keyword case and parenthesisation
// are the same query. The AST can, because the parser already did that
// work.
//
// The output is deliberately readable — "UPDATE accounts SET balance = ?
// WHERE id = ?" rather than an opaque digest — because it is what the
// console shows in a list; the hash exists to key the accounting.

// Shape is one statement's fingerprint.
type Shape struct {
	// Text is the normalised statement: the same for every execution of
	// one shape, with values replaced by "?".
	Text string
	// Hash is a stable digest of Text, the key the accounting is under.
	Hash string
	// Tables are the relations the statement names, deduplicated and
	// sorted — what the shape touches, for the console to group by.
	Tables []string
}

// Fingerprint normalises a parsed statement to its shape.
//
// Statement kinds whose shape does not vary meaningfully — DDL, session
// and transaction control — normalise to a kind and their subject
// rather than to a full rendering: two CREATE TABLEs are not
// interesting as "the same shape", and nobody optimises a COMMIT.
func Fingerprint(stmt Statement) Shape {
	f := &fingerprinter{}
	f.stmt(stmt)
	text := strings.TrimSpace(f.b.String())
	sum := sha256.Sum256([]byte(text))
	tables := f.tableList()
	return Shape{Text: text, Hash: hex.EncodeToString(sum[:8]), Tables: tables}
}

type fingerprinter struct {
	b      strings.Builder
	tables map[string]struct{}
	// depth guards against a pathological nesting depth turning a
	// fingerprint into an unbounded walk. The parser bounds recursion at
	// parse time (issue #135), so this is a second belt rather than the
	// only one.
	depth int
}

const fingerprintMaxDepth = 100

func (f *fingerprinter) tableList() []string {
	if len(f.tables) == 0 {
		return nil
	}
	out := make([]string, 0, len(f.tables))
	for t := range f.tables {
		out = append(out, t)
	}
	sort.Strings(out)
	return out
}

func (f *fingerprinter) table(name string) {
	if name == "" {
		return
	}
	if f.tables == nil {
		f.tables = map[string]struct{}{}
	}
	f.tables[strings.ToLower(name)] = struct{}{}
}

func (f *fingerprinter) w(s string) { f.b.WriteString(s) }

func (f *fingerprinter) sp() {
	if f.b.Len() > 0 && !strings.HasSuffix(f.b.String(), " ") && !strings.HasSuffix(f.b.String(), "(") {
		f.b.WriteByte(' ')
	}
}

func (f *fingerprinter) kw(s string) { f.sp(); f.w(s) }

func (f *fingerprinter) stmt(s Statement) {
	if f.depth > fingerprintMaxDepth {
		f.kw("…")
		return
	}
	f.depth++
	defer func() { f.depth-- }()
	switch t := s.(type) {
	case *Select:
		f.selectStmt(t)
	case *Insert:
		f.insert(t)
	case *Update:
		f.update(t)
	case *Delete:
		f.delete(t)
	case *Explain:
		f.kw("EXPLAIN")
		if t.Analyze {
			f.kw("ANALYZE")
		}
		f.stmt(t.Stmt)
	default:
		// Everything else is named by kind and subject. These are not
		// shapes anyone optimises, and rendering each of them in full
		// would be a second copy of the grammar.
		f.kw(strings.ToUpper(statementKindOf(s)))
		if name := statementSubject(s); name != "" {
			f.table(name)
			f.kw(name)
		}
	}
}

func (f *fingerprinter) selectStmt(s *Select) {
	if s == nil {
		return
	}
	for i, c := range s.With {
		if i == 0 {
			f.kw("WITH")
		} else {
			f.w(",")
		}
		f.kw(c.Name)
		f.kw("AS (")
		f.stmt(c.Query)
		f.w(")")
	}
	f.kw("SELECT")
	if s.Distinct {
		f.kw("DISTINCT")
	}
	for i, e := range s.Exprs {
		if i > 0 {
			f.w(",")
		}
		f.sp()
		f.selectExpr(e)
	}
	if s.Table != "" {
		f.table(s.Table)
		f.kw("FROM")
		f.kw(strings.ToLower(s.Table))
	} else if s.Derived != nil {
		f.kw("FROM (")
		f.selectStmt(s.Derived)
		f.w(")")
	} else if s.FuncTable != nil {
		f.kw("FROM")
		f.sp()
		f.expr(*s.FuncTable)
	}
	for _, j := range s.Joins {
		f.join(j)
	}
	f.where(s.Where)
	if len(s.GroupBy) > 0 {
		f.kw("GROUP BY")
		f.kw(strings.ToLower(strings.Join(s.GroupBy, ", ")))
	}
	if len(s.Having) > 0 {
		f.kw("HAVING")
		for i, h := range s.Having {
			if i > 0 {
				f.kw("AND")
			}
			f.sp()
			if h.Agg != nil {
				f.selectExpr(*h.Agg)
			} else {
				f.w(strings.ToLower(h.Column))
			}
			f.kw(strings.ToUpper(h.Op))
			f.sp()
			f.expr(h.Value)
		}
	}
	f.orderBy(s.OrderBy)
	// The presence of LIMIT/OFFSET is a shape; the number is a value.
	if s.Limit >= 0 || s.LimitParam > 0 {
		f.kw("LIMIT ?")
	}
	if s.Offset > 0 || s.OffsetParam > 0 {
		f.kw("OFFSET ?")
	}
	if s.ForUpdate {
		f.kw("FOR UPDATE")
	}
	if s.AsOf != "" || s.AsOfMaxStaleness != "" {
		f.kw("AS OF SYSTEM TIME ?")
	}
	if s.Union != nil {
		op := s.SetOp
		if op == "" {
			op = "UNION"
		}
		f.kw(op)
		if s.UnionAll {
			f.kw("ALL")
		}
		f.selectStmt(s.Union)
	}
}

func (f *fingerprinter) join(j JoinClause) {
	switch {
	case j.Cross:
		f.kw("CROSS")
	case j.Full:
		f.kw("FULL")
	case j.Right:
		f.kw("RIGHT")
	case j.Left:
		f.kw("LEFT")
	}
	if j.Natural {
		f.kw("NATURAL")
	}
	f.kw("JOIN")
	if j.Table != "" {
		f.table(j.Table)
		f.kw(strings.ToLower(j.Table))
	} else if j.Derived != nil {
		f.kw("(")
		f.selectStmt(j.Derived)
		f.w(")")
	}
	if len(j.Using) > 0 {
		f.kw("USING (" + strings.ToLower(strings.Join(j.Using, ", ")) + ")")
	}
	if len(j.On) > 0 || len(j.Filter) > 0 {
		f.kw("ON")
		for i, c := range j.On {
			if i > 0 {
				f.kw("AND")
			}
			f.kw(colRef(c.L) + " = " + colRef(c.R))
		}
		if len(j.Filter) > 0 {
			if len(j.On) > 0 {
				f.kw("AND")
			}
			f.comparisons(j.Filter)
		}
	}
}

// colRef renders a possibly-qualified column reference.
func colRef(c ColumnRef) string {
	if c.Table != "" {
		return strings.ToLower(c.Table + "." + c.Column)
	}
	return strings.ToLower(c.Column)
}

func (f *fingerprinter) where(cs []Comparison) {
	if len(cs) == 0 {
		return
	}
	f.kw("WHERE")
	f.comparisons(cs)
}

func (f *fingerprinter) comparisons(cs []Comparison) {
	for i, c := range cs {
		if i > 0 {
			f.kw("AND")
		}
		f.comparison(c)
	}
}

func (f *fingerprinter) comparison(c Comparison) {
	if f.depth > fingerprintMaxDepth {
		f.kw("…")
		return
	}
	f.depth++
	defer func() { f.depth-- }()
	if c.Op == "OR" {
		f.kw("(")
		for i, group := range c.Or {
			if i > 0 {
				f.kw("OR")
			}
			f.kw("(")
			f.comparisons(group)
			f.w(")")
		}
		f.w(")")
		return
	}
	switch {
	case c.Expr != nil:
		f.sp()
		f.expr(*c.Expr)
	case c.Column != "":
		f.kw(strings.ToLower(c.Column))
		for range c.Path {
			f.w("->?")
		}
	}
	f.kw(strings.ToUpper(c.Op))
	switch {
	case strings.HasSuffix(c.Op, "NULL"), c.Op == "TRUE", c.Op == "FALSE":
		// The operator is the whole predicate.
	case c.Sub != nil:
		f.kw("(")
		f.selectStmt(c.Sub)
		f.w(")")
	case len(c.Values) > 0:
		// An IN list of three and an IN list of three hundred are one
		// shape: the length is a value, not a shape.
		f.kw("(?)")
	default:
		f.sp()
		f.expr(c.Value)
	}
}

func (f *fingerprinter) selectExpr(e SelectExpr) {
	if e.Star {
		f.w("*")
		return
	}
	if e.Agg != "" {
		f.w(strings.ToUpper(e.Agg) + "(")
		if e.AggDistinct {
			f.w("DISTINCT ")
		}
		switch {
		case e.AggStar:
			f.w("*")
		case e.AggCol != "":
			f.w(strings.ToLower(e.AggCol))
		case e.AggArg != nil:
			f.expr(*e.AggArg)
		}
		f.w(")")
		if e.Window != nil {
			f.w(" OVER ()")
		}
		return
	}
	f.expr(e.Expr)
}

func (f *fingerprinter) orderBy(cols []OrderCol) {
	if len(cols) == 0 {
		return
	}
	f.kw("ORDER BY")
	for i, o := range cols {
		if i > 0 {
			f.w(",")
		}
		f.sp()
		switch {
		case o.Agg != nil:
			f.selectExpr(*o.Agg)
		case o.Column != "":
			f.w(strings.ToLower(o.Column))
		case o.Expr != nil:
			f.expr(*o.Expr)
		default:
			f.w("?")
		}
		if o.Desc {
			f.w(" DESC")
		}
	}
}

// expr renders a value expression with every literal and parameter
// replaced by "?": that substitution is the whole point, and it is done
// on the parsed value rather than on text that merely looks like one.
func (f *fingerprinter) expr(e Expr) {
	if f.depth > fingerprintMaxDepth {
		f.w("…")
		return
	}
	f.depth++
	defer func() { f.depth-- }()
	// A binary operation's left operand is Left when set, and otherwise
	// this node's own atom (balance - 7 parses as {Column: balance,
	// BinOp: -, Right: 7}). Missing that second form would render
	// "balance - ?" as "balance", collapsing every arithmetic shape on a
	// column into one — the sort of silent mis-grouping this whole
	// facility exists to avoid.
	if e.BinOp != "" && e.Right != nil {
		f.w("(")
		if e.Left != nil {
			f.expr(*e.Left)
		} else {
			f.atom(e)
		}
		f.w(" " + e.BinOp + " ")
		f.expr(*e.Right)
		f.w(")")
		if e.Cast != "" {
			f.w("::" + e.Cast)
		}
		return
	}
	if e.Left != nil {
		f.w("(")
		f.expr(*e.Left)
		f.w(")")
		if e.Cast != "" {
			f.w("::" + e.Cast)
		}
		return
	}
	f.atom(e)
	if e.Cast != "" {
		f.w("::" + e.Cast)
	}
}

// atom renders the value an expression node carries in its own right,
// ignoring any binary operator hanging off it.
func (f *fingerprinter) atom(e Expr) {
	switch {
	case e.Lit != nil, e.Param > 0, e.IsDefault:
		// A literal, a bound parameter and DEFAULT are all one thing to
		// a shape: a value.
		f.w("?")
	case e.Sub != nil:
		f.w("(")
		f.selectStmt(e.Sub)
		f.w(")")
	case e.Case != nil:
		f.w("CASE")
		for range e.Case.Whens {
			f.w(" WHEN ? THEN ?")
		}
		if e.Case.Else != nil {
			f.w(" ELSE ?")
		}
		f.w(" END")
	case e.Cmp != nil:
		f.w("(")
		f.comparison(*e.Cmp)
		f.w(")")
	case e.Func != "":
		f.w(strings.ToLower(e.Func) + "(")
		for i, a := range e.Args {
			if i > 0 {
				f.w(", ")
			}
			f.expr(a)
		}
		f.w(")")
	case e.Window != nil:
		f.selectExpr(*e.Window)
	case e.Column != "":
		f.w(strings.ToLower(e.Column))
		for range e.Path {
			f.w("->?")
		}
	default:
		f.w("?")
	}
}

func (f *fingerprinter) insert(s *Insert) {
	f.table(s.Table)
	if s.Upsert {
		f.kw("UPSERT INTO")
	} else {
		f.kw("INSERT INTO")
	}
	f.kw(strings.ToLower(s.Table))
	if len(s.Columns) > 0 {
		f.kw("(" + strings.ToLower(strings.Join(s.Columns, ", ")) + ")")
	}
	switch {
	case s.DefaultValues:
		f.kw("DEFAULT VALUES")
	case s.Select != nil:
		f.sp()
		f.selectStmt(s.Select)
	default:
		// One row of N values and a thousand rows of N values are one
		// shape: the row count is a value.
		f.kw("VALUES (")
		if len(s.Rows) > 0 {
			for i := range s.Rows[0] {
				if i > 0 {
					f.w(", ")
				}
				f.expr(s.Rows[0][i])
			}
		}
		f.w(")")
	}
	if s.OnConflict != nil {
		f.kw("ON CONFLICT")
		if len(s.OnConflict.Columns) > 0 {
			f.kw("(" + strings.ToLower(strings.Join(s.OnConflict.Columns, ", ")) + ")")
		}
		if s.OnConflict.DoNothing {
			f.kw("DO NOTHING")
		} else {
			f.kw("DO UPDATE SET")
			f.setClauses(s.OnConflict.Set)
		}
	}
	f.returning(s.Returning)
}

func (f *fingerprinter) update(s *Update) {
	f.table(s.Table)
	f.kw("UPDATE")
	f.kw(strings.ToLower(s.Table))
	f.kw("SET")
	f.setClauses(s.Set)
	f.where(s.Where)
	f.returning(s.Returning)
}

func (f *fingerprinter) delete(s *Delete) {
	f.table(s.Table)
	f.kw("DELETE FROM")
	f.kw(strings.ToLower(s.Table))
	f.where(s.Where)
	f.returning(s.Returning)
}

func (f *fingerprinter) setClauses(set []SetClause) {
	for i, c := range set {
		if i > 0 {
			f.w(",")
		}
		f.kw(strings.ToLower(c.Column))
		f.w(" = ")
		f.expr(c.Value)
	}
}

func (f *fingerprinter) returning(rs []SelectExpr) {
	if len(rs) == 0 {
		return
	}
	f.kw("RETURNING")
	for i, e := range rs {
		if i > 0 {
			f.w(",")
		}
		f.sp()
		f.selectExpr(e)
	}
}

// statementSubject names the object a non-DML statement acts on, so two
// CREATE TABLEs on different tables are different shapes.
func statementSubject(s Statement) string {
	switch t := s.(type) {
	case *CreateTable:
		return t.Name
	case *DropTable:
		return t.Name
	case *AlterTable:
		return t.Table
	case *CreateIndex:
		return t.Table
	case *DropIndex:
		return t.Name
	case *Truncate:
		if len(t.Tables) > 0 {
			return t.Tables[0]
		}
	case *CreateView:
		return t.Name
	case *DropView:
		if len(t.Names) > 0 {
			return t.Names[0]
		}
	case *CopyFrom:
		return t.Table
	}
	return ""
}

// statementKindOf names a statement for the fingerprint of kinds that
// are not rendered in full: the concrete type's name, which is already
// the SQL keyword in this parser ("CreateTable", "Commit").
func statementKindOf(s Statement) string {
	name := fmt.Sprintf("%T", s)
	if i := strings.LastIndex(name, "."); i >= 0 {
		name = name[i+1:]
	}
	return name
}
