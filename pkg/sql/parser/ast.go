// Package parser is datax's hand-rolled SQL lexer and recursive-descent
// parser for the v1 grammar subset (see docs/sql.md).
package parser

import "github.com/sthorne/datax/pkg/sql/types"

// Statement is any parsed SQL statement.
type Statement interface{ stmt() }

// PathStep is one JSONB path operator applied to a column reference:
// -> 'key' (Text=false, result stays jsonb) or ->> 'key' (Text=true,
// result rendered as text; terminal only).
type PathStep struct {
	Key  string `json:"key"`
	Text bool   `json:"text,omitempty"`
	// IsIndex marks -> n / ->> n: Index is an array position (negative
	// from the end).
	IsIndex bool `json:"is_index,omitempty"`
	Index   int  `json:"index,omitempty"`
	// Keys is a #> '{a,b}' / #>> '{a,b}' path: each key an object field
	// or, when numeric, an array position.
	Keys []string `json:"keys,omitempty"`
}

// Expr is a scalar expression: a literal, a parameter ($N), a column
// reference, a function call, or arithmetic over those. A single binary
// op keeps the flat historical shape (Column/Lit/Param on the node
// itself, operator, Right); chained or parenthesized arithmetic nests
// through Left.
type Expr struct {
	Lit    *types.Datum `json:"lit,omitempty"`
	Param  int          `json:"param,omitempty"` // 1-based; 0 = none
	Column string       `json:"column,omitempty"`
	// Path is the ->/->> chain applied to Column (JSONB extraction).
	Path []PathStep `json:"path,omitempty"`
	// Sub is a scalar subquery: (SELECT ...) used as a value. Uncorrelated
	// only — the executor evaluates it once and splices the result.
	Sub *Select `json:"sub,omitempty"`
	// Func is a builtin call (lower-cased): coalesce, length, lower,
	// upper, abs, now. Args are its arguments.
	Func string `json:"func,omitempty"`
	Args []Expr `json:"args,omitempty"`
	// Binary op: "+", "-", "*", "/". The left operand is Left when set,
	// else this node's own Column/Lit/Param/Func.
	BinOp string `json:"bin_op,omitempty"`
	Left  *Expr  `json:"left,omitempty"`
	Right *Expr  `json:"right,omitempty"`
	// Case is a CASE expression (simple: CASE x WHEN v THEN r ...; searched:
	// CASE WHEN cond THEN r ...).
	Case *CaseExpr `json:"case,omitempty"`
	// Cmp is a comparison used as a boolean value ('d' = any(stxkind) AS
	// enabled, ORDER BY f(x) = 'DEFAULT'): NULL when either side is.
	Cmp *Comparison `json:"cmp,omitempty"`
	// IsDefault is the DEFAULT keyword used as a value (INSERT VALUES
	// (DEFAULT, ...), UPDATE SET c = DEFAULT).
	IsDefault bool `json:"is_default,omitempty"`
	// Cast is the last ::type applied when it changes the value: only
	// "regclass" ('t'::regclass resolves a table name to its OID) is kept;
	// every other cast is absorbed.
	Cast string `json:"cast,omitempty"`
	// Window is a window function call used as a value inside an
	// expression; the executor computes it as a window item and
	// substitutes its value.
	Window *SelectExpr `json:"window,omitempty"`
}

// CaseExpr is CASE [operand] WHEN ... THEN ... [ELSE ...] END.
type CaseExpr struct {
	Operand *Expr      `json:"operand,omitempty"`
	Whens   []CaseWhen `json:"whens"`
	Else    *Expr      `json:"else,omitempty"`
}

// CaseWhen is one arm: Value compares against the operand (simple form)
// or Cond holds (searched form).
type CaseWhen struct {
	Value  *Expr        `json:"value,omitempty"`
	Cond   []Comparison `json:"cond,omitempty"`
	Result Expr         `json:"result"`
}

// Comparison is one WHERE conjunct: col op value, col IS [NOT] NULL,
// col [NOT] IN (list | subquery), or [NOT] EXISTS (subquery). The executor
// rewrites evaluated EXISTS conjuncts to the constant ops TRUE/FALSE.
type Comparison struct {
	Column string     // lower-cased; empty for [NOT] EXISTS, TRUE/FALSE, and OR
	Path   []PathStep // ->/->> chain applied to Column (JSONB extraction)
	Op     string     // = != < <= > >= ~ !~ ~* !~* | IS [NOT] NULL | [NOT] IN | [NOT] EXISTS | TRUE FALSE | OR
	Value  Expr       // scalar ops: literal, param, or scalar subquery
	Values []Expr     // [NOT] IN: the value list (spliced from Sub when a subquery)
	Sub    *Select    // [NOT] IN / [NOT] EXISTS subquery
	// Expr, when set, is a computed left-hand side (qty * 2 > 10,
	// lower(name) = 'x'); Column/Path are empty then. Computed LHS
	// conjuncts are never usable for key bounds and are v1-restricted to
	// single-table queries.
	Expr *Expr `json:"expr,omitempty"`
	// Escape is the ESCAPE character of a LIKE / ILIKE / SIMILAR TO
	// pattern ("" = backslash).
	Escape string `json:"escape,omitempty"`
	// Or (Op "OR") is a disjunction of conjunctions: the conjunct is true
	// when ANY inner []Comparison is entirely true. NOT was eliminated at
	// parse time (De Morgan + operator negation — sound under SQL
	// three-valued logic because WHERE keeps only TRUE); scalar subqueries
	// may appear inside OR, IN and EXISTS subqueries may not.
	Or [][]Comparison `json:"or,omitempty"`
}

type ColumnDef struct {
	Name       string
	Type       types.Family
	NotNull    bool
	PrimaryKey bool // column-level PRIMARY KEY shorthand
	// Default is a constant DEFAULT; DefaultExpr any other DEFAULT
	// expression (now(), gen_random_uuid(), unique_rowid(), nextval('s'),
	// arithmetic over constants), kept as an expression.
	Default     *types.Datum
	DefaultExpr *Expr
	// Serial marks SERIAL / BIGSERIAL / SMALLSERIAL (an owned sequence
	// with DEFAULT nextval); Identity is GENERATED { ALWAYS | BY DEFAULT }
	// AS IDENTITY ("always" / "by default"), with optional sequence
	// options.
	Serial      bool
	Identity    string
	IdentitySeq *SequenceOptions
	// Precision/Scale carry a DECIMAL(p,s) typmod (0 precision = bare
	// DECIMAL, unconstrained). Width, MaxLen, Char, NoTZ and
	// TimePrecision are the integer width (2/4, 0 = 8), the VARCHAR(n) /
	// CHAR(n) length, CHAR padding, TIMESTAMP without time zone and
	// TIMESTAMP(p) as p+1 (see TypeSpec).
	Precision     int32
	Scale         int32
	Width         int32
	MaxLen        int32
	Char          bool
	NoTZ          bool
	TimePrecision int32
	// TypeName names a user-defined type (an enum) when Type is Enum;
	// the executor resolves it.
	TypeName string
	// Hidden marks a system-managed column (CREATE TABLE AS's rowid
	// primary key); never parsed.
	Hidden bool
	// Constraints are the column's UNIQUE, CHECK and REFERENCES clauses,
	// as table constraints over this one column.
	Constraints []ConstraintDef
}

// ConstraintDef is a table constraint: [CONSTRAINT name] UNIQUE (cols)
// | CHECK (expr) | FOREIGN KEY (cols) REFERENCES t [(cols)] [ON DELETE
// action] [ON UPDATE action]. Name is "" when the statement gave none.
type ConstraintDef struct {
	Name    string
	Kind    string // "unique", "check", "foreign"
	Columns []string
	// Check is the CHECK expression's source text; CheckFails the lowered
	// negation (the row violates the constraint when it holds), for
	// validation at parse time.
	Check      string
	CheckFails []Comparison
	// Foreign keys: the referenced table and columns (empty = its
	// primary key) and the actions ("restrict", "cascade", "set null").
	RefTable   string
	RefColumns []string
	OnDelete   string
	OnUpdate   string
	// NotValid is ALTER TABLE ... ADD CONSTRAINT ... NOT VALID: existing
	// rows are not checked.
	NotValid bool
}

type CreateTable struct {
	Name        string
	IfNotExists bool
	Columns     []ColumnDef
	// Like lists the LIKE source tables of the column list, in order,
	// each expanded into columns (and, per its options, defaults,
	// constraints and indexes) where it was written.
	Like []LikeClause
	// As is CREATE TABLE ... AS query: the table takes the query's
	// output columns (Columns then holds at most the column names to
	// use) and, unless NoData, its rows. AsText is the query as written.
	As     *Select
	AsText string
	NoData bool
	// AsColumns are the column names given before AS (CREATE TABLE t (a,
	// b) AS ...), positional over the query's output.
	AsColumns  []string
	PrimaryKey []string // table-level constraint (column names)
	// PrimaryKeyName is an explicit CONSTRAINT name on the primary key
	// (accepted; the primary key is always named <table>_pkey).
	PrimaryKeyName string
	Constraints    []ConstraintDef
	// Options is the trailing WITH (name = value, ...) list, lowercased
	// names mapping to raw literal text (e.g. timeseries=true,
	// retention='7d', shards=8). Nil when no WITH clause was given.
	Options map[string]string
}

// LikeClause is LIKE source [INCLUDING | EXCLUDING option ...] inside a
// CREATE TABLE column list. The primary key is always copied (a table
// needs one); Defaults, Constraints and Indexes follow the options
// (INCLUDING ALL sets every one). Position is the index in Columns
// before which the copied columns go.
type LikeClause struct {
	Table       string
	Defaults    bool
	Constraints bool
	Indexes     bool
	Comments    bool
	Position    int
}

// CommentOn is COMMENT ON TABLE | VIEW | INDEX | COLUMN name IS 'text'
// | NULL. Kind is "table" (views too), "index" or "column"; Column is
// the column name for Kind "column" (Name then names the table).
type CommentOn struct {
	Kind   string
	Name   string
	Column string
	// Text is the comment; nil removes it (IS NULL).
	Text *string
}

// SetType is ALTER [COLUMN] c [SET DATA] TYPE t: the column and the new
// type with its modifiers (the ColumnDef fields).
type SetType struct {
	Column        string
	Type          types.Family
	Precision     int32
	Scale         int32
	Width         int32
	MaxLen        int32
	Char          bool
	NoTZ          bool
	TimePrecision int32
	TypeName      string
}

// TypeSpec is a parsed column type: the family and the modifiers datax
// enforces. Width is an integer's width in bytes (2 for INT2 /
// SMALLINT, 4 for INT4 / INT / INTEGER, 0 = 8 for INT8 / BIGINT);
// MaxLen the VARCHAR(n) / CHAR(n) length (0 = unbounded) with Char set
// for CHAR(n); NoTZ marks TIMESTAMP [WITHOUT TIME ZONE] as opposed to
// TIMESTAMPTZ; Precision/Scale are DECIMAL(p,s); TimePrecision is
// TIMESTAMP(p) stored as p+1 (0 = undeclared, so TIMESTAMP(0) is 1).
type TypeSpec struct {
	Family        types.Family
	Precision     int32
	Scale         int32
	Width         int32
	MaxLen        int32
	Char          bool
	NoTZ          bool
	TimePrecision int32
	// TypeName is the name of a user-defined type (Family Enum).
	TypeName string
}

// SequenceOptions are the options of CREATE / ALTER SEQUENCE and of an
// identity column; a nil pointer leaves the option unset.
type SequenceOptions struct {
	Increment  *int64
	MinValue   *int64 // NoMin: NO MINVALUE
	MaxValue   *int64
	NoMin      bool
	NoMax      bool
	Start      *int64
	Cache      *int64
	Cycle      *bool
	Restart    *int64 // ALTER SEQUENCE RESTART [WITH n]; RestartSet marks a bare RESTART
	RestartSet bool
	// OwnedBy is "table.column" (or "none" / "" for none).
	OwnedBy string
}

// CreateSequence is CREATE SEQUENCE [IF NOT EXISTS] name [options].
type CreateSequence struct {
	Name        string
	IfNotExists bool
	Options     SequenceOptions
}

// DropSequence is DROP SEQUENCE [IF EXISTS] name.
type DropSequence struct {
	Name     string
	IfExists bool
}

// AlterSequence is ALTER SEQUENCE [IF EXISTS] name [options | RESTART [WITH n]].
type AlterSequence struct {
	Name     string
	IfExists bool
	Options  SequenceOptions
}

// ShowSequences is SHOW SEQUENCES.
type ShowSequences struct{}

// CreateType is CREATE TYPE [IF NOT EXISTS] name AS ENUM ('a', 'b', ...).
type CreateType struct {
	Name        string
	IfNotExists bool
	Labels      []string
}

// AlterType is ALTER TYPE name ADD VALUE [IF NOT EXISTS] 'label'
// (appended: BEFORE / AFTER are refused at parse).
type AlterType struct {
	Name           string
	AddValue       string
	IfNotExistsVal bool
}

// DropType is DROP TYPE [IF EXISTS] name.
type DropType struct {
	Name     string
	IfExists bool
}

// ShowFunctions is SHOW FUNCTIONS: the builtin function reference.
type ShowFunctions struct{}

// CreateIndex is CREATE [UNIQUE] INDEX [IF NOT EXISTS] name ON table (cols).
type CreateIndex struct {
	Unique      bool
	IfNotExists bool
	Name        string
	Table       string
	Columns     []string
}

// DropIndex is DROP INDEX [IF EXISTS] [db.]name: the index is found on
// whichever table of the database carries it.
type DropIndex struct {
	Name     string
	IfExists bool
}

// AlterIndex is ALTER INDEX [IF EXISTS] [db.]name RENAME TO new.
type AlterIndex struct {
	Name     string
	NewName  string
	IfExists bool
}

// Truncate is TRUNCATE [TABLE] t [, ...] [RESTART IDENTITY | CONTINUE
// IDENTITY] [CASCADE | RESTRICT].
type Truncate struct {
	Tables          []string
	RestartIdentity bool
	Cascade         bool
}

// Explain wraps a statement whose access plan should be described instead
// of executed.
type Explain struct {
	Stmt Statement
	// Analyze runs the statement and reports each stage's actual rows and
	// time along with the plan.
	Analyze bool
}

type DropTable struct {
	Name     string
	IfExists bool
	// Cascade drops the foreign keys of other tables that reference this
	// one along with it; without it such a table is refused.
	Cascade bool
}

type Insert struct {
	With    []CTE
	Table   string
	Columns []string // empty = all columns in order
	Rows    [][]Expr
	// Select is the INSERT ... SELECT source (Rows is then empty): its
	// output rows are inserted as if written as VALUES.
	Select *Select
	// DefaultValues is INSERT INTO t DEFAULT VALUES (one row of defaults).
	DefaultValues bool
	// Overriding is OVERRIDING { SYSTEM | USER } VALUE ("system" /
	// "user"): SYSTEM lets an explicit value into a GENERATED ALWAYS
	// identity column.
	Overriding string
	// OnConflict is the ON CONFLICT clause; Upsert marks UPSERT INTO
	// (ON CONFLICT (primary key) DO UPDATE SET every target column).
	OnConflict *OnConflict
	Upsert     bool
	// Returning is the RETURNING list (nil = none): expressions over the
	// written row, "*" for every visible column.
	Returning []SelectExpr
}

// OnConflict is ON CONFLICT [(cols) | ON CONSTRAINT name] DO NOTHING |
// DO UPDATE SET ... [WHERE ...]. In Set values and Where, "excluded.col"
// names the row proposed for insertion.
type OnConflict struct {
	Columns    []string // the conflict target's columns (nil with Constraint or none)
	Constraint string   // ON CONSTRAINT name (a primary key or unique index name)
	DoNothing  bool
	Set        []SetClause
	Where      []Comparison
}

// SetClause is one "column = value" of UPDATE SET or DO UPDATE SET.
type SetClause struct {
	Column string
	Value  Expr
}

// CopyFormat selects the data encoding of a COPY FROM STDIN stream.
type CopyFormat int

const (
	CopyFormatText CopyFormat = iota
	CopyFormatCSV
	CopyFormatBinary
)

func (f CopyFormat) String() string {
	switch f {
	case CopyFormatCSV:
		return "csv"
	case CopyFormatBinary:
		return "binary"
	default:
		return "text"
	}
}

// CopyFrom is COPY table [(cols)] FROM STDIN [format clause]. The data
// itself travels out of band (pgwire copy-in sub-protocol), so this
// statement only names the target and the encoding.
type CopyFrom struct {
	Table   string
	Columns []string // empty = all visible columns in order
	Format  CopyFormat
}

type SelectExpr struct {
	Star  bool
	Expr  Expr
	Alias string
	// Agg is an aggregate call (COUNT, SUM, AVG, MIN, MAX, STRING_AGG,
	// ARRAY_AGG, BOOL_AND, BOOL_OR, STDDEV, VARIANCE, PERCENTILE_CONT,
	// PERCENTILE_DISC, JSONB_AGG, JSONB_OBJECT_AGG, ..., upper-cased).
	// AggStar marks COUNT(*); AggCol names a plain column argument, else
	// AggArg is the argument expression. AggArgs are further arguments
	// (string_agg's separator, jsonb_object_agg's value); AggDistinct
	// marks DISTINCT; AggFilter is FILTER (WHERE ...); AggOrder is WITHIN
	// GROUP (ORDER BY ...) for the ordered-set aggregates.
	Agg         string
	AggStar     bool
	AggCol      string
	AggArg      *Expr
	AggArgs     []Expr
	AggDistinct bool
	AggFilter   []Comparison
	AggOrder    []OrderCol
	// Window makes the call a window function: an aggregate computed
	// over each row's frame, or one of the window-only functions
	// (ROW_NUMBER, RANK, DENSE_RANK, PERCENT_RANK, CUME_DIST, NTILE, LAG,
	// LEAD, FIRST_VALUE, LAST_VALUE, NTH_VALUE) in Agg.
	Window *WindowSpec
}

// windowExprDoc: Expr.Window (below) holds a window function call used
// inside a value expression (amount - lag(amount) OVER (ORDER BY at)).

// WindowSpec is an OVER clause: [name] [PARTITION BY exprs] [ORDER BY
// terms] [frame]. Name refers to a WINDOW clause entry the spec extends
// (or, alone, uses).
type WindowSpec struct {
	Name        string
	PartitionBy []Expr
	OrderBy     []OrderCol
	Frame       *WindowFrame
}

// WindowFrame is ROWS | RANGE BETWEEN start AND end (end defaults to
// CURRENT ROW).
type WindowFrame struct {
	Mode       string // "ROWS" or "RANGE"
	Start, End FrameBound
}

// FrameBound is one frame edge: "unbounded preceding", "preceding" (by
// Offset rows), "current row", "following" (by Offset rows), or
// "unbounded following".
type FrameBound struct {
	Kind   string
	Offset int64
}

// NamedWindow is one WINDOW clause entry.
type NamedWindow struct {
	Name string
	Spec WindowSpec
}

// OrderCol is one ORDER BY term: an output/column name, or (Expr set) a
// computed expression evaluated per row.
type OrderCol struct {
	Column string
	Expr   *Expr
	Desc   bool
	// Nulls is "first" or "last" when NULLS FIRST | LAST was written; ""
	// is PostgreSQL's default (NULLS LAST ascending, NULLS FIRST
	// descending).
	Nulls string
	// Position is a 1-based output position, kept when the select list
	// is not known at parse time (ORDER BY after a parenthesized query);
	// otherwise positions are rewritten to output names.
	Position int
	// Agg is an aggregate call (ORDER BY count(*) DESC) in a grouped
	// query: sorted by, and computed when no select item is the same.
	Agg *SelectExpr
}

// HavingCond is one HAVING conjunct: an aggregate call (HAVING COUNT(*) > 5)
// or a group-column/output name (HAVING city != 'x') compared to a value.
type HavingCond struct {
	Agg    *SelectExpr // aggregate form; nil for the column form
	Column string
	Op     string
	Value  Expr
}

// ColumnRef is a possibly-qualified column reference (t.c or c).
type ColumnRef struct {
	Table  string // alias or table name; "" = unqualified
	Column string
}

// JoinCond is one ON conjunct: an equality between two column references.
type JoinCond struct {
	L, R ColumnRef
}

// JoinClause is [INNER | LEFT [OUTER]] JOIN table [AS] alias ON conds.
type JoinClause struct {
	Left  bool // LEFT OUTER; false = inner
	Cross bool // CROSS JOIN / comma join: no ON clause
	// Right and Full mark RIGHT [OUTER] and FULL [OUTER] joins: the
	// joined table's unmatched rows are kept, NULL-extended on every
	// earlier side (Full also keeps the earlier sides' unmatched rows,
	// like Left).
	Right, Full bool
	// Using lists the columns of JOIN ... USING (cols): each is equated
	// between the joined table and the earlier side that has it, shows
	// once in SELECT *, and resolves unqualified without ambiguity.
	// Natural asks for USING over every column name the sides share (a
	// cross join when they share none).
	Using   []string
	Natural bool
	Table   string
	Alias   string
	// Derived is a subquery joined as a member (JOIN (SELECT ...) AS d
	// ON ...); Table is empty then and Alias names it.
	Derived *Select
	// FuncTable is a table function joined in (FROM t, f(x) [WITH
	// ORDINALITY] AS a(c1, c2)), with its column names; Table is empty
	// then. Parsed for the catalog queries tools send; not executable.
	FuncTable *Expr
	FuncCols  []string
	On        []JoinCond
	// Filter holds the ON conjuncts that are not join-key equalities
	// (tc.relkind = 't', a.attnum > 0, NOT a.attisdropped). They are part
	// of the join condition: a candidate match failing them is not a
	// match (so a LEFT JOIN NULL-extends), unlike a WHERE conjunct.
	Filter []Comparison
}

// CTE is one WITH member: name [(cols)] AS (query). Query is a Select
// (or VALUES), or a data-modifying statement with RETURNING. Recursive
// marks a WITH RECURSIVE list: a member whose query's set operation
// refers to its own name iterates from the first member's rows.
type CTE struct {
	Name      string
	Columns   []string
	Query     Statement
	Recursive bool
	// Qualified is a second, database-qualified name the bound relation
	// answers to (a view referenced as db.v; see pkg/sql/view.go).
	Qualified string
	// Definer, when set, is the role whose privileges the member's query
	// runs with: a view's owner (pkg/sql/view.go).
	Definer string
}

type Select struct {
	// With are the statement's WITH members, materialized in order
	// before it runs and visible to it (and to each other, in order) as
	// relations named by the member.
	With     []CTE
	Distinct bool
	Exprs    []SelectExpr
	Table    string  // empty for FROM-less and derived-table selects
	Alias    string  // optional alias for Table (or the derived table's alias)
	Derived  *Select // FROM (SELECT ...) AS alias — materialized subquery
	// FuncTable is FROM unnest(array) [AS] alias [(column)]: a
	// set-returning call materialized as a one-column table named Alias
	// with column FuncCol.
	FuncTable *Expr
	FuncCol   string
	// Joins are the JOIN clauses in syntactic order; execution joins
	// left-deep in exactly this order.
	Joins []JoinClause
	// Union chains the next member of a UNION [ALL] (its own Union links
	// further members). ORDER BY and LIMIT written after the last member
	// apply to the whole union and are carried by the head select.
	Union    *Select
	UnionAll bool
	// Windows are the WINDOW clause's named specifications.
	Windows []NamedWindow
	// SetOp is the operator between this member and Union: "UNION" (or
	// ""), "INTERSECT" or "EXCEPT". Members form a flat list in written
	// order; INTERSECT binds tighter than the other two, which associate
	// left to right, and the executor applies that precedence.
	SetOp   string
	Where   []Comparison
	GroupBy []string
	Having  []HavingCond
	OrderBy []OrderCol
	Limit   int64 // -1 = none
	// Offset skips that many rows before Limit applies (0 = none).
	Offset int64
	// LimitParam and OffsetParam name the parameter ($n) supplying the
	// count at execution (0 = written as a number, or absent).
	LimitParam, OffsetParam int
	// AsOf is the AS OF SYSTEM TIME operand ("" = none): a string literal
	// holding a negative duration ('-5s'), an RFC 3339 timestamp, or a
	// Unix-nanoseconds integer. The read runs at that fixed timestamp and
	// may be served by follower replicas.
	AsOf string
	// AsOfMaxStaleness is the AS OF SYSTEM TIME with_max_staleness('10s')
	// operand ("" = none): a POSITIVE duration string. The gateway picks
	// one statement timestamp — the freshest its local replicas can serve
	// from their closed timestamps, never older than now minus the bound —
	// and ranges that cannot serve it locally fall back to their leader.
	// Mutually exclusive with AsOf by construction.
	AsOfMaxStaleness string
	// ForUpdate locks the selected rows (write intents) for the enclosing
	// transaction, serializing read-modify-write against other writers.
	ForUpdate bool
}

type Update struct {
	With      []CTE
	Table     string
	Set       []SetClause
	Where     []Comparison
	Returning []SelectExpr
}

// AlterTable is ALTER TABLE [IF EXISTS] t <action>: one of the fields
// below is set.
type AlterTable struct {
	Table string
	// IfExists makes a missing table a no-op.
	IfExists bool
	AddCol   *ColumnDef // set for ADD COLUMN
	DropCol  string     // set for DROP COLUMN
	// AddColIfNotExists / DropColIfExists are ADD COLUMN IF NOT EXISTS /
	// DROP COLUMN IF EXISTS: an existing / missing column is a no-op.
	AddColIfNotExists bool
	DropColIfExists   bool
	// RenameTo is RENAME TO new; RenameCol and RenameConstraint are
	// RENAME [COLUMN] a TO b and RENAME CONSTRAINT a TO b.
	RenameTo         string
	RenameCol        *Rename
	RenameConstraint *Rename
	// SetDefault is ALTER [COLUMN] c SET DEFAULT value; DropDefault is
	// ALTER [COLUMN] c DROP DEFAULT (the column name).
	SetDefault  *SetDefault
	DropDefault string
	// SetType is ALTER [COLUMN] c [SET DATA] TYPE t (an online rewrite).
	SetType *SetType
	// SetOptions is ALTER TABLE ... SET (name = value, ...) — today only
	// shards = N, the online re-shard.
	SetOptions map[string]string
	// AddConstraint is ADD [CONSTRAINT name] ...; DropConstraint is DROP
	// CONSTRAINT [IF EXISTS] name; ValidateConstraint is VALIDATE
	// CONSTRAINT name.
	AddConstraint          *ConstraintDef
	DropConstraint         string
	DropConstraintIfExists bool
	ValidateConstraint     string
	// SetNotNull / DropNotNull are ALTER [COLUMN] c SET NOT NULL / DROP
	// NOT NULL (the column name).
	SetNotNull  string
	DropNotNull string
	// SplitAt is SPLIT AT VALUES (k, ...), (k, ...): one primary-key
	// tuple (a prefix of the key is allowed) per range boundary to carve.
	SplitAt [][]Expr
}

// CreateView is CREATE [OR REPLACE] VIEW name [(cols)] AS query.
type CreateView struct {
	Name      string
	Columns   []string
	OrReplace bool
	Query     *Select
	// Text is the query's SQL as written; the view stores it.
	Text string
}

// DropView is DROP VIEW [IF EXISTS] name [, ...] [CASCADE | RESTRICT].
type DropView struct {
	Names    []string
	IfExists bool
	Cascade  bool
}

// Rename is an old name and its replacement.
type Rename struct{ From, To string }

// SetDefault is a column's new default: a constant (Default) or an
// expression (Expr), exactly as a column definition carries them.
type SetDefault struct {
	Column  string
	Default *types.Datum
	Expr    *Expr
}

type Delete struct {
	With      []CTE
	Table     string
	Where     []Comparison
	Returning []SelectExpr
}

// CreateRole is CREATE ROLE / CREATE USER / ALTER ROLE / ALTER USER
// (issue #98). CREATE USER is CREATE ROLE ... LOGIN; the options are
// LOGIN | NOLOGIN, PASSWORD 'pw' | PASSWORD NULL, INHERIT | NOINHERIT and
// IN ROLE r [, ...]. A nil option was not spelled (ALTER leaves it
// alone; CREATE applies the default).
type CreateRole struct {
	Name string
	// Alter marks ALTER ROLE / ALTER USER: the role must already exist.
	Alter bool
	// IfNotExists is CREATE ... IF NOT EXISTS: an existing role is a no-op.
	IfNotExists bool
	// IsUser marks the USER spelling (LOGIN by default).
	IsUser bool
	Login  *bool
	// Password: nil = not spelled; "" = PASSWORD NULL (no password).
	Password *string
	Inherit  *bool
	// InRoles are the roles the new role is made a member of (IN ROLE).
	InRoles []string
}

// DropRole is DROP ROLE / DROP USER [IF EXISTS] name [, ...].
type DropRole struct {
	Names    []string
	IfExists bool
}

// GrantRevoke is GRANT / REVOKE in both forms: role membership
// (GRANT admin TO alice [WITH ADMIN OPTION]; REVOKE [ADMIN OPTION FOR]
// admin FROM alice) when Roles is set, and object privileges otherwise
// (GRANT SELECT, INSERT ON t1, t2 TO alice, PUBLIC [WITH GRANT OPTION];
// REVOKE [GRANT OPTION FOR] ALL ON ALL TABLES IN SCHEMA public FROM bob).
type GrantRevoke struct {
	Revoke bool

	// Membership form.
	Roles       []string
	AdminOption bool

	// Privilege form. Privileges are upper-cased (SELECT INSERT UPDATE
	// DELETE TRUNCATE USAGE CREATE CONNECT; ALL stays "ALL"). ObjectKind
	// is "table" (the default), "sequence", "database" or "schema";
	// Objects the names, or — AllInSchema — every table / sequence of the
	// public schema of the current database.
	Privileges  []string
	ObjectKind  string
	Objects     []string
	AllInSchema bool
	// Grantees are role names, lower-cased; "public" is the pseudo-role.
	Grantees []string
	// GrantOption is WITH GRANT OPTION (GRANT) or GRANT OPTION FOR
	// (REVOKE: only the option is revoked).
	GrantOption bool
}

// AlterOwner is ALTER TABLE | VIEW | SEQUENCE | TYPE | DATABASE name
// OWNER TO role.
type AlterOwner struct {
	Kind  string // table, view, sequence, type, database
	Name  string
	Owner string
}

// ReassignOwned is REASSIGN OWNED BY role [, ...] TO role.
type ReassignOwned struct {
	From []string
	To   string
}

// DropOwned is DROP OWNED BY role [, ...] [CASCADE | RESTRICT].
type DropOwned struct {
	Roles   []string
	Cascade bool
}

// AlterDefaultPrivileges is ALTER DEFAULT PRIVILEGES [FOR ROLE r [, ...]]
// [IN SCHEMA public] GRANT privs ON TABLES | SEQUENCES TO grantee [, ...]
// [WITH GRANT OPTION] (or REVOKE ... FROM ...). ForRoles empty means the
// current user.
type AlterDefaultPrivileges struct {
	ForRoles    []string
	Revoke      bool
	Privileges  []string
	ObjectKind  string // "tables" or "sequences"
	Grantees    []string
	GrantOption bool
}

type Begin struct{}
type Commit struct{}
type Rollback struct{}

// Savepoint / ReleaseSavepoint / RollbackToSavepoint are the SQL savepoint
// statements (PG semantics; see docs/transactions.md).
type Savepoint struct{ Name string }
type ReleaseSavepoint struct{ Name string }
type RollbackToSavepoint struct{ Name string }

// ShowTables is SHOW TABLES [FROM database].
type ShowTables struct{ Database string }

// Show is one of the introspection statements: SHOW COLUMNS FROM t, SHOW
// INDEXES FROM t, SHOW CREATE TABLE | VIEW t, SHOW VIEWS, SHOW USERS, SHOW
// GRANTS [ON t] [FOR user], SHOW ALL. Kind is "columns", "indexes", "create", "views", "users",
// "grants", or "all".
type Show struct {
	Kind  string
	Table string
	User  string
	// Database is SHOW GRANTS ON DATABASE d; OnRole is SHOW GRANTS ON ROLE
	// [Role] (the membership listing).
	Database string
	OnRole   bool
	Role     string
}

// Analyze collects table statistics (row count, per-column distinct
// estimates) for one table, or every table when Table is empty. Runs
// outside any transaction (chunked frozen-timestamp sweep); admin-only.
type Analyze struct{ Table string }

// ShowStats renders the stored statistics for a table (read-only).
type ShowStats struct{ Table string }

// PlacementOptions is the option list of CREATE DATABASE ... WITH (...)
// and ALTER DATABASE ... SET (...) — issue #176. It carries the text an
// operator wrote; the executor turns it into a base.PlacementPolicy, so
// the parser stays free of placement semantics. SetReplicas and
// SetConstraints distinguish an option that was named from one that was
// left alone, which is what lets ALTER change a replica count without
// disturbing the constraints.
type PlacementOptions struct {
	Replicas       int
	SetReplicas    bool
	Constraints    []string
	SetConstraints bool
}

// CreateDatabase is
// CREATE DATABASE [IF NOT EXISTS] name [WITH] (placement options).
type CreateDatabase struct {
	Name        string
	IfNotExists bool
	Placement   *PlacementOptions
}

// DropDatabase is DROP DATABASE [IF EXISTS] name [CASCADE | RESTRICT].
type DropDatabase struct {
	Name     string
	IfExists bool
	Cascade  bool
}

// AlterDatabase is ALTER DATABASE name RENAME TO new, or
// ALTER DATABASE name SET (placement options) when Placement is set.
type AlterDatabase struct {
	Name      string
	NewName   string
	Placement *PlacementOptions
}

// ShowPlacement is SHOW PLACEMENT FOR DATABASE name — the policy a
// database carries and what the cluster resolves it to.
type ShowPlacement struct{ Database string }

// ShowDatabases is SHOW DATABASES.
type ShowDatabases struct{}

// Use is USE name (CockroachDB syntax for SET database = name).
type Use struct{ Name string }

// SetVar is SET [SESSION | LOCAL] name {TO | =} value, RESET name (Reset,
// or RESET ALL with an empty Name), SET TIME ZONE x, SET NAMES x, SET
// [SESSION CHARACTERISTICS AS] TRANSACTION ..., and SHOW name (Name
// "show:name"). The session honors every variable it knows and refuses
// the rest (42704).
type SetVar struct {
	Name string
	// Value is the literal, identifier, number or comma-joined list after
	// = / TO ("DEFAULT" resets).
	Value string
	// Local marks SET LOCAL (the block's end restores the value).
	Local bool
	// Reset marks RESET name / RESET ALL.
	Reset bool
}

func (*CreateTable) stmt()            {}
func (*Show) stmt()                   {}
func (*CreateSequence) stmt()         {}
func (*CreateType) stmt()             {}
func (*AlterType) stmt()              {}
func (*DropType) stmt()               {}
func (*DropSequence) stmt()           {}
func (*AlterSequence) stmt()          {}
func (*ShowSequences) stmt()          {}
func (*ShowFunctions) stmt()          {}
func (*CreateIndex) stmt()            {}
func (*DropIndex) stmt()              {}
func (*CreateView) stmt()             {}
func (*CommentOn) stmt()              {}
func (*DropView) stmt()               {}
func (*AlterIndex) stmt()             {}
func (*Truncate) stmt()               {}
func (*Explain) stmt()                {}
func (*DropTable) stmt()              {}
func (*Insert) stmt()                 {}
func (*CopyFrom) stmt()               {}
func (*Select) stmt()                 {}
func (*Update) stmt()                 {}
func (*Delete) stmt()                 {}
func (*AlterTable) stmt()             {}
func (*CreateRole) stmt()             {}
func (*DropRole) stmt()               {}
func (*GrantRevoke) stmt()            {}
func (*AlterOwner) stmt()             {}
func (*ReassignOwned) stmt()          {}
func (*DropOwned) stmt()              {}
func (*AlterDefaultPrivileges) stmt() {}
func (*Begin) stmt()                  {}
func (*Commit) stmt()                 {}
func (*Savepoint) stmt()              {}
func (*ReleaseSavepoint) stmt()       {}
func (*RollbackToSavepoint) stmt()    {}
func (*Rollback) stmt()               {}
func (*ShowTables) stmt()             {}
func (*Analyze) stmt()                {}
func (*ShowStats) stmt()              {}
func (*SetVar) stmt()                 {}
func (*CreateDatabase) stmt()         {}
func (*ShowPlacement) stmt()          {}
func (*DropDatabase) stmt()           {}
func (*AlterDatabase) stmt()          {}
func (*ShowDatabases) stmt()          {}
func (*Use) stmt()                    {}
