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
	// DECIMAL, unconstrained). Typmods on other types are still accepted
	// and ignored (documented).
	Precision int32
	Scale     int32
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
	PrimaryKey  []string // table-level constraint (column names)
	// PrimaryKeyName is an explicit CONSTRAINT name on the primary key
	// (accepted; the primary key is always named <table>_pkey).
	PrimaryKeyName string
	Constraints    []ConstraintDef
	// Options is the trailing WITH (name = value, ...) list, lowercased
	// names mapping to raw literal text (e.g. timeseries=true,
	// retention='7d', shards=8). Nil when no WITH clause was given.
	Options map[string]string
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

// CreateUser is CREATE USER / ALTER USER ... PASSWORD 'pw'.
type CreateUser struct {
	Name     string
	Password string
	Alter    bool // ALTER USER: the user must already exist
	// IfNotExists is CREATE USER IF NOT EXISTS: an existing user is a
	// no-op (the password is left alone).
	IfNotExists bool
}

// DropUser is DROP USER [IF EXISTS] name.
type DropUser struct {
	Name     string
	IfExists bool
}

// GrantRevoke is GRANT/REVOKE: either the admin role
// (GRANT ADMIN TO user / REVOKE ADMIN FROM user) or per-table privileges
// (GRANT SELECT, INSERT ON t TO user / REVOKE ALL ON t FROM user).
type GrantRevoke struct {
	Revoke     bool
	Admin      bool     // admin-role form; Privileges/Table empty
	Privileges []string // upper-cased: SELECT INSERT UPDATE DELETE ALL
	// Database names the database for GRANT ... ON DATABASE (Table empty).
	Database string
	Table    string
	User     string
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
// INDEXES FROM t, SHOW CREATE TABLE t, SHOW USERS, SHOW GRANTS [ON t]
// [FOR user], SHOW ALL. Kind is "columns", "indexes", "create", "users",
// "grants", or "all".
type Show struct {
	Kind  string
	Table string
	User  string
}

// Analyze collects table statistics (row count, per-column distinct
// estimates) for one table, or every table when Table is empty. Runs
// outside any transaction (chunked frozen-timestamp sweep); admin-only.
type Analyze struct{ Table string }

// ShowStats renders the stored statistics for a table (read-only).
type ShowStats struct{ Table string }

// CreateDatabase is CREATE DATABASE [IF NOT EXISTS] name.
type CreateDatabase struct {
	Name        string
	IfNotExists bool
}

// DropDatabase is DROP DATABASE [IF EXISTS] name [CASCADE | RESTRICT].
type DropDatabase struct {
	Name     string
	IfExists bool
	Cascade  bool
}

// AlterDatabase is ALTER DATABASE name RENAME TO new.
type AlterDatabase struct {
	Name    string
	NewName string
}

// ShowDatabases is SHOW DATABASES.
type ShowDatabases struct{}

// Use is USE name (CockroachDB syntax for SET database = name).
type Use struct{ Name string }

// SetVar is `SET name = value` / `SET SESSION ...`: parsed and ignored
// (clients send these at startup).
type SetVar struct {
	Name string
	// Value is the literal or identifier after = / TO, when there is one.
	Value string
}

func (*CreateTable) stmt()         {}
func (*Show) stmt()                {}
func (*CreateSequence) stmt()      {}
func (*DropSequence) stmt()        {}
func (*AlterSequence) stmt()       {}
func (*ShowSequences) stmt()       {}
func (*ShowFunctions) stmt()       {}
func (*CreateIndex) stmt()         {}
func (*DropIndex) stmt()           {}
func (*AlterIndex) stmt()          {}
func (*Truncate) stmt()            {}
func (*Explain) stmt()             {}
func (*DropTable) stmt()           {}
func (*Insert) stmt()              {}
func (*CopyFrom) stmt()            {}
func (*Select) stmt()              {}
func (*Update) stmt()              {}
func (*Delete) stmt()              {}
func (*AlterTable) stmt()          {}
func (*CreateUser) stmt()          {}
func (*DropUser) stmt()            {}
func (*GrantRevoke) stmt()         {}
func (*Begin) stmt()               {}
func (*Commit) stmt()              {}
func (*Savepoint) stmt()           {}
func (*ReleaseSavepoint) stmt()    {}
func (*RollbackToSavepoint) stmt() {}
func (*Rollback) stmt()            {}
func (*ShowTables) stmt()          {}
func (*Analyze) stmt()             {}
func (*ShowStats) stmt()           {}
func (*SetVar) stmt()              {}
func (*CreateDatabase) stmt()      {}
func (*DropDatabase) stmt()        {}
func (*AlterDatabase) stmt()       {}
func (*ShowDatabases) stmt()       {}
func (*Use) stmt()                 {}
