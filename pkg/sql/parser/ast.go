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
}

type CreateTable struct {
	Name        string
	IfNotExists bool
	Columns     []ColumnDef
	PrimaryKey  []string // table-level constraint (column names)
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

// AlterSequence is ALTER SEQUENCE name [options | RESTART [WITH n]].
type AlterSequence struct {
	Name    string
	Options SequenceOptions
}

// ShowSequences is SHOW SEQUENCES.
type ShowSequences struct{}

// CreateIndex is CREATE [UNIQUE] INDEX name ON table (cols).
type CreateIndex struct {
	Unique  bool
	Name    string
	Table   string
	Columns []string
}

// Explain wraps a statement whose access plan should be described instead
// of executed.
type Explain struct {
	Stmt Statement
}

type DropTable struct {
	Name     string
	IfExists bool
}

type Insert struct {
	Table   string
	Columns []string // empty = all columns in order
	Rows    [][]Expr
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
	// Agg is an aggregate call (COUNT/SUM/AVG/MIN/MAX, upper-cased).
	// AggStar marks COUNT(*); otherwise AggCol names the argument column.
	Agg     string
	AggStar bool
	AggCol  string
}

// OrderCol is one ORDER BY term: an output/column name, or (Expr set) a
// computed expression evaluated per row.
type OrderCol struct {
	Column string
	Expr   *Expr
	Desc   bool
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
	Table string
	Alias string
	On    []JoinCond
	// Filter holds the ON conjuncts that are not join-key equalities
	// (tc.relkind = 't', a.attnum > 0, NOT a.attisdropped). They are part
	// of the join condition: a candidate match failing them is not a
	// match (so a LEFT JOIN NULL-extends), unlike a WHERE conjunct.
	Filter []Comparison
}

type Select struct {
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
	Where    []Comparison
	GroupBy  []string
	Having   []HavingCond
	OrderBy  []OrderCol
	Limit    int64 // -1 = none
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
	Table     string
	Set       []SetClause
	Where     []Comparison
	Returning []SelectExpr
}

// AlterTable is ALTER TABLE t ADD [COLUMN] def | DROP [COLUMN] name.
type AlterTable struct {
	Table   string
	AddCol  *ColumnDef // set for ADD COLUMN
	DropCol string     // set for DROP COLUMN
	// SetOptions is ALTER TABLE ... SET (name = value, ...) — today only
	// shards = N, the online re-shard.
	SetOptions map[string]string
}

type Delete struct {
	Table     string
	Where     []Comparison
	Returning []SelectExpr
}

// CreateUser is CREATE USER / ALTER USER ... PASSWORD 'pw'.
type CreateUser struct {
	Name     string
	Password string
	Alter    bool // ALTER USER: the user must already exist
}

// DropUser is DROP USER name.
type DropUser struct {
	Name string
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
func (*CreateIndex) stmt()         {}
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
