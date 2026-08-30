// Package parser is datax's hand-rolled SQL lexer and recursive-descent
// parser for the v1 grammar subset (see docs/sql.md).
package parser

import "github.com/sthorne/datax/pkg/sql/types"

// Statement is any parsed SQL statement.
type Statement interface{ stmt() }

// Expr is a scalar expression: a literal, a parameter ($N), a column
// reference, or column ± literal/param (for UPDATE ... SET x = x + 1).
type Expr struct {
	Lit    *types.Datum `json:"lit,omitempty"`
	Param  int          `json:"param,omitempty"` // 1-based; 0 = none
	Column string       `json:"column,omitempty"`
	// Sub is a scalar subquery: (SELECT ...) used as a value. Uncorrelated
	// only — the executor evaluates it once and splices the result.
	Sub *Select `json:"sub,omitempty"`
	// Binary op: Column/Lit/Param on the left, operator, right operand.
	BinOp string `json:"bin_op,omitempty"` // "+", "-"
	Right *Expr  `json:"right,omitempty"`
}

// Comparison is one WHERE conjunct: col op value, col IS [NOT] NULL,
// col [NOT] IN (list | subquery), or [NOT] EXISTS (subquery). The executor
// rewrites evaluated EXISTS conjuncts to the constant ops TRUE/FALSE.
type Comparison struct {
	Column string  // lower-cased; empty for [NOT] EXISTS and TRUE/FALSE
	Op     string  // = != < <= > >= | IS [NOT] NULL | [NOT] IN | [NOT] EXISTS | TRUE FALSE
	Value  Expr    // scalar ops: literal, param, or scalar subquery
	Values []Expr  // [NOT] IN: the value list (spliced from Sub when a subquery)
	Sub    *Select // [NOT] IN / [NOT] EXISTS subquery
}

type ColumnDef struct {
	Name       string
	Type       types.Family
	NotNull    bool
	PrimaryKey bool // column-level PRIMARY KEY shorthand
	// Default is the DEFAULT literal (constants only; no expressions).
	Default *types.Datum
}

type CreateTable struct {
	Name        string
	IfNotExists bool
	Columns     []ColumnDef
	PrimaryKey  []string // table-level constraint (column names)
}

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

// OrderCol is one ORDER BY term.
type OrderCol struct {
	Column string
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
	Table string
	Alias string
	On    []JoinCond
}

type Select struct {
	Distinct bool
	Exprs    []SelectExpr
	Table    string  // empty for FROM-less and derived-table selects
	Alias    string  // optional alias for Table (or the derived table's alias)
	Derived  *Select // FROM (SELECT ...) AS alias — materialized subquery
	Join     *JoinClause
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
	// ForUpdate locks the selected rows (write intents) for the enclosing
	// transaction, serializing read-modify-write against other writers.
	ForUpdate bool
}

type Update struct {
	Table string
	Set   []struct {
		Column string
		Value  Expr
	}
	Where []Comparison
}

// AlterTable is ALTER TABLE t ADD [COLUMN] def | DROP [COLUMN] name.
type AlterTable struct {
	Table   string
	AddCol  *ColumnDef // set for ADD COLUMN
	DropCol string     // set for DROP COLUMN
}

type Delete struct {
	Table string
	Where []Comparison
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
	Table      string
	User       string
}

type Begin struct{}
type Commit struct{}
type Rollback struct{}

// Savepoint / ReleaseSavepoint / RollbackToSavepoint are the SQL savepoint
// statements (PG semantics; see docs/transactions.md).
type Savepoint struct{ Name string }
type ReleaseSavepoint struct{ Name string }
type RollbackToSavepoint struct{ Name string }
type ShowTables struct{}

// SetVar is `SET name = value` / `SET SESSION ...`: parsed and ignored
// (clients send these at startup).
type SetVar struct{ Name string }

func (*CreateTable) stmt()         {}
func (*CreateIndex) stmt()         {}
func (*Explain) stmt()             {}
func (*DropTable) stmt()           {}
func (*Insert) stmt()              {}
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
func (*SetVar) stmt()              {}
