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
	// Binary op: Column/Lit/Param on the left, operator, right operand.
	BinOp string `json:"bin_op,omitempty"` // "+", "-"
	Right *Expr  `json:"right,omitempty"`
}

// Comparison is one WHERE conjunct: col op value.
type Comparison struct {
	Column string // lower-cased
	Op     string // = != < <= > >=
	Value  Expr   // literal or param
}

type ColumnDef struct {
	Name       string
	Type       types.Family
	NotNull    bool
	PrimaryKey bool // column-level PRIMARY KEY shorthand
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
}

type Select struct {
	Exprs []SelectExpr
	Table string // empty for FROM-less selects
	Where []Comparison
	Limit int64 // -1 = none
}

type Update struct {
	Table string
	Set   []struct {
		Column string
		Value  Expr
	}
	Where []Comparison
}

type Delete struct {
	Table string
	Where []Comparison
}

type Begin struct{}
type Commit struct{}
type Rollback struct{}
type ShowTables struct{}

// SetVar is `SET name = value` / `SET SESSION ...`: parsed and ignored
// (clients send these at startup).
type SetVar struct{ Name string }

func (*CreateTable) stmt() {}
func (*CreateIndex) stmt() {}
func (*Explain) stmt()     {}
func (*DropTable) stmt()   {}
func (*Insert) stmt()      {}
func (*Select) stmt()      {}
func (*Update) stmt()      {}
func (*Delete) stmt()      {}
func (*Begin) stmt()       {}
func (*Commit) stmt()      {}
func (*Rollback) stmt()    {}
func (*ShowTables) stmt()  {}
func (*SetVar) stmt()      {}
