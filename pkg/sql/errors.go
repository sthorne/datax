// Package sql executes datax's SQL subset over the transactional KV layer:
// sessions, a planner-less executor, and SQLSTATE-carrying errors.
package sql

import (
	"errors"
	"fmt"

	"github.com/sthorne/datax/pkg/kvclient"
	"github.com/sthorne/datax/pkg/sql/catalog"
	"github.com/sthorne/datax/pkg/sql/parser"
)

// SQLSTATE codes used by datax.
const (
	CodeSerializationFailure   = "40001"
	CodeUndefinedTable         = "42P01"
	CodeDuplicateTable         = "42P07"
	CodeUndefinedColumn        = "42703"
	CodeUndefinedFunction      = "42883"
	CodeUniqueViolation        = "23505"
	CodeNotNullViolation       = "23502"
	CodeSyntaxError            = "42601"
	CodeFeatureNotSupported    = "0A000"
	CodeInFailedTransaction    = "25P02"
	CodeActiveTransaction      = "25001"
	CodeNoActiveTransaction    = "25P01"
	CodeInvalidSavepoint       = "3B001"
	CodeGrouping               = "42803"
	CodeDivisionByZero         = "22012"
	CodeAmbiguousColumn        = "42702"
	CodeCardinality            = "21000"
	CodeInsufficientPriv       = "42501"
	CodeInternal               = "XX000"
	CodeInvalidParameter       = "08P01"
	CodeUndefinedObject        = "42704"
	CodeInvalidColumnReference = "42P10"
	CodeSequenceLimit          = "2200H"
	CodeObjectNotInState       = "55000"
	CodeGeneratedAlways        = "428C9"
	CodeDuplicateObject        = "42710"
	CodeCheckViolation         = "23514"
	CodeForeignKeyViolation    = "23503"
	CodeProgramLimitExceeded   = "54000"
	CodeInvalidRegexp          = "2201B"
	CodeInvalidParameterValue  = "22023"
	// DECIMAL(p,s): integer digits exceed precision−scale.
	CodeNumericValueOutOfRange = "22003"
	// COPY FROM STDIN.
	CodeInvalidTextRepresentation = "22P02"
	CodeBadCopyFormat             = "22P04"
	CodeQueryCanceled             = "57014"
	CodeInvalidCatalogName        = "3D000"
	CodeDuplicateDatabase         = "42P04"
	CodeDependentObjectsExist     = "2BP01"
	CodeObjectInUse               = "55006"
	CodeWrongObjectType           = "42809"
	CodeInvalidObjectDefinition   = "42P17"
)

// Error is a SQL-level error with a SQLSTATE code.
type Error struct {
	Code string
	Msg  string
}

func (e *Error) Error() string { return e.Msg }

func newErrf(code, format string, args ...any) *Error {
	return &Error{Code: code, Msg: fmt.Sprintf(format, args...)}
}

// ToSQLError normalizes any error into a *Error with the right SQLSTATE.
func ToSQLError(err error) *Error {
	if err == nil {
		return nil
	}
	var se *Error
	if errors.As(err, &se) {
		return se
	}
	var syn *parser.SyntaxError
	if errors.As(err, &syn) {
		return &Error{Code: CodeSyntaxError, Msg: syn.Error()}
	}
	if kvclient.IsRetryable(err) {
		return &Error{Code: CodeSerializationFailure, Msg: "restart transaction: " + err.Error()}
	}
	var nf *catalog.ErrTableNotFound
	if errors.As(err, &nf) {
		return &Error{Code: CodeUndefinedTable, Msg: nf.Error()}
	}
	var dbNF *catalog.ErrDatabaseNotFound
	if errors.As(err, &dbNF) {
		return &Error{Code: CodeInvalidCatalogName, Msg: dbNF.Error()}
	}
	var dbEx *catalog.ErrDatabaseExists
	if errors.As(err, &dbEx) {
		return &Error{Code: CodeDuplicateDatabase, Msg: dbEx.Error()}
	}
	var ex *catalog.ErrTableExists
	if errors.As(err, &ex) {
		return &Error{Code: CodeDuplicateTable, Msg: ex.Error()}
	}
	return &Error{Code: CodeInternal, Msg: err.Error()}
}
