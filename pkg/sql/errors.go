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
	CodeSerializationFailure  = "40001"
	CodeUndefinedTable        = "42P01"
	CodeDuplicateTable        = "42P07"
	CodeUndefinedColumn       = "42703"
	CodeUniqueViolation       = "23505"
	CodeNotNullViolation      = "23502"
	CodeSyntaxError           = "42601"
	CodeFeatureNotSupported   = "0A000"
	CodeInFailedTransaction   = "25P02"
	CodeActiveTransaction     = "25001"
	CodeNoActiveTransaction   = "25P01"
	CodeInvalidSavepoint      = "3B001"
	CodeGrouping              = "42803"
	CodeDivisionByZero        = "22012"
	CodeAmbiguousColumn       = "42702"
	CodeCardinality           = "21000"
	CodeInsufficientPriv      = "42501"
	CodeInternal              = "XX000"
	CodeInvalidParameter      = "08P01"
	CodeUndefinedObject       = "42704"
	CodeDuplicateObject       = "42710"
	CodeInvalidParameterValue = "22023"
	// COPY FROM STDIN.
	CodeInvalidTextRepresentation = "22P02"
	CodeBadCopyFormat             = "22P04"
	CodeQueryCanceled             = "57014"
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
	var ex *catalog.ErrTableExists
	if errors.As(err, &ex) {
		return &Error{Code: CodeDuplicateTable, Msg: ex.Error()}
	}
	return &Error{Code: CodeInternal, Msg: err.Error()}
}
