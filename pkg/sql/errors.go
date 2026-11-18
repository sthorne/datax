// Package sql executes datax's SQL subset over the transactional KV layer:
// sessions, a planner-less executor, and SQLSTATE-carrying errors.
package sql

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/sthorne/datax/pkg/kvclient"
	"github.com/sthorne/datax/pkg/kvpb"
	"github.com/sthorne/datax/pkg/sql/catalog"
	"github.com/sthorne/datax/pkg/sql/parser"
)

// SQLSTATE codes used by datax.
const (
	CodeSerializationFailure = "40001"
	CodeUndefinedTable       = "42P01"
	CodeDuplicateTable       = "42P07"
	CodeUndefinedColumn      = "42703"
	CodeDuplicateColumn      = "42701"
	CodeUndefinedFunction    = "42883"
	CodeUniqueViolation      = "23505"
	CodeNotNullViolation     = "23502"
	CodeSyntaxError          = "42601"
	CodeFeatureNotSupported  = "0A000"
	CodeInFailedTransaction  = "25P02"
	CodeActiveTransaction    = "25001"
	CodeNoActiveTransaction  = "25P01"
	CodeInvalidSavepoint     = "3B001"
	CodeGrouping             = "42803"
	CodeDivisionByZero       = "22012"
	CodeAmbiguousColumn      = "42702"
	CodeCardinality          = "21000"
	CodeInsufficientPriv     = "42501"
	CodeInternal             = "XX000"
	CodeInvalidParameter     = "08P01"
	// CodeInvalidBinaryRepresentation: a binary-format parameter the wire
	// layer could not decode (invalid_binary_representation).
	CodeInvalidBinaryRepresentation = "22P03"
	CodeUndefinedObject             = "42704"
	CodeInvalidColumnReference      = "42P10"
	CodeSequenceLimit               = "2200H"
	CodeObjectNotInState            = "55000"
	// CodeCantChangeRuntimeParam: SET of a setting that cannot be
	// changed (server_version_num).
	CodeCantChangeRuntimeParam = "55P02"
	CodeGeneratedAlways        = "428C9"
	CodeDuplicateObject        = "42710"
	CodeReservedName           = "42939"
	CodeCheckViolation         = "23514"
	CodeForeignKeyViolation    = "23503"
	CodeProgramLimitExceeded   = "54000"
	// statement_memory_limit exceeded.
	CodeOutOfMemory           = "53200"
	CodeInvalidRegexp         = "2201B"
	CodeInvalidParameterValue = "22023"
	// DECIMAL(p,s): integer digits exceed precision−scale.
	CodeNumericValueOutOfRange = "22003"
	// COPY FROM STDIN.
	CodeInvalidTextRepresentation = "22P02"
	CodeBadCopyFormat             = "22P04"
	CodeQueryCanceled             = "57014"
	CodeLockNotAvailable          = "55P03"
	CodeAdminShutdown             = "57P01"
	CodeIdleInTransactionTimeout  = "25P03"
	CodeReadOnlyTransaction       = "25006"
	CodeInvalidCatalogName        = "3D000"
	CodeInvalidSchemaName         = "3F000"
	CodeDuplicateDatabase         = "42P04"
	CodeDependentObjectsExist     = "2BP01"
	CodeObjectInUse               = "55006"
	CodeWrongObjectType           = "42809"
	CodeInvalidGrantOperation     = "0LP01"
	CodeInvalidObjectDefinition   = "42P17"
	// CodeInvalidTableDefinition: invalid_table_definition (a second
	// primary key).
	CodeInvalidTableDefinition = "42P16"
	// CodeInvalidForeignKey: invalid_foreign_key (a reference to columns
	// no primary key or unique constraint covers).
	CodeInvalidForeignKey = "42830"

	// CodeSnapshotTooOld is PostgreSQL's snapshot_too_old: a read whose
	// timestamp lies below the history the cluster keeps.
	CodeSnapshotTooOld = "72000"

	// The key service (issue #310): a role past its rate limit
	// (configuration_limit_exceeded: a configured limit, retry later); a
	// blob that does not open (external_routine_invocation_exception, as
	// pgcrypto's decrypt reports a wrong key or damaged data); a
	// connection that may not carry key material
	// (invalid_authorization_specification, as pg_hba refuses an
	// unencrypted connection).
	CodeConfigLimitExceeded  = "53400"
	CodeExternalRoutine      = "39000"
	CodeInvalidAuthorization = "28000"

	// CodeAmbiguousParameter is PostgreSQL's ambiguous_parameter: one
	// parameter deduced as two incompatible types.
	CodeAmbiguousParameter = "42P08"

	// CodeDatatypeMismatch is PostgreSQL's datatype_mismatch: an
	// operator class or collation that does not apply to the column's
	// type.
	CodeDatatypeMismatch = "42804"

	// CodeWindowing is PostgreSQL's windowing_error, and
	// CodeUndefinedParameter its undefined_parameter: an index
	// expression with a window function or a $n.
	CodeWindowing          = "42P20"
	CodeUndefinedParameter = "42P02"
)

// Error is a SQL-level error with a SQLSTATE code.
type Error struct {
	Code string
	Msg  string
	// Detail and Hint are the error's DETAIL and HINT fields ("" =
	// none).
	Detail string
	Hint   string
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
	var ve *catalog.ValueError
	if errors.As(err, &ve) {
		return &Error{Code: ve.Code, Msg: ve.Msg}
	}
	if serr, ok := sealErrorToSQL(err); ok {
		return serr
	}
	var syn *parser.SyntaxError
	if errors.As(err, &syn) {
		return &Error{Code: CodeSyntaxError, Msg: syn.Error()}
	}
	if kvclient.IsRetryable(err) {
		return &Error{Code: CodeSerializationFailure, Msg: restartMessage(err)}
	}
	var kerr *kvpb.Error
	if errors.As(err, &kerr) && kerr.IsBelowGCThreshold() {
		// What is left of a GC-threshold refusal once a transaction that
		// could restart has been given 40001 (kvclient): a read at a
		// timestamp the client chose, below the history kept.
		// PostgreSQL's code for that is snapshot_too_old; retrying at the
		// same timestamp cannot succeed (issue #313).
		return &Error{Code: CodeSnapshotTooOld, Msg: "snapshot too old: the history at the timestamp read has been garbage-collected (AS OF SYSTEM TIME must lie within the table's GC window): " + err.Error()}
	}
	if errors.As(err, &kerr) && kerr.TooLarge != nil {
		return &Error{Code: CodeProgramLimitExceeded, Msg: fmt.Sprintf("the statement writes about %d MiB to one range in a single piece, and a single write can carry %d MiB: a row and what is written with it must stay well below that (store a large value as a large object: lo_create, lowrite)",
			(kerr.TooLarge.Bytes+1<<19)>>20, kerr.TooLarge.Limit>>20)}
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return &Error{Code: CodeQueryCanceled, Msg: "canceling statement due to statement timeout"}
	}
	if errors.Is(err, context.Canceled) {
		return &Error{Code: CodeQueryCanceled, Msg: "canceling statement due to user request"}
	}
	if kvclient.IsLockTimeout(err) {
		return &Error{Code: CodeLockNotAvailable, Msg: "canceling statement due to lock timeout"}
	}
	if kvclient.IsLockNotAvailable(err) {
		return &Error{Code: CodeLockNotAvailable, Msg: "could not obtain lock on row"}
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

// restartMessage is a retryable error's 40001 text: "restart
// transaction: " once, in front. kvclient.RetryableError already
// carries the prefix in its own text, so prefixing it again doubled it
// ("restart transaction: restart transaction: ..."); a wrapped one
// ("loread: restart transaction: ...") has the prefix moved to the
// front.
func restartMessage(err error) string {
	const prefix = "restart transaction: "
	msg := err.Error()
	if strings.HasPrefix(msg, prefix) {
		return msg
	}
	return prefix + strings.Replace(msg, prefix, "", 1)
}
