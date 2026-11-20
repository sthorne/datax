package sql

import (
	"fmt"
	"strings"
	"testing"

	"github.com/sthorne/datax/pkg/kvclient"
	"github.com/sthorne/datax/pkg/kvpb"
	"github.com/sthorne/datax/pkg/util/hlc"
)

// TestToSQLErrorRestartPrefix: a retryable error's 40001 message says
// "restart transaction: " once, whether it arrives bare or wrapped.
func TestToSQLErrorRestartPrefix(t *testing.T) {
	re := &kvclient.RetryableError{Cause: &kvpb.Error{Message: "TransactionAbortedError(ABORT_REASON_ABORTED_RECORD_FOUND)"}}
	for _, tc := range []struct {
		err  error
		want string
	}{
		{re, "restart transaction: TransactionAbortedError(ABORT_REASON_ABORTED_RECORD_FOUND)"},
		{fmt.Errorf("loread: %w", re), "restart transaction: loread: TransactionAbortedError(ABORT_REASON_ABORTED_RECORD_FOUND)"},
	} {
		serr := ToSQLError(tc.err)
		if serr.Code != CodeSerializationFailure || serr.Msg != tc.want {
			t.Errorf("ToSQLError(%q) = %s %q, want 40001 %q", tc.err, serr.Code, serr.Msg, tc.want)
		}
	}
}

// TestToSQLErrorBelowGCThreshold (issue #374): a GC-threshold refusal is
// a restart (40001) wherever the timestamp was the cluster's, however it
// reaches SQL — as the coordinator's RetryableError, or bare from a
// path the coordinator did not convert, wrapped or not — and a retry
// loop treats it as one. Only a historical read's SnapshotTooOldError
// is snapshot_too_old (72000).
func TestToSQLErrorBelowGCThreshold(t *testing.T) {
	ts, thr := hlc.Timestamp{WallTime: 100}, hlc.Timestamp{WallTime: 200}
	refusal := kvpb.NewBelowGCThresholdError(25, ts, thr)
	untyped := &kvpb.Error{Message: refusal.Message} // from a peer without the typed detail
	for _, tc := range []struct {
		name      string
		err       error
		code      string
		retryable bool
	}{
		{"converted", &kvclient.RetryableError{Cause: refusal}, CodeSerializationFailure, true},
		{"bare", refusal, CodeSerializationFailure, true},
		{"wrapped", fmt.Errorf("rolling back to savepoint %q: %w", "s", refusal), CodeSerializationFailure, true},
		{"untyped", untyped, CodeSerializationFailure, true},
		{"historical", &kvclient.SnapshotTooOldError{Cause: refusal}, CodeSnapshotTooOld, false},
		{"historical wrapped", fmt.Errorf("scan: %w", &kvclient.SnapshotTooOldError{Cause: refusal}), CodeSnapshotTooOld, false},
	} {
		if got := kvclient.IsRetryable(tc.err); got != tc.retryable {
			t.Errorf("%s: IsRetryable = %v, want %v", tc.name, got, tc.retryable)
		}
		serr := ToSQLError(tc.err)
		if serr.Code != tc.code || !strings.Contains(serr.Msg, "is below the GC threshold") {
			t.Errorf("%s: ToSQLError = %s %q, want %s naming the threshold", tc.name, serr.Code, serr.Msg, tc.code)
		}
	}
	if got := refusal.RetryTimestamp(ts); !thr.Less(got) {
		t.Errorf("a restart after the refusal reads at %s, want above the threshold %s", got, thr)
	}
}

// TestImplicitRetryable (issue #392): an implicit statement is re-run on
// a retryable error even once a statement has made it into its 40001
// (*Error) inside the retry loop, and never on a storage-overload shed.
func TestImplicitRetryable(t *testing.T) {
	abort := &kvclient.RetryableError{Cause: &kvpb.Error{Message: "aborted", TxnAborted: &kvpb.TxnAbortedError{}}}
	shed := &kvpb.Error{Message: "shed", TxnRetry: &kvpb.TxnRetryError{}, StorageOverloaded: &kvpb.StorageOverloadedError{}}
	for _, tc := range []struct {
		name string
		err  error
		want bool
	}{
		{"retryable", abort, true},
		{"as its 40001", ToSQLError(abort), true},
		{"wrapped 40001", fmt.Errorf("create: %w", ToSQLError(abort)), true},
		{"40001 without its cause", &Error{Code: CodeSerializationFailure, Msg: "restart transaction"}, true},
		{"other SQL error", newErrf(CodeDuplicateTable, "relation %q already exists", "t"), false},
		{"storage overloaded", shed, false},
		{"storage overloaded, retryable", &kvclient.RetryableError{Cause: shed}, false},
		{"storage overloaded, as its 40001", ToSQLError(&kvclient.RetryableError{Cause: shed}), false},
	} {
		if got := implicitRetryable(tc.err); got != tc.want {
			t.Errorf("%s: implicitRetryable = %v, want %v", tc.name, got, tc.want)
		}
	}
	// A retry loop that only knows kvclient's errors (RunTxn) still sees
	// a 40001 the statement made inside it as retryable.
	if !kvclient.IsRetryable(ToSQLError(abort)) {
		t.Errorf("kvclient.IsRetryable(ToSQLError(retryable)) = false")
	}
}
