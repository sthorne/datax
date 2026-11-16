package sql

import (
	"fmt"
	"testing"

	"github.com/sthorne/datax/pkg/kvclient"
	"github.com/sthorne/datax/pkg/kvpb"
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
