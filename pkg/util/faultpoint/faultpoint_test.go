package faultpoint

import "testing"

// An unarmed process passes every point; arming happens only through the
// environment at init (exercised by pkg/testutils/crash).
func TestUnarmed(t *testing.T) {
	if Armed() != "" {
		t.Skip("armed by the environment")
	}
	for i := 0; i < 1000; i++ {
		Hit("raft-apply")
	}
}
