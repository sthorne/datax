package advisory

import "testing"

// TestKeys: the two key forms name locks as PostgreSQL's pg_locks
// shows them (classid, objid, objsubid), and never the same lock.
func TestKeys(t *testing.T) {
	for _, tt := range []struct {
		got, want Key
	}{
		{Int64Key("d", 50), Key{"d", 0, 50, 1}},
		{Int64Key("d", -1), Key{"d", 0xffffffff, 0xffffffff, 1}},
		{Int64Key("d", 1<<32|2), Key{"d", 1, 2, 1}},
		{Int32Key("d", 1, 2), Key{"d", 1, 2, 2}},
		{Int32Key("d", -1, -2), Key{"d", 0xffffffff, 0xfffffffe, 2}},
	} {
		if tt.got != tt.want {
			t.Errorf("got %+v, want %+v", tt.got, tt.want)
		}
	}
	if Int64Key("d", 1<<32|2) == Int32Key("d", 1, 2) {
		t.Error("the bigint and the int4-pair forms name the same lock")
	}
	if Int64Key("a", 1) == Int64Key("b", 1) {
		t.Error("locks of different databases are the same lock")
	}
	if Exclusive.String() != "ExclusiveLock" || Share.String() != "ShareLock" {
		t.Errorf("mode names %q %q", Exclusive, Share)
	}
}
