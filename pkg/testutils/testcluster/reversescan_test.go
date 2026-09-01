package testcluster

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/kvclient"
	"github.com/sthorne/datax/pkg/version"
)

// TestReverseScanAcrossRanges: a transactional reverse scan stitches
// range boundaries back-to-front, honors MaxRows with Resume as the
// exclusive end of the next page, and the version gate opens on a
// finalized-v3 cluster. Issue #53 (TS3).
func TestReverseScanAcrossRanges(t *testing.T) {
	tc := Start(t, 3)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	db := tc.Nodes[0].DB()

	key := func(i int) keys.Key { return keys.Key(fmt.Sprintf("rk%02d", i)) }
	for i := 0; i < 50; i++ {
		if err := db.Put(ctx, key(i), []byte(fmt.Sprintf("v%02d", i))); err != nil {
			t.Fatal(err)
		}
	}
	for _, at := range []int{10, 20, 30, 40} {
		if _, err := db.AdminSplit(ctx, key(at)); err != nil {
			t.Fatalf("split at %d: %v", at, err)
		}
	}

	// Full reverse scan: all 50 rows, descending, across 5 ranges.
	err := db.RunTxn(ctx, "revscan", func(ctx context.Context, txn *kvclient.Txn) error {
		rows, err := txn.ReverseScan(ctx, keys.Key("rk00"), keys.Key("rk99"), 0)
		if err != nil {
			return err
		}
		if len(rows) != 50 {
			return fmt.Errorf("rows: %d", len(rows))
		}
		for i, kv := range rows {
			want := key(49 - i)
			if !kv.Key.Equal(want) {
				return fmt.Errorf("row %d: %s, want %s", i, kv.Key, want)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	// Paged: MaxRows spanning a range boundary; chaining pages via the
	// exclusive-end contract covers everything exactly once.
	var got []string
	end := keys.Key("rk99")
	err = db.RunTxn(ctx, "revscan-paged", func(ctx context.Context, txn *kvclient.Txn) error {
		for page := 0; ; page++ {
			if page > 20 {
				return fmt.Errorf("pagination did not terminate")
			}
			rows, err := txn.ReverseScan(ctx, keys.Key("rk00"), end, 7)
			if err != nil {
				return err
			}
			for _, kv := range rows {
				got = append(got, string(kv.Key))
			}
			if len(rows) < 7 {
				return nil
			}
			end = rows[len(rows)-1].Key.Clone()
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 50 || got[0] != "rk49" || got[49] != "rk00" {
		t.Fatalf("paged rows: %d, first %q last %q", len(got), got[0], got[len(got)-1])
	}

	// The gate: open once the fresh cluster's v3 bootstrap version is
	// mirrored; a v2 gate closes it.
	deadline := time.Now().Add(30 * time.Second)
	for !db.ReverseScansOK() {
		if time.Now().After(deadline) {
			t.Fatal("version gate never opened on a v3 cluster")
		}
		time.Sleep(100 * time.Millisecond)
	}
	db.SetVersionGate(func() version.Version { return version.V2 })
	if db.ReverseScansOK() {
		t.Fatal("gate open at v2")
	}
	db.SetVersionGate(func() version.Version { return version.V3 })
	if !db.ReverseScansOK() {
		t.Fatal("gate closed at v3")
	}
}
