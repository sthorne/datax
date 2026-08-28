package testcluster

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/sthorne/datax/pkg/keys"
)

func TestSplitAndRouting(t *testing.T) {
	tc := Start(t, 3)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	db := tc.Nodes[0].DB()

	// Data in two "tables".
	for i := 0; i < 5; i++ {
		if err := db.Put(ctx, append(keys.TableDataPrefix(100), fmt.Sprintf("k%d", i)...), []byte(fmt.Sprintf("t100-%d", i))); err != nil {
			t.Fatalf("put: %v", err)
		}
		if err := db.Put(ctx, append(keys.TableDataPrefix(200), fmt.Sprintf("k%d", i)...), []byte(fmt.Sprintf("t200-%d", i))); err != nil {
			t.Fatalf("put: %v", err)
		}
	}

	// Split at the boundary of table 200.
	sr, err := db.AdminSplit(ctx, keys.TableDataPrefix(200))
	if err != nil {
		t.Fatalf("split: %v", err)
	}
	if sr.Left.RangeID != 1 || sr.Right.RangeID == 1 {
		t.Fatalf("unexpected split result: %v / %v", sr.Left, sr.Right)
	}
	if !sr.Left.EndKey.Equal(keys.TableDataPrefix(200)) || !sr.Right.StartKey.Equal(keys.TableDataPrefix(200)) {
		t.Fatalf("split bounds wrong: left end %s right start %s", sr.Left.EndKey, sr.Right.StartKey)
	}

	// Both sides remain readable and writable — including from a node with a
	// cold cache (node 3's client may have stale routing).
	for i, dbi := range []interface {
		Get(context.Context, keys.Key) ([]byte, error)
	}{tc.Nodes[0].DB(), tc.Nodes[2].DB()} {
		v, err := dbi.Get(ctx, append(keys.TableDataPrefix(100), "k3"...))
		if err != nil || string(v) != "t100-3" {
			t.Fatalf("client %d: LHS read after split: %q, %v", i, v, err)
		}
		v, err = dbi.Get(ctx, append(keys.TableDataPrefix(200), "k4"...))
		if err != nil || string(v) != "t200-4" {
			t.Fatalf("client %d: RHS read after split: %q, %v", i, v, err)
		}
	}
	if err := db.Put(ctx, append(keys.TableDataPrefix(200), "post-split"...), []byte("new")); err != nil {
		t.Fatalf("RHS write after split: %v", err)
	}

	// A scan spanning the split point stitches both ranges.
	rows, err := db.Scan(ctx, keys.TableDataPrefix(100), keys.TableDataPrefix(200).PrefixEnd(), 0)
	if err != nil {
		t.Fatalf("cross-range scan: %v", err)
	}
	if len(rows) != 11 {
		t.Fatalf("cross-range scan rows: got %d, want 11", len(rows))
	}

	// A second split of the RHS works too (exercises range ID allocation).
	if _, err := db.AdminSplit(ctx, append(keys.TableDataPrefix(200), "k3"...)); err != nil {
		t.Fatalf("second split: %v", err)
	}
	rows, err = db.Scan(ctx, keys.TableDataPrefix(200), keys.TableDataPrefix(200).PrefixEnd(), 0)
	if err != nil || len(rows) != 6 {
		t.Fatalf("scan after second split: %d rows, %v", len(rows), err)
	}
}

func TestSplitRoutingFromJoinedNode(t *testing.T) {
	tc := Start(t, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	db := tc.Nodes[0].DB()

	if err := db.Put(ctx, append(keys.TableDataPrefix(5), "x"...), []byte("v")); err != nil {
		t.Fatal(err)
	}
	if _, err := db.AdminSplit(ctx, keys.TableDataPrefix(5)); err != nil {
		t.Fatalf("split: %v", err)
	}

	// A node joining after the split must discover the RHS via meta lookup.
	n2 := tc.AddNode("")
	v, err := n2.DB().Get(ctx, append(keys.TableDataPrefix(5), "x"...))
	if err != nil || string(v) != "v" {
		t.Fatalf("joined node read: %q, %v", v, err)
	}
}
