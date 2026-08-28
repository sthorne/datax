package testcluster

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/sthorne/datax/pkg/base"
	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/server"
	"github.com/sthorne/datax/pkg/storage"
)

// TestAutoSplitBySize: bulk writes drive a range past the (tiny, test-
// configured) size threshold; the housekeeping split pass divides it at
// row-aligned boundaries, routing self-heals, and every replica agrees on
// the replicated SizeBytes accounting.
func TestAutoSplitBySize(t *testing.T) {
	const threshold = 16 << 10
	tc, engines := StartWithEngines(t, 3, func(c *server.Config) {
		c.SplitSizeThreshold = threshold
	})
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	db := tc.Nodes[0].DB()

	prefix := keys.TableDataPrefix(760)
	value := make([]byte, 200)
	const numKeys = 200
	for i := 0; i < numKeys; i++ {
		k := append(prefix.Clone(), fmt.Sprintf("row-%04d", i)...)
		if err := db.Put(ctx, k, value); err != nil {
			t.Fatal(err)
		}
	}

	// Drive split passes until every range is under the threshold (each
	// pass splits oversized led ranges once, halving them).
	deadline := time.Now().Add(30 * time.Second)
	var descs []kvpbRangeDescs
	for {
		for _, n := range tc.Nodes {
			n.Store().RunAutoSplitOnce(ctx)
		}
		raw, err := tc.ranges(ctx)
		if err == nil && len(raw) >= 3 {
			over := false
			for _, d := range raw {
				for _, n := range tc.Nodes {
					if r, ok := n.Store().GetReplica(d.RangeID); ok && r.IsLeader() && r.SizeBytes() > threshold {
						over = true
					}
				}
			}
			if !over {
				descs = descs[:0]
				for _, d := range raw {
					descs = append(descs, kvpbRangeDescs{id: uint64(d.RangeID), start: d.StartKey, end: d.EndKey})
				}
				break
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("auto-split did not converge: %d ranges", len(raw))
		}
		time.Sleep(200 * time.Millisecond)
	}

	// Interior boundaries are row-aligned keys inside our table.
	for _, d := range descs {
		if len(d.start) > 0 && !keys.Key(d.start).Equal(keys.MinKey) {
			if !keys.Key(d.start).HasPrefix(prefix) {
				continue // the first table-range boundary may sit at a system key
			}
			if len(d.start) != len(prefix)+len("row-0000") {
				t.Fatalf("split boundary %q is not row-aligned", d.start)
			}
		}
	}

	// Routing self-heals: everything is still readable through any node.
	rows, err := tc.Nodes[1].DB().Scan(ctx, prefix, prefix.PrefixEnd(), 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != numKeys {
		t.Fatalf("scan after splits: %d rows, want %d", len(rows), numKeys)
	}

	// Cross-replica SizeBytes agreement: each range's persisted accounting
	// converges to the same value on every replica.
	sizeOf := func(eng *storage.Engine, rangeID uint64) (int64, bool) {
		raw, err := eng.Get(keys.RaftAppliedStateKey(base.RangeID(rangeID)))
		if err != nil || raw == nil {
			return 0, false
		}
		var st struct {
			SizeBytes int64 `json:"size_bytes"`
		}
		if jsonUnmarshal(raw, &st) != nil {
			return 0, false
		}
		return st.SizeBytes, true
	}
	deadline = time.Now().Add(15 * time.Second)
	for {
		agree := true
		for _, d := range descs {
			s0, ok0 := sizeOf(engines[0], d.id)
			s1, ok1 := sizeOf(engines[1], d.id)
			s2, ok2 := sizeOf(engines[2], d.id)
			if !ok0 || !ok1 || !ok2 || s0 != s1 || s1 != s2 {
				agree = false
			}
		}
		if agree {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("replicas disagree on SizeBytes accounting")
		}
		time.Sleep(100 * time.Millisecond)
	}
}

type kvpbRangeDescs struct {
	id         uint64
	start, end keys.Key
}
