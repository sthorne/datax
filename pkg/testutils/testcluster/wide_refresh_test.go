package testcluster

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/kvpb"
)

// TestWideReadSetRefreshesPrecisely: a transaction that read 1,000 keys
// one by one — a 1,000-row INSERT's uniqueness probes — refreshes exactly
// those keys, in one batch, when its timestamp moves. Its reads used to be
// condensed past 512 into spans covering the keys between them, so a row
// another worker committed next to its keys failed the refresh (every
// push of a bulk INSERT became a restart), and the refresh sent one batch
// per span, one after another. A write to a key it did read still fails
// the refresh.
func TestWideReadSetRefreshesPrecisely(t *testing.T) {
	tc := Start(t, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	db := tc.Nodes[0].DB()
	key := func(i int) keys.Key { return append(keys.TableDataPrefix(433), fmt.Sprintf("%06d", i)...) }
	const n = 1000
	read := make([]keys.Key, n)
	for i := range read {
		read[i] = key(2 * i) // the even keys; the odd ones lie between them
	}

	for _, c := range []struct {
		name    string
		write   keys.Key
		refresh bool
	}{
		{"neighbour written", key(2*(n/2) + 1), true},
		{"read key written", key(2 * (n / 2)), false},
	} {
		t.Run(c.name, func(t *testing.T) {
			reader := db.NewTxn("reader")
			defer func() { _ = reader.Rollback(ctx) }()
			if _, err := reader.GetBatch(ctx, read); err != nil {
				t.Fatal(err)
			}
			writer := db.NewTxn("writer")
			if err := writer.Put(ctx, c.write, []byte("v")); err != nil {
				t.Fatal(err)
			}
			if err := writer.Commit(ctx); err != nil {
				t.Fatal(err)
			}
			var batches, refreshes atomic.Int64
			db.TestingSetSendObserver(func(_ context.Context, ba *kvpb.BatchRequest, _ *kvpb.BatchResponse) {
				m := int64(0)
				for _, ru := range ba.Requests {
					if ru.Refresh != nil {
						m++
					}
				}
				if m > 0 {
					batches.Add(1)
					refreshes.Add(m)
				}
			})
			err := reader.AdvanceTo(ctx, db.Clock().Now())
			db.TestingSetSendObserver(nil)
			if c.refresh {
				if err != nil {
					t.Fatalf("refresh failed over a write to a key the transaction did not read: %v", err)
				}
				if batches.Load() != 1 || refreshes.Load() != n {
					t.Fatalf("refresh sent %d requests in %d batches, want %d in 1", refreshes.Load(), batches.Load(), n)
				}
				return
			}
			if err == nil {
				t.Fatal("refresh passed over a write to a key the transaction read")
			}
		})
	}
}
