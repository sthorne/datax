package testcluster

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

// TestLargeScanAcrossNodes: a result set larger than gRPC's default
// message limit (4 MiB) comes back whole and promptly through every
// gateway, whichever node leads the range — the range pages its answer
// by bytes and the client stitches the pages (before the fix the scan
// from a non-leader gateway ran into the per-attempt timeout on every
// try and hung until the lease happened to move).
func TestLargeScanAcrossNodes(t *testing.T) {
	tc := Start(t, 3)
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()
	root, err := pgx.Connect(ctx, pgURL(tc, 0))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = root.Close(ctx) }()
	if _, err := root.Exec(ctx, `CREATE TABLE big (k INT8 PRIMARY KEY, pad TEXT)`); err != nil {
		t.Fatal(err)
	}
	// ~30k rows of 300 bytes: ~9 MiB of rows, two pages at the 8 MiB target
	// and past the old 4 MiB limit either way.
	const rows = 30000
	pad := strings.Repeat("x", 300)
	for i := 0; i < rows; i += 500 {
		var sb strings.Builder
		sb.WriteString("INSERT INTO big VALUES ")
		for j := i; j < i+500; j++ {
			if j > i {
				sb.WriteByte(',')
			}
			fmt.Fprintf(&sb, "(%d, '%s')", j, pad)
		}
		if _, err := root.Exec(ctx, sb.String()); err != nil {
			t.Fatal(err)
		}
	}
	wantSum := int64(rows) * int64(rows-1) / 2
	for i := range tc.Nodes {
		conn, err := pgx.Connect(ctx, pgURL(tc, i))
		if err != nil {
			t.Fatal(err)
		}
		for _, q := range []string{
			`SELECT count(*), sum(k) FROM big`,
			`SELECT count(*), sum(k) FROM (SELECT k, pad FROM big ORDER BY k DESC) t`,
		} {
			start := time.Now()
			var n, sum int64
			if err := conn.QueryRow(ctx, q).Scan(&n, &sum); err != nil {
				t.Fatalf("n%d: %s: %v", i+1, q, err)
			}
			if n != rows || sum != wantSum {
				t.Fatalf("n%d: %s: %d rows, sum %d (want %d, %d)", i+1, q, n, sum, rows, wantSum)
			}
			if d := time.Since(start); d > 20*time.Second {
				t.Fatalf("n%d: %s took %s", i+1, q, d)
			}
		}
		// The rows themselves, streamed through pgwire.
		r, err := conn.Query(ctx, `SELECT k FROM big ORDER BY k`)
		if err != nil {
			t.Fatal(err)
		}
		var got int64
		for r.Next() {
			var k int64
			if err := r.Scan(&k); err != nil {
				t.Fatal(err)
			}
			if k != got {
				t.Fatalf("n%d: row %d has key %d", i+1, got, k)
			}
			got++
		}
		r.Close()
		if got != rows {
			t.Fatalf("n%d: streamed %d rows", i+1, got)
		}
		_ = conn.Close(ctx)
	}
}
