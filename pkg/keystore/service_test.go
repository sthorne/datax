package keystore

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/sthorne/datax/pkg/sql"
)

// TestServiceRateLimit (issue #310): each role has a token bucket of its
// own on a node, a second's worth deep, refilled at the rate; a call past
// it is refused with 53400; the rate is read once a lease, and 0 is no
// limit.
func TestServiceRateLimit(t *testing.T) {
	ctx := context.Background()
	clk := &fakeClock{now: time.Unix(1000, 0)}
	rate, reads := 3, 0
	s := NewService(ServiceConfig{Store: &Store{}, Lease: 10 * time.Second, Now: clk.Now,
		Rate: func(context.Context) (int, error) { reads++; return rate, nil }})
	for i := 0; i < 3; i++ {
		if err := s.allow(ctx, "app"); err != nil {
			t.Fatalf("call %d: %v", i, err)
		}
	}
	err := s.allow(ctx, "app")
	var se *sql.Error
	if !errors.As(err, &se) || se.Code != sql.CodeConfigLimitExceeded {
		t.Fatalf("past the burst: %v", err)
	}
	if err := s.allow(ctx, "other"); err != nil {
		t.Fatalf("another role: %v", err)
	}
	clk.Add(400 * time.Millisecond)
	if err := s.allow(ctx, "app"); err != nil {
		t.Fatalf("after a refill: %v", err)
	}
	rate = 0
	clk.Add(5 * time.Second)
	// Still 3 a second within the lease: the setting is read once a
	// lease (the bucket has refilled meanwhile).
	if err := s.allow(ctx, "app"); err != nil {
		t.Fatalf("within the lease: %v", err)
	}
	clk.Add(5 * time.Second)
	for i := 0; i < 100; i++ {
		if err := s.allow(ctx, "app"); err != nil {
			t.Fatalf("no limit: %v", err)
		}
	}
	if reads != 2 {
		t.Fatalf("the rate was read %d times, want 2", reads)
	}
}
