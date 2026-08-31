// Package hlc implements a hybrid logical clock: physical wall time combined
// with a logical counter, giving timestamps that are monotonic on each node
// and consistent with causality across nodes (every message carries a
// timestamp that ratchets the receiver's clock).
package hlc

import (
	"fmt"
	"sync"
	"time"
)

// Timestamp is an HLC reading. The zero value is "empty" and sorts before
// every real timestamp.
type Timestamp struct {
	WallTime int64 `json:"wall"`    // nanoseconds since Unix epoch
	Logical  int32 `json:"logical"` // ticks within the same wall time
}

func (t Timestamp) IsEmpty() bool { return t.WallTime == 0 && t.Logical == 0 }

func (t Timestamp) Less(u Timestamp) bool {
	return t.WallTime < u.WallTime || (t.WallTime == u.WallTime && t.Logical < u.Logical)
}

func (t Timestamp) LessEq(u Timestamp) bool { return !u.Less(t) }

func (t Timestamp) Equal(u Timestamp) bool { return t == u }

// Next returns the smallest timestamp strictly greater than t.
func (t Timestamp) Next() Timestamp {
	if t.Logical == int32(^uint32(0)>>1) {
		return Timestamp{WallTime: t.WallTime + 1}
	}
	return Timestamp{WallTime: t.WallTime, Logical: t.Logical + 1}
}

// AddNanos returns t advanced by d nanoseconds, logical reset.
func (t Timestamp) AddNanos(d int64) Timestamp {
	return Timestamp{WallTime: t.WallTime + d}
}

// Forward returns the larger of t and u.
func (t Timestamp) Forward(u Timestamp) Timestamp {
	if t.Less(u) {
		return u
	}
	return t
}

func (t Timestamp) String() string {
	return fmt.Sprintf("%d.%09d,%d", t.WallTime/1e9, t.WallTime%1e9, t.Logical)
}

// Clock is a hybrid logical clock. Safe for concurrent use.
type Clock struct {
	physical  func() int64 // physical clock, ns since epoch
	maxOffset time.Duration

	mu   sync.Mutex
	last Timestamp
}

// NewClock creates a clock from the given physical source (nil for real
// time). maxOffset is the tolerated physical skew between nodes.
func NewClock(physical func() int64, maxOffset time.Duration) *Clock {
	if physical == nil {
		physical = func() int64 { return time.Now().UnixNano() }
	}
	return &Clock{physical: physical, maxOffset: maxOffset}
}

func (c *Clock) MaxOffset() time.Duration { return c.maxOffset }

// Now returns a timestamp strictly greater than every previous Now or
// Update result on this clock.
func (c *Clock) Now() Timestamp {
	c.mu.Lock()
	defer c.mu.Unlock()
	phys := c.physical()
	if phys > c.last.WallTime {
		c.last = Timestamp{WallTime: phys}
	} else {
		c.last = c.last.Next()
	}
	return c.last
}

// Update ratchets the clock forward to at least remote. It returns an error
// if remote's wall time is further than maxOffset ahead of physical time —
// the caller should treat that as fatal, because a clock that far off
// undermines the uncertainty-interval guarantee.
func (c *Clock) Update(remote Timestamp) error {
	if remote.IsEmpty() {
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	phys := c.physical()
	if remote.WallTime > phys+int64(c.maxOffset) {
		return fmt.Errorf("remote clock %s is %.3fs ahead of local physical clock; exceeds max offset %s",
			remote, float64(remote.WallTime-phys)/1e9, c.maxOffset)
	}
	if c.last.Less(remote) {
		c.last = remote
	}
	return nil
}
