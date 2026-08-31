package hlc

import (
	"testing"
	"time"
)

func TestTimestampOrdering(t *testing.T) {
	a := Timestamp{WallTime: 5, Logical: 0}
	b := Timestamp{WallTime: 5, Logical: 1}
	c := Timestamp{WallTime: 6, Logical: 0}
	if !a.Less(b) || !b.Less(c) || c.Less(a) {
		t.Fatal("ordering broken")
	}
	if !a.Next().Equal(b) {
		t.Fatalf("Next: got %s", a.Next())
	}
	if !(Timestamp{}).IsEmpty() || a.IsEmpty() {
		t.Fatal("IsEmpty broken")
	}
	if a.Forward(c) != c || c.Forward(a) != c {
		t.Fatal("Forward broken")
	}
}

func TestClockMonotonic(t *testing.T) {
	phys := int64(1000)
	c := NewClock(func() int64 { return phys }, time.Second)
	prev := c.Now()
	for i := 0; i < 100; i++ {
		// Physical clock frozen: logical must climb.
		cur := c.Now()
		if !prev.Less(cur) {
			t.Fatalf("not monotonic: %s then %s", prev, cur)
		}
		prev = cur
	}
	phys = 5000
	cur := c.Now()
	if cur.WallTime != 5000 || cur.Logical != 0 {
		t.Fatalf("physical advance not taken: %s", cur)
	}
}

func TestClockUpdate(t *testing.T) {
	phys := int64(1000)
	c := NewClock(func() int64 { return phys }, time.Duration(500))
	if err := c.Update(Timestamp{WallTime: 1400}); err != nil {
		t.Fatalf("within offset: %v", err)
	}
	now := c.Now()
	if now.WallTime != 1400 || now.Logical != 1 {
		t.Fatalf("clock not ratcheted: %s", now)
	}
	if err := c.Update(Timestamp{WallTime: 2000}); err == nil {
		t.Fatal("expected max-offset violation")
	}
}
