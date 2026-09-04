package events

import "testing"

func TestRingOrderBoundsAndFilter(t *testing.T) {
	r := New()
	if got := r.Recent(0, 0, true); len(got) != 0 {
		t.Fatalf("empty ring returned %d events", len(got))
	}
	for i := 0; i < RingSize+10; i++ {
		if i%50 == 0 {
			r.RecordAudit("auth-failure", "principal x")
		} else {
			r.Record("split", "r%d split", i)
		}
	}
	all := r.Recent(0, 0, true)
	if len(all) != RingSize {
		t.Fatalf("ring holds %d, want %d", len(all), RingSize)
	}
	for i := 1; i < len(all); i++ {
		if all[i].Seq != all[i-1].Seq+1 {
			t.Fatalf("events out of order at %d: %d then %d", i, all[i-1].Seq, all[i].Seq)
		}
	}
	if all[0].Seq != 11 {
		t.Fatalf("oldest retained seq %d, want 11 (10 evicted)", all[0].Seq)
	}
	noAudit := r.Recent(0, 0, false)
	for _, ev := range noAudit {
		if ev.Audit {
			t.Fatal("audit event leaked past the filter")
		}
	}
	if len(noAudit) >= len(all) {
		t.Fatal("audit filter removed nothing")
	}
	since := r.Recent(r.Seq()-3, 0, true)
	if len(since) != 3 || since[2].Seq != r.Seq() {
		t.Fatalf("since: got %d events ending at %d, want 3 ending at %d", len(since), since[len(since)-1].Seq, r.Seq())
	}
	if lim := r.Recent(0, 5, true); len(lim) != 5 || lim[4].Seq != r.Seq() {
		t.Fatalf("limit should keep the newest 5, got %d ending at %d", len(lim), lim[len(lim)-1].Seq)
	}
	var nilRing *Ring
	nilRing.Record("x", "no panic on a nil ring")
	if nilRing.Recent(0, 0, true) != nil {
		t.Fatal("nil ring should read as empty")
	}
}
