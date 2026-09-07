package events

import (
	"encoding/json"
	"strings"
	"testing"
)

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

// A keyed event is served in full to a reader who may see every table
// its keys belong to, and at its redacted form to one who may not; an
// event without keys is untouched (issue #213).
func TestRedactKeyedEvents(t *testing.T) {
	r := New()
	r.Record("plain", "r1 rebalanced")
	r.RecordKeyed("split", []uint64{5}, `r2 split at /table/users/1/"alice"`, "r2 split at /table/5/1")
	r.RecordKeyed("merge", []uint64{5, 6}, `r3 absorbed r4; now [/table/users/1/"a", /table/orders/1/"b")`, "r3 absorbed r4; now [/table/5/1, /table/6/1)")
	all := Redact(r.Recent(0, 0, true), func(uint64) bool { return true })
	if all[1].Summary != `r2 split at /table/users/1/"alice"` {
		t.Errorf("a reader who sees every table got %q", all[1].Summary)
	}
	sees5 := Redact(r.Recent(0, 0, true), func(id uint64) bool { return id == 5 })
	if sees5[0].Summary != "r1 rebalanced" || sees5[1].Summary != `r2 split at /table/users/1/"alice"` {
		t.Errorf("unexpected: %q, %q", sees5[0].Summary, sees5[1].Summary)
	}
	if sees5[2].Summary != "r3 absorbed r4; now [/table/5/1, /table/6/1)" {
		t.Errorf("an event touching a table the reader may not see was served in full: %q", sees5[2].Summary)
	}
	none := Redact(r.Recent(0, 0, true), func(uint64) bool { return false })
	if none[1].Summary != "r2 split at /table/5/1" {
		t.Errorf("a reader who sees nothing got %q", none[1].Summary)
	}
	// Neither form is a wire field: the ring serves Summary alone.
	raw, _ := json.Marshal(none[1])
	if strings.Contains(string(raw), "alice") || strings.Contains(string(raw), "Redacted") || strings.Contains(string(raw), "Tables") {
		t.Errorf("the redacted event's JSON carries what it should not: %s", raw)
	}
}
