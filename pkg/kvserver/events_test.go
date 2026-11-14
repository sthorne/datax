package kvserver

import (
	"strings"
	"testing"

	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/util/events"
)

// TestRecordKeyedCarriesItsKeys (issue #280): this layer has no catalog,
// so a keyed event's summary prints its keys raw. The event also carries
// the summary with a mark where each key goes and the keys themselves,
// which is what lets the server print them by name for a reader who may
// see them. The marks and the keys have to agree — the server renders
// nothing when they do not — and the keys have to be copies, because a
// boundary key is often a slice of a buffer that is reused.
func TestRecordKeyedCarriesItsKeys(t *testing.T) {
	ring := events.New()
	left := append(keys.TableDataPrefix(7), "left"...)
	right := append(keys.TableDataPrefix(9), "right"...)
	recordKeyed(ring, "split", "%s split at %s; %s takes [%s, %s) after %d bytes", "r2", left, "r3", left, right, 4096)
	evs := ring.Recent(0, 0, true)
	if len(evs) != 1 {
		t.Fatalf("recorded %d events, want 1", len(evs))
	}
	ev := evs[0]
	if got := strings.Count(ev.Template, events.KeyMark); got != 3 || len(ev.Keys) != 3 {
		t.Fatalf("template %q has %d marks for %d keys, want 3 and 3", ev.Template, got, len(ev.Keys))
	}
	if want := "r2 split at " + events.KeyMark + "; r3 takes [" + events.KeyMark + ", " + events.KeyMark + ") after 4096 bytes"; ev.Template != want {
		t.Errorf("template %q, want %q", ev.Template, want)
	}
	if string(ev.Keys[0]) != string(left) || string(ev.Keys[1]) != string(left) || string(ev.Keys[2]) != string(right) {
		t.Errorf("keys %q are not the split's, in order", ev.Keys)
	}
	if len(ev.Tables) != 3 || ev.Tables[0] != 7 || ev.Tables[2] != 9 {
		t.Errorf("tables %v, want the table of each key", ev.Tables)
	}
	// The raw and prefix forms are what they were: nothing about who is
	// served which changes here.
	if !strings.Contains(ev.Summary, left.String()) || strings.Contains(ev.Redacted, "left") {
		t.Errorf("summary %q / redacted %q are not the full and prefix forms", ev.Summary, ev.Redacted)
	}
	// A copy: scribbling on the caller's buffer afterwards changes nothing.
	left[len(left)-1] = 'X'
	if string(ev.Keys[0]) == string(left) {
		t.Error("the event holds the caller's key slice rather than a copy of it")
	}
}
