package server

import (
	"testing"

	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/util/events"
)

// viewEvents lays an event's keys between the pieces of its template,
// in order. The cluster test reads a real split and checks that its
// table is named and no key is left by number; it would pass with the
// pieces laid in any order (the QA re-review of #285 walked them in
// reverse), so the sentence itself is read here.
func TestViewEventsPutsEachKeyWhereItsMarkIs(t *testing.T) {
	n := &Node{}
	all := keyViewer{all: true}
	k := [][]byte{keys.TableDataPrefix(4201), keys.TableDataPrefix(4202), keys.TableDataPrefix(4203)}
	p := func(i int) string { return keys.Pretty(keys.Key(k[i])) }
	m := events.KeyMark

	got := n.viewEvents(all, []events.Event{{
		Kind:     "split",
		Summary:  "raw",
		Template: "r2 split at " + m + "; r3 takes [" + m + ", " + m + ")",
		Keys:     k,
	}})
	if want := "r2 split at " + p(0) + "; r3 takes [" + p(1) + ", " + p(2) + ")"; len(got) != 1 || got[0].Summary != want {
		t.Errorf("viewEvents printed %q, want %q", got[0].Summary, want)
	}

	// A mark first and a mark last: no piece is dropped at either end.
	got = n.viewEvents(all, []events.Event{{Summary: "raw", Template: m + " into " + m, Keys: k[:2]}})
	if want := p(0) + " into " + p(1); got[0].Summary != want {
		t.Errorf("viewEvents printed %q, want %q", got[0].Summary, want)
	}

	// Marks and keys that disagree are not guessed at: the summary the
	// event was recorded with stands.
	for _, ev := range []events.Event{
		{Summary: "raw", Template: "a " + m + " b " + m, Keys: k},
		{Summary: "raw", Template: "a " + m + " b " + m, Keys: k[:1]},
		{Summary: "raw", Template: "no marks", Keys: k[:1]},
		{Summary: "raw", Keys: k},
	} {
		if got := n.viewEvents(all, []events.Event{ev}); got[0].Summary != "raw" {
			t.Errorf("template %q with %d keys printed %q, want the recorded summary", ev.Template, len(ev.Keys), got[0].Summary)
		}
	}
}
