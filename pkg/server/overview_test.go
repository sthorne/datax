package server

import (
	"strings"
	"testing"
)

// TestOverviewSectionDegrades (issue #147): a section of /api/overview
// that fails is absent and named in the errors map, rather than failing
// the whole request.
func TestOverviewSectionDegrades(t *testing.T) {
	doc := OverviewStatus{Errors: map[string]string{}}
	fine := overviewSection(&doc, "health", func() *HealthStatus { return &HealthStatus{Checks: 3} })
	if fine == nil || fine.Checks != 3 || len(doc.Errors) != 0 {
		t.Fatalf("a healthy section: %+v, errors %v", fine, doc.Errors)
	}
	broken := overviewSection(&doc, "events", func() *EventsStatus { panic("ring unavailable") })
	if broken != nil {
		t.Fatalf("a failed section produced %+v", broken)
	}
	if e := doc.Errors["events"]; !strings.Contains(e, "ring unavailable") {
		t.Fatalf("errors: %v", doc.Errors)
	}
}
