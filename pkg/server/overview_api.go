package server

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"

	"github.com/sthorne/datax/pkg/util/events"
)

// OverviewStatus is the /api/overview document (issue #147): what the
// console's overview draws, in one request per poll instead of one per
// section — the cluster document, the health problems and the tail of
// the event ring. The individual endpoints stay; they are the API
// surface and the other views use them. Each section degrades on its
// own: a section that could not be produced is absent and named in
// Errors with the reason (the cluster document's own partial-data note
// is mirrored there too), so one slow subsystem cannot blank the page.
type OverviewStatus struct {
	Cluster ClusterStatus `json:"cluster"`
	Health  *HealthStatus `json:"health,omitempty"`
	Events  *EventsStatus `json:"events,omitempty"`
	// Errors maps a section ("ranges", "health", "events") to why it is
	// missing or partial; empty when everything was produced.
	Errors map[string]string `json:"errors"`
}

// overviewEventsLimit is how much of the event ring the overview carries
// by default (?limit= overrides, bounded by the ring).
const overviewEventsLimit = 50

func (n *Node) serveOverviewAPI(w http.ResponseWriter, req *http.Request) {
	doc := OverviewStatus{Errors: map[string]string{}}
	doc.Cluster = n.clusterDoc(req)
	if doc.Cluster.Error != "" {
		doc.Errors["ranges"] = doc.Cluster.Error
	}
	doc.Health = overviewSection(&doc, "health", func() *HealthStatus { return n.healthDoc(req) })
	doc.Events = overviewSection(&doc, "events", func() *EventsStatus {
		since, _ := strconv.ParseUint(req.URL.Query().Get("since"), 10, 64)
		limit, _ := strconv.Atoi(req.URL.Query().Get("limit"))
		if limit <= 0 || limit > events.RingSize {
			limit = overviewEventsLimit
		}
		ev := &EventsStatus{NodeID: int(n.ident.NodeID), Latest: n.events.Seq(), Events: n.events.Recent(since, limit, doc.Cluster.Principal.Admin)}
		// The operations view reads the same poll as everything else
		// (issue #153); pairing is over the whole ring, not the tail
		// this document carries, so an operation that started before it
		// is still reported as running.
		ev.Operations = operationsFrom(n.events.Recent(0, 0, doc.Cluster.Principal.Admin), doc.Cluster.Now)
		if ev.Events == nil {
			ev.Events = []events.Event{}
		}
		return ev
	})
	w.Header().Set("Content-Type", "application/json")
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	_ = enc.Encode(doc)
}

// overviewSection produces one section, turning a panic in it into an
// Errors entry rather than a failed request.
func overviewSection[T any](doc *OverviewStatus, name string, produce func() *T) (out *T) {
	defer func() {
		if r := recover(); r != nil {
			out = nil
			doc.Errors[name] = fmt.Sprintf("%s unavailable: %v", name, r)
		}
	}()
	return produce()
}
