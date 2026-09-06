package server

import (
	"context"
	"fmt"
	"math"
	"sort"
	"sync"
	"time"
)

// Capacity forecasting (issue #156).
//
// Free space was a point reading: a percentage that turned amber under
// 15% and red under 5%. By the time it is amber the question is "how
// long have I got", and nothing could answer it — although store.disk_free
// is recorded per node and the metrics table keeps a week of it.
//
// This fits a line through that window and reports the slope. The
// discipline is that a forecast is only offered where the series has one
// to give: a flat or rising series says "not filling", never a number,
// and a series too short or too noisy to fit says so rather than
// extrapolating from three samples and a hope.
const (
	// capacityWindow is how far back the fit reaches. A day is long
	// enough to see through a compaction's sawtooth and short enough
	// that last week's workload does not dominate today's answer.
	capacityWindow = 24 * time.Hour
	// capacityMinSamples and capacityMinSpan are the least evidence a
	// forecast is made from: fewer, and the answer is "not yet known".
	capacityMinSamples = 12
	capacityMinSpan    = 2 * time.Hour
	// capacityMinFit is the least coefficient of determination a fit
	// must reach to be reported. Below it the series is not a trend, it
	// is noise, and a number drawn from it would be worse than none.
	capacityMinFit = 0.5
	// capacityCacheFor bounds how often the fit is recomputed: it reads
	// a day of samples per node, which is not work to repeat on a 3s
	// console poll.
	capacityCacheFor = 2 * time.Minute

	// The thresholds at which a projected fill becomes a problem. Disks
	// fill slowly, so the notice is wider than it is for certificates.
	capacityCriticalDays = 3
	capacityWarnDays     = 14
)

// Forecast is one store's disk trend.
type Forecast struct {
	NodeID int `json:"node_id"`
	// Filling is false when the series is flat or rising: the store is
	// not on its way to full, and DaysToFull carries no number.
	Filling bool `json:"filling"`
	// GrowthBytesPerDay is how fast the used space is growing (negative
	// while the store is shrinking).
	GrowthBytesPerDay float64 `json:"growth_bytes_per_day"`
	// DaysToFull is set only when Filling: free space over the growth
	// rate, in days.
	DaysToFull float64 `json:"days_to_full,omitempty"`
	FreeBytes  float64 `json:"free_bytes"`
	// Fit is the fit's coefficient of determination, Samples how many
	// readings it saw, and WindowHours how much time they span. They are
	// carried so the console can say how much the number is worth
	// instead of presenting every projection as equally certain.
	Fit         float64 `json:"fit"`
	Samples     int     `json:"samples"`
	WindowHours float64 `json:"window_hours"`
	// Reason explains an absent forecast in the console's own words.
	Reason string `json:"reason,omitempty"`
}

type capacityCache struct {
	mu         sync.Mutex
	at         time.Time
	forecasts  []Forecast
	refreshing bool
}

// capacityForecasts returns the last computed forecasts and kicks off a
// refresh when they have gone stale. It never blocks the caller on a
// query: the panel that reads this must answer while the metrics table
// is slow or missing, so an empty answer is a valid one.
func (n *Node) capacityForecasts() []Forecast {
	n.capacity.mu.Lock()
	stale := n.capacity.at.IsZero() || time.Since(n.capacity.at) >= capacityCacheFor
	out := n.capacity.forecasts
	if !stale || n.capacity.refreshing {
		n.capacity.mu.Unlock()
		return out
	}
	n.capacity.refreshing = true
	n.capacity.mu.Unlock()
	if err := n.stopper.RunWorker(func(ctx context.Context) {
		f := n.computeForecasts(ctx)
		n.capacity.mu.Lock()
		n.capacity.forecasts, n.capacity.at, n.capacity.refreshing = f, time.Now(), false
		n.capacity.mu.Unlock()
	}); err != nil {
		n.capacity.mu.Lock()
		n.capacity.refreshing = false
		n.capacity.mu.Unlock()
	}
	return out
}

// computeForecasts fits store.disk_free for every node the registry
// knows, from the recorded series.
func (n *Node) computeForecasts(ctx context.Context) []Forecast {
	nodes := n.registry.All()
	if len(nodes) == 0 {
		return nil
	}
	ids := make([]int64, 0, len(nodes))
	for _, nd := range nodes {
		ids = append(ids, int64(nd.NodeID))
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	now := time.Now()
	from := now.Add(-capacityWindow)
	out := make([]Forecast, 0, len(ids))
	for _, id := range ids {
		pts, err := n.rawSeries(ctx, "store.disk_free", id, from, now)
		if err != nil {
			// The table may not exist yet, or the query may fail. Either
			// way the answer is "not known", said once per node rather
			// than turning the panel into an error page.
			out = append(out, Forecast{NodeID: int(id), Reason: "no recorded history for this store yet"})
			continue
		}
		out = append(out, fitForecast(int(id), pts, now))
	}
	return out
}

// fitForecast turns one node's free-space samples into a forecast by
// least squares. Exported behavior, kept a pure function so the rules
// about what will not be claimed are testable without a cluster.
func fitForecast(id int, pts []tsample, now time.Time) Forecast {
	f := Forecast{NodeID: id, Samples: len(pts)}
	if len(pts) > 0 {
		f.FreeBytes = pts[len(pts)-1].v
	}
	if len(pts) < capacityMinSamples {
		f.Reason = fmt.Sprintf("only %d readings recorded; a trend needs at least %d", len(pts), capacityMinSamples)
		return f
	}
	span := time.Duration(pts[len(pts)-1].t - pts[0].t)
	f.WindowHours = span.Hours()
	if span < capacityMinSpan {
		f.Reason = fmt.Sprintf("the readings span only %s; a trend needs at least %s", span.Truncate(time.Minute), capacityMinSpan)
		return f
	}
	// Least squares on (seconds since the first sample, free bytes).
	t0 := pts[0].t
	var n, sx, sy, sxx, sxy float64
	for _, p := range pts {
		x := float64(p.t-t0) / float64(time.Second)
		n, sx, sy, sxx, sxy = n+1, sx+x, sy+p.v, sxx+x*x, sxy+x*p.v
	}
	den := n*sxx - sx*sx
	if den == 0 {
		f.Reason = "every reading has the same timestamp"
		return f
	}
	slope := (n*sxy - sx*sy) / den // free bytes per second
	intercept := (sy - slope*sx) / n

	// How much of the variation the line explains. A store whose free
	// space wanders with compaction rather than trending will not clear
	// this, and gets no number.
	mean := sy / n
	var ssTot, ssRes float64
	for _, p := range pts {
		x := float64(p.t-t0) / float64(time.Second)
		pred := intercept + slope*x
		ssTot += (p.v - mean) * (p.v - mean)
		ssRes += (p.v - pred) * (p.v - pred)
	}
	if ssTot > 0 {
		f.Fit = 1 - ssRes/ssTot
	} else {
		// A perfectly flat series: the fit is exact and the slope zero.
		f.Fit = 1
	}
	f.GrowthBytesPerDay = -slope * 86400 // used space grows as free shrinks
	if f.Fit < capacityMinFit {
		f.Reason = fmt.Sprintf("free space is not trending (fit %.2f); it is moving with the workload rather than filling", f.Fit)
		f.GrowthBytesPerDay = 0
		return f
	}
	if slope >= 0 {
		f.Reason = "free space is flat or rising: this store is not filling"
		return f
	}
	f.Filling = true
	f.DaysToFull = f.FreeBytes / (-slope * 86400)
	if math.IsInf(f.DaysToFull, 0) || math.IsNaN(f.DaysToFull) || f.DaysToFull < 0 {
		f.Filling, f.DaysToFull = false, 0
		f.Reason = "the projection is not a number"
	}
	return f
}

// capacityProblems is the health check: a store on course to fill enters
// the problems panel like everything else, before it is the amber
// percentage that prompts the question too late.
func capacityProblems(fs []Forecast) []Problem {
	var out []Problem
	for _, f := range fs {
		if !f.Filling {
			continue
		}
		switch {
		case f.DaysToFull <= capacityCriticalDays:
			out = append(out, Problem{Severity: SeverityCritical, Check: "disk-filling", Node: f.NodeID, Section: "nodes",
				Summary: fmt.Sprintf("n%d's store is on course to fill in %s at %s/day (%s free); a full disk is a hard stall",
					f.NodeID, fmtDays(f.DaysToFull), fmtBytesGo(uint64(f.GrowthBytesPerDay)), fmtBytesGo(uint64(f.FreeBytes)))})
		case f.DaysToFull <= capacityWarnDays:
			out = append(out, Problem{Severity: SeverityWarning, Check: "disk-filling", Node: f.NodeID, Section: "nodes",
				Summary: fmt.Sprintf("n%d's store is on course to fill in %s at %s/day (%s free)",
					f.NodeID, fmtDays(f.DaysToFull), fmtBytesGo(uint64(f.GrowthBytesPerDay)), fmtBytesGo(uint64(f.FreeBytes)))})
		}
	}
	return out
}

func fmtDays(d float64) string {
	if d < 1 {
		return fmt.Sprintf("%.0f hours", d*24)
	}
	if d < 10 {
		return fmt.Sprintf("%.1f days", d)
	}
	return fmt.Sprintf("%.0f days", d)
}
