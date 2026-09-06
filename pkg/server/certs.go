package server

import (
	"crypto/x509"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/sthorne/datax/pkg/security"
)

// Certificate expiry (issue #156).
//
// --certs-dir turns on mutual internode TLS and SQL TLS, and nothing
// reported when any of it expired. A node certificate that lapses takes
// the node out of the cluster; a lapsed CA takes the cluster out. The
// dates were sitting in certificates already parsed and loaded, so this
// reports them three ways: as a Prometheus gauge, so an alert fires
// without anyone opening the console; as health problems, so they reach
// the problems panel like everything else; and on the security view.
const (
	// certWarnBefore and certCriticalBefore are how far ahead an expiry
	// is worth saying something about. Thirty days is enough notice to
	// rotate through a change window; seven is enough to do it today.
	certWarnBefore     = 30 * 24 * time.Hour
	certCriticalBefore = 7 * 24 * time.Hour
)

// certTracker holds what this node knows about its certificates: the
// static material loaded at startup, and the client certificates that
// have actually been presented to the HTTP listener.
//
// The client half is bounded and is a record of what reached this node,
// not a directory of who could: an identity that stops connecting ages
// out with the node's memory of it, which is the honest scope for
// something derived from observed traffic.
type certTracker struct {
	loaded []security.CertInfo

	mu      sync.Mutex
	clients map[string]security.CertInfo
}

// certClientMax bounds the observed-client table. Past it, further
// identities are not recorded rather than growing the map without limit;
// the console says so rather than implying the list is complete.
const certClientMax = 100

func newCertTracker(loaded []security.CertInfo) *certTracker {
	return &certTracker{loaded: loaded, clients: map[string]security.CertInfo{}}
}

// observeClient records a client certificate presented to the HTTP
// listener, keyed by its subject so one identity is one row however
// often it connects.
func (t *certTracker) observeClient(c *x509.Certificate) {
	if t == nil || c == nil {
		return
	}
	info := security.CertInfoFrom(c)
	t.mu.Lock()
	defer t.mu.Unlock()
	if _, seen := t.clients[info.Subject]; !seen && len(t.clients) >= certClientMax {
		return
	}
	t.clients[info.Subject] = info
}

// all returns the loaded certificates followed by the observed clients,
// soonest expiry first, and whether the client table is full.
func (t *certTracker) all() (out []security.CertInfo, clientsFull bool) {
	if t == nil {
		return nil, false
	}
	out = append(out, t.loaded...)
	t.mu.Lock()
	for _, c := range t.clients {
		out = append(out, c)
	}
	clientsFull = len(t.clients) >= certClientMax
	t.mu.Unlock()
	sort.Slice(out, func(i, j int) bool {
		if !out[i].NotAfter.Equal(out[j].NotAfter) {
			return out[i].NotAfter.Before(out[j].NotAfter)
		}
		return out[i].Subject < out[j].Subject
	})
	return out, clientsFull
}

// certExpiryCollector publishes datax_cert_expiry_seconds{kind,subject}
// — seconds until each certificate expires, negative once it has. It is
// computed on each scrape rather than sampled, because the value is a
// function of the clock and nothing else.
type certExpiryCollector struct {
	desc *prometheus.Desc
	node *Node
}

func newCertExpiryCollector(n *Node) *certExpiryCollector {
	return &certExpiryCollector{
		desc: prometheus.NewDesc("datax_cert_expiry_seconds",
			"Seconds until the certificate expires; negative once it has.",
			[]string{"kind", "subject"}, nil),
		node: n,
	}
}

func (c *certExpiryCollector) Describe(ch chan<- *prometheus.Desc) { ch <- c.desc }

func (c *certExpiryCollector) Collect(ch chan<- prometheus.Metric) {
	certs, _ := c.node.certs.all()
	now := time.Now()
	for _, ci := range certs {
		ch <- prometheus.MustNewConstMetric(c.desc, prometheus.GaugeValue,
			ci.NotAfter.Sub(now).Seconds(), ci.Kind, ci.Subject)
	}
}

// certProblems is the health check: an expiring certificate is a
// scheduled outage, so it is reported before it happens rather than
// after. An already-expired one is critical whatever its kind.
func certProblems(certs []security.CertInfo, now time.Time) []Problem {
	var out []Problem
	for _, ci := range certs {
		left := ci.NotAfter.Sub(now)
		switch {
		case left <= 0:
			out = append(out, Problem{Severity: SeverityCritical, Check: "cert-expired", Section: "security",
				Summary: fmt.Sprintf("the %s certificate for %q expired %s ago; %s",
					ci.Kind, ci.Subject, fmtCertDuration(-left), certConsequence(ci.Kind))})
		case left <= certCriticalBefore:
			out = append(out, Problem{Severity: SeverityCritical, Check: "cert-expiring", Section: "security",
				Summary: fmt.Sprintf("the %s certificate for %q expires in %s; %s",
					ci.Kind, ci.Subject, fmtCertDuration(left), certConsequence(ci.Kind))})
		case left <= certWarnBefore:
			out = append(out, Problem{Severity: SeverityWarning, Check: "cert-expiring", Section: "security",
				Summary: fmt.Sprintf("the %s certificate for %q expires in %s; rotate it before the window closes",
					ci.Kind, ci.Subject, fmtCertDuration(left))})
		}
	}
	return out
}

// certConsequence says what is actually lost, because "a certificate is
// expiring" is not a reason to act and "the cluster stops" is.
func certConsequence(kind string) string {
	switch kind {
	case "ca":
		return "every node authenticates against this CA, so its lapse stops the whole cluster"
	case "node":
		return "this node cannot reach its peers or serve SQL without it"
	default:
		return "this client can no longer authenticate by certificate"
	}
}

func fmtCertDuration(d time.Duration) string {
	if d < 0 {
		d = -d
	}
	if days := int(d.Hours() / 24); days >= 1 {
		return fmt.Sprintf("%dd", days)
	}
	if h := int(d.Hours()); h >= 1 {
		return fmt.Sprintf("%dh", h)
	}
	return fmt.Sprintf("%dm", int(d.Minutes()))
}
