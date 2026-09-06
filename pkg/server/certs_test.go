package server

import (
	"crypto/x509"
	"crypto/x509/pkix"
	"strings"
	"testing"
	"time"

	"github.com/sthorne/datax/pkg/security"
)

func certAt(kind, subject string, notAfter time.Time) security.CertInfo {
	return security.CertInfo{Kind: kind, Subject: subject,
		NotBefore: notAfter.AddDate(-1, 0, 0), NotAfter: notAfter}
}

// TestCertProblems (issue #156): the thresholds, and — the point of the
// check — that the summary says what the lapse would cost. "A
// certificate is expiring" is not a reason to act; "the cluster stops"
// is.
func TestCertProblems(t *testing.T) {
	now := time.Now()
	for _, tc := range []struct {
		name     string
		cert     security.CertInfo
		want     string
		check    string
		mentions string
	}{
		{"comfortable", certAt("node", "n1", now.AddDate(0, 0, 90)), "", "", ""},
		{"a month out warns", certAt("node", "n1", now.AddDate(0, 0, 20)), SeverityWarning, "cert-expiring", "rotate"},
		{"a week out is critical", certAt("node", "n1", now.AddDate(0, 0, 3)), SeverityCritical, "cert-expiring", "peers"},
		{"an expired one is critical", certAt("node", "n1", now.AddDate(0, 0, -1)), SeverityCritical, "cert-expired", "peers"},
		{"the CA names the cluster", certAt("ca", "datax", now.AddDate(0, 0, 3)), SeverityCritical, "cert-expiring", "whole cluster"},
	} {
		got := certProblems([]security.CertInfo{tc.cert}, now)
		if tc.want == "" {
			if len(got) != 0 {
				t.Errorf("%s: expected no problem, got %+v", tc.name, got)
			}
			continue
		}
		if len(got) != 1 {
			t.Errorf("%s: got %d problems, want 1: %+v", tc.name, len(got), got)
			continue
		}
		if got[0].Severity != tc.want || got[0].Check != tc.check {
			t.Errorf("%s: got %s/%s, want %s/%s", tc.name, got[0].Severity, got[0].Check, tc.want, tc.check)
		}
		if got[0].Section != "security" {
			t.Errorf("%s: section %q, want security", tc.name, got[0].Section)
		}
		if !strings.Contains(got[0].Summary, tc.mentions) {
			t.Errorf("%s: summary does not say what is lost (%q): %q", tc.name, tc.mentions, got[0].Summary)
		}
	}
}

// A node with no certificates — an insecure cluster — produces no
// problems, rather than a problem about a certificate that does not
// exist.
func TestCertProblemsInsecure(t *testing.T) {
	if p := certProblems(nil, time.Now()); len(p) != 0 {
		t.Fatalf("insecure clusters have nothing to expire: %+v", p)
	}
}

// TestCertTracker: the loaded material is reported as-is, observed
// clients are folded by subject rather than by connection, and the
// observed table is bounded.
func TestCertTracker(t *testing.T) {
	now := time.Now()
	tr := newCertTracker([]security.CertInfo{
		certAt("node", "n1", now.AddDate(0, 0, 60)),
		certAt("ca", "datax", now.AddDate(1, 0, 0)),
	})
	got, full := tr.all()
	if len(got) != 2 || full {
		t.Fatalf("got %d certs (full=%v): %+v", len(got), full, got)
	}
	// Soonest expiry first: the one to act on leads.
	if got[0].Kind != "node" {
		t.Errorf("expected the soonest expiry first, got %+v", got)
	}

	mk := func(cn string) *x509.Certificate {
		return &x509.Certificate{Subject: pkix.Name{CommonName: cn},
			NotBefore: now.AddDate(0, 0, -1), NotAfter: now.AddDate(0, 0, 10)}
	}
	// One identity connecting repeatedly is one row.
	for i := 0; i < 5; i++ {
		tr.observeClient(mk("app"))
	}
	tr.observeClient(mk("reports"))
	got, _ = tr.all()
	if len(got) != 4 {
		t.Fatalf("two loaded plus two client identities, got %d: %+v", len(got), got)
	}
	clients := 0
	for _, c := range got {
		if c.Kind == "client" {
			clients++
		}
	}
	if clients != 2 {
		t.Errorf("five connections from two identities are two rows, got %d", clients)
	}

	// Bounded: past the limit, further identities are not recorded, and
	// the tracker says the list is a sample.
	for i := 0; i < certClientMax+10; i++ {
		tr.observeClient(mk("user" + string(rune('a'+i%26)) + string(rune('a'+i/26))))
	}
	got, full = tr.all()
	if !full {
		t.Errorf("the client table should report itself full")
	}
	clients = 0
	for _, c := range got {
		if c.Kind == "client" {
			clients++
		}
	}
	if clients > certClientMax {
		t.Errorf("the client table grew past its bound: %d", clients)
	}

	// A nil certificate is a no-op rather than a panic.
	tr.observeClient(nil)
}

// A tracker holding nothing answers rather than panicking: insecure mode
// has one, and the health check and the collector both read it.
func TestCertTrackerEmpty(t *testing.T) {
	tr := newCertTracker(nil)
	got, full := tr.all()
	if len(got) != 0 || full {
		t.Fatalf("%+v %v", got, full)
	}
}
