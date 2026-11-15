package main

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/sthorne/datax/pkg/server"
)

// TestDebugAudit (issue #293): `datax debug audit` end to end against a
// running node over SQL — export as JSON lines, heads, and verify from
// the cluster and from the export, with and without heads saved
// earlier; an export with a line changed or removed does not verify.
func TestDebugAudit(t *testing.T) {
	n, err := server.Start(server.Config{
		Listen: "127.0.0.1:0", PGListen: "127.0.0.1:0", Dir: t.TempDir(), BootstrapSelf: true,
		GCInterval: -1, MetricsRecordInterval: -1, AuditFlushInterval: 20 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(n.Stop)
	url := "postgres://root@" + n.SQLAddr() + "/datax?sslmode=disable"
	for _, name := range []string{"cli_audit_a", "cli_audit_b", "cli_audit_c"} {
		backupTestSQL(t, n, `CREATE ROLE `+name+` LOGIN PASSWORD 'cli-secret-pw'`)
	}

	work := t.TempDir()
	export, heads := filepath.Join(work, "audit.jsonl"), filepath.Join(work, "heads.json")
	deadline := time.Now().Add(30 * time.Second)
	for {
		if err := debugAudit([]string{"export", "--url", url, "--out", export}, &bytes.Buffer{}); err != nil {
			t.Fatalf("export: %v", err)
		}
		raw, err := os.ReadFile(export)
		if err != nil {
			t.Fatal(err)
		}
		if strings.Contains(string(raw), `"target":"cli_audit_c"`) {
			if strings.Contains(string(raw), "cli-secret-pw") {
				t.Fatalf("the export holds a password:\n%s", raw)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("the records never reached the table:\n%s", raw)
		}
		time.Sleep(50 * time.Millisecond)
	}
	if err := debugAudit([]string{"heads", "--url", url, "--out", heads}, &bytes.Buffer{}); err != nil {
		t.Fatalf("heads: %v", err)
	}
	if raw, _ := os.ReadFile(heads); !strings.Contains(string(raw), `"node": 1`) || !strings.Contains(string(raw), `"hash": "`) {
		t.Fatalf("heads: %s", raw)
	}
	backupTestSQL(t, n, `CREATE ROLE cli_audit_later`)

	var out bytes.Buffer
	if err := debugAudit([]string{"verify", "--url", url}, &out); err != nil || !strings.Contains(out.String(), "node 1: ok") || !strings.Contains(out.String(), "against the table alone") {
		t.Fatalf("verify from the cluster: %v\n%s", err, out.String())
	}
	out.Reset()
	if err := debugAudit([]string{"verify", "--url", url, "--heads", heads}, &out); err != nil || strings.Contains(out.String(), "against the table alone") {
		t.Fatalf("verify from the cluster against saved heads: %v\n%s", err, out.String())
	}
	out.Reset()
	if err := debugAudit([]string{"verify", "--file", export, "--heads", heads}, &out); err != nil {
		t.Fatalf("verify from the export: %v\n%s", err, out.String())
	}

	// The export altered, then shortened: neither verifies, and the
	// command says so with its exit status.
	raw, err := os.ReadFile(export)
	if err != nil {
		t.Fatal(err)
	}
	altered := filepath.Join(work, "altered.jsonl")
	if err := os.WriteFile(altered, bytes.Replace(raw, []byte(`"target":"cli_audit_b"`), []byte(`"target":"cli_audit_x"`), 1), 0o600); err != nil {
		t.Fatal(err)
	}
	out.Reset()
	if err := debugAudit([]string{"verify", "--file", altered}, &out); err == nil || !strings.Contains(out.String(), "BROKEN") || !strings.Contains(out.String(), "it was altered") {
		t.Fatalf("an altered export verified: %v\n%s", err, out.String())
	}
	lines := bytes.SplitAfter(raw, []byte("\n"))
	short := filepath.Join(work, "short.jsonl")
	if err := os.WriteFile(short, bytes.Join(lines[:len(lines)-2], nil), 0o600); err != nil {
		t.Fatal(err)
	}
	out.Reset()
	if err := debugAudit([]string{"verify", "--file", short}, &out); err != nil {
		t.Fatalf("an export missing only its newest record cannot be faulted without a head, and was: %v\n%s", err, out.String())
	}
	out.Reset()
	if err := debugAudit([]string{"verify", "--file", short, "--heads", heads}, &out); err == nil || !strings.Contains(out.String(), "newest records were removed") {
		t.Fatalf("a shortened export verified against saved heads: %v\n%s", err, out.String())
	}
	if err := debugAudit([]string{"rewrite"}, &out); err == nil {
		t.Fatal("an unknown subcommand was accepted")
	}
}
