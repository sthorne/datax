package main

import (
	"bytes"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestBenchHelpers(t *testing.T) {
	if got := scaleDurations([]string{"--preload", "10", "--duration", "20s", "-duration", "1m"}, 0.1); got[3] != "2s" || got[5] != "6s" {
		t.Fatalf("scaleDurations: %v", got)
	}
	if got := scaleDurations([]string{"--duration", "20s"}, 1); got[1] != "20s" {
		t.Fatalf("scaleDurations(1): %v", got)
	}
	d := counterDeltas(map[string]float64{"datax_a": 1, "datax_b": 5, "go_gc": 1}, map[string]float64{"datax_a": 4, "datax_b": 5, "datax_c": 2, "go_gc": 9})
	if len(d) != 2 || d["datax_a"] != 3 || d["datax_c"] != 2 {
		t.Fatalf("counterDeltas: %v", d)
	}
	dir := t.TempDir()
	if err := writeJSON(filepath.Join(dir, "a", "kv.json"), &benchResult{Name: "kv", OpsPerSec: 100}); err != nil {
		t.Fatal(err)
	}
	if err := writeJSON(filepath.Join(dir, "a", "noname.json"), &benchResult{OpsPerSec: 1}); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "a", "notes.txt"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	got, err := loadBenchResults(filepath.Join(dir, "a"))
	if err != nil || len(got) != 2 || got["kv"].OpsPerSec != 100 || got["noname"] == nil {
		t.Fatalf("loadBenchResults: %v %v", got, err)
	}
	if err := writeJSON(filepath.Join(dir, "b", "kv.json"), &benchResult{Name: "kv", OpsPerSec: 90, P99us: 10}); err != nil {
		t.Fatal(err)
	}
	if err := runBenchCompare([]string{filepath.Join(dir, "a"), filepath.Join(dir, "b")}); err != nil {
		t.Fatalf("compare: %v", err)
	}
	if err := runBenchCompare([]string{"--fail-on-regression", filepath.Join(dir, "a"), filepath.Join(dir, "b")}); err == nil {
		t.Fatal("a 10% throughput drop did not fail --fail-on-regression")
	}
}

// TestBenchVectors: the vector-knn workload's rows are a function of
// the seed and the row number, cluster around the centre the row number
// selects, and render as vector literals.
func TestBenchVectors(t *testing.T) {
	a, b := benchVectorAt(1, 42, 10, 8), benchVectorAt(1, 42, 10, 8)
	if len(a) != 8 || benchVectorText(a) != benchVectorText(b) {
		t.Fatalf("row 42 is not reproducible: %v, %v", a, b)
	}
	if benchVectorText(a) == benchVectorText(benchVectorAt(2, 42, 10, 8)) {
		t.Fatal("another seed drew the same vector")
	}
	dist := func(x, y []float32) float64 {
		var s float64
		for i := range x {
			s += float64(x[i]-y[i]) * float64(x[i]-y[i])
		}
		return s
	}
	// Rows j and j+10 share a centre (10 groups); row j+1 has another.
	// In 64 dimensions the two squared distances are about 46 and 174.
	for j := int64(0); j < 50; j++ {
		row := benchVectorAt(1, j, 10, 64)
		same, other := dist(row, benchVectorAt(1, j+10, 10, 64)), dist(row, benchVectorAt(1, j+1, 10, 64))
		if same >= other {
			t.Fatalf("row %d is nearer another centre's row (%.2f) than its own centre's (%.2f)", j, other, same)
		}
	}
	if got := benchVectorText([]float32{1, -0.5, 3e10}); got != "[1,-0.5,3e+10]" {
		t.Fatalf("benchVectorText: %s", got)
	}
}

// captureStdout returns what fn printed to standard output.
func captureStdout(t *testing.T, fn func()) string {
	t.Helper()
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	saved := os.Stdout
	os.Stdout = w
	done := make(chan string)
	go func() {
		var buf bytes.Buffer
		_, _ = io.Copy(&buf, r)
		done <- buf.String()
	}()
	defer func() { os.Stdout = saved }()
	fn()
	_ = w.Close()
	os.Stdout = saved
	return <-done
}

// TestBenchSetExitRule: the set runner runs every workload whatever the
// earlier ones did, then fails if any did not complete or counted more
// errors than its tolerance, naming each with its count and its most
// frequent error. Issue #267.
func TestBenchSetExitRule(t *testing.T) {
	dir := t.TempDir()
	set := filepath.Join(dir, "set.json")
	if err := os.WriteFile(set, []byte(`{"workloads": [
		{"name": "clean",     "args": ["kv", "--duration", "1s"]},
		{"name": "routing",   "args": ["kv", "--duration", "1s"]},
		{"name": "no-setup",  "args": ["scan"]},
		{"name": "tolerated", "args": ["bank"], "max_errors": 5},
		{"name": "over",      "args": ["bank"], "max_errors": 5},
		{"name": "any",       "args": ["ingest"], "max_errors": -1},
		{"name": "last",      "args": ["kv"]}
	]}`), 0o644); err != nil {
		t.Fatal(err)
	}
	records := map[string]*benchResult{
		"clean": {Ops: 100, Retries: 40},
		"routing": {Ops: 100, Errors: 80, ErrorSamples: map[string]int64{
			"routing did not converge": 77, "connection reset by peer": 3}},
		"tolerated": {Errors: 5, ErrorSamples: map[string]int64{"x": 5}},
		"over":      {Errors: 6, ErrorSamples: map[string]int64{"deadline exceeded": 6}},
		"any":       {Errors: 1000},
		"last":      {Ops: 1},
	}
	var ranNames []string
	fake := func(args []string) (*benchResult, error) {
		name := ""
		for i, a := range args {
			if a == "--name" {
				name = args[i+1]
			}
		}
		ranNames = append(ranNames, name)
		if name == "no-setup" {
			return nil, errors.New("connecting: connection refused")
		}
		return records[name], nil
	}
	runSet := func(extra ...string) (string, error) {
		ranNames = nil
		var err error
		out := captureStdout(t, func() {
			err = runBenchSetWith(append([]string{"--set", set, "--out", filepath.Join(dir, "out")}, extra...), fake)
		})
		return out, err
	}

	out, err := runSet()
	if got := strings.Join(ranNames, " "); got != "clean routing no-setup tolerated over any last" {
		t.Fatalf("workloads run: %q — a failing workload must not stop the ones after it", got)
	}
	if err == nil || !strings.Contains(err.Error(), "3 of 7 workloads failed") {
		t.Fatalf("exit: %v", err)
	}
	// The verdict closes the output: the summary line, then one line per
	// failing workload, in the set's order.
	tail := out[strings.LastIndex(out, "=== bench run:"):]
	want := []string{
		"=== bench run: 4 of 7 workloads clean",
		"FAIL routing: 80 errors (tolerance 0); most frequent, 77 times: routing did not converge",
		"FAIL no-setup: did not complete: connecting: connection refused",
		"FAIL over: 6 errors (tolerance 5); most frequent, 6 times: deadline exceeded",
	}
	lines := strings.Split(strings.TrimSpace(tail), "\n")
	if len(lines) != len(want) {
		t.Fatalf("summary:\n%s", tail)
	}
	for i, w := range want {
		if !strings.HasPrefix(lines[i], w) {
			t.Fatalf("summary line %d: %q, want prefix %q", i, lines[i], w)
		}
	}

	// The runner's tolerance covers the workloads that set none of their
	// own; a workload's own still decides for it.
	if out, err = runSet("--max-errors", "80", "--only", "routing"); err != nil {
		t.Fatalf("80 errors within --max-errors 80: %v\n%s", err, out)
	}
	if _, err = runSet("--max-errors", "79", "--only", "routing"); err == nil {
		t.Fatal("80 errors passed --max-errors 79")
	}
	if _, err = runSet("--max-errors", "100", "--only", "over"); err == nil {
		t.Fatal("--max-errors overrode a workload's own max_errors")
	}
	if out, err = runSet("--max-errors", "-1", "--only", "routing"); err != nil {
		t.Fatalf("--max-errors -1: %v\n%s", err, out)
	}
	// Serialization retries are not errors.
	if out, err = runSet("--only", "clean"); err != nil || !strings.Contains(out, "1 of 1 workloads clean") {
		t.Fatalf("a run with retries and no errors: %v\n%s", err, out)
	}

	// A set that cannot be run as written is refused before anything runs.
	for _, bad := range []string{
		`{"workloads": [{"name": "a", "args": ["kv"]}, {"name": "b", "args": []}]}`,
		`{"workloads": [{"name": "a", "args": ["kv"]}, {"name": "b", "args": ["kv", "--max-errors", "3"]}]}`,
	} {
		if err := os.WriteFile(set, []byte(bad), 0o644); err != nil {
			t.Fatal(err)
		}
		if _, err := runSet(); err == nil || len(ranNames) != 0 {
			t.Fatalf("set %s: err %v after running %v", bad, err, ranNames)
		}
	}
}

// TestBenchErrorCheck: the single-workload commands' exit rule, and the
// choice of the error a failure names.
func TestBenchErrorCheck(t *testing.T) {
	res := &benchResult{Errors: 3, ErrorSamples: map[string]int64{"b": 1, "a": 1, "c": 1}}
	if err := checkBenchErrors(res, 3); err != nil {
		t.Fatalf("3 errors within a tolerance of 3: %v", err)
	}
	if err := checkBenchErrors(res, -1); err != nil {
		t.Fatalf("a negative tolerance: %v", err)
	}
	err := checkBenchErrors(res, 0)
	if err == nil || !strings.Contains(err.Error(), "3 errors") || !strings.Contains(err.Error(), "1 times: a") {
		t.Fatalf("3 errors over a tolerance of 0: %v", err)
	}
	if err := checkBenchErrors(&benchResult{Retries: 500}, 0); err != nil {
		t.Fatalf("retries alone: %v", err)
	}
	// More errors than samples (only the first kinds are sampled).
	if err := checkBenchErrors(&benchResult{Errors: 2}, 0); err == nil || strings.Contains(err.Error(), "most frequent") {
		t.Fatalf("errors without samples: %v", err)
	}
	if msg, n := topBenchError(map[string]int64{"rare": 2, "common": 9}); msg != "common" || n != 9 {
		t.Fatalf("topBenchError: %q %d", msg, n)
	}
}
