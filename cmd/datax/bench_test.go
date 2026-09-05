package main

import (
	"os"
	"path/filepath"
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
