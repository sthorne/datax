package sysstats

import (
	"runtime"
	"testing"
	"time"
)

// TestSampleRuntimeFields: the runtime figures are present on every
// platform, uptime advances, and the sampler never panics without a
// store directory.
func TestSampleRuntimeFields(t *testing.T) {
	s := New("")
	a := s.Sample()
	if a.Goroutines <= 0 || a.Cores != runtime.NumCPU() {
		t.Fatalf("runtime fields: %+v", a)
	}
	time.Sleep(20 * time.Millisecond)
	b := s.Sample()
	if !b.At.After(a.At) {
		t.Fatal("sample time did not advance")
	}
	if got := s.Latest(); got.At != b.At {
		t.Fatal("Latest should be the last sample")
	}
	// No store directory: disk figures are reported as unavailable, not
	// as zeros pretending to be measurements.
	if !contains(b.Unavailable, "disk") {
		t.Fatalf("expected disk to be unavailable without a dir, got %v", b.Unavailable)
	}
}

func TestPauseP99(t *testing.T) {
	var ms runtime.MemStats
	ms.NumGC = 10
	for i := 0; i < 10; i++ {
		ms.PauseNs[i] = uint64(i + 1)
	}
	if got := pauseP99(ms); got != 10 {
		t.Fatalf("p99 of 1..10 = %d, want 10", got)
	}
	ms.NumGC = 0
	if got := pauseP99(ms); got != 0 {
		t.Fatalf("p99 with no GCs = %d, want 0", got)
	}
}

func contains(list []string, s string) bool {
	for _, x := range list {
		if x == s {
			return true
		}
	}
	return false
}
