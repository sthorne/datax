// Package sysstats samples the machine a node runs on — CPU, memory, the
// store's disk, network, file descriptors and the Go runtime — so the
// dashboard, /status and /metrics can show an operator what the host is
// doing, not only what the database is doing. Host-level figures come from
// /proc on Linux (via prometheus/procfs, already in the dependency tree)
// and are reported as unavailable elsewhere; process and runtime figures
// come from the Go runtime everywhere. Rates are deltas between
// consecutive samples, so a sampler must be driven at a steady cadence
// (Run) for them to mean anything.
package sysstats

import (
	"context"
	"runtime"
	"sync"
	"time"
)

// Sample is one observation of the machine.
type Sample struct {
	At time.Time `json:"at"`

	// Host CPU, in percent of all cores, over the interval since the
	// previous sample; Iowait is the share of that interval the CPUs sat
	// waiting on disk. Cores is the logical CPU count.
	CPUPercent    float64 `json:"cpu_percent"`
	IowaitPercent float64 `json:"iowait_percent"`
	Cores         int     `json:"cores"`
	// Load averages, as /proc/loadavg reports them (JSON snake_case
	// like every other field — and like kvpb.MachineSummary's load1,
	// which is this figure as the heartbeat carries it; issue #146).
	Load1  float64 `json:"load1"`
	Load5  float64 `json:"load5"`
	Load15 float64 `json:"load15"`

	// Host memory in bytes (MemTotal and MemAvailable as the kernel
	// defines them); RSS is this process's resident set.
	MemTotal     uint64 `json:"mem_total"`
	MemAvailable uint64 `json:"mem_available"`
	RSS          uint64 `json:"rss"`

	// The process's own CPU, in percent of one core over the interval.
	ProcessCPUPercent float64 `json:"process_cpu_percent"`

	// The store directory's filesystem, in bytes, and the throughput of
	// the block device backing it over the interval.
	DiskTotal        uint64  `json:"disk_total"`
	DiskFree         uint64  `json:"disk_free"`
	DiskReadBytesPS  float64 `json:"disk_read_bytes_ps"`
	DiskWriteBytesPS float64 `json:"disk_write_bytes_ps"`
	// DiskBusyPercent is the share of the interval the device had I/O in
	// flight (utilization).
	DiskBusyPercent float64 `json:"disk_busy_percent"`

	// Network throughput over every non-loopback interface, over the
	// interval.
	NetRxBytesPS float64 `json:"net_rx_bytes_ps"`
	NetTxBytesPS float64 `json:"net_tx_bytes_ps"`

	// File descriptors held by the process and the soft limit it runs
	// under (Pebble holds one per open sstable).
	OpenFDs int `json:"open_fds"`
	FDLimit int `json:"fd_limit"`

	// Go runtime.
	Goroutines  int      `json:"goroutines"`
	HeapInUse   uint64   `json:"heap_in_use"`
	NumGC       uint32   `json:"num_gc"`
	GCPauseP99  int64    `json:"gc_pause_p99_ns"` // over the runtime's recent pause ring
	ProcessUp   int64    `json:"process_uptime_seconds"`
	Unavailable []string `json:"unavailable,omitempty"` // host figures this platform cannot provide
}

// Sampler takes samples for one process and one store directory.
type Sampler struct {
	dir   string
	start time.Time
	host  hostReader

	mu       sync.Mutex
	latest   Sample
	prev     rawCounters
	hasRaw   bool
	onSample []func(Sample)
}

// OnSample registers fn to run with every sample taken (after it is
// stored), for exporters that need a push rather than a pull.
func (s *Sampler) OnSample(fn func(Sample)) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.onSample = append(s.onSample, fn)
}

// rawCounters are the cumulative kernel counters a rate is derived from.
type rawCounters struct {
	at        time.Time
	cpuBusy   float64 // seconds of non-idle CPU summed over cores
	cpuTotal  float64 // seconds of all CPU states summed over cores
	cpuIowait float64
	procCPU   float64 // seconds of process CPU time
	diskRead  uint64  // bytes
	diskWrite uint64
	diskBusy  float64 // seconds the device was busy
	netRx     uint64
	netTx     uint64
	hostCPUOK bool
	procCPUOK bool
	diskOK    bool
	netOK     bool
}

// New returns a sampler for the process and the store at dir ("" for an
// in-memory store: disk figures are then reported as unavailable).
func New(dir string) *Sampler {
	return &Sampler{dir: dir, start: time.Now(), host: newHostReader()}
}

// Run samples every interval until ctx ends. The first sample is taken
// immediately, so Latest is meaningful right away (with zero rates).
func (s *Sampler) Run(ctx context.Context, interval time.Duration) {
	s.Sample()
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			s.Sample()
		}
	}
}

// Latest returns the most recent sample (the zero Sample before the first).
func (s *Sampler) Latest() Sample {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.latest
}

// Sample takes a sample now and returns it.
func (s *Sampler) Sample() Sample {
	now := time.Now()
	out := Sample{At: now, Cores: runtime.NumCPU(), ProcessUp: int64(now.Sub(s.start).Seconds())}

	// Go runtime: available everywhere.
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	out.Goroutines = runtime.NumGoroutine()
	out.HeapInUse = ms.HeapInuse
	out.NumGC = ms.NumGC
	out.GCPauseP99 = pauseP99(ms)

	raw := rawCounters{at: now}
	s.host.fill(&out, &raw, s.dir)

	s.mu.Lock()
	if s.hasRaw {
		dt := now.Sub(s.prev.at).Seconds()
		if dt > 0 {
			if raw.hostCPUOK && s.prev.hostCPUOK && raw.cpuTotal > s.prev.cpuTotal {
				total := raw.cpuTotal - s.prev.cpuTotal
				out.CPUPercent = clampPct(100 * (raw.cpuBusy - s.prev.cpuBusy) / total)
				out.IowaitPercent = clampPct(100 * (raw.cpuIowait - s.prev.cpuIowait) / total)
			}
			if raw.procCPUOK && s.prev.procCPUOK {
				out.ProcessCPUPercent = clampPct(100 * (raw.procCPU - s.prev.procCPU) / dt)
			}
			if raw.diskOK && s.prev.diskOK {
				out.DiskReadBytesPS = float64(raw.diskRead-s.prev.diskRead) / dt
				out.DiskWriteBytesPS = float64(raw.diskWrite-s.prev.diskWrite) / dt
				out.DiskBusyPercent = clampPct(100 * (raw.diskBusy - s.prev.diskBusy) / dt)
			}
			if raw.netOK && s.prev.netOK {
				out.NetRxBytesPS = float64(raw.netRx-s.prev.netRx) / dt
				out.NetTxBytesPS = float64(raw.netTx-s.prev.netTx) / dt
			}
		}
	}
	s.prev, s.hasRaw = raw, true
	s.latest = out
	fns := s.onSample
	s.mu.Unlock()
	for _, fn := range fns {
		fn(out)
	}
	return out
}

func clampPct(v float64) float64 {
	if v < 0 {
		return 0
	}
	if v > 100 {
		return 100
	}
	return v
}

// pauseP99 is the 99th percentile of the runtime's ring of the last 256
// GC pause durations, in nanoseconds.
func pauseP99(ms runtime.MemStats) int64 {
	n := int(ms.NumGC)
	if n > len(ms.PauseNs) {
		n = len(ms.PauseNs)
	}
	if n == 0 {
		return 0
	}
	pauses := make([]uint64, 0, n)
	for i := 0; i < n; i++ {
		pauses = append(pauses, ms.PauseNs[i])
	}
	// Small n: an insertion sort is plenty.
	for i := 1; i < len(pauses); i++ {
		for j := i; j > 0 && pauses[j-1] > pauses[j]; j-- {
			pauses[j-1], pauses[j] = pauses[j], pauses[j-1]
		}
	}
	idx := (len(pauses)*99 + 99) / 100
	if idx >= len(pauses) {
		idx = len(pauses) - 1
	}
	return int64(pauses[idx])
}

// hostReader fills the host-level fields and the cumulative counters
// (platform-specific; see linux.go and other.go).
type hostReader interface {
	fill(out *Sample, raw *rawCounters, dir string)
}
