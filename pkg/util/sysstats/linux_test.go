//go:build linux

package sysstats

import (
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"
)

// fixtureProc builds a /proc tree from testdata, with the given stat and
// net/dev variants and a self/ that points at this test process's real
// /proc/self (procfs needs a real pid directory for the process figures).
func fixtureProc(t *testing.T, stat, netdev string) string {
	t.Helper()
	root := t.TempDir()
	proc := filepath.Join(root, "proc")
	if err := os.MkdirAll(filepath.Join(proc, "net"), 0o755); err != nil {
		t.Fatal(err)
	}
	copyFile(t, "testdata/proc/"+stat, filepath.Join(proc, "stat"))
	copyFile(t, "testdata/proc/loadavg", filepath.Join(proc, "loadavg"))
	copyFile(t, "testdata/proc/meminfo", filepath.Join(proc, "meminfo"))
	copyFile(t, "testdata/proc/net/"+netdev, filepath.Join(proc, "net", "dev"))
	// procfs resolves self through the pid directory it links to.
	pid := strconv.Itoa(os.Getpid())
	if err := os.Symlink(pid, filepath.Join(proc, "self")); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("/proc/"+pid, filepath.Join(proc, pid)); err != nil {
		t.Fatal(err)
	}
	return proc
}

func copyFile(t *testing.T, src, dst string) {
	t.Helper()
	b, err := os.ReadFile(src)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(dst, b, 0o644); err != nil {
		t.Fatal(err)
	}
}

// TestLinuxHostFigures: host CPU, load, memory, network and disk figures
// are read from /proc-format fixtures and rates are deltas between
// samples over the elapsed time.
func TestLinuxHostFigures(t *testing.T) {
	// The "store" lives on major 259, minor 0 (nvme0n1 in the fixture).
	fakeStatfs := func(string) (uint64, uint64, uint64, bool) {
		dev := uint64(259) << 8 // dev_t with major 259, minor 0
		return 1 << 40, 1 << 39, dev, true
	}
	r := &linuxReader{procRoot: fixtureProc(t, "stat", "dev"), diskstats: "testdata/diskstats", statfs: fakeStatfs}
	s := &Sampler{dir: "/store", start: time.Now(), host: r}
	first := s.Sample()
	if first.Load1 != 0.5 || first.Load5 != 0.25 || first.Load15 != 0.1 {
		t.Fatalf("load: %+v", first)
	}
	if first.MemTotal != 16384000*1024 || first.MemAvailable != 8192000*1024 {
		t.Fatalf("memory: total=%d available=%d", first.MemTotal, first.MemAvailable)
	}
	if first.DiskTotal != 1<<40 || first.DiskFree != 1<<39 {
		t.Fatalf("disk space: %+v", first)
	}
	if first.RSS == 0 || first.OpenFDs == 0 || first.FDLimit == 0 {
		t.Fatalf("process figures from /proc/self: rss=%d fds=%d limit=%d", first.RSS, first.OpenFDs, first.FDLimit)
	}
	if len(first.Unavailable) != 0 {
		t.Fatalf("nothing should be unavailable on the fixture, got %v", first.Unavailable)
	}
	// Rates need a previous sample: the first reports zero.
	if first.CPUPercent != 0 || first.NetRxBytesPS != 0 || first.DiskReadBytesPS != 0 {
		t.Fatalf("first sample should carry no rates: %+v", first)
	}

	// Advance the counters and pretend one second passed.
	r.procRoot = fixtureProc(t, "stat2", "dev2")
	r.diskstats = "testdata/diskstats2"
	s.mu.Lock()
	s.prev.at = s.prev.at.Add(-time.Second)
	s.mu.Unlock()
	second := s.Sample()

	// CPU: busy went from 1550 to 2350 jiffies (+800) of a total that went
	// from 9750 to 10850 (+1100): 72.7 %; iowait +100 of 1100: 9.1 %.
	if second.CPUPercent < 72 || second.CPUPercent > 73.5 {
		t.Fatalf("cpu%%: %v", second.CPUPercent)
	}
	if second.IowaitPercent < 9 || second.IowaitPercent > 9.2 {
		t.Fatalf("iowait%%: %v", second.IowaitPercent)
	}
	// Network: eth0 rx +2,000,000 and tx +1,000,000 over ~1 s; lo ignored.
	if second.NetRxBytesPS < 1.9e6 || second.NetRxBytesPS > 2.1e6 || second.NetTxBytesPS < 0.95e6 || second.NetTxBytesPS > 1.05e6 {
		t.Fatalf("net rates: rx=%v tx=%v", second.NetRxBytesPS, second.NetTxBytesPS)
	}
	// Disk: nvme0n1 read sectors +2048 (1 MiB), written +4096 (2 MiB),
	// busy +1000 ms.
	if second.DiskReadBytesPS < 1.0e6 || second.DiskReadBytesPS > 1.1e6 || second.DiskWriteBytesPS < 2.0e6 || second.DiskWriteBytesPS > 2.2e6 {
		t.Fatalf("disk rates: read=%v write=%v", second.DiskReadBytesPS, second.DiskWriteBytesPS)
	}
	if second.DiskBusyPercent < 95 || second.DiskBusyPercent > 100 {
		t.Fatalf("disk busy%%: %v", second.DiskBusyPercent)
	}
}

// TestDiskstatsMatchesDevice: the store's device is matched by
// major:minor, a partition under its own entry.
func TestDiskstatsMatchesDevice(t *testing.T) {
	rd, wr, busy, ok := readDiskstats("testdata/diskstats", 8, 1)
	if !ok || rd != 18000*512 || wr != 38000*512 || busy != 0.9 {
		t.Fatalf("sda1: ok=%v rd=%d wr=%d busy=%v", ok, rd, wr, busy)
	}
	if _, _, _, ok := readDiskstats("testdata/diskstats", 8, 7); ok {
		t.Fatal("unknown minor should not match")
	}
	if _, _, _, ok := readDiskstats("testdata/missing", 8, 0); ok {
		t.Fatal("missing file should report not found")
	}
}

func TestDevMajorMinor(t *testing.T) {
	// 259:0 as the kernel encodes it: minor in the low byte and bits 20+,
	// major in bits 8-19 and 32+.
	dev := uint64(259) << 8
	if unixMajor(dev) != 259 || unixMinor(dev) != 0 {
		t.Fatalf("259:0 decoded as %d:%d", unixMajor(dev), unixMinor(dev))
	}
	dev = uint64(8)<<8 | 17
	if unixMajor(dev) != 8 || unixMinor(dev) != 17 {
		t.Fatalf("8:17 decoded as %d:%d", unixMajor(dev), unixMinor(dev))
	}
}

// TestUnavailableWithoutProc: a missing /proc tree degrades to
// "unavailable" host figures rather than an error or a zero measurement.
func TestUnavailableWithoutProc(t *testing.T) {
	r := &linuxReader{procRoot: t.TempDir() + "/nope", diskstats: "testdata/missing",
		statfs: func(string) (uint64, uint64, uint64, bool) { return 0, 0, 0, false }}
	s := &Sampler{dir: "/store", start: time.Now(), host: r}
	out := s.Sample()
	for _, want := range []string{"proc", "disk"} {
		if !contains(out.Unavailable, want) {
			t.Fatalf("expected %q unavailable, got %v", want, out.Unavailable)
		}
	}
	if out.Goroutines == 0 {
		t.Fatal("runtime figures should still be present")
	}
}
