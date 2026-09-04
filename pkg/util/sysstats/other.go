//go:build !linux

package sysstats

// otherReader provides what the Go runtime and Statfs can on platforms
// without /proc: disk space, and nothing else at host level.
type otherReader struct{}

func newHostReader() hostReader { return otherReader{} }

func (otherReader) fill(out *Sample, raw *rawCounters, dir string) {
	out.Unavailable = append(out.Unavailable, "cpu", "load", "memory", "network", "process", "disk-io")
	if dir == "" {
		out.Unavailable = append(out.Unavailable, "disk")
		return
	}
	total, free, ok := statfsSpace(dir)
	if !ok {
		out.Unavailable = append(out.Unavailable, "disk")
		return
	}
	out.DiskTotal, out.DiskFree = total, free
}
