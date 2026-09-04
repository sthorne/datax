//go:build linux

package sysstats

import (
	"bufio"
	"os"
	"strconv"
	"strings"
	"syscall"

	"github.com/prometheus/procfs"
)

// linuxReader reads the host through procfs. procRoot lets tests point it
// at a fixture tree.
type linuxReader struct {
	procRoot  string
	diskstats string // path of the diskstats file (tests override)
	statfs    func(dir string) (total, free uint64, dev uint64, ok bool)
}

func newHostReader() hostReader {
	return &linuxReader{procRoot: "/proc", diskstats: "/proc/diskstats", statfs: statfsDir}
}

func (r *linuxReader) fill(out *Sample, raw *rawCounters, dir string) {
	fs, err := procfs.NewFS(r.procRoot)
	if err != nil {
		out.Unavailable = append(out.Unavailable, "proc")
		fillDiskSpace(out, raw, dir, r.statfs, r.diskstats)
		return
	}
	if st, err := fs.Stat(); err == nil {
		c := st.CPUTotal
		raw.cpuTotal = c.User + c.Nice + c.System + c.Idle + c.Iowait + c.IRQ + c.SoftIRQ + c.Steal
		raw.cpuBusy = raw.cpuTotal - c.Idle - c.Iowait
		raw.cpuIowait = c.Iowait
		raw.hostCPUOK = true
	} else {
		out.Unavailable = append(out.Unavailable, "cpu")
	}
	if la, err := fs.LoadAvg(); err == nil {
		out.Load1, out.Load5, out.Load15 = la.Load1, la.Load5, la.Load15
	} else {
		out.Unavailable = append(out.Unavailable, "load")
	}
	if mi, err := fs.Meminfo(); err == nil {
		if mi.MemTotal != nil {
			out.MemTotal = *mi.MemTotal * 1024
		}
		if mi.MemAvailable != nil {
			out.MemAvailable = *mi.MemAvailable * 1024
		}
	} else {
		out.Unavailable = append(out.Unavailable, "memory")
	}
	if nd, err := fs.NetDev(); err == nil {
		for name, line := range nd {
			if name == "lo" {
				continue
			}
			raw.netRx += line.RxBytes
			raw.netTx += line.TxBytes
		}
		raw.netOK = true
	} else {
		out.Unavailable = append(out.Unavailable, "network")
	}
	if self, err := fs.Self(); err == nil {
		if st, err := self.Stat(); err == nil {
			out.RSS = uint64(st.ResidentMemory())
			raw.procCPU = st.CPUTime()
			raw.procCPUOK = true
		}
		if n, err := self.FileDescriptorsLen(); err == nil {
			out.OpenFDs = n
		}
		if lim, err := self.Limits(); err == nil {
			out.FDLimit = int(lim.OpenFiles)
		}
	} else {
		out.Unavailable = append(out.Unavailable, "process")
	}
	fillDiskSpace(out, raw, dir, r.statfs, r.diskstats)
}

// fillDiskSpace reports the store filesystem's size and the throughput
// counters of the device backing it (matched by major:minor in
// /proc/diskstats; a partition is reported under its own name there).
func fillDiskSpace(out *Sample, raw *rawCounters, dir string, statfs func(string) (uint64, uint64, uint64, bool), diskstats string) {
	if dir == "" {
		out.Unavailable = append(out.Unavailable, "disk")
		return
	}
	total, free, dev, ok := statfs(dir)
	if !ok {
		out.Unavailable = append(out.Unavailable, "disk")
		return
	}
	out.DiskTotal, out.DiskFree = total, free
	major, minor := unixMajor(dev), unixMinor(dev)
	rd, wr, busy, found := readDiskstats(diskstats, major, minor)
	if !found {
		out.Unavailable = append(out.Unavailable, "disk-io")
		return
	}
	raw.diskRead, raw.diskWrite, raw.diskBusy, raw.diskOK = rd, wr, busy, true
}

// statfsDir reports the filesystem holding dir and the device it is on.
func statfsDir(dir string) (total, free uint64, dev uint64, ok bool) {
	var st syscall.Statfs_t
	if err := syscall.Statfs(dir, &st); err != nil {
		return 0, 0, 0, false
	}
	var fst syscall.Stat_t
	if err := syscall.Stat(dir, &fst); err != nil {
		return 0, 0, 0, false
	}
	return st.Blocks * uint64(st.Bsize), st.Bavail * uint64(st.Bsize), uint64(fst.Dev), true
}

// Linux dev_t layout (glibc's gnu_dev_major/minor).
func unixMajor(dev uint64) uint32 {
	return uint32((dev>>8)&0xfff) | uint32((dev>>32)&^0xfff)
}

func unixMinor(dev uint64) uint32 {
	return uint32(dev&0xff) | uint32((dev>>12)&^0xff)
}

// readDiskstats returns the cumulative bytes read and written and the
// seconds spent with I/O in flight for the device with major:minor, from
// a /proc/diskstats-format file. Sectors are 512 bytes in that file
// regardless of the device's block size.
func readDiskstats(path string, major, minor uint32) (readBytes, writeBytes uint64, busySeconds float64, found bool) {
	f, err := os.Open(path)
	if err != nil {
		return 0, 0, 0, false
	}
	defer func() { _ = f.Close() }()
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		fields := strings.Fields(sc.Text())
		if len(fields) < 14 {
			continue
		}
		maj, err1 := strconv.ParseUint(fields[0], 10, 32)
		min, err2 := strconv.ParseUint(fields[1], 10, 32)
		if err1 != nil || err2 != nil || uint32(maj) != major || uint32(min) != minor {
			continue
		}
		rs, _ := strconv.ParseUint(fields[5], 10, 64)  // sectors read
		ws, _ := strconv.ParseUint(fields[9], 10, 64)  // sectors written
		ms, _ := strconv.ParseUint(fields[12], 10, 64) // ms spent doing I/O
		return rs * 512, ws * 512, float64(ms) / 1000, true
	}
	return 0, 0, 0, false
}
