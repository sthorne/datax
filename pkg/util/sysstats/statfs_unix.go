//go:build unix && !linux

package sysstats

import "syscall"

func statfsSpace(dir string) (total, free uint64, ok bool) {
	var st syscall.Statfs_t
	if err := syscall.Statfs(dir, &st); err != nil {
		return 0, 0, false
	}
	return uint64(st.Blocks) * uint64(st.Bsize), uint64(st.Bavail) * uint64(st.Bsize), true
}
