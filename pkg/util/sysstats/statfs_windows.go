//go:build windows

package sysstats

func statfsSpace(dir string) (total, free uint64, ok bool) { return 0, 0, false }
