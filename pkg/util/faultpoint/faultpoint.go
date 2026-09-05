// Package faultpoint kills the process at a named point in its own code
// path, for crash-consistency tests: a child node started with
// DATAX_FAULT_POINT=<name>:<n> sends itself SIGKILL the n-th time the
// code reaches Hit(name). Unset, Hit is a cheap atomic load. Points:
//
//	raft-append   after a Ready's HardState and entries are synced to the
//	              raft log, before any of them is applied
//	raft-apply    after a committed entry is applied to the state machine
//	flush-begin   when Pebble starts flushing a memtable
package faultpoint

import (
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
)

// EnvVar names the point and count: "raft-apply:200".
const EnvVar = "DATAX_FAULT_POINT"

var (
	armed atomic.Bool
	name  string
	count atomic.Int64
	limit int64
)

func init() {
	spec := os.Getenv(EnvVar)
	if spec == "" {
		return
	}
	n, c, _ := strings.Cut(spec, ":")
	limit = 1
	if c != "" {
		if v, err := strconv.ParseInt(c, 10, 64); err == nil && v > 0 {
			limit = v
		}
	}
	name = n
	armed.Store(true)
}

// Hit records one pass through point and kills the process when the
// armed point's count is reached. SIGKILL, not os.Exit: nothing deferred
// runs, no buffer is flushed — the crash a test wants.
func Hit(point string) {
	if !armed.Load() || point != name {
		return
	}
	if count.Add(1) == limit {
		_ = syscall.Kill(os.Getpid(), syscall.SIGKILL)
		select {} // never returns: the signal is delivered asynchronously
	}
}

// Armed reports the armed point ("" when none), for logs.
func Armed() string {
	if !armed.Load() {
		return ""
	}
	return name + ":" + strconv.FormatInt(limit, 10)
}
