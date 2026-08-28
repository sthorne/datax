// Package log is a thin wrapper over log/slog with a shared default logger.
package log

import (
	"fmt"
	"log/slog"
	"os"
	"sync/atomic"
)

var logger atomic.Pointer[slog.Logger]

func init() {
	SetVerbose(false)
}

// SetVerbose switches debug-level logging on or off.
func SetVerbose(v bool) {
	level := slog.LevelInfo
	if v {
		level = slog.LevelDebug
	}
	h := slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: level})
	logger.Store(slog.New(h))
}

func get() *slog.Logger { return logger.Load() }

func Debugf(format string, args ...any) { get().Debug(fmt.Sprintf(format, args...)) }
func Infof(format string, args ...any)  { get().Info(fmt.Sprintf(format, args...)) }
func Warnf(format string, args ...any)  { get().Warn(fmt.Sprintf(format, args...)) }
func Errorf(format string, args ...any) { get().Error(fmt.Sprintf(format, args...)) }

// Fatalf logs at error level and exits the process. Used for invariant
// violations where continuing could corrupt data (e.g. clock skew beyond
// the configured maximum).
func Fatalf(format string, args ...any) {
	get().Error("FATAL: " + fmt.Sprintf(format, args...))
	os.Exit(1)
}
