package stop

import (
	"context"
	"testing"
)

// The cost of running work as a task of the Stopper (issue #331): every
// unary RPC, raft stream message, HTTP request and SQL connection pays
// it. "direct" is the same call without a task, for scale.

func noopTask(context.Context) {}

func BenchmarkRunTask(b *testing.B) {
	ctx := context.Background()
	b.Run("direct", func(b *testing.B) {
		b.ReportAllocs()
		f := noopTask
		for b.Loop() {
			f(ctx)
		}
	})
	b.Run("serial", func(b *testing.B) {
		s := NewStopper()
		defer s.Stop()
		b.ReportAllocs()
		for b.Loop() {
			if err := s.RunTask(ctx, "bench", noopTask); err != nil {
				b.Fatal(err)
			}
		}
	})
	// A caller's context that can be canceled, as gRPC's and net/http's
	// are: the task's context must follow it as well as the Stopper.
	b.Run("serial-cancelable-ctx", func(b *testing.B) {
		s := NewStopper()
		defer s.Stop()
		cctx, cancel := context.WithCancel(ctx)
		defer cancel()
		b.ReportAllocs()
		for b.Loop() {
			if err := s.RunTask(cctx, "bench", noopTask); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("parallel", func(b *testing.B) {
		s := NewStopper()
		defer s.Stop()
		b.ReportAllocs()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				if err := s.RunTask(ctx, "bench", noopTask); err != nil {
					b.Error(err)
					return
				}
			}
		})
	})
	b.Run("parallel-cancelable-ctx", func(b *testing.B) {
		s := NewStopper()
		defer s.Stop()
		b.ReportAllocs()
		b.RunParallel(func(pb *testing.PB) {
			cctx, cancel := context.WithCancel(ctx)
			defer cancel()
			for pb.Next() {
				if err := s.RunTask(cctx, "bench", noopTask); err != nil {
					b.Error(err)
					return
				}
			}
		})
	})
}
