package rpc

import (
	"context"
	"testing"

	"google.golang.org/grpc"

	"github.com/sthorne/datax/pkg/util/stop"
)

// BenchmarkTaskInterceptor (issue #331): what the stopper's unary
// interceptor adds to every unary call, around a no-op handler. "none"
// calls the handler directly, for scale.
func BenchmarkTaskInterceptor(b *testing.B) {
	info := &grpc.UnaryServerInfo{FullMethod: "/datax.rpcpb.Internode/Batch"}
	handler := func(ctx context.Context, req any) (any, error) { return req, nil }
	req := new(int)
	// gRPC hands a handler a cancelable context per call.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b.Run("none", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			if _, err := handler(ctx, req); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("serial", func(b *testing.B) {
		st := stop.NewStopper()
		defer st.Stop()
		icpt := taskInterceptor(st)
		b.ReportAllocs()
		for b.Loop() {
			if _, err := icpt(ctx, req, info, handler); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("parallel", func(b *testing.B) {
		st := stop.NewStopper()
		defer st.Stop()
		icpt := taskInterceptor(st)
		b.ReportAllocs()
		b.RunParallel(func(pb *testing.PB) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			for pb.Next() {
				if _, err := icpt(ctx, req, info, handler); err != nil {
					b.Error(err)
					return
				}
			}
		})
	})
}
