package grpc

import (
	"context"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/status"
)

// Observability returns the server options that give every RPC a span with
// the gRPC semantic attributes only (service, method, status code; no
// message bodies, metadata or identifiers: security.md) and the RPC metrics
// by method and status code. The caller's trace context propagates from the
// internal callers (API, Workflow, Proxy, MCP, Knowledge).
func Observability(reg prometheus.Registerer) []grpc.ServerOption {
	calls := prometheus.NewCounterVec(prometheus.CounterOpts{Name: "anvilkit_control_rpcs_total", Help: "Control RPCs by full method and status code."},
		[]string{"method", "code"})
	duration := prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: "anvilkit_control_rpc_duration_seconds", Help: "Control RPC duration by full method.",
		Buckets: prometheus.ExponentialBuckets(0.001, 2, 16)}, []string{"method"})
	reg.MustRegister(calls, duration)
	record := func(method string, start time.Time, err error) {
		calls.WithLabelValues(method, status.Code(err).String()).Inc()
		duration.WithLabelValues(method).Observe(time.Since(start).Seconds())
	}
	return []grpc.ServerOption{
		grpc.StatsHandler(otelgrpc.NewServerHandler()),
		grpc.ChainUnaryInterceptor(func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
			start := time.Now()
			resp, err := handler(ctx, req)
			record(info.FullMethod, start, err)
			return resp, err
		}),
		grpc.ChainStreamInterceptor(func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
			start := time.Now()
			err := handler(srv, ss)
			record(info.FullMethod, start, err)
			return err
		}),
	}
}
