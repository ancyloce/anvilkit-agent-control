package bootstrap

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.37.0"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"
	"go.uber.org/fx"

	"github.com/ancyloce/anvilkit-agent-control/internal/config"
)

const serviceName = "anvilkit-agent-control"

// newTracer installs the process's tracer provider: spans go over OTLP/gRPC
// to the placed collector under telemetry.otlp_tls (plaintext only with the
// development guard) or nowhere. The provider is global: the gRPC server stats
// handler continues the callers' traces; the stop hook flushes within the
// stop deadline.
func newTracer(lc fx.Lifecycle, cfg config.Config) (trace.Tracer, error) {
	otel.SetTextMapPropagator(propagation.TraceContext{})
	if cfg.Telemetry.OTLPEndpoint == "" {
		return noop.NewTracerProvider().Tracer(serviceName), nil
	}
	transport, err := clientTransport("telemetry.otlp_tls", cfg.Telemetry.OTLPTLS, cfg.Development.Enabled, slog.Default())
	if err != nil {
		return nil, err
	}
	exporter, err := otlptracegrpc.New(context.Background(), otlptracegrpc.WithEndpoint(cfg.Telemetry.OTLPEndpoint), otlptracegrpc.WithDialOption(transport))
	if err != nil {
		return nil, err
	}
	provider := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter),
		sdktrace.WithResource(resource.NewSchemaless(semconv.ServiceName(serviceName))),
		sdktrace.WithSampler(sdktrace.ParentBased(sdktrace.TraceIDRatioBased(cfg.Telemetry.SampleRatio))),
	)
	otel.SetTracerProvider(provider)
	lc.Append(fx.Hook{OnStop: func(ctx context.Context) error {
		flush, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		return provider.Shutdown(flush)
	}})
	return provider.Tracer(serviceName), nil
}

// newMetrics is the process registry and, when telemetry.metrics_listen is
// placed, serves /metrics there (never on the gRPC listener).
func newMetrics(lc fx.Lifecycle, cfg config.Config, log *slog.Logger) *prometheus.Registry {
	reg := prometheus.NewRegistry()
	reg.MustRegister(collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	if cfg.Telemetry.MetricsListen == "" {
		return reg
	}
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))
	srv := &http.Server{Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	lc.Append(fx.Hook{
		OnStart: func(context.Context) error {
			l, err := net.Listen("tcp", cfg.Telemetry.MetricsListen)
			if err != nil {
				return err
			}
			go func() {
				if err := srv.Serve(l); err != nil && !errors.Is(err, http.ErrServerClosed) {
					log.Error("metrics listener stopped", "error", err)
				}
			}()
			return nil
		},
		OnStop: func(ctx context.Context) error { return srv.Shutdown(ctx) },
	})
	return reg
}
