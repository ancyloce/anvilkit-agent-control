package bootstrap

import (
	"context"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/fx"

	"github.com/ancyloce/anvilkit-agent-control/internal/adapters/retention"
	"github.com/ancyloce/anvilkit-agent-control/internal/config"
)

// retentionMaxConns is the retention pool's exact per-replica bound, counted
// by the deployment lock's connection budget (deploy/gitops/lock.yaml).
const retentionMaxConns = 1

// retentionPasses bounds the passes of one interval, so a large first
// backlog is worked off over several intervals, never in one burst.
const retentionPasses = 20

// retentionMetrics are the signals of the operation-event retention.
type retentionMetrics struct {
	deleted, operations prometheus.Counter
	failures            *prometheus.CounterVec
	lastSuccess         prometheus.Gauge
}

func newRetentionMetrics(reg *prometheus.Registry) retentionMetrics {
	m := retentionMetrics{
		deleted:    prometheus.NewCounter(prometheus.CounterOpts{Name: "anvilkit_control_event_retention_deleted_total", Help: "Operation events deleted past the retention period (RB-A27)."}),
		operations: prometheus.NewCounter(prometheus.CounterOpts{Name: "anvilkit_control_event_retention_operations_total", Help: "Operations whose event history the retention pruned."}),
		failures: prometheus.NewCounterVec(prometheus.CounterOpts{Name: "anvilkit_control_event_retention_failures_total",
			Help: "Retention passes refused (identity: the connection is not the retention identity) or failed (delete)."}, []string{"reason"}),
		lastSuccess: prometheus.NewGauge(prometheus.GaugeOpts{Name: "anvilkit_control_event_retention_last_success_timestamp_seconds", Help: "Unix time of the last completed retention run."}),
	}
	m.failures.WithLabelValues("identity")
	m.failures.WithLabelValues("delete")
	reg.MustRegister(m.deleted, m.operations, m.failures, m.lastSuccess)
	return m
}

// startRetention runs the retention of operation events (RB-A27) on every
// replica: each interval it verifies that its own pool connects as the
// retention identity (never the app role or the table owner), then prunes
// up to retention.batch operations per pass until a pass finds fewer.
// Replicas may overlap; the second delete of a row finds it gone. Without
// the identity (development only) every event is kept.
func startRetention(lc fx.Lifecycle, cfg config.Config, app *pgxpool.Pool, reg *prometheus.Registry, log *slog.Logger) error {
	r := cfg.Retention
	if r.Database.URL == "" {
		log.Warn("DEVELOPMENT_ONLY: operation-event retention disabled (no retention identity); every operation event is kept")
		return nil
	}
	poolCfg, err := boundedPoolConfig(r.Database.URL, retentionMaxConns, cfg.Database)
	if err != nil {
		return err
	}
	pool, err := pgxpool.NewWithConfig(context.Background(), poolCfg)
	if err != nil {
		return err
	}
	m := newRetentionMetrics(reg)
	appUser := app.Config().ConnConfig.User
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	lc.Append(fx.Hook{
		OnStart: func(context.Context) error {
			go func() {
				defer close(done)
				tick := time.NewTicker(r.Interval)
				defer tick.Stop()
				for {
					select {
					case <-ctx.Done():
						return
					case <-tick.C:
						retainEvents(ctx, pool, appUser, r, m, log)
					}
				}
			}()
			return nil
		},
		OnStop: func(stop context.Context) error {
			cancel()
			select {
			case <-done:
			case <-stop.Done():
			}
			pool.Close()
			return nil
		},
	})
	return nil
}

// retainEvents is one interval's run: the identity check, then passes until
// one prunes fewer than the batch (or retentionPasses ran).
func retainEvents(ctx context.Context, pool *pgxpool.Pool, appUser string, r config.Retention, m retentionMetrics, log *slog.Logger) {
	if err := retention.VerifyIdentity(ctx, pool, appUser); err != nil {
		if ctx.Err() == nil {
			m.failures.WithLabelValues("identity").Inc()
			log.Error("operation-event retention refused: the connection is not the retention identity", "error", err)
		}
		return
	}
	before := time.Now().Add(-r.OperationEvents)
	for range retentionPasses {
		events, ops, err := retention.PruneOperationEvents(ctx, pool, before, r.Batch)
		if err != nil {
			if ctx.Err() == nil {
				m.failures.WithLabelValues("delete").Inc()
				log.Warn("operation-event retention failed; retried next interval", "error", err)
			}
			return
		}
		m.deleted.Add(float64(events))
		m.operations.Add(float64(ops))
		if ops < r.Batch {
			break
		}
	}
	m.lastSuccess.SetToCurrentTime()
}
