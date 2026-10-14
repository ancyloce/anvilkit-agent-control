package bootstrap

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/fx"

	"github.com/ancyloce/anvilkit-agent-control/internal/adapters/outbox"
	"github.com/ancyloce/anvilkit-agent-control/internal/adapters/postgres"
	"github.com/ancyloce/anvilkit-agent-control/internal/config"
	"github.com/ancyloce/anvilkit-agent-control/internal/transport/identity"
)

// eventMetrics are the signals of the event publication.
type eventMetrics struct {
	forwarded, failures, wakes, rejected, pruned prometheus.Counter
	lag                                          prometheus.Gauge
}

func newEventMetrics(reg *prometheus.Registry) eventMetrics {
	m := eventMetrics{
		forwarded: prometheus.NewCounter(prometheus.CounterOpts{Name: "anvilkit_control_outbox_forwarded_total", Help: "Outbox messages published to JetStream."}),
		failures:  prometheus.NewCounter(prometheus.CounterOpts{Name: "anvilkit_control_outbox_forward_failures_total", Help: "JetStream publications that failed (the message is redelivered)."}),
		wakes:     prometheus.NewCounter(prometheus.CounterOpts{Name: "anvilkit_control_event_wakes_total", Help: "operation.changed messages consumed to wake event streams."}),
		rejected:  prometheus.NewCounter(prometheus.CounterOpts{Name: "anvilkit_control_event_wakes_rejected_total", Help: "Messages the operation event consumer dropped as malformed."}),
		pruned:    prometheus.NewCounter(prometheus.CounterOpts{Name: "anvilkit_control_outbox_pruned_total", Help: "Acknowledged outbox rows deleted past the retention."}),
		lag:       prometheus.NewGauge(prometheus.GaugeOpts{Name: "anvilkit_control_outbox_oldest_unforwarded_seconds", Help: "Age of the oldest outbox message the forwarder has not acknowledged."}),
	}
	reg.MustRegister(m.forwarded, m.failures, m.wakes, m.rejected, m.pruned, m.lag)
	return m
}

// startEvents runs the replica's event publication (P2.8): the forwarder
// module publishes the outbox to JetStream, the waker consumes
// operation.changed and wakes the event streams through the notifier
// (beside its LISTEN wake), and the retention loop deletes acknowledged
// rows past the retention and reports the forwarding lag. Without
// events.enabled (development only) the outbox keeps every event and
// nothing is published.
func startEvents(lc fx.Lifecycle, cfg config.Config, pool *pgxpool.Pool, notifier *postgres.Notifier, reg *prometheus.Registry, log *slog.Logger) error {
	e := cfg.Events
	if !e.Enabled {
		log.Warn("DEVELOPMENT_ONLY: event publication disabled; operation events accumulate in the outbox and reach no consumer")
		return nil
	}
	var natsTLS *tls.Config
	switch e.NATS.TLS.Mode {
	case "development":
		if !cfg.Development.Enabled {
			return errors.New("events.nats.tls: development mode without development.enabled")
		}
		log.Warn("DEVELOPMENT_ONLY plaintext transport", "connection", "nats")
	case "tls":
		roots, err := caPool(e.NATS.TLS.CAFile)
		if err != nil {
			return fmt.Errorf("events.nats.tls: %w", err)
		}
		natsTLS = &tls.Config{RootCAs: roots, ServerName: e.NATS.TLS.ServerName, MinVersion: tls.VersionTLS13}
	case "mtls":
		r, err := identity.New(identity.Files{CertFile: e.NATS.TLS.CertFile, KeyFile: e.NATS.TLS.KeyFile, CAFile: e.NATS.TLS.CAFile}, 0, log)
		if err != nil {
			return fmt.Errorf("events.nats.tls: %w", err)
		}
		r.Start()
		natsTLS = identity.ClientTLSConfig(r, e.NATS.TLS.ServerName)
	default:
		return fmt.Errorf("events.nats.tls: unknown mode %q", e.NATS.TLS.Mode)
	}
	m := newEventMetrics(reg)
	nc, err := outbox.Connect(outbox.NATSConfig{
		URL: e.NATS.URL, Name: e.NATS.Name, PublishTimeout: e.NATS.PublishTimeout, TLS: natsTLS,
		CredsFile: e.NATS.CredsFile, CredsNKey: e.NATS.CredsKind == config.NATSCredsNKey,
	})
	if err != nil {
		return fmt.Errorf("events: %w", err)
	}
	fwd, err := outbox.NewForwarder(pool, nc, outbox.ForwarderConfig{
		ConsumerGroup: e.ConsumerGroup, PollInterval: e.PollInterval, AckDeadline: e.AckDeadline, ResendInterval: e.ResendInterval,
		BatchSize: e.BatchSize, CloseTimeout: e.CloseTimeout,
	}, m.forwarded, m.failures)
	if err != nil {
		nc.Close()
		return fmt.Errorf("events: %w", err)
	}
	var waker *outbox.Waker
	if e.Wake {
		if waker, err = outbox.NewWaker(nc, notifier.Wake, log, m.wakes, m.rejected); err != nil {
			_ = fwd.Close()
			nc.Close()
			return fmt.Errorf("events: %w", err)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	lc.Append(fx.Hook{
		OnStart: func(context.Context) error {
			wg.Add(1)
			go func() {
				defer wg.Done()
				if err := fwd.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
					log.Error("outbox forwarder stopped", "error", err)
				}
			}()
			if waker != nil {
				wg.Add(1)
				go func() { defer wg.Done(); waker.Run(ctx) }()
			}
			wg.Add(1)
			go func() { defer wg.Done(); retainOutbox(ctx, pool, e, m, log) }()
			log.Info("event publication running", "stream", outbox.Stream, "consumerGroup", e.ConsumerGroup, "wake", e.Wake)
			return nil
		},
		OnStop: func(stop context.Context) error {
			cancel()
			err := fwd.Close()
			done := make(chan struct{})
			go func() { wg.Wait(); close(done) }()
			select {
			case <-done:
			case <-stop.Done():
			}
			nc.Close()
			return err
		},
	})
	return nil
}

// retainOutbox reports the forwarding lag every poll-bounded tick and
// deletes acknowledged rows past the retention every retention interval,
// a bounded batch at a time.
func retainOutbox(ctx context.Context, pool *pgxpool.Pool, e config.Events, m eventMetrics, log *slog.Logger) {
	lagTick := time.NewTicker(15 * time.Second)
	defer lagTick.Stop()
	pruneTick := time.NewTicker(e.RetentionInterval)
	defer pruneTick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-lagTick.C:
			if age, err := outbox.Oldest(ctx, pool, e.ConsumerGroup); err == nil {
				m.lag.Set(age.Seconds())
			}
		case <-pruneTick.C:
			n, err := outbox.Prune(ctx, pool, e.ConsumerGroup, time.Now().Add(-e.Retention), e.RetentionBatch)
			if err != nil && ctx.Err() == nil {
				log.Warn("outbox retention failed; retried next interval", "error", err)
				continue
			}
			m.pruned.Add(float64(n))
		}
	}
}
