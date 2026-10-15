package bootstrap

import (
	"context"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/fx"

	"github.com/ancyloce/anvilkit-agent-control/internal/adapters/postgres/sqlc"
	"github.com/ancyloce/anvilkit-agent-control/internal/application"
	grpctransport "github.com/ancyloce/anvilkit-agent-control/internal/transport/grpc"
)

// obligationInterval is how often the unresolved-obligation gauges read the
// database (two indexed aggregates; never under an operation's locks).
const obligationInterval = 30 * time.Second

// startOperationalMetrics registers the operator's signals of P3.6 (the
// runbooks' alerts read them): the gRPC capacity pools and their
// CAPACITY_EXHAUSTED refusals, the database pool, the relay backlog by lane,
// and the unresolved obligations by class and state with the age of the
// oldest. Counts and ages only: no identifier reaches a label.
func startOperationalMetrics(lc fx.Lifecycle, pool *pgxpool.Pool, relay *application.Relay, srv *grpctransport.Server, reg *prometheus.Registry, log *slog.Logger) {
	reg.MustRegister(srv.Collectors()...)
	stat := func(f func(*pgxpool.Stat) float64) func() float64 { return func() float64 { return f(pool.Stat()) } }
	reg.MustRegister(
		prometheus.NewGaugeFunc(prometheus.GaugeOpts{Name: "anvilkit_control_db_pool_acquired_connections", Help: "Database connections in use."},
			stat(func(s *pgxpool.Stat) float64 { return float64(s.AcquiredConns()) })),
		prometheus.NewGaugeFunc(prometheus.GaugeOpts{Name: "anvilkit_control_db_pool_total_connections", Help: "Database connections open."},
			stat(func(s *pgxpool.Stat) float64 { return float64(s.TotalConns()) })),
		prometheus.NewGaugeFunc(prometheus.GaugeOpts{Name: "anvilkit_control_db_pool_max_connections", Help: "The database pool's bound."},
			stat(func(s *pgxpool.Stat) float64 { return float64(s.MaxConns()) })),
		prometheus.NewCounterFunc(prometheus.CounterOpts{Name: "anvilkit_control_db_pool_empty_acquires_total", Help: "Acquires that waited for a connection."},
			stat(func(s *pgxpool.Stat) float64 { return float64(s.EmptyAcquireCount()) })),
		relayBacklog{relay: relay, desc: prometheus.NewDesc("anvilkit_control_relay_backlog", "Pending relay intents the last tick of each lane saw (starts, answers, commands).", []string{"lane"}, nil)},
	)
	count := prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "anvilkit_control_unresolved_obligations",
		Help: "Unresolved obligations by class (dispatch, effect) and state (authorized/permitted, unknown)."}, []string{"class", "state"})
	oldest := prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "anvilkit_control_unresolved_obligation_oldest_seconds",
		Help: "Age of the oldest unresolved obligation by class and state; 0 when there is none."}, []string{"class", "state"})
	failures := prometheus.NewCounter(prometheus.CounterOpts{Name: "anvilkit_control_unresolved_obligation_reads_failed_total", Help: "Failed reads of the obligation gauges."})
	reg.MustRegister(count, oldest, failures)
	q := sqlc.New(pool)
	read := func(ctx context.Context) {
		rows, err := q.UnresolvedObligations(ctx)
		if err != nil {
			failures.Inc()
			log.Warn("unresolved obligation gauges not refreshed", "error", err)
			return
		}
		for _, class := range [][2]string{{"dispatch", "authorized"}, {"dispatch", "unknown"}, {"effect", "permitted"}, {"effect", "unknown"}} {
			count.WithLabelValues(class[0], class[1]).Set(0)
			oldest.WithLabelValues(class[0], class[1]).Set(0)
		}
		for _, r := range rows {
			count.WithLabelValues(r.Class, r.State).Set(float64(r.Obligations))
			oldest.WithLabelValues(r.Class, r.State).Set(r.OldestSeconds)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	lc.Append(fx.Hook{
		OnStart: func(context.Context) error {
			go func() {
				defer close(done)
				t := time.NewTicker(obligationInterval)
				defer t.Stop()
				for {
					rctx, rcancel := context.WithTimeout(ctx, 10*time.Second)
					read(rctx)
					rcancel()
					select {
					case <-ctx.Done():
						return
					case <-t.C:
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
			return nil
		},
	})
}

// relayBacklog reads the relay's per-lane backlog at scrape time.
type relayBacklog struct {
	relay *application.Relay
	desc  *prometheus.Desc
}

func (r relayBacklog) Describe(ch chan<- *prometheus.Desc) { ch <- r.desc }

func (r relayBacklog) Collect(ch chan<- prometheus.Metric) {
	for lane, n := range r.relay.Lag() {
		ch <- prometheus.MustNewConstMetric(r.desc, prometheus.GaugeValue, float64(n), lane)
	}
}
