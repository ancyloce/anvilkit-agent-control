package bootstrap

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/fx"

	"github.com/ancyloce/anvilkit-agent-control/internal/config"
	"github.com/ancyloce/anvilkit-agent-control/internal/migrate"
	grpctransport "github.com/ancyloce/anvilkit-agent-control/internal/transport/grpc"
)

// health is the plaintext HTTP probe listener of the Pod (health.listen):
// /healthz answers 200 while the process runs, /readyz 200 only while the
// gRPC listener serves and no shutdown has begun. It carries no business
// endpoint and no gRPC health service; the mTLS business port stays the
// only path to Control. It starts before and stops after the gRPC server
// so a probe during the drain sees NOT READY, not a refused connection.
type health struct {
	srv      *http.Server
	listen   string
	ready    func() bool
	stopping atomic.Bool
	// P1.4 (CTL-INF-05): readiness also needs the database (pinged every
	// interval) and, since P3.1 (MIG-02), a schema at or above the
	// binary's minimum version (checked at start and every interval until
	// it holds); liveness (/healthz) stays the process alone.
	ping         func(context.Context) error
	interval     time.Duration
	timeout      time.Duration
	schema       func(context.Context) error
	mu           sync.Mutex
	reasons      map[string]string
	cancelChecks context.CancelFunc
}

// pinger is what readiness pings (the store's pool).
type pinger interface{ Ping(context.Context) error }

func newHealth(cfg config.Config, grpcServer *grpctransport.Server, pool *pgxpool.Pool, log *slog.Logger) *health {
	h := &health{listen: cfg.Health.Listen, ready: grpcServer.Ready, ping: pool.Ping, interval: cfg.Health.CheckInterval, timeout: cfg.Health.CheckTimeout, reasons: map[string]string{}}
	h.schema = func(ctx context.Context) error { return checkSchema(ctx, pool) }
	h.install(log)
	return h
}

func (h *health) install(log *slog.Logger) {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, _ *http.Request) {
		if reason := h.notReady(); reason != "" {
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte(reason))
			return
		}
		w.WriteHeader(http.StatusOK)
	})
	h.srv = &http.Server{Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	_ = log
}

// notReady names the first reason readiness is withheld, or "".
func (h *health) notReady() string {
	if h.stopping.Load() {
		return "stopping"
	}
	if h.ready != nil && !h.ready() {
		return "listener not serving"
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, k := range []string{"schema", "database"} {
		if r := h.reasons[k]; r != "" {
			return k + ": " + r
		}
	}
	return ""
}

func (h *health) reason(key string) string {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.reasons[key]
}

func (h *health) setReason(key, reason string) {
	h.mu.Lock()
	h.reasons[key] = reason
	h.mu.Unlock()
}

// checks runs the schema check and the periodic database ping until ctx
// ends; a failed ping withdraws readiness, the next success restores it. A
// schema behind the minimum keeps the replica not ready and is checked
// again every interval, so the replica becomes ready once the migration
// Job has run, without a restart.
func (h *health) checks(ctx context.Context, log *slog.Logger) {
	schemaOK := h.schema == nil
	checkSchema := func() {
		if schemaOK {
			return
		}
		if err := h.schema(ctx); err != nil {
			if ctx.Err() == nil && h.reason("schema") != err.Error() {
				log.Error("schema check failed; the replica stays not ready", "error", err)
			}
			h.setReason("schema", err.Error())
			return
		}
		schemaOK = true
		h.setReason("schema", "")
	}
	checkSchema()
	if h.ping == nil || h.interval <= 0 {
		return
	}
	t := time.NewTicker(h.interval)
	defer t.Stop()
	for {
		checkSchema()
		pctx, cancel := context.WithTimeout(ctx, h.timeout)
		err := h.ping(pctx)
		cancel()
		if err != nil && ctx.Err() == nil {
			h.setReason("database", err.Error())
			log.Warn("database ping failed; readiness withdrawn", "error", err)
		} else if err == nil {
			h.setReason("database", "")
		}
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

// checkSchema compares the applied migration version with the binary's
// minimum, the highest migration it embeds. A schema ahead of the minimum
// is accepted: an expand migration of the next release may be applied
// while this image still serves (P3.4 upgrade case); a contracting change
// raises the next binary's minimum instead.
func checkSchema(ctx context.Context, pool *pgxpool.Pool) error {
	minimum, err := migrate.ExpectedVersion()
	if err != nil {
		return err
	}
	var applied int64
	if err := pool.QueryRow(ctx, "SELECT coalesce(max(version_id), 0) FROM goose_db_version WHERE is_applied").Scan(&applied); err != nil {
		return fmt.Errorf("schema version unreadable: %w", err)
	}
	return schemaVerdict(applied, minimum)
}

// schemaVerdict names the version pair when the applied schema is behind
// the binary's minimum.
func schemaVerdict(applied, minimum int64) error {
	if applied < minimum {
		return fmt.Errorf("schema at version %d, the binary needs at least %d", applied, minimum)
	}
	return nil
}

// register binds the listener first and shuts it down last (Fx runs stop
// hooks in reverse); withdraw flips readiness at the start of the drain.
func (h *health) register(lc fx.Lifecycle, log *slog.Logger) {
	lc.Append(fx.Hook{
		OnStart: func(context.Context) error {
			l, err := net.Listen("tcp", h.listen)
			if err != nil {
				return err
			}
			checks, cancel := context.WithCancel(context.Background())
			h.cancelChecks = cancel
			go h.checks(checks, log)
			go func() {
				if err := h.srv.Serve(l); err != nil && !errors.Is(err, http.ErrServerClosed) {
					log.Error("health listener stopped", "error", err)
				}
			}()
			log.Info("health listener up", "listen", l.Addr().String())
			return nil
		},
		OnStop: func(ctx context.Context) error {
			if h.cancelChecks != nil {
				h.cancelChecks()
			}
			return h.srv.Shutdown(ctx)
		},
	})
}

func (h *health) withdraw() { h.stopping.Store(true) }
