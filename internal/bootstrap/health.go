package bootstrap

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"sync/atomic"
	"time"

	"go.uber.org/fx"

	"github.com/ancyloce/anvilkit-agent-control/internal/config"
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
}

func newHealth(cfg config.Config, grpcServer *grpctransport.Server) *health {
	h := &health{listen: cfg.Health.Listen, ready: grpcServer.Ready}
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, _ *http.Request) {
		if h.stopping.Load() || !h.ready() {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	})
	h.srv = &http.Server{Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	return h
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
			go func() {
				if err := h.srv.Serve(l); err != nil && !errors.Is(err, http.ErrServerClosed) {
					log.Error("health listener stopped", "error", err)
				}
			}()
			log.Info("health listener up", "listen", l.Addr().String())
			return nil
		},
		OnStop: func(ctx context.Context) error { return h.srv.Shutdown(ctx) },
	})
}

func (h *health) withdraw() { h.stopping.Store(true) }
