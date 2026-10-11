package bootstrap

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// P1.4 (CTL-INF-05): readiness follows the database ping and the schema
// check; liveness is the process alone.
func TestReadinessFollowsTheDatabaseAndTheSchema(t *testing.T) {
	var dbDown atomic.Bool
	schemaErr := errors.New("schema at version 13, the binary expects 14")
	h := &health{ready: func() bool { return true }, interval: 20 * time.Millisecond, timeout: 10 * time.Millisecond, reasons: map[string]string{}}
	h.ping = func(context.Context) error {
		if dbDown.Load() {
			return errors.New("connection refused")
		}
		return nil
	}
	h.schema = func(context.Context) error { return schemaErr }
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	h.install(log)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go h.checks(ctx, log)
	get := func(path string) (int, string) {
		rec := httptest.NewRecorder()
		h.srv.Handler.ServeHTTP(rec, httptest.NewRequest("GET", path, nil))
		return rec.Code, rec.Body.String()
	}
	require.Eventually(t, func() bool { code, body := get("/readyz"); return code == 503 && body == "schema: "+schemaErr.Error() }, time.Second, 5*time.Millisecond, "a schema behind the binary keeps the replica not ready, with the reason")
	code, _ := get("/healthz")
	require.Equal(t, 200, code, "liveness is the process alone")

	// With the schema right, readiness follows the ping.
	h2 := &health{ready: func() bool { return true }, interval: 20 * time.Millisecond, timeout: 10 * time.Millisecond, reasons: map[string]string{}, ping: h.ping}
	h2.install(log)
	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()
	go h2.checks(ctx2, log)
	get2 := func(path string) (int, string) {
		rec := httptest.NewRecorder()
		h2.srv.Handler.ServeHTTP(rec, httptest.NewRequest("GET", path, nil))
		return rec.Code, rec.Body.String()
	}
	require.Eventually(t, func() bool { code, _ := get2("/readyz"); return code == 200 }, time.Second, 5*time.Millisecond)
	dbDown.Store(true)
	require.Eventually(t, func() bool {
		code, body := get2("/readyz")
		return code == 503 && body == "database: connection refused"
	}, time.Second, 5*time.Millisecond, "readiness drops within a few intervals of the outage")
	code, _ = get2("/healthz")
	require.Equal(t, 200, code, "liveness never fails on a database outage")
	dbDown.Store(false)
	require.Eventually(t, func() bool { code, _ := get2("/readyz"); return code == 200 }, time.Second, 5*time.Millisecond, "readiness returns with the next successful ping")
	h2.withdraw()
	code, body := get2("/readyz")
	require.Equal(t, 503, code)
	require.Equal(t, "stopping", body)
}
