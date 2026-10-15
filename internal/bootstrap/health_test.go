package bootstrap

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/ancyloce/anvilkit-agent-control/internal/migrate"
	"github.com/ancyloce/anvilkit-agent-control/internal/testdb"
)

// P1.4 (CTL-INF-05): readiness follows the database ping and the schema
// check; liveness is the process alone.
func TestReadinessFollowsTheDatabaseAndTheSchema(t *testing.T) {
	var dbDown atomic.Bool
	schemaErr := errors.New("schema at version 13, the binary needs at least 14")
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

// P3.1 AC4 (MIG-02): a replica started against a schema behind the binary's
// minimum stays not ready and names both versions; it becomes ready once
// the migration Job has run, without a restart; a schema ahead of the
// minimum (the next release's expand migration) is accepted.
func TestSchemaBehindTheMinimumWithholdsReadiness(t *testing.T) {
	require.EqualError(t, schemaVerdict(16, 17), "schema at version 16, the binary needs at least 17")
	require.NoError(t, schemaVerdict(17, 17))
	require.NoError(t, schemaVerdict(18, 17), "an expand migration of the next release does not stop this image")

	inst := testdb.Start(t)
	ctx := context.Background()
	db, err := migrate.Open(strings.Replace(inst.AppDSN, "anvilkit_control_app:app", "anvilkit_control_migrator:migrator", 1))
	require.NoError(t, err)
	defer db.Close()
	behind, err := migrate.DownTo(ctx, db, migrate.Latest-1)
	require.NoError(t, err)
	require.Equal(t, migrate.Latest-1, behind)

	pool := inst.Pool(t)
	h := &health{ready: func() bool { return true }, ping: pool.Ping, interval: 20 * time.Millisecond, timeout: time.Second, reasons: map[string]string{}}
	h.schema = func(ctx context.Context) error { return checkSchema(ctx, pool) }
	var logged strings.Builder
	var mu sync.Mutex
	log := slog.New(slog.NewTextHandler(&lockedWriter{w: &logged, mu: &mu}, nil))
	h.install(log)
	checks, cancel := context.WithCancel(ctx)
	defer cancel()
	go h.checks(checks, log)
	get := func() (int, string) {
		rec := httptest.NewRecorder()
		h.srv.Handler.ServeHTTP(rec, httptest.NewRequest("GET", "/readyz", nil))
		return rec.Code, rec.Body.String()
	}
	want := fmt.Sprintf("schema: schema at version %d, the binary needs at least %d", migrate.Latest-1, migrate.Latest)
	require.Eventually(t, func() bool { code, body := get(); return code == 503 && body == want }, 5*time.Second, 10*time.Millisecond)
	mu.Lock()
	require.Contains(t, logged.String(), fmt.Sprintf("schema at version %d, the binary needs at least %d", migrate.Latest-1, migrate.Latest), "the version pair is logged")
	mu.Unlock()

	_, err = migrate.Up(ctx, db)
	require.NoError(t, err)
	require.Eventually(t, func() bool { code, _ := get(); return code == 200 }, 5*time.Second, 10*time.Millisecond, "the migrated schema makes the replica ready without a restart")
}

type lockedWriter struct {
	w  io.Writer
	mu *sync.Mutex
}

func (l *lockedWriter) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.w.Write(p)
}
