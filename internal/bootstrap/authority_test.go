package bootstrap

import (
	"context"
	"log/slog"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/fx"

	"github.com/ancyloce/anvilkit-agent-control/internal/adapters/postgres"
	"github.com/ancyloce/anvilkit-agent-control/internal/application"
	"github.com/ancyloce/anvilkit-agent-control/internal/config"
	"github.com/ancyloce/anvilkit-agent-control/internal/domain"
	"github.com/ancyloce/anvilkit-agent-control/internal/testdb"
)

// TestProductionStartNeedsAPriceBook: outside the DEVELOPMENT_ONLY chain an
// empty price book refuses the start with a named reason; the reviewed
// initial file is published idempotently and loaded (P2.1 AC7).
func TestProductionStartNeedsAPriceBook(t *testing.T) {
	inst := testdb.Start(t)
	store := postgres.NewStore(inst.Pool(t))
	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))
	prices := application.NewStoredPrices(store, log)
	finance := application.NewFinance(store, prices.Refresh, domain.SystemClock{}, log)
	ctx := context.Background()
	var cfg config.Config
	err := startPriceBook(ctx, cfg, prices, finance, log)
	require.ErrorContains(t, err, "production price book: the database holds no price revision and dispatch.prices.initial_file names none (ENV-06)")

	book := filepath.Join(t.TempDir(), "prices.yaml")
	require.NoError(t, os.WriteFile(book, []byte(`schemaVersion: 1
prices:
  - revision: deepseek-chat-2026-10
    kind: model
    route: deepseek-chat
    provider: deepseek
    model: deepseek-chat
    currency: USD
    effective_from: 2026-10-01T00:00:00Z
    effective_until: 2027-01-01T00:00:00Z
    per_million: {input_units: "270000", output_units: "1100000", reasoning_units: "1100000", cached_input_units: "70000"}
    max_exposure: "200000"
`), 0o600))
	cfg.Dispatch.Prices.InitialFile = book
	require.NoError(t, startPriceBook(ctx, cfg, prices, finance, log))
	require.Equal(t, 1, prices.Count())
	require.NoError(t, startPriceBook(ctx, cfg, prices, finance, log), "a restart publishes nothing new")
	p, ok := prices.PriceByRevision("deepseek-chat-2026-10")
	require.True(t, ok)
	require.Equal(t, int64(1_100_000), p.PerMillion[domain.UsageOutput])

	bad := filepath.Join(t.TempDir(), "bad.yaml")
	require.NoError(t, os.WriteFile(bad, []byte("schemaVersion: 1\nprices:\n  - revision: x\n    unknown: 1\n"), 0o600))
	cfg.Dispatch.Prices.InitialFile = bad
	require.ErrorContains(t, startPriceBook(ctx, cfg, prices, finance, log), "production price book: dispatch.prices.initial_file")
}

// TestModuleGraphResolves: every provider of the rewired dispatch chain is
// present (a dry run: no constructor runs).
func TestModuleGraphResolves(t *testing.T) {
	require.NoError(t, fx.ValidateApp(Module()))
}
