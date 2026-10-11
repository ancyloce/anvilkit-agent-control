package application_test

import (
	"context"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/ancyloce/anvilkit-agent-control/internal/adapters/development"
	"github.com/ancyloce/anvilkit-agent-control/internal/adapters/inventory"
	"github.com/ancyloce/anvilkit-agent-control/internal/adapters/postgres"
	"github.com/ancyloce/anvilkit-agent-control/internal/application"
	"github.com/ancyloce/anvilkit-agent-control/internal/domain"
	"github.com/ancyloce/anvilkit-agent-control/internal/testdb"
)

// P1.5 AC6 (CTL-APP-08): recovery epochs come from a sequence, so runs
// begun concurrently on different scopes hold distinct epochs; a second run
// on a scope whose closure is open is refused RECOVERY_IN_PROGRESS, and a
// run over every scope cannot begin beside an open one.
func TestRecoveryEpochsAreUniqueAndScopesExclusive(t *testing.T) {
	inst := testdb.Start(t)
	fsInv, err := inventory.NewFilesystem(filepath.Join(t.TempDir(), "inventory"))
	require.NoError(t, err)
	inv := &hookedInventory{inner: fsInv}
	auth := &authorityDouble{allowed: map[string]bool{"tenant_a/" + modelRoute: true}}
	prices := newPriceSource(fixturePrices(t, "fx-model-r", 1))
	base := newDispatchProcess(t, inst, inv, auth, prices)
	store := postgres.NewStore(base.pool)
	queries := development.NewOutcomeQuery(inv, []string{issuer})
	effects := application.NewEffects(store, inv, queries, domain.SystemClock{}, testLog)
	recovery := application.NewRecovery(store, inv, queries, base.dispatch, effects, base.exec, []domain.Profile{profile},
		development.NewNotSentEvidence(inv, []string{issuer}), development.NewDispositionEvidence(inv, []string{issuer}), domain.SystemClock{}, testLog, 2)
	ctx := context.Background()
	opCmd := func(tenant, id string) domain.CommandIdentity {
		return domain.CommandIdentity{TenantID: tenant, CommandID: id, ActorID: operatorActor, RequestDigest: application.DigestOf([]byte(id))}
	}
	begin := func(tenant, id string) (*domain.RecoveryRun, error) {
		c := opCmd(tenant, id)
		run, _, err := recovery.Begin(ctx, c, opScope(c), tenant, time.Now().Add(-time.Hour), time.Now().Add(time.Minute), 0, id)
		return run, err
	}

	// Two scopes begin concurrently: distinct epochs.
	var wg sync.WaitGroup
	runs := make([]*domain.RecoveryRun, 2)
	errs := make([]error, 2)
	for i, tenant := range []string{"tenant_a", "tenant_b"} {
		wg.Add(1)
		go func(i int, tenant string) {
			defer wg.Done()
			runs[i], errs[i] = begin(tenant, "begin_"+tenant)
		}(i, tenant)
	}
	wg.Wait()
	require.NoError(t, errs[0])
	require.NoError(t, errs[1])
	require.NotEqual(t, runs[0].RecoveryEpoch, runs[1].RecoveryEpoch, "concurrent runs never share an epoch")
	var distinct int
	require.NoError(t, base.pool.QueryRow(ctx, "SELECT count(DISTINCT recovery_epoch) FROM recovery_runs").Scan(&distinct))
	require.Equal(t, 2, distinct)

	// A second run on an open scope is refused; the same command reenters its run.
	_, err = begin("tenant_a", "begin_tenant_a_again")
	require.ErrorIs(t, err, domain.ErrStaleExecution)
	require.ErrorContains(t, err, "RECOVERY_IN_PROGRESS")
	again, existing, err := recovery.Begin(ctx, opCmd("tenant_a", "begin_tenant_a"), opScope(opCmd("tenant_a", "begin_tenant_a")), "tenant_a", time.Now().Add(-time.Hour), time.Now().Add(time.Minute), 0, "begin_tenant_a")
	require.NoError(t, err)
	require.True(t, existing)
	require.Equal(t, runs[0].ID, again.ID)
	// A run over every scope cannot begin beside the open ones.
	global := domain.CommandIdentity{TenantID: "", CommandID: "begin_all", ActorID: operatorActor, RequestDigest: application.DigestOf([]byte("begin_all"))}
	_, _, err = recovery.Begin(ctx, global, opScope(global), domain.ScopeAll, time.Now().Add(-time.Hour), time.Now().Add(time.Minute), 0, "all")
	require.Error(t, err, "a global run beside open scope runs is refused (or its operator scope is)")
	// The sequence advanced: no epoch is ever reused.
	var next int64
	require.NoError(t, base.pool.QueryRow(ctx, "SELECT nextval('recovery_epoch_seq')").Scan(&next))
	require.Greater(t, uint64(next), runs[0].RecoveryEpoch)
	require.Greater(t, uint64(next), runs[1].RecoveryEpoch)
}
