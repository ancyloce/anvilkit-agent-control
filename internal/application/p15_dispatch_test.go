package application_test

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/ancyloce/anvilkit-agent-control/internal/adapters/inventory"
	"github.com/ancyloce/anvilkit-agent-control/internal/application"
	"github.com/ancyloce/anvilkit-agent-control/internal/domain"
	"github.com/ancyloce/anvilkit-agent-control/internal/testdb"
)

// fakeOutcomeQuery answers the original-identity query of a dispatch as scripted.
type fakeOutcomeQuery struct {
	known   bool
	outcome domain.DispatchOutcome
	usage   *domain.Usage
	asked   int
}

func (f *fakeOutcomeQuery) QueryDispatch(_ context.Context, d *domain.Dispatch) (application.DispatchOutcomeReport, error) {
	f.asked++
	if !f.known {
		return application.DispatchOutcomeReport{}, nil
	}
	return application.DispatchOutcomeReport{Known: true, Source: "proxy-query", Sequence: 1, Outcome: f.outcome, Usage: f.usage, ObservedAt: time.Now()}, nil
}

func (f *fakeOutcomeQuery) QueryEffect(context.Context, *domain.Effect) (application.EffectOutcomeReport, error) {
	return application.EffectOutcomeReport{}, nil
}

// P1.5 AC3 (CTL-APP-05): a dispatch still PREPARED when its attempt closes
// is denied under the close with its reservation released; an AUTHORIZED
// dispatch whose sender vanished is settled by the periodic reconciler from
// its original identity, outside any recovery run.
func TestPreparedDispatchesAreDeniedAtCloseAndAuthorizedOnesReconciled(t *testing.T) {
	inst := testdb.Start(t)
	fsInv, err := inventory.NewFilesystem(filepath.Join(t.TempDir(), "inventory"))
	require.NoError(t, err)
	inv := &hookedInventory{inner: fsInv}
	auth := &authorityDouble{allowed: map[string]bool{"tenant_a/" + modelRoute: true}}
	prices := newPriceSource(fixturePrices(t, "fx-model-p15", 1))
	p := newDispatchProcess(t, inst, inv, auth, prices)
	ctx := context.Background()
	pools(t, p.pool, "p15", [3]int64{1_000_000_000, 100_000_000, 10_000_000})

	// PREPARED (inventory uncertain) at the attempt's close: denied, released.
	op, at := funded(t, p, "p15c", 100_000)
	inv.failing.Store(true)
	req := modelRequest(op, at, "call-p15c", "proxy-a", 1000)
	_, err = p.dispatch.Admit(ctx, admitCmd("call-p15c", "body"), req)
	require.ErrorIs(t, err, domain.ErrEffectUncertain)
	inv.failing.Store(false)
	reserved, _ := allocations(t, p.pool, op.ID)
	require.Equal(t, []int64{1000, 1000, 1000}, reserved)
	_, _, _, err = p.exec.CloseAttempt(ctx, cmd("tenant_a", at.ID+":close", "c"), at.ID, domain.OutcomeInfrastructureFailed, domain.CleanupNotRequired, "OBSERVER_FAILED")
	require.NoError(t, err)
	var state, denial string
	require.NoError(t, p.pool.QueryRow(ctx, "SELECT state, coalesce(denial_code, '') FROM dispatches WHERE call_id = 'call-p15c'").Scan(&state, &denial))
	require.Equal(t, "denied", state)
	require.Equal(t, "ATTEMPT_CLOSED", denial)
	reserved, _ = allocations(t, p.pool, op.ID)
	require.Equal(t, []int64{0, 0, 0}, reserved, "the reservation of the denied dispatch is released")
	puts := inv.puts.Load()
	a, err := p.dispatch.Admit(ctx, admitCmd("call-p15c", "body"), req)
	require.NoError(t, err)
	require.False(t, a.Allowed, "the reentry finds the denial; nothing is inventoried or sent")
	require.Equal(t, domain.DispatchDenied, a.Dispatch.State)
	require.Equal(t, puts, inv.puts.Load())

	// AUTHORIZED with a vanished sender: the reconciler settles it from the original identity.
	op2, at2 := funded(t, p, "p15r", 100_000)
	granted, err := p.dispatch.Admit(ctx, admitCmd("call-p15r", "body"), modelRequest(op2, at2, "call-p15r", "proxy-a", 1000))
	require.NoError(t, err)
	require.True(t, granted.Allowed)
	unknown := &fakeOutcomeQuery{known: false}
	n, err := p.dispatch.ReconcileUnresolved(ctx, unknown, 0, 10)
	require.NoError(t, err)
	require.Zero(t, n, "an unknown answer settles nothing; the exposure stays")
	require.GreaterOrEqual(t, unknown.asked, 1)
	reserved, _ = allocations(t, p.pool, op2.ID)
	require.Equal(t, []int64{1000, 1000, 1000}, reserved)
	known := &fakeOutcomeQuery{known: true, outcome: domain.DispatchSucceeded, usage: reported(domain.Usage{Input: 100, Output: 50})}
	n, err = p.dispatch.ReconcileUnresolved(ctx, known, 0, 10)
	require.NoError(t, err)
	require.Equal(t, 1, n)
	require.NoError(t, p.pool.QueryRow(ctx, "SELECT state FROM dispatches WHERE call_id = 'call-p15r'").Scan(&state))
	require.Equal(t, "observed", state)
	reserved, _ = allocations(t, p.pool, op2.ID)
	require.Equal(t, []int64{0, 0, 0}, reserved, "a definite outcome with usage releases the reservation")
	kinds, total := entries(t, p.pool, granted.Dispatch.ID)
	require.Equal(t, []string{"actual"}, kinds)
	require.Equal(t, int64(100*3+50*15), total, "the usage the original identity reported is charged")
	// Settled dispatches are not asked about again.
	n, err = p.dispatch.ReconcileUnresolved(ctx, known, 0, 10)
	require.NoError(t, err)
	require.Zero(t, n)
	require.Equal(t, 1, known.asked)
}

// P1.5 AC5 (CTL-APP-07): a fence that lands between the launch's first
// transaction and its inventory write refuses the confirmation; no Job is
// created from an unconfirmed launch.
func TestLaunchConfirmationRevalidatesTheFence(t *testing.T) {
	inst := testdb.Start(t)
	fsInv, err := inventory.NewFilesystem(filepath.Join(t.TempDir(), "inventory"))
	require.NoError(t, err)
	inv := &hookedInventory{inner: fsInv}
	auth := &authorityDouble{allowed: map[string]bool{}}
	p := newDispatchProcess(t, inst, inv, auth, newPriceSource(fixturePrices(t, "fx-model-p15l", 1)))
	ctx := context.Background()
	op, at := funded(t, p, "p15l", 0)
	inv.setHook(func(key string) {
		if strings.HasPrefix(key, "job-launch/") {
			// A hold fences the operation while the obligation is being written.
			_, err := p.pool.Exec(ctx, "UPDATE operations SET control_state = 'hold_pending' WHERE operation_id = $1", op.ID)
			require.NoError(t, err)
		}
	})
	_, _, err = p.exec.PrepareLaunch(ctx, cmd("tenant_a", at.ID+":launch", "l"), at.ID, "lk-p15", "kind-dev", imageDig, time.Now().Add(10*time.Minute))
	require.ErrorIs(t, err, domain.ErrStaleExecution, "the confirmation is refused under the fence")
	var inventoryState string
	require.NoError(t, p.pool.QueryRow(ctx, "SELECT inventory_state FROM launches WHERE attempt_id = $1", at.ID).Scan(&inventoryState))
	require.NotEqual(t, "confirmed", inventoryState, "an unconfirmed launch creates no Job")
	inv.setHook(nil)
	// The same command after the fence stays refused (the operation is fenced).
	_, _, err = p.exec.PrepareLaunch(ctx, cmd("tenant_a", at.ID+":launch", "l"), at.ID, "lk-p15", "kind-dev", imageDig, time.Now().Add(10*time.Minute))
	require.ErrorIs(t, err, domain.ErrStaleExecution)
}
