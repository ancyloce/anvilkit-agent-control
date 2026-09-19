package application_test

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/ancyloce/anvilkit-agent-control/internal/adapters/inventory"
	"github.com/ancyloce/anvilkit-agent-control/internal/adapters/postgres"
	"github.com/ancyloce/anvilkit-agent-control/internal/application"
	"github.com/ancyloce/anvilkit-agent-control/internal/domain"
	"github.com/ancyloce/anvilkit-agent-control/internal/testdb"
	"github.com/stretchr/testify/require"
)

func TestSettlementRegressions(t *testing.T) {
	inst := testdb.Start(t)
	inv, err := inventory.NewFilesystem(filepath.Join(t.TempDir(), "inventory"))
	require.NoError(t, err)
	pool := inst.Pool(t)
	store := postgres.NewStore(pool)
	p := profile
	p.MultiStep, p.SupportsControl, p.Definitions = true, true, []string{"original", "replacement"}
	profiles := []domain.Profile{p}
	ops := application.NewOperations(store, inv, profiles, domain.SystemClock{}, testLog)
	execution := func() *application.Execution {
		return application.NewExecution(store, inv, manifests, profiles, domain.SystemClock{}, testLog)
	}
	generations := application.NewGenerations(store, nil, profiles, nil, domain.SystemClock{}, testLog)
	ctx := context.Background()
	create := func(t *testing.T) *domain.Operation {
		t.Helper()
		op, _, err := ops.Create(ctx, cmd("tenant_a", domain.NewID("cmd"), "body"), scopeA, domain.KindLocalCheck, subject, nil)
		require.NoError(t, err)
		require.NoError(t, store.Tx(ctx, func(r application.Repo) error {
			if err := r.UpsertResourcePool(ctx, &domain.ResourcePool{ID: "regression", Class: "generation", Capacity: 10}); err != nil {
				return err
			}
			return r.InsertPermit(ctx, &domain.Permit{ID: domain.NewID("prm"), PoolID: "regression", OwnerKind: "operation", OwnerID: op.ID, FenceEpoch: op.ExecutionEpoch, State: domain.PermitActive, GrantedAt: time.Now()})
		}))
		return op
	}
	active := func(t *testing.T, id string) int {
		t.Helper()
		var n int
		require.NoError(t, pool.QueryRow(ctx, "SELECT count(*) FROM permits WHERE owner_id=$1 AND state='active'", id).Scan(&n))
		return n
	}
	t.Run("F05 old close cannot erase a later attempt's unknown cleanup", func(t *testing.T) {
		for _, cancel := range []bool{false, true} {
			t.Run(map[bool]string{false: "success", true: "cancellation"}[cancel], func(t *testing.T) {
				op := create(t)
				run := execution()
				a, _, err := run.OpenAttempt(ctx, cmd("tenant_a", op.ID+":open-a", "a"), op.ID, "analysis", 0, p.ID)
				require.NoError(t, err)
				closeA := cmd("tenant_a", op.ID+":close-a", "close-a")
				_, _, _, err = run.CloseAttempt(ctx, closeA, a.ID, domain.OutcomeCompleted, domain.CleanupComplete, "")
				require.NoError(t, err)
				b, _, err := run.OpenAttempt(ctx, cmd("tenant_a", op.ID+":open-b", "b"), op.ID, "codegen", 0, p.ID)
				require.NoError(t, err)
				if cancel {
					current, err := ops.Get(ctx, scopeA, op.ID)
					require.NoError(t, err)
					_, _, err = ops.SubmitCommand(ctx, cmd("tenant_a", op.ID+":cancel", "cancel"), scopeA, op.ID, domain.CommandCancel, current.Revision, "")
					require.NoError(t, err)
				}
				closeB := cmd("tenant_a", op.ID+":close-b", "close-b")
				_, current, _, err := run.CloseAttempt(ctx, closeB, b.ID, domain.OutcomeCompleted, domain.CleanupUnknown, "")
				require.NoError(t, err)
				revision := current.Revision
				for range 2 {
					_, current, existing, err := execution().CloseAttempt(ctx, closeA, a.ID, domain.OutcomeCompleted, domain.CleanupComplete, "")
					require.NoError(t, err)
					require.True(t, existing)
					require.Equal(t, domain.LifecycleReconciling, current.Lifecycle)
					require.Equal(t, domain.CleanupUnknown, current.Cleanup)
					require.Equal(t, revision, current.Revision)
					require.Equal(t, 1, active(t, op.ID))
				}
				outcome, terminal := domain.OperationSucceeded, domain.LifecycleSucceeded
				if cancel {
					outcome, terminal = domain.OperationCanceled, domain.LifecycleCanceled
					require.Equal(t, domain.ControlCancelPending, current.Control)
				}
				settle := cmd("tenant_a", op.ID+":settle", "settle")
				for range 2 {
					current, _, err = execution().SettleOperation(ctx, settle, op.ID, outcome, "", "done")
					require.NoError(t, err)
					require.Equal(t, domain.LifecycleReconciling, current.Lifecycle)
					require.Equal(t, 1, active(t, op.ID))
				}
				var cleanup string
				require.NoError(t, pool.QueryRow(ctx, "SELECT cleanup_state FROM attempts WHERE attempt_id=$1", b.ID).Scan(&cleanup))
				require.Equal(t, "unknown", cleanup)
				_, _, _, err = execution().CloseAttempt(ctx, closeB, b.ID, domain.OutcomeCompleted, domain.CleanupComplete, "")
				require.NoError(t, err)
				current, _, err = execution().SettleOperation(ctx, settle, op.ID, outcome, "", "done")
				require.NoError(t, err)
				require.Equal(t, terminal, current.Lifecycle)
				require.Equal(t, 0, active(t, op.ID))
				revision = current.Revision
				var events int
				var released time.Time
				require.NoError(t, pool.QueryRow(ctx, "SELECT count(*) FROM operation_events WHERE operation_id=$1", op.ID).Scan(&events))
				require.NoError(t, pool.QueryRow(ctx, "SELECT released_at FROM permits WHERE owner_id=$1", op.ID).Scan(&released))
				for range 2 {
					for i, at := range []*domain.Attempt{a, b} {
						_, _, existing, err := execution().CloseAttempt(ctx, []domain.CommandIdentity{closeA, closeB}[i], at.ID, domain.OutcomeCompleted, domain.CleanupComplete, "")
						require.NoError(t, err)
						require.True(t, existing)
					}
					current, existing, err := execution().SettleOperation(ctx, settle, op.ID, outcome, "", "done")
					require.NoError(t, err)
					require.True(t, existing)
					require.Equal(t, revision, current.Revision)
				}
				var finalEvents int
				var finalReleased time.Time
				require.NoError(t, pool.QueryRow(ctx, "SELECT count(*) FROM operation_events WHERE operation_id=$1", op.ID).Scan(&finalEvents))
				require.NoError(t, pool.QueryRow(ctx, "SELECT released_at FROM permits WHERE owner_id=$1", op.ID).Scan(&finalReleased))
				require.Equal(t, events, finalEvents)
				require.Equal(t, released, finalReleased)
			})
		}
	})
	t.Run("F04 F05 persisted unknown cleanup guards stale operation projections", func(t *testing.T) {
		op := create(t)
		run := execution()
		at, _, err := run.OpenAttempt(ctx, cmd("tenant_a", op.ID+":open", "open"), op.ID, "analysis", 0, p.ID)
		require.NoError(t, err)
		close := cmd("tenant_a", op.ID+":close", "close")
		_, _, _, err = run.CloseAttempt(ctx, close, at.ID, domain.OutcomeCompleted, domain.CleanupUnknown, "")
		require.NoError(t, err)
		// Model the projection left by the old replay bug. Neither normal
		// settlement nor terminal repair may release this persisted obligation.
		settle := cmd("tenant_a", op.ID+":settle", "settle")
		for _, lifecycle := range []string{"running", "succeeded"} {
			_, err = pool.Exec(ctx, "UPDATE operations SET lifecycle=$2, cleanup_state='complete' WHERE operation_id=$1", op.ID, lifecycle)
			require.NoError(t, err)
			current, _, err := execution().SettleOperation(ctx, settle, op.ID, domain.OperationSucceeded, "", "done")
			require.NoError(t, err)
			require.Equal(t, 1, active(t, op.ID))
			if lifecycle == "running" {
				require.Equal(t, domain.LifecycleReconciling, current.Lifecycle)
			}
		}
		_, _, _, err = run.CloseAttempt(ctx, close, at.ID, domain.OutcomeCompleted, domain.CleanupComplete, "")
		require.NoError(t, err)
		_, _, err = execution().SettleOperation(ctx, settle, op.ID, domain.OperationSucceeded, "", "done")
		require.NoError(t, err)
		require.Equal(t, 0, active(t, op.ID))
	})
	t.Run("F01 late receipts preserve cancellation", func(t *testing.T) {
		for _, kind := range []domain.CommandKind{domain.CommandHold, domain.CommandResume, domain.CommandChangeDefinition} {
			t.Run(string(kind), func(t *testing.T) {
				op := create(t)
				if kind == domain.CommandResume {
					_, err := pool.Exec(ctx, "UPDATE operations SET control_state='hold_applied', lifecycle='suspended' WHERE operation_id=$1", op.ID)
					require.NoError(t, err)
				}
				receipt, _, err := ops.SubmitCommand(ctx, cmd("tenant_a", domain.NewID("cmd"), "control"), scopeA, op.ID, kind, op.Revision, "replacement")
				require.NoError(t, err)
				require.Equal(t, domain.OutcomePending, receipt.Outcome)
				current, err := ops.Get(ctx, scopeA, op.ID)
				require.NoError(t, err)
				_, _, err = ops.SubmitCommand(ctx, cmd("tenant_a", domain.NewID("cmd"), "cancel"), scopeA, op.ID, domain.CommandCancel, current.Revision, "")
				require.NoError(t, err)
				late, err := generations.RecordCommandRelay(ctx, "tenant_a", receipt.CommandID, domain.OutcomeApplied, "")
				require.NoError(t, err)
				require.Equal(t, domain.OutcomeRejected, late.Outcome)
				current, err = ops.Get(ctx, scopeA, op.ID)
				require.NoError(t, err)
				require.Equal(t, domain.LifecycleCanceled, current.Lifecycle)
				require.Equal(t, domain.ControlCancelApplied, current.Control)
				require.Equal(t, "original", current.DefinitionActivation)
			})
		}
	})
	t.Run("F04 cancel before attempt releases permit and terminal reentry repairs cleanup", func(t *testing.T) {
		op := create(t)
		_, _, err := ops.SubmitCommand(ctx, cmd("tenant_a", domain.NewID("cmd"), "cancel"), scopeA, op.ID, domain.CommandCancel, op.Revision, "")
		require.NoError(t, err)
		require.Equal(t, 0, active(t, op.ID))
		// Model an older terminal record whose permit cleanup was missed.
		_, err = pool.Exec(ctx, "UPDATE permits SET state='active', released_at=NULL, release_evidence=NULL WHERE owner_id=$1", op.ID)
		require.NoError(t, err)
		_, _, err = execution().SettleOperation(ctx, cmd("tenant_a", op.ID+":settle", "settle"), op.ID, domain.OperationCanceled, "", "")
		require.NoError(t, err)
		require.Equal(t, 0, active(t, op.ID))
	})
	t.Run("F05 repeated settlement resumes after obligations resolve and process restarts", func(t *testing.T) {
		op := create(t)
		_, err := pool.Exec(ctx, "UPDATE operations SET finance_state='exposure_unknown' WHERE operation_id=$1", op.ID)
		require.NoError(t, err)
		command := cmd("tenant_a", op.ID+":settle", "settle")
		for i := 0; i < 2; i++ {
			current, _, err := execution().SettleOperation(ctx, command, op.ID, domain.OperationSucceeded, "", "candidate_ready")
			require.NoError(t, err)
			require.Equal(t, domain.LifecycleReconciling, current.Lifecycle)
			require.Equal(t, 1, active(t, op.ID))
		}
		_, err = pool.Exec(ctx, "UPDATE operations SET finance_state='settled' WHERE operation_id=$1", op.ID)
		require.NoError(t, err)
		current, _, err := execution().SettleOperation(ctx, command, op.ID, domain.OperationSucceeded, "", "candidate_ready")
		require.NoError(t, err)
		require.Equal(t, domain.LifecycleSucceeded, current.Lifecycle)
		require.Equal(t, 0, active(t, op.ID))
		revision := current.Revision
		current, _, err = execution().SettleOperation(ctx, command, op.ID, domain.OperationSucceeded, "", "candidate_ready")
		require.NoError(t, err)
		require.Equal(t, revision, current.Revision)
	})
	t.Run("F04 F05 an authorized sender retains its permit until observed then settles cancellation", func(t *testing.T) {
		op := create(t)
		run := execution()
		at, _, err := run.OpenAttempt(ctx, cmd("tenant_a", op.ID+":open", "open"), op.ID, "analysis", 0, p.ID)
		require.NoError(t, err)
		pools(t, pool, "unsettled", [3]int64{10_000_000, 10_000_000, 10_000_000})
		auth := &authorityDouble{allowed: map[string]bool{"tenant_a/" + modelRoute: true}}
		dispatch := application.NewDispatch(store, inv, fixturePrices(t, "regression", 1), auth, nil, domain.SystemClock{}, testLog)
		_, _, err = dispatch.Allocate(ctx, scopeA, op.ID, usd(1_000_000))
		require.NoError(t, err)
		sent, err := dispatch.Admit(ctx, cmd("tenant_a", op.ID+":send", "send"), modelRequest(op, at, op.ID+":send", "proxy", 1000))
		require.NoError(t, err)
		require.True(t, sent.Allowed)
		current, err := ops.Get(ctx, scopeA, op.ID)
		require.NoError(t, err)
		_, _, err = ops.SubmitCommand(ctx, cmd("tenant_a", op.ID+":cancel", "cancel"), scopeA, op.ID, domain.CommandCancel, current.Revision, "")
		require.NoError(t, err)
		_, current, _, err = run.CloseAttempt(ctx, cmd("tenant_a", op.ID+":close", "close"), at.ID, domain.OutcomeCanceled, domain.CleanupComplete, "")
		require.NoError(t, err)
		require.Equal(t, domain.LifecycleReconciling, current.Lifecycle)
		require.Equal(t, 1, active(t, op.ID))
		command := cmd("tenant_a", op.ID+":settle", "settle")
		current, _, err = run.SettleOperation(ctx, command, op.ID, domain.OperationCanceled, "", "")
		require.NoError(t, err)
		require.Equal(t, domain.LifecycleReconciling, current.Lifecycle)
		_, _, err = dispatch.Observe(ctx, sent.Dispatch.ID, "proxy", 1, domain.DispatchSucceeded, reported(domain.Usage{}), "receipt", time.Now())
		require.NoError(t, err)
		current, _, err = execution().SettleOperation(ctx, command, op.ID, domain.OperationCanceled, "", "")
		require.NoError(t, err)
		require.Equal(t, domain.LifecycleCanceled, current.Lifecycle)
		require.Equal(t, 0, active(t, op.ID))
	})

}
