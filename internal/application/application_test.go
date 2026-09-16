package application_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	"github.com/ancyloce/anvilkit-agent-control/internal/adapters/inventory"
	"github.com/ancyloce/anvilkit-agent-control/internal/adapters/jobs"
	"github.com/ancyloce/anvilkit-agent-control/internal/adapters/postgres"
	"github.com/ancyloce/anvilkit-agent-control/internal/application"
	"github.com/ancyloce/anvilkit-agent-control/internal/domain"
	"github.com/ancyloce/anvilkit-agent-control/internal/testdb"
)

var (
	profile   = domain.Profile{ID: "local-check-v1", Kind: domain.KindLocalCheck, OperationDeadline: 15 * time.Minute, StepID: "local-check", JobProfileID: "local-check-v1"}
	subject   = domain.Subject{ProfileID: "local-check-v1", SubjectDigest: application.DigestOf([]byte("subject"))}
	scopeA    = domain.Scope{TenantID: "tenant_a", ProjectID: "proj", ActorID: "user_a"}
	scopeB    = domain.Scope{TenantID: "tenant_b", ProjectID: "proj", ActorID: "user_b"}
	imageDig  = domain.Digest("sha256:9db7b59979c38555a39def84a31fb98b5296952f9e3afd4f6f11f05b07adfab0")
	testLog   = slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))
	manifests = jobs.Contract{}
	// fixedResult is the reviewed expectedResult.resultDigest of the
	// local-check-v1 fixture profile (contracts/jobs/profiles.json): the one
	// result output Control accepts without a finalized object.
	fixedResult = "sha256:0dc7fa9db7237a2b5c96f70f59bb00f73bb86a0ca5554e91c312f9ada26e18b3"
)

// flakyInventory models the two uncertain outcomes of an inventory write
// on top of the real filesystem adapter: while failing is set the write
// never happens; while ackLost is set the write happens (the object is
// durably published) but the caller gets an error instead of the version,
// as when the process dies or the connection drops after the backend
// committed.
type flakyInventory struct {
	inner   *inventory.Filesystem
	failing atomic.Bool
	ackLost atomic.Bool
}

func (f *flakyInventory) Put(ctx context.Context, key string, body []byte) (string, error) {
	if f.failing.Load() {
		return "", errors.New("inventory backend unreachable")
	}
	version, err := f.inner.Put(ctx, key, body)
	if err == nil && f.ackLost.Load() {
		return "", errors.New("connection reset after the object was published")
	}
	return version, err
}

func (f *flakyInventory) Get(ctx context.Context, key string) ([]byte, string, error) {
	return f.inner.Get(ctx, key)
}

func (f *flakyInventory) List(ctx context.Context, prefix, cursor string, limit int) (application.InventoryPage, error) {
	if f.failing.Load() {
		return application.InventoryPage{}, errors.New("inventory backend unreachable")
	}
	return f.inner.List(ctx, prefix, cursor, limit)
}

// objectPath is where the filesystem adapter publishes a key.
func (f *flakyInventory) objectPath(key string) string { return filepath.Join(f.inner.Root(), key) }

type fakeWorkflows struct {
	mu      sync.Mutex
	starts  map[string]int
	cancels map[string]int
}

func (f *fakeWorkflows) Start(_ context.Context, id, _ string) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.starts[id]++
	return "run-" + id, nil
}

func (f *fakeWorkflows) Cancel(_ context.Context, id string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.cancels[id]++
	return nil
}

func (f *fakeWorkflows) StartRecovery(_ context.Context, runID string) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.starts["recovery:"+runID]++
	return "run-" + runID, nil
}

// process models one Control replica: its own pool and use cases over the
// shared database and the shared inventory.
type process struct {
	pool *pgxpool.Pool
	ops  *application.Operations
	exec *application.Execution
}

func newProcess(t *testing.T, inst *testdb.Instance, inv application.Inventory) *process {
	pool := inst.Pool(t)
	store := postgres.NewStore(pool)
	return &process{
		pool: pool,
		ops:  application.NewOperations(store, inv, []domain.Profile{profile}, domain.SystemClock{}, testLog),
		exec: application.NewExecution(store, inv, manifests, domain.SystemClock{}, testLog),
	}
}

func cmd(tenant, id string, body string) domain.CommandIdentity {
	return domain.CommandIdentity{TenantID: tenant, CommandID: id, ActorID: "user_" + tenant, RequestDigest: application.DigestOf([]byte(body))}
}

func eventCount(t *testing.T, pool *pgxpool.Pool, opID string) (count int, lastRevision int64, lastSeq int64) {
	t.Helper()
	require.NoError(t, pool.QueryRow(context.Background(),
		"SELECT count(*), coalesce(max(revision),0), coalesce(max(event_seq),0) FROM operation_events WHERE operation_id = $1", opID).Scan(&count, &lastRevision, &lastSeq))
	return
}

func TestControl(t *testing.T) {
	inst := testdb.Start(t)
	fsInv, err := inventory.NewFilesystem(filepath.Join(t.TempDir(), "inventory"))
	require.NoError(t, err)
	inv := &flakyInventory{inner: fsInv}
	p1, p2 := newProcess(t, inst, inv), newProcess(t, inst, inv)
	ctx := context.Background()

	t.Run("duplicate requests on two processes converge on one operation", func(t *testing.T) {
		c := cmd("tenant_a", "cmd_dup", "body")
		type res struct {
			op       *domain.Operation
			existing bool
			err      error
		}
		results := make(chan res, 2)
		var wg sync.WaitGroup
		for _, p := range []*process{p1, p2} {
			wg.Add(1)
			go func(p *process) {
				defer wg.Done()
				op, existing, err := p.ops.Create(ctx, c, scopeA, domain.KindLocalCheck, subject)
				results <- res{op, existing, err}
			}(p)
		}
		wg.Wait()
		close(results)
		var ids []string
		existingCount := 0
		for r := range results {
			require.NoError(t, r.err)
			ids = append(ids, r.op.ID)
			if r.existing {
				existingCount++
			}
			require.Equal(t, domain.IntakeConfirmed, r.op.Intake, "acknowledged only after intake inventory confirmation")
			require.NotEmpty(t, r.op.IntakeVersion)
		}
		require.Equal(t, ids[0], ids[1], "same command identity, same operation")
		require.Equal(t, 1, existingCount, "exactly one caller created it")
		count, lastRev, lastSeq := eventCount(t, p1.pool, ids[0])
		require.Equal(t, 2, count, "accepted + intake_confirmed events")
		op, err := p2.ops.Get(ctx, scopeA, ids[0])
		require.NoError(t, err)
		require.Equal(t, int64(op.Revision), lastRev, "projection revision equals the last committed event revision")
		require.Equal(t, int64(op.CoveredEventSeq()), lastSeq)
		_, _, err = p1.ops.Create(ctx, cmd("tenant_a", "cmd_dup", "different body"), scopeA, domain.KindLocalCheck, subject)
		require.ErrorIs(t, err, domain.ErrIdempotencyConflict, "changed digest conflicts")
	})

	t.Run("cross-tenant reads and commands are not found", func(t *testing.T) {
		op, _, err := p1.ops.Create(ctx, cmd("tenant_a", "cmd_scope", "body"), scopeA, domain.KindLocalCheck, subject)
		require.NoError(t, err)
		_, err = p2.ops.Get(ctx, scopeB, op.ID)
		require.ErrorIs(t, err, domain.ErrNotFound)
		_, err = p2.ops.ListEvents(ctx, scopeB, op.ID, 0, 10)
		require.ErrorIs(t, err, domain.ErrNotFound)
		_, _, err = p2.ops.SubmitCommand(ctx, cmd("tenant_b", "cmd_scope_cancel", "cancel"), scopeB, op.ID, domain.CommandCancel, op.Revision, "")
		require.ErrorIs(t, err, domain.ErrNotFound)
		_, _, err = p2.exec.OpenAttempt(ctx, cmd("tenant_b", "cmd_scope_att", "open"), op.ID, "local-check", 0, "local-check-v1")
		require.ErrorIs(t, err, domain.ErrNotFound)
	})

	t.Run("unqualified profile is rejected before any record exists", func(t *testing.T) {
		_, _, err := p1.ops.Create(ctx, cmd("tenant_a", "cmd_badprofile", "body"), scopeA, domain.KindGeneration, domain.Subject{ProfileID: "codegen-v1", SubjectDigest: subject.SubjectDigest})
		require.ErrorIs(t, err, domain.ErrProfileUnqualified)
		var n int
		require.NoError(t, p1.pool.QueryRow(ctx, "SELECT count(*) FROM operations WHERE command_id = 'cmd_badprofile'").Scan(&n))
		require.Zero(t, n)
	})

	t.Run("lost intake acknowledgement survives a database restart under the original identity", func(t *testing.T) {
		inv.failing.Store(true)
		c := cmd("tenant_a", "cmd_lost_intake", "body")
		_, _, err := p1.ops.Create(ctx, c, scopeA, domain.KindLocalCheck, subject)
		require.ErrorIs(t, err, domain.ErrEffectUncertain, "no acknowledgement while the inventory write is uncertain")
		var opID, intake string
		require.NoError(t, p1.pool.QueryRow(ctx, "SELECT operation_id, intake_state FROM operations WHERE command_id = 'cmd_lost_intake'").Scan(&opID, &intake))
		require.Equal(t, "pending", intake)

		inst.Restart(t)
		p1.pool.Reset()
		p2.pool.Reset()
		inv.failing.Store(false)
		op, existing, err := p2.ops.Create(ctx, c, scopeA, domain.KindLocalCheck, subject)
		require.NoError(t, err)
		require.True(t, existing)
		require.Equal(t, opID, op.ID, "the original identity is confirmed, no replacement is created")
		require.Equal(t, domain.IntakeConfirmed, op.Intake)
		n, err := p1.ops.ReconcileIntake(ctx, 0, 10)
		require.NoError(t, err)
		require.Zero(t, n, "nothing left pending")
	})

	t.Run("intake inventory written but acknowledgement lost: the other process confirms the original", func(t *testing.T) {
		inv.ackLost.Store(true)
		c := cmd("tenant_a", "cmd_lost_ack", "body")
		_, _, err := p1.ops.Create(ctx, c, scopeA, domain.KindLocalCheck, subject)
		require.ErrorIs(t, err, domain.ErrEffectUncertain)
		var opID, intake string
		require.NoError(t, p1.pool.QueryRow(ctx, "SELECT operation_id, intake_state FROM operations WHERE command_id = 'cmd_lost_ack'").Scan(&opID, &intake))
		require.Equal(t, "pending", intake, "the record exists but was not acknowledged")
		published, err := os.ReadFile(inv.objectPath("intake/" + opID))
		require.NoError(t, err, "the obligation was durably published before the acknowledgement was lost")

		inv.ackLost.Store(false)
		op, existing, err := p2.ops.Create(ctx, c, scopeA, domain.KindLocalCheck, subject)
		require.NoError(t, err)
		require.True(t, existing)
		require.Equal(t, opID, op.ID)
		require.Equal(t, domain.IntakeConfirmed, op.Intake)
		require.Equal(t, string(application.DigestOf(published)), op.IntakeVersion, "the confirmed version is the one already published, not a second object")
		entries, err := os.ReadDir(filepath.Dir(inv.objectPath("intake/" + opID)))
		require.NoError(t, err)
		for _, e := range entries {
			require.NotContains(t, e.Name(), ".partial", "no interrupted write is left behind")
		}
	})

	t.Run("launch inventory written but acknowledgement lost: the same command confirms the original launch", func(t *testing.T) {
		op, _, err := p1.ops.Create(ctx, cmd("tenant_a", "cmd_lost_launch", "body"), scopeA, domain.KindLocalCheck, subject)
		require.NoError(t, err)
		at, _, err := p1.exec.OpenAttempt(ctx, cmd("tenant_a", "cmd_lost_launch_open", "open"), op.ID, "local-check", 0, "local-check-v1")
		require.NoError(t, err)
		inv.ackLost.Store(true)
		lc := cmd("tenant_a", "cmd_lost_launch_launch", "launch")
		_, _, err = p1.exec.PrepareLaunch(ctx, lc, at.ID, "validator-lost-1", "kind-anvilkit-dev", imageDig, time.Now().Add(2*time.Minute))
		require.ErrorIs(t, err, domain.ErrEffectUncertain)
		var launchID, state string
		require.NoError(t, p1.pool.QueryRow(ctx, "SELECT launch_id, inventory_state FROM launches WHERE attempt_id = $1", at.ID).Scan(&launchID, &state))
		require.Equal(t, "pending", state)
		_, err = os.Stat(inv.objectPath("job-launch/" + launchID))
		require.NoError(t, err, "the launch obligation is published")

		inv.ackLost.Store(false)
		l, existing, err := p2.exec.PrepareLaunch(ctx, lc, at.ID, "validator-lost-1", "kind-anvilkit-dev", imageDig, time.Now().Add(2*time.Minute))
		require.NoError(t, err)
		require.True(t, existing)
		require.Equal(t, launchID, l.ID, "no second launch identity")
		require.Equal(t, domain.InventoryConfirmed, l.Inventory)
		var launches int
		require.NoError(t, p1.pool.QueryRow(ctx, "SELECT count(*) FROM launches WHERE operation_id = $1", op.ID).Scan(&launches))
		require.Equal(t, 1, launches)
		// A changed request under the same command identity is a conflict, not a new launch.
		_, _, err = p2.exec.PrepareLaunch(ctx, cmd("tenant_a", "cmd_lost_launch_launch", "launch-changed"), at.ID, "validator-lost-1", "kind-anvilkit-dev", imageDig, time.Now().Add(2*time.Minute))
		require.ErrorIs(t, err, domain.ErrIdempotencyConflict)
	})

	t.Run("no launch is recorded after the absolute deadline", func(t *testing.T) {
		op, _, err := p1.ops.Create(ctx, cmd("tenant_a", "cmd_deadline", "body"), scopeA, domain.KindLocalCheck, subject)
		require.NoError(t, err)
		at, _, err := p1.exec.OpenAttempt(ctx, cmd("tenant_a", "cmd_deadline_open", "open"), op.ID, "local-check", 0, "local-check-v1")
		require.NoError(t, err)
		// Move the absolute deadline into the past as time would.
		_, err = p1.pool.Exec(ctx, "UPDATE attempts SET deadline = now() - interval '1 second' WHERE attempt_id = $1", at.ID)
		require.NoError(t, err)
		_, err = p1.pool.Exec(ctx, "UPDATE operations SET deadline = now() - interval '1 second' WHERE operation_id = $1", op.ID)
		require.NoError(t, err)
		_, _, err = p2.exec.PrepareLaunch(ctx, cmd("tenant_a", "cmd_deadline_launch", "launch"), at.ID, "validator-deadline-1", "kind-anvilkit-dev", imageDig, time.Now().Add(2*time.Minute))
		require.ErrorIs(t, err, domain.ErrStaleExecution)
		var launches int
		require.NoError(t, p1.pool.QueryRow(ctx, "SELECT count(*) FROM launches WHERE operation_id = $1", op.ID).Scan(&launches))
		require.Zero(t, launches)
		_, _, err = p2.exec.OpenAttempt(ctx, cmd("tenant_a", "cmd_deadline_open2", "open"), op.ID, "local-check", 1, "local-check-v1")
		require.ErrorIs(t, err, domain.ErrStaleExecution, "no new attempt after the deadline either")
	})

	t.Run("cancel stays pending while cleanup is unknown", func(t *testing.T) {
		op, _, err := p1.ops.Create(ctx, cmd("tenant_a", "cmd_unknown", "body"), scopeA, domain.KindLocalCheck, subject)
		require.NoError(t, err)
		at, _, err := p1.exec.OpenAttempt(ctx, cmd("tenant_a", "cmd_unknown_open", "open"), op.ID, "local-check", 0, "local-check-v1")
		require.NoError(t, err)
		_, _, err = p1.exec.PrepareLaunch(ctx, cmd("tenant_a", "cmd_unknown_launch", "launch"), at.ID, "validator-unknown-1", "kind-anvilkit-dev", imageDig, time.Now().Add(2*time.Minute))
		require.NoError(t, err)
		_, _, err = p1.exec.RegisterInstance(ctx, cmd("tenant_a", "cmd_unknown_reg", "reg"), at.ID, "validator-unknown-1", "kind-anvilkit-dev", "job-u", "pod-u", imageDig)
		require.NoError(t, err)
		current, err := p2.ops.Get(ctx, scopeA, op.ID)
		require.NoError(t, err)
		rc, _, err := p2.ops.SubmitCommand(ctx, cmd("tenant_a", "cmd_unknown_cancel", "cancel"), scopeA, op.ID, domain.CommandCancel, current.Revision, "")
		require.NoError(t, err)
		require.Equal(t, domain.OutcomePending, rc.Outcome)

		// The Workflow could not confirm that the Job and its Pods stopped.
		_, closedOp, _, err := p1.exec.CloseAttempt(ctx, cmd("tenant_a", "cmd_unknown_close", "close"), at.ID, domain.OutcomeCanceled, domain.CleanupUnknown, "CANCELED")
		require.NoError(t, err)
		require.Equal(t, domain.LifecycleReconciling, closedOp.Lifecycle)
		require.Equal(t, domain.ControlCancelPending, closedOp.Control, "cancel is not applied while senders may still run")
		require.Equal(t, domain.CleanupUnknown, closedOp.Cleanup)
		var outcome string
		require.NoError(t, p1.pool.QueryRow(ctx, "SELECT outcome FROM operation_commands WHERE command_id = 'cmd_unknown_cancel'").Scan(&outcome))
		require.Equal(t, "pending", outcome, "the tracked cancel command is not settled as applied")
		rc2, _, err := p2.ops.SubmitCommand(ctx, cmd("tenant_a", "cmd_unknown_cancel2", "cancel"), scopeA, op.ID, domain.CommandCancel, closedOp.Revision, "")
		require.NoError(t, err)
		require.Equal(t, domain.OutcomePending, rc2.Outcome, "a repeated cancel while reconciling is still pending")
		_, _, err = p2.exec.OpenAttempt(ctx, cmd("tenant_a", "cmd_unknown_open2", "open"), op.ID, "local-check", 1, "local-check-v1")
		require.ErrorIs(t, err, domain.ErrStaleExecution, "reconciling fences new dispatch")
		// Reentering the same close from the other process returns the original outcome.
		_, again, existing, err := p2.exec.CloseAttempt(ctx, cmd("tenant_a", "cmd_unknown_close", "close"), at.ID, domain.OutcomeCanceled, domain.CleanupUnknown, "CANCELED")
		require.NoError(t, err)
		require.True(t, existing)
		require.Equal(t, domain.LifecycleReconciling, again.Lifecycle)
		// A different outcome for the closed attempt is still a conflict.
		_, _, _, err = p2.exec.CloseAttempt(ctx, cmd("tenant_a", "cmd_unknown_close_other", "close"), at.ID, domain.OutcomeCompleted, domain.CleanupComplete, "")
		require.ErrorIs(t, err, domain.ErrIdempotencyConflict)

		// Later cleanup evidence from the durable recovery owner settles the
		// same attempt: the reconciling operation becomes canceled, the cancel
		// fence is applied and every pending cancel command settles as applied.
		before, _, beforeSeq := eventCount(t, p1.pool, op.ID)
		settledAt, settledOp, existing, err := p2.exec.CloseAttempt(ctx, cmd("tenant_a", "cmd_unknown_close_settled", "close"), at.ID, domain.OutcomeCanceled, domain.CleanupComplete, "CANCELED")
		require.NoError(t, err)
		require.False(t, existing)
		require.Equal(t, domain.CleanupComplete, settledAt.Cleanup)
		require.Equal(t, domain.OutcomeCanceled, settledAt.Outcome)
		require.Equal(t, domain.LifecycleCanceled, settledOp.Lifecycle)
		require.Equal(t, domain.ControlCancelApplied, settledOp.Control)
		require.Equal(t, domain.CleanupComplete, settledOp.Cleanup)
		require.Empty(t, settledOp.FailureCode, "the uncertainty code is cleared by the evidence")
		after, _, afterSeq := eventCount(t, p1.pool, op.ID)
		require.Equal(t, before+1, after, "the settlement is one more committed event")
		require.Equal(t, beforeSeq+1, afterSeq)
		var pendingCancels int
		require.NoError(t, p1.pool.QueryRow(ctx, "SELECT count(*) FROM operation_commands WHERE operation_id = $1 AND kind = 'cancel' AND outcome <> 'applied'", op.ID).Scan(&pendingCancels))
		require.Equal(t, 0, pendingCancels, "both tracked cancel commands settled as applied")
		// The settlement is itself idempotent, and a new attempt is still fenced (terminal).
		_, again2, existing, err := p1.exec.CloseAttempt(ctx, cmd("tenant_a", "cmd_unknown_close_settled", "close"), at.ID, domain.OutcomeCanceled, domain.CleanupComplete, "CANCELED")
		require.NoError(t, err)
		require.True(t, existing)
		require.Equal(t, domain.LifecycleCanceled, again2.Lifecycle)
		_, _, err = p2.exec.OpenAttempt(ctx, cmd("tenant_a", "cmd_unknown_open3", "open"), op.ID, "local-check", 2, "local-check-v1")
		require.ErrorIs(t, err, domain.ErrStaleExecution)
	})

	t.Run("a Pod registered while the closed attempt reconciles is evidence and never reopens the attempt", func(t *testing.T) {
		op, _, err := p1.ops.Create(ctx, cmd("tenant_a", "cmd_late_pod", "body"), scopeA, domain.KindLocalCheck, subject)
		require.NoError(t, err)
		at, _, err := p1.exec.OpenAttempt(ctx, cmd("tenant_a", "cmd_late_pod_open", "open"), op.ID, "local-check", 0, "local-check-v1")
		require.NoError(t, err)
		_, _, err = p1.exec.PrepareLaunch(ctx, cmd("tenant_a", "cmd_late_pod_launch", "launch"), at.ID, "validator-late-1", "kind-anvilkit-dev", imageDig, time.Now().Add(2*time.Minute))
		require.NoError(t, err)
		current, err := p2.ops.Get(ctx, scopeA, op.ID)
		require.NoError(t, err)
		rc, _, err := p2.ops.SubmitCommand(ctx, cmd("tenant_a", "cmd_late_pod_cancel", "cancel"), scopeA, op.ID, domain.CommandCancel, current.Revision, "")
		require.NoError(t, err)
		require.Equal(t, domain.OutcomePending, rc.Outcome)
		// The create request never resolved and nothing was observed in the
		// window: the attempt closes canceled with cleanup unknown.
		_, closedOp, _, err := p1.exec.CloseAttempt(ctx, cmd("tenant_a", "cmd_late_pod_close", "close"), at.ID, domain.OutcomeCanceled, domain.CleanupUnknown, "CANCELED")
		require.NoError(t, err)
		require.Equal(t, domain.LifecycleReconciling, closedOp.Lifecycle)
		before, beforeRev, beforeSeq := eventCount(t, p1.pool, op.ID)

		// A reconciliation round observes the late Job's Pod and registers it
		// under the running path's command identity, on either process.
		inst, existing, err := p2.exec.RegisterInstance(ctx, cmd("tenant_a", at.ID+":register:pod-late", "reg"), at.ID, "validator-late-1", "kind-anvilkit-dev", "job-late", "pod-late", imageDig)
		require.NoError(t, err)
		require.False(t, existing)
		require.True(t, inst.Current, "the only physical instance of the launch")
		again, existing, err := p1.exec.RegisterInstance(ctx, cmd("tenant_a", at.ID+":register:pod-late", "reg"), at.ID, "validator-late-1", "kind-anvilkit-dev", "job-late", "pod-late", imageDig)
		require.NoError(t, err)
		require.True(t, existing, "a lost registration receipt reenters the same instance")
		require.Equal(t, inst.ID, again.ID)
		_, err = p1.exec.ObserveInstance(ctx, at.ID, inst.ID, domain.PhaseSucceeded, nil, time.Now())
		require.NoError(t, err)
		var state string
		require.NoError(t, p1.pool.QueryRow(ctx, "SELECT state FROM attempts WHERE attempt_id = $1", at.ID).Scan(&state))
		require.Equal(t, "closed", state, "the closed attempt is not reopened by late evidence")
		after, afterRev, afterSeq := eventCount(t, p1.pool, op.ID)
		require.Equal(t, [3]int64{int64(before), beforeRev, beforeSeq}, [3]int64{int64(after), afterRev, afterSeq}, "no projection change, no event")
		open, err := p2.ops.Get(ctx, scopeA, op.ID)
		require.NoError(t, err)
		require.Equal(t, domain.LifecycleReconciling, open.Lifecycle)
		require.Equal(t, domain.ControlCancelPending, open.Control, "the cancel stays pending until the cleanup is confirmed")
		var instances int
		require.NoError(t, p1.pool.QueryRow(ctx, "SELECT count(*) FROM physical_instances WHERE attempt_id = $1", at.ID).Scan(&instances))
		require.Equal(t, 1, instances)

		// The settlement with the cleanup evidence applies the pending cancel
		// under the same attempt; the transition ids stay unique.
		_, settledOp, existing, err := p1.exec.CloseAttempt(ctx, cmd("tenant_a", "cmd_late_pod_close_settled", "close"), at.ID, domain.OutcomeCanceled, domain.CleanupComplete, "CANCELED")
		require.NoError(t, err)
		require.False(t, existing)
		require.Equal(t, domain.LifecycleCanceled, settledOp.Lifecycle)
		require.Equal(t, domain.ControlCancelApplied, settledOp.Control)
		var outcome string
		require.NoError(t, p1.pool.QueryRow(ctx, "SELECT outcome FROM operation_commands WHERE command_id = 'cmd_late_pod_cancel'").Scan(&outcome))
		require.Equal(t, "applied", outcome)
	})

	t.Run("a completed attempt with unknown cleanup succeeds once the cleanup evidence arrives", func(t *testing.T) {
		op, _, err := p1.ops.Create(ctx, cmd("tenant_a", "cmd_settle", "body"), scopeA, domain.KindLocalCheck, subject)
		require.NoError(t, err)
		at, _, err := p1.exec.OpenAttempt(ctx, cmd("tenant_a", "cmd_settle_open", "open"), op.ID, "local-check", 0, "local-check-v1")
		require.NoError(t, err)
		l, _, err := p1.exec.PrepareLaunch(ctx, cmd("tenant_a", "cmd_settle_launch", "launch"), at.ID, "validator-settle-1", "kind-anvilkit-dev", imageDig, time.Now().Add(2*time.Minute))
		require.NoError(t, err)
		inst, _, err := p1.exec.RegisterInstance(ctx, cmd("tenant_a", "cmd_settle_reg", "reg"), at.ID, "validator-settle-1", "kind-anvilkit-dev", "job-s", "pod-s", imageDig)
		require.NoError(t, err)
		manifest, _ := json.Marshal(map[string]any{
			"schemaVersion": 1, "launchId": l.ID, "attemptId": at.ID, "jobKind": "validator", "profileId": "local-check-v1",
			"verdict": "certified", "outputs": []map[string]any{{"class": "result", "digest": fixedResult, "sizeBytes": "24"}},
			"completedAt": "2026-09-14T12:00:00Z",
		})
		_, _, err = p1.exec.AcceptResult(ctx, cmd("tenant_a", "cmd_settle_acc", "accept"), at.ID, inst.ID, "local-check-v1", domain.VerdictCertified, "", application.DigestOf(manifest), manifest, "observer-test", nil)
		require.NoError(t, err)
		_, closedOp, _, err := p1.exec.CloseAttempt(ctx, cmd("tenant_a", "cmd_settle_close", "close"), at.ID, domain.OutcomeCompleted, domain.CleanupUnknown, "")
		require.NoError(t, err)
		require.Equal(t, domain.LifecycleReconciling, closedOp.Lifecycle)
		require.Equal(t, "EFFECT_UNCERTAIN", closedOp.FailureCode)
		_, settledOp, existing, err := p2.exec.CloseAttempt(ctx, cmd("tenant_a", "cmd_settle_close_settled", "close"), at.ID, domain.OutcomeCompleted, domain.CleanupComplete, "")
		require.NoError(t, err)
		require.False(t, existing)
		require.Equal(t, domain.LifecycleSucceeded, settledOp.Lifecycle)
		require.Equal(t, domain.ControlNone, settledOp.Control)
		require.Empty(t, settledOp.FailureCode)
		require.Equal(t, domain.RelaySettled, settledOp.Relay)
		var stages int
		require.NoError(t, p1.pool.QueryRow(ctx, "SELECT count(*) FROM stage_manifests WHERE operation_id = $1", op.ID).Scan(&stages))
		require.Equal(t, 1, stages, "settlement never adds a result")
	})

	t.Run("accepted results keep their original bytes and are verified on read", func(t *testing.T) {
		op, _, err := p1.ops.Create(ctx, cmd("tenant_a", "cmd_bytes", "body"), scopeA, domain.KindLocalCheck, subject)
		require.NoError(t, err)
		at, _, err := p1.exec.OpenAttempt(ctx, cmd("tenant_a", "cmd_bytes_open", "open"), op.ID, "local-check", 0, "local-check-v1")
		require.NoError(t, err)
		l, _, err := p1.exec.PrepareLaunch(ctx, cmd("tenant_a", "cmd_bytes_launch", "launch"), at.ID, "validator-bytes-1", "kind-anvilkit-dev", imageDig, time.Now().Add(2*time.Minute))
		require.NoError(t, err)
		inst, _, err := p1.exec.RegisterInstance(ctx, cmd("tenant_a", "cmd_bytes_reg", "reg"), at.ID, "validator-bytes-1", "kind-anvilkit-dev", "job-b", "pod-b", imageDig)
		require.NoError(t, err)
		// Non-canonical bytes as a real observer may submit them: member order
		// and whitespace that jsonb normalizes away.
		manifest := []byte("{\"verdict\": \"certified\",\n  \"schemaVersion\": 1, \"launchId\": \"" + l.ID + "\", \"attemptId\": \"" + at.ID + "\", \"jobKind\": \"validator\", \"profileId\": \"local-check-v1\",\n  \"outputs\": [ {\"class\": \"result\", \"digest\": \"" + fixedResult + "\", \"sizeBytes\": \"24\"} ],\n  \"completedAt\": \"2026-09-14T12:00:00Z\"}")
		digest := application.DigestOf(manifest)
		st, _, err := p2.exec.AcceptResult(ctx, cmd("tenant_a", "cmd_bytes_acc", "accept"), at.ID, inst.ID, "local-check-v1", domain.VerdictCertified, "", digest, manifest, "observer-test", nil)
		require.NoError(t, err)

		got, err := p1.exec.GetAcceptedStage(ctx, at.ID, "")
		require.NoError(t, err)
		require.Equal(t, manifest, got.ResultManifest, "the retrieved bytes are the submitted bytes")
		require.Equal(t, digest, application.DigestOf(got.ResultManifest), "and they verify against result_digest")
		var normalized string
		require.NoError(t, p1.pool.QueryRow(ctx, "SELECT result_manifest::text FROM stage_manifests WHERE stage_id = $1", st.ID).Scan(&normalized))
		require.NotEqual(t, string(manifest), normalized, "the jsonb copy is normalized and cannot stand in for the bytes")
		require.NotEqual(t, digest, application.DigestOf([]byte(normalized)))

		// Storage that no longer holds the accepted bytes is an integrity
		// failure, never a served result.
		_, err = p1.pool.Exec(ctx, "UPDATE stage_manifests SET result_manifest_bytes = $2 WHERE stage_id = $1", st.ID, []byte(normalized))
		require.NoError(t, err)
		_, err = p2.exec.GetAcceptedStage(ctx, at.ID, "")
		require.ErrorIs(t, err, domain.ErrIntegrity)
		_, err = p1.pool.Exec(ctx, "UPDATE stage_manifests SET result_manifest_bytes = $2 WHERE stage_id = $1", st.ID, manifest)
		require.NoError(t, err)

		// A stage accepted before the original bytes were retained (migration
		// 00002) is served without a manifest and is never byte-verified from
		// the normalized copy.
		_, err = p1.pool.Exec(ctx, "UPDATE stage_manifests SET result_manifest_bytes = NULL WHERE stage_id = $1", st.ID)
		require.NoError(t, err)
		legacy, err := p1.exec.GetAcceptedStage(ctx, at.ID, "")
		require.NoError(t, err)
		require.Nil(t, legacy.ResultManifest)
		require.Equal(t, digest, legacy.ResultDigest)
	})

	t.Run("relay starts each confirmed operation exactly once and cancel is relayed", func(t *testing.T) {
		wf := &fakeWorkflows{starts: map[string]int{}, cancels: map[string]int{}}
		relay := application.NewRelay(postgres.NewStore(p1.pool), wf, p1.ops, nil, domain.SystemClock{}, testLog, time.Second)
		op, _, err := p1.ops.Create(ctx, cmd("tenant_a", "cmd_relay", "body"), scopeA, domain.KindLocalCheck, subject)
		require.NoError(t, err)
		require.NoError(t, relay.Tick(ctx))
		require.NoError(t, relay.Tick(ctx))
		require.Equal(t, 1, wf.starts[op.ID], "a second tick does not start a second workflow")
		got, err := p1.ops.Get(ctx, scopeA, op.ID)
		require.NoError(t, err)
		require.Equal(t, domain.RelayStarted, got.Relay)
		require.Equal(t, "scheduled", got.Phase)

		// Cancel while no attempt is open: fence + canceled in one commit, then relayed.
		rc, existing, err := p1.ops.SubmitCommand(ctx, cmd("tenant_a", "cmd_relay_cancel", "cancel"), scopeA, op.ID, domain.CommandCancel, got.Revision, "")
		require.NoError(t, err)
		require.False(t, existing)
		require.Equal(t, domain.OutcomeApplied, rc.Outcome)
		require.NoError(t, relay.Tick(ctx))
		require.Equal(t, 1, wf.cancels[op.ID])
		got, err = p1.ops.Get(ctx, scopeA, op.ID)
		require.NoError(t, err)
		require.Equal(t, domain.LifecycleCanceled, got.Lifecycle)
		require.Equal(t, domain.RelaySettled, got.Relay)
		_, _, err = p2.exec.OpenAttempt(ctx, cmd("tenant_a", "cmd_relay_att", "open"), op.ID, "local-check", 0, "local-check-v1")
		require.ErrorIs(t, err, domain.ErrStaleExecution, "a canceled operation is fenced for new attempts")
		rc2, existing, err := p1.ops.SubmitCommand(ctx, cmd("tenant_a", "cmd_relay_cancel", "cancel"), scopeA, op.ID, domain.CommandCancel, got.Revision, "")
		require.NoError(t, err)
		require.True(t, existing, "same command returns the original receipt")
		require.Equal(t, rc.OperationRevision, rc2.OperationRevision)

		// The started run can open its attempt before the relay's mark lands
		// (the mark follows the start): the mark records the relay state and
		// bumps the revision, but never moves the phase back to scheduled.
		raced, _, err := p1.ops.Create(ctx, cmd("tenant_a", "cmd_relay_raced", "body"), scopeA, domain.KindLocalCheck, subject)
		require.NoError(t, err)
		_, _, err = p2.exec.OpenAttempt(ctx, cmd("tenant_a", "cmd_relay_raced_att", "open"), raced.ID, "local-check", 0, "local-check-v1")
		require.NoError(t, err)
		require.NoError(t, relay.Tick(ctx))
		got, err = p1.ops.Get(ctx, scopeA, raced.ID)
		require.NoError(t, err)
		require.Equal(t, domain.RelayStarted, got.Relay)
		require.Equal(t, "attempt_open", got.Phase, "relay:started does not regress a later phase")
		require.Equal(t, domain.Revision(4), got.Revision, "accepted, intake_confirmed, attempt opened, relay started")
	})

	t.Run("execution chain accepts exactly one result and settles the operation", func(t *testing.T) {
		op, _, err := p1.ops.Create(ctx, cmd("tenant_a", "cmd_exec", "body"), scopeA, domain.KindLocalCheck, subject)
		require.NoError(t, err)
		at, existing, err := p1.exec.OpenAttempt(ctx, cmd("tenant_a", "cmd_exec_open", "open"), op.ID, "local-check", 0, "local-check-v1")
		require.NoError(t, err)
		require.False(t, existing)
		at2, existing, err := p2.exec.OpenAttempt(ctx, cmd("tenant_a", "cmd_exec_open", "open"), op.ID, "local-check", 0, "local-check-v1")
		require.NoError(t, err)
		require.True(t, existing)
		require.Equal(t, at.ID, at2.ID)

		deadline := time.Now().Add(2 * time.Minute)
		l, _, err := p1.exec.PrepareLaunch(ctx, cmd("tenant_a", "cmd_exec_launch", "launch"), at.ID, "validator-exec-1", "kind-anvilkit-dev", imageDig, deadline)
		require.NoError(t, err)
		require.Equal(t, domain.InventoryConfirmed, l.Inventory)
		l2, existing, err := p2.exec.PrepareLaunch(ctx, cmd("tenant_a", "cmd_exec_launch", "launch"), at.ID, "validator-exec-1", "kind-anvilkit-dev", imageDig, deadline)
		require.NoError(t, err)
		require.True(t, existing)
		require.Equal(t, l.ID, l2.ID)
		_, _, err = p2.exec.RegisterInstance(ctx, cmd("tenant_a", "cmd_exec_reg_bad", "reg"), at.ID, "validator-exec-1", "kind-anvilkit-dev", "job-1", "pod-x", domain.Digest("sha256:1111111111111111111111111111111111111111111111111111111111111111"))
		require.ErrorIs(t, err, domain.ErrStaleExecution, "an image other than the launched one is not registered")

		inst1, _, err := p1.exec.RegisterInstance(ctx, cmd("tenant_a", "cmd_exec_reg1", "reg"), at.ID, "validator-exec-1", "kind-anvilkit-dev", "job-1", "pod-1", imageDig)
		require.NoError(t, err)
		require.True(t, inst1.Current)
		inst2, _, err := p2.exec.RegisterInstance(ctx, cmd("tenant_a", "cmd_exec_reg2", "reg"), at.ID, "validator-exec-1", "kind-anvilkit-dev", "job-1", "pod-2", imageDig)
		require.NoError(t, err)
		require.False(t, inst2.Current, "a duplicate Pod is recorded but never current")

		manifest, _ := json.Marshal(map[string]any{
			"schemaVersion": 1, "launchId": l.ID, "attemptId": at.ID, "jobKind": "validator", "profileId": "local-check-v1",
			"verdict": "certified", "outputs": []map[string]any{{"class": "result", "digest": fixedResult, "sizeBytes": "24"}},
			"completedAt": "2026-09-14T12:00:00Z",
		})
		digest := application.DigestOf(manifest)
		_, _, err = p2.exec.AcceptResult(ctx, cmd("tenant_a", "cmd_exec_acc_dup", "accept"), at.ID, inst2.ID, "local-check-v1", domain.VerdictCertified, "", digest, manifest, "observer-test", nil)
		require.ErrorIs(t, err, domain.ErrStaleExecution, "the non-current Pod cannot own the result")
		_, _, err = p1.exec.AcceptResult(ctx, cmd("tenant_a", "cmd_exec_acc_bad", "accept"), at.ID, inst1.ID, "local-check-v1", domain.VerdictCertified, "", domain.Digest("sha256:2222222222222222222222222222222222222222222222222222222222222222"), manifest, "observer-test", nil)
		require.ErrorIs(t, err, domain.ErrInvalid, "manifest bytes must hash to the declared digest")
		forged := append([]byte{}, manifest...)
		forged = forged[:len(forged)-1]
		forged = append(forged, []byte(`,"exitCode":0}`)...)
		_, _, err = p1.exec.AcceptResult(ctx, cmd("tenant_a", "cmd_exec_acc_forged", "accept"), at.ID, inst1.ID, "local-check-v1", domain.VerdictCertified, "", application.DigestOf(forged), forged, "observer-test", nil)
		require.ErrorIs(t, err, domain.ErrInvalid, "a manifest outside the jobs contract is rejected")

		// Provenance and embedded outputs: the manifest must name the launch
		// this instance came from, the reviewed profile's job kind and the
		// request's failure code, and may embed nothing but the fixture's
		// reviewed fixed result; every other output needs a finalized object.
		claim := func(fields map[string]any) []byte {
			m := map[string]any{
				"schemaVersion": 1, "launchId": l.ID, "attemptId": at.ID, "jobKind": "validator", "profileId": "local-check-v1",
				"verdict": "certified", "outputs": []map[string]any{{"class": "result", "digest": fixedResult, "sizeBytes": "24"}},
				"completedAt": "2026-09-14T12:00:00Z",
			}
			for k, v := range fields {
				m[k] = v
			}
			b, err := json.Marshal(m)
			require.NoError(t, err)
			return b
		}
		accept := func(id string, m []byte, verdict domain.Verdict, failureCode, profileID string) error {
			_, _, err := p2.exec.AcceptResult(ctx, cmd("tenant_a", "cmd_exec_acc_"+id, "accept"), at.ID, inst1.ID, profileID, verdict, failureCode, application.DigestOf(m), m, "observer-test", nil)
			return err
		}
		require.ErrorIs(t, accept("launch", claim(map[string]any{"launchId": "lch_other"}), domain.VerdictCertified, "", "local-check-v1"), domain.ErrStaleExecution, "a manifest of another launch is not this instance's result")
		require.ErrorIs(t, accept("kind", claim(map[string]any{"jobKind": "codegen"}), domain.VerdictCertified, "", "local-check-v1"), domain.ErrInvalid, "a job kind other than the reviewed profile's is refused")
		require.ErrorIs(t, accept("embedded", claim(map[string]any{"outputs": []map[string]any{{"class": "result", "digest": string(subject.SubjectDigest), "sizeBytes": "24"}}}), domain.VerdictCertified, "", "local-check-v1"), domain.ErrInvalid, "an embedded result other than the reviewed fixed result is refused")
		require.ErrorIs(t, accept("source", claim(map[string]any{"outputs": []map[string]any{{"class": "result", "digest": fixedResult, "sizeBytes": "24"}, {"class": "source", "digest": string(subject.SubjectDigest), "sizeBytes": "10"}}}), domain.VerdictCertified, "", "local-check-v1"), domain.ErrInvalid, "a source output without a finalized object is refused, fixture profile or not")
		require.ErrorIs(t, accept("emptyhandle", claim(map[string]any{"outputs": []map[string]any{{"class": "evidence", "digest": string(subject.SubjectDigest), "sizeBytes": "10", "handle": ""}}}), domain.VerdictCertified, "", "local-check-v1"), domain.ErrInvalid, "an emptied handle does not bypass the binding")
		failed := claim(map[string]any{"verdict": "invalid", "failureCode": "CANDIDATE_TEST_FAILED", "outputs": []map[string]any{}})
		require.ErrorIs(t, accept("failure", failed, domain.VerdictInvalid, "OBSERVER_FAILED", "local-check-v1"), domain.ErrInvalid, "a failure code other than the manifest's conflicts")
		require.ErrorIs(t, accept("nocode", failed, domain.VerdictInvalid, "", "local-check-v1"), domain.ErrInvalid, "a manifest failure code the request does not carry conflicts")
		require.ErrorIs(t, accept("unreviewed", claim(map[string]any{"profileId": "codegen-v1"}), domain.VerdictCertified, "", "codegen-v1"), domain.ErrStaleExecution, "a profile no reviewed job profile carries accepts nothing")
		// The prescribed fixed result: a certified LocalCheck result names
		// the reviewed digest and size exactly once; nothing else stands in
		// for it, and a contract-valid empty failure manifest still passes.
		reviewed, err := manifests.JobProfile("local-check-v1")
		require.NoError(t, err)
		require.Equal(t, &domain.FixedResult{Digest: domain.Digest(fixedResult), SizeBytes: 24}, reviewed.ExpectedResult, "the reviewed profile carries the digest and the byte size of the fixed result")
		require.ErrorIs(t, accept("empty", claim(map[string]any{"outputs": []map[string]any{}}), domain.VerdictCertified, "", "local-check-v1"), domain.ErrInvalid, "a certified result without its fixed result is refused")
		require.ErrorIs(t, accept("size", claim(map[string]any{"outputs": []map[string]any{{"class": "result", "digest": fixedResult, "sizeBytes": "25"}}}), domain.VerdictCertified, "", "local-check-v1"), domain.ErrInvalid, "the reviewed digest with another size is not the fixed result")
		require.ErrorIs(t, accept("twice", claim(map[string]any{"outputs": []map[string]any{{"class": "result", "digest": fixedResult, "sizeBytes": "24"}, {"class": "result", "digest": fixedResult, "sizeBytes": "24"}}}), domain.VerdictCertified, "", "local-check-v1"), domain.ErrInvalid, "the fixed result named twice is refused")
		var refused int
		require.NoError(t, p1.pool.QueryRow(ctx, "SELECT count(*) FROM stage_manifests WHERE attempt_id = $1", at.ID).Scan(&refused))
		require.Zero(t, refused, "no refusal left a stage behind")
		require.NoError(t, p1.pool.QueryRow(ctx, "SELECT count(*) FROM stage_artifacts sa JOIN stage_manifests sm ON sm.stage_id = sa.stage_id WHERE sm.attempt_id = $1", at.ID).Scan(&refused))
		require.Zero(t, refused)
		var attemptState, opPhase string
		require.NoError(t, p1.pool.QueryRow(ctx, "SELECT a.state, o.phase FROM attempts a JOIN operations o ON o.operation_id = a.operation_id WHERE a.attempt_id = $1", at.ID).Scan(&attemptState, &opPhase))
		require.Equal(t, string(domain.AttemptRunning), attemptState, "no refusal moved the attempt")
		require.Equal(t, "running", opPhase, "no refusal moved the operation")

		st, existing, err := p1.exec.AcceptResult(ctx, cmd("tenant_a", "cmd_exec_acc", "accept"), at.ID, inst1.ID, "local-check-v1", domain.VerdictCertified, "", digest, manifest, "observer-test", nil)
		require.NoError(t, err)
		require.False(t, existing)
		st2, existing, err := p2.exec.AcceptResult(ctx, cmd("tenant_a", "cmd_exec_acc", "accept"), at.ID, inst1.ID, "local-check-v1", domain.VerdictCertified, "", digest, manifest, "observer-test", nil)
		require.NoError(t, err)
		require.True(t, existing)
		require.Equal(t, st.ID, st2.ID, "repeated acceptance is idempotent")
		other, _ := json.Marshal(map[string]any{
			"schemaVersion": 1, "launchId": l.ID, "attemptId": at.ID, "jobKind": "validator", "profileId": "local-check-v1",
			"verdict": "certified", "outputs": []map[string]any{}, "completedAt": "2026-09-14T12:00:01Z",
		})
		_, _, err = p2.exec.AcceptResult(ctx, cmd("tenant_a", "cmd_exec_acc2", "accept"), at.ID, inst1.ID, "local-check-v1", domain.VerdictCertified, "", application.DigestOf(other), other, "observer-test", nil)
		require.ErrorIs(t, err, domain.ErrIdempotencyConflict, "a second different result for the attempt is refused")

		var stages int
		require.NoError(t, p1.pool.QueryRow(ctx, "SELECT count(*) FROM stage_manifests WHERE attempt_id = $1", at.ID).Scan(&stages))
		require.Equal(t, 1, stages, "exactly one accepted result is visible")

		closedAt, closedOp, existing, err := p1.exec.CloseAttempt(ctx, cmd("tenant_a", "cmd_exec_close", "close"), at.ID, domain.OutcomeCompleted, domain.CleanupComplete, "")
		require.NoError(t, err)
		require.False(t, existing)
		require.Equal(t, domain.AttemptClosed, closedAt.State)
		require.Equal(t, domain.LifecycleSucceeded, closedOp.Lifecycle)
		require.Equal(t, domain.CleanupComplete, closedOp.Cleanup)
		_, _, existing, err = p2.exec.CloseAttempt(ctx, cmd("tenant_a", "cmd_exec_close", "close"), at.ID, domain.OutcomeCompleted, domain.CleanupComplete, "")
		require.NoError(t, err)
		require.True(t, existing)

		count, lastRev, lastSeq := eventCount(t, p1.pool, op.ID)
		final, err := p1.ops.Get(ctx, scopeA, op.ID)
		require.NoError(t, err)
		require.Equal(t, int64(final.Revision), lastRev)
		require.Equal(t, int64(final.CoveredEventSeq()), lastSeq)
		require.Equal(t, int(lastSeq), count, "every projection change committed exactly one event")
		page, err := p1.ops.ListEvents(ctx, scopeA, op.ID, 0, 100)
		require.NoError(t, err)
		phases := make([]string, 0, len(page.Events))
		for _, ev := range page.Events {
			phases = append(phases, ev.Payload.Phase)
		}
		require.Equal(t, []string{"intake", "accepted", "attempt_open", "launch_prepared", "running", "result_accepted", "closed"}, phases)
		reset, err := p1.ops.ListEvents(ctx, scopeA, op.ID, 999, 100)
		require.NoError(t, err)
		require.True(t, reset.ResetRequired, "a cursor beyond the covered sequence requires a reset")
	})

	t.Run("cancel persists under concurrent execution load and settles on close", func(t *testing.T) {
		op, _, err := p1.ops.Create(ctx, cmd("tenant_a", "cmd_load", "body"), scopeA, domain.KindLocalCheck, subject)
		require.NoError(t, err)
		at, _, err := p1.exec.OpenAttempt(ctx, cmd("tenant_a", "cmd_load_open", "open"), op.ID, "local-check", 0, "local-check-v1")
		require.NoError(t, err)
		_, _, err = p1.exec.PrepareLaunch(ctx, cmd("tenant_a", "cmd_load_launch", "launch"), at.ID, "validator-load-1", "kind-anvilkit-dev", imageDig, time.Now().Add(2*time.Minute))
		require.NoError(t, err)
		inst1, _, err := p1.exec.RegisterInstance(ctx, cmd("tenant_a", "cmd_load_reg", "reg"), at.ID, "validator-load-1", "kind-anvilkit-dev", "job-l", "pod-l", imageDig)
		require.NoError(t, err)

		var wg sync.WaitGroup
		stop := make(chan struct{})
		for i := 0; i < 8; i++ {
			wg.Add(1)
			go func(p *process) {
				defer wg.Done()
				for {
					select {
					case <-stop:
						return
					default:
						_, _ = p.exec.ObserveInstance(ctx, at.ID, inst1.ID, domain.PhaseRunning, nil, time.Now())
						_, _ = p.ops.Get(ctx, scopeA, op.ID)
					}
				}
			}([]*process{p1, p2}[i%2])
		}
		current, err := p2.ops.Get(ctx, scopeA, op.ID)
		require.NoError(t, err)
		// Two racing cancels with the same expected revision: exactly one is applied.
		var applied, conflicts atomic.Int32
		var cw sync.WaitGroup
		for i := 0; i < 2; i++ {
			cw.Add(1)
			go func(i int, p *process) {
				defer cw.Done()
				_, _, err := p.ops.SubmitCommand(ctx, cmd("tenant_a", fmt.Sprintf("cmd_load_cancel_%d", i), "cancel"), scopeA, op.ID, domain.CommandCancel, current.Revision, "")
				switch {
				case err == nil:
					applied.Add(1)
				case errors.Is(err, domain.ErrRevisionConflict):
					conflicts.Add(1)
				default:
					t.Error(err)
				}
			}(i, []*process{p1, p2}[i])
		}
		cw.Wait()
		close(stop)
		wg.Wait()
		require.Equal(t, int32(1), applied.Load())
		require.Equal(t, int32(1), conflicts.Load())

		got, err := p2.ops.Get(ctx, scopeA, op.ID)
		require.NoError(t, err)
		require.Equal(t, domain.ControlCancelPending, got.Control, "an open attempt keeps the cancel pending until senders are quiescent")
		require.Equal(t, domain.LifecycleRunning, got.Lifecycle)
		_, _, err = p1.exec.PrepareLaunch(ctx, cmd("tenant_a", "cmd_load_launch2", "launch"), at.ID, "validator-load-2", "kind-anvilkit-dev", imageDig, time.Now().Add(2*time.Minute))
		require.ErrorIs(t, err, domain.ErrStaleExecution, "the fence denies new launches")

		_, closedOp, _, err := p2.exec.CloseAttempt(ctx, cmd("tenant_a", "cmd_load_close", "close"), at.ID, domain.OutcomeCanceled, domain.CleanupComplete, "CANCELED")
		require.NoError(t, err)
		require.Equal(t, domain.LifecycleCanceled, closedOp.Lifecycle)
		require.Equal(t, domain.ControlCancelApplied, closedOp.Control)
		var outcome string
		require.NoError(t, p1.pool.QueryRow(ctx, "SELECT outcome FROM operation_commands WHERE operation_id = $1 AND kind = 'cancel'", op.ID).Scan(&outcome))
		require.Equal(t, "applied", outcome, "the pending cancel command settles as applied")
	})
}
