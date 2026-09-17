package application_test

import (
	"context"
	"encoding/json"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/ancyloce/anvilkit-agent-control/internal/adapters/inventory"
	"github.com/ancyloce/anvilkit-agent-control/internal/application"
	"github.com/ancyloce/anvilkit-agent-control/internal/domain"
	"github.com/ancyloce/anvilkit-agent-control/internal/testdb"
)

// GetInstance (P09) answers the registration the launcher recorded for a
// Pod, under the launch key it was registered with: an unregistered Pod is
// not found, another launch key is not found, a duplicate Pod is answered
// as registered but not current.
func TestGetInstanceAnswersOnlyTheLaunchersRegistration(t *testing.T) {
	inst := testdb.Start(t)
	inv, err := inventory.NewFilesystem(filepath.Join(t.TempDir(), "inventory"))
	require.NoError(t, err)
	p := newProcess(t, inst, inv)
	ctx := context.Background()
	op, _, err := p.ops.Create(ctx, cmd("tenant_a", "cmd_getinst", "body"), scopeA, domain.KindLocalCheck, subject, nil)
	require.NoError(t, err)
	at, _, err := p.exec.OpenAttempt(ctx, cmd("tenant_a", op.ID+":open", "open"), op.ID, "local-check", 0, profile.ID)
	require.NoError(t, err)
	_, _, err = p.exec.PrepareLaunch(ctx, cmd("tenant_a", at.ID+":launch", "launch"), at.ID, "getinst-1", "kind-anvilkit-dev", imageDig, time.Now().Add(time.Hour))
	require.NoError(t, err)

	_, err = p.exec.GetInstance(ctx, "kind-anvilkit-dev", "getinst-1", "pod-getinst")
	require.ErrorIs(t, err, domain.ErrNotFound, "a Pod the launcher has not registered is not found")

	first, _, err := p.exec.RegisterInstance(ctx, cmd("tenant_a", at.ID+":register:pod-getinst", "reg"), at.ID, "getinst-1", "kind-anvilkit-dev", "job-getinst", "pod-getinst", imageDig)
	require.NoError(t, err)
	scope, err := p.exec.GetInstance(ctx, "kind-anvilkit-dev", "getinst-1", "pod-getinst")
	require.NoError(t, err)
	require.Equal(t, first.ID, scope.Instance.ID)
	require.True(t, scope.Instance.Current)
	require.Equal(t, at.ID, scope.Attempt.ID)
	require.Equal(t, "tenant_a", scope.TenantID)
	require.Equal(t, op.RecoveryEpoch, scope.RecoveryEpoch)

	_, err = p.exec.GetInstance(ctx, "kind-anvilkit-dev", "other-key", "pod-getinst")
	require.ErrorIs(t, err, domain.ErrNotFound, "the launch key must be the registered one")
	_, err = p.exec.GetInstance(ctx, "other-backend", "getinst-1", "pod-getinst")
	require.ErrorIs(t, err, domain.ErrNotFound)

	dup, _, err := p.exec.RegisterInstance(ctx, cmd("tenant_a", at.ID+":register:pod-dup", "reg"), at.ID, "getinst-1", "kind-anvilkit-dev", "job-getinst", "pod-dup", imageDig)
	require.NoError(t, err)
	require.False(t, dup.Current)
	scope, err = p.exec.GetInstance(ctx, "kind-anvilkit-dev", "getinst-1", "pod-dup")
	require.NoError(t, err)
	require.False(t, scope.Instance.Current, "a duplicate Pod is registered without authority")
}

// The registration answers the facts that decide authority as of one read
// (P09 R3): the operation's control intent, lifecycle and current
// execution epoch beside the attempt's state and deadline. A cancel makes
// the operation cancel_pending while the attempt still runs; closing the
// attempt as canceled leaves the instance row current (the historical
// record of physical ownership is never rewritten) while the attempt is
// closed and Control's own acceptance refuses the instance as stale.
func TestGetInstanceReportsCancellationAndClosure(t *testing.T) {
	inst := testdb.Start(t)
	inv, err := inventory.NewFilesystem(filepath.Join(t.TempDir(), "inventory"))
	require.NoError(t, err)
	p := newProcess(t, inst, inv)
	ctx := context.Background()
	op, _, err := p.ops.Create(ctx, cmd("tenant_a", "cmd_getinst_lc", "body"), scopeA, domain.KindLocalCheck, subject, nil)
	require.NoError(t, err)
	at, _, err := p.exec.OpenAttempt(ctx, cmd("tenant_a", op.ID+":open", "open"), op.ID, "local-check", 0, profile.ID)
	require.NoError(t, err)
	l, _, err := p.exec.PrepareLaunch(ctx, cmd("tenant_a", at.ID+":launch", "launch"), at.ID, "getinst-lc", "kind-anvilkit-dev", imageDig, time.Now().Add(time.Hour))
	require.NoError(t, err)
	first, _, err := p.exec.RegisterInstance(ctx, cmd("tenant_a", at.ID+":register:pod-lc", "reg"), at.ID, "getinst-lc", "kind-anvilkit-dev", "job-lc", "pod-lc", imageDig)
	require.NoError(t, err)

	scope, err := p.exec.GetInstance(ctx, "kind-anvilkit-dev", "getinst-lc", "pod-lc")
	require.NoError(t, err)
	require.NotNil(t, scope.Operation)
	require.Equal(t, op.ID, scope.Operation.ID)
	require.Equal(t, domain.ControlNone, scope.Operation.Control)
	require.Equal(t, domain.LifecycleRunning, scope.Operation.Lifecycle)
	require.Equal(t, scope.Attempt.ExecutionEpoch, scope.Operation.ExecutionEpoch, "the attempt runs under the operation's current epoch")
	require.Equal(t, domain.AttemptRunning, scope.Attempt.State)
	require.True(t, scope.Instance.Current)

	current, err := p.ops.Get(ctx, scopeA, op.ID)
	require.NoError(t, err)
	rc, _, err := p.ops.SubmitCommand(ctx, cmd("tenant_a", "cmd_getinst_cancel", "cancel"), scopeA, op.ID, domain.CommandCancel, current.Revision, "")
	require.NoError(t, err)
	require.Equal(t, domain.OutcomePending, rc.Outcome, "an open attempt keeps the cancel pending")
	scope, err = p.exec.GetInstance(ctx, "kind-anvilkit-dev", "getinst-lc", "pod-lc")
	require.NoError(t, err)
	require.Equal(t, domain.ControlCancelPending, scope.Operation.Control, "the cancel fence is visible to the registration's reader")
	require.True(t, scope.Operation.FencedForNewDispatch())
	require.Equal(t, domain.AttemptRunning, scope.Attempt.State, "the attempt is still open until the launcher closes it")
	require.True(t, scope.Instance.Current)

	_, _, _, err = p.exec.CloseAttempt(ctx, cmd("tenant_a", "cmd_getinst_close", "close"), at.ID, domain.OutcomeCanceled, domain.CleanupComplete, "CANCELED")
	require.NoError(t, err)
	scope, err = p.exec.GetInstance(ctx, "kind-anvilkit-dev", "getinst-lc", "pod-lc")
	require.NoError(t, err)
	require.Equal(t, domain.AttemptClosed, scope.Attempt.State)
	require.Equal(t, domain.LifecycleCanceled, scope.Operation.Lifecycle)
	require.Equal(t, first.ID, scope.Instance.ID)
	require.True(t, scope.Instance.Current, "the historical record of physical ownership is preserved, not rewritten into a decision")

	// Control's final transaction still decides: the closed attempt accepts
	// nothing from its (historically current) instance.
	manifest, _ := json.Marshal(map[string]any{
		"schemaVersion": 1, "launchId": l.ID, "attemptId": at.ID, "jobKind": "validator", "profileId": "local-check-v1",
		"verdict": "certified", "outputs": []map[string]any{{"class": "result", "digest": fixedResult, "sizeBytes": "24"}},
		"completedAt": "2026-09-16T12:00:00Z",
	})
	_, _, err = p.exec.AcceptResult(ctx, cmd("tenant_a", "cmd_getinst_acc", "accept"), at.ID, first.ID, "local-check-v1", domain.VerdictCertified, "", application.DigestOf(manifest), manifest, "observer-test", nil)
	require.ErrorIs(t, err, domain.ErrStaleExecution)
}
