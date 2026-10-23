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

// Plan 0005 Phase 2: every transition Control commits for an attempt carries
// the attempt's step, visit and ordinal; an accepted result adds the
// observer's verdict; the trusted sidecar's progress report becomes one
// event with the node fields and changes nothing else; the projection's
// LatestProgress is the newest attempt-bound event.
func TestAttemptTransitionsCarryAttemptFactsAndProgressIsDisplayOnly(t *testing.T) {
	inst := testdb.Start(t)
	inv, err := inventory.NewFilesystem(filepath.Join(t.TempDir(), "inventory"))
	require.NoError(t, err)
	p := newProcess(t, inst, inv)
	ctx := context.Background()
	op, _, err := p.ops.Create(ctx, cmd("tenant_a", "cmd_progress_lc", "body"), scopeA, domain.KindLocalCheck, subject, nil)
	require.NoError(t, err)
	none, err := p.ops.LatestProgress(ctx, op)
	require.NoError(t, err)
	require.Nil(t, none, "no attempt yet, no progress")

	at, _, err := p.exec.OpenAttempt(ctx, cmd("tenant_a", op.ID+":open", "open"), op.ID, "local-check", 0, profile.ID)
	require.NoError(t, err)
	l, _, err := p.exec.PrepareLaunch(ctx, cmd("tenant_a", at.ID+":launch", "launch"), at.ID, "progress-lc", "kind-anvilkit-dev", imageDig, time.Now().Add(time.Hour))
	require.NoError(t, err)
	owner, _, err := p.exec.RegisterInstance(ctx, cmd("tenant_a", at.ID+":register:pod-p1", "reg"), at.ID, "progress-lc", "kind-anvilkit-dev", "job-p", "pod-p1", imageDig)
	require.NoError(t, err)
	other, _, err := p.exec.RegisterInstance(ctx, cmd("tenant_a", at.ID+":register:pod-p2", "reg"), at.ID, "progress-lc", "kind-anvilkit-dev", "job-p", "pod-p2", imageDig)
	require.NoError(t, err)
	require.False(t, other.Current)

	// The attempt transitions name the attempt; the intake transitions do not.
	page, err := p.ops.ListEvents(ctx, scopeA, op.ID, 0, 100)
	require.NoError(t, err)
	byTransition := map[string]domain.ChangedPayload{}
	for _, ev := range page.Events {
		byTransition[ev.TransitionID] = ev.Payload
	}
	require.False(t, byTransition["accepted"].HasAttempt(), "intake carries no attempt")
	for _, tr := range []string{"attempt:" + at.ID + ":opened", "launch:" + l.ID, "instance:" + owner.ID + ":registered"} {
		pl, ok := byTransition[tr]
		require.True(t, ok, tr)
		require.Equal(t, domain.ChangedPayload{Lifecycle: pl.Lifecycle, Phase: pl.Phase, Control: pl.Control, Cleanup: pl.Cleanup, Finance: pl.Finance, Step: "local-check", Visit: "0", AttemptOrdinal: "1"}, pl, tr)
	}

	// A progress report: one event, the node fields, nothing else moves.
	before, err := p.ops.Get(ctx, scopeA, op.ID)
	require.NoError(t, err)
	ev, existing, err := p.exec.RecordAttemptProgress(ctx, cmd("tenant_a", at.ID+":progress:code:1:started", "p"), at.ID, owner.ID, at.ExecutionEpoch, "code", "started", "1", "")
	require.NoError(t, err)
	require.False(t, existing)
	require.Equal(t, "progress:"+at.ID+":code:1:started", ev.TransitionID)
	require.Equal(t, domain.ChangedPayload{Lifecycle: domain.LifecycleRunning, Phase: "running", Control: domain.ControlNone, Cleanup: before.Cleanup, Finance: before.Finance, Step: "local-check", Visit: "0", AttemptOrdinal: "1", Node: "code", NodeState: "started", Round: "1"}, ev.Payload)
	after, err := p.ops.Get(ctx, scopeA, op.ID)
	require.NoError(t, err)
	require.Equal(t, before.Phase, after.Phase)
	require.Equal(t, before.Lifecycle, after.Lifecycle)
	require.Equal(t, before.Revision+1, after.Revision, "the event is a transition of the projection (one revision)")
	require.Equal(t, ev.EventSeq, after.CoveredEventSeq())
	latest, err := p.ops.LatestProgress(ctx, after)
	require.NoError(t, err)
	require.Equal(t, ev.TransitionID, latest.TransitionID)

	// Idempotent per transition; a different outcome for the same transition conflicts.
	again, existing, err := p.exec.RecordAttemptProgress(ctx, cmd("tenant_a", at.ID+":progress:code:1:started", "p"), at.ID, owner.ID, at.ExecutionEpoch, "code", "started", "1", "")
	require.NoError(t, err)
	require.True(t, existing)
	require.Equal(t, ev.EventSeq, again.EventSeq)
	_, _, err = p.exec.RecordAttemptProgress(ctx, cmd("tenant_a", at.ID+":progress:code:1:started:x", "p"), at.ID, owner.ID, at.ExecutionEpoch, "code", "started", "1", "OTHER")
	require.ErrorIs(t, err, domain.ErrIdempotencyConflict)

	// Refusals: vocabulary, epoch, a non-current instance.
	_, _, err = p.exec.RecordAttemptProgress(ctx, cmd("tenant_a", at.ID+":progress:bad", "p"), at.ID, owner.ID, at.ExecutionEpoch, "seal", "started", "1", "")
	require.ErrorIs(t, err, domain.ErrInvalid)
	_, _, err = p.exec.RecordAttemptProgress(ctx, cmd("tenant_a", at.ID+":progress:bad2", "p"), at.ID, owner.ID, at.ExecutionEpoch, "code", "running", "1", "")
	require.ErrorIs(t, err, domain.ErrInvalid)
	_, _, err = p.exec.RecordAttemptProgress(ctx, cmd("tenant_a", at.ID+":progress:epoch", "p"), at.ID, owner.ID, at.ExecutionEpoch+1, "code", "completed", "1", "")
	require.ErrorIs(t, err, domain.ErrStaleExecution)
	_, _, err = p.exec.RecordAttemptProgress(ctx, cmd("tenant_a", at.ID+":progress:other", "p"), at.ID, other.ID, at.ExecutionEpoch, "code", "completed", "1", "")
	require.ErrorIs(t, err, domain.ErrStaleExecution)

	// A failed node carries its controlled code; a completed one may carry a classification code.
	failed, _, err := p.exec.RecordAttemptProgress(ctx, cmd("tenant_a", at.ID+":progress:validate:1:completed", "p"), at.ID, owner.ID, at.ExecutionEpoch, "validate", "completed", "1", "CANDIDATE_BUILD_FAILED")
	require.NoError(t, err)
	require.Equal(t, "CANDIDATE_BUILD_FAILED", failed.Payload.OutcomeCode)
	require.Empty(t, failed.Payload.Verdict, "a team-internal classification is never a verdict")

	// The accepted result carries the observer's verdict and the attempt; the
	// closed attempt refuses further progress.
	manifest, _ := json.Marshal(map[string]any{
		"schemaVersion": 1, "launchId": l.ID, "attemptId": at.ID, "jobKind": "validator", "profileId": "local-check-v1",
		"verdict": "certified", "outputs": []map[string]any{{"class": "result", "digest": fixedResult, "sizeBytes": "24"}},
		"completedAt": "2026-10-23T12:00:00Z",
	})
	epoch := at.ExecutionEpoch
	st, _, err := p.exec.AcceptResult(ctx, cmd("tenant_a", at.ID+":accept", "accept"), at.ID, owner.ID, "local-check-v1", domain.VerdictCertified, "", application.DigestOf(manifest), manifest, "observer-test", &epoch)
	require.NoError(t, err)
	page, err = p.ops.ListEvents(ctx, scopeA, op.ID, 0, 100)
	require.NoError(t, err)
	var accepted *domain.ChangedPayload
	for i := range page.Events {
		if page.Events[i].TransitionID == "stage:"+st.ID {
			accepted = &page.Events[i].Payload
		}
	}
	require.NotNil(t, accepted)
	require.Equal(t, "result_accepted", accepted.Phase)
	require.Equal(t, domain.VerdictCertified, accepted.Verdict)
	require.Equal(t, "local-check", accepted.Step)
	_, _, err = p.exec.RecordAttemptProgress(ctx, cmd("tenant_a", at.ID+":progress:late", "p"), at.ID, owner.ID, at.ExecutionEpoch, "validate", "completed", "2", "")
	require.ErrorIs(t, err, domain.ErrStaleExecution, "an attempt with its result accepted takes no progress")
	_, _, _, err = p.exec.CloseAttempt(ctx, cmd("tenant_a", at.ID+":close", "close"), at.ID, domain.OutcomeCompleted, domain.CleanupComplete, "")
	require.NoError(t, err)
	_, _, err = p.exec.RecordAttemptProgress(ctx, cmd("tenant_a", at.ID+":progress:closed", "p"), at.ID, owner.ID, at.ExecutionEpoch, "validate", "completed", "2", "")
	require.ErrorIs(t, err, domain.ErrStaleExecution, "a closed attempt takes no progress")
	final, err := p.ops.Get(ctx, scopeA, op.ID)
	require.NoError(t, err)
	latest, err = p.ops.LatestProgress(ctx, final)
	require.NoError(t, err)
	require.Equal(t, "attempt:"+at.ID+":closed", latest.TransitionID, "the newest attempt-bound event is the close")
	require.Equal(t, "local-check", latest.Payload.Step)
}
