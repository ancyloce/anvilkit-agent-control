package application_test

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/ancyloce/anvilkit-agent-control/internal/adapters/inventory"
	"github.com/ancyloce/anvilkit-agent-control/internal/adapters/postgres"
	"github.com/ancyloce/anvilkit-agent-control/internal/application"
	"github.com/ancyloce/anvilkit-agent-control/internal/domain"
	"github.com/ancyloce/anvilkit-agent-control/internal/testdb"
)

var releaseProfile = domain.Profile{ID: "release-v1", Kind: domain.KindRelease, OperationDeadline: 720 * time.Hour, StepID: "certify", MultiStep: true}

// Releases (P21) against real PostgreSQL: the intake binds the exact saved
// revision of a source operation, the projection binds the operation's
// accepted certification, and publication/activation permits follow only
// the recorded approval and receipts.
func TestReleases(t *testing.T) {
	inst := testdb.Start(t)
	pool := inst.Pool(t)
	store := postgres.NewStore(pool)
	inv, err := inventory.NewFilesystem(filepath.Join(t.TempDir(), "inventory"))
	require.NoError(t, err)
	ops := application.NewOperations(store, inv, []domain.Profile{profile, previewProfile, releaseProfile}, domain.SystemClock{}, testLog)
	previews := application.NewPreviews(store, domain.SystemClock{})
	releases := application.NewReleases(store, domain.SystemClock{})
	effects := application.NewEffects(store, inv, nil, domain.SystemClock{}, testLog)
	ctx := context.Background()
	exec := func(sql string, args ...any) {
		t.Helper()
		_, err := pool.Exec(ctx, sql, args...)
		require.NoError(t, err, sql)
	}
	source := application.DigestOf([]byte("edited source tar"))
	lineage := application.DigestOf([]byte("component source lineage"))
	transfer := func(id, tenant, class string, digest domain.Digest, size int) {
		exec(`INSERT INTO artifact_transfers (transfer_id, tenant_id, class, media_type, expected_digest, expected_size, handle, state,
			object_version, command_id, request_digest, deadline, actual_digest, actual_size) VALUES ($1, $2, $3, 'application/octet-stream', $4, $5, $6, 'finalized', 'v1', $1, $4, now() + interval '1 hour', $4, $5)`,
			"xfer_"+id, tenant, class, string(digest), size, "hdl_"+id)
	}
	transfer("src", "tenant_a", "source", source, 10)

	// A preview that saved revision 4, and one that only has its base.
	saved, _, err := ops.Create(ctx, cmd("tenant_a", "prev_saved", "saved"), scopeA, domain.KindPreviewBuild, domain.Subject{ProfileID: "preview-build-v1", SubjectDigest: lineage, SourceRevision: "3", SourceHandle: "hdl_src"}, nil)
	require.NoError(t, err)
	rec := domain.PreviewRecord{State: domain.PreviewSaving, SourceDigest: source, BuildProfileID: "b", HostProfileID: "h"}
	_, _, err = previews.Record(ctx, saved.ID, rec)
	require.NoError(t, err)
	rec.ExpectedRevision, rec.State, rec.SourceRevision = 1, domain.PreviewBuilding, "4"
	_, _, err = previews.Record(ctx, saved.ID, rec)
	require.NoError(t, err)
	baseOnly, _, err := ops.Create(ctx, cmd("tenant_a", "prev_base", "base"), scopeA, domain.KindPreviewBuild, domain.Subject{ProfileID: "preview-build-v1", SubjectDigest: lineage, SourceRevision: "3", SourceHandle: "hdl_src"}, nil)
	require.NoError(t, err)

	subj := func(sourceOp, revision string) domain.Subject {
		return domain.Subject{ProfileID: "release-v1", SubjectDigest: lineage, SourceRevision: revision, SourceOperationID: sourceOp, PackageVersion: "1.0.0"}
	}
	t.Run("a release binds the exact saved revision of its source operation", func(t *testing.T) {
		withHandle := subj(saved.ID, "4")
		withHandle.SourceHandle = "hdl_src"
		otherLineage := subj(saved.ID, "4")
		otherLineage.SubjectDigest = source
		noVersion := subj(saved.ID, "4")
		noVersion.PackageVersion = ""
		for name, s := range map[string]domain.Subject{
			"unknown source operation": subj("op_missing", "4"),
			"only a base revision":     subj(baseOnly.ID, "3"),
			"another revision":         subj(saved.ID, "3"),
			"another lineage":          otherLineage,
			"caller-named artifact":    withHandle,
			"no version":               noVersion,
		} {
			_, _, err := ops.Create(ctx, cmd("tenant_a", "rel_bad_"+name, name), scopeA, domain.KindRelease, s, nil)
			require.ErrorIs(t, err, domain.ErrInvalid, name)
		}
		_, _, err := ops.Create(ctx, cmd("tenant_b", "rel_other_tenant", "t"), domain.Scope{TenantID: "tenant_b", ActorID: "user_tenant_b"}, domain.KindRelease, subj(saved.ID, "4"), nil)
		require.ErrorIs(t, err, domain.ErrInvalid, "another tenant's source operation")
		op, _, err := ops.Create(ctx, cmd("tenant_a", "rel_ok", "ok"), scopeA, domain.KindRelease, subj(saved.ID, "4"), nil)
		require.NoError(t, err)
		require.Equal(t, "hdl_src", op.Subject.SourceHandle, "Control binds the saved source artifact")
		require.Equal(t, saved.ID, op.Subject.SourceOperationID)
	})

	op, _, err := ops.Create(ctx, cmd("tenant_a", "rel_flow", "flow"), scopeA, domain.KindRelease, subj(saved.ID, "4"), nil)
	require.NoError(t, err)

	// The accepted certified stage of the release's certify attempt.
	h := func(s string) domain.Digest { return application.DigestOf([]byte(s)) }
	cert := &domain.ReleaseCertification{EvidenceDigest: h("evidence"), Npm: domain.ArtifactDigestRef{Digest: h("npm"), SizeBytes: "3"},
		Browser: domain.ArtifactDigestRef{Digest: h("browser"), SizeBytes: "7"}, CSS: []domain.ArtifactDigestRef{{Digest: h("css"), SizeBytes: "3"}}}
	exec(`INSERT INTO attempts (attempt_id, operation_id, tenant_id, step_id, visit_ordinal, attempt_ordinal, profile_id, execution_epoch, command_id, request_digest, state, deadline)
		VALUES ('att_rel', $1, 'tenant_a', 'certify', 0, 1, 'validator-fixed-dev-v1', 1, 'cmd_att_rel', $2, 'result_accepted', now() + interval '1 hour')`, op.ID, string(source))
	exec(`INSERT INTO launches (launch_id, attempt_id, operation_id, launch_key, backend, profile_id, image_digest, execution_epoch, launch_epoch, deadline, command_id, request_digest, inventory_state)
		VALUES ('lch_rel', 'att_rel', $1, 'rel-key', 'kind', 'validator-fixed-dev-v1', $2, 1, 1, now() + interval '1 hour', 'cmd_lch_rel', $2, 'confirmed')`, op.ID, string(source))
	exec(`INSERT INTO physical_instances (instance_id, attempt_id, launch_id, launch_key, backend, job_uid, pod_uid, image_digest, launch_epoch, phase, is_current)
		VALUES ('inst_rel', 'att_rel', 'lch_rel', 'rel-key', 'kind', 'job', 'pod-rel', $1, 1, 'succeeded', true)`, string(source))
	exec(`INSERT INTO stage_manifests (stage_id, attempt_id, instance_id, operation_id, phase_ordinal, profile_id, verdict, result_digest, result_manifest, observer_identity, command_id, request_digest)
		VALUES ('stg_rel', 'att_rel', 'inst_rel', $1, 1, 'validator-fixed-dev-v1', 'certified', $2, '{"verdict": "certified", "schemaVersion": 1}', 'observer', 'cmd_stg_rel', $2)`, op.ID, string(source))
	for class, a := range map[string]domain.ArtifactDigestRef{"evidence": {Digest: cert.EvidenceDigest, SizeBytes: "8"}, "npm": cert.Npm, "browser": cert.Browser, "css": cert.CSS[0]} {
		transfer(class, "tenant_a", class, a.Digest, 3)
		exec(`INSERT INTO stage_artifacts (stage_id, transfer_id, handle, class, digest, size_bytes, object_version) VALUES ('stg_rel', $1, $2, $3, $4, $5, 'v1')`,
			"xfer_"+class, "hdl_"+class, class, string(a.Digest), map[string]int{"evidence": 8, "npm": 3, "browser": 7, "css": 3}[class])
	}

	subject := &domain.ReleaseSubject{
		SchemaVersion: 1, ComponentID: "cmp_hero", PuckType: "Hero", SourceRevision: "4", SourceDigest: h("manifest"), PackageName: "@anvilkit/hero", Version: "1.0.0",
		Npm: cert.Npm, Browser: cert.Browser, CSS: cert.CSS, BuildProfileID: "build-support-dev-v1", BuildProfileDigest: h("b"),
		ValidatorProfileID: "validator-dev-v1", ValidatorProfileDigest: h("v"), HostAbi: "host-abi-dev-v1", HostAbiDigest: h("a"),
		Destinations:                domain.ReleaseDestinations{NpmRegistry: "https://registry.anvilkit.invalid/", BrowserOrigin: "https://components.anvilkit.invalid"},
		CertificationEvidenceDigest: cert.EvidenceDigest,
	}
	subject.SubjectDigest, err = domain.ComputeSubjectDigest(*subject)
	require.NoError(t, err)
	pending := domain.ReleaseTarget{State: domain.TargetPending}
	deadline := time.Now().Add(24 * time.Hour)
	_, _, err = releases.Record(ctx, op.ID, domain.ReleaseRecord{State: domain.ReleaseCertifying, Npm: pending, Browser: pending, Activation: pending})
	require.NoError(t, err)
	awaiting := domain.ReleaseRecord{ExpectedRevision: 1, State: domain.ReleaseAwaitingApproval, Subject: subject, ReleaseID: "rel_1", ReviewEffectID: "eff_rev",
		Approval: &domain.Approval{State: domain.ApprovalPending, SubjectDigest: subject.SubjectDigest}, ApprovalDeadline: &deadline, Npm: pending, Browser: pending, Activation: pending}
	_, _, err = releases.Record(ctx, op.ID, awaiting)
	require.NoError(t, err)
	again, existing, err := releases.Record(ctx, op.ID, awaiting)
	require.NoError(t, err)
	require.True(t, existing, "the same transition repeated (nanosecond deadline normalized)")
	require.Equal(t, uint64(2), again.Revision)

	epoch := uint64(1)
	effectDeadline := time.Now().Add(time.Hour)
	prepare := func(kind domain.EffectKind, target, command string) application.Permit {
		t.Helper()
		p, err := effects.Prepare(ctx, cmd("tenant_a", command, command), domain.EffectRequest{
			OperationID: op.ID, Owner: "workflow", Kind: kind, Occurrence: map[string]uint64{"npm": 1, "browser": 2, "activation": 1}[target],
			CanonicalSubject: domain.ReleaseEffectSubject(subject.SubjectDigest, target), ExecutionEpoch: epoch, Deadline: effectDeadline,
		})
		require.NoError(t, err)
		return p
	}
	t.Run("no publication permit before an approval of exactly the subject", func(t *testing.T) {
		p := prepare(domain.EffectPublication, "npm", "pub_early")
		require.False(t, p.Permitted)
		require.Equal(t, domain.DenyApprovalRequired, p.DenialCode)
	})

	publishing := awaiting
	publishing.ExpectedRevision, publishing.State = 2, domain.ReleasePublishing
	publishing.Approval = &domain.Approval{State: domain.ApprovalApproved, SubjectDigest: subject.SubjectDigest, ApproverID: "maintainer_a"}
	_, _, err = releases.Record(ctx, op.ID, publishing)
	require.NoError(t, err)

	t.Run("publication is permitted once under the approval; activation waits for both receipts", func(t *testing.T) {
		p := prepare(domain.EffectPublication, "browser", "pub_browser")
		require.True(t, p.Permitted)
		again := prepare(domain.EffectPublication, "browser", "pub_browser")
		require.False(t, again.Permitted, "a permit is issued once")
		act := prepare(domain.EffectActivation, "activation", "act_early")
		require.False(t, act.Permitted)
		require.Equal(t, domain.DenyReceiptsRequired, act.DenialCode)
	})

	t.Run("the release is read by its tenant only", func(t *testing.T) {
		got, err := releases.Get(ctx, "tenant_a", op.ID)
		require.NoError(t, err)
		require.Equal(t, domain.ReleasePublishing, got.State)
		require.Equal(t, subject.SubjectDigest, got.Subject.SubjectDigest)
		require.Equal(t, lineage, got.Lineage)
		require.Equal(t, "4", got.SourceRevision)
		_, err = releases.Get(ctx, "tenant_b", op.ID)
		require.ErrorIs(t, err, domain.ErrNotFound)
	})
}
