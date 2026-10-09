package application_test

import (
	"context"
	"errors"
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

var releaseProfile = domain.Profile{ID: "release-v1", Kind: domain.KindRelease, OperationDeadline: 720 * time.Hour, StepID: "certify", MultiStep: true, AttemptWindow: time.Hour}

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
	_, _, err = previews.Record(ctx, cmd("tenant_a", "rec", "rec"), saved.ID, rec)
	require.NoError(t, err)
	rec.ExpectedRevision, rec.State, rec.SourceRevision = 1, domain.PreviewBuilding, "4"
	_, _, err = previews.Record(ctx, cmd("tenant_a", "rec", "rec"), saved.ID, rec)
	require.NoError(t, err)
	baseOnly, _, err := ops.Create(ctx, cmd("tenant_a", "prev_base", "base"), scopeA, domain.KindPreviewBuild, domain.Subject{ProfileID: "preview-build-v1", SubjectDigest: lineage, SourceRevision: "3", SourceHandle: "hdl_src"}, nil)
	require.NoError(t, err)

	subj := func(sourceOp, revision string) domain.Subject {
		return domain.Subject{ProfileID: "release-v1", SubjectDigest: lineage, SourceRevision: revision, SourceOperationID: sourceOp, PackageVersion: "1.0.0"}
	}
	// The lineage's identity, as a Generation's candidate registration
	// binds it (P0.8; TestLineageIdentities covers the binding itself).
	require.NoError(t, store.Tx(ctx, func(r application.Repo) error {
		_, err := r.InsertLineageIdentity(ctx, &domain.LineageIdentity{TenantID: "tenant_a", Lineage: lineage, Identity: domain.ComponentIdentity{ComponentID: "cmp_hero", PuckType: "Hero", PackageName: "@anvilkit/hero"},
			OperationID: "op_generation", BriefID: "brf_hero", RecordedAt: time.Now()})
		return err
	}))
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

	t.Run("an attempt of a release is bounded by the attempt window, not the approval-spanning deadline", func(t *testing.T) {
		exec := application.NewExecution(store, inv, manifests, []domain.Profile{profile, previewProfile, releaseProfile}, domain.SystemClock{}, testLog)
		at, _, err := exec.OpenAttempt(ctx, cmd("tenant_a", "rel_window_open", "w"), op.ID, "publication_npm", 0, "release-v1")
		require.NoError(t, err)
		require.WithinDuration(t, time.Now().Add(time.Hour), at.Deadline, time.Minute)
		require.True(t, at.Deadline.Before(op.Deadline))
	})

	// The accepted certified stage of the release's certify attempt.
	h := func(s string) domain.Digest { return application.DigestOf([]byte(s)) }
	cert := &domain.ReleaseCertification{EvidenceDigest: h("evidence"), Npm: domain.ArtifactDigestRef{Digest: h("npm"), SizeBytes: "3"},
		Browser: domain.ArtifactDigestRef{Digest: h("browser"), SizeBytes: "7"}, CSS: []domain.ArtifactDigestRef{{Digest: h("css"), SizeBytes: "3"}}}
	exec(`INSERT INTO attempts (attempt_id, operation_id, tenant_id, step_id, visit_ordinal, attempt_ordinal, profile_id, execution_epoch, command_id, request_digest, state, deadline)
		VALUES ('att_rel', $1, 'tenant_a', 'certify', 0, 1, 'validator-source-v1', 1, 'cmd_att_rel', $2, 'result_accepted', now() + interval '1 hour')`, op.ID, string(source))
	exec(`INSERT INTO launches (launch_id, attempt_id, operation_id, launch_key, backend, profile_id, image_digest, execution_epoch, launch_epoch, deadline, command_id, request_digest, inventory_state)
		VALUES ('lch_rel', 'att_rel', $1, 'rel-key', 'kind', 'validator-source-v1', $2, 1, 1, now() + interval '1 hour', 'cmd_lch_rel', $2, 'confirmed')`, op.ID, string(source))
	exec(`INSERT INTO physical_instances (instance_id, attempt_id, launch_id, launch_key, backend, job_uid, pod_uid, image_digest, launch_epoch, phase, is_current)
		VALUES ('inst_rel', 'att_rel', 'lch_rel', 'rel-key', 'kind', 'job', 'pod-rel', $1, 1, 'succeeded', true)`, string(source))
	exec(`INSERT INTO stage_manifests (stage_id, attempt_id, instance_id, operation_id, phase_ordinal, profile_id, verdict, result_digest, result_manifest, observer_identity, command_id, request_digest)
		VALUES ('stg_rel', 'att_rel', 'inst_rel', $1, 1, 'validator-source-v1', 'certified', $2, '{"verdict": "certified", "schemaVersion": 1}', 'observer', 'cmd_stg_rel', $2)`, op.ID, string(source))
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
	_, _, err = releases.Record(ctx, cmd("tenant_a", "rec", "rec"), op.ID, domain.ReleaseRecord{State: domain.ReleaseCertifying, Npm: pending, Browser: pending, Activation: pending})
	require.NoError(t, err)
	epoch := uint64(1)
	effectDeadline := time.Now().Add(time.Hour)
	prepare := func(kind domain.EffectKind, target, command string) application.Permit {
		t.Helper()
		p, err := effects.Prepare(ctx, cmd("tenant_a", command, command), domain.EffectRequest{
			OperationID: op.ID, Owner: "workflow", Kind: kind, Occurrence: map[string]uint64{"pub_early": 1, "pub_browser": 2, "pub_npm": 3, "act_early": 1, "act": 2, "review": 1}[command],
			CanonicalSubject: domain.ReleaseEffectSubject(subject.SubjectDigest, target), ExecutionEpoch: epoch, Deadline: effectDeadline,
		})
		require.NoError(t, err)
		return p
	}
	observe := func(effectID string, receipt domain.Digest, ref string) {
		t.Helper()
		_, _, err := effects.Observe(ctx, "tenant_a", effectID, "workflow", 1, domain.EffectOutcomeSucceeded, string(receipt), ref, time.Now())
		require.NoError(t, err)
	}
	// The review is registered under its own effect; an approval is
	// decided under it (B-38).
	review := prepare(domain.EffectReview, "review", "review")
	require.True(t, review.Permitted)
	observe(review.Effect.ID, subject.SubjectDigest, "review:rel_1")
	awaiting := domain.ReleaseRecord{ExpectedRevision: 1, State: domain.ReleaseAwaitingApproval, Subject: subject, ReleaseID: "rel_1", ReviewEffectID: review.Effect.ID,
		Approval: &domain.Approval{State: domain.ApprovalPending, SubjectDigest: subject.SubjectDigest}, ApprovalDeadline: &deadline, Npm: pending, Browser: pending, Activation: pending}
	_, _, err = releases.Record(ctx, cmd("tenant_a", "rec", "rec"), op.ID, awaiting)
	require.NoError(t, err)
	again, existing, err := releases.Record(ctx, cmd("tenant_a", "rec", "rec"), op.ID, awaiting)
	require.NoError(t, err)
	require.True(t, existing, "the same transition repeated (nanosecond deadline normalized)")
	require.Equal(t, uint64(2), again.Revision)

	t.Run("no publication permit before an approval of exactly the subject", func(t *testing.T) {
		p := prepare(domain.EffectPublication, "npm", "pub_early")
		require.False(t, p.Permitted)
		require.Equal(t, domain.DenyApprovalRequired, p.DenialCode)
	})

	publishing := awaiting
	publishing.ExpectedRevision, publishing.State = 2, domain.ReleasePublishing
	publishing.Approval = &domain.Approval{State: domain.ApprovalApproved, SubjectDigest: subject.SubjectDigest, ApproverID: "maintainer_a"}
	_, _, err = releases.Record(ctx, cmd("tenant_a", "rec", "rec"), op.ID, publishing)
	require.NoError(t, err)

	var npm, browser application.Permit
	t.Run("publication is permitted once under the approval; activation waits for both receipts", func(t *testing.T) {
		browser = prepare(domain.EffectPublication, "browser", "pub_browser")
		require.True(t, browser.Permitted)
		again := prepare(domain.EffectPublication, "browser", "pub_browser")
		require.False(t, again.Permitted, "a permit is issued once")
		act := prepare(domain.EffectActivation, "activation", "act_early")
		require.False(t, act.Permitted)
		require.Equal(t, domain.DenyReceiptsRequired, act.DenialCode)
	})

	// B-38 (P0.2 AC3): a succeeded target is accepted only when the
	// operation's effect ledger holds a succeeded effect whose outcome is
	// the target's receipt.
	npm = prepare(domain.EffectPublication, "npm", "pub_npm")
	require.True(t, npm.Permitted)
	target := func(effectID, receiptID, destination string) domain.ReleaseTarget {
		return domain.ReleaseTarget{State: domain.TargetSucceeded, EffectID: effectID, ReceiptID: receiptID, ReceiptDigest: string(h("r")), SubjectDigest: subject.SubjectDigest,
			Destination: destination, Version: "1.0.0"}
	}
	published := publishing
	published.ExpectedRevision, published.State = 3, domain.ReleasePublished
	published.Npm = target(npm.Effect.ID, "r_npm", subject.Destinations.NpmRegistry)
	published.Browser = target(browser.Effect.ID, "r_browser", subject.Destinations.BrowserOrigin)
	published.Browser.ManifestDigest = string(h("m"))
	t.Run("a succeeded target without its succeeded effect is refused", func(t *testing.T) {
		_, _, err := releases.Record(ctx, cmd("tenant_a", "rec", "rec"), op.ID, published)
		require.ErrorIs(t, err, domain.ErrIntegrity, "neither effect was observed")
		observe(npm.Effect.ID, h("other receipt"), "receipt:r_npm")
		_, _, err = releases.Record(ctx, cmd("tenant_a", "rec", "rec"), op.ID, published)
		require.ErrorIs(t, err, domain.ErrIntegrity, "the npm effect observed another receipt")
		_, _, err = releases.Record(ctx, cmd("tenant_b", "rec", "rec"), op.ID, published)
		require.ErrorIs(t, err, domain.ErrNotFound, "another tenant's command")
	})
	t.Run("targets the ledger holds publish; activation is permitted from the ledger", func(t *testing.T) {
		observe(browser.Effect.ID, h("r"), "receipt:r_browser")
		published.Npm.ReceiptDigest = string(h("other receipt"))
		got, _, err := releases.Record(ctx, cmd("tenant_a", "rec", "rec"), op.ID, published)
		require.NoError(t, err)
		require.True(t, got.Published())
		act := prepare(domain.EffectActivation, "activation", "act")
		require.True(t, act.Permitted, act.DenialCode)
	})

	t.Run("the release is read by its tenant only", func(t *testing.T) {
		got, err := releases.Get(ctx, "tenant_a", op.ID)
		require.NoError(t, err)
		require.Equal(t, domain.ReleasePublished, got.State)
		require.Equal(t, subject.SubjectDigest, got.Subject.SubjectDigest)
		require.Equal(t, lineage, got.Lineage)
		require.Equal(t, "4", got.SourceRevision)
		_, err = releases.Get(ctx, "tenant_b", op.ID)
		require.ErrorIs(t, err, domain.ErrNotFound)
	})
}

// Lineage identities (P0.8, F-P0.8-1) against real PostgreSQL: a frozen
// brief carries the identity it allocates; the first candidate
// registration of a Generation on a lineage binds it, another Generation
// under the same identity is permitted and one under another identity is
// denied; a brief without an identity and a preview's save bind nothing;
// previews and releases read the lineage's identity, and a release of a
// lineage without one is refused.
func TestLineageIdentities(t *testing.T) {
	inst := testdb.Start(t)
	pool := inst.Pool(t)
	store := postgres.NewStore(pool)
	inv, err := inventory.NewFilesystem(filepath.Join(t.TempDir(), "inventory"))
	require.NoError(t, err)
	profiles := []domain.Profile{
		{ID: "preparation-v1", Kind: domain.KindPreparation, OperationDeadline: 10 * time.Hour, StepID: "analysis", MultiStep: true},
		{ID: "generation-v1", Kind: domain.KindGeneration, OperationDeadline: 2 * time.Hour, StepID: "codegen", MultiStep: true},
		previewProfile, releaseProfile,
	}
	ops := application.NewOperations(store, inv, profiles, domain.SystemClock{}, testLog)
	preparations := application.NewPreparations(store, profiles, domain.SystemClock{}, testLog)
	previews := application.NewPreviews(store, domain.SystemClock{})
	effects := application.NewEffects(store, inv, nil, domain.SystemClock{}, testLog)
	ctx := context.Background()
	exec := func(sql string, args ...any) {
		t.Helper()
		_, err := pool.Exec(ctx, sql, args...)
		require.NoError(t, err, sql)
	}
	transfer := func(id, class, operationID string, digest domain.Digest) {
		exec(`INSERT INTO artifact_transfers (transfer_id, tenant_id, operation_id, class, media_type, expected_digest, expected_size, handle, state,
			object_version, command_id, request_digest, deadline, actual_digest, actual_size) VALUES ($1, 'tenant_a', NULLIF($2, ''), $3, 'application/octet-stream', $4, 10, $5, 'finalized', 'v1', $1, $4, now() + interval '1 hour', $4, 10)`,
			"xfer_"+id, operationID, class, string(digest), "hdl_"+id)
	}
	hero := &domain.ComponentIdentity{ComponentID: "cmp_hero", PuckType: "Hero", PackageName: "@acme/hero"}
	banner := &domain.ComponentIdentity{ComponentID: "cmp_banner", PuckType: "Banner", PackageName: "@acme/banner"}
	// brief freezes a brief allocating the identity (none when nil) on a
	// succeeded preparation of its own.
	brief := func(name string, identity *domain.ComponentIdentity) *domain.Brief {
		t.Helper()
		prompt := application.DigestOf([]byte("prompt " + name))
		transfer("prompt_"+name, "prompt", "", prompt)
		prep, _, err := ops.Create(ctx, cmd("tenant_a", "prep_"+name, name), scopeA, domain.KindPreparation, domain.Subject{ProfileID: "preparation-v1", SubjectDigest: prompt},
			&domain.PreparationIntake{Prompt: domain.ArtifactBinding{TransferID: "xfer_prompt_" + name, Digest: prompt}})
		require.NoError(t, err)
		body := application.DigestOf([]byte("brief " + name))
		transfer("brief_"+name, "brief", prep.ID, body)
		b, _, err := preparations.RecordBrief(ctx, cmd("tenant_a", prep.ID+":brief", name), prep.ID, domain.ArtifactBinding{TransferID: "xfer_brief_" + name, Digest: body},
			application.DigestOf([]byte("requirements")), nil, nil, nil, identity)
		require.NoError(t, err)
		exec(`UPDATE operations SET lifecycle = 'succeeded', phase = 'brief_frozen' WHERE operation_id = $1`, prep.ID)
		return b
	}
	generation := func(name string, lineage domain.Digest, b *domain.Brief) *domain.Operation {
		t.Helper()
		op, _, err := ops.Create(ctx, cmd("tenant_a", "gen_"+name, name), scopeA, domain.KindGeneration, domain.Subject{ProfileID: "generation-v1", SubjectDigest: lineage, BriefID: b.ID}, nil)
		require.NoError(t, err)
		return op
	}
	// register prepares the business write on "source:<lineage>" of the
	// operation: a Generation's candidate registration or a preview's save.
	effectDeadline := time.Now().Add(10 * time.Minute)
	register := func(op *domain.Operation) application.Permit {
		t.Helper()
		p, err := effects.Prepare(ctx, cmd("tenant_a", op.ID+":source:1", "register"), domain.EffectRequest{
			OperationID: op.ID, Owner: "workflow", Kind: domain.EffectBusinessWrite, Occurrence: 1, CanonicalSubject: "source:" + string(op.Subject.SubjectDigest),
			ExpectedRevision: "3", ExecutionEpoch: op.ExecutionEpoch, Deadline: effectDeadline,
		})
		require.NoError(t, err)
		return p
	}
	recorded := func(lineage domain.Digest) *domain.LineageIdentity {
		t.Helper()
		var l *domain.LineageIdentity
		err := store.Read(ctx, func(r application.Repo) error {
			var err error
			l, err = r.GetLineageIdentity(ctx, "tenant_a", lineage)
			return err
		})
		if errors.Is(err, domain.ErrNotFound) {
			return nil
		}
		require.NoError(t, err)
		return l
	}
	bound, unbound := application.DigestOf([]byte("bound lineage")), application.DigestOf([]byte("unbound lineage"))
	heroBrief := brief("hero", hero)

	var first *domain.Operation
	t.Run("the first candidate registration binds the brief's identity; the same identity again is permitted", func(t *testing.T) {
		require.Nil(t, recorded(bound))
		first = generation("first", bound, heroBrief)
		p := register(first)
		require.True(t, p.Permitted, p.DenialCode)
		l := recorded(bound)
		require.NotNil(t, l)
		require.Equal(t, *hero, l.Identity)
		require.Equal(t, first.ID, l.OperationID)
		require.Equal(t, heroBrief.ID, l.BriefID)
		second := register(generation("second", bound, heroBrief))
		require.True(t, second.Permitted, second.DenialCode)
		require.Equal(t, first.ID, recorded(bound).OperationID, "the identity is recorded once")
		// F-P0.8-2: the registration is the Generation's candidate effect,
		// whose observation answers the registered revision of a release.
		var op *domain.Operation
		require.NoError(t, store.Read(ctx, func(r application.Repo) error {
			var err error
			op, err = r.GetOperationScoped(ctx, first.ID, "tenant_a")
			return err
		}))
		require.Equal(t, p.Effect.ID, op.CandidateEffectID)
		require.Greater(t, op.Revision, first.Revision, "recorded as a transition of the operation")
		again := register(first)
		require.Equal(t, p.Effect.ID, again.Effect.ID, "the same command reenters the same effect")
	})

	t.Run("a registration under another identity is denied IDENTITY_MISMATCH", func(t *testing.T) {
		other := generation("other", bound, brief("banner", banner))
		p := register(other)
		require.False(t, p.Permitted)
		require.Equal(t, domain.EffectDenied, p.Effect.State)
		require.Equal(t, domain.DenyIdentityMismatch, p.DenialCode)
		again := register(other)
		require.False(t, again.Permitted, "the same command reenters its denial")
		require.Equal(t, p.Effect.ID, again.Effect.ID)
		require.Equal(t, *hero, recorded(bound).Identity, "the lineage keeps its identity")
	})

	transfer("src", "source", "", application.DigestOf([]byte("edited source tar")))
	t.Run("a brief without an identity and a preview's save bind nothing", func(t *testing.T) {
		p := register(generation("unallocated", unbound, brief("legacy", nil)))
		require.True(t, p.Permitted, p.DenialCode)
		require.Nil(t, recorded(unbound), "a brief frozen before P0.8 allocates nothing")
		preview, _, err := ops.Create(ctx, cmd("tenant_a", "prev_save", "p"), scopeA, domain.KindPreviewBuild, domain.Subject{ProfileID: "preview-build-v1", SubjectDigest: unbound, SourceRevision: "3", SourceHandle: "hdl_src"}, nil)
		require.NoError(t, err)
		p = register(preview)
		require.True(t, p.Permitted, p.DenialCode)
		require.Nil(t, recorded(unbound), "a preview's save is no candidate registration")
	})

	// saved is a preview of the lineage that saved revision 4.
	saved := func(name string, lineage domain.Digest) *domain.Operation {
		t.Helper()
		op, _, err := ops.Create(ctx, cmd("tenant_a", "prev_"+name, name), scopeA, domain.KindPreviewBuild, domain.Subject{ProfileID: "preview-build-v1", SubjectDigest: lineage, SourceRevision: "3", SourceHandle: "hdl_src"}, nil)
		require.NoError(t, err)
		rec := domain.PreviewRecord{State: domain.PreviewSaving, SourceDigest: application.DigestOf([]byte("edited source tar")), BuildProfileID: "b", HostProfileID: "h"}
		_, _, err = previews.Record(ctx, cmd("tenant_a", "rec_"+name, "rec"), op.ID, rec)
		require.NoError(t, err)
		rec.ExpectedRevision, rec.State, rec.SourceRevision = 1, domain.PreviewBuilding, "4"
		_, _, err = previews.Record(ctx, cmd("tenant_a", "rec_"+name, "rec"), op.ID, rec)
		require.NoError(t, err)
		return op
	}
	release := func(name string, lineage domain.Digest, source *domain.Operation) (*domain.Operation, error) {
		op, _, err := ops.Create(ctx, cmd("tenant_a", "rel_"+name, name), scopeA, domain.KindRelease,
			domain.Subject{ProfileID: "release-v1", SubjectDigest: lineage, SourceRevision: "4", SourceOperationID: source.ID, PackageVersion: "1.0.0"}, nil)
		return op, err
	}

	boundPreview, unboundPreview := saved("bound", bound), saved("unbound", unbound)
	var boundRelease *domain.Operation
	t.Run("a release of a lineage without an identity is refused; with one it proceeds", func(t *testing.T) {
		_, err := release("unbound", unbound, unboundPreview)
		require.ErrorIs(t, err, domain.ErrInvalid)
		require.ErrorContains(t, err, domain.IdentityUnallocated)
		var err2 error
		boundRelease, err2 = release("bound", bound, boundPreview)
		require.NoError(t, err2)
		require.Equal(t, "hdl_src", boundRelease.Subject.SourceHandle)
	})

	t.Run("a release of the Generation itself reads the revision its candidate registration answered (F-P0.8-2)", func(t *testing.T) {
		gen, err := ops.Get(ctx, scopeA, first.ID)
		require.NoError(t, err)
		require.NotEmpty(t, gen.CandidateEffectID)
		// The Generation's accepted certified stage binds its source.
		src := application.DigestOf([]byte("generated source tar"))
		transfer("gensrc", "source", first.ID, src)
		exec(`INSERT INTO attempts (attempt_id, operation_id, tenant_id, step_id, visit_ordinal, attempt_ordinal, profile_id, execution_epoch, command_id, request_digest, state, deadline)
			VALUES ('att_gen', $1, 'tenant_a', 'codegen', 0, 1, 'codegen-team-dev-v1', 1, 'cmd_att_gen', $2, 'result_accepted', now() + interval '1 hour')`, first.ID, string(src))
		exec(`INSERT INTO launches (launch_id, attempt_id, operation_id, launch_key, backend, profile_id, image_digest, execution_epoch, launch_epoch, deadline, command_id, request_digest, inventory_state)
			VALUES ('lch_gen', 'att_gen', $1, 'gen-key', 'kind', 'codegen-team-dev-v1', $2, 1, 1, now() + interval '1 hour', 'cmd_lch_gen', $2, 'confirmed')`, first.ID, string(src))
		exec(`INSERT INTO physical_instances (instance_id, attempt_id, launch_id, launch_key, backend, job_uid, pod_uid, image_digest, launch_epoch, phase, is_current)
			VALUES ('inst_gen', 'att_gen', 'lch_gen', 'gen-key', 'kind', 'job', 'pod-gen', $1, 1, 'succeeded', true)`, string(src))
		exec(`INSERT INTO stage_manifests (stage_id, attempt_id, instance_id, operation_id, phase_ordinal, profile_id, verdict, result_digest, result_manifest, observer_identity, command_id, request_digest)
			VALUES ('stg_gen', 'att_gen', 'inst_gen', $1, 1, 'codegen-team-dev-v1', 'certified', $2, '{"verdict": "certified", "schemaVersion": 1}', 'observer', 'cmd_stg_gen', $2)`, first.ID, string(src))
		exec(`INSERT INTO stage_artifacts (stage_id, transfer_id, handle, class, digest, size_bytes, object_version) VALUES ('stg_gen', 'xfer_gensrc', 'hdl_gensrc', 'source', $1, 10, 'v1')`, string(src))
		_, err = release("gen-unregistered", bound, first)
		require.ErrorIs(t, err, domain.ErrInvalid, "no registered revision is observed yet")
		_, _, err = effects.Observe(ctx, "tenant_a", gen.CandidateEffectID, "workflow", 1, domain.EffectOutcomeSucceeded, "4", "cand_first", time.Now())
		require.NoError(t, err)
		rel, err := release("gen", bound, first)
		require.NoError(t, err)
		require.Equal(t, "hdl_gensrc", rel.Subject.SourceHandle, "the Generation's certified source at its registered revision")
	})

	t.Run("previews and releases read the lineage's identity; another kind or an unbound lineage none", func(t *testing.T) {
		for name, c := range map[string]struct {
			op   *domain.Operation
			want *domain.ComponentIdentity
		}{
			"preview of the bound lineage":    {boundPreview, hero},
			"release of the bound lineage":    {boundRelease, hero},
			"preview of an unbound lineage":   {unboundPreview, nil},
			"generation of the bound lineage": {first, nil},
		} {
			op, err := ops.Get(ctx, scopeA, c.op.ID)
			require.NoError(t, err, name)
			got, err := ops.LineageIdentity(ctx, op)
			require.NoError(t, err, name)
			require.Equal(t, c.want, got, name)
		}
	})
}
