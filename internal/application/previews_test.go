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

var previewProfile = domain.Profile{ID: "preview-build-v1", Kind: domain.KindPreviewBuild, OperationDeadline: 30 * time.Minute, StepID: "preview-build", MultiStep: true}

func TestPreviews(t *testing.T) {
	inst := testdb.Start(t)
	pool := inst.Pool(t)
	store := postgres.NewStore(pool)
	inv, err := inventory.NewFilesystem(filepath.Join(t.TempDir(), "inventory"))
	require.NoError(t, err)
	ops := application.NewOperations(store, inv, []domain.Profile{profile, previewProfile}, domain.SystemClock{}, testLog)
	previews := application.NewPreviews(store, domain.SystemClock{})
	ctx := context.Background()
	source := application.DigestOf([]byte("edited source tar"))
	lineage := application.DigestOf([]byte("component source lineage"))
	transfer := func(id, tenant, class string, digest domain.Digest, state string) {
		_, err := pool.Exec(ctx, `INSERT INTO artifact_transfers (transfer_id, tenant_id, class, media_type, expected_digest, expected_size, handle, state,
			object_version, command_id, request_digest, deadline, actual_digest, actual_size) VALUES ($1, $2, $3, 'application/x-tar', $4, 10, $5, $6, 'v1', $1, $4, now() + interval '1 hour', $4, 10)`,
			"xfer_"+id, tenant, class, string(digest), "hdl_"+id, state)
		require.NoError(t, err)
	}
	transfer("src", "tenant_a", "source", source, "finalized")
	transfer("prompt", "tenant_a", "prompt", source, "finalized")
	transfer("begun", "tenant_a", "source", source, "begun")
	transfer("other", "tenant_b", "source", source, "finalized")
	subj := func(handle string) domain.Subject {
		return domain.Subject{ProfileID: "preview-build-v1", SubjectDigest: lineage, SourceRevision: "3", SourceHandle: handle}
	}

	t.Run("a preview build names a finalized source artifact of its tenant holding the subject digest", func(t *testing.T) {
		for name, s := range map[string]domain.Subject{
			"no base revision":   {ProfileID: "preview-build-v1", SubjectDigest: lineage, SourceHandle: "hdl_src"},
			"no source artifact": {ProfileID: "preview-build-v1", SubjectDigest: lineage, SourceRevision: "3"},
			"another class":      subj("hdl_prompt"),
			"not finalized":      subj("hdl_begun"),
			"another tenant":     subj("hdl_other"),
		} {
			_, _, err := ops.Create(ctx, cmd("tenant_a", "prev_bad_"+name, name), scopeA, domain.KindPreviewBuild, s, nil)
			require.ErrorIs(t, err, domain.ErrInvalid, name)
		}
		_, _, err := ops.Create(ctx, cmd("tenant_a", "lc_handle", "lc"), scopeA, domain.KindLocalCheck, domain.Subject{ProfileID: "local-check-v1", SubjectDigest: source, SourceHandle: "hdl_src"}, nil)
		require.ErrorIs(t, err, domain.ErrInvalid, "only a preview build names a source artifact")
		op, _, err := ops.Create(ctx, cmd("tenant_a", "prev_ok", "ok"), scopeA, domain.KindPreviewBuild, subj("hdl_src"), nil)
		require.NoError(t, err)
		require.Equal(t, "hdl_src", op.Subject.SourceHandle)
	})

	op, _, err := ops.Create(ctx, cmd("tenant_a", "prev_flow", "flow"), scopeA, domain.KindPreviewBuild, subj("hdl_src"), nil)
	require.NoError(t, err)
	module := &domain.PreviewArtifact{Handle: "hdl_mod", Class: "browser", Digest: application.DigestOf([]byte("module")), SizeBytes: 6, TransferID: "xfer_mod", ObjectVersion: "v1"}
	rec := func(exp uint64, state domain.PreviewState) domain.PreviewRecord {
		return domain.PreviewRecord{ExpectedRevision: exp, State: state, SourceDigest: source, BuildProfileID: "build-support-dev-v1", HostProfileID: "host-abi-dev-v1"}
	}

	t.Run("transitions are recorded under the expected revision; a repeat answers the recorded one", func(t *testing.T) {
		wrong := rec(0, domain.PreviewSaving)
		wrong.SourceDigest = lineage
		_, _, err := previews.Record(ctx, cmd("tenant_a", "rec", "rec"), op.ID, wrong)
		require.ErrorIs(t, err, domain.ErrInvalid, "the recorded source digest is the edited artifact's")
		p, existing, err := previews.Record(ctx, cmd("tenant_a", "rec", "rec"), op.ID, rec(0, domain.PreviewSaving))
		require.NoError(t, err)
		require.False(t, existing)
		require.Equal(t, uint64(1), p.Revision)
		require.Equal(t, "3", p.BaseRevision)
		building := rec(1, domain.PreviewBuilding)
		building.SourceRevision = "4"
		_, _, err = previews.Record(ctx, cmd("tenant_a", "rec", "rec"), op.ID, building)
		require.NoError(t, err)
		_, _, err = previews.Record(ctx, cmd("tenant_a", "rec", "rec"), op.ID, rec(1, domain.PreviewBuilding))
		require.Error(t, err, "a repeat must be identical")
		ready := rec(2, domain.PreviewReady)
		ready.SourceRevision, ready.Module = "4", module
		ready.Styles = []domain.PreviewArtifact{{Handle: "hdl_css", Class: "css", Digest: application.DigestOf([]byte("css")), SizeBytes: 3, TransferID: "xfer_css", ObjectVersion: "v1"}}
		p, existing, err = previews.Record(ctx, cmd("tenant_a", "rec", "rec"), op.ID, ready)
		require.NoError(t, err)
		require.False(t, existing)
		p, existing, err = previews.Record(ctx, cmd("tenant_a", "rec", "rec"), op.ID, ready)
		require.NoError(t, err)
		require.True(t, existing, "the same transition repeated")
		require.Equal(t, uint64(3), p.Revision)
		stale := rec(3, domain.PreviewStale)
		stale.SourceRevision, stale.Module = "4", module
		_, _, err = previews.Record(ctx, cmd("tenant_a", "rec", "rec"), op.ID, stale)
		require.ErrorIs(t, err, domain.ErrInvalid, "a ready preview never moves")
		_, _, err = previews.Record(ctx, cmd("tenant_a", "rec", "rec"), op.ID, rec(1, domain.PreviewFailed))
		require.ErrorIs(t, err, domain.ErrRevisionConflict)
		got, err := previews.Get(ctx, "tenant_a", op.ID)
		require.NoError(t, err)
		require.Equal(t, domain.PreviewReady, got.State)
		require.Equal(t, *module, *got.Module)
		require.Len(t, got.Styles, 1)
		_, err = previews.Get(ctx, "tenant_b", op.ID)
		require.ErrorIs(t, err, domain.ErrNotFound)
	})

	t.Run("a conflict names the current revision; a failure names its code; a stale build keeps its artifacts", func(t *testing.T) {
		conflict, _, err := ops.Create(ctx, cmd("tenant_a", "prev_conflict", "c"), scopeA, domain.KindPreviewBuild, subj("hdl_src"), nil)
		require.NoError(t, err)
		_, _, err = previews.Record(ctx, cmd("tenant_a", "rec", "rec"), conflict.ID, rec(0, domain.PreviewSaving))
		require.NoError(t, err)
		_, _, err = previews.Record(ctx, cmd("tenant_a", "rec", "rec"), conflict.ID, rec(1, domain.PreviewConflict))
		require.ErrorIs(t, err, domain.ErrInvalid)
		c := rec(1, domain.PreviewConflict)
		c.CurrentRevision = "5"
		p, _, err := previews.Record(ctx, cmd("tenant_a", "rec", "rec"), conflict.ID, c)
		require.NoError(t, err)
		require.Equal(t, "5", p.CurrentRevision)

		late, _, err := ops.Create(ctx, cmd("tenant_a", "prev_stale", "s"), scopeA, domain.KindPreviewBuild, subj("hdl_src"), nil)
		require.NoError(t, err)
		_, _, err = previews.Record(ctx, cmd("tenant_a", "rec", "rec"), late.ID, rec(0, domain.PreviewSaving))
		require.NoError(t, err)
		b := rec(1, domain.PreviewBuilding)
		b.SourceRevision = "6"
		_, _, err = previews.Record(ctx, cmd("tenant_a", "rec", "rec"), late.ID, b)
		require.NoError(t, err)
		s := rec(2, domain.PreviewStale)
		s.SourceRevision, s.CurrentRevision, s.Module = "6", "7", module
		p, _, err = previews.Record(ctx, cmd("tenant_a", "rec", "rec"), late.ID, s)
		require.NoError(t, err)
		require.Equal(t, domain.PreviewStale, p.State)
		require.NotNil(t, p.Module, "the stale build stays readable as a diagnostic")

		lc, _, err := ops.Create(ctx, cmd("tenant_a", "lc_preview", "lc"), scopeA, domain.KindLocalCheck, subject, nil)
		require.NoError(t, err)
		_, _, err = previews.Record(ctx, cmd("tenant_a", "rec", "rec"), lc.ID, rec(0, domain.PreviewSaving))
		require.ErrorIs(t, err, domain.ErrInvalid, "only a preview build has a preview")
	})

	t.Run("the source of a preview build is its edited artifact at the saved revision", func(t *testing.T) {
		src, err := previews.Source(ctx, "tenant_a", op.ID)
		require.NoError(t, err)
		require.Equal(t, "hdl_src", src.Source.Handle)
		require.Equal(t, source, src.Source.Digest)
		require.Equal(t, lineage, src.Lineage)
		require.Equal(t, "4", src.Revision, "the revision its save created")
		_, err = previews.Source(ctx, "tenant_b", op.ID)
		require.ErrorIs(t, err, domain.ErrNotFound)
	})
}
