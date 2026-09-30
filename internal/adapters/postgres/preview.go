package postgres

import (
	"context"
	"encoding/json"
	"time"

	"github.com/ancyloce/anvilkit-agent-control/internal/adapters/postgres/sqlc"
	"github.com/ancyloce/anvilkit-agent-control/internal/domain"
)

func toPreview(m sqlc.Preview) *domain.Preview {
	p := &domain.Preview{
		OperationID: m.OperationID, TenantID: m.TenantID, SubjectDigest: domain.Digest(m.SubjectDigest), BaseRevision: m.BaseRevision,
		State: domain.PreviewState(m.State), SourceRevision: deref(m.SourceRevision), CurrentRevision: deref(m.CurrentRevision),
		SourceDigest: domain.Digest(m.SourceDigest), BuildProfileID: m.BuildProfileID, HostProfileID: m.HostProfileID,
		FailureCode: deref(m.FailureCode), Revision: uint64(m.Revision), UpdatedAt: m.UpdatedAt.Time.UTC(),
	}
	if len(m.Module) > 0 && string(m.Module) != "null" {
		var a domain.PreviewArtifact
		if json.Unmarshal(m.Module, &a) == nil {
			p.Module = &a
		}
	}
	_ = json.Unmarshal(m.Styles, &p.Styles)
	return p
}

func previewJSON(p *domain.Preview) (module, styles []byte) {
	if p.Module != nil {
		module, _ = json.Marshal(p.Module)
	}
	st := p.Styles
	if st == nil {
		st = []domain.PreviewArtifact{}
	}
	styles, _ = json.Marshal(st)
	return module, styles
}

func (r *repo) GetPreview(ctx context.Context, operationID string) (*domain.Preview, error) {
	m, err := r.q.GetPreview(ctx, operationID)
	if err != nil {
		return nil, mapErr(err)
	}
	return toPreview(m), nil
}

func (r *repo) LockPreview(ctx context.Context, operationID string) (*domain.Preview, error) {
	m, err := r.q.LockPreview(ctx, operationID)
	if err != nil {
		return nil, mapErr(err)
	}
	return toPreview(m), nil
}

func (r *repo) InsertPreview(ctx context.Context, p *domain.Preview) error {
	module, styles := previewJSON(p)
	return r.q.InsertPreview(ctx, sqlc.InsertPreviewParams{
		OperationID: p.OperationID, TenantID: p.TenantID, SubjectDigest: string(p.SubjectDigest), BaseRevision: p.BaseRevision, State: string(p.State),
		SourceRevision: strPtr(p.SourceRevision), CurrentRevision: strPtr(p.CurrentRevision), SourceDigest: string(p.SourceDigest), Module: module,
		Styles: styles, BuildProfileID: p.BuildProfileID, HostProfileID: p.HostProfileID, FailureCode: strPtr(p.FailureCode), Revision: int64(p.Revision),
		UpdatedAt: ts(p.UpdatedAt.Truncate(time.Microsecond)),
	})
}

func (r *repo) UpdatePreview(ctx context.Context, p *domain.Preview, expected uint64) (bool, error) {
	module, styles := previewJSON(p)
	n, err := r.q.UpdatePreview(ctx, sqlc.UpdatePreviewParams{
		OperationID: p.OperationID, State: string(p.State), SourceRevision: strPtr(p.SourceRevision), CurrentRevision: strPtr(p.CurrentRevision),
		SourceDigest: string(p.SourceDigest), Module: module, Styles: styles, BuildProfileID: p.BuildProfileID, HostProfileID: p.HostProfileID,
		FailureCode: strPtr(p.FailureCode), Revision: int64(p.Revision), UpdatedAt: ts(p.UpdatedAt.Truncate(time.Microsecond)), ExpectedRevision: int64(expected),
	})
	return n == 1, err
}

func (r *repo) GetCertifiedSourceArtifact(ctx context.Context, operationID string) (*domain.StageArtifact, error) {
	m, err := r.q.GetCertifiedSourceArtifact(ctx, operationID)
	if err != nil {
		return nil, mapErr(err)
	}
	return &domain.StageArtifact{StageID: m.StageID, TransferID: m.TransferID, Handle: m.Handle, Class: domain.ArtifactClass(m.Class), Digest: domain.Digest(m.Digest), SizeBytes: m.SizeBytes, ObjectVersion: m.ObjectVersion}, nil
}
