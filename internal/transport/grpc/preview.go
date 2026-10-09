package grpc

import (
	"context"
	"fmt"
	"strconv"

	"google.golang.org/protobuf/types/known/timestamppb"

	controlv1 "github.com/ancyloce/anvilkit-agent-contracts/go/anvilkit/control/v1"
	"github.com/ancyloce/anvilkit-agent-control/internal/application"
	"github.com/ancyloce/anvilkit-agent-control/internal/domain"
)

// previewServer adapts anvilkit.control.v1 PreviewService (P20).
type previewServer struct {
	controlv1.UnimplementedPreviewServiceServer
	previews *application.Previews
}

var previewStateToProto = map[domain.PreviewState]controlv1.PreviewState{
	domain.PreviewSaving: controlv1.PreviewState_PREVIEW_STATE_SAVING, domain.PreviewConflict: controlv1.PreviewState_PREVIEW_STATE_CONFLICT,
	domain.PreviewBuilding: controlv1.PreviewState_PREVIEW_STATE_BUILDING, domain.PreviewReady: controlv1.PreviewState_PREVIEW_STATE_READY,
	domain.PreviewStale: controlv1.PreviewState_PREVIEW_STATE_STALE, domain.PreviewFailed: controlv1.PreviewState_PREVIEW_STATE_FAILED,
}

func previewArtifactOf(a *controlv1.ArtifactReference) (*domain.PreviewArtifact, error) {
	if a == nil {
		return nil, nil
	}
	size, err := strconv.ParseInt(a.GetSizeBytes(), 10, 64)
	if err != nil {
		return nil, fmt.Errorf("%w: artifact size %q", domain.ErrInvalid, a.GetSizeBytes())
	}
	return &domain.PreviewArtifact{Handle: a.GetHandle(), Class: a.GetClass(), Digest: domain.Digest(a.GetDigest()), SizeBytes: size,
		TransferID: a.GetTransferId(), ObjectVersion: a.GetObjectVersion()}, nil
}

func toArtifactRef(a domain.PreviewArtifact) *controlv1.ArtifactReference {
	return &controlv1.ArtifactReference{Handle: a.Handle, Class: a.Class, Digest: string(a.Digest), SizeBytes: strconv.FormatInt(a.SizeBytes, 10),
		TransferId: a.TransferID, ObjectVersion: a.ObjectVersion}
}

func toPreview(p *domain.Preview) *controlv1.Preview {
	out := &controlv1.Preview{
		OperationId: p.OperationID, TenantId: p.TenantID, SubjectDigest: string(p.SubjectDigest), BaseRevision: p.BaseRevision,
		State: previewStateToProto[p.State], SourceRevision: optString(p.SourceRevision), CurrentRevision: optString(p.CurrentRevision),
		SourceDigest: string(p.SourceDigest), BuildProfileId: p.BuildProfileID, HostProfileId: p.HostProfileID, FailureCode: optString(p.FailureCode),
		Revision: strconv.FormatUint(p.Revision, 10), UpdatedAt: timestamppb.New(p.UpdatedAt),
	}
	if p.Module != nil {
		out.Module = toArtifactRef(*p.Module)
	}
	for _, s := range p.Styles {
		out.Styles = append(out.Styles, toArtifactRef(s))
	}
	return out
}

func (s *previewServer) RecordPreview(ctx context.Context, req *controlv1.RecordPreviewRequest) (*controlv1.RecordPreviewResponse, error) {
	cmd, err := commandIdentity(req.GetCommand())
	if err != nil {
		return nil, toStatus(err)
	}
	expected, err := strconv.ParseUint(req.GetExpectedRevision(), 10, 64)
	if err != nil {
		return nil, toStatus(fmt.Errorf("%w: expected revision", domain.ErrInvalid))
	}
	var state domain.PreviewState
	for k, v := range previewStateToProto {
		if v == req.GetState() {
			state = k
		}
	}
	module, err := previewArtifactOf(req.GetModule())
	if err != nil {
		return nil, toStatus(err)
	}
	styles := make([]domain.PreviewArtifact, 0, len(req.GetStyles()))
	for _, a := range req.GetStyles() {
		st, err := previewArtifactOf(a)
		if err != nil {
			return nil, toStatus(err)
		}
		styles = append(styles, *st)
	}
	p, existing, err := s.previews.Record(ctx, cmd, req.GetOperationId(), domain.PreviewRecord{
		ExpectedRevision: expected, State: state, SourceRevision: req.GetSourceRevision(), CurrentRevision: req.GetCurrentRevision(),
		SourceDigest: domain.Digest(req.GetSourceDigest()), Module: module, Styles: styles, BuildProfileID: req.GetBuildProfileId(),
		HostProfileID: req.GetHostProfileId(), FailureCode: req.GetFailureCode(),
	})
	if err != nil {
		return nil, toStatus(err)
	}
	return &controlv1.RecordPreviewResponse{Preview: toPreview(p), Existing: existing}, nil
}

func (s *previewServer) GetPreview(ctx context.Context, req *controlv1.GetPreviewRequest) (*controlv1.GetPreviewResponse, error) {
	p, err := s.previews.Get(ctx, req.GetTenantId(), req.GetOperationId())
	if err != nil {
		return nil, toStatus(err)
	}
	return &controlv1.GetPreviewResponse{Preview: toPreview(p)}, nil
}

func (s *previewServer) GetSource(ctx context.Context, req *controlv1.GetSourceRequest) (*controlv1.GetSourceResponse, error) {
	src, err := s.previews.Source(ctx, req.GetTenantId(), req.GetOperationId())
	if err != nil {
		return nil, toStatus(err)
	}
	a := src.Source
	out := &controlv1.GetSourceResponse{
		Source:  &controlv1.ArtifactReference{Handle: a.Handle, Class: string(a.Class), Digest: string(a.Digest), SizeBytes: strconv.FormatInt(a.SizeBytes, 10), TransferId: a.TransferID, ObjectVersion: a.ObjectVersion},
		Lineage: string(src.Lineage),
	}
	if src.Revision != "" {
		out.Revision = &src.Revision
	}
	return out, nil
}
