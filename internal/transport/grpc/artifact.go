package grpc

import (
	"context"

	"google.golang.org/protobuf/types/known/timestamppb"

	controlv1 "github.com/ancyloce/anvilkit-agent-contracts/go/anvilkit/control/v1"
	"github.com/ancyloce/anvilkit-agent-control/internal/application"
	"github.com/ancyloce/anvilkit-agent-control/internal/domain"
)

// artifactServer adapts anvilkit.control.v1.ArtifactService to the scoped
// transfer (DD-02 §6). The object key of a transfer never appears on this
// surface; callers see handles, transfer ids and, when they are trusted to
// upload, the capability.
type artifactServer struct {
	controlv1.UnimplementedArtifactServiceServer
	artifacts *application.Artifacts
}

var artifactClassFromProto = map[controlv1.ArtifactClass]domain.ArtifactClass{
	controlv1.ArtifactClass_ARTIFACT_CLASS_PROMPT: domain.ArtifactPrompt, controlv1.ArtifactClass_ARTIFACT_CLASS_BRIEF: domain.ArtifactBrief,
	controlv1.ArtifactClass_ARTIFACT_CLASS_SOURCE: domain.ArtifactSource, controlv1.ArtifactClass_ARTIFACT_CLASS_STAGE: domain.ArtifactStage,
	controlv1.ArtifactClass_ARTIFACT_CLASS_RESULT: domain.ArtifactResult, controlv1.ArtifactClass_ARTIFACT_CLASS_EVIDENCE: domain.ArtifactEvidence,
	controlv1.ArtifactClass_ARTIFACT_CLASS_ANSWER: domain.ArtifactAnswer, controlv1.ArtifactClass_ARTIFACT_CLASS_ARGUMENT: domain.ArtifactArgument,
	controlv1.ArtifactClass_ARTIFACT_CLASS_NPM: domain.ArtifactNpm, controlv1.ArtifactClass_ARTIFACT_CLASS_BROWSER: domain.ArtifactBrowser,
	controlv1.ArtifactClass_ARTIFACT_CLASS_CSS: domain.ArtifactCSS,
}

var artifactClassToProto = map[domain.ArtifactClass]controlv1.ArtifactClass{
	domain.ArtifactPrompt: controlv1.ArtifactClass_ARTIFACT_CLASS_PROMPT, domain.ArtifactBrief: controlv1.ArtifactClass_ARTIFACT_CLASS_BRIEF,
	domain.ArtifactSource: controlv1.ArtifactClass_ARTIFACT_CLASS_SOURCE, domain.ArtifactStage: controlv1.ArtifactClass_ARTIFACT_CLASS_STAGE,
	domain.ArtifactResult: controlv1.ArtifactClass_ARTIFACT_CLASS_RESULT, domain.ArtifactEvidence: controlv1.ArtifactClass_ARTIFACT_CLASS_EVIDENCE,
	domain.ArtifactAnswer: controlv1.ArtifactClass_ARTIFACT_CLASS_ANSWER, domain.ArtifactArgument: controlv1.ArtifactClass_ARTIFACT_CLASS_ARGUMENT,
	domain.ArtifactNpm: controlv1.ArtifactClass_ARTIFACT_CLASS_NPM, domain.ArtifactBrowser: controlv1.ArtifactClass_ARTIFACT_CLASS_BROWSER,
	domain.ArtifactCSS: controlv1.ArtifactClass_ARTIFACT_CLASS_CSS,
}

var transferStateToProto = map[domain.TransferState]controlv1.TransferState{
	domain.TransferBegun: controlv1.TransferState_TRANSFER_STATE_BEGUN, domain.TransferFinalized: controlv1.TransferState_TRANSFER_STATE_FINALIZED,
	domain.TransferRejected: controlv1.TransferState_TRANSFER_STATE_REJECTED, domain.TransferExpired: controlv1.TransferState_TRANSFER_STATE_EXPIRED,
}

func toTransfer(t *domain.Transfer) *controlv1.Transfer {
	out := &controlv1.Transfer{
		TransferId: t.ID, Handle: t.Handle, Class: artifactClassToProto[t.Class], ExpectedDigest: string(t.ExpectedDigest), ExpectedSize: domain.Revision(t.ExpectedSize).String(),
		State: transferStateToProto[t.State], Deadline: timestamppb.New(t.Deadline), ObjectVersion: optString(t.ObjectVersion), ReasonCode: optString(t.ReasonCode),
		TenantId: t.TenantID, OperationId: optString(t.OperationID), AttemptId: optString(t.AttemptID), MediaType: t.MediaType,
		ExecutionEpoch: domain.Revision(t.ExecutionEpoch).String(), RecoveryEpoch: domain.Revision(t.RecoveryEpoch).String(), CreatedAt: timestamppb.New(t.CreatedAt),
	}
	if t.FinalizedAt != nil {
		out.FinalizedAt = timestamppb.New(*t.FinalizedAt)
	}
	return out
}

func toCapability(c *application.UploadCapability) *controlv1.TransferCapability {
	if c == nil {
		return nil
	}
	return &controlv1.TransferCapability{Url: c.URL, Method: c.Method, Headers: c.Headers, ExpiresAt: timestamppb.New(c.ExpiresAt)}
}

func (s *artifactServer) BeginTransfer(ctx context.Context, req *controlv1.BeginTransferRequest) (*controlv1.BeginTransferResponse, error) {
	cmd, err := commandIdentity(req.GetCommand())
	if err != nil {
		return nil, toStatus(err)
	}
	digest, err := domain.ParseDigest(req.GetExpectedDigest())
	if err != nil {
		return nil, toStatus(err)
	}
	size, err := domain.ParseRevision(req.GetExpectedSize())
	if err != nil {
		return nil, toStatus(err)
	}
	t, existing, cap, err := s.artifacts.Begin(ctx, cmd, scope(req.GetScope()), domain.TransferRequest{
		Class: artifactClassFromProto[req.GetClass()], MediaType: req.GetMediaType(), ExpectedDigest: digest, ExpectedSize: int64(size),
		OperationID: req.GetOperationId(), AttemptID: req.GetAttemptId(), Deadline: req.GetDeadline().AsTime(),
	})
	if err != nil {
		return nil, toStatus(err)
	}
	return &controlv1.BeginTransferResponse{Transfer: toTransfer(t), Existing: existing, Upload: toCapability(cap)}, nil
}

func (s *artifactServer) ResolveHandle(ctx context.Context, req *controlv1.ResolveHandleRequest) (*controlv1.ResolveHandleResponse, error) {
	t, cap, err := s.artifacts.Resolve(ctx, req.GetHandle(), req.GetInstanceId())
	if err != nil {
		return nil, toStatus(err)
	}
	return &controlv1.ResolveHandleResponse{Transfer: toTransfer(t), Capability: cap.URL, ExpiresAt: timestamppb.New(cap.ExpiresAt), Upload: toCapability(cap)}, nil
}

func (s *artifactServer) FinalizeTransfer(ctx context.Context, req *controlv1.FinalizeTransferRequest) (*controlv1.FinalizeTransferResponse, error) {
	cmd, err := commandIdentity(req.GetCommand())
	if err != nil {
		return nil, toStatus(err)
	}
	t, existing, err := s.artifacts.Finalize(ctx, cmd, req.GetTransferId(), req.GetHandle(), req.GetObjectVersion(), req.GetInstanceId())
	if err != nil {
		return nil, toStatus(err)
	}
	return &controlv1.FinalizeTransferResponse{Transfer: toTransfer(t), Existing: existing}, nil
}

func (s *artifactServer) GetTransfer(ctx context.Context, req *controlv1.GetTransferRequest) (*controlv1.GetTransferResponse, error) {
	t, err := s.artifacts.Get(ctx, scope(req.GetScope()), req.GetTransferId(), req.GetHandle())
	if err != nil {
		return nil, toStatus(err)
	}
	return &controlv1.GetTransferResponse{Transfer: toTransfer(t)}, nil
}

func (s *artifactServer) ReadArtifact(ctx context.Context, req *controlv1.ReadArtifactRequest) (*controlv1.ReadArtifactResponse, error) {
	t, cap, err := s.artifacts.Read(ctx, req.GetHandle(), req.GetTransferId(), req.GetOperationId(), req.GetInstanceId())
	if err != nil {
		return nil, toStatus(err)
	}
	return &controlv1.ReadArtifactResponse{Transfer: toTransfer(t), Download: toCapability(cap)}, nil
}
