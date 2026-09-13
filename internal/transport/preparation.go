package transport

import (
	"context"
	"errors"
	"strconv"

	"connectrpc.com/connect"
	pb "github.com/ancyloce/anvilkit-agent-control/internal/contracts/controlv1"
	rpc "github.com/ancyloce/anvilkit-agent-control/internal/contracts/controlv1/controlv1connect"
	values "github.com/ancyloce/anvilkit-agent-control/internal/contracts/valuesv1"
	"github.com/ancyloce/anvilkit-agent-control/internal/preparation"
	"github.com/ancyloce/anvilkit-agent-control/internal/storage"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// The preparation methods (development plan S2, 2026-09-13). The actor-facing
// ones carry the API's authenticated context; the round method carries the
// Workflow service identity and no actor.

var artifactKinds = map[string]values.ArtifactKind{"evidence": values.ArtifactKind_EVIDENCE, "preparation-input": values.ArtifactKind_PREPARATION_INPUT}
var artifactKindNames = map[values.ArtifactKind]string{values.ArtifactKind_EVIDENCE: "evidence", values.ArtifactKind_PREPARATION_INPUT: "preparation-input"}

func refToProto(ref storage.Ref) *values.ArtifactRef {
	size, _ := strconv.ParseUint(ref.SizeBytes, 10, 64)
	return &values.ArtifactRef{Kind: artifactKinds[ref.Kind].Enum(), RefId: proto.String(ref.RefID), SubjectDigest: proto.String(ref.SubjectDigest), ContentDigest: proto.String(ref.ContentDigest), SizeBytes: proto.Uint64(size), ObjectVersion: proto.String(ref.ObjectVersion)}
}

func refFromProto(ref *values.ArtifactRef) (storage.Ref, bool) {
	if ref == nil || len(ref.ProtoReflect().GetUnknown()) != 0 || ref.Kind == nil || ref.RefId == nil || ref.SubjectDigest == nil || ref.ContentDigest == nil || ref.SizeBytes == nil || ref.ObjectVersion == nil {
		return storage.Ref{}, false
	}
	name, known := artifactKindNames[ref.GetKind()]
	if !known || ref.GetSizeBytes() == 0 {
		return storage.Ref{}, false
	}
	return storage.Ref{Kind: name, RefID: ref.GetRefId(), SubjectDigest: ref.GetSubjectDigest(), ContentDigest: ref.GetContentDigest(), SizeBytes: strconv.FormatUint(ref.GetSizeBytes(), 10), ObjectVersion: ref.GetObjectVersion()}, true
}

var businessStages = map[string]pb.BusinessStage{"admission_pending": pb.BusinessStage_BUSINESS_STAGE_ADMISSION_PENDING, "queued": pb.BusinessStage_BUSINESS_STAGE_QUEUED, "analyzing": pb.BusinessStage_BUSINESS_STAGE_ANALYZING, "awaiting_input": pb.BusinessStage_BUSINESS_STAGE_AWAITING_INPUT, "brief_ready": pb.BusinessStage_BUSINESS_STAGE_BRIEF_READY, "expired": pb.BusinessStage_BUSINESS_STAGE_EXPIRED, "failed": pb.BusinessStage_BUSINESS_STAGE_FAILED, "canceled": pb.BusinessStage_BUSINESS_STAGE_CANCELED}

func projectionToProto(p storage.Preparation) *pb.PreparationProjection {
	projection := &pb.PreparationProjection{Round: proto.Uint64(0)}
	var rejections uint64
	for i := range p.Rounds {
		r := &p.Rounds[i]
		rejections += uint64(r.ContentRejections)
		if r.AnswerSetRef != nil {
			projection.AcceptedAnswerSetRef = refToProto(*r.AnswerSetRef)
		}
		if r.BriefRef != nil {
			projection.BriefRef = refToProto(*r.BriefRef)
			projection.BriefRevision = proto.Uint64(uint64(r.BriefRevision))
		}
	}
	if current := p.Current(); current != nil {
		projection.Round = proto.Uint64(uint64(current.Ordinal))
		if current.QuestionSetRef != nil {
			projection.QuestionSetRef = refToProto(*current.QuestionSetRef)
			projection.QuestionSetRevision = proto.Uint64(uint64(current.QuestionSetRevision))
			projection.QuestionSetExpiresAt = timestamppb.New(*current.ExpiresAt)
		}
	}
	projection.ContentRejections = proto.Uint64(rejections)
	return projection
}

func (h controlHandler) admitPreparation(ctx context.Context, auth *disclosureRequest, m *pb.AdmitOperationRequest) (*connect.Response[pb.AdmitOperationResponse], error) {
	if h.preparations == nil {
		return nil, connect.NewError(connect.CodeFailedPrecondition, errors.New("QUALIFICATION_REQUIRED"))
	}
	source := map[pb.IntakeSource]string{pb.IntakeSource_INTAKE_SOURCE_UI: "ui", pb.IntakeSource_INTAKE_SOURCE_API: "api"}[m.GetIntakeSource()]
	if !h.service.ValidID(m.GetCommandId()) || source == "" || m.PreparationInput == nil || len(m.GetPreparationInput()) == 0 || int64(len(m.GetPreparationInput())) > h.preparations.Limits().CommandBodyMaxBytes ||
		len(m.SubjectRefs) > 0 || len(m.ApiSubjectRefs) > 0 || m.ActivationId != nil || m.AuthorizedFundingRef != nil || m.OriginalSourceRevision != nil || m.ClientRequestId != nil || m.ClientSequence != nil || m.LocalCheckFixtureId != nil || m.PreparationOperationId != nil || m.BriefRef != nil || m.BriefRevision != nil {
		return nil, localError(storage.ErrInvalid)
	}
	p, err := h.preparations.Admit(ctx, auth.principal, m.GetCommandId(), source, m.GetPreparationInput())
	if p.ID != "" {
		auth.operationID = p.ID
	}
	if err != nil {
		if errors.Is(err, storage.ErrRevision) || errors.Is(err, storage.ErrLeaseLost) {
			return nil, connect.NewError(connect.CodeUnavailable, errors.New("DEPENDENCY_UNAVAILABLE"))
		}
		return nil, localError(err)
	}
	return connect.NewResponse(&pb.AdmitOperationResponse{OperationId: proto.String(p.ID), OperationRevision: proto.Uint64(uint64(p.Revision)), AcceptedAt: timestamppb.New(p.AcceptedAt), QueueExpiresAt: timestamppb.New(p.QueueExpiresAt), RequestDigest: proto.String(p.RequestDigest), Existing: proto.Bool(p.Existing),
		FundingAuthority: pb.FundingAuthority_FUNDING_AUTHORITY_TEST.Enum(), AuthorizedFundingRef: proto.String(p.AuthorizedFundingRef)}), nil
}

func (h controlHandler) RecordPreparationAnswers(ctx context.Context, request *connect.Request[pb.RecordPreparationAnswersRequest]) (*connect.Response[pb.RecordPreparationAnswersResponse], error) {
	m := request.Msg
	if m == nil || len(m.ProtoReflect().GetUnknown()) != 0 {
		return nil, localError(storage.ErrInvalid)
	}
	auth, err := commandPrincipal(ctx, m.Context, rpc.ControlServiceRecordPreparationAnswersProcedure)
	if err != nil {
		return nil, err
	}
	if h.preparations == nil {
		return nil, connect.NewError(connect.CodeFailedPrecondition, errors.New("QUALIFICATION_REQUIRED"))
	}
	questionSet, ok := refFromProto(m.QuestionSetRef)
	if !h.service.ValidID(m.GetOperationId()) || !h.service.ValidID(m.GetCommandId()) || m.GetExpectedOperationRevision() < 1 || !ok || questionSet.Kind != "evidence" || m.AnswerSetRef != nil || m.AnswerSet == nil || len(m.GetAnswerSet()) == 0 || int64(len(m.GetAnswerSet())) > h.preparations.Limits().CommandBodyMaxBytes {
		return nil, localError(storage.ErrInvalid)
	}
	auth.operationID = m.GetOperationId()
	outcome, err := h.preparations.Answers(ctx, auth.principal, m.GetOperationId(), m.GetCommandId(), m.GetExpectedOperationRevision(), questionSet, m.GetAnswerSet())
	if err != nil && !errors.Is(err, preparation.ErrStale) {
		return nil, localError(err)
	}
	decision := map[storage.AnswerDecision]pb.AcceptResultDecision{storage.AnswerAccepted: pb.AcceptResultDecision_ACCEPT_RESULT_DECISION_ACCEPTED, storage.AnswerDuplicate: pb.AcceptResultDecision_ACCEPT_RESULT_DECISION_DUPLICATE, storage.AnswerStale: pb.AcceptResultDecision_ACCEPT_RESULT_DECISION_STALE}[outcome.Decision]
	response := &pb.RecordPreparationAnswersResponse{Decision: decision.Enum(), OperationRevision: proto.Uint64(uint64(outcome.Preparation.Revision)), QuestionSetRevision: proto.Uint64(uint64(outcome.Round.QuestionSetRevision))}
	if outcome.Round.AnswerSetRef != nil {
		response.AcceptedAnswerSetRef = refToProto(*outcome.Round.AnswerSetRef)
	}
	if outcome.Round.DeliveryIntentID != "" {
		response.DeliveryIntentId = proto.String(outcome.Round.DeliveryIntentID)
	}
	if outcome.EventSeq > 0 {
		response.EventSeq = proto.Uint64(uint64(outcome.EventSeq))
	}
	if outcome.Decision == storage.AnswerStale {
		response.ReasonCode = proto.String("ANSWER_SET_ALREADY_ACCEPTED")
	}
	return connect.NewResponse(response), nil
}

func (h controlHandler) RecordPreparationRound(ctx context.Context, request *connect.Request[pb.RecordPreparationRoundRequest]) (*connect.Response[pb.RecordPreparationRoundResponse], error) {
	m := request.Msg
	if m == nil || len(m.ProtoReflect().GetUnknown()) != 0 {
		return nil, localError(storage.ErrInvalid)
	}
	auth, ok := ctx.Value(disclosureContextKey{}).(*disclosureRequest)
	if !ok || !auth.workflow {
		return nil, connect.NewError(connect.CodePermissionDenied, errors.New("PERMISSION_DENIED"))
	}
	if h.preparations == nil {
		return nil, connect.NewError(connect.CodeFailedPrecondition, errors.New("QUALIFICATION_REQUIRED"))
	}
	claimed := m.Context
	if claimed == nil || len(claimed.ProtoReflect().GetUnknown()) != 0 || claimed.GetServiceIdentity() != preparation.WorkflowService || claimed.GetDestinationMethod() != rpc.ControlServiceRecordPreparationRoundProcedure || claimed.ActorId != nil || claimed.TenantId != nil || claimed.ActorRole != nil {
		return nil, connect.NewError(connect.CodePermissionDenied, errors.New("PERMISSION_DENIED"))
	}
	if !h.service.ValidID(m.GetOperationId()) || m.GetExecutionGeneration() < 1 || m.GetRound() < 1 || (m.QuestionSet == nil) == (m.Brief == nil) {
		return nil, localError(storage.ErrInvalid)
	}
	auth.operationID = m.GetOperationId()
	outcome, err := h.preparations.Round(ctx, m.GetOperationId(), m.GetExecutionGeneration(), m.GetRound(), m.QuestionSet, m.Brief)
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			return nil, connect.NewError(connect.CodeNotFound, errors.New("NOT_FOUND"))
		}
		return nil, localError(err)
	}
	p := outcome.Preparation
	decision := pb.AcceptResultDecision_ACCEPT_RESULT_DECISION_ACCEPTED
	if outcome.Duplicate {
		decision = pb.AcceptResultDecision_ACCEPT_RESULT_DECISION_DUPLICATE
	}
	response := &pb.RecordPreparationRoundResponse{Decision: decision.Enum(), OperationRevision: proto.Uint64(uint64(p.Revision)), BusinessStage: businessStages[p.Stage].Enum(), EventSeq: proto.Uint64(uint64(p.NextSequence - 1))}
	if outcome.Round.QuestionSetRef != nil {
		response.QuestionSetRef = refToProto(*outcome.Round.QuestionSetRef)
		response.QuestionSetRevision = proto.Uint64(uint64(outcome.Round.QuestionSetRevision))
		response.QuestionSetExpiresAt = timestamppb.New(*outcome.Round.ExpiresAt)
	}
	if outcome.Round.BriefRef != nil {
		response.BriefRef = refToProto(*outcome.Round.BriefRef)
		response.BriefRevision = proto.Uint64(uint64(outcome.Round.BriefRevision))
	}
	return connect.NewResponse(response), nil
}

func (h controlHandler) ReadPreparation(ctx context.Context, request *connect.Request[pb.ReadPreparationRequest]) (*connect.Response[pb.ReadPreparationResponse], error) {
	m := request.Msg
	if m == nil || len(m.ProtoReflect().GetUnknown()) != 0 || !h.service.ValidID(m.GetOperationId()) {
		return nil, localError(storage.ErrInvalid)
	}
	auth, ok := ctx.Value(disclosureContextKey{}).(*disclosureRequest)
	if !ok {
		return nil, connect.NewError(connect.CodeUnauthenticated, errors.New("UNAUTHENTICATED"))
	}
	if h.preparations == nil {
		return nil, connect.NewError(connect.CodeFailedPrecondition, errors.New("QUALIFICATION_REQUIRED"))
	}
	auth.operationID = m.GetOperationId()
	claimed := m.Context
	if auth.workflow {
		if claimed == nil || len(claimed.ProtoReflect().GetUnknown()) != 0 || claimed.GetServiceIdentity() != preparation.WorkflowService || claimed.GetDestinationMethod() != rpc.ControlServiceReadPreparationProcedure || claimed.ActorId != nil || claimed.TenantId != nil || claimed.ActorRole != nil {
			return nil, connect.NewError(connect.CodePermissionDenied, errors.New("PERMISSION_DENIED"))
		}
	} else {
		if _, err := commandPrincipal(ctx, claimed, rpc.ControlServiceReadPreparationProcedure); err != nil {
			return nil, err
		}
		if err := h.preparations.AuthorizeRead(ctx, auth.principal, m.GetOperationId()); err != nil {
			auth.denied = true
			return nil, localError(err)
		}
	}
	detail, err := h.preparations.Read(ctx, m.GetOperationId())
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			return nil, connect.NewError(connect.CodeNotFound, errors.New("NOT_FOUND"))
		}
		return nil, localError(err)
	}
	p := detail.Preparation
	response := &pb.ReadPreparationResponse{Status: publicStatus(p.Status).Enum(), BusinessStage: businessStages[p.Stage].Enum(), OperationRevision: proto.Uint64(uint64(p.Revision)), ExecutionGeneration: proto.Uint64(uint64(p.ExecutionGeneration)), Preparation: projectionToProto(p), Input: detail.Input}
	if detail.QuestionSet != nil {
		response.QuestionSet = detail.QuestionSet
	}
	if detail.AcceptedAnswerSet != nil {
		response.AcceptedAnswerSet = detail.AcceptedAnswerSet
	}
	if detail.Brief != nil {
		response.Brief = detail.Brief
	}
	return connect.NewResponse(response), nil
}
