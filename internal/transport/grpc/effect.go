package grpc

import (
	"context"

	"google.golang.org/protobuf/types/known/timestamppb"

	controlv1 "github.com/ancyloce/anvilkit-agent-contracts/go/anvilkit/control/v1"
	"github.com/ancyloce/anvilkit-agent-control/internal/application"
	"github.com/ancyloce/anvilkit-agent-control/internal/domain"
)

// effectServer adapts anvilkit.control.v1.EffectService to the guarded
// business-write permit chain (DD-06 §3); permitted=true is answered
// exactly once per mutation.
type effectServer struct {
	controlv1.UnimplementedEffectServiceServer
	effects *application.Effects
}

var effectKindFromProto = map[controlv1.EffectKind]domain.EffectKind{
	controlv1.EffectKind_EFFECT_KIND_BUSINESS_WRITE: domain.EffectBusinessWrite, controlv1.EffectKind_EFFECT_KIND_PUBLICATION: domain.EffectPublication,
	controlv1.EffectKind_EFFECT_KIND_ACTIVATION: domain.EffectActivation, controlv1.EffectKind_EFFECT_KIND_REVIEW: domain.EffectReview,
}

var effectKindToProto = map[domain.EffectKind]controlv1.EffectKind{
	domain.EffectBusinessWrite: controlv1.EffectKind_EFFECT_KIND_BUSINESS_WRITE, domain.EffectPublication: controlv1.EffectKind_EFFECT_KIND_PUBLICATION,
	domain.EffectActivation: controlv1.EffectKind_EFFECT_KIND_ACTIVATION, domain.EffectReview: controlv1.EffectKind_EFFECT_KIND_REVIEW,
}

var effectStateToProto = map[domain.EffectState]controlv1.EffectState{
	domain.EffectPrepared: controlv1.EffectState_EFFECT_STATE_PREPARED, domain.EffectPermitted: controlv1.EffectState_EFFECT_STATE_PERMITTED,
	domain.EffectSucceeded: controlv1.EffectState_EFFECT_STATE_SUCCEEDED, domain.EffectFailed: controlv1.EffectState_EFFECT_STATE_FAILED,
	domain.EffectUnknown: controlv1.EffectState_EFFECT_STATE_UNKNOWN, domain.EffectConfirmedNotSent: controlv1.EffectState_EFFECT_STATE_CONFIRMED_NOT_SENT,
	domain.EffectDenied: controlv1.EffectState_EFFECT_STATE_DENIED,
}

var effectOutcomeToProto = map[domain.EffectOutcome]controlv1.EffectOutcome{
	domain.EffectOutcomeSucceeded: controlv1.EffectOutcome_EFFECT_OUTCOME_SUCCEEDED, domain.EffectOutcomeFailed: controlv1.EffectOutcome_EFFECT_OUTCOME_FAILED,
	domain.EffectOutcomeUnknown: controlv1.EffectOutcome_EFFECT_OUTCOME_UNKNOWN,
}

var effectOutcomeFromProto = map[controlv1.EffectOutcome]domain.EffectOutcome{
	controlv1.EffectOutcome_EFFECT_OUTCOME_SUCCEEDED: domain.EffectOutcomeSucceeded, controlv1.EffectOutcome_EFFECT_OUTCOME_FAILED: domain.EffectOutcomeFailed,
	controlv1.EffectOutcome_EFFECT_OUTCOME_UNKNOWN: domain.EffectOutcomeUnknown,
}

func toEffect(e *domain.Effect) *controlv1.Effect {
	out := &controlv1.Effect{
		EffectId: e.ID, OperationId: e.OperationID, AttemptId: e.AttemptID, Kind: effectKindToProto[e.Kind], Occurrence: domain.Revision(e.Occurrence).String(), Owner: e.Owner,
		CanonicalSubject: e.CanonicalSubject, ExpectedRevision: optString(e.ExpectedRevision), State: effectStateToProto[e.State], Outcome: effectOutcomeToProto[e.Outcome],
		DenialCode: optString(e.DenialCode), OutcomeRef: optString(e.OutcomeRef), ExecutionEpoch: domain.Revision(e.ExecutionEpoch).String(), RecoveryEpoch: domain.Revision(e.RecoveryEpoch).String(),
		Deadline: timestamppb.New(e.Deadline), CreatedAt: timestamppb.New(e.CreatedAt),
	}
	if e.ObservedAt != nil {
		out.ObservedAt = timestamppb.New(*e.ObservedAt)
	}
	return out
}

func (s *effectServer) PrepareEffect(ctx context.Context, req *controlv1.PrepareEffectRequest) (*controlv1.PrepareEffectResponse, error) {
	cmd, err := commandIdentity(req.GetCommand())
	if err != nil {
		return nil, toStatus(err)
	}
	opID, attemptID, _, epoch, err := binding(req.GetBinding())
	if err != nil {
		return nil, toStatus(err)
	}
	occurrence, err := domain.ParseRevision(req.GetOccurrence())
	if err != nil {
		return nil, toStatus(err)
	}
	r := domain.EffectRequest{
		OperationID: opID, AttemptID: attemptID, Owner: req.GetOwner(), Kind: effectKindFromProto[req.GetKind()], Occurrence: uint64(occurrence), CanonicalSubject: req.GetCanonicalSubject(),
		ExpectedRevision: req.GetExpectedRevision(), RequestDigest: cmd.RequestDigest, ExecutionEpoch: epoch, Deadline: req.GetDeadline().AsTime(),
	}
	if lease := req.GetLease(); lease != nil {
		fence, err := domain.ParseRevision(lease.GetFence())
		if err != nil {
			return nil, toStatus(err)
		}
		r.LeaseID, r.LeaseFence = lease.GetLeaseId(), uint64(fence)
		if lease.GetExpiresAt() != nil {
			r.LeaseExpiresAt = lease.GetExpiresAt().AsTime()
		}
	}
	permit, err := s.effects.Prepare(ctx, cmd, r)
	if err != nil {
		return nil, toStatus(err)
	}
	return &controlv1.PrepareEffectResponse{Effect: toEffect(permit.Effect), Permitted: permit.Permitted, DenialCode: optString(permit.DenialCode)}, nil
}

func (s *effectServer) ObserveEffect(ctx context.Context, req *controlv1.ObserveEffectRequest) (*controlv1.ObserveEffectResponse, error) {
	sequence, err := domain.ParseRevision(req.GetSequence())
	if err != nil {
		return nil, toStatus(err)
	}
	e, existing, err := s.effects.Observe(ctx, req.GetTenantId(), req.GetEffectId(), req.GetSource(), uint64(sequence), effectOutcomeFromProto[req.GetOutcome()], req.GetReceiptDigest(), req.GetNativeReference(), req.GetObservedAt().AsTime())
	if err != nil {
		return nil, toStatus(err)
	}
	return &controlv1.ObserveEffectResponse{Effect: toEffect(e), Existing: existing}, nil
}

func (s *effectServer) GetEffect(ctx context.Context, req *controlv1.GetEffectRequest) (*controlv1.GetEffectResponse, error) {
	var occurrence uint64
	if req.GetOccurrence() != "" {
		v, err := domain.ParseRevision(req.GetOccurrence())
		if err != nil {
			return nil, toStatus(err)
		}
		occurrence = uint64(v)
	}
	e, err := s.effects.Get(ctx, req.GetTenantId(), req.GetEffectId(), req.GetOperationId(), effectKindFromProto[req.GetKind()], occurrence)
	if err != nil {
		return nil, toStatus(err)
	}
	return &controlv1.GetEffectResponse{Effect: toEffect(e)}, nil
}

func (s *effectServer) QueryEffectOutcome(ctx context.Context, req *controlv1.QueryEffectOutcomeRequest) (*controlv1.QueryEffectOutcomeResponse, error) {
	e, report, err := s.effects.QueryOriginal(ctx, req.GetTenantId(), req.GetEffectId())
	if err != nil {
		return nil, toStatus(err)
	}
	return &controlv1.QueryEffectOutcomeResponse{Effect: toEffect(e), Known: report.Known}, nil
}
