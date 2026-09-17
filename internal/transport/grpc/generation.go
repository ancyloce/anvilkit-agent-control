package grpc

import (
	"context"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	controlv1 "github.com/ancyloce/anvilkit-agent-contracts/go/anvilkit/control/v1"
	"github.com/ancyloce/anvilkit-agent-control/internal/application"
	"github.com/ancyloce/anvilkit-agent-control/internal/domain"
)

type generationServer struct {
	controlv1.UnimplementedGenerationServiceServer
	generations *application.Generations
}

var permitStateToProto = map[domain.PermitState]controlv1.PermitState{
	domain.PermitActive: controlv1.PermitState_PERMIT_STATE_ACTIVE, domain.PermitReleased: controlv1.PermitState_PERMIT_STATE_RELEASED,
	domain.PermitExpiredUnconfirmed: controlv1.PermitState_PERMIT_STATE_EXPIRED_UNCONFIRMED,
}

var leaseStateToProto = map[domain.LeaseState]controlv1.LeaseState{
	domain.LeaseNone: controlv1.LeaseState_LEASE_STATE_NONE, domain.LeaseHeld: controlv1.LeaseState_LEASE_STATE_HELD,
	domain.LeaseLost: controlv1.LeaseState_LEASE_STATE_LOST, domain.LeaseReleased: controlv1.LeaseState_LEASE_STATE_RELEASED,
}

var leaseStateFromProto = map[controlv1.LeaseState]domain.LeaseState{
	controlv1.LeaseState_LEASE_STATE_HELD: domain.LeaseHeld, controlv1.LeaseState_LEASE_STATE_LOST: domain.LeaseLost, controlv1.LeaseState_LEASE_STATE_RELEASED: domain.LeaseReleased,
}

var outcomeFromProto = map[controlv1.CommandOutcome]domain.CommandOutcome{
	controlv1.CommandOutcome_COMMAND_OUTCOME_PENDING: domain.OutcomePending, controlv1.CommandOutcome_COMMAND_OUTCOME_APPLIED: domain.OutcomeApplied,
	controlv1.CommandOutcome_COMMAND_OUTCOME_BLOCKED: domain.OutcomeBlocked, controlv1.CommandOutcome_COMMAND_OUTCOME_REJECTED: domain.OutcomeRejected,
}

func toPermit(p *domain.Permit) *controlv1.ExecutionPermit {
	if p == nil {
		return nil
	}
	out := &controlv1.ExecutionPermit{PermitId: p.ID, PoolId: p.PoolID, OperationId: p.OwnerID, FenceEpoch: domain.Revision(p.FenceEpoch).String(), State: permitStateToProto[p.State], GrantedAt: timestamppb.New(p.GrantedAt)}
	if p.ReleasedAt != nil {
		out.ReleasedAt = timestamppb.New(*p.ReleasedAt)
	}
	return out
}

func optTime(t *time.Time) *timestamppb.Timestamp {
	if t == nil {
		return nil
	}
	return timestamppb.New(*t)
}

func toLease(l domain.LeaseRecord) *controlv1.LeaseRecord {
	return &controlv1.LeaseRecord{State: leaseStateToProto[l.State], LeaseId: l.LeaseID, Fence: domain.Revision(l.Fence).String(), ExpiresAt: optTime(l.ExpiresAt), Occurrence: domain.Revision(l.Occurrence).String()}
}

func (s *generationServer) GetGeneration(ctx context.Context, req *controlv1.GetGenerationRequest) (*controlv1.GetGenerationResponse, error) {
	v, err := s.generations.Get(ctx, req.GetTenantId(), req.GetOperationId())
	if err != nil {
		return nil, toStatus(err)
	}
	op := v.Operation
	out := &controlv1.Generation{
		OperationId: op.ID, TenantId: op.TenantID, Permit: toPermit(v.Permit), ActiveDeadline: optTime(op.ActiveDeadline), QueueDeadline: timestamppb.New(op.Deadline),
		Lease: toLease(op.Lease), DefinitionActivation: op.DefinitionActivation, MaxRepairs: domain.Revision(v.Profile.MaxRepairs).String(),
		CodegenProfileId: v.Profile.CodegenProfileID, ValidatorProfileId: v.Profile.ValidatorProfileID, CandidateEffectId: optString(op.CandidateEffectID),
		Subject: &controlv1.OperationSubject{ProfileId: op.Subject.ProfileID, SubjectDigest: string(op.Subject.SubjectDigest), BriefId: optString(op.Subject.BriefID), SourceRevision: optString(op.Subject.SourceRevision)},
		ActorId: op.ActorID,
	}
	if v.Brief != nil {
		out.Brief = toBrief(v.Brief)
	}
	if v.Funding != nil {
		out.Funding = &controlv1.Money{Currency: v.Funding.Amount.Currency, Amount: v.Funding.Amount.AmountString()}
	}
	return &controlv1.GetGenerationResponse{Generation: out}, nil
}

func (s *generationServer) RequestExecutionPermit(ctx context.Context, req *controlv1.RequestExecutionPermitRequest) (*controlv1.RequestExecutionPermitResponse, error) {
	cmd, err := commandIdentity(req.GetCommand())
	if err != nil {
		return nil, toStatus(err)
	}
	a, err := s.generations.RequestExecutionPermit(ctx, cmd, req.GetOperationId())
	if err != nil {
		return nil, toStatus(err)
	}
	return &controlv1.RequestExecutionPermitResponse{
		Granted: a.Granted, Permit: toPermit(a.Permit), ActiveDeadline: optTime(a.ActiveDeadline), QueueDeadline: timestamppb.New(a.QueueDeadline),
		QueuedAhead: domain.Revision(uint64(a.QueuedAhead)).String(),
	}, nil
}

func (s *generationServer) RecordLease(ctx context.Context, req *controlv1.RecordLeaseRequest) (*controlv1.RecordLeaseResponse, error) {
	cmd, err := commandIdentity(req.GetCommand())
	if err != nil {
		return nil, toStatus(err)
	}
	occurrence, err := domain.ParseRevision(req.GetOccurrence())
	if err != nil {
		return nil, toStatus(err)
	}
	var fence uint64
	if req.GetFence() != "" {
		f, err := domain.ParseRevision(req.GetFence())
		if err != nil {
			return nil, toStatus(err)
		}
		fence = uint64(f)
	}
	var expires *time.Time
	if req.ExpiresAt != nil {
		t := req.GetExpiresAt().AsTime()
		expires = &t
	}
	op, existing, err := s.generations.RecordLease(ctx, cmd, req.GetOperationId(), uint64(occurrence), leaseStateFromProto[req.GetState()], req.GetLeaseId(), fence, expires)
	if err != nil {
		return nil, toStatus(err)
	}
	return &controlv1.RecordLeaseResponse{Lease: toLease(op.Lease), Operation: toView(op), Existing: existing}, nil
}

func (s *generationServer) RecordFunding(ctx context.Context, req *controlv1.RecordFundingRequest) (*controlv1.RecordFundingResponse, error) {
	cmd, err := commandIdentity(req.GetCommand())
	if err != nil {
		return nil, toStatus(err)
	}
	f, existing, err := s.generations.RecordFunding(ctx, cmd, req.GetOperationId())
	if err != nil {
		return nil, toStatus(err)
	}
	op, err := s.generations.Operation(ctx, cmd.TenantID, req.GetOperationId())
	if err != nil {
		return nil, toStatus(err)
	}
	return &controlv1.RecordFundingResponse{Amount: &controlv1.Money{Currency: f.Amount.Currency, Amount: f.Amount.AmountString()}, Existing: existing, Operation: toView(op)}, nil
}

func (s *generationServer) RecordCommandRelay(ctx context.Context, req *controlv1.RecordCommandRelayRequest) (*controlv1.RecordCommandRelayResponse, error) {
	c, err := s.generations.RecordCommandRelay(ctx, req.GetTenantId(), req.GetCommandId(), outcomeFromProto[req.GetOutcome()], req.GetReasonCode())
	if err != nil {
		return nil, toStatus(err)
	}
	return &controlv1.RecordCommandRelayResponse{Receipt: toReceipt(c)}, nil
}
