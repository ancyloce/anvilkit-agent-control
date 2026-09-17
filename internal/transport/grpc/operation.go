package grpc

import (
	"context"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	controlv1 "github.com/ancyloce/anvilkit-agent-contracts/go/anvilkit/control/v1"
	"github.com/ancyloce/anvilkit-agent-control/internal/application"
	"github.com/ancyloce/anvilkit-agent-control/internal/domain"
)

type operationServer struct {
	controlv1.UnimplementedOperationServiceServer
	ops          *application.Operations
	preparations *application.Preparations
}

// streamPoll bounds how long a stream waits before re-reading events; it
// is a transport detail, not a business clock.
const streamPoll = 250 * time.Millisecond

func commandIdentity(c *controlv1.CommandIdentity) (domain.CommandIdentity, error) {
	d, err := domain.ParseDigest(c.GetRequestDigest())
	if err != nil {
		return domain.CommandIdentity{}, err
	}
	return domain.CommandIdentity{TenantID: c.GetTenantId(), CommandID: c.GetCommandId(), ActorID: c.GetActorId(), RequestDigest: d}, nil
}

func scope(s *controlv1.Scope) domain.Scope {
	return domain.Scope{TenantID: s.GetTenantId(), ProjectID: s.GetProjectId(), ActorID: s.GetActorId()}
}

var kindFromProto = map[controlv1.OperationKind]domain.OperationKind{
	controlv1.OperationKind_OPERATION_KIND_PREPARATION:   domain.KindPreparation,
	controlv1.OperationKind_OPERATION_KIND_GENERATION:    domain.KindGeneration,
	controlv1.OperationKind_OPERATION_KIND_REFINEMENT:    domain.KindRefinement,
	controlv1.OperationKind_OPERATION_KIND_PREVIEW_BUILD: domain.KindPreviewBuild,
	controlv1.OperationKind_OPERATION_KIND_RELEASE:       domain.KindRelease,
	controlv1.OperationKind_OPERATION_KIND_LOCAL_CHECK:   domain.KindLocalCheck,
}

var kindToProto = map[domain.OperationKind]controlv1.OperationKind{
	domain.KindPreparation:  controlv1.OperationKind_OPERATION_KIND_PREPARATION,
	domain.KindGeneration:   controlv1.OperationKind_OPERATION_KIND_GENERATION,
	domain.KindRefinement:   controlv1.OperationKind_OPERATION_KIND_REFINEMENT,
	domain.KindPreviewBuild: controlv1.OperationKind_OPERATION_KIND_PREVIEW_BUILD,
	domain.KindRelease:      controlv1.OperationKind_OPERATION_KIND_RELEASE,
	domain.KindLocalCheck:   controlv1.OperationKind_OPERATION_KIND_LOCAL_CHECK,
}

var lifecycleToProto = map[domain.Lifecycle]controlv1.Lifecycle{
	domain.LifecycleAccepted: controlv1.Lifecycle_LIFECYCLE_ACCEPTED, domain.LifecycleRunning: controlv1.Lifecycle_LIFECYCLE_RUNNING,
	domain.LifecycleWaiting: controlv1.Lifecycle_LIFECYCLE_WAITING, domain.LifecycleReconciling: controlv1.Lifecycle_LIFECYCLE_RECONCILING,
	domain.LifecycleSuspended: controlv1.Lifecycle_LIFECYCLE_SUSPENDED, domain.LifecycleSucceeded: controlv1.Lifecycle_LIFECYCLE_SUCCEEDED,
	domain.LifecycleFailed: controlv1.Lifecycle_LIFECYCLE_FAILED, domain.LifecycleCanceled: controlv1.Lifecycle_LIFECYCLE_CANCELED,
}

var controlToProto = map[domain.ControlState]controlv1.ControlState{
	domain.ControlNone: controlv1.ControlState_CONTROL_STATE_NONE, domain.ControlCancelPending: controlv1.ControlState_CONTROL_STATE_CANCEL_PENDING,
	domain.ControlCancelApplied: controlv1.ControlState_CONTROL_STATE_CANCEL_APPLIED, domain.ControlHoldPending: controlv1.ControlState_CONTROL_STATE_HOLD_PENDING,
	domain.ControlHoldApplied: controlv1.ControlState_CONTROL_STATE_HOLD_APPLIED,
}

var cleanupToProto = map[domain.CleanupState]controlv1.CleanupState{
	domain.CleanupNotRequired: controlv1.CleanupState_CLEANUP_STATE_NOT_REQUIRED, domain.CleanupPending: controlv1.CleanupState_CLEANUP_STATE_PENDING,
	domain.CleanupComplete: controlv1.CleanupState_CLEANUP_STATE_COMPLETE, domain.CleanupUnknown: controlv1.CleanupState_CLEANUP_STATE_UNKNOWN,
}

var cleanupFromProto = map[controlv1.CleanupState]domain.CleanupState{
	controlv1.CleanupState_CLEANUP_STATE_NOT_REQUIRED: domain.CleanupNotRequired, controlv1.CleanupState_CLEANUP_STATE_PENDING: domain.CleanupPending,
	controlv1.CleanupState_CLEANUP_STATE_COMPLETE: domain.CleanupComplete, controlv1.CleanupState_CLEANUP_STATE_UNKNOWN: domain.CleanupUnknown,
}

var financeToProto = map[domain.FinanceState]controlv1.FinanceState{
	domain.FinanceNotFunded: controlv1.FinanceState_FINANCE_STATE_NOT_FUNDED, domain.FinanceFunded: controlv1.FinanceState_FINANCE_STATE_FUNDED,
	domain.FinanceSettling: controlv1.FinanceState_FINANCE_STATE_SETTLING, domain.FinanceSettled: controlv1.FinanceState_FINANCE_STATE_SETTLED,
	domain.FinanceExposureUnknown: controlv1.FinanceState_FINANCE_STATE_EXPOSURE_UNKNOWN,
}

var commandKindFromProto = map[controlv1.CommandKind]domain.CommandKind{
	controlv1.CommandKind_COMMAND_KIND_CANCEL: domain.CommandCancel, controlv1.CommandKind_COMMAND_KIND_HOLD: domain.CommandHold,
	controlv1.CommandKind_COMMAND_KIND_RESUME: domain.CommandResume, controlv1.CommandKind_COMMAND_KIND_CHANGE_DEFINITION: domain.CommandChangeDefinition,
}

var commandKindToProto = map[domain.CommandKind]controlv1.CommandKind{
	domain.CommandCancel: controlv1.CommandKind_COMMAND_KIND_CANCEL, domain.CommandHold: controlv1.CommandKind_COMMAND_KIND_HOLD,
	domain.CommandResume: controlv1.CommandKind_COMMAND_KIND_RESUME, domain.CommandChangeDefinition: controlv1.CommandKind_COMMAND_KIND_CHANGE_DEFINITION,
}

var outcomeToProto = map[domain.CommandOutcome]controlv1.CommandOutcome{
	domain.OutcomePending: controlv1.CommandOutcome_COMMAND_OUTCOME_PENDING, domain.OutcomeApplied: controlv1.CommandOutcome_COMMAND_OUTCOME_APPLIED,
	domain.OutcomeBlocked: controlv1.CommandOutcome_COMMAND_OUTCOME_BLOCKED, domain.OutcomeRejected: controlv1.CommandOutcome_COMMAND_OUTCOME_REJECTED,
}

func optString(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

func toView(o *domain.Operation) *controlv1.OperationView {
	subject := &controlv1.OperationSubject{ProfileId: o.Subject.ProfileID, SubjectDigest: string(o.Subject.SubjectDigest), BriefId: optString(o.Subject.BriefID), SourceRevision: optString(o.Subject.SourceRevision)}
	v := &controlv1.OperationView{
		OperationId: o.ID, TenantId: o.TenantID, ProjectId: o.ProjectID, ActorId: o.ActorID, Kind: kindToProto[o.Kind], Subject: subject,
		Lifecycle: lifecycleToProto[o.Lifecycle], Phase: o.Phase, Control: controlToProto[o.Control], Cleanup: cleanupToProto[o.Cleanup], Finance: financeToProto[o.Finance],
		Revision: o.Revision.String(), CoveredEventSeq: domain.Revision(o.CoveredEventSeq()).String(), ExecutionEpoch: domain.Revision(o.ExecutionEpoch).String(),
		CreatedAt: timestamppb.New(o.CreatedAt), UpdatedAt: timestamppb.New(o.UpdatedAt), Deadline: timestamppb.New(o.Deadline), FailureCode: optString(o.FailureCode),
	}
	if o.ActiveDeadline != nil {
		v.ActiveDeadline = timestamppb.New(*o.ActiveDeadline)
	}
	return v
}

// toClarification is the open question set on a waiting preparation's
// projection.
func toClarification(qs *domain.QuestionSet) *controlv1.Clarification {
	c := &controlv1.Clarification{
		QuestionSetId: qs.ID, QuestionSetRevision: domain.Revision(qs.Revision).String(), Round: domain.Revision(qs.Round).String(),
		AskedAt: timestamppb.New(qs.AskedAt), ExpiresAt: timestamppb.New(qs.ExpiresAt),
	}
	for _, q := range qs.Questions {
		c.Questions = append(c.Questions, &controlv1.Question{QuestionId: q.ID, Text: q.Text})
	}
	return c
}

// view projects the operation and, for a waiting preparation, its open
// question set.
func (s *operationServer) view(ctx context.Context, o *domain.Operation) (*controlv1.OperationView, error) {
	v := toView(o)
	if o.Kind == domain.KindPreparation && o.Lifecycle == domain.LifecycleWaiting && s.preparations != nil {
		prep, err := s.preparations.Get(ctx, o.TenantID, o.ID)
		if err != nil {
			return nil, err
		}
		for _, qs := range prep.QuestionSets {
			if qs.State == domain.QuestionSetOpen {
				v.Clarification = toClarification(qs)
			}
		}
	}
	return v, nil
}

func toEvent(ev domain.Event) *controlv1.OperationEvent {
	return &controlv1.OperationEvent{
		OperationId: ev.OperationID, EventSeq: domain.Revision(ev.EventSeq).String(), TransitionId: ev.TransitionID, EventType: ev.EventType,
		Revision: ev.Revision.String(), OccurredAt: timestamppb.New(ev.OccurredAt),
		Payload: &controlv1.OperationEvent_OperationChanged{OperationChanged: &controlv1.OperationChangedPayload{
			Lifecycle: lifecycleToProto[ev.Payload.Lifecycle], Phase: ev.Payload.Phase, Control: controlToProto[ev.Payload.Control],
			Cleanup: cleanupToProto[ev.Payload.Cleanup], Finance: financeToProto[ev.Payload.Finance], FailureCode: optString(ev.Payload.FailureCode),
		}},
	}
}

func toReceipt(c *domain.Command) *controlv1.CommandReceipt {
	r := &controlv1.CommandReceipt{
		CommandId: c.CommandID, OperationId: c.OperationID, Kind: commandKindToProto[c.Kind], Outcome: outcomeToProto[c.Outcome],
		OperationRevision: c.OperationRevision.String(), ReasonCode: optString(c.ReasonCode), AcceptedAt: timestamppb.New(c.AcceptedAt),
	}
	if c.SettledAt != nil {
		r.SettledAt = timestamppb.New(*c.SettledAt)
	}
	return r
}

func (s *operationServer) CreateOperation(ctx context.Context, req *controlv1.CreateOperationRequest) (*controlv1.CreateOperationResponse, error) {
	cmd, err := commandIdentity(req.GetCommand())
	if err != nil {
		return nil, toStatus(err)
	}
	digest, err := domain.ParseDigest(req.GetSubject().GetSubjectDigest())
	if err != nil {
		return nil, toStatus(err)
	}
	subject := domain.Subject{ProfileID: req.GetSubject().GetProfileId(), SubjectDigest: digest, BriefID: req.GetSubject().GetBriefId(), SourceRevision: req.GetSubject().GetSourceRevision()}
	var intake *domain.PreparationIntake
	if p := req.GetPreparation(); p != nil {
		in, err := toIntake(p)
		if err != nil {
			return nil, toStatus(err)
		}
		intake = &in
	}
	op, existing, err := s.ops.Create(ctx, cmd, scope(req.GetScope()), kindFromProto[req.GetKind()], subject, intake)
	if err != nil {
		return nil, toStatus(err)
	}
	return &controlv1.CreateOperationResponse{Operation: toView(op), Existing: existing}, nil
}

func toIntake(p *controlv1.PreparationIntake) (domain.PreparationIntake, error) {
	digest, err := domain.ParseDigest(p.GetPrompt().GetDigest())
	if err != nil {
		return domain.PreparationIntake{}, err
	}
	in := domain.PreparationIntake{Prompt: domain.ArtifactBinding{TransferID: p.GetPrompt().GetTransferId(), Digest: digest}}
	for _, r := range p.GetBrandReferences() {
		rev, err := domain.ParseRevision(r.GetRevision())
		if err != nil {
			return domain.PreparationIntake{}, err
		}
		in.BrandReferences = append(in.BrandReferences, domain.SourceReference{SourceID: r.GetSourceId(), Revision: uint64(rev)})
	}
	for _, r := range p.GetAssetReferences() {
		rev, err := domain.ParseRevision(r.GetRevision())
		if err != nil {
			return domain.PreparationIntake{}, err
		}
		in.AssetReferences = append(in.AssetReferences, domain.SourceReference{SourceID: r.GetSourceId(), Revision: uint64(rev)})
	}
	return in, nil
}

func (s *operationServer) GetOperation(ctx context.Context, req *controlv1.GetOperationRequest) (*controlv1.GetOperationResponse, error) {
	op, err := s.ops.Get(ctx, scope(req.GetScope()), req.GetOperationId())
	if err != nil {
		return nil, toStatus(err)
	}
	v, err := s.view(ctx, op)
	if err != nil {
		return nil, toStatus(err)
	}
	return &controlv1.GetOperationResponse{Operation: v}, nil
}

func parseAfter(s string) (uint64, error) {
	if s == "" {
		return 0, nil
	}
	r, err := domain.ParseRevision(s)
	return uint64(r), err
}

func (s *operationServer) ListOperationEvents(ctx context.Context, req *controlv1.ListOperationEventsRequest) (*controlv1.ListOperationEventsResponse, error) {
	after, err := parseAfter(req.GetAfterEventSeq())
	if err != nil {
		return nil, toStatus(err)
	}
	page, err := s.ops.ListEvents(ctx, scope(req.GetScope()), req.GetOperationId(), after, int(req.GetLimit()))
	if err != nil {
		return nil, toStatus(err)
	}
	out := &controlv1.ListOperationEventsResponse{CoveredEventSeq: domain.Revision(page.CoveredEventSeq).String(), ResetRequired: page.ResetRequired}
	for _, ev := range page.Events {
		out.Events = append(out.Events, toEvent(ev))
	}
	return out, nil
}

// StreamOperationEvents re-reads committed events after the cursor until
// the operation is terminal or the client goes away; it never buffers
// beyond one page so a slow consumer only delays itself.
func (s *operationServer) StreamOperationEvents(req *controlv1.StreamOperationEventsRequest, stream controlv1.OperationService_StreamOperationEventsServer) error {
	ctx := stream.Context()
	after, err := parseAfter(req.GetAfterEventSeq())
	if err != nil {
		return toStatus(err)
	}
	for {
		page, err := s.ops.ListEvents(ctx, scope(req.GetScope()), req.GetOperationId(), after, 200)
		if err != nil {
			return toStatus(err)
		}
		if page.ResetRequired {
			return toStatus(domain.ErrInvalid)
		}
		for _, ev := range page.Events {
			if err := stream.Send(&controlv1.StreamOperationEventsResponse{Event: toEvent(ev)}); err != nil {
				return err
			}
			after = ev.EventSeq
		}
		if page.Terminal && after >= page.CoveredEventSeq {
			return nil
		}
		select {
		case <-ctx.Done():
			return toStatus(ctx.Err())
		case <-time.After(streamPoll):
		}
	}
}

func (s *operationServer) SubmitCommand(ctx context.Context, req *controlv1.SubmitCommandRequest) (*controlv1.SubmitCommandResponse, error) {
	cmd, err := commandIdentity(req.GetCommand())
	if err != nil {
		return nil, toStatus(err)
	}
	expected, err := domain.ParseRevision(req.GetExpectedRevision())
	if err != nil {
		return nil, toStatus(err)
	}
	c, existing, err := s.ops.SubmitCommand(ctx, cmd, scope(req.GetScope()), req.GetOperationId(), commandKindFromProto[req.GetKind()], expected, req.GetTargetDefinitionActivation())
	if err != nil {
		return nil, toStatus(err)
	}
	return &controlv1.SubmitCommandResponse{Receipt: toReceipt(c), Existing: existing}, nil
}

func (s *operationServer) GetCommand(ctx context.Context, req *controlv1.GetCommandRequest) (*controlv1.GetCommandResponse, error) {
	c, err := s.ops.GetCommand(ctx, scope(req.GetScope()), req.GetOperationId(), req.GetCommandId())
	if err != nil {
		return nil, toStatus(err)
	}
	return &controlv1.GetCommandResponse{Receipt: toReceipt(c)}, nil
}
