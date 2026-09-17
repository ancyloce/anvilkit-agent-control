package grpc

import (
	"context"

	"google.golang.org/protobuf/types/known/timestamppb"

	controlv1 "github.com/ancyloce/anvilkit-agent-contracts/go/anvilkit/control/v1"
	"github.com/ancyloce/anvilkit-agent-control/internal/application"
	"github.com/ancyloce/anvilkit-agent-control/internal/domain"
)

type executionServer struct {
	controlv1.UnimplementedExecutionServiceServer
	exec *application.Execution
}

var attemptStateToProto = map[domain.AttemptState]controlv1.AttemptState{
	domain.AttemptOpen: controlv1.AttemptState_ATTEMPT_STATE_OPEN, domain.AttemptLaunchPrepared: controlv1.AttemptState_ATTEMPT_STATE_LAUNCH_PREPARED,
	domain.AttemptRunning: controlv1.AttemptState_ATTEMPT_STATE_RUNNING, domain.AttemptResultAccepted: controlv1.AttemptState_ATTEMPT_STATE_RESULT_ACCEPTED,
	domain.AttemptClosed: controlv1.AttemptState_ATTEMPT_STATE_CLOSED,
}

var attemptOutcomeToProto = map[domain.AttemptOutcome]controlv1.AttemptOutcome{
	domain.OutcomeCompleted: controlv1.AttemptOutcome_ATTEMPT_OUTCOME_COMPLETED, domain.OutcomeFailed: controlv1.AttemptOutcome_ATTEMPT_OUTCOME_FAILED,
	domain.OutcomeInfrastructureFailed: controlv1.AttemptOutcome_ATTEMPT_OUTCOME_INFRASTRUCTURE_FAILED, domain.OutcomeCanceled: controlv1.AttemptOutcome_ATTEMPT_OUTCOME_CANCELED,
	domain.OutcomeUnknown: controlv1.AttemptOutcome_ATTEMPT_OUTCOME_UNKNOWN,
}

var attemptOutcomeFromProto = map[controlv1.AttemptOutcome]domain.AttemptOutcome{
	controlv1.AttemptOutcome_ATTEMPT_OUTCOME_COMPLETED: domain.OutcomeCompleted, controlv1.AttemptOutcome_ATTEMPT_OUTCOME_FAILED: domain.OutcomeFailed,
	controlv1.AttemptOutcome_ATTEMPT_OUTCOME_INFRASTRUCTURE_FAILED: domain.OutcomeInfrastructureFailed, controlv1.AttemptOutcome_ATTEMPT_OUTCOME_CANCELED: domain.OutcomeCanceled,
	controlv1.AttemptOutcome_ATTEMPT_OUTCOME_UNKNOWN: domain.OutcomeUnknown,
}

var phaseToProto = map[domain.InstancePhase]controlv1.InstancePhase{
	domain.PhasePending: controlv1.InstancePhase_INSTANCE_PHASE_PENDING, domain.PhaseRunning: controlv1.InstancePhase_INSTANCE_PHASE_RUNNING,
	domain.PhaseSucceeded: controlv1.InstancePhase_INSTANCE_PHASE_SUCCEEDED, domain.PhaseFailed: controlv1.InstancePhase_INSTANCE_PHASE_FAILED,
	domain.PhaseUnknown: controlv1.InstancePhase_INSTANCE_PHASE_UNKNOWN,
}

var phaseFromProto = map[controlv1.InstancePhase]domain.InstancePhase{
	controlv1.InstancePhase_INSTANCE_PHASE_PENDING: domain.PhasePending, controlv1.InstancePhase_INSTANCE_PHASE_RUNNING: domain.PhaseRunning,
	controlv1.InstancePhase_INSTANCE_PHASE_SUCCEEDED: domain.PhaseSucceeded, controlv1.InstancePhase_INSTANCE_PHASE_FAILED: domain.PhaseFailed,
	controlv1.InstancePhase_INSTANCE_PHASE_UNKNOWN: domain.PhaseUnknown,
}

var verdictToProto = map[domain.Verdict]controlv1.Verdict{
	domain.VerdictCertified: controlv1.Verdict_VERDICT_CERTIFIED, domain.VerdictRepairable: controlv1.Verdict_VERDICT_REPAIRABLE,
	domain.VerdictInvalid: controlv1.Verdict_VERDICT_INVALID, domain.VerdictInfrastructureFailed: controlv1.Verdict_VERDICT_INFRASTRUCTURE_FAILED,
	domain.VerdictCanceled: controlv1.Verdict_VERDICT_CANCELED,
}

var verdictFromProto = map[controlv1.Verdict]domain.Verdict{
	controlv1.Verdict_VERDICT_CERTIFIED: domain.VerdictCertified, controlv1.Verdict_VERDICT_REPAIRABLE: domain.VerdictRepairable,
	controlv1.Verdict_VERDICT_INVALID: domain.VerdictInvalid, controlv1.Verdict_VERDICT_INFRASTRUCTURE_FAILED: domain.VerdictInfrastructureFailed,
	controlv1.Verdict_VERDICT_CANCELED: domain.VerdictCanceled,
}

func toAttempt(a *domain.Attempt) *controlv1.Attempt {
	return &controlv1.Attempt{
		AttemptId: a.ID, OperationId: a.OperationID, StepId: a.StepID, VisitOrdinal: domain.Revision(a.VisitOrdinal).String(),
		AttemptOrdinal: domain.Revision(a.AttemptOrdinal).String(), ProfileId: a.ProfileID, ExecutionEpoch: domain.Revision(a.ExecutionEpoch).String(),
		State: attemptStateToProto[a.State], Outcome: attemptOutcomeToProto[a.Outcome], Deadline: timestamppb.New(a.Deadline),
		AcceptedStageId: optString(a.AcceptedStageID), FailureCode: optString(a.FailureCode),
	}
}

func toInstance(i *domain.Instance) *controlv1.PhysicalInstance {
	out := &controlv1.PhysicalInstance{
		InstanceId: i.ID, AttemptId: i.AttemptID, LaunchKey: i.LaunchKey, Backend: i.Backend, JobUid: i.JobUID, PodUid: i.PodUID,
		ImageDigest: string(i.ImageDigest), LaunchEpoch: domain.Revision(i.LaunchEpoch).String(), Phase: phaseToProto[i.Phase], Current: i.Current,
		ExitCode: i.ExitCode, RegisteredAt: timestamppb.New(i.RegisteredAt),
	}
	if i.ObservedAt != nil {
		out.ObservedAt = timestamppb.New(*i.ObservedAt)
	}
	return out
}

func toStage(s *domain.Stage) *controlv1.AcceptedStage {
	out := &controlv1.AcceptedStage{
		StageId: s.ID, AttemptId: s.AttemptID, InstanceId: s.InstanceID, PhaseOrdinal: domain.Revision(s.PhaseOrdinal).String(),
		Verdict: verdictToProto[s.Verdict], ResultDigest: string(s.ResultDigest), ObserverIdentity: s.ObserverIdentity, ProfileId: s.ProfileID,
		AcceptedAt: timestamppb.New(s.AcceptedAt), FailureCode: optString(s.FailureCode),
		ExecutionEpoch: domain.Revision(s.ExecutionEpoch).String(), RecoveryEpoch: domain.Revision(s.RecoveryEpoch).String(),
	}
	for _, a := range s.Artifacts {
		out.Artifacts = append(out.Artifacts, &controlv1.ArtifactReference{Handle: a.Handle, Class: string(a.Class), Digest: string(a.Digest), SizeBytes: domain.Revision(a.SizeBytes).String(), TransferId: a.TransferID, ObjectVersion: a.ObjectVersion})
	}
	return out
}

func (s *executionServer) OpenAttempt(ctx context.Context, req *controlv1.OpenAttemptRequest) (*controlv1.OpenAttemptResponse, error) {
	cmd, err := commandIdentity(req.GetCommand())
	if err != nil {
		return nil, toStatus(err)
	}
	visit, err := domain.ParseRevision(req.GetVisitOrdinal())
	if err != nil {
		return nil, toStatus(err)
	}
	at, existing, err := s.exec.OpenAttempt(ctx, cmd, req.GetOperationId(), req.GetStepId(), uint64(visit), req.GetProfileId())
	if err != nil {
		return nil, toStatus(err)
	}
	return &controlv1.OpenAttemptResponse{Attempt: toAttempt(at), Existing: existing}, nil
}

func (s *executionServer) PrepareLaunch(ctx context.Context, req *controlv1.PrepareLaunchRequest) (*controlv1.PrepareLaunchResponse, error) {
	cmd, err := commandIdentity(req.GetCommand())
	if err != nil {
		return nil, toStatus(err)
	}
	image, err := domain.ParseDigest(req.GetImageDigest())
	if err != nil {
		return nil, toStatus(err)
	}
	l, existing, err := s.exec.PrepareLaunch(ctx, cmd, req.GetAttemptId(), req.GetLaunchKey(), req.GetBackend(), image, req.GetDeadline().AsTime())
	if err != nil {
		return nil, toStatus(err)
	}
	return &controlv1.PrepareLaunchResponse{
		LaunchId: l.ID, AttemptId: l.AttemptID, LaunchKey: l.LaunchKey, LaunchEpoch: domain.Revision(l.LaunchEpoch).String(),
		InventoryVersion: l.InventoryVersion, Existing: existing,
	}, nil
}

func (s *executionServer) RegisterInstance(ctx context.Context, req *controlv1.RegisterInstanceRequest) (*controlv1.RegisterInstanceResponse, error) {
	cmd, err := commandIdentity(req.GetCommand())
	if err != nil {
		return nil, toStatus(err)
	}
	image, err := domain.ParseDigest(req.GetImageDigest())
	if err != nil {
		return nil, toStatus(err)
	}
	inst, existing, err := s.exec.RegisterInstance(ctx, cmd, req.GetAttemptId(), req.GetLaunchKey(), req.GetBackend(), req.GetJobUid(), req.GetPodUid(), image)
	if err != nil {
		return nil, toStatus(err)
	}
	return &controlv1.RegisterInstanceResponse{Instance: toInstance(inst), Existing: existing}, nil
}

func (s *executionServer) ObserveInstance(ctx context.Context, req *controlv1.ObserveInstanceRequest) (*controlv1.ObserveInstanceResponse, error) {
	inst, err := s.exec.ObserveInstance(ctx, req.GetAttemptId(), req.GetInstanceId(), phaseFromProto[req.GetPhase()], req.ExitCode, req.GetObservedAt().AsTime())
	if err != nil {
		return nil, toStatus(err)
	}
	return &controlv1.ObserveInstanceResponse{Instance: toInstance(inst)}, nil
}

func (s *executionServer) AcceptResult(ctx context.Context, req *controlv1.AcceptResultRequest) (*controlv1.AcceptResultResponse, error) {
	cmd, err := commandIdentity(req.GetCommand())
	if err != nil {
		return nil, toStatus(err)
	}
	digest, err := domain.ParseDigest(req.GetResultDigest())
	if err != nil {
		return nil, toStatus(err)
	}
	var epoch *uint64
	if req.GetExecutionEpoch() != "" {
		rev, err := domain.ParseRevision(req.GetExecutionEpoch())
		if err != nil {
			return nil, toStatus(err)
		}
		v := uint64(rev)
		epoch = &v
	}
	st, existing, err := s.exec.AcceptResult(ctx, cmd, req.GetAttemptId(), req.GetInstanceId(), req.GetProfileId(), verdictFromProto[req.GetVerdict()], req.GetFailureCode(), digest, req.GetResultManifest(), req.GetObserverIdentity(), epoch)
	if err != nil {
		return nil, toStatus(err)
	}
	return &controlv1.AcceptResultResponse{Stage: toStage(st), Existing: existing}, nil
}

func (s *executionServer) GetAcceptedStage(ctx context.Context, req *controlv1.GetAcceptedStageRequest) (*controlv1.GetAcceptedStageResponse, error) {
	st, err := s.exec.GetAcceptedStage(ctx, req.GetAttemptId(), req.GetTenantId(), req.GetOperationId())
	if err != nil {
		return nil, toStatus(err)
	}
	return &controlv1.GetAcceptedStageResponse{Stage: toStage(st)}, nil
}

func (s *executionServer) CloseAttempt(ctx context.Context, req *controlv1.CloseAttemptRequest) (*controlv1.CloseAttemptResponse, error) {
	cmd, err := commandIdentity(req.GetCommand())
	if err != nil {
		return nil, toStatus(err)
	}
	at, op, existing, err := s.exec.CloseAttempt(ctx, cmd, req.GetAttemptId(), attemptOutcomeFromProto[req.GetOutcome()], cleanupFromProto[req.GetCleanup()], req.GetFailureCode())
	if err != nil {
		return nil, toStatus(err)
	}
	return &controlv1.CloseAttemptResponse{Attempt: toAttempt(at), Operation: toView(op), Existing: existing}, nil
}

var operationOutcomeFromProto = map[controlv1.OperationOutcome]domain.OperationOutcome{
	controlv1.OperationOutcome_OPERATION_OUTCOME_SUCCEEDED: domain.OperationSucceeded, controlv1.OperationOutcome_OPERATION_OUTCOME_FAILED: domain.OperationFailed,
	controlv1.OperationOutcome_OPERATION_OUTCOME_CANCELED: domain.OperationCanceled,
}

func (s *executionServer) SettleOperation(ctx context.Context, req *controlv1.SettleOperationRequest) (*controlv1.SettleOperationResponse, error) {
	cmd, err := commandIdentity(req.GetCommand())
	if err != nil {
		return nil, toStatus(err)
	}
	op, existing, err := s.exec.SettleOperation(ctx, cmd, req.GetOperationId(), operationOutcomeFromProto[req.GetOutcome()], req.GetFailureCode(), req.GetPhase())
	if err != nil {
		return nil, toStatus(err)
	}
	return &controlv1.SettleOperationResponse{Operation: toView(op), Existing: existing}, nil
}
