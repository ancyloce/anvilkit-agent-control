package grpc

import (
	"context"

	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	controlv1 "github.com/ancyloce/anvilkit-agent-contracts/go/anvilkit/control/v1"
	"github.com/ancyloce/anvilkit-agent-control/internal/application"
	"github.com/ancyloce/anvilkit-agent-control/internal/domain"
)

// recoveryServer adapts anvilkit.control.v1.RecoveryService to the
// rollback-window reconciliation (DD-02 §5). It runs on the reserved
// control pool: recovery is an operator path that generation traffic must
// never starve.
type recoveryServer struct {
	controlv1.UnimplementedRecoveryServiceServer
	recovery *application.Recovery
}

var recoveryPhaseToProto = map[domain.RecoveryPhase]controlv1.RecoveryPhase{
	domain.RecoveryFenced: controlv1.RecoveryPhase_RECOVERY_PHASE_FENCED, domain.RecoveryEnumerating: controlv1.RecoveryPhase_RECOVERY_PHASE_ENUMERATING,
	domain.RecoveryReconciling: controlv1.RecoveryPhase_RECOVERY_PHASE_RECONCILING, domain.RecoveryRestricted: controlv1.RecoveryPhase_RECOVERY_PHASE_RESTRICTED,
	domain.RecoveryReopened: controlv1.RecoveryPhase_RECOVERY_PHASE_REOPENED,
}

var findingStatusToProto = map[domain.FindingStatus]controlv1.FindingStatus{
	domain.FindingPresent: controlv1.FindingStatus_FINDING_STATUS_PRESENT, domain.FindingMissing: controlv1.FindingStatus_FINDING_STATUS_MISSING,
	domain.FindingRestored: controlv1.FindingStatus_FINDING_STATUS_RESTORED, domain.FindingResolved: controlv1.FindingStatus_FINDING_STATUS_RESOLVED,
	domain.FindingUnresolved: controlv1.FindingStatus_FINDING_STATUS_UNRESOLVED, domain.FindingDisposed: controlv1.FindingStatus_FINDING_STATUS_DISPOSED,
	domain.FindingOutsideScope: controlv1.FindingStatus_FINDING_STATUS_OUTSIDE_SCOPE,
}

var findingStatusFromProto = map[controlv1.FindingStatus]domain.FindingStatus{
	controlv1.FindingStatus_FINDING_STATUS_PRESENT: domain.FindingPresent, controlv1.FindingStatus_FINDING_STATUS_MISSING: domain.FindingMissing,
	controlv1.FindingStatus_FINDING_STATUS_RESTORED: domain.FindingRestored, controlv1.FindingStatus_FINDING_STATUS_RESOLVED: domain.FindingResolved,
	controlv1.FindingStatus_FINDING_STATUS_UNRESOLVED: domain.FindingUnresolved, controlv1.FindingStatus_FINDING_STATUS_DISPOSED: domain.FindingDisposed,
	controlv1.FindingStatus_FINDING_STATUS_OUTSIDE_SCOPE: domain.FindingOutsideScope,
}

var decisionFromProto = map[controlv1.DispositionDecision]domain.DispositionDecision{
	controlv1.DispositionDecision_DISPOSITION_DECISION_CONFIRM_NOT_SENT: domain.DisposeConfirmNotSent, controlv1.DispositionDecision_DISPOSITION_DECISION_RESOLVE_SUCCEEDED: domain.DisposeResolveSucceeded,
	controlv1.DispositionDecision_DISPOSITION_DECISION_RESOLVE_FAILED: domain.DisposeResolveFailed, controlv1.DispositionDecision_DISPOSITION_DECISION_RETAIN_EXPOSURE: domain.DisposeRetainExposure,
}

var decisionToProto = map[domain.DispositionDecision]controlv1.DispositionDecision{
	domain.DisposeConfirmNotSent: controlv1.DispositionDecision_DISPOSITION_DECISION_CONFIRM_NOT_SENT, domain.DisposeResolveSucceeded: controlv1.DispositionDecision_DISPOSITION_DECISION_RESOLVE_SUCCEEDED,
	domain.DisposeResolveFailed: controlv1.DispositionDecision_DISPOSITION_DECISION_RESOLVE_FAILED, domain.DisposeRetainExposure: controlv1.DispositionDecision_DISPOSITION_DECISION_RETAIN_EXPOSURE,
}

var instancePhaseFromProto = map[controlv1.InstancePhase]domain.InstancePhase{
	controlv1.InstancePhase_INSTANCE_PHASE_PENDING: domain.PhasePending, controlv1.InstancePhase_INSTANCE_PHASE_RUNNING: domain.PhaseRunning,
	controlv1.InstancePhase_INSTANCE_PHASE_SUCCEEDED: domain.PhaseSucceeded, controlv1.InstancePhase_INSTANCE_PHASE_FAILED: domain.PhaseFailed,
	controlv1.InstancePhase_INSTANCE_PHASE_UNKNOWN: domain.PhaseUnknown,
}

func toRun(run *domain.RecoveryRun, progress []*domain.RecoveryProgress, unsettled uint64) *controlv1.RecoveryRun {
	out := &controlv1.RecoveryRun{
		RunId: run.ID, ScopeKey: run.ScopeKey, RecoveryEpoch: domain.Revision(run.RecoveryEpoch).String(), Phase: recoveryPhaseToProto[run.Phase],
		WindowStart: timestamppb.New(run.WindowStart), WindowEnd: timestamppb.New(run.WindowEnd), ClockUncertainty: durationpb.New(run.Skew),
		FencedOperations: domain.Revision(run.FencedCount).String(), UnsettledFindings: domain.Revision(unsettled).String(), CreatedAt: timestamppb.New(run.CreatedAt),
	}
	for _, p := range progress {
		out.Progress = append(out.Progress, toProgress(p))
	}
	if run.ReopenedAt != nil {
		out.ReopenedAt = timestamppb.New(*run.ReopenedAt)
	}
	return out
}

func toProgress(p *domain.RecoveryProgress) *controlv1.ClassProgress {
	return &controlv1.ClassProgress{Class: p.Class, Cursor: p.Cursor, Complete: p.Complete, Seen: domain.Revision(p.Seen).String()}
}

func toFinding(f *domain.RecoveryFinding) *controlv1.Finding {
	return &controlv1.Finding{
		FindingId: f.ID, RunId: f.RunID, Class: f.Class, ObligationId: f.ObligationID, TenantId: f.TenantID, InventoryVersion: f.InventoryVersion,
		RecordedAt: timestamppb.New(f.RecordedAt), Status: findingStatusToProto[f.Status], Outcome: f.Outcome, EvidenceRef: f.EvidenceRef, Detail: f.Detail, LaunchKey: f.LaunchKey,
	}
}

func (s *recoveryServer) BeginRecovery(ctx context.Context, req *controlv1.BeginRecoveryRequest) (*controlv1.BeginRecoveryResponse, error) {
	cmd, err := commandIdentity(req.GetCommand())
	if err != nil {
		return nil, toStatus(err)
	}
	run, existing, err := s.recovery.Begin(ctx, cmd, req.GetScopeKey(), req.GetWindowStart().AsTime(), req.GetWindowEnd().AsTime(), req.GetClockUncertainty().AsDuration(), req.GetReason())
	if err != nil {
		return nil, toStatus(err)
	}
	full, progress, unsettled, err := s.recovery.Get(ctx, run.ID)
	if err != nil {
		return nil, toStatus(err)
	}
	return &controlv1.BeginRecoveryResponse{Run: toRun(full, progress, unsettled), Existing: existing}, nil
}

func (s *recoveryServer) GetRecovery(ctx context.Context, req *controlv1.GetRecoveryRequest) (*controlv1.GetRecoveryResponse, error) {
	run, progress, unsettled, err := s.recovery.Get(ctx, req.GetRunId())
	if err != nil {
		return nil, toStatus(err)
	}
	return &controlv1.GetRecoveryResponse{Run: toRun(run, progress, unsettled)}, nil
}

func (s *recoveryServer) EnumerateInventory(ctx context.Context, req *controlv1.EnumerateInventoryRequest) (*controlv1.EnumerateInventoryResponse, error) {
	p, err := s.recovery.Enumerate(ctx, req.GetRunId(), req.GetClass())
	if err != nil {
		return nil, toStatus(err)
	}
	return &controlv1.EnumerateInventoryResponse{Progress: toProgress(p)}, nil
}

func (s *recoveryServer) ListFindings(ctx context.Context, req *controlv1.ListFindingsRequest) (*controlv1.ListFindingsResponse, error) {
	findings, next, complete, err := s.recovery.Findings(ctx, req.GetRunId(), findingStatusFromProto[req.GetStatus()], req.GetCursor(), int(req.GetLimit()))
	if err != nil {
		return nil, toStatus(err)
	}
	out := &controlv1.ListFindingsResponse{NextCursor: next, Complete: complete}
	for _, f := range findings {
		out.Findings = append(out.Findings, toFinding(f))
	}
	return out, nil
}

func (s *recoveryServer) ReconcileFinding(ctx context.Context, req *controlv1.ReconcileFindingRequest) (*controlv1.ReconcileFindingResponse, error) {
	f, err := s.recovery.Reconcile(ctx, req.GetRunId(), req.GetFindingId())
	if err != nil {
		return nil, toStatus(err)
	}
	return &controlv1.ReconcileFindingResponse{Finding: toFinding(f)}, nil
}

func (s *recoveryServer) RecordLaunchOutcome(ctx context.Context, req *controlv1.RecordLaunchOutcomeRequest) (*controlv1.RecordLaunchOutcomeResponse, error) {
	outcome := application.LaunchOutcome{JobUID: req.GetJobUid(), Stopped: req.GetStopped()}
	for _, p := range req.GetPods() {
		phase, ok := instancePhaseFromProto[p.GetPhase()]
		if !ok {
			phase = domain.PhaseUnknown
		}
		outcome.Pods = append(outcome.Pods, application.PodEvidence{PodUID: p.GetPodUid(), Phase: phase, ExitCode: p.ExitCode, ObservedAt: p.GetObservedAt().AsTime()})
	}
	f, err := s.recovery.RecordOutcome(ctx, req.GetRunId(), req.GetFindingId(), outcome)
	if err != nil {
		return nil, toStatus(err)
	}
	return &controlv1.RecordLaunchOutcomeResponse{Finding: toFinding(f)}, nil
}

func (s *recoveryServer) EvaluateRecovery(ctx context.Context, req *controlv1.EvaluateRecoveryRequest) (*controlv1.EvaluateRecoveryResponse, error) {
	if _, _, err := s.recovery.Evaluate(ctx, req.GetRunId()); err != nil {
		return nil, toStatus(err)
	}
	run, progress, unsettled, err := s.recovery.Get(ctx, req.GetRunId())
	if err != nil {
		return nil, toStatus(err)
	}
	return &controlv1.EvaluateRecoveryResponse{Run: toRun(run, progress, unsettled)}, nil
}

func (s *recoveryServer) DisposeObligation(ctx context.Context, req *controlv1.DisposeObligationRequest) (*controlv1.DisposeObligationResponse, error) {
	cmd, err := commandIdentity(req.GetCommand())
	if err != nil {
		return nil, toStatus(err)
	}
	digest, err := domain.ParseDigest(req.GetEvidenceDigest())
	if err != nil {
		return nil, toStatus(err)
	}
	epoch, err := domain.ParseRevision(req.GetRecoveryEpoch())
	if err != nil {
		return nil, toStatus(err)
	}
	d, existing, err := s.recovery.Dispose(ctx, cmd, application.DispositionRequest{
		Class: req.GetClass(), ObligationID: req.GetObligationId(), Decision: decisionFromProto[req.GetDecision()], EvidenceRef: req.GetEvidenceRef(),
		EvidenceDigest: digest, RecoveryEpoch: uint64(epoch), RunID: req.GetRunId(), Reason: req.GetReason(),
	})
	if err != nil {
		return nil, toStatus(err)
	}
	return &controlv1.DisposeObligationResponse{Disposition: &controlv1.Disposition{
		DispositionId: d.ID, Class: d.Class, ObligationId: d.ObligationID, Decision: decisionToProto[d.Decision], EvidenceRef: d.EvidenceRef, EvidenceDigest: string(d.EvidenceDigest),
		RecoveryEpoch: domain.Revision(d.RecoveryEpoch).String(), RunId: optString(d.RunID), DecidedAt: timestamppb.New(d.DecidedAt),
	}, Existing: existing}, nil
}
