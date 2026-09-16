package grpc

import (
	"context"

	"google.golang.org/protobuf/types/known/timestamppb"

	controlv1 "github.com/ancyloce/anvilkit-agent-contracts/go/anvilkit/control/v1"
	"github.com/ancyloce/anvilkit-agent-control/internal/application"
	"github.com/ancyloce/anvilkit-agent-control/internal/domain"
)

// dispatchServer adapts anvilkit.control.v1.DispatchService to the
// application's single-use admission (DD-02 §4). Money crosses the wire as
// canonical decimal strings, counters as canonical unsigned decimals; the
// response says dispatch_allowed=true exactly once per call.
type dispatchServer struct {
	controlv1.UnimplementedDispatchServiceServer
	dispatch *application.Dispatch
}

var dispatchStateToProto = map[domain.DispatchState]controlv1.DispatchState{
	domain.DispatchPrepared: controlv1.DispatchState_DISPATCH_STATE_PREPARED, domain.DispatchAuthorized: controlv1.DispatchState_DISPATCH_STATE_AUTHORIZED,
	domain.DispatchObserved: controlv1.DispatchState_DISPATCH_STATE_OBSERVED, domain.DispatchUnknown: controlv1.DispatchState_DISPATCH_STATE_UNKNOWN,
	domain.DispatchConfirmedNotSent: controlv1.DispatchState_DISPATCH_STATE_CONFIRMED_NOT_SENT, domain.DispatchDenied: controlv1.DispatchState_DISPATCH_STATE_DENIED,
}

var dispatchOutcomeToProto = map[domain.DispatchOutcome]controlv1.DispatchOutcome{
	domain.DispatchSucceeded: controlv1.DispatchOutcome_DISPATCH_OUTCOME_SUCCEEDED, domain.DispatchFailed: controlv1.DispatchOutcome_DISPATCH_OUTCOME_FAILED,
	domain.DispatchCanceled: controlv1.DispatchOutcome_DISPATCH_OUTCOME_CANCELED, domain.DispatchOutcomeUnknown: controlv1.DispatchOutcome_DISPATCH_OUTCOME_UNKNOWN,
}

var dispatchOutcomeFromProto = map[controlv1.DispatchOutcome]domain.DispatchOutcome{
	controlv1.DispatchOutcome_DISPATCH_OUTCOME_SUCCEEDED: domain.DispatchSucceeded, controlv1.DispatchOutcome_DISPATCH_OUTCOME_FAILED: domain.DispatchFailed,
	controlv1.DispatchOutcome_DISPATCH_OUTCOME_CANCELED: domain.DispatchCanceled, controlv1.DispatchOutcome_DISPATCH_OUTCOME_UNKNOWN: domain.DispatchOutcomeUnknown,
}

func money(m *controlv1.Money) (domain.Money, error) {
	return domain.ParseMoney(m.GetCurrency(), m.GetAmount())
}

func toMoney(m domain.Money) *controlv1.Money {
	return &controlv1.Money{Currency: m.Currency, Amount: m.AmountString()}
}

func toDispatch(d *domain.Dispatch) *controlv1.Dispatch {
	out := &controlv1.Dispatch{
		DispatchId: d.ID, CallId: d.CallID, Owner: d.Owner, State: dispatchStateToProto[d.State], Outcome: dispatchOutcomeToProto[d.Outcome],
		ReservedExposure: toMoney(d.Reserved), MeterRevision: d.MeterRevision, Deadline: timestamppb.New(d.Deadline), AdmittedAt: timestamppb.New(d.AdmittedAt),
	}
	if d.ObservedAt != nil {
		out.ObservedAt = timestamppb.New(*d.ObservedAt)
	}
	return out
}

func toAdmission(a application.Admission) *controlv1.Admission {
	return &controlv1.Admission{Dispatch: toDispatch(a.Dispatch), DispatchAllowed: a.Allowed, DenialCode: optString(a.DenialCode)}
}

func binding(b *controlv1.ExecutionBinding) (operationID, attemptID, instanceID string, epoch uint64, err error) {
	rev, err := domain.ParseRevision(b.GetExecutionEpoch())
	if err != nil {
		return "", "", "", 0, err
	}
	return b.GetOperationId(), b.GetAttemptId(), b.GetInstanceId(), uint64(rev), nil
}

func (s *dispatchServer) AdmitModel(ctx context.Context, req *controlv1.AdmitModelRequest) (*controlv1.AdmitModelResponse, error) {
	cmd, err := commandIdentity(req.GetCommand())
	if err != nil {
		return nil, toStatus(err)
	}
	opID, attemptID, instanceID, epoch, err := binding(req.GetBinding())
	if err != nil {
		return nil, toStatus(err)
	}
	exposure, err := money(req.GetMaxExposure())
	if err != nil {
		return nil, toStatus(err)
	}
	a, err := s.dispatch.Admit(ctx, cmd, domain.AdmissionRequest{
		Kind: domain.DispatchModel, CallID: req.GetCallId(), Owner: req.GetOwner(), RequestDigest: cmd.RequestDigest, OperationID: opID, AttemptID: attemptID,
		InstanceID: instanceID, ExecutionEpoch: epoch, RouteID: req.GetRouteId(), Provider: req.GetProvider(), Model: req.GetModel(), MaxExposure: exposure, Deadline: req.GetDeadline().AsTime(),
		SupersedesCallID: req.GetSupersedesCallId(), EvidenceRef: req.GetEvidenceRef(),
	})
	if err != nil {
		return nil, toStatus(err)
	}
	return &controlv1.AdmitModelResponse{Admission: toAdmission(a)}, nil
}

func (s *dispatchServer) AdmitTool(ctx context.Context, req *controlv1.AdmitToolRequest) (*controlv1.AdmitToolResponse, error) {
	cmd, err := commandIdentity(req.GetCommand())
	if err != nil {
		return nil, toStatus(err)
	}
	opID, attemptID, instanceID, epoch, err := binding(req.GetBinding())
	if err != nil {
		return nil, toStatus(err)
	}
	grantRevision, err := domain.ParseRevision(req.GetGrantRevision())
	if err != nil {
		return nil, toStatus(err)
	}
	exposure, err := money(req.GetMaxExposure())
	if err != nil {
		return nil, toStatus(err)
	}
	a, err := s.dispatch.Admit(ctx, cmd, domain.AdmissionRequest{
		Kind: domain.DispatchTool, CallID: req.GetCallId(), Owner: req.GetOwner(), RequestDigest: cmd.RequestDigest, OperationID: opID, AttemptID: attemptID,
		InstanceID: instanceID, ExecutionEpoch: epoch, RouteID: req.GetServerId() + "/" + req.GetMethod(), GrantID: req.GetGrantId(), GrantRevision: uint64(grantRevision),
		ServerID: req.GetServerId(), Method: req.GetMethod(), MaxExposure: exposure, Deadline: req.GetDeadline().AsTime(),
		SupersedesCallID: req.GetSupersedesCallId(), EvidenceRef: req.GetEvidenceRef(),
	})
	if err != nil {
		return nil, toStatus(err)
	}
	return &controlv1.AdmitToolResponse{Admission: toAdmission(a)}, nil
}

func counter(s string) (uint64, error) {
	if s == "" {
		return 0, nil
	}
	rev, err := domain.ParseRevision(s)
	if err != nil {
		return 0, err
	}
	if uint64(rev) > uint64(1<<63-1) {
		return 0, domain.ErrInvalid
	}
	return uint64(rev), nil
}

// ObserveDispatch keeps a report without cumulative_usage apart from one
// with explicit (possibly zero) counters: the message is absent, not
// zero-valued, and the application refuses to settle a definite outcome
// on it.
func (s *dispatchServer) ObserveDispatch(ctx context.Context, req *controlv1.ObserveDispatchRequest) (*controlv1.ObserveDispatchResponse, error) {
	sequence, err := domain.ParseRevision(req.GetSequence())
	if err != nil {
		return nil, toStatus(err)
	}
	var usage *domain.Usage
	if reported := req.GetCumulativeUsage(); reported != nil {
		usage = &domain.Usage{}
		for _, c := range []struct {
			raw string
			dst *uint64
		}{
			{reported.GetInputUnits(), &usage.Input}, {reported.GetOutputUnits(), &usage.Output},
			{reported.GetReasoningUnits(), &usage.Reasoning}, {reported.GetCachedInputUnits(), &usage.CachedInput},
		} {
			v, err := counter(c.raw)
			if err != nil {
				return nil, toStatus(err)
			}
			*c.dst = v
		}
	}
	d, existing, err := s.dispatch.Observe(ctx, req.GetDispatchId(), req.GetSource(), uint64(sequence), dispatchOutcomeFromProto[req.GetOutcome()], usage, req.GetNativeReference(), req.GetObservedAt().AsTime())
	if err != nil {
		return nil, toStatus(err)
	}
	return &controlv1.ObserveDispatchResponse{Dispatch: toDispatch(d), Existing: existing}, nil
}

func (s *dispatchServer) GetDispatch(ctx context.Context, req *controlv1.GetDispatchRequest) (*controlv1.GetDispatchResponse, error) {
	d, err := s.dispatch.Get(ctx, req.GetDispatchId(), req.GetOwner(), req.GetCallId())
	if err != nil {
		return nil, toStatus(err)
	}
	return &controlv1.GetDispatchResponse{Dispatch: toDispatch(d)}, nil
}

func (s *dispatchServer) ConfirmNotSent(ctx context.Context, req *controlv1.ConfirmNotSentRequest) (*controlv1.ConfirmNotSentResponse, error) {
	cmd, err := commandIdentity(req.GetCommand())
	if err != nil {
		return nil, toStatus(err)
	}
	digest, err := domain.ParseDigest(req.GetEvidenceDigest())
	if err != nil {
		return nil, toStatus(err)
	}
	d, _, err := s.dispatch.ConfirmNotSent(ctx, cmd, req.GetDispatchId(), req.GetEvidenceRef(), digest)
	if err != nil {
		return nil, toStatus(err)
	}
	return &controlv1.ConfirmNotSentResponse{Dispatch: toDispatch(d)}, nil
}
