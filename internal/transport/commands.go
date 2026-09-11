package transport

import (
	"context"
	"errors"
	"regexp"

	"connectrpc.com/connect"
	pb "github.com/ancyloce/anvilkit-agent-control/internal/contracts/controlv1"
	rpc "github.com/ancyloce/anvilkit-agent-control/internal/contracts/controlv1/controlv1connect"
	"github.com/ancyloce/anvilkit-agent-control/internal/disclosure"
	"github.com/ancyloce/anvilkit-agent-control/internal/localcheck"
	"github.com/ancyloce/anvilkit-agent-control/internal/storage"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var reasonCode = regexp.MustCompile(`^[A-Z][A-Z0-9_]{1,63}$`)

func commandPrincipal(ctx context.Context, claimed *pb.AuthenticatedContext, method string) (*disclosureRequest, error) {
	auth, ok := ctx.Value(disclosureContextKey{}).(*disclosureRequest)
	if !ok {
		return nil, connect.NewError(connect.CodeUnauthenticated, errors.New("UNAUTHENTICATED"))
	}
	if claimed == nil || len(claimed.ProtoReflect().GetUnknown()) != 0 {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("INVALID_ARGUMENT"))
	}
	if claimed.GetActorId() != auth.principal.ActorID || claimed.GetTenantId() != auth.principal.TenantID || claimed.GetServiceIdentity() != disclosure.APIService || claimed.GetDestinationMethod() != method || claimed.ActorRole != nil {
		return nil, connect.NewError(connect.CodePermissionDenied, errors.New("PERMISSION_DENIED"))
	}
	return auth, nil
}

func localError(err error) error {
	code, message := connect.CodeUnavailable, "DEPENDENCY_UNAVAILABLE"
	switch {
	case errors.Is(err, localcheck.ErrDenied):
		code, message = connect.CodePermissionDenied, "PERMISSION_DENIED"
	case errors.Is(err, storage.ErrInvalid):
		code, message = connect.CodeInvalidArgument, "INVALID_ARGUMENT"
	case errors.Is(err, storage.ErrConflict):
		code, message = connect.CodeAborted, "IDEMPOTENCY_CONFLICT"
	case errors.Is(err, storage.ErrRevision):
		code, message = connect.CodeAborted, "REVISION_CONFLICT"
	case errors.Is(err, storage.ErrNotFound):
		code, message = connect.CodePermissionDenied, "PERMISSION_DENIED"
	case errors.Is(err, storage.ErrLocalCapacity):
		code, message = connect.CodeResourceExhausted, "OVERLOADED"
	case errors.Is(err, context.Canceled):
		code, message = connect.CodeCanceled, "CANCELED"
	case errors.Is(err, context.DeadlineExceeded):
		code, message = connect.CodeDeadlineExceeded, "TRANSPORT_TIMEOUT"
	}
	return connect.NewError(code, errors.New(message))
}

func (h controlHandler) AdmitOperation(ctx context.Context, request *connect.Request[pb.AdmitOperationRequest]) (*connect.Response[pb.AdmitOperationResponse], error) {
	m := request.Msg
	if m == nil || len(m.ProtoReflect().GetUnknown()) != 0 {
		return nil, localError(storage.ErrInvalid)
	}
	auth, err := commandPrincipal(ctx, m.Context, rpc.ControlServiceAdmitOperationProcedure)
	if err != nil {
		return nil, err
	}
	if m.Kind == nil || m.IntakeSource == nil || m.CommandId == nil {
		return nil, localError(storage.ErrInvalid)
	}
	if h.localChecks == nil || m.GetKind() != pb.OperationKind_OPERATION_KIND_LOCAL_CHECK {
		return nil, connect.NewError(connect.CodeFailedPrecondition, errors.New("QUALIFICATION_REQUIRED"))
	}
	if !h.service.ValidID(m.GetCommandId()) || m.GetIntakeSource() != pb.IntakeSource_INTAKE_SOURCE_API || m.LocalCheckFixtureId == nil || len(m.SubjectRefs) > 0 || len(m.ApiSubjectRefs) > 0 || m.ActivationId != nil || m.AuthorizedFundingRef != nil || m.OriginalSourceRevision != nil || m.ClientRequestId != nil || m.ClientSequence != nil {
		return nil, localError(storage.ErrInvalid)
	}
	c, err := h.localChecks.Admit(ctx, auth.principal, m.GetCommandId(), m.GetLocalCheckFixtureId())
	if c.ID != "" {
		auth.operationID = c.ID
	}
	if err != nil {
		// AdmitOperation has no revision argument. Keep ABORTED exclusive to
		// a changed command digest; a failed intake recheck is unacknowledged.
		if errors.Is(err, storage.ErrRevision) || errors.Is(err, storage.ErrLeaseLost) {
			return nil, connect.NewError(connect.CodeUnavailable, errors.New("DEPENDENCY_UNAVAILABLE"))
		}
		return nil, localError(err)
	}
	return connect.NewResponse(&pb.AdmitOperationResponse{OperationId: proto.String(c.ID), OperationRevision: proto.Uint64(1), AcceptedAt: timestamppb.New(c.AcceptedAt), QueueExpiresAt: timestamppb.New(c.QueueExpiresAt), RequestDigest: proto.String(c.RequestDigest), Existing: proto.Bool(c.Existing)}), nil
}

func (h controlHandler) Cancel(ctx context.Context, request *connect.Request[pb.ControlCommandRequest]) (*connect.Response[pb.ControlCommandResponse], error) {
	m := request.Msg
	if m == nil || len(m.ProtoReflect().GetUnknown()) != 0 {
		return nil, localError(storage.ErrInvalid)
	}
	auth, err := commandPrincipal(ctx, m.Context, rpc.ControlServiceCancelProcedure)
	if err != nil {
		return nil, err
	}
	if h.localChecks == nil {
		return nil, connect.NewError(connect.CodeFailedPrecondition, errors.New("QUALIFICATION_REQUIRED"))
	}
	if !h.service.ValidID(m.GetOperationId()) || !h.service.ValidID(m.GetCommandId()) || m.GetExpectedOperationRevision() < 1 || (m.ReasonCode != nil && !reasonCode.MatchString(m.GetReasonCode())) {
		return nil, localError(storage.ErrInvalid)
	}
	auth.operationID = m.GetOperationId()
	c, coalesced, err := h.localChecks.Cancel(ctx, auth.principal, m.GetOperationId(), m.GetCommandId(), m.GetReasonCode(), m.GetExpectedOperationRevision())
	if err != nil {
		return nil, localError(err)
	}
	status := map[string]pb.PublicStatus{"pending": pb.PublicStatus_PUBLIC_STATUS_PENDING, "running": pb.PublicStatus_PUBLIC_STATUS_RUNNING, "blocked": pb.PublicStatus_PUBLIC_STATUS_BLOCKED, "succeeded": pb.PublicStatus_PUBLIC_STATUS_SUCCEEDED, "failed": pb.PublicStatus_PUBLIC_STATUS_FAILED, "canceled": pb.PublicStatus_PUBLIC_STATUS_CANCELED, "expired": pb.PublicStatus_PUBLIC_STATUS_EXPIRED}[c.Status]
	control := pb.ControlState_CONTROL_STATE_RUNNING
	if c.ControlState == "blocked" {
		control = pb.ControlState_CONTROL_STATE_BLOCKED
	}
	tracked := c.CancelCommandID
	if tracked == "" {
		tracked = m.GetCommandId()
	}
	response := &pb.ControlCommandResponse{OperationId: proto.String(c.ID), OperationRevision: proto.Uint64(uint64(c.Revision)), Status: status.Enum(), ControlState: control.Enum(), Coalesced: proto.Bool(coalesced), TrackedCommandId: proto.String(tracked)}
	if c.CancelAcknowledgedAt != nil {
		response.FenceAcknowledgedAt = timestamppb.New(*c.CancelAcknowledgedAt)
	}
	return connect.NewResponse(response), nil
}
