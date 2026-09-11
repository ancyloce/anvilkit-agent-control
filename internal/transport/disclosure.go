package transport

import (
	"context"
	"errors"

	"connectrpc.com/connect"
	pb "github.com/ancyloce/anvilkit-agent-control/internal/contracts/controlv1"
	rpc "github.com/ancyloce/anvilkit-agent-control/internal/contracts/controlv1/controlv1connect"
	"github.com/ancyloce/anvilkit-agent-control/internal/disclosure"
	"github.com/ancyloce/anvilkit-agent-control/internal/localcheck"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type disclosureContextKey struct{}
type disclosureRequest struct {
	principal   disclosure.Principal
	operationID string
	denied      bool
}
type controlHandler struct {
	rpc.UnimplementedControlServiceHandler
	service     *disclosure.Service
	localChecks *localcheck.Service
}

func (h controlHandler) GetDisclosureAuthorization(ctx context.Context, request *connect.Request[pb.GetDisclosureAuthorizationRequest]) (*connect.Response[pb.GetDisclosureAuthorizationResponse], error) {
	auth, ok := ctx.Value(disclosureContextKey{}).(*disclosureRequest)
	if !ok {
		return nil, connect.NewError(connect.CodeUnauthenticated, errors.New("UNAUTHENTICATED"))
	}
	message := request.Msg
	if message != nil && h.service.ValidID(message.GetOperationId()) {
		auth.operationID = message.GetOperationId()
	}
	if message == nil || len(message.ProtoReflect().GetUnknown()) != 0 || message.Context == nil || len(message.Context.ProtoReflect().GetUnknown()) != 0 || !h.service.ValidID(message.GetOperationId()) {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("INVALID_ARGUMENT"))
	}
	claimed := message.Context
	if claimed.GetActorId() != auth.principal.ActorID || claimed.GetTenantId() != auth.principal.TenantID || claimed.GetServiceIdentity() != disclosure.APIService || claimed.GetDestinationMethod() != rpc.ControlServiceGetDisclosureAuthorizationProcedure || claimed.ActorRole != nil {
		return nil, connect.NewError(connect.CodePermissionDenied, errors.New("PERMISSION_DENIED"))
	}
	decision, err := h.service.Authorize(ctx, auth.principal, message.GetOperationId())
	if err != nil {
		code := connect.CodeUnavailable
		if errors.Is(ctx.Err(), context.Canceled) {
			code = connect.CodeCanceled
		}
		if errors.Is(ctx.Err(), context.DeadlineExceeded) {
			code = connect.CodeDeadlineExceeded
		}
		return nil, connect.NewError(code, errors.New("DEPENDENCY_UNAVAILABLE"))
	}
	if !decision.Allowed {
		auth.denied = true
		return connect.NewResponse(&pb.GetDisclosureAuthorizationResponse{Decision: pb.DisclosureDecision_DISCLOSURE_DECISION_DENY.Enum()}), nil
	}
	return connect.NewResponse(&pb.GetDisclosureAuthorizationResponse{
		Decision:           pb.DisclosureDecision_DISCLOSURE_DECISION_ALLOW.Enum(),
		BoundResourceScope: proto.String(decision.BoundResourceScope),
		FreshUntil:         timestamppb.New(decision.FreshUntil),
		EvidenceRevision:   proto.Uint64(decision.EvidenceRevision),
	}), nil
}
