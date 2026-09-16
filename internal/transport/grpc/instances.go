package grpc

import (
	"context"

	controlv1 "github.com/ancyloce/anvilkit-agent-contracts/go/anvilkit/control/v1"
	"github.com/ancyloce/anvilkit-agent-control/internal/domain"
)

// GetInstance is the trusted access sidecar's read of its own registration
// (P09): backend, launch key and Pod UID name the evidence the launcher
// recorded; the answer carries the instance, its attempt, the operation
// as of the same read and the scope.
func (s *executionServer) GetInstance(ctx context.Context, req *controlv1.GetInstanceRequest) (*controlv1.GetInstanceResponse, error) {
	scope, err := s.exec.GetInstance(ctx, req.GetBackend(), req.GetLaunchKey(), req.GetPodUid())
	if err != nil {
		return nil, toStatus(err)
	}
	return &controlv1.GetInstanceResponse{
		Instance: toInstance(scope.Instance), Attempt: toAttempt(scope.Attempt), Operation: toView(scope.Operation), TenantId: scope.TenantID,
		RecoveryEpoch: domain.Revision(scope.RecoveryEpoch).String(),
	}, nil
}
