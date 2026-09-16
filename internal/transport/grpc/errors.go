// Package grpc is Control's grpc-go transport (A03). It maps proto messages
// to application calls and domain errors to gRPC codes per contracts.md §4.
package grpc

import (
	"context"
	"errors"
	"log/slog"
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/ancyloce/anvilkit-agent-control/internal/application"
	"github.com/ancyloce/anvilkit-agent-control/internal/domain"
)

// toStatus maps domain errors to the public error codes. The message is a
// controlled reason; never a body, secret or source text.
func toStatus(err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, domain.ErrNotFound):
		return status.Error(codes.NotFound, "NOT_FOUND")
	case errors.Is(err, domain.ErrIdempotencyConflict):
		return status.Error(codes.Aborted, "IDEMPOTENCY_CONFLICT: "+err.Error())
	case errors.Is(err, domain.ErrRevisionConflict):
		return status.Error(codes.Aborted, "REVISION_CONFLICT: "+err.Error())
	case errors.Is(err, domain.ErrStaleExecution):
		return status.Error(codes.FailedPrecondition, "STALE_EXECUTION: "+err.Error())
	case errors.Is(err, domain.ErrProfileUnqualified):
		return status.Error(codes.FailedPrecondition, "PROFILE_UNQUALIFIED: "+err.Error())
	case errors.Is(err, domain.ErrInvalid):
		return status.Error(codes.InvalidArgument, "INVALID_ARGUMENT: "+err.Error())
	case errors.Is(err, domain.ErrEffectUncertain):
		return status.Error(codes.Unavailable, "EFFECT_UNCERTAIN: "+err.Error())
	case errors.Is(err, domain.ErrCapacityExhausted):
		return status.Error(codes.ResourceExhausted, "CAPACITY_EXHAUSTED")
	case errors.Is(err, domain.ErrBudgetExhausted):
		return status.Error(codes.ResourceExhausted, "BUDGET_EXHAUSTED: "+err.Error())
	case errors.Is(err, domain.ErrForbidden):
		return status.Error(codes.PermissionDenied, "FORBIDDEN: "+err.Error())
	case errors.Is(err, domain.ErrEvidenceInsufficient):
		// An accepted business outcome (contracts.md §4), never a retryable
		// 503: the call stays as it is until real evidence exists.
		return status.Error(codes.FailedPrecondition, "EFFECT_UNCERTAIN: "+err.Error())
	case errors.Is(err, domain.ErrIntegrity):
		return status.Error(codes.DataLoss, "RESULT_INTEGRITY: "+err.Error())
	case errors.Is(err, domain.ErrUnavailable):
		return status.Error(codes.Unavailable, "DEPENDENCY_UNAVAILABLE: "+err.Error())
	case errors.Is(err, application.ErrDuplicateKey):
		return status.Error(codes.Aborted, "IDEMPOTENCY_CONFLICT: concurrent duplicate")
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		return status.FromContextError(err).Err()
	default:
		if _, ok := status.FromError(err); ok {
			return err
		}
		slog.Default().Error("unmapped control error", "error", err)
		return status.Error(codes.Unavailable, "DEPENDENCY_UNAVAILABLE")
	}
}

// capacity reserves control capacity so cancel/query are never starved by
// execution traffic (DD-02 §2): two independent bounded pools selected by
// method (A14 explicit concurrency limits).
type capacity struct {
	control   chan struct{}
	execution chan struct{}
}

func newCapacity(control, execution int) *capacity {
	return &capacity{control: make(chan struct{}, control), execution: make(chan struct{}, execution)}
}

// pool routes execution-side traffic (attempts, launches, results, the
// per-call dispatch admission and business-write permits) to the execution
// pool so operator commands, queries and recovery keep their reserved
// capacity.
func (c *capacity) pool(method string) chan struct{} {
	for _, prefix := range []string{"/anvilkit.control.v1.ExecutionService/", "/anvilkit.control.v1.DispatchService/", "/anvilkit.control.v1.EffectService/", "/anvilkit.control.v1.ArtifactService/"} {
		if strings.HasPrefix(method, prefix) {
			return c.execution
		}
	}
	return c.control
}

func (c *capacity) acquire(ctx context.Context, method string) (func(), error) {
	p := c.pool(method)
	select {
	case p <- struct{}{}:
		return func() { <-p }, nil
	default:
		return nil, status.Error(codes.ResourceExhausted, "CAPACITY_EXHAUSTED")
	}
}

func (c *capacity) unary() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		release, err := c.acquire(ctx, info.FullMethod)
		if err != nil {
			return nil, err
		}
		defer release()
		return handler(ctx, req)
	}
}

func (c *capacity) stream() grpc.StreamServerInterceptor {
	return func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		release, err := c.acquire(ss.Context(), info.FullMethod)
		if err != nil {
			return err
		}
		defer release()
		return handler(srv, ss)
	}
}
