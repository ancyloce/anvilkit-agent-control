// Package grpc is Control's grpc-go transport (A03). It maps proto messages
// to application calls and domain errors to gRPC codes per contracts.md §4.
package grpc

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"time"

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

// Capacity bounds the server (P1.4): three independent pools (control,
// execution, streams), a reserve of control slots only SubmitCommand may
// take, and a per-tenant bound on event streams. The gRPC health service
// takes no slot.
type Capacity struct {
	Control         int
	Execution       int
	Streams         int
	StreamPerTenant int
	CommandReserve  int
}

type pool struct {
	mu      sync.Mutex
	limit   int
	reserve int
	held    int
}

func (p *pool) acquire(privileged bool) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	bound := p.limit
	if !privileged {
		bound -= p.reserve
	}
	if p.held >= bound {
		return false
	}
	p.held++
	return true
}

func (p *pool) release() {
	p.mu.Lock()
	p.held--
	p.mu.Unlock()
}

type capacity struct {
	control   *pool
	execution *pool
	streams   *pool
	tenants   *tenantStreams
}

func newCapacity(c Capacity) *capacity {
	if c.Streams <= 0 {
		c.Streams = 256
	}
	if c.StreamPerTenant <= 0 {
		c.StreamPerTenant = c.Streams
	}
	return &capacity{
		control:   &pool{limit: c.Control, reserve: c.CommandReserve},
		execution: &pool{limit: c.Execution},
		streams:   &pool{limit: c.Streams},
		tenants:   &tenantStreams{limit: c.StreamPerTenant, held: map[string]int{}},
	}
}

const (
	healthService   = "/grpc.health.v1.Health/"
	submitCommand   = "/anvilkit.control.v1.OperationService/SubmitCommand"
	streamEventsRPC = "/anvilkit.control.v1.OperationService/StreamOperationEvents"
)

var executionPrefixes = []string{"/anvilkit.control.v1.ExecutionService/", "/anvilkit.control.v1.DispatchService/", "/anvilkit.control.v1.EffectService/", "/anvilkit.control.v1.ArtifactService/", "/anvilkit.control.v1.GenerationService/"}

// pool routes a method to its pool; nil for the exempt health service.
func (c *capacity) pool(method string) *pool {
	if strings.HasPrefix(method, healthService) {
		return nil
	}
	if method == streamEventsRPC {
		return c.streams
	}
	for _, prefix := range executionPrefixes {
		if strings.HasPrefix(method, prefix) {
			return c.execution
		}
	}
	return c.control
}

func (c *capacity) acquire(method string) (func(), error) {
	p := c.pool(method)
	if p == nil {
		return func() {}, nil
	}
	if !p.acquire(method == submitCommand) {
		return nil, status.Error(codes.ResourceExhausted, "CAPACITY_EXHAUSTED")
	}
	return p.release, nil
}

func (c *capacity) unary() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		release, err := c.acquire(info.FullMethod)
		if err != nil {
			return nil, err
		}
		defer release()
		return handler(ctx, req)
	}
}

func (c *capacity) stream() grpc.StreamServerInterceptor {
	return func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		release, err := c.acquire(info.FullMethod)
		if err != nil {
			return err
		}
		defer release()
		return handler(srv, ss)
	}
}

// tenantStreams bounds the open event streams per tenant; the handler
// acquires it once it knows the tenant of the request.
type tenantStreams struct {
	mu    sync.Mutex
	limit int
	held  map[string]int
}

func (t *tenantStreams) acquire(tenant string) (func(), error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.held[tenant] >= t.limit {
		return nil, status.Error(codes.ResourceExhausted, "CAPACITY_EXHAUSTED: streams of the tenant")
	}
	t.held[tenant]++
	return func() {
		t.mu.Lock()
		t.held[tenant]--
		if t.held[tenant] == 0 {
			delete(t.held, tenant)
		}
		t.mu.Unlock()
	}, nil
}

// defaultDeadline bounds a unary RPC whose caller set no deadline (P1.4);
// a caller's own deadline is kept as it is.
func defaultDeadline(d time.Duration) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if _, ok := ctx.Deadline(); !ok && d > 0 {
			var cancel context.CancelFunc
			ctx, cancel = context.WithTimeout(ctx, d)
			defer cancel()
		}
		return handler(ctx, req)
	}
}
