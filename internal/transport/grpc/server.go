package grpc

import (
	"context"
	"errors"
	"fmt"
	"github.com/prometheus/client_golang/prometheus"
	"net"
	"strings"
	"sync/atomic"
	"time"

	"buf.build/go/protovalidate"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	controlv1 "github.com/ancyloce/anvilkit-agent-contracts/go/anvilkit/control/v1"
	"github.com/ancyloce/anvilkit-agent-control/internal/application"
	"github.com/ancyloce/anvilkit-agent-control/internal/transport/identity"
)

// Server owns the grpc-go server, its interceptors and the health service.
type Server struct {
	grpc    *grpc.Server
	health  *health.Server
	listen  string
	serving atomic.Bool
	cap     *capacity
}

// Identity is the listener's transport: with a Reloader the server is
// mTLS-only (TLS 1.3, client certificates required and verified against the
// current bundle, every RPC authorized by the peer's workload identity
// under TrustDomain through Policy); without one it is the DEVELOPMENT_ONLY
// plaintext listener that authorizes nothing (the configuration loader
// admits that only under development.enabled).
type Identity struct {
	Reloader         *identity.Reloader
	TrustDomain      string
	Policy           identity.Policy
	MaxConnectionAge time.Duration
}

// Dependencies are the use cases and transport collaborators of the server.
type Dependencies struct {
	Ops          *application.Operations
	Exec         *application.Execution
	Dispatch     *application.Dispatch
	Effects      *application.Effects
	Recovery     *application.Recovery
	Artifacts    *application.Artifacts
	Preparations *application.Preparations
	Generations  *application.Generations
	Grants       *application.GrantPolicies
	Previews     *application.Previews
	Releases     *application.Releases
	// Finance administers prices, pools and cost caps; Authorization
	// serves AuthorizationService (P2.1). Nil leaves them unregistered.
	Finance       *application.Finance
	Authorization *application.Authorization
	// Notifier wakes event streams when an event of their operation commits
	// (P1.4); nil means the streams poll at the fallback interval only.
	Notifier application.EventNotifier
	// DefaultDeadline bounds unary RPCs without a caller deadline; zero disables it.
	DefaultDeadline time.Duration
	// MaxConnectionAge makes clients re-balance across replicas (zero: 1h).
	MaxConnectionAge time.Duration
}

// NewServer builds the plaintext development listener (tests).
func NewServer(listen string, controlCapacity, executionCapacity int, ops *application.Operations, exec *application.Execution, dispatch *application.Dispatch, effects *application.Effects, recovery *application.Recovery, artifacts *application.Artifacts, preparations *application.Preparations, generations *application.Generations, grants *application.GrantPolicies, previews *application.Previews, releases *application.Releases, extra ...grpc.ServerOption) (*Server, error) {
	return NewServerWithIdentity(listen, nil, Capacity{Control: controlCapacity, Execution: executionCapacity, Streams: 256, StreamPerTenant: 32},
		Dependencies{Ops: ops, Exec: exec, Dispatch: dispatch, Effects: effects, Recovery: recovery, Artifacts: artifacts, Preparations: preparations, Generations: generations, Grants: grants, Previews: previews, Releases: releases}, extra...)
}

// NewServerWithIdentity is NewServer with the listener identity; id nil is
// the plaintext development listener.
func NewServerWithIdentity(listen string, id *Identity, capacity Capacity, deps Dependencies, extra ...grpc.ServerOption) (*Server, error) {
	validator, err := protovalidate.New()
	if err != nil {
		return nil, err
	}
	ops, exec, dispatch, effects, recovery, artifacts, preparations, generations, grants, previews, releases := deps.Ops, deps.Exec, deps.Dispatch, deps.Effects, deps.Recovery, deps.Artifacts, deps.Preparations, deps.Generations, deps.Grants, deps.Previews, deps.Releases
	cap := newCapacity(capacity)
	unary := []grpc.UnaryServerInterceptor{cap.unary(), defaultDeadline(deps.DefaultDeadline), validateUnary(validator)}
	stream := []grpc.StreamServerInterceptor{cap.stream()}
	var authz *identity.Authorizer
	var opts []grpc.ServerOption
	if id != nil {
		if id.Reloader == nil {
			return nil, errors.New("identity: mtls needs loaded material")
		}
		policy := id.Policy
		if deps.Finance == nil {
			policy = withoutService(policy, "PriceAdminService", "BudgetPoolService")
		}
		if deps.Authorization == nil {
			policy = withoutService(policy, "AuthorizationService")
		}
		authz, err = identity.NewAuthorizer(id.TrustDomain, policy)
		if err != nil {
			return nil, err
		}
		// Authorization runs first: an unauthorized caller never reaches the
		// capacity pools or validation.
		unary = append([]grpc.UnaryServerInterceptor{authz.Unary()}, unary...)
		stream = append([]grpc.StreamServerInterceptor{authz.Stream()}, stream...)
		opts = append(opts, grpc.Creds(identity.NewServerCredentials(id.Reloader)))
		if deps.MaxConnectionAge <= 0 {
			deps.MaxConnectionAge = id.MaxConnectionAge
		}
	}
	// Connections age out on every listener so clients re-balance across
	// replicas behind the ClusterIP (P1.4).
	age := deps.MaxConnectionAge
	if age <= 0 {
		age = time.Hour
	}
	opts = append(opts, grpc.KeepaliveParams(keepalive.ServerParameters{MaxConnectionAge: age, MaxConnectionAgeGrace: 30 * time.Second}))
	opts = append(opts, grpc.ChainUnaryInterceptor(unary...), grpc.ChainStreamInterceptor(stream...))
	// The extra options (observability) register their interceptors first,
	// so metrics and spans are outermost and count a refusal by the
	// capacity pools or the authorizer as well (P1.5, CTL-INF-13).
	s := grpc.NewServer(append(append([]grpc.ServerOption(nil), extra...), opts...)...)
	h := health.NewServer()
	grpc_health_v1.RegisterHealthServer(s, h)
	controlv1.RegisterOperationServiceServer(s, &operationServer{ops: ops, preparations: preparations, streams: cap.tenants, notifier: deps.Notifier})
	controlv1.RegisterPreparationServiceServer(s, &preparationServer{preparations: preparations})
	controlv1.RegisterGenerationServiceServer(s, &generationServer{generations: generations})
	controlv1.RegisterExecutionServiceServer(s, &executionServer{exec: exec})
	controlv1.RegisterDispatchServiceServer(s, &dispatchServer{dispatch: dispatch})
	controlv1.RegisterEffectServiceServer(s, &effectServer{effects: effects})
	controlv1.RegisterRecoveryServiceServer(s, &recoveryServer{recovery: recovery})
	controlv1.RegisterArtifactServiceServer(s, &artifactServer{artifacts: artifacts})
	controlv1.RegisterGrantPolicyServiceServer(s, &grantPolicyServer{grants: grants})
	controlv1.RegisterPreviewServiceServer(s, &previewServer{previews: previews})
	controlv1.RegisterReleaseServiceServer(s, &releaseServer{releases: releases})
	if deps.Finance != nil {
		controlv1.RegisterPriceAdminServiceServer(s, &priceAdminServer{finance: deps.Finance})
		controlv1.RegisterBudgetPoolServiceServer(s, &budgetPoolServer{finance: deps.Finance})
	}
	if deps.Authorization != nil {
		controlv1.RegisterAuthorizationServiceServer(s, &authorizationServer{authorization: deps.Authorization})
	}
	if authz != nil {
		if err := authz.Check(s); err != nil {
			return nil, err
		}
	}
	return &Server{grpc: s, health: h, listen: listen, cap: cap}, nil
}

// withoutService drops the entries of services the server does not register
// (a build without Finance or Authorization), so the completeness check
// still names every other stale entry.
func withoutService(p identity.Policy, services ...string) identity.Policy {
	out := identity.Policy{}
	for method, allowed := range p {
		drop := false
		for _, svc := range services {
			if strings.HasPrefix(method, "/anvilkit.control.v1."+svc+"/") {
				drop = true
			}
		}
		if !drop {
			out[method] = allowed
		}
	}
	return out
}

// Ready reports whether the listener serves (the /readyz answer).
func (s *Server) Ready() bool { return s.serving.Load() }

// validateUnary enforces the protovalidate rules of every request before
// any handler runs (A04).
func validateUnary(v protovalidate.Validator) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if msg, ok := req.(proto.Message); ok {
			if err := v.Validate(msg); err != nil {
				return nil, status.Error(codes.InvalidArgument, "INVALID_ARGUMENT: "+err.Error())
			}
		}
		return handler(ctx, req)
	}
}

// Start listens and serves in the background; readiness turns SERVING.
func (s *Server) Start() (net.Addr, error) {
	ln, err := net.Listen("tcp", s.listen)
	if err != nil {
		return nil, fmt.Errorf("listen %s: %w", s.listen, err)
	}
	go func() { _ = s.grpc.Serve(ln) }()
	s.health.SetServingStatus("", grpc_health_v1.HealthCheckResponse_SERVING)
	s.serving.Store(true)
	return ln.Addr(), nil
}

// Stop withdraws readiness, drains in-flight calls within the timeout and
// then forces the stop (DD-09 §3).
func (s *Server) Stop(timeout time.Duration) {
	s.serving.Store(false)
	s.health.SetServingStatus("", grpc_health_v1.HealthCheckResponse_NOT_SERVING)
	done := make(chan struct{})
	go func() { s.grpc.GracefulStop(); close(done) }()
	select {
	case <-done:
	case <-time.After(timeout):
		s.grpc.Stop()
	}
}

// Collectors are the capacity pool gauges and the CAPACITY_EXHAUSTED refusal
// counter of this server (P3.6); the caller registers them.
func (s *Server) Collectors() []prometheus.Collector {
	return s.cap.collectors()
}
