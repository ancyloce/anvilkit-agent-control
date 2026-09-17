package grpc

import (
	"context"
	"fmt"
	"net"
	"time"

	"buf.build/go/protovalidate"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	controlv1 "github.com/ancyloce/anvilkit-agent-contracts/go/anvilkit/control/v1"
	"github.com/ancyloce/anvilkit-agent-control/internal/application"
)

// Server owns the grpc-go server, its interceptors and the health service.
type Server struct {
	grpc   *grpc.Server
	health *health.Server
	listen string
}

func NewServer(listen string, controlCapacity, executionCapacity int, ops *application.Operations, exec *application.Execution, dispatch *application.Dispatch, effects *application.Effects, recovery *application.Recovery, artifacts *application.Artifacts, preparations *application.Preparations, generations *application.Generations) (*Server, error) {
	validator, err := protovalidate.New()
	if err != nil {
		return nil, err
	}
	cap := newCapacity(controlCapacity, executionCapacity)
	s := grpc.NewServer(
		grpc.ChainUnaryInterceptor(cap.unary(), validateUnary(validator)),
		grpc.ChainStreamInterceptor(cap.stream()),
	)
	h := health.NewServer()
	grpc_health_v1.RegisterHealthServer(s, h)
	controlv1.RegisterOperationServiceServer(s, &operationServer{ops: ops, preparations: preparations})
	controlv1.RegisterPreparationServiceServer(s, &preparationServer{preparations: preparations})
	controlv1.RegisterGenerationServiceServer(s, &generationServer{generations: generations})
	controlv1.RegisterExecutionServiceServer(s, &executionServer{exec: exec})
	controlv1.RegisterDispatchServiceServer(s, &dispatchServer{dispatch: dispatch})
	controlv1.RegisterEffectServiceServer(s, &effectServer{effects: effects})
	controlv1.RegisterRecoveryServiceServer(s, &recoveryServer{recovery: recovery})
	controlv1.RegisterArtifactServiceServer(s, &artifactServer{artifacts: artifacts})
	return &Server{grpc: s, health: h, listen: listen}, nil
}

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
	return ln.Addr(), nil
}

// Stop withdraws readiness, drains in-flight calls within the timeout and
// then forces the stop (DD-09 §3).
func (s *Server) Stop(timeout time.Duration) {
	s.health.SetServingStatus("", grpc_health_v1.HealthCheckResponse_NOT_SERVING)
	done := make(chan struct{})
	go func() { s.grpc.GracefulStop(); close(done) }()
	select {
	case <-done:
	case <-time.After(timeout):
		s.grpc.Stop()
	}
}
