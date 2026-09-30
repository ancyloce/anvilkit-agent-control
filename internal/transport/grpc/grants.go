package grpc

import (
	"context"
	"fmt"

	"google.golang.org/protobuf/types/known/timestamppb"

	controlv1 "github.com/ancyloce/anvilkit-agent-contracts/go/anvilkit/control/v1"
	"github.com/ancyloce/anvilkit-agent-control/internal/application"
	"github.com/ancyloce/anvilkit-agent-control/internal/domain"
)

// grantPolicyServer adapts anvilkit.control.v1.GrantPolicyService (DD-08 §2)
// to the application's registration receipt and revocation barrier. The
// caller is MCP (plaintext and unauthenticated in DEVELOPMENT_ONLY; the
// workload identity that restricts it to MCP is ENV-03).
type grantPolicyServer struct {
	controlv1.UnimplementedGrantPolicyServiceServer
	grants *application.GrantPolicies
}

var revocationStateToProto = map[string]controlv1.RevocationState{
	"fenced": controlv1.RevocationState_REVOCATION_STATE_FENCED, "converging": controlv1.RevocationState_REVOCATION_STATE_CONVERGING,
	"converged": controlv1.RevocationState_REVOCATION_STATE_CONVERGED,
}

func toRevocationStatus(g *domain.GrantPolicy) *controlv1.RevocationStatus {
	out := &controlv1.RevocationStatus{
		GrantId: g.GrantID, GrantRevision: fmt.Sprintf("%d", g.GrantRevision), State: revocationStateToProto[g.RevocationState],
		InFlightCalls: fmt.Sprintf("%d", g.InFlightCalls), UnknownCalls: fmt.Sprintf("%d", g.UnknownCalls),
	}
	if g.FencedAt != nil {
		out.FencedAt = timestamppb.New(*g.FencedAt)
	}
	if g.ConvergedAt != nil {
		out.ConvergedAt = timestamppb.New(*g.ConvergedAt)
	}
	return out
}

func (s *grantPolicyServer) RegisterPolicy(ctx context.Context, req *controlv1.RegisterPolicyRequest) (*controlv1.RegisterPolicyResponse, error) {
	cmd, err := commandIdentity(req.GetCommand())
	if err != nil {
		return nil, toStatus(err)
	}
	rev, err := domain.ParseRevision(req.GetGrantRevision())
	if err != nil {
		return nil, toStatus(err)
	}
	digest, err := domain.ParseDigest(req.GetPolicyDigest())
	if err != nil {
		return nil, toStatus(err)
	}
	reg := application.Registration{GrantID: req.GetGrantId(), Revision: uint64(rev), PolicyDigest: digest, ServerID: req.GetServerId(), Methods: req.GetMethods()}
	if req.GetCostCap() != nil {
		m, err := money(req.GetCostCap())
		if err != nil {
			return nil, toStatus(err)
		}
		reg.CostCap = &m
	}
	if req.GetExpiresAt() != nil {
		at := req.GetExpiresAt().AsTime()
		reg.ExpiresAt = &at
	}
	p, existing, err := s.grants.Register(ctx, cmd, reg)
	if err != nil {
		return nil, toStatus(err)
	}
	return &controlv1.RegisterPolicyResponse{ReceiptId: p.ReceiptID, PolicyEpoch: fmt.Sprintf("%d", p.PolicyEpoch), Existing: existing}, nil
}

func (s *grantPolicyServer) BeginRevocation(ctx context.Context, req *controlv1.BeginRevocationRequest) (*controlv1.BeginRevocationResponse, error) {
	cmd, err := commandIdentity(req.GetCommand())
	if err != nil {
		return nil, toStatus(err)
	}
	rev, err := domain.ParseRevision(req.GetGrantRevision())
	if err != nil {
		return nil, toStatus(err)
	}
	p, existing, err := s.grants.BeginRevocation(ctx, cmd, req.GetGrantId(), uint64(rev))
	if err != nil {
		return nil, toStatus(err)
	}
	return &controlv1.BeginRevocationResponse{Status: toRevocationStatus(p), Existing: existing}, nil
}

func (s *grantPolicyServer) GetRevocation(ctx context.Context, req *controlv1.GetRevocationRequest) (*controlv1.GetRevocationResponse, error) {
	rev, err := domain.ParseRevision(req.GetGrantRevision())
	if err != nil {
		return nil, toStatus(err)
	}
	p, err := s.grants.Revocation(ctx, req.GetGrantId(), uint64(rev))
	if err != nil {
		return nil, toStatus(err)
	}
	return &controlv1.GetRevocationResponse{Status: toRevocationStatus(p)}, nil
}
