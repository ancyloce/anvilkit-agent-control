package grpc

import (
	"context"
	"strconv"

	"google.golang.org/protobuf/types/known/timestamppb"

	controlv1 "github.com/ancyloce/anvilkit-agent-contracts/go/anvilkit/control/v1"
	"github.com/ancyloce/anvilkit-agent-control/internal/application"
	"github.com/ancyloce/anvilkit-agent-control/internal/domain"
)

// priceAdminServer and budgetPoolServer adapt the operator administration of
// the price book, the pools and the cost caps (P2.1); the API workload
// carries the operator's verified roles and Finance decides on them.
type priceAdminServer struct {
	controlv1.UnimplementedPriceAdminServiceServer
	finance *application.Finance
}

type budgetPoolServer struct {
	controlv1.UnimplementedBudgetPoolServiceServer
	finance *application.Finance
}

// authorizationServer adapts AuthorizationService (P2.1): Knowledge asks
// before disclosure, MCP before execution.
type authorizationServer struct {
	controlv1.UnimplementedAuthorizationServiceServer
	authorization *application.Authorization
}

func unitPrice(s string) (int64, error) {
	v, err := strconv.ParseInt(s, 10, 64)
	if err != nil || v < 0 {
		return 0, domain.ErrInvalid
	}
	return v, nil
}

func priceFromProto(p *controlv1.PriceRevision) (domain.Price, error) {
	out := domain.Price{
		Revision: p.GetRevision(), Kind: domain.DispatchKind(p.GetKind()), Route: p.GetRoute(), Provider: p.GetProvider(), Model: p.GetModel(), Currency: p.GetCurrency(),
		EffectiveFrom: p.GetEffectiveFrom().AsTime(), EffectiveUntil: p.GetEffectiveUntil().AsTime(), PerMillion: map[domain.UsageCategory]int64{},
		InputIncludesCached: p.GetInputIncludesCached(), OutputIncludesReasoning: p.GetOutputIncludesReasoning(),
	}
	pm := p.GetPerMillion()
	for c, s := range map[domain.UsageCategory]string{domain.UsageInput: pm.GetInputUnits(), domain.UsageOutput: pm.GetOutputUnits(), domain.UsageReasoning: pm.GetReasoningUnits(), domain.UsageCachedInput: pm.GetCachedInputUnits()} {
		v, err := unitPrice(s)
		if err != nil {
			return domain.Price{}, err
		}
		out.PerMillion[c] = v
	}
	exposure, err := domain.ParseMoney(p.GetCurrency(), p.GetMaxExposure())
	if err != nil {
		return domain.Price{}, err
	}
	out.MaxExposure = exposure.Amount
	return out, nil
}

func toPriceRevision(r *domain.PriceRecord) *controlv1.PriceRevision {
	f := strconv.FormatInt
	out := &controlv1.PriceRevision{
		Revision: r.Revision, Kind: string(r.Kind), Route: r.Route, Provider: r.Provider, Model: r.Model, Currency: r.Currency,
		EffectiveFrom: timestamppb.New(r.EffectiveFrom), EffectiveUntil: timestamppb.New(r.EffectiveUntil),
		PerMillion: &controlv1.UnitPrices{InputUnits: f(r.PerMillion[domain.UsageInput], 10), OutputUnits: f(r.PerMillion[domain.UsageOutput], 10), ReasoningUnits: f(r.PerMillion[domain.UsageReasoning], 10), CachedInputUnits: f(r.PerMillion[domain.UsageCachedInput], 10)},
		MaxExposure: f(r.MaxExposure, 10), InputIncludesCached: r.InputIncludesCached, OutputIncludesReasoning: r.OutputIncludesReasoning,
		Digest: string(r.Digest), PublishedAt: timestamppb.New(r.PublishedAt),
	}
	if r.WithdrawnAt != nil {
		out.WithdrawnAt = timestamppb.New(*r.WithdrawnAt)
	}
	return out
}

func (s *priceAdminServer) PublishPriceRevision(ctx context.Context, req *controlv1.PublishPriceRevisionRequest) (*controlv1.PublishPriceRevisionResponse, error) {
	cmd, err := commandIdentity(req.GetCommand())
	if err != nil {
		return nil, toStatus(err)
	}
	p, err := priceFromProto(req.GetPrice())
	if err != nil {
		return nil, toStatus(err)
	}
	rec, existing, err := s.finance.PublishPrice(ctx, cmd, scope(ctx, req.GetScope()), p)
	if err != nil {
		return nil, toStatus(err)
	}
	return &controlv1.PublishPriceRevisionResponse{Price: toPriceRevision(rec), Existing: existing}, nil
}

func (s *priceAdminServer) WithdrawPriceRevision(ctx context.Context, req *controlv1.WithdrawPriceRevisionRequest) (*controlv1.WithdrawPriceRevisionResponse, error) {
	cmd, err := commandIdentity(req.GetCommand())
	if err != nil {
		return nil, toStatus(err)
	}
	rec, existing, err := s.finance.WithdrawPrice(ctx, cmd, scope(ctx, req.GetScope()), req.GetRevision(), req.GetReason())
	if err != nil {
		return nil, toStatus(err)
	}
	return &controlv1.WithdrawPriceRevisionResponse{Price: toPriceRevision(rec), Existing: existing}, nil
}

func (s *priceAdminServer) GetPriceRevision(ctx context.Context, req *controlv1.GetPriceRevisionRequest) (*controlv1.GetPriceRevisionResponse, error) {
	rec, err := s.finance.GetPrice(ctx, scope(ctx, req.GetScope()), req.GetRevision())
	if err != nil {
		return nil, toStatus(err)
	}
	return &controlv1.GetPriceRevisionResponse{Price: toPriceRevision(rec)}, nil
}

var budgetLevelFromProto = map[controlv1.BudgetLevel]domain.BudgetLevel{
	controlv1.BudgetLevel_BUDGET_LEVEL_PLATFORM: domain.LevelPlatform, controlv1.BudgetLevel_BUDGET_LEVEL_TENANT: domain.LevelTenant, controlv1.BudgetLevel_BUDGET_LEVEL_ACTOR: domain.LevelActor,
}

var budgetLevelToProto = map[domain.BudgetLevel]controlv1.BudgetLevel{
	domain.LevelPlatform: controlv1.BudgetLevel_BUDGET_LEVEL_PLATFORM, domain.LevelTenant: controlv1.BudgetLevel_BUDGET_LEVEL_TENANT, domain.LevelActor: controlv1.BudgetLevel_BUDGET_LEVEL_ACTOR,
}

var capLevelFromProto = map[controlv1.CostCapLevel]domain.CapLevel{
	controlv1.CostCapLevel_COST_CAP_LEVEL_TENANT: domain.CapTenant, controlv1.CostCapLevel_COST_CAP_LEVEL_PROJECT: domain.CapProject, controlv1.CostCapLevel_COST_CAP_LEVEL_OPERATION: domain.CapOperation,
}

var capLevelToProto = map[domain.CapLevel]controlv1.CostCapLevel{
	domain.CapTenant: controlv1.CostCapLevel_COST_CAP_LEVEL_TENANT, domain.CapProject: controlv1.CostCapLevel_COST_CAP_LEVEL_PROJECT, domain.CapOperation: controlv1.CostCapLevel_COST_CAP_LEVEL_OPERATION,
}

func toBudgetPool(p *domain.BudgetPool) *controlv1.BudgetPool {
	m := func(v int64) *controlv1.Money { return toMoney(domain.Money{Currency: p.Currency, Amount: v}) }
	return &controlv1.BudgetPool{
		PoolId: p.ID, Level: budgetLevelToProto[p.Level], TenantId: p.TenantID, ActorId: p.ActorID, PeriodStart: timestamppb.New(p.PeriodStart), PeriodEnd: timestamppb.New(p.PeriodEnd),
		Cap: m(p.Cap), Allocated: m(p.Allocated), Available: m(max(0, p.Cap-p.Allocated)), Revision: domain.Revision(p.Revision).String(),
	}
}

func toCostCap(c *domain.CostCap) *controlv1.CostCap {
	m := func(v int64) *controlv1.Money { return toMoney(domain.Money{Currency: c.Currency, Amount: v}) }
	return &controlv1.CostCap{CapId: c.ID, Level: capLevelToProto[c.Level], TenantId: c.TenantID, ProjectId: c.ProjectID, Cap: m(c.Cap), Reserved: m(c.Reserved), Consumed: m(c.Consumed), Revision: domain.Revision(c.Revision).String()}
}

func (s *budgetPoolServer) CreatePool(ctx context.Context, req *controlv1.CreatePoolRequest) (*controlv1.CreatePoolResponse, error) {
	cmd, err := commandIdentity(req.GetCommand())
	if err != nil {
		return nil, toStatus(err)
	}
	capAmount, err := money(req.GetCap())
	if err != nil {
		return nil, toStatus(err)
	}
	pool, existing, err := s.finance.CreatePool(ctx, cmd, scope(ctx, req.GetScope()), application.PoolSpec{
		Level: budgetLevelFromProto[req.GetLevel()], TenantID: req.GetTenantId(), ActorID: req.GetActorId(),
		PeriodStart: req.GetPeriodStart().AsTime(), PeriodEnd: req.GetPeriodEnd().AsTime(), Cap: capAmount,
	})
	if err != nil {
		return nil, toStatus(err)
	}
	return &controlv1.CreatePoolResponse{Pool: toBudgetPool(pool), Existing: existing}, nil
}

func (s *budgetPoolServer) AdjustPool(ctx context.Context, req *controlv1.AdjustPoolRequest) (*controlv1.AdjustPoolResponse, error) {
	cmd, err := commandIdentity(req.GetCommand())
	if err != nil {
		return nil, toStatus(err)
	}
	capAmount, err := money(req.GetCap())
	if err != nil {
		return nil, toStatus(err)
	}
	expected, err := domain.ParseRevision(req.GetExpectedRevision())
	if err != nil {
		return nil, toStatus(err)
	}
	pool, err := s.finance.AdjustPool(ctx, cmd, scope(ctx, req.GetScope()), req.GetPoolId(), uint64(expected), capAmount)
	if err != nil {
		return nil, toStatus(err)
	}
	return &controlv1.AdjustPoolResponse{Pool: toBudgetPool(pool)}, nil
}

func (s *budgetPoolServer) GetPool(ctx context.Context, req *controlv1.GetPoolRequest) (*controlv1.GetPoolResponse, error) {
	pool, err := s.finance.GetPool(ctx, scope(ctx, req.GetScope()), req.GetPoolId())
	if err != nil {
		return nil, toStatus(err)
	}
	return &controlv1.GetPoolResponse{Pool: toBudgetPool(pool)}, nil
}

func (s *budgetPoolServer) SetCostCap(ctx context.Context, req *controlv1.SetCostCapRequest) (*controlv1.SetCostCapResponse, error) {
	cmd, err := commandIdentity(req.GetCommand())
	if err != nil {
		return nil, toStatus(err)
	}
	capAmount, err := money(req.GetCap())
	if err != nil {
		return nil, toStatus(err)
	}
	var expected *uint64
	if req.ExpectedRevision != nil {
		rev, err := domain.ParseRevision(req.GetExpectedRevision())
		if err != nil {
			return nil, toStatus(err)
		}
		v := uint64(rev)
		expected = &v
	}
	c, err := s.finance.SetCostCap(ctx, cmd, scope(ctx, req.GetScope()), capLevelFromProto[req.GetLevel()], req.GetTenantId(), req.GetProjectId(), capAmount, expected)
	if err != nil {
		return nil, toStatus(err)
	}
	return &controlv1.SetCostCapResponse{Cap: toCostCap(c)}, nil
}

func (s *budgetPoolServer) GetCostCap(ctx context.Context, req *controlv1.GetCostCapRequest) (*controlv1.GetCostCapResponse, error) {
	c, err := s.finance.GetCostCap(ctx, scope(ctx, req.GetScope()), capLevelFromProto[req.GetLevel()], req.GetTenantId(), req.GetProjectId(), req.GetCurrency())
	if err != nil {
		return nil, toStatus(err)
	}
	return &controlv1.GetCostCapResponse{Cap: toCostCap(c)}, nil
}

func authorizationQuery(ctx context.Context, q *controlv1.AuthorizationQuery, disclosure bool) domain.AuthorizationQuery {
	return domain.AuthorizationQuery{Scope: scope(ctx, q.GetScope()), ResourceType: q.GetResourceType(), ResourceID: q.GetResourceId(), Action: q.GetAction(), Disclosure: disclosure}
}

func toDecision(d domain.Decision) *controlv1.AuthorizationDecision {
	out := &controlv1.AuthorizationDecision{Decision: controlv1.Decision_DECISION_DENY, Revision: d.Revision, FreshUntil: timestamppb.New(d.FreshUntil)}
	if d.Allow {
		out.Decision = controlv1.Decision_DECISION_ALLOW
	} else if d.ReasonCode != "" {
		reason := d.ReasonCode
		out.ReasonCode = &reason
	}
	return out
}

func (s *authorizationServer) CheckDisclosure(ctx context.Context, req *controlv1.CheckDisclosureRequest) (*controlv1.CheckDisclosureResponse, error) {
	d, err := s.authorization.Decide(ctx, authorizationQuery(ctx, req.GetQuery(), true))
	if err != nil {
		return nil, toStatus(err)
	}
	return &controlv1.CheckDisclosureResponse{Decision: toDecision(d)}, nil
}

func (s *authorizationServer) CheckExecution(ctx context.Context, req *controlv1.CheckExecutionRequest) (*controlv1.CheckExecutionResponse, error) {
	d, err := s.authorization.Decide(ctx, authorizationQuery(ctx, req.GetQuery(), false))
	if err != nil {
		return nil, toStatus(err)
	}
	return &controlv1.CheckExecutionResponse{Decision: toDecision(d)}, nil
}
