package postgres

import (
	"context"
	"errors"
	"time"

	"github.com/ancyloce/anvilkit-agent-control/internal/adapters/postgres/sqlc"
	"github.com/ancyloce/anvilkit-agent-control/internal/domain"
)

func toPriceRecord(m sqlc.PriceRevision) *domain.PriceRecord {
	return &domain.PriceRecord{
		Price: domain.Price{
			Revision: m.Revision, Kind: domain.DispatchKind(m.Kind), Route: m.Route, Provider: m.Provider, Model: m.Model, Currency: m.Currency,
			EffectiveFrom: fromTs(m.EffectiveFrom), EffectiveUntil: fromTs(m.EffectiveUntil),
			PerMillion: map[domain.UsageCategory]int64{
				domain.UsageInput: m.InputUnits, domain.UsageOutput: m.OutputUnits, domain.UsageReasoning: m.ReasoningUnits, domain.UsageCachedInput: m.CachedInputUnits,
			},
			InputIncludesCached: m.InputIncludesCached, OutputIncludesReasoning: m.OutputIncludesReasoning, MaxExposure: m.MaxExposure,
		},
		Digest: domain.Digest(m.Digest), PublishedBy: m.PublishedBy, CommandID: m.CommandID, PublishedAt: fromTs(m.PublishedAt),
		WithdrawnAt: fromTsPtr(m.WithdrawnAt), WithdrawnBy: deref(m.WithdrawnBy),
	}
}

func (r *repo) InsertPriceRevision(ctx context.Context, p *domain.PriceRecord) error {
	return mapErr(r.q.InsertPriceRevision(ctx, sqlc.InsertPriceRevisionParams{
		Revision: p.Revision, Kind: string(p.Kind), Route: p.Route, Provider: p.Provider, Model: p.Model, Currency: p.Currency,
		EffectiveFrom: ts(p.EffectiveFrom), EffectiveUntil: ts(p.EffectiveUntil),
		InputUnits: p.PerMillion[domain.UsageInput], OutputUnits: p.PerMillion[domain.UsageOutput], ReasoningUnits: p.PerMillion[domain.UsageReasoning], CachedInputUnits: p.PerMillion[domain.UsageCachedInput],
		MaxExposure: p.MaxExposure, InputIncludesCached: p.InputIncludesCached, OutputIncludesReasoning: p.OutputIncludesReasoning,
		Digest: string(p.Digest), PublishedBy: p.PublishedBy, CommandID: p.CommandID, PublishedAt: ts(p.PublishedAt),
	}))
}

func (r *repo) GetPriceRevision(ctx context.Context, revision string) (*domain.PriceRecord, error) {
	m, err := r.q.GetPriceRevision(ctx, revision)
	if err != nil {
		return nil, mapErr(err)
	}
	return toPriceRecord(m), nil
}

func (r *repo) LockPriceRevision(ctx context.Context, revision string) (*domain.PriceRecord, error) {
	m, err := r.q.LockPriceRevision(ctx, revision)
	if err != nil {
		return nil, mapErr(err)
	}
	return toPriceRecord(m), nil
}

func (r *repo) WithdrawPriceRevision(ctx context.Context, revision string, at time.Time, by string) error {
	return r.q.WithdrawPriceRevision(ctx, sqlc.WithdrawPriceRevisionParams{Revision: revision, WithdrawnAt: ts(at), WithdrawnBy: &by})
}

func (r *repo) ListPriceRevisions(ctx context.Context) ([]*domain.PriceRecord, error) {
	rows, err := r.q.ListPriceRevisions(ctx)
	if err != nil {
		return nil, mapErr(err)
	}
	out := make([]*domain.PriceRecord, 0, len(rows))
	for _, m := range rows {
		out = append(out, toPriceRecord(m))
	}
	return out, nil
}

func (r *repo) CountPriceRevisions(ctx context.Context) (int64, error) {
	n, err := r.q.CountPriceRevisions(ctx)
	return n, mapErr(err)
}

func toCostCap(m sqlc.CostCap) *domain.CostCap {
	return &domain.CostCap{
		ID: m.CapID, Level: domain.CapLevel(m.Level), TenantID: m.TenantID, ProjectID: m.ProjectID, Currency: m.Currency, Cap: m.CapAmount,
		Reserved: m.Reserved, Consumed: m.Consumed, Revision: uint64(m.Revision), UpdatedBy: m.UpdatedBy, UpdatedAt: fromTs(m.UpdatedAt),
	}
}

func (r *repo) GetCostCap(ctx context.Context, level domain.CapLevel, tenantID, projectID, currency string) (*domain.CostCap, error) {
	m, err := r.q.GetCostCap(ctx, sqlc.GetCostCapParams{Level: string(level), TenantID: tenantID, ProjectID: projectID, Currency: currency})
	if err != nil {
		return nil, mapErr(err)
	}
	return toCostCap(m), nil
}

func (r *repo) LockCostCap(ctx context.Context, capID string) (*domain.CostCap, error) {
	m, err := r.q.LockCostCapByID(ctx, capID)
	if err != nil {
		return nil, mapErr(err)
	}
	return toCostCap(m), nil
}

func (r *repo) InsertCostCap(ctx context.Context, c *domain.CostCap) error {
	return mapErr(r.q.InsertCostCap(ctx, sqlc.InsertCostCapParams{
		CapID: c.ID, Level: string(c.Level), TenantID: c.TenantID, ProjectID: c.ProjectID, Currency: c.Currency, CapAmount: c.Cap,
		Reserved: c.Reserved, Consumed: c.Consumed, Revision: int64(c.Revision), UpdatedBy: c.UpdatedBy, UpdatedAt: ts(c.UpdatedAt),
	}))
}

func (r *repo) UpdateCostCapLimit(ctx context.Context, c *domain.CostCap) error {
	return r.q.UpdateCostCapLimit(ctx, sqlc.UpdateCostCapLimitParams{CapID: c.ID, CapAmount: c.Cap, Revision: int64(c.Revision), UpdatedBy: c.UpdatedBy, UpdatedAt: ts(c.UpdatedAt)})
}

func (r *repo) UpdateCostCapCounters(ctx context.Context, c *domain.CostCap) error {
	return r.q.UpdateCostCapCounters(ctx, sqlc.UpdateCostCapCountersParams{CapID: c.ID, Reserved: c.Reserved, Consumed: c.Consumed, Revision: int64(c.Revision)})
}

func (r *repo) LockOperationCostCaps(ctx context.Context, operationID string) ([]*domain.CostCap, error) {
	rows, err := r.q.LockOperationCostCaps(ctx, operationID)
	if err != nil {
		return nil, mapErr(err)
	}
	out := make([]*domain.CostCap, 0, len(rows))
	for _, m := range rows {
		out = append(out, toCostCap(m))
	}
	return out, nil
}

func (r *repo) InsertPool(ctx context.Context, p *domain.BudgetPool, createdBy string) error {
	return mapErr(r.q.InsertPool(ctx, sqlc.InsertPoolParams{
		PoolID: p.ID, Level: string(p.Level), TenantID: strPtr(p.TenantID), ActorID: strPtr(p.ActorID), PeriodStart: ts(p.PeriodStart), PeriodEnd: ts(p.PeriodEnd),
		Currency: p.Currency, CapAmount: p.Cap, Allocated: p.Allocated, Revision: int64(p.Revision), CreatedBy: createdBy,
	}))
}

func (r *repo) GetPool(ctx context.Context, poolID string) (*domain.BudgetPool, error) {
	m, err := r.q.GetPool(ctx, poolID)
	if err != nil {
		return nil, mapErr(err)
	}
	return toPool(m), nil
}

func (r *repo) GetPoolByScope(ctx context.Context, level domain.BudgetLevel, tenantID, actorID string, periodStart time.Time) (*domain.BudgetPool, error) {
	m, err := r.q.GetPoolByScope(ctx, sqlc.GetPoolByScopeParams{Level: string(level), TenantKey: tenantID, ActorKey: actorID, PeriodStart: ts(periodStart)})
	if err != nil {
		return nil, mapErr(err)
	}
	return toPool(m), nil
}

func (r *repo) UpdatePoolCap(ctx context.Context, p *domain.BudgetPool) error {
	return r.q.UpdatePoolCap(ctx, sqlc.UpdatePoolCapParams{PoolID: p.ID, CapAmount: p.Cap, Revision: int64(p.Revision)})
}

func (r *repo) InsertAllocationReturn(ctx context.Context, a *domain.Allocation, amount int64, at time.Time) error {
	return mapErr(r.q.InsertAllocationReturn(ctx, sqlc.InsertAllocationReturnParams{AllocationID: a.ID, PoolID: a.PoolID, OperationID: a.OperationID, Currency: a.Currency, Amount: amount, ReturnedAt: ts(at)}))
}

func (r *repo) HasAllocationReturn(ctx context.Context, allocationID string) (bool, error) {
	_, err := r.q.GetAllocationReturn(ctx, allocationID)
	if errors.Is(mapErr(err), domain.ErrNotFound) {
		return false, nil
	}
	return err == nil, mapErr(err)
}

func (r *repo) ListFinanceUnsettled(ctx context.Context, limit int) ([]string, error) {
	ids, err := r.q.ListFinanceUnsettled(ctx, int32(limit))
	return ids, mapErr(err)
}

func (r *repo) GetOperation(ctx context.Context, operationID string) (*domain.Operation, error) {
	m, err := r.q.GetOperationByID(ctx, operationID)
	if err != nil {
		return nil, mapErr(err)
	}
	return toOperation(m), nil
}

func (r *repo) ListAllocationsByOperation(ctx context.Context, operationID string) ([]*domain.Allocation, error) {
	rows, err := r.q.ListAllocationsByOperation(ctx, operationID)
	if err != nil {
		return nil, mapErr(err)
	}
	out := make([]*domain.Allocation, 0, len(rows))
	for _, m := range rows {
		out = append(out, toAllocation(m))
	}
	return out, nil
}
