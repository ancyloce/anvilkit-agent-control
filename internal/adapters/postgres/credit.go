package postgres

import (
	"context"
	"errors"
	"time"

	"github.com/ancyloce/anvilkit-agent-control/internal/adapters/postgres/sqlc"
	"github.com/ancyloce/anvilkit-agent-control/internal/domain"
)

// Credit pools and credit entries (DEC-066, migration 00020).

func toCreditPool(m sqlc.CreditPool) *domain.CreditPool {
	return &domain.CreditPool{
		ID: m.CreditPoolID, Level: domain.BudgetLevel(m.Level), TenantID: m.TenantID, ActorID: deref(m.ActorID),
		PeriodStart: fromTs(m.PeriodStart), PeriodEnd: fromTs(m.PeriodEnd), Limit: m.LimitCredits, Used: m.UsedCredits, Revision: uint64(m.Revision),
	}
}

func (r *repo) ListCurrentCreditPools(ctx context.Context, tenantID, actorID string, at time.Time) ([]*domain.CreditPool, error) {
	rows, err := r.q.ListCurrentCreditPools(ctx, sqlc.ListCurrentCreditPoolsParams{TenantID: tenantID, At: ts(at), ActorID: actorID})
	if err != nil {
		return nil, mapErr(err)
	}
	out := make([]*domain.CreditPool, 0, len(rows))
	for _, m := range rows {
		out = append(out, toCreditPool(m))
	}
	return out, nil
}

func (r *repo) LockCreditPool(ctx context.Context, creditPoolID string) (*domain.CreditPool, error) {
	m, err := r.q.LockCreditPool(ctx, creditPoolID)
	if err != nil {
		return nil, mapErr(err)
	}
	return toCreditPool(m), nil
}

func (r *repo) GetCreditPoolByScope(ctx context.Context, level domain.BudgetLevel, tenantID, actorID string, periodStart time.Time) (*domain.CreditPool, error) {
	m, err := r.q.GetCreditPoolByScope(ctx, sqlc.GetCreditPoolByScopeParams{Level: string(level), TenantID: tenantID, ActorKey: actorID, PeriodStart: ts(periodStart)})
	if err != nil {
		return nil, mapErr(err)
	}
	return toCreditPool(m), nil
}

func (r *repo) InsertCreditPool(ctx context.Context, p *domain.CreditPool, createdBy string) error {
	return mapErr(r.q.InsertCreditPool(ctx, sqlc.InsertCreditPoolParams{
		CreditPoolID: p.ID, Level: string(p.Level), TenantID: p.TenantID, ActorID: strPtr(p.ActorID), PeriodStart: ts(p.PeriodStart), PeriodEnd: ts(p.PeriodEnd),
		LimitCredits: p.Limit, UsedCredits: p.Used, Revision: int64(p.Revision), CreatedBy: createdBy,
	}))
}

func (r *repo) UpdateCreditPool(ctx context.Context, p *domain.CreditPool) error {
	return mapErr(r.q.UpdateCreditPool(ctx, sqlc.UpdateCreditPoolParams{CreditPoolID: p.ID, LimitCredits: p.Limit, UsedCredits: p.Used, Revision: int64(p.Revision)}))
}

func (r *repo) HasCreditEntry(ctx context.Context, creditPoolID, operationID string) (bool, error) {
	_, err := r.q.GetCreditEntry(ctx, sqlc.GetCreditEntryParams{CreditPoolID: creditPoolID, OperationID: operationID})
	if errors.Is(mapErr(err), domain.ErrNotFound) {
		return false, nil
	}
	return err == nil, mapErr(err)
}

func (r *repo) InsertCreditEntry(ctx context.Context, e *domain.CreditEntry) error {
	return mapErr(r.q.InsertCreditEntry(ctx, sqlc.InsertCreditEntryParams{
		EntryID: e.ID, OperationID: e.OperationID, CreditPoolID: e.CreditPoolID, Credits: e.Credits, CreatedAt: ts(e.CreatedAt),
	}))
}
