package postgres

import (
	"context"
	"time"

	"github.com/ancyloce/anvilkit-agent-control/internal/adapters/postgres/sqlc"
	"github.com/ancyloce/anvilkit-agent-control/internal/domain"
)

// P1.5 repository methods: the PREPARED dispatches of a closing attempt,
// the unresolved dispatches of the periodic reconciler and the recovery
// epoch sequence.

func (r *repo) ListPreparedDispatchesByAttempt(ctx context.Context, attemptID string) ([]*domain.Dispatch, error) {
	rows, err := r.q.ListPreparedDispatchesByAttempt(ctx, attemptID)
	if err != nil {
		return nil, mapErr(err)
	}
	out := make([]*domain.Dispatch, 0, len(rows))
	for _, m := range rows {
		out = append(out, toDispatch(m))
	}
	return out, nil
}

func (r *repo) ListUnresolvedDispatches(ctx context.Context, age time.Duration, limit int) ([]*domain.Dispatch, error) {
	rows, err := r.q.ListUnresolvedDispatches(ctx, sqlc.ListUnresolvedDispatchesParams{Limit: int32(limit), AgeSeconds: age.Seconds()})
	if err != nil {
		return nil, mapErr(err)
	}
	out := make([]*domain.Dispatch, 0, len(rows))
	for _, m := range rows {
		out = append(out, toDispatch(m))
	}
	return out, nil
}

func (r *repo) NextRecoveryEpoch(ctx context.Context) (uint64, error) {
	v, err := r.q.NextRecoveryEpoch(ctx)
	if err != nil {
		return 0, mapErr(err)
	}
	return uint64(v), nil
}

func (r *repo) AnyAdmissionClosed(ctx context.Context) (bool, error) {
	v, err := r.q.AnyAdmissionClosed(ctx)
	return v, mapErr(err)
}

func (r *repo) ListReconcilingSettlements(ctx context.Context, limit int) ([]string, error) {
	return r.q.ListReconcilingSettlements(ctx, int32(limit))
}
