package postgres

import (
	"context"
	"time"

	"github.com/ancyloce/anvilkit-agent-control/internal/adapters/postgres/sqlc"
	"github.com/ancyloce/anvilkit-agent-control/internal/domain"
)

// ---- recovery runs ----

func (r *repo) GetLaunch(ctx context.Context, launchID string) (*domain.Launch, error) {
	m, err := r.q.GetLaunch(ctx, launchID)
	if err != nil {
		return nil, mapErr(err)
	}
	return toLaunch(m), nil
}

func toRun(m sqlc.RecoveryRun) *domain.RecoveryRun {
	return &domain.RecoveryRun{
		ID: m.RunID, ScopeKey: m.ScopeKey, TenantID: m.TenantID, CommandID: m.CommandID, ActorID: m.ActorID, RequestDigest: domain.Digest(m.RequestDigest),
		RecoveryEpoch: uint64(m.RecoveryEpoch), Phase: domain.RecoveryPhase(m.Phase), WindowStart: fromTs(m.WindowStart), WindowEnd: fromTs(m.WindowEnd),
		Skew: time.Duration(m.SkewMicros) * time.Microsecond, Reason: m.Reason, FencedCount: uint64(m.FencedCount), CreatedAt: fromTs(m.CreatedAt),
		UpdatedAt: fromTs(m.UpdatedAt), ReopenedAt: fromTsPtr(m.ReopenedAt),
	}
}

func (r *repo) GetRecoveryRunByCommand(ctx context.Context, scopeKey, commandID string) (*domain.RecoveryRun, error) {
	m, err := r.q.GetRecoveryRunByCommand(ctx, sqlc.GetRecoveryRunByCommandParams{ScopeKey: scopeKey, CommandID: commandID})
	if err != nil {
		return nil, mapErr(err)
	}
	return toRun(m), nil
}

func (r *repo) GetRecoveryRun(ctx context.Context, runID string) (*domain.RecoveryRun, error) {
	m, err := r.q.GetRecoveryRun(ctx, runID)
	if err != nil {
		return nil, mapErr(err)
	}
	return toRun(m), nil
}

func (r *repo) LockRecoveryRun(ctx context.Context, runID string) (*domain.RecoveryRun, error) {
	m, err := r.q.LockRecoveryRun(ctx, runID)
	if err != nil {
		return nil, mapErr(err)
	}
	return toRun(m), nil
}

func (r *repo) MaxRecoveryEpoch(ctx context.Context) (uint64, error) {
	n, err := r.q.MaxRecoveryEpoch(ctx)
	return uint64(n), mapErr(err)
}

func (r *repo) InsertRecoveryRun(ctx context.Context, run *domain.RecoveryRun) error {
	return r.q.InsertRecoveryRun(ctx, sqlc.InsertRecoveryRunParams{
		RunID: run.ID, ScopeKey: run.ScopeKey, TenantID: run.TenantID, CommandID: run.CommandID, ActorID: run.ActorID, RequestDigest: string(run.RequestDigest),
		RecoveryEpoch: int64(run.RecoveryEpoch), Phase: string(run.Phase), WindowStart: ts(run.WindowStart), WindowEnd: ts(run.WindowEnd),
		SkewMicros: int64(run.Skew / time.Microsecond), Reason: run.Reason, FencedCount: int64(run.FencedCount), CreatedAt: ts(run.CreatedAt), UpdatedAt: ts(run.UpdatedAt), ReopenedAt: tsPtr(run.ReopenedAt),
	})
}

func (r *repo) UpdateRecoveryRun(ctx context.Context, run *domain.RecoveryRun) error {
	return r.q.UpdateRecoveryRun(ctx, sqlc.UpdateRecoveryRunParams{RunID: run.ID, Phase: string(run.Phase), FencedCount: int64(run.FencedCount), UpdatedAt: ts(run.UpdatedAt), ReopenedAt: tsPtr(run.ReopenedAt)})
}

func (r *repo) ListRecoveryPending(ctx context.Context, limit int) ([]*domain.RecoveryRun, error) {
	rows, err := r.q.ListRecoveryPending(ctx, int32(limit))
	if err != nil {
		return nil, mapErr(err)
	}
	out := make([]*domain.RecoveryRun, 0, len(rows))
	for _, m := range rows {
		out = append(out, toRun(m))
	}
	return out, nil
}

func (r *repo) ListActiveOperations(ctx context.Context, scopeKey string) ([]*domain.Operation, error) {
	rows, err := r.q.ListActiveOperations(ctx, scopeKey)
	if err != nil {
		return nil, mapErr(err)
	}
	out := make([]*domain.Operation, 0, len(rows))
	for _, m := range rows {
		out = append(out, toOperation(m))
	}
	return out, nil
}

func toProgress(m sqlc.RecoveryProgress) *domain.RecoveryProgress {
	return &domain.RecoveryProgress{RunID: m.RunID, Class: m.Class, Cursor: m.Cursor, Complete: m.Complete, Seen: uint64(m.Seen)}
}

func (r *repo) GetRecoveryProgress(ctx context.Context, runID, class string) (*domain.RecoveryProgress, error) {
	m, err := r.q.GetRecoveryProgress(ctx, sqlc.GetRecoveryProgressParams{RunID: runID, Class: class})
	if err != nil {
		return nil, mapErr(err)
	}
	return toProgress(m), nil
}

func (r *repo) UpsertRecoveryProgress(ctx context.Context, p *domain.RecoveryProgress) error {
	return r.q.UpsertRecoveryProgress(ctx, sqlc.UpsertRecoveryProgressParams{RunID: p.RunID, Class: p.Class, Cursor: p.Cursor, Complete: p.Complete, Seen: int64(p.Seen)})
}

func (r *repo) ListRecoveryProgress(ctx context.Context, runID string) ([]*domain.RecoveryProgress, error) {
	rows, err := r.q.ListRecoveryProgress(ctx, runID)
	if err != nil {
		return nil, mapErr(err)
	}
	out := make([]*domain.RecoveryProgress, 0, len(rows))
	for _, m := range rows {
		out = append(out, toProgress(m))
	}
	return out, nil
}

func toFinding(m sqlc.RecoveryFinding) *domain.RecoveryFinding {
	return &domain.RecoveryFinding{
		ID: m.FindingID, RunID: m.RunID, Class: m.Class, ObligationID: m.ObligationID, TenantID: m.TenantID, InventoryKey: m.InventoryKey, InventoryVersion: m.InventoryVersion,
		RecordedAt: fromTs(m.RecordedAt), Status: domain.FindingStatus(m.Status), Outcome: m.Outcome, EvidenceRef: m.EvidenceRef, Detail: m.Detail,
		CreatedAt: fromTs(m.CreatedAt), UpdatedAt: fromTs(m.UpdatedAt),
	}
}

func (r *repo) GetFinding(ctx context.Context, findingID string) (*domain.RecoveryFinding, error) {
	m, err := r.q.GetFinding(ctx, findingID)
	if err != nil {
		return nil, mapErr(err)
	}
	return toFinding(m), nil
}

func (r *repo) LockFinding(ctx context.Context, findingID string) (*domain.RecoveryFinding, error) {
	m, err := r.q.LockFinding(ctx, findingID)
	if err != nil {
		return nil, mapErr(err)
	}
	return toFinding(m), nil
}

func (r *repo) GetFindingByObligation(ctx context.Context, runID, class, obligationID string) (*domain.RecoveryFinding, error) {
	m, err := r.q.GetFindingByObligation(ctx, sqlc.GetFindingByObligationParams{RunID: runID, Class: class, ObligationID: obligationID})
	if err != nil {
		return nil, mapErr(err)
	}
	return toFinding(m), nil
}

func (r *repo) InsertFinding(ctx context.Context, f *domain.RecoveryFinding) error {
	return r.q.InsertFinding(ctx, sqlc.InsertFindingParams{
		FindingID: f.ID, RunID: f.RunID, Class: f.Class, ObligationID: f.ObligationID, TenantID: f.TenantID, InventoryKey: f.InventoryKey, InventoryVersion: f.InventoryVersion,
		RecordedAt: ts(f.RecordedAt), Status: string(f.Status), Outcome: f.Outcome, EvidenceRef: f.EvidenceRef, Detail: f.Detail, CreatedAt: ts(f.CreatedAt), UpdatedAt: ts(f.UpdatedAt),
	})
}

func (r *repo) UpdateFinding(ctx context.Context, f *domain.RecoveryFinding) error {
	return r.q.UpdateFinding(ctx, sqlc.UpdateFindingParams{FindingID: f.ID, Status: string(f.Status), Outcome: f.Outcome, EvidenceRef: f.EvidenceRef, Detail: f.Detail, UpdatedAt: ts(f.UpdatedAt)})
}

func (r *repo) ListFindings(ctx context.Context, runID string, status domain.FindingStatus, afterClass, afterObligationID string, limit int) ([]*domain.RecoveryFinding, error) {
	rows, err := r.q.ListFindings(ctx, sqlc.ListFindingsParams{RunID: runID, Column2: string(status), Column3: afterClass, Column4: afterObligationID, Limit: int32(limit)})
	if err != nil {
		return nil, mapErr(err)
	}
	out := make([]*domain.RecoveryFinding, 0, len(rows))
	for _, m := range rows {
		out = append(out, toFinding(m))
	}
	return out, nil
}

func (r *repo) CountUnsettledFindings(ctx context.Context, runID string) (uint64, error) {
	n, err := r.q.CountUnsettledFindings(ctx, runID)
	return uint64(n), mapErr(err)
}

func (r *repo) IsAdmissionClosed(ctx context.Context, tenantID string) (bool, error) {
	ok, err := r.q.IsAdmissionClosed(ctx, tenantID)
	return ok, mapErr(err)
}

func (r *repo) GetAdmissionClosure(ctx context.Context, scopeKey string) (*domain.AdmissionClosure, error) {
	m, err := r.q.GetAdmissionClosure(ctx, scopeKey)
	if err != nil {
		return nil, mapErr(err)
	}
	return &domain.AdmissionClosure{ScopeKey: m.ScopeKey, RunID: m.RunID, ClosedAt: fromTs(m.ClosedAt), ReopenedAt: fromTsPtr(m.ReopenedAt)}, nil
}

func (r *repo) UpsertAdmissionClosure(ctx context.Context, c *domain.AdmissionClosure) error {
	return r.q.UpsertAdmissionClosure(ctx, sqlc.UpsertAdmissionClosureParams{ScopeKey: c.ScopeKey, RunID: c.RunID, ClosedAt: ts(c.ClosedAt)})
}

func (r *repo) ReopenAdmission(ctx context.Context, scopeKey, runID string, at time.Time) error {
	return r.q.ReopenAdmission(ctx, sqlc.ReopenAdmissionParams{ScopeKey: scopeKey, RunID: runID, ReopenedAt: ts(at)})
}
